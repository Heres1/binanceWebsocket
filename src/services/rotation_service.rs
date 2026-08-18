//! 日级动量轮动服务
//!
//! 策略逻辑（3年跨周期回测验证：年化+49.4%，17次调仓；加追踪止损后Sharpe 1.17、回撤39.9%）：
//! - 品种池：BTC/ETH/SOL，每 rebalance_interval_days 天评估一次
//! - 信号：90日动量（close/close_90d前 - 1）最高者胜出
//! - 过滤：候选品种价格必须 > 其50日均线，否则空仓；最高动量 ≤ 0 也空仓
//! - 风控：持仓期价格从峰值回撤 ≥ trailing_stop_pct（默认12%）强制平仓转USDT，
//!   止损后等满一个完整调仓周期才允许再入场（防震荡市反复止损）
//! - 执行：全仓轮动（卖出旧品种→买入新品种），目标=当前则不动
//!
//! 与5m事件驱动策略完全不同：本服务不订阅任何市场事件，
//! 每 check_interval_seconds 定时拉取日线数据评估，调仓频率极低（月度）。

use crate::clients::BinanceClient;
use crate::config::RotationConfig;
use crate::error::{DomainError, ServiceError};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// 持久化状态（重启不重复调仓）
/// 新增字段均带 serde(default)，兼容旧状态文件
#[derive(Debug, Clone, Serialize, Deserialize)]
#[derive(Default)]
struct RotationState {
    last_rebalance_ms: u64,          // 上次调仓时间
    current_holding: Option<String>, // 当前持仓品种（None=空仓持USDT）
    #[serde(default)]
    entry_price: Option<f64>, // 当前持仓入场价（日志追溯用）
    #[serde(default)]
    peak_price: Option<f64>, // 持仓期间峰值价（追踪止损基准）
    #[serde(default)]
    last_stop_loss_ms: u64, // 上次追踪止损触发时间（日志追溯用）
}

/// 追踪止损判定（纯函数，便于单元测试）：
/// 返回(是否触发止损, 更新后的峰值)。价格无效(peak<=0)时不触发。
fn evaluate_trailing_stop(peak: f64, price: f64, stop_pct: f64) -> (bool, f64) {
    if peak <= 0.0 || price <= 0.0 {
        return (false, peak);
    }
    let new_peak = peak.max(price);
    let drawdown = (new_peak - price) / new_peak;
    (drawdown >= stop_pct, new_peak)
}


impl RotationState {
    fn load(path: &str) -> Self {
        std::fs::read_to_string(path)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default()
    }
    fn save(&self, path: &str) {
        if let Ok(s) = serde_json::to_string_pretty(self) {
            // 确保父目录存在：服务器全新部署时 data/ 可能不存在（.gitignore忽略），
            // 否则状态写入静默失败，重启后调仓计时无限重置
            if let Some(parent) = std::path::Path::new(path).parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            // 原子写入：先写临时文件再重命名，避免写入中途被kill导致状态文件损坏
            let tmp = format!("{}.tmp", path);
            if let Err(e) = std::fs::write(&tmp, &s).and_then(|_| std::fs::rename(&tmp, path)) {
                log::error!(
                    "❌ 轮动状态保存失败({}): {}（重启后将丢失调仓计时）",
                    path,
                    e
                );
            }
        }
    }
}

/// 日级动量轮动服务
pub struct RotationService {
    config: RotationConfig,
    client: BinanceClient,
    state_file: String,
}

/// 按交易对 stepSize 向下取整数量
fn round_step_size(quantity: f64, symbol: &str) -> f64 {
    // 与 Binance exchangeInfo LOT_SIZE.stepSize 保持一致（2026-08 查询）
    let decimals: u32 = match symbol {
        "BTCUSDT" => 5,  // stepSize 0.00001
        "ETHUSDT" => 4,  // stepSize 0.0001
        "SOLUSDT" | "BNBUSDT" | "LTCUSDT" => 3, // stepSize 0.001
        "LINKUSDT" | "DOTUSDT" | "AVAXUSDT" => 2, // stepSize 0.01
        "ADAUSDT" | "XRPUSDT" | "TRXUSDT" => 1, // stepSize 0.1
        "DOGEUSDT" => 0, // stepSize 1
        _ => 2,          // 未知品种保守取2位，避免超精度被拒单
    };
    let factor = 10_f64.powi(decimals as i32);
    ((quantity * factor) + 1e-9).floor() / factor
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// symbol -> 资产名（"BTCUSDT" -> "BTC"）
fn base_asset(symbol: &str) -> &str {
    symbol.strip_suffix("USDT").unwrap_or(symbol)
}

impl RotationService {
    pub fn new(config: RotationConfig, client: BinanceClient) -> Self {
        Self {
            config,
            client,
            state_file: "data/rotation_state.json".to_string(),
        }
    }

    /// 启动定时轮询任务（阻塞式，spawn到tokio任务中运行）
    pub async fn run(self: Arc<Self>) {
        log::info!(
            "🔄 动量轮动服务启动 | 品种: {:?} | 动量{}d | MA{}d | 调仓间隔{}d | 干跑: {} | 追踪止损: {}",
            self.config.symbols,
            self.config.momentum_lookback_days,
            self.config.ma_filter_days,
            self.config.rebalance_interval_days,
            self.config.dry_run,
            if self.config.trailing_stop_enabled {
                format!("开({:.0}%)", self.config.trailing_stop_pct * 100.0)
            } else {
                "关".to_string()
            }
        );
        loop {
            if let Err(e) = self.tick().await {
                log::warn!("⚠️ 轮动评估失败: {}（下个周期重试）", e);
            }
            tokio::time::sleep(std::time::Duration::from_secs(
                self.config.check_interval_seconds,
            ))
            .await;
        }
    }

    /// 单次评估：拉数据→算信号→查持仓→按需换仓
    async fn tick(&self) -> Result<(), DomainError> {
        let mut state = RotationState::load(&self.state_file);
        let now = now_ms();
        let interval_ms = self.config.rebalance_interval_days * 86400 * 1000;

        // 首次运行：识别当前实际持仓；默认等满一个周期再评估，
        // rebalance_on_start=true 时直接落入下方调仓流程立即按信号对齐（全新部署时对齐仓位用）
        if state.last_rebalance_ms == 0 {
            state.current_holding = self.detect_current_holding().await?;
            if !self.config.rebalance_on_start {
                state.last_rebalance_ms = now; // 从启动时刻起一个完整周期后再评估
                state.save(&self.state_file);
                log::info!(
                    "🔄 轮动服务初始化 | 当前持仓: {} | 首次调仓评估将于{}天后",
                    state.current_holding.as_deref().unwrap_or("空仓(USDT)"),
                    self.config.rebalance_interval_days
                );
                return Ok(());
            }
            log::info!(
                "🔄 轮动服务初始化 | 当前持仓: {} | rebalance_on_start=true，立即执行首次信号对齐",
                state.current_holding.as_deref().unwrap_or("空仓(USDT)")
            );
        }

        // === 追踪止损检查（每次tick执行，不等调仓日）===
        if self.config.trailing_stop_enabled {
            if let Some(holding) = state.current_holding.clone() {
                match self.check_trailing_stop(&holding, &mut state).await {
                    Ok(true) => {
                        // 已触发止损并处理完毕（含状态重置），本轮结束
                        return Ok(());
                    }
                    Ok(false) => {} // 未触发，继续正常流程
                    Err(e) => {
                        // 价格拉取失败：仅记录不中止，等下次tick重试
                        log::warn!("⚠️ {} 追踪止损检查失败: {}（下次重试）", holding, e);
                    }
                }
            }
        }

        // 未到调仓时间（首次运行且rebalance_on_start=true时elapsed必然>=interval，直接通过）
        let elapsed_ms = now.saturating_sub(state.last_rebalance_ms); // saturating_sub防时钟回拨下溢
        if elapsed_ms < interval_ms {
            let remain_h = (interval_ms - elapsed_ms) / 3_600_000;
            log::info!(
                "🔄 距下次调仓评估还有约 {}h | 当前: {}",
                remain_h,
                state.current_holding.as_deref().unwrap_or("空仓")
            );
            return Ok(());
        }

        // === 调仓日：计算目标品种 ===
        let target = self.compute_target().await?;
        let current = self.detect_current_holding().await?;
        log::info!(
            "🔄 调仓评估 | 当前: {} | 目标: {}",
            current.as_deref().unwrap_or("空仓(USDT)"),
            target.as_deref().unwrap_or("空仓(USDT)")
        );

        state.last_rebalance_ms = now;

        if target == current {
            log::info!("✅ 目标与当前一致，无需调仓");
            state.current_holding = current;
            // 旧状态无峰值记录时补初始化（追踪基准），有持仓才需要
            if state.current_holding.is_some() && state.peak_price.is_none() {
                if let Some(h) = state.current_holding.as_deref() {
                    if let Ok(p) = self.fetch_current_price(h).await {
                        state.peak_price = Some(p);
                        state.entry_price = state.entry_price.or(Some(p));
                        log::info!("📌 {} 追踪基准初始化 | 峰值=入场价 {:.2}", h, p);
                    }
                }
            }
            state.save(&self.state_file);
            return Ok(());
        }

        // === 执行换仓 ===
        if self.config.dry_run {
            log::info!(
                "🧪 [干跑] 应换仓: {} → {}",
                current.as_deref().unwrap_or("空仓"),
                target.as_deref().unwrap_or("空仓")
            );
            // 干跑不真实下单，状态保持实际持仓，避免心跳日志显示与真实账户不符
            state.current_holding = current;
        } else {
            self.execute_rotation(current.as_deref(), target.as_deref())
                .await?;
            state.current_holding = target;
            // 换仓后重置追踪基准：新仓以当前价为入场价与峰值起点
            match &state.current_holding {
                Some(h) => {
                    if let Ok(p) = self.fetch_current_price(h).await {
                        state.entry_price = Some(p);
                        state.peak_price = Some(p);
                        log::info!("📌 {} 新开仓追踪基准 | 入场价=峰值 {:.2}", h, p);
                    }
                }
                None => {
                    state.entry_price = None;
                    state.peak_price = None;
                }
            }
        }

        state.save(&self.state_file);
        Ok(())
    }

    /// 追踪止损检查：拉取持仓现价，更新峰值，回撤超阈值则强制平仓
    /// 返回 Ok(true)=已触发并处理，Ok(false)=未触发
    async fn check_trailing_stop(
        &self,
        holding: &str,
        state: &mut RotationState,
    ) -> Result<bool, DomainError> {
        let price = self.fetch_current_price(holding).await?;
        let now = now_ms();

        // 旧状态（升级前开的仓）无峰值记录：以当前价初始化，从升级时刻起追踪
        let peak = state.peak_price.unwrap_or(price);
        let (triggered, new_peak) =
            evaluate_trailing_stop(peak, price, self.config.trailing_stop_pct);
        state.peak_price = Some(new_peak);

        if !triggered {
            state.save(&self.state_file);
            return Ok(false);
        }

        // === 触发止损 ===
        let entry = state.entry_price.unwrap_or(new_peak);
        let drawdown = (new_peak - price) / new_peak;
        let hold_days = now.saturating_sub(state.last_rebalance_ms) / 86_400_000;
        log::info!(
            "🛑 [追踪止损触发] {} | 入场价 {:.2} | 峰值 {:.2} | 现价 {:.2} | 回撤 {:.1}% ≥ {:.0}% | 持仓约{}天",
            holding,
            entry,
            new_peak,
            price,
            drawdown * 100.0,
            self.config.trailing_stop_pct * 100.0,
            hold_days
        );

        if self.config.dry_run {
            log::info!("🧪 [干跑] 应止损清仓 {} → USDT（不实际下单）", holding);
            // 干跑不真实下单：状态保持实际持仓，但重置计时避免反复触发日志
            state.last_stop_loss_ms = now;
            state.last_rebalance_ms = now; // 等满一个完整周期再评估（防震荡市反复止损）
            state.save(&self.state_file);
            return Ok(true);
        }

        self.execute_rotation(Some(holding), None).await?;
        log::info!("✅ 止损清仓完成，转入USDT，{}天后重新评估", self.config.rebalance_interval_days);

        state.current_holding = None;
        state.entry_price = None;
        state.peak_price = None;
        state.last_stop_loss_ms = now;
        state.last_rebalance_ms = now; // 等满一个完整周期才允许再入场
        state.save(&self.state_file);
        Ok(true)
    }

    /// 拉取品种最新价（日线最新收盘价）
    async fn fetch_current_price(&self, symbol: &str) -> Result<f64, DomainError> {
        let klines = self
            .client
            .get_klines(symbol, "1d", None, None, Some(1))
            .await?;
        klines
            .last()
            .map(|k| k.close)
            .ok_or_else(|| ServiceError::MarketData(format!("{} 无法获取最新价格", symbol)).into())
    }

    /// 计算目标品种：90日动量最高 + 价格>MA50，否则None（空仓）
    async fn compute_target(&self) -> Result<Option<String>, DomainError> {
        let lb = self.config.momentum_lookback_days;
        let ma_n = self.config.ma_filter_days;
        let need = (lb + 2).max(ma_n + 1) as u16; // 需要的日线数量

        let mut best: Option<(String, f64)> = None;
        let mut fetched = 0usize; // 成功获取有效数据的品种数
        for symbol in &self.config.symbols {
            // 单品种拉取失败仅跳过（不中止整个调仓），全部失败时在下方统一报错
            let klines = match self
                .client
                .get_klines(symbol, "1d", None, None, Some(need))
                .await
            {
                Ok(k) => k,
                Err(e) => {
                    log::warn!("⚠️ {} 日线拉取失败，本次跳过: {}", symbol, e);
                    continue;
                }
            };
            // 需同时满足动量与均线的数据量要求，防止切片越界panic导致任务静默死亡
            if klines.len() < lb + 1 || klines.len() < ma_n {
                log::warn!("⚠️ {} 日线数据不足({}根)，跳过", symbol, klines.len());
                continue;
            }
            fetched += 1;
            let closes: Vec<f64> = klines.iter().map(|k| k.close).collect();
            let n = closes.len();
            let price = closes[n - 1];
            let momentum = price / closes[n - 1 - lb] - 1.0;
            let ma: f64 = closes[n - ma_n..].iter().sum::<f64>() / ma_n as f64;
            let above_ma = price > ma;
            log::info!(
                "📊 {} | 现价 {:.2} | {}日动量 {:+.2}% | MA{} {:.2} | 均线上: {}",
                symbol,
                price,
                lb,
                momentum * 100.0,
                ma_n,
                ma,
                above_ma
            );
            if momentum > 0.0 && above_ma {
                let is_better = best.as_ref().is_none_or(|(_, bm)| momentum > *bm);
                if is_better {
                    best = Some((symbol.clone(), momentum));
                }
            }
        }
        // 全部品种都无有效数据时中止本次调仓（防止"无数据→误判目标空仓→错误全卖"），下周期重试
        if fetched == 0 {
            return Err(ServiceError::MarketData(
                "所有品种日线数据拉取失败，本次调仓中止".to_string(),
            )
            .into());
        }
        Ok(best.map(|(s, _)| s))
    }

    /// 查询账户实际持仓：哪个品种的估值 > min_usdt_value
    async fn detect_current_holding(&self) -> Result<Option<String>, DomainError> {
        let account = self.client.get_account().await?;
        for symbol in &self.config.symbols {
            let asset = base_asset(symbol);
            if let Some(b) = account.balances.iter().find(|b| b.asset == asset) {
                let qty: f64 =
                    b.free.parse().unwrap_or(0.0) + b.locked.parse::<f64>().unwrap_or(0.0);
                if qty <= 0.0 {
                    continue;
                }
                // 用最新日收盘价估值
                let klines = self
                    .client
                    .get_klines(symbol, "1d", None, None, Some(1))
                    .await?;
                if let Some(k) = klines.last() {
                    let value = qty * k.close;
                    if value >= self.config.min_usdt_value {
                        return Ok(Some(symbol.clone()));
                    }
                }
            }
        }
        Ok(None)
    }

    /// 执行换仓：先卖旧品种（全卖），再买新品种（用99%可用USDT防手续费不足）
    async fn execute_rotation(
        &self,
        current: Option<&str>,
        target: Option<&str>,
    ) -> Result<(), DomainError> {
        // 1. 卖出旧品种
        if let Some(old_symbol) = current {
            let asset = base_asset(old_symbol);
            let balance = self.client.get_balance(asset).await?;
            let free: f64 = balance.free.parse().unwrap_or(0.0);
            let qty = round_step_size(free, old_symbol);
            if qty > 0.0 {
                log::info!("🔴 [轮动卖出] {} 数量 {}", old_symbol, qty);
                let resp = self
                    .client
                    .place_order(old_symbol, "SELL", "MARKET", qty, None, None)
                    .await?;
                log::info!(
                    "✅ 卖出成交: {} | 成交均价 {} | 成交额 {}",
                    old_symbol,
                    resp.price,
                    resp.cummulative_quote_qty
                );
            }
        }

        // 2. 买入新品种
        if let Some(new_symbol) = target {
            // 等待卖出结算（市价单通常即时，保守等2秒）
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            let usdt = self.client.get_balance("USDT").await?;
            let free_usdt: f64 = usdt.free.parse().unwrap_or(0.0);
            let spend = free_usdt * 0.99; // 留1%防价格波动导致NOTIONAL不足
            let klines = self
                .client
                .get_klines(new_symbol, "1d", None, None, Some(1))
                .await?;
            let price = klines
                .last()
                .map(|k| k.close)
                .ok_or_else(|| ServiceError::MarketData("无法获取最新价格".to_string()))?;
            let qty = round_step_size(spend / price, new_symbol);
            if qty * price < self.config.min_usdt_value {
                return Err(ServiceError::Order(format!(
                    "买入金额 {:.2} USDT 低于最小限制",
                    qty * price
                ))
                .into());
            }
            log::info!(
                "🟢 [轮动买入] {} 数量 {} (≈{:.2} USDT)",
                new_symbol,
                qty,
                qty * price
            );
            let resp = self
                .client
                .place_order(new_symbol, "BUY", "MARKET", qty, None, None)
                .await?;
            log::info!(
                "✅ 买入成交: {} | 成交均价 {} | 成交额 {}",
                new_symbol,
                resp.price,
                resp.cummulative_quote_qty
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_round_step_size_all_symbols() {
        // 与 Binance LOT_SIZE stepSize 对齐，超精度下单会被拒单
        assert!((round_step_size(1.23456789, "BTCUSDT") - 1.23456).abs() < 1e-9);
        assert!((round_step_size(12.34567, "ETHUSDT") - 12.3456).abs() < 1e-9);
        assert!((round_step_size(100.12345, "SOLUSDT") - 100.123).abs() < 1e-9);
        assert!((round_step_size(100.12345, "BNBUSDT") - 100.123).abs() < 1e-9);
        assert!((round_step_size(250.789, "LINKUSDT") - 250.78).abs() < 1e-9);
        assert!((round_step_size(666.666, "XRPUSDT") - 666.6).abs() < 1e-9);
        assert!((round_step_size(2500.7, "DOGEUSDT") - 2500.0).abs() < 1e-9);
    }

    #[test]
    fn test_trailing_stop_not_triggered_below_threshold() {
        // 峰值100，现价88.1（回撤11.9%）< 12%阈值，不触发
        let (triggered, new_peak) = evaluate_trailing_stop(100.0, 88.1, 0.12);
        assert!(!triggered, "回撤11.9%不应触发12%止损");
        assert!((new_peak - 100.0).abs() < 1e-9);
    }

    #[test]
    fn test_trailing_stop_triggered_at_threshold() {
        // 峰值100，现价88.0（回撤恰好12%），触发
        let (triggered, _) = evaluate_trailing_stop(100.0, 88.0, 0.12);
        assert!(triggered, "回撤恰好12%应触发止损");
    }

    #[test]
    fn test_trailing_stop_triggered_above_threshold() {
        // 峰值100，现价85（回撤15%）> 12%，触发
        let (triggered, _) = evaluate_trailing_stop(100.0, 85.0, 0.12);
        assert!(triggered, "回撤15%应触发止损");
    }

    #[test]
    fn test_peak_updates_on_new_high() {
        // 现价高于峰值时，峰值应更新，且不触发止损
        let (triggered, new_peak) = evaluate_trailing_stop(100.0, 110.0, 0.12);
        assert!(!triggered);
        assert!((new_peak - 110.0).abs() < 1e-9, "峰值应更新为新高110");
    }

    #[test]
    fn test_invalid_price_no_trigger() {
        // 价格为0或负数时不触发（防数据异常误止损）
        let (t1, _) = evaluate_trailing_stop(100.0, 0.0, 0.12);
        let (t2, _) = evaluate_trailing_stop(0.0, 100.0, 0.12);
        assert!(!t1);
        assert!(!t2);
    }

    #[test]
    fn test_state_backward_compat() {
        // 旧状态文件（无新字段）应能正常反序列化，新字段为默认值
        let old_json = r#"{"last_rebalance_ms":1000,"current_holding":"BTCUSDT"}"#;
        let state: RotationState = serde_json::from_str(old_json).unwrap();
        assert_eq!(state.current_holding.as_deref(), Some("BTCUSDT"));
        assert!(state.peak_price.is_none(), "旧文件peak_price应为None");
        assert!(state.entry_price.is_none(), "旧文件entry_price应为None");
        assert_eq!(state.last_stop_loss_ms, 0);
    }
}
