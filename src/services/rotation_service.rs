//! 日级动量轮动服务
//!
//! 策略逻辑（3年跨周期回测验证：年化+49.4%，17次调仓）：
//! - 品种池：BTC/ETH/SOL，每 rebalance_interval_days 天评估一次
//! - 信号：90日动量（close/close_90d前 - 1）最高者胜出
//! - 过滤：候选品种价格必须 > 其50日均线，否则空仓；最高动量 ≤ 0 也空仓
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
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RotationState {
    last_rebalance_ms: u64,          // 上次调仓时间
    current_holding: Option<String>, // 当前持仓品种（None=空仓持USDT）
}

impl Default for RotationState {
    fn default() -> Self {
        Self {
            last_rebalance_ms: 0,
            current_holding: None,
        }
    }
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
    let decimals: u32 = match symbol {
        "BTCUSDT" => 5,
        "ETHUSDT" => 4,
        "SOLUSDT" => 2,
        _ => 5,
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
            "🔄 动量轮动服务启动 | 品种: {:?} | 动量{}d | MA{}d | 调仓间隔{}d | 干跑: {}",
            self.config.symbols,
            self.config.momentum_lookback_days,
            self.config.ma_filter_days,
            self.config.rebalance_interval_days,
            self.config.dry_run
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

        // 未到调仓时间（首次运行且rebalance_on_start=true时elapsed必然>=interval，直接通过）
        let elapsed_ms = now.saturating_sub(state.last_rebalance_ms); // saturating_sub防时钟回拨下溢
        if elapsed_ms < interval_ms {
            let remain_h = (interval_ms - elapsed_ms) / 3600_000;
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
        }

        state.save(&self.state_file);
        Ok(())
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
                let is_better = best.as_ref().map_or(true, |(_, bm)| momentum > *bm);
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
