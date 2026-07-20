//! 动量短线策略
//!
//! 基于多数据流（K线、成交流、盘口）的高频动量短线策略
//! 核心逻辑：趋势方向 + RSI超卖/超买回归 + 成交量确认 + 盘口强度

use crate::config::StrategyConfig;
use crate::error::EventBusError;
use crate::event_bus::{EventBus, EventHandler, EventType, TokioEventBus};
use crate::events::{
    AggTradeEvent, BookTickerEvent, DomainEvent, KlineCompletedEvent, OrderFilledEvent,
    OrderRejectedEvent, TradingSignalEvent,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;

use super::indicators::{VolumeRatio, ADX, ATR, EMA, RSI};

/// 持仓状态
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
enum Position {
    /// 空仓
    None,
    /// 持有多头（已买入BTC）
    Long,
    /// 持有空头（已做空）
    Short,
}

/// 可持久化的策略状态
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistentState {
    position: Position,
    entry_price: f64,
    #[serde(default)]
    entry_quantity: f64,
    entry_time: u64,
    last_trade_time: u64,
    daily_trades: u32,
    daily_pnl: f64,
    #[serde(default)]
    daily_wins: u32,
    #[serde(default)]
    daily_losses: u32,
    #[serde(default)]
    daily_pnl_usdt: f64,
    last_day: u32,
    // 入场快照（重启后仍可记录入场动机）
    #[serde(default)]
    entry_score: u32,
    #[serde(default)]
    entry_path: String,
    #[serde(default)]
    entry_atr: f64, // 入场时ATR快照（防止ATR收缩导致止损过早触发）
    // 追踪止损状态（重启后需要恢复，否则追踪止损会失效）
    #[serde(default)]
    highest_since_entry: f64,
    #[serde(default)]
    trailing_active: bool,
    #[serde(default)]
    breakeven_active: bool,
    #[serde(default)]
    lowest_since_entry: f64,
}

impl PersistentState {
    fn save(&self, state_file: &str) {
        if let Some(parent) = std::path::Path::new(state_file).parent() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                log::error!("创建策略状态目录失败: {} | {}", parent.display(), e);
                return;
            }
        }
        match serde_json::to_string_pretty(self) {
            Ok(json) => {
                // 原子写入：使用唯一临时文件，避免多次异步保存并发覆盖同一个 .tmp 文件
                let temp_file = format!(
                    "{}.{}.{}.tmp",
                    state_file,
                    std::process::id(),
                    chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
                );
                if let Err(e) = std::fs::write(&temp_file, &json) {
                    log::error!("保存策略状态失败(写临时文件): {} | {}", temp_file, e);
                    return;
                }
                if let Err(e) = std::fs::rename(&temp_file, state_file) {
                    log::error!(
                        "保存策略状态失败(rename): {} -> {} | {}",
                        temp_file,
                        state_file,
                        e
                    );
                    let _ = std::fs::remove_file(&temp_file);
                }
            }
            Err(e) => log::error!("序列化策略状态失败: {}", e),
        }
    }

    fn load(state_file: &str) -> Option<Self> {
        match std::fs::read_to_string(state_file) {
            Ok(json) => match serde_json::from_str(&json) {
                Ok(state) => {
                    log::info!("✅ 恢复策略状态成功 ({})", state_file);
                    Some(state)
                }
                Err(e) => {
                    log::warn!("解析策略状态文件失败: {}，使用默认状态", e);
                    None
                }
            },
            Err(_) => None, // 文件不存在，正常情况
        }
    }
}

/// 策略内部状态
struct StrategyState {
    // 1分钟K线指标
    ema_fast_1m: EMA, // EMA(7)
    ema_slow_1m: EMA, // EMA(21)
    rsi_1m: RSI,      // RSI(14)

    // 5分钟K线指标
    ema_fast_5m: EMA,        // EMA(7)
    ema_slow_5m: EMA,        // EMA(21)
    ema_trend_5m: EMA,       // EMA(50) - 趋势环境过滤
    prev_ema_slow_5m: f64,   // 上一根5mK线的EMA21值（用于计算斜率）
    ema50_history: Vec<f64>, // EMA50历史值（最近20根=100分钟，宏观方向判断）

    // 成交量比率（60秒滑动窗口）
    volume_ratio: VolumeRatio,

    // 盘口数据
    best_bid: f64,
    best_bid_qty: f64,
    best_ask: f64,
    best_ask_qty: f64,
    last_data_time: u64, // 最后收到数据的时间戳

    // 持仓状态
    position: Position,
    entry_price: f64,
    entry_quantity: f64,
    entry_time: u64,

    // 追踪止损状态
    highest_since_entry: f64, // 入场后最高价
    trailing_active: bool,    // 追踪止损是否激活
    breakeven_active: bool,   // 保本止损是否激活
    lowest_since_entry: f64,  // 入场后最低价（做空用）

    // 突破入场状态
    recent_highs: Vec<f64>,
    recent_lows: Vec<f64>,

    // 冷却与统计
    last_trade_time: u64,
    daily_trades: u32,
    daily_pnl: f64,
    last_day: u32, // 用于日重置

    // 出场失败熔断保护
    exit_blocked_until: u64,        // 出场失败后的屏蔽截止时间(ms)
    last_exit_pnl: f64,             // 最后一次出场累加的pnl(用于回滚)
    consecutive_exit_failures: u32, // 连续出场失败次数

    // RSI状态追踪（检测回升）
    rsi_was_oversold: bool,   // RSI曾经低于超卖线
    rsi_oversold_bars: usize, // 超卖标志已持续的K线数（过期机制）

    // 连续止损熔断
    consecutive_stop_losses: u32, // 连续止损次数
    loss_cooldown_until: u64,     // 连续止损后的暂停截止时间(ms)

    // 预热计数
    kline_1m_count: usize,
    kline_5m_count: usize,

    // ATR/ADX 指标
    atr_5m: ATR, // 5分钟ATR(14)
    adx_5m: ADX, // 5分钟ADX(14)

    // 今日细化统计（含USDT盈亏与胜负拆分）
    daily_wins: u32,     // 今日盈利笔数
    daily_losses: u32,   // 今日亏损笔数
    daily_pnl_usdt: f64, // 今日累计USDT净盈亏

    // 当前仓位入场快照（出场日志显示入场动机，便于优化回溯）
    entry_score: u32,   // 入场评分快照
    entry_path: String, // 入场路径("突破"/"RSI反弹"/"均值回归"/"击穿"/"RSI超买")
    entry_atr: f64,     // 入场时ATR快照（用于止损计算时取max防止ATR收缩导致止损过早触发）
}

impl StrategyState {
    fn new(state_file: &str) -> Self {
        // 尝试从文件恢复持仓状态
        let persisted = PersistentState::load(state_file);

        let mut state = Self {
            ema_fast_1m: EMA::new(7),
            ema_slow_1m: EMA::new(21),
            rsi_1m: RSI::new(14),
            ema_fast_5m: EMA::new(7),
            ema_slow_5m: EMA::new(21),
            ema_trend_5m: EMA::new(50),
            prev_ema_slow_5m: 0.0,
            ema50_history: Vec::with_capacity(20),
            volume_ratio: VolumeRatio::new(60),
            best_bid: 0.0,
            best_bid_qty: 0.0,
            best_ask: 0.0,
            best_ask_qty: 0.0,
            last_data_time: 0,
            position: Position::None,
            entry_price: 0.0,
            entry_quantity: 0.0,
            entry_time: 0,
            highest_since_entry: 0.0,
            trailing_active: false,
            breakeven_active: false,
            lowest_since_entry: f64::MAX,
            recent_highs: Vec::with_capacity(20),
            recent_lows: Vec::with_capacity(20),
            last_trade_time: 0,
            daily_trades: 0,
            daily_pnl: 0.0,
            last_day: 0,
            exit_blocked_until: 0,
            last_exit_pnl: 0.0,
            consecutive_exit_failures: 0,
            rsi_was_oversold: false,
            rsi_oversold_bars: 0,
            consecutive_stop_losses: 0,
            loss_cooldown_until: 0,
            kline_1m_count: 0,
            kline_5m_count: 0,
            atr_5m: ATR::new(14, 100),
            adx_5m: ADX::new(14),
            daily_wins: 0,
            daily_losses: 0,
            daily_pnl_usdt: 0.0,
            entry_score: 0,
            entry_path: String::new(),
            entry_atr: 0.0,
        };

        // 恢复持久化状态（含合法性校验）
        if let Some(ps) = persisted {
            let has_position = ps.position == Position::Long || ps.position == Position::Short;
            if has_position && ps.entry_price <= 0.0 {
                log::error!(
                    "恢复状态异常: {:?}持仓中但entry_price={:.8}，丢弃该状态",
                    ps.position,
                    ps.entry_price
                );
            } else if has_position && ps.entry_time == 0 {
                log::error!(
                    "恢复状态异常: {:?}持仓中但entry_time=0，丢弃该状态",
                    ps.position
                );
            } else {
                log::info!(
                    "恢复持仓状态: {:?} | 入场价: {:.2} | 数量:{:.6} | 路径:{} | 评分:{} | 入场ATR:{:.1}",
                    ps.position,
                    ps.entry_price,
                    ps.entry_quantity,
                    ps.entry_path,
                    ps.entry_score,
                    ps.entry_atr
                );
                state.position = ps.position;
                state.entry_price = ps.entry_price;
                state.entry_quantity = ps.entry_quantity;
                state.entry_time = ps.entry_time;
                state.last_trade_time = ps.last_trade_time;
                state.daily_trades = ps.daily_trades;
                state.daily_pnl = ps.daily_pnl;
                state.daily_wins = ps.daily_wins;
                state.daily_losses = ps.daily_losses;
                state.daily_pnl_usdt = ps.daily_pnl_usdt;
                state.last_day = ps.last_day;
                state.entry_score = ps.entry_score;
                state.entry_path = ps.entry_path;
                state.entry_atr = ps.entry_atr;
                // 恢复追踪止损状态
                state.highest_since_entry = ps.highest_since_entry;
                state.lowest_since_entry = ps.lowest_since_entry;
                state.trailing_active = ps.trailing_active;
                state.breakeven_active = ps.breakeven_active;
                log::info!("  └─ 追踪止损状态: trailing_active={} breakeven_active={} 最高价:{:.2} 最低价:{:.2}",
                    ps.trailing_active, ps.breakeven_active, ps.highest_since_entry, ps.lowest_since_entry);
            }
        }

        state
    }

    /// 创建持久化快照（不执行IO，可在锁外保存）
    fn snapshot(&self) -> PersistentState {
        PersistentState {
            position: self.position.clone(),
            entry_price: self.entry_price,
            entry_quantity: self.entry_quantity,
            entry_time: self.entry_time,
            last_trade_time: self.last_trade_time,
            daily_trades: self.daily_trades,
            daily_pnl: self.daily_pnl,
            daily_wins: self.daily_wins,
            daily_losses: self.daily_losses,
            daily_pnl_usdt: self.daily_pnl_usdt,
            last_day: self.last_day,
            entry_score: self.entry_score,
            entry_path: self.entry_path.clone(),
            entry_atr: self.entry_atr,
            // 保存追踪止损状态
            highest_since_entry: self.highest_since_entry,
            trailing_active: self.trailing_active,
            breakeven_active: self.breakeven_active,
            lowest_since_entry: self.lowest_since_entry,
        }
    }

    /// 是否完成预热（需要足够的K线数据，EMA50需要50根5mK线）
    fn is_warmed_up(&self) -> bool {
        self.kline_1m_count >= 21 && self.kline_5m_count >= 50
    }

    /// 重置每日统计
    fn check_daily_reset(&mut self, timestamp_ms: u64) {
        let day = (timestamp_ms / 86400000) as u32;
        if day != self.last_day {
            // 输出昨日摘要
            if self.last_day > 0 {
                let closed_trades = self.daily_wins + self.daily_losses;
                let win_rate = if closed_trades > 0 {
                    self.daily_wins as f64 / closed_trades as f64 * 100.0
                } else {
                    0.0
                };
                log::info!(
                    "📅 日摘要 | {}笔入场/{}笔已平({}胜{}负 胜率{:.0}%) | 净盈亏:{:+.3}%({:+.2}U)",
                    self.daily_trades,
                    closed_trades,
                    self.daily_wins,
                    self.daily_losses,
                    win_rate,
                    self.daily_pnl,
                    self.daily_pnl_usdt
                );
            }
            self.last_day = day;
            self.daily_trades = 0;
            self.daily_pnl = 0.0;
            self.daily_wins = 0;
            self.daily_losses = 0;
            self.daily_pnl_usdt = 0.0;
            self.consecutive_stop_losses = 0;
            self.loss_cooldown_until = 0;
        }
    }
}

/// 动量短线策略
pub struct MomentumStrategy {
    config: StrategyConfig,
    event_bus: Arc<TokioEventBus>,
    state: Arc<Mutex<StrategyState>>,
    state_file: String,
}

impl MomentumStrategy {
    /// 创建新的动量策略
    pub fn new(config: StrategyConfig, event_bus: Arc<TokioEventBus>) -> Self {
        let state_file = format!("data/strategy_state_{}.json", config.symbol.to_lowercase());
        let direction = if config.allow_short {
            "多空"
        } else {
            "做多"
        };
        log::info!(
            "动量策略 v{} ({}) 初始化:",
            env!("CARGO_PKG_VERSION"),
            env!("GIT_HASH")
        );
        log::info!(
            "   交易对: {}({}) | 每笔: {}",
            config.symbol,
            direction,
            config.quantity_per_trade
        );
        log::info!(
            "   止盈: {}% | 止损: {}% | 冷却: {}秒",
            config.take_profit_pct,
            config.stop_loss_pct,
            config.cooldown_seconds
        );

        let mut state = StrategyState::new(&state_file);

        // 现货模式保护：如果状态文件恢复出 Position::Short 但当前禁止做空，丢弃异常状态
        if state.position == Position::Short && !config.allow_short {
            log::error!("⚠️ 状态文件异常: 恢复出空头持仓但当前为现货模式(allow_short=false)，丢弃该状态 | {} @ {:.2}",
                config.symbol, state.entry_price);
            state.position = Position::None;
            state.entry_price = 0.0;
            state.entry_time = 0;
        }

        Self {
            state: Arc::new(Mutex::new(state)),
            state_file,
            config,
            event_bus,
        }
    }

    /// 处理K线事件
    async fn on_kline(&self, event: &KlineCompletedEvent) {
        if event.symbol != self.config.symbol {
            return;
        }
        // 数据验证
        if event.close <= 0.0 {
            return;
        }

        let mut state = self.state.lock().await;

        // 数据超时检测：如果持仓中且超过30秒没收到BookTicker，紧急平仓
        // 注意: 使用当前系统时间与last_data_time比较，而不是用kline的close_time
        // 因为kline的close_time是K线周期的结束边界（5m线可能是未来时间）
        if (state.position == Position::Long || state.position == Position::Short)
            && state.last_data_time > 0
            && state.entry_price > 0.0
        {
            let now_ms = chrono::Utc::now().timestamp_millis() as u64;
            if now_ms > state.last_data_time && (now_ms - state.last_data_time) > 30000 {
                // 紧急平仓前先做日重置，防止跨天边界的日盈亏统计错乱
                state.check_daily_reset(event.close_time);
                let current_price = event.close;
                let pnl_pct = if state.position == Position::Long {
                    (current_price - state.entry_price) / state.entry_price * 100.0
                } else {
                    (state.entry_price - current_price) / state.entry_price * 100.0
                };
                let signal_type = if state.position == Position::Long {
                    "SELL"
                } else {
                    "COVER"
                };
                let net_pnl_pct = pnl_pct - self.config.round_trip_fee_pct;
                let exit_quantity = if state.entry_quantity > 0.0 {
                    state.entry_quantity
                } else {
                    self.config.quantity_per_trade
                };
                log::warn!(
                    "⚠️ 数据超时紧急平仓 | {} | {:?} | 入场: {:.2} | 当前: {:.2} | 净盈亏: {:.3}%",
                    self.config.symbol,
                    state.position,
                    state.entry_price,
                    current_price,
                    net_pnl_pct
                );

                state.position = Position::None;
                state.daily_pnl += net_pnl_pct;
                state.last_exit_pnl = net_pnl_pct;
                state.rsi_was_oversold = false;
                let snap = state.snapshot();
                let path = self.state_file.clone();
                drop(state);
                tokio::task::spawn_blocking(move || snap.save(&path));
                self.emit_signal(
                    signal_type,
                    current_price,
                    now_ms,
                    None,
                    None,
                    Some(exit_quantity),
                )
                .await;
                return;
            }
        }

        match event.interval.as_str() {
            "1m" => {
                // 只在K线收盘时更新指标
                if event.is_closed {
                    state.kline_1m_count += 1;
                    state.ema_fast_1m.update(event.close);
                    state.ema_slow_1m.update(event.close);
                    let rsi = state.rsi_1m.update(event.close);

                    // 追踪RSI状态
                    if rsi < self.config.rsi_oversold {
                        state.rsi_was_oversold = true;
                        state.rsi_oversold_bars = 0;
                    } else if state.rsi_was_oversold {
                        state.rsi_oversold_bars += 1;
                        // 超卖信号过期：30根1m K线（30分钟）内未入场则重置
                        if state.rsi_oversold_bars > 30 {
                            state.rsi_was_oversold = false;
                            state.rsi_oversold_bars = 0;
                        }
                    }

                    // 更新最近20根K线最高/最低价
                    state.recent_highs.push(event.high);
                    if state.recent_highs.len() > 20 {
                        state.recent_highs.remove(0);
                    }
                    state.recent_lows.push(event.low);
                    if state.recent_lows.len() > 20 {
                        state.recent_lows.remove(0);
                    }
                }
            }
            "5m" => {
                if event.is_closed {
                    state.kline_5m_count += 1;
                    // 记录上一根K线的EMA21值（用于斜率计算）
                    state.prev_ema_slow_5m = state.ema_slow_5m.value().unwrap_or(0.0);
                    state.ema_fast_5m.update(event.close);
                    state.ema_slow_5m.update(event.close);
                    state.ema_trend_5m.update(event.close);
                    // ATR/ADX更新（需要high/low/close）
                    state.atr_5m.update(event.high, event.low, event.close);
                    state.adx_5m.update(event.high, event.low, event.close);
                    // 记录EMA50历史值（宏观方向过滤）
                    let new_ema50 = state.ema_trend_5m.value().unwrap_or(0.0);
                    state.ema50_history.push(new_ema50);
                    if state.ema50_history.len() > 20 {
                        state.ema50_history.remove(0);
                    }
                }
            }
            _ => {}
        }
    }

    /// 处理聚合成交事件
    async fn on_agg_trade(&self, event: &AggTradeEvent) {
        if event.symbol != self.config.symbol {
            return;
        }
        // 数据验证
        if event.quantity <= 0.0 || event.price <= 0.0 {
            return;
        }

        let mut state = self.state.lock().await;
        state
            .volume_ratio
            .add_trade(event.timestamp, event.quantity, event.is_buyer_maker);
    }

    /// 处理盘口事件（主要触发点）
    async fn on_book_ticker(&self, event: &BookTickerEvent) {
        if event.symbol != self.config.symbol {
            return;
        }

        let mut state = self.state.lock().await;

        // 更新盘口
        state.best_bid = event.best_bid;
        state.best_bid_qty = event.best_bid_qty;
        state.best_ask = event.best_ask;
        state.best_ask_qty = event.best_ask_qty;
        state.last_data_time = event.timestamp;

        // 数据验证：价格异常则跳过
        if event.best_bid <= 0.0 || event.best_ask <= 0.0 || event.best_bid >= event.best_ask {
            return;
        }

        // 价差异常保护：点差超过0.5%视为异常行情，不交易
        let spread_pct = (event.best_ask - event.best_bid) / event.best_bid * 100.0;
        if spread_pct > 0.5 {
            log::warn!(
                "异常点差: {:.4}%，跳过本次信号 (bid={:.2}, ask={:.2})",
                spread_pct,
                event.best_bid,
                event.best_ask
            );
            return;
        }

        // 日重置检查
        state.check_daily_reset(event.timestamp);

        let now_ms = event.timestamp;

        // 出场检查不需要预热！确保已有持仓能及时止损/止盈
        // 检查出场条件（阶梯式保护机制）
        if state.position == Position::Long && state.entry_price > 0.0 {
            // 熔断保护：出场失败后60秒内不重试
            if now_ms < state.exit_blocked_until {
                drop(state);
                return;
            }
            let current_price = state.best_bid;
            let hold_secs = (now_ms - state.entry_time) / 1000;
            let rsi = state.rsi_1m.value().unwrap_or(50.0);

            state.highest_since_entry = state.highest_since_entry.max(current_price);
            let pnl_pct = (current_price - state.entry_price) / state.entry_price * 100.0;
            let highest_pnl_pct =
                (state.highest_since_entry - state.entry_price) / state.entry_price * 100.0;

            // ATR动态止损（使用max(当前ATR, 入场ATR)防止ATR收缩导致止损过早触发）
            let current_atr = state.atr_5m.value().unwrap_or(0.0);
            let effective_atr = current_atr.max(state.entry_atr); // 核心修复：不允许止损收紧
            let use_atr = self.config.use_atr_stops && effective_atr > 0.0;

            let dynamic_sl = if use_atr {
                // ATR基础止损价
                let atr_sl = state.entry_price - self.config.atr_stop_multiplier * effective_atr;
                // ATR追踪止损：浮盈超过 N*ATR 后开启
                let atr_trailing_trigger = self.config.atr_trailing_multiplier * effective_atr;
                let atr_trailing_dist = self.config.atr_trailing_distance * effective_atr;
                let profit_amount = state.highest_since_entry - state.entry_price;
                if profit_amount >= atr_trailing_trigger {
                    if !state.trailing_active {
                        log::info!(
                            "📌 ATR追踪激活 | {} | 最高: {:.2} | 追踪止损: {:.2} | ATR: {:.2}",
                            self.config.symbol,
                            state.highest_since_entry,
                            state.highest_since_entry - atr_trailing_dist,
                            current_atr
                        );
                    }
                    state.trailing_active = true;
                    state.highest_since_entry - atr_trailing_dist
                } else if highest_pnl_pct >= self.config.breakeven_trigger_pct {
                    if !state.breakeven_active {
                        log::info!(
                            "📌 保本激活 | {} | 止损上移至: {:.2} | 当前浮盈: {:.2}%",
                            self.config.symbol,
                            state.entry_price,
                            highest_pnl_pct
                        );
                    }
                    state.breakeven_active = true;
                    state.entry_price
                } else {
                    atr_sl
                }
            } else {
                // 回退到固定%止损
                if highest_pnl_pct >= self.config.trailing_trigger_pct {
                    if !state.trailing_active {
                        log::info!(
                            "📌 追踪激活 | {} | 最高: {:.2} | 追踪止损: {:.2} | 当前浮盈: {:.2}%",
                            self.config.symbol,
                            state.highest_since_entry,
                            state.highest_since_entry
                                * (1.0 - self.config.trailing_distance_pct / 100.0),
                            highest_pnl_pct
                        );
                    }
                    state.trailing_active = true;
                    state.highest_since_entry * (1.0 - self.config.trailing_distance_pct / 100.0)
                } else if highest_pnl_pct >= self.config.breakeven_trigger_pct {
                    if !state.breakeven_active {
                        let be_price =
                            state.entry_price * (1.0 + self.config.round_trip_fee_pct / 100.0);
                        log::info!(
                            "📌 保本激活 | {} | 止损上移至: {:.2}(含费) | 当前浮盈: {:.2}%",
                            self.config.symbol,
                            be_price,
                            highest_pnl_pct
                        );
                    }
                    state.breakeven_active = true;
                    state.entry_price * (1.0 + self.config.round_trip_fee_pct / 100.0)
                } else {
                    state.entry_price * (1.0 - self.config.stop_loss_pct / 100.0)
                }
            };

            let should_exit = current_price <= dynamic_sl
                || pnl_pct >= self.config.take_profit_pct
                || hold_secs >= self.config.max_hold_seconds
                || rsi > self.config.rsi_overbought
                || (hold_secs >= self.config.stale_exit_seconds
                    && pnl_pct < self.config.stale_pnl_threshold_pct
                    && !state.trailing_active);

            if should_exit {
                let reason = if pnl_pct >= self.config.take_profit_pct {
                    "硬止盈"
                } else if state.trailing_active && current_price <= dynamic_sl {
                    "追踪止损"
                } else if state.breakeven_active && current_price <= dynamic_sl {
                    "保本止损"
                } else if current_price <= dynamic_sl {
                    "止损"
                } else if hold_secs >= self.config.stale_exit_seconds
                    && pnl_pct < self.config.stale_pnl_threshold_pct
                    && !state.trailing_active
                {
                    "僵尸早退"
                } else if hold_secs >= self.config.max_hold_seconds {
                    "时间止损"
                } else {
                    "RSI超买"
                };

                let net_pnl_pct = pnl_pct - self.config.round_trip_fee_pct;
                let exit_quantity = if state.entry_quantity > 0.0 {
                    state.entry_quantity
                } else {
                    self.config.quantity_per_trade
                };
                let net_pnl_usdt = exit_quantity * state.entry_price * net_pnl_pct / 100.0;
                let hold_min = hold_secs / 60;
                log::info!("🔴 [平多] {} | {:.2}→{:.2} | 毛:{:+.3}% 净:{:+.3}%({:+.2}U) | 原因:{} | 持仓:{}min({}s) | 峰值:{:.2}%(动态SL:{:.2} ATR:{:.1}/入场:{:.1}) | [入场路径:{} 评分:{}] | 今日:{}笔{:+.3}%({:+.2}U)",
                    self.config.symbol, state.entry_price, current_price, pnl_pct,
                    net_pnl_pct, net_pnl_usdt, reason, hold_min, hold_secs,
                    highest_pnl_pct, dynamic_sl, current_atr, state.entry_atr,
                    state.entry_path, state.entry_score,
                    state.daily_trades, state.daily_pnl + net_pnl_pct,
                    state.daily_pnl_usdt + net_pnl_usdt);

                state.position = Position::None;
                state.daily_pnl += net_pnl_pct;
                state.last_exit_pnl = net_pnl_pct; // 记录本次出场累加的pnl，用于失败回滚
                if net_pnl_pct > 0.0 {
                    state.daily_wins += 1;
                } else {
                    state.daily_losses += 1;
                }
                state.daily_pnl_usdt += net_pnl_usdt;
                state.last_trade_time = now_ms;
                state.rsi_was_oversold = false;

                // 连续止损熔断计数：只有净亏损的止损/超时才计入，盈利的时间退出不算连续止损
                if (reason == "止损" || reason == "时间止损") && net_pnl_pct < 0.0 {
                    state.consecutive_stop_losses += 1;
                    let cooldown_ms = match state.consecutive_stop_losses {
                        1 => self.config.post_stoploss_cooldown_seconds * 1000, // 第1次止损: 使用配置的延长冷却
                        2 => 1_200_000,                                         // 2连亏: 冷却20分钟
                        3 => 7_200_000,                                         // 3连亏: 暂停2小时
                        n if n >= 4 => 14_400_000,                              // 4+连亏: 暂停4小时
                        _ => 0,
                    };
                    if cooldown_ms > 0 {
                        state.loss_cooldown_until = now_ms + cooldown_ms;
                        log::warn!(
                            "⚠️ 连续{}次止损 | {} | 暂停交易{}min",
                            state.consecutive_stop_losses,
                            self.config.symbol,
                            cooldown_ms / 60_000
                        );
                    }
                } else if net_pnl_pct > 0.0 {
                    // 盈利出场重置计数
                    state.consecutive_stop_losses = 0;
                    state.loss_cooldown_until = 0;
                }

                let snap = state.snapshot();
                let path = self.state_file.clone();
                drop(state);
                tokio::task::spawn_blocking(move || snap.save(&path));
                self.emit_signal(
                    "SELL",
                    current_price,
                    now_ms,
                    None,
                    None,
                    Some(exit_quantity),
                )
                .await;
                return;
            }
        }

        // 做空出场检查
        if state.position == Position::Short && state.entry_price > 0.0 {
            // 熔断保护：出场失败后60秒内不重试
            if now_ms < state.exit_blocked_until {
                drop(state);
                return;
            }
            let current_price = state.best_ask; // 平空用ask
            let hold_secs = (now_ms - state.entry_time) / 1000;

            state.lowest_since_entry = state.lowest_since_entry.min(current_price);
            let pnl_pct = (state.entry_price - current_price) / state.entry_price * 100.0;
            let lowest_pnl_pct =
                (state.entry_price - state.lowest_since_entry) / state.entry_price * 100.0;

            let dynamic_sl = {
                let current_atr = state.atr_5m.value().unwrap_or(0.0);
                let effective_atr = current_atr.max(state.entry_atr);
                let use_atr = self.config.use_atr_stops && effective_atr > 0.0;
                if use_atr {
                    // ATR基础止损价（做空：入场价 + N*ATR）
                    let atr_sl =
                        state.entry_price + self.config.atr_stop_multiplier * effective_atr;
                    let atr_trailing_trigger = self.config.atr_trailing_multiplier * effective_atr;
                    let atr_trailing_dist = self.config.atr_trailing_distance * effective_atr;
                    let profit_amount = state.entry_price - state.lowest_since_entry;
                    if profit_amount >= atr_trailing_trigger {
                        if !state.trailing_active {
                            log::info!("📌 空ATR追踪激活 | {} | 最低: {:.2} | 追踪止损: {:.2} | ATR: {:.2}",
                                self.config.symbol, state.lowest_since_entry,
                                state.lowest_since_entry + atr_trailing_dist, current_atr);
                        }
                        state.trailing_active = true;
                        state.lowest_since_entry + atr_trailing_dist
                    } else if lowest_pnl_pct >= self.config.breakeven_trigger_pct {
                        if !state.breakeven_active {
                            log::info!(
                                "📌 空保本激活 | {} | 止损下移至: {:.2} | 当前浮盈: {:.2}%",
                                self.config.symbol,
                                state.entry_price,
                                lowest_pnl_pct
                            );
                        }
                        state.breakeven_active = true;
                        state.entry_price
                    } else {
                        atr_sl
                    }
                } else {
                    // 回退到固定%止损
                    if lowest_pnl_pct >= self.config.trailing_trigger_pct {
                        if !state.trailing_active {
                            log::info!("📌 空追踪激活 | {} | 最低: {:.2} | 追踪止损: {:.2} | 当前浮盈: {:.2}%",
                                self.config.symbol, state.lowest_since_entry,
                                state.lowest_since_entry * (1.0 + self.config.trailing_distance_pct / 100.0),
                                lowest_pnl_pct);
                        }
                        state.trailing_active = true;
                        state.lowest_since_entry * (1.0 + self.config.trailing_distance_pct / 100.0)
                    } else if lowest_pnl_pct >= self.config.breakeven_trigger_pct {
                        if !state.breakeven_active {
                            let be_price =
                                state.entry_price * (1.0 - self.config.round_trip_fee_pct / 100.0);
                            log::info!(
                                "📌 空保本激活 | {} | 止损下移至: {:.2}(含费) | 当前浮盈: {:.2}%",
                                self.config.symbol,
                                be_price,
                                lowest_pnl_pct
                            );
                        }
                        state.breakeven_active = true;
                        state.entry_price * (1.0 - self.config.round_trip_fee_pct / 100.0)
                    } else {
                        state.entry_price * (1.0 + self.config.stop_loss_pct / 100.0)
                    }
                }
            };

            let should_exit = current_price >= dynamic_sl
                || pnl_pct >= self.config.take_profit_pct
                || hold_secs >= self.config.max_hold_seconds
                || (hold_secs >= self.config.stale_exit_seconds
                    && pnl_pct < self.config.stale_pnl_threshold_pct
                    && !state.trailing_active);

            if should_exit {
                let reason = if pnl_pct >= self.config.take_profit_pct {
                    "空止盈"
                } else if state.trailing_active && current_price >= dynamic_sl {
                    "空追踪止损"
                } else if state.breakeven_active && current_price >= dynamic_sl {
                    "空保本止损"
                } else if current_price >= dynamic_sl {
                    "空止损"
                } else if hold_secs >= self.config.stale_exit_seconds
                    && pnl_pct < self.config.stale_pnl_threshold_pct
                    && !state.trailing_active
                {
                    "空僵尸早退"
                } else {
                    "空超时"
                };

                let net_pnl_pct = pnl_pct - self.config.round_trip_fee_pct;
                let short_atr = state.atr_5m.value().unwrap_or(0.0);
                let exit_quantity = if state.entry_quantity > 0.0 {
                    state.entry_quantity
                } else {
                    self.config.quantity_per_trade
                };
                let net_pnl_usdt = exit_quantity * state.entry_price * net_pnl_pct / 100.0;
                let hold_min = hold_secs / 60;
                log::info!("🔴 [平空] {} | {:.2}→{:.2} | 毛:{:+.3}% 净:{:+.3}%({:+.2}U) | 原因:{} | 持仓:{}min({}s) | 峰值:{:.2}%(动态SL:{:.2} ATR:{:.1}/入场:{:.1}) | [入场路径:{} 评分:{}] | 今日:{}笔{:+.3}%({:+.2}U)",
                    self.config.symbol, state.entry_price, current_price, pnl_pct,
                    net_pnl_pct, net_pnl_usdt, reason, hold_min, hold_secs,
                    lowest_pnl_pct, dynamic_sl, short_atr, state.entry_atr,
                    state.entry_path, state.entry_score,
                    state.daily_trades, state.daily_pnl + net_pnl_pct,
                    state.daily_pnl_usdt + net_pnl_usdt);

                state.position = Position::None;
                state.daily_pnl += net_pnl_pct;
                state.last_exit_pnl = net_pnl_pct; // 记录本次出场累加的pnl，用于失败回滚
                if net_pnl_pct > 0.0 {
                    state.daily_wins += 1;
                } else {
                    state.daily_losses += 1;
                }
                state.daily_pnl_usdt += net_pnl_usdt;
                state.last_trade_time = now_ms;

                // 连续止损熔断计数：只有净亏损的止损/超时才计入，盈利退出不算连续止损
                if (reason == "空止损" || reason == "空超时") && net_pnl_pct < 0.0 {
                    state.consecutive_stop_losses += 1;
                    let cooldown_ms = match state.consecutive_stop_losses {
                        1 => self.config.post_stoploss_cooldown_seconds * 1000, // 第1次止损: 使用配置的延长冷却
                        2 => 1_200_000,                                         // 2连亏: 冷却20分钟
                        3 => 7_200_000,                                         // 3连亏: 暂停2小时
                        n if n >= 4 => 14_400_000,                              // 4+连亏: 暂停4小时
                        _ => 0,
                    };
                    if cooldown_ms > 0 {
                        state.loss_cooldown_until = now_ms + cooldown_ms;
                        log::warn!(
                            "⚠️ 连续{}次止损 | {} | 暂停交易{}min",
                            state.consecutive_stop_losses,
                            self.config.symbol,
                            cooldown_ms / 60_000
                        );
                    }
                } else if net_pnl_pct > 0.0 {
                    // 盈利出场重置计数
                    state.consecutive_stop_losses = 0;
                    state.loss_cooldown_until = 0;
                }

                let snap = state.snapshot();
                let path = self.state_file.clone();
                drop(state);
                tokio::task::spawn_blocking(move || snap.save(&path));
                self.emit_signal(
                    "COVER",
                    current_price,
                    now_ms,
                    None,
                    None,
                    Some(exit_quantity),
                )
                .await;
                return;
            }
        }

        // 检查入场条件（空仓时）
        if state.position == Position::None {
            // 预热未完成不入场（仅阻止入场，不阻止出场）
            if !state.is_warmed_up() {
                return;
            }

            // 冷却检查
            if now_ms - state.last_trade_time < self.config.cooldown_seconds * 1000 {
                return;
            }

            // 连续止损熔断检查
            if now_ms < state.loss_cooldown_until {
                return;
            }

            // 日最大交易次数
            if state.daily_trades >= self.config.max_daily_trades {
                return;
            }

            // 日最大亏损
            if state.daily_pnl <= -self.config.max_daily_loss_pct {
                return;
            }

            // === 趋势环境识别（L2层） ===
            let ema_fast_5m = state.ema_fast_5m.value().unwrap_or(0.0);
            let ema_slow_5m = state.ema_slow_5m.value().unwrap_or(0.0);
            let ema_trend = state.ema_trend_5m.value().unwrap_or(0.0);
            let rsi = state.rsi_1m.value().unwrap_or(50.0);
            let vol_ratio = state.volume_ratio.ratio();
            let current_price = state.best_ask;

            // === ADX过滤 ===
            let adx_val = state.adx_5m.value().unwrap_or(0.0);
            let adx_ready = state.adx_5m.is_ready();
            let adx_above_threshold = !adx_ready || adx_val >= self.config.adx_min_threshold;

            // === 波动率政权过滤 ===
            let atr_pct = state.atr_5m.percentile();
            let volatility_normal = match atr_pct {
                Some(p) => {
                    p >= self.config.atr_percentile_low && p <= self.config.atr_percentile_high
                }
                None => true, // 数据不足时不过滤
            };

            // EMA21斜率：判断中期趋势动能方向
            let ema21_slope = if state.prev_ema_slow_5m > 0.0 {
                (ema_slow_5m - state.prev_ema_slow_5m) / state.prev_ema_slow_5m * 100.0
            } else {
                0.0
            };

            // === 做多入场条件 ===
            // EMA50宏观方向过滤：确认中期趋势上升
            let ema50_macro_rising = if state.ema50_history.len() >= 20 {
                ema_trend > state.ema50_history[0] // 对比20根bar前（100分钟）
            } else {
                false
            };

            // 趋势环境确认：价格在EMA50之上至少0.15% + EMA21斜率非下降 + EMA50宏观上升
            let price_above_ema50_pct = if ema_trend > 0.0 {
                (current_price - ema_trend) / ema_trend * 100.0
            } else {
                0.0
            };
            let long_trend_env_ok = ema_trend > 0.0
                && price_above_ema50_pct > 0.15  // 至少高于EMA50 0.15%，避免边缘试探
                && price_above_ema50_pct < self.config.max_ema50_distance_pct  // 趋势过度延伸过滤
                && ema21_slope > self.config.mean_revert_min_slope  // 使用配置化斜率门槛，便于回测同步优化
                && ema50_macro_rising; // EMA50宏观方向必须上升

            // 条件1: 5分钟趋势向上 + 趋势强度过滤
            let trend_up = ema_fast_5m > ema_slow_5m;
            let trend_strength = if ema_slow_5m > 0.0 {
                (ema_fast_5m - ema_slow_5m) / ema_slow_5m * 100.0
            } else {
                0.0
            };
            let trend_strong_enough = trend_strength >= self.config.min_trend_strength_pct;

            // 条件2: RSI从超卖回升（加天花板：RSI超过overbought时不入场）
            let rsi_recovering = state.rsi_was_oversold
                && rsi > (self.config.rsi_oversold + 5.0)
                && rsi < self.config.rsi_overbought;

            // 条件3: RSI反弹路径需要更高VR门槛（回测验证VR≥2.5过滤弱反弹）
            let rsi_bounce_vr_ok = vol_ratio > 2.5;

            // 条件4: 盘口有买盘支撑
            let bid_support = state.best_bid_qty > state.best_ask_qty * 1.5;

            // 突破入场条件：价格突破最近N根K线最高价
            let breakout_signal = if state.recent_highs.len() >= 10 {
                let lookback_high = state.recent_highs[..state.recent_highs.len() - 1]
                    .iter()
                    .copied()
                    .fold(f64::NEG_INFINITY, f64::max);
                let breakout_pct = (state.best_ask - lookback_high) / lookback_high * 100.0;
                // RSI < 65: 防止在RSI高位追高（RSI>=65时的突破信号可靠性差）
                breakout_pct > 0.05 && vol_ratio > self.config.volume_ratio_threshold && rsi < 65.0
            } else {
                false
            };

            // RSI反弹入场 或 突破入场（必须通过趋势环境确认）
            // 突破路径额外要求斜率>配置值（更强的趋势确认）
            let breakout_slope_ok = ema21_slope > self.config.breakout_min_slope;
            let slope_positive = ema21_slope > 0.03;
            let rsi_bounce_not_late = rsi < 55.0;

            // === 多因子评分系统 ===
            let mut entry_score: u32 = 0;
            if trend_up {
                entry_score += 15;
            }
            if trend_strong_enough {
                entry_score += 10;
            }
            if adx_above_threshold {
                entry_score += 15;
            }
            if rsi_recovering {
                entry_score += 15;
            }
            if vol_ratio > self.config.volume_ratio_threshold {
                entry_score += 15;
            }
            if bid_support {
                entry_score += 10;
            }
            if slope_positive {
                entry_score += 10;
            }
            if volatility_normal {
                entry_score += 10;
            }

            // 趋势入场：评分达标 + 趋势环境确认
            // RSI反弹路径额外要求RSI<55，避免实盘中RSI接近60时追入反弹末端
            let trend_entry_signal = long_trend_env_ok
                && entry_score >= self.config.entry_score_threshold
                && ((trend_up
                    && trend_strong_enough
                    && rsi_recovering
                    && rsi_bounce_vr_ok
                    && bid_support
                    && rsi_bounce_not_late)
                    || (breakout_signal && trend_up && trend_strong_enough && breakout_slope_ok));

            // === 均值回归入场信号（超跌反弹，不需要趋势确认） ===
            // 加速下跌保护：斜率 > 配置值 时才允许均值回归
            let mean_revert_signal = if state.recent_highs.len() >= 20 {
                let recent_high = state
                    .recent_highs
                    .iter()
                    .copied()
                    .fold(f64::NEG_INFINITY, f64::max);
                let drop_pct = (recent_high - current_price) / recent_high * 100.0;
                // 从20bar高点跌>1.2% + RSI<38 + VR>2.5(强买盘) + 盘口支撑 + 上升趋势保护 + 波动率过滤
                drop_pct > 1.2
                    && rsi < 38.0
                    && vol_ratio > 2.5
                    && bid_support
                    && ema21_slope > self.config.mean_revert_min_slope
                    && ema_trend > 0.0
                    && current_price >= ema_trend
                    && ema50_macro_rising
                    && volatility_normal
            } else {
                false
            };

            let entry_signal = trend_entry_signal || mean_revert_signal;

            if entry_signal {
                let entry_price = state.best_ask;
                // 优先判断RSI反弹路径：两种条件同时满足时RSI反弹更具体
                let entry_path = if mean_revert_signal {
                    "均值回归"
                } else if trend_up && rsi_recovering && rsi_bounce_vr_ok && bid_support {
                    "RSI反弹"
                } else {
                    "突破"
                };
                let current_atr = state.atr_5m.value().unwrap_or(0.0);
                let sl_price = if self.config.use_atr_stops && current_atr > 0.0 {
                    entry_price - self.config.atr_stop_multiplier * current_atr
                } else {
                    entry_price * (1.0 - self.config.stop_loss_pct / 100.0)
                };
                let tp_price = entry_price * (1.0 + self.config.take_profit_pct / 100.0);

                let sl_dist_pct = (entry_price - sl_price) / entry_price * 100.0;
                let tp_dist_pct = (tp_price - entry_price) / entry_price * 100.0;
                log::info!("🟢 [开多] {} @ {:.2} | 路径:{} | 评分:{}/100 | RSI:{:.1} | 量比:{:.2} | ADX:{:.1} | ATR:{:.1} | 斜率:{:+.3}% | 距EMA50:{:+.3}% | SL:{:.2}(-{:.2}%) | TP:{:.2}(+{:.2}%) | 今日第{}笔",
                    self.config.symbol, entry_price, entry_path, entry_score, rsi, vol_ratio, adx_val, current_atr,
                    ema21_slope, price_above_ema50_pct, sl_price, sl_dist_pct, tp_price, tp_dist_pct,
                    state.daily_trades + 1);
                log::info!("📊 [入场因子] trend↑:{} 强度:{:.3}% ADX:{:.1}≥{:.0} rsi_recover:{} RSI未过热:{} 量比:{:.2}≥{:.1} 买盘支撑:{} 斜率正:{} 波动率正常:{} | ema50宏观上升:{} 趋势环境:{}",
                    trend_up, trend_strength, adx_val, self.config.adx_min_threshold,
                    rsi_recovering, rsi_bounce_not_late, vol_ratio, self.config.volume_ratio_threshold,
                    bid_support, slope_positive, volatility_normal,
                    ema50_macro_rising, long_trend_env_ok);
                state.entry_score = entry_score;
                state.entry_path = entry_path.to_string();
                state.entry_atr = current_atr; // 记录入场ATR快照，出场时取max防止收缩

                state.position = Position::Long;
                state.entry_price = entry_price;
                state.entry_quantity = 0.0;
                state.entry_time = now_ms;
                state.highest_since_entry = entry_price;
                state.trailing_active = false;
                state.breakeven_active = false;
                state.rsi_was_oversold = false;
                state.rsi_oversold_bars = 0;
                state.last_trade_time = now_ms;
                state.daily_trades += 1;
                state.consecutive_exit_failures = 0; // 新仓位清零失败计数
                let snap = state.snapshot();
                let path = self.state_file.clone();
                drop(state);
                tokio::task::spawn_blocking(move || snap.save(&path));
                self.emit_signal(
                    "BUY",
                    entry_price,
                    now_ms,
                    Some(sl_price),
                    Some(tp_price),
                    None,
                )
                .await;
            } else if self.config.allow_short {
                // === 做空入场条件 ===
                // 趋势环境确认：价格在EMA50之下至少0.15% + EMA21斜率非上升
                let price_below_ema50_pct = if ema_trend > 0.0 {
                    (ema_trend - state.best_bid) / ema_trend * 100.0
                } else {
                    0.0
                };
                let short_trend_env_ok = ema_trend > 0.0
                    && price_below_ema50_pct > 0.15  // 至少低于EMA50 0.15%
                    && ema21_slope < -0.03; // 斜率<-0.03% 确认下降动能（回测验证）

                let trend_down = ema_fast_5m < ema_slow_5m;
                let short_trend_strength = if ema_slow_5m > 0.0 {
                    (ema_slow_5m - ema_fast_5m) / ema_slow_5m * 100.0
                } else {
                    0.0
                };
                let short_trend_strong = short_trend_strength >= self.config.min_trend_strength_pct;
                let breakdown_signal = if state.recent_lows.len() >= 10 {
                    let lookback_low = state.recent_lows[..state.recent_lows.len() - 1]
                        .iter()
                        .copied()
                        .fold(f64::INFINITY, f64::min);
                    let breakdown_pct = (lookback_low - state.best_bid) / lookback_low * 100.0;
                    // RSI > 40: 防止RSI已处于极度超卖时做空击穿（下跌动能已耗尽）
                    breakdown_pct > 0.05
                        && vol_ratio < (1.0 / self.config.volume_ratio_threshold)
                        && rsi > 40.0
                } else {
                    false
                };

                let rsi_overbought_short = rsi > 70.0;
                let sell_pressure = vol_ratio < (1.0 / self.config.volume_ratio_threshold);

                let short_signal = short_trend_env_ok
                    && adx_above_threshold
                    && ((breakdown_signal && trend_down && short_trend_strong)
                        || (trend_down
                            && short_trend_strong
                            && rsi_overbought_short
                            && sell_pressure));

                if short_signal {
                    let entry_price = state.best_bid; // 做空用bid
                    let short_path = if breakdown_signal && trend_down {
                        "击穿"
                    } else {
                        "RSI超买"
                    };
                    let current_atr = state.atr_5m.value().unwrap_or(0.0);
                    let sl_price = if self.config.use_atr_stops && current_atr > 0.0 {
                        entry_price + self.config.atr_stop_multiplier * current_atr
                    } else {
                        entry_price * (1.0 + self.config.stop_loss_pct / 100.0)
                    };
                    let tp_price = entry_price * (1.0 - self.config.take_profit_pct / 100.0);

                    let sl_dist_pct = (sl_price - entry_price) / entry_price * 100.0;
                    let tp_dist_pct = (entry_price - tp_price) / entry_price * 100.0;
                    log::info!("🟡 [开空] {} @ {:.2} | 路径:{} | RSI:{:.1} | 量比:{:.2} | ADX:{:.1} | ATR:{:.1} | 斜率:{:+.3}% | 距EMA50:-{:.3}% | SL:{:.2}(+{:.2}%) | TP:{:.2}(-{:.2}%) | 今日第{}笔",
                        self.config.symbol, entry_price, short_path, rsi, vol_ratio, adx_val, current_atr,
                        ema21_slope, price_below_ema50_pct, sl_price, sl_dist_pct, tp_price, tp_dist_pct,
                        state.daily_trades + 1);
                    state.entry_score = 0;
                    state.entry_path = short_path.to_string();
                    state.entry_atr = current_atr; // 记录入场ATR快照

                    state.position = Position::Short;
                    state.entry_price = entry_price;
                    state.entry_quantity = self.config.quantity_per_trade;
                    state.entry_time = now_ms;
                    state.lowest_since_entry = entry_price;
                    state.trailing_active = false;
                    state.breakeven_active = false;
                    state.rsi_was_oversold = false;
                    state.rsi_oversold_bars = 0;
                    state.last_trade_time = now_ms;
                    state.daily_trades += 1;
                    state.consecutive_exit_failures = 0; // 新仓位清零失败计数
                    let snap = state.snapshot();
                    let path = self.state_file.clone();
                    drop(state);
                    tokio::task::spawn_blocking(move || snap.save(&path));
                    self.emit_signal(
                        "SHORT",
                        entry_price,
                        now_ms,
                        Some(sl_price),
                        Some(tp_price),
                        Some(self.config.quantity_per_trade),
                    )
                    .await;
                }
            }
        }
    }

    /// 发布交易信号事件
    async fn emit_signal(
        &self,
        signal_type: &str,
        price: f64,
        timestamp: u64,
        stop_loss: Option<f64>,
        take_profit: Option<f64>,
        suggested_quantity: Option<f64>,
    ) {
        let signal = TradingSignalEvent {
            signal_id: format!("momentum_{}_{}", signal_type.to_lowercase(), timestamp),
            strategy_id: "momentum_scalp".to_string(),
            symbol: self.config.symbol.clone(),
            signal_type: signal_type.to_string(),
            strength: 1.0,
            suggested_price: price,
            suggested_quantity,
            stop_loss_price: stop_loss,
            take_profit_price: take_profit,
            timestamp,
        };

        if let Err(e) = self
            .event_bus
            .publish(DomainEvent::TradingSignal(signal))
            .await
        {
            log::error!("发布交易信号失败: {}", e);
        }
    }

    async fn on_order_filled(&self, event: &OrderFilledEvent) {
        if event.symbol != self.config.symbol {
            return;
        }

        let mut state = self.state.lock().await;
        let base_asset = event
            .symbol
            .strip_suffix("USDT")
            .unwrap_or(event.symbol.as_str());
        let net_fill_qty = if event.side == "BUY" && event.commission_asset == base_asset {
            (event.fill_qty - event.commission).max(0.0)
        } else {
            event.fill_qty
        };

        match event.side.as_str() {
            "BUY" => {
                if state.position == Position::Long {
                    state.entry_price = event.fill_price;
                    state.entry_quantity = net_fill_qty;
                    log::info!(
                        "📌 实际成交入场已记录 | {} | 均价:{:.2} | 数量:{:.6} | 成交额:{:.2}U | 信号:{}",
                        event.symbol,
                        event.fill_price,
                        net_fill_qty,
                        event.actual_quote_qty,
                        event.signal_id
                    );
                }
            }
            "SELL" => {
                state.entry_quantity = 0.0;
                if state.position == Position::None {
                    state.entry_price = 0.0;
                    state.entry_atr = 0.0;
                }
                log::info!(
                    "📌 实际平仓成交已确认 | {} | 均价:{:.2} | 数量:{:.6} | 成交额:{:.2}U | 信号:{}",
                    event.symbol,
                    event.fill_price,
                    event.fill_qty,
                    event.actual_quote_qty,
                    event.signal_id
                );
            }
            _ => {}
        }

        let snap = state.snapshot();
        let path = self.state_file.clone();
        drop(state);
        tokio::task::spawn_blocking(move || snap.save(&path));
    }

    /// 处理订单拒绝事件（入场/平仓失败时回滚策略状态）
    async fn on_order_rejected(&self, event: &OrderRejectedEvent) {
        // 只处理本品种的拒绝事件
        if event.symbol != self.config.symbol {
            return;
        }

        let order_id = event.order_id.as_deref().unwrap_or("");
        let is_exit_failure = order_id.contains("_sell_") || order_id.contains("_cover_");
        let is_entry_failure = order_id.contains("_buy_") || order_id.contains("_short_");

        if is_entry_failure {
            // === 入场失败回滚：策略已设置了position但实际未成交，需要回滚为空仓 ===
            let mut state = self.state.lock().await;
            if state.position != Position::None {
                log::warn!(
                    "⚠️ 入场失败回滚 | {} | {:?} -> None | 入场价: {:.2} | 原因: {}",
                    self.config.symbol,
                    state.position,
                    state.entry_price,
                    event.reason
                );
                state.position = Position::None;
                state.entry_price = 0.0;
                state.entry_quantity = 0.0;
                state.entry_time = 0;
                state.highest_since_entry = 0.0;
                state.lowest_since_entry = f64::MAX;
                state.trailing_active = false;
                state.breakeven_active = false;
                state.entry_score = 0;
                state.entry_path.clear();
                state.entry_atr = 0.0;
                state.entry_quantity = 0.0;
                // 回滚 daily_trades（入场时+1了，现在要-1）
                if state.daily_trades > 0 {
                    state.daily_trades -= 1;
                }
                if event.reason.contains("资金不足") {
                    state.loss_cooldown_until =
                        event.timestamp + self.config.post_stoploss_cooldown_seconds * 1000;
                    log::warn!(
                        "⚠️ 资金不足暂停入场 | {} | 暂停{}min，避免重复拒单",
                        self.config.symbol,
                        self.config.post_stoploss_cooldown_seconds / 60
                    );
                }
                let snap = state.snapshot();
                let path = self.state_file.clone();
                drop(state);
                tokio::task::spawn_blocking(move || snap.save(&path));
            }
            return;
        }

        if !is_exit_failure {
            return;
        }

        // === 平仓失败回滚：策略已设置position=None但实际未平仓，需要恢复持仓 ===
        let mut state = self.state.lock().await;
        if state.position == Position::None && state.entry_price > 0.0 {
            // 根据order_id推断原始持仓方向
            let original_position = if order_id.contains("_sell_") {
                Position::Long
            } else {
                Position::Short
            };

            // 回滚 daily_pnl（之前在出场时已累加，现在要减回去）
            if state.last_exit_pnl != 0.0 {
                state.daily_pnl -= state.last_exit_pnl;
                log::info!(
                    "  └─ 回滚 daily_pnl: 减去 {:+.3}% | 修正后: {:+.3}%",
                    state.last_exit_pnl,
                    state.daily_pnl
                );
                state.last_exit_pnl = 0.0;
            }

            // 累加连续失败计数
            state.consecutive_exit_failures += 1;

            // 超过3次连续失败：强制放弃持仓，避免死循环
            if state.consecutive_exit_failures >= 3 {
                log::error!(
                    "❗ 连续{}次平仓失败 | {} | 强制清除持仓状态 | 入场价: {:.2} | 原因: {}",
                    state.consecutive_exit_failures,
                    self.config.symbol,
                    state.entry_price,
                    event.reason
                );
                state.position = Position::None;
                state.entry_price = 0.0;
                state.consecutive_exit_failures = 0;
                state.exit_blocked_until = 0;
                let snap = state.snapshot();
                let path = self.state_file.clone();
                drop(state);
                tokio::task::spawn_blocking(move || snap.save(&path));
                return;
            }

            // 设置熔断保护：60秒内不再尝试出场
            state.exit_blocked_until = event.timestamp + 60_000;

            log::warn!(
                "⚠️ 平仓失败回滚 | {} | 恢复{:?}持仓 | 入场价: {:.2} | 60s熔断({}/3) | 原因: {}",
                self.config.symbol,
                original_position,
                state.entry_price,
                state.consecutive_exit_failures,
                event.reason
            );
            // 恢复追踪止损状态（使用当前价格作为基准，因为回滚时无法确定回滚前的精确峰值）
            let current_price = state.best_bid.max(0.0001); // 使用当前盘口价
            if original_position == Position::Long {
                state.highest_since_entry = state.highest_since_entry.max(current_price);
            } else {
                state.lowest_since_entry = if state.lowest_since_entry == 0.0 {
                    current_price
                } else {
                    state.lowest_since_entry.min(current_price)
                };
            }
            // 保持原有的trailing_active和breakeven_active状态不变
            state.position = original_position;
            // 保存回滚后的状态
            let snap = state.snapshot();
            let path = self.state_file.clone();
            drop(state);
            tokio::task::spawn_blocking(move || snap.save(&path));
        }
    }
}

#[async_trait]
impl EventHandler for MomentumStrategy {
    async fn handle(&self, event: &DomainEvent) -> Result<(), EventBusError> {
        match event {
            DomainEvent::KlineCompleted(e) => self.on_kline(e).await,
            DomainEvent::AggTrade(e) => self.on_agg_trade(e).await,
            DomainEvent::BookTicker(e) => self.on_book_ticker(e).await,
            DomainEvent::OrderFilled(e) => self.on_order_filled(e).await,
            DomainEvent::OrderRejected(e) => self.on_order_rejected(e).await,
            _ => {}
        }
        Ok(())
    }

    fn event_types(&self) -> Vec<EventType> {
        vec![
            EventType::KlineCompleted,
            EventType::AggTrade,
            EventType::BookTicker,
            EventType::OrderFilled,
            EventType::OrderRejected,
        ]
    }
}
