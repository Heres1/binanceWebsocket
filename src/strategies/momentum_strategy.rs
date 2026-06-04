//! 动量短线策略
//!
//! 基于多数据流（K线、成交流、盘口）的高频动量短线策略
//! 核心逻辑：趋势方向 + RSI超卖/超买回归 + 成交量确认 + 盘口强度

use crate::config::StrategyConfig;
use crate::error::EventBusError;
use crate::event_bus::{EventBus, EventHandler, EventType, TokioEventBus};
use crate::events::{
    AggTradeEvent, BookTickerEvent, DomainEvent, KlineCompletedEvent, TradingSignalEvent,
    OrderRejectedEvent,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;

use super::indicators::{EMA, RSI, VolumeRatio};



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
    entry_time: u64,
    last_trade_time: u64,
    daily_trades: u32,
    daily_pnl: f64,
    last_day: u32,
}

impl PersistentState {
    fn save(&self, state_file: &str) {
        if let Some(parent) = std::path::Path::new(state_file).parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        match serde_json::to_string_pretty(self) {
            Ok(json) => {
                // 原子写入：先写临时文件，再rename，防止并发写入损坏
                let temp_file = format!("{}.tmp", state_file);
                if let Err(e) = std::fs::write(&temp_file, &json) {
                    log::error!("保存策略状态失败(写临时文件): {}", e);
                    return;
                }
                if let Err(e) = std::fs::rename(&temp_file, state_file) {
                    log::error!("保存策略状态失败(rename): {}", e);
                    let _ = std::fs::remove_file(&temp_file);
                }
            }
            Err(e) => log::error!("序列化策略状态失败: {}", e),
        }
    }
    
    fn load(state_file: &str) -> Option<Self> {
        match std::fs::read_to_string(state_file) {
            Ok(json) => {
                match serde_json::from_str(&json) {
                    Ok(state) => {
                        log::info!("✅ 恢复策略状态成功 ({})", state_file);
                        Some(state)
                    }
                    Err(e) => {
                        log::warn!("解析策略状态文件失败: {}，使用默认状态", e);
                        None
                    }
                }
            }
            Err(_) => None, // 文件不存在，正常情况
        }
    }
}

/// 策略内部状态
struct StrategyState {
    // 1分钟K线指标
    ema_fast_1m: EMA,   // EMA(7)
    ema_slow_1m: EMA,   // EMA(21)
    rsi_1m: RSI,        // RSI(14)

    // 5分钟K线指标
    ema_fast_5m: EMA,   // EMA(7)
    ema_slow_5m: EMA,   // EMA(21)
    ema_trend_5m: EMA,  // EMA(50) - 趋势环境过滤
    prev_ema_slow_5m: f64, // 上一根5mK线的EMA21值（用于计算斜率）
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
    entry_time: u64,

    // 追踪止损状态
    highest_since_entry: f64,   // 入场后最高价
    trailing_active: bool,      // 追踪止损是否激活
    breakeven_active: bool,     // 保本止损是否激活
    lowest_since_entry: f64,    // 入场后最低价（做空用）

    // 突破入场状态
    recent_highs: Vec<f64>,
    recent_lows: Vec<f64>,

    // 冷却与统计
    last_trade_time: u64,
    daily_trades: u32,
    daily_pnl: f64,
    last_day: u32, // 用于日重置

    // 出场失败熔断保护
    exit_blocked_until: u64,    // 出场失败后的屏蔽截止时间(ms)
    last_exit_pnl: f64,         // 最后一次出场累加的pnl(用于回滚)
    consecutive_exit_failures: u32, // 连续出场失败次数

    // RSI状态追踪（检测回升）
    rsi_was_oversold: bool, // RSI曾经低于超卖线
    rsi_oversold_bars: usize, // 超卖标志已持续的K线数（过期机制）

    // 连续止损熔断
    consecutive_stop_losses: u32,  // 连续止损次数
    loss_cooldown_until: u64,      // 连续止损后的暂停截止时间(ms)

    // 预热计数
    kline_1m_count: usize,
    kline_5m_count: usize,
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
        };
        
        // 恢复持久化状态（含合法性校验）
        if let Some(ps) = persisted {
            let has_position = ps.position == Position::Long || ps.position == Position::Short;
            if has_position && ps.entry_price <= 0.0 {
                log::error!("恢复状态异常: {:?}持仓中但entry_price={:.8}，丢弃该状态", ps.position, ps.entry_price);
            } else if has_position && ps.entry_time == 0 {
                log::error!("恢复状态异常: {:?}持仓中但entry_time=0，丢弃该状态", ps.position);
            } else {
                log::info!("恢复持仓状态: {:?} | 入场价: {:.2}", ps.position, ps.entry_price);
                state.position = ps.position;
                state.entry_price = ps.entry_price;
                state.entry_time = ps.entry_time;
                state.last_trade_time = ps.last_trade_time;
                state.daily_trades = ps.daily_trades;
                state.daily_pnl = ps.daily_pnl;
                state.last_day = ps.last_day;
            }
        }
        
        state
    }
    
    /// 创建持久化快照（不执行IO，可在锁外保存）
    fn snapshot(&self) -> PersistentState {
        PersistentState {
            position: self.position.clone(),
            entry_price: self.entry_price,
            entry_time: self.entry_time,
            last_trade_time: self.last_trade_time,
            daily_trades: self.daily_trades,
            daily_pnl: self.daily_pnl,
            last_day: self.last_day,
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
                log::info!("📅 日摘要 | 交易:{}笔 | 日盈亏:{:+.3}%",
                    self.daily_trades, self.daily_pnl);
            }
            self.last_day = day;
            self.daily_trades = 0;
            self.daily_pnl = 0.0;
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
        let direction = if config.allow_short { "多空" } else { "做多" };
        log::info!("动量策略 v{} ({}) 初始化:", env!("CARGO_PKG_VERSION"), env!("GIT_HASH"));
        log::info!("   交易对: {}({}) | 每笔: {}", config.symbol, direction, config.quantity_per_trade);
        log::info!("   止盈: {}% | 止损: {}% | 冷却: {}秒", config.take_profit_pct, config.stop_loss_pct, config.cooldown_seconds);

        Self {
            state: Arc::new(Mutex::new(StrategyState::new(&state_file))),
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
            && state.last_data_time > 0 && state.entry_price > 0.0
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
                let signal_type = if state.position == Position::Long { "SELL" } else { "COVER" };
                let net_pnl_pct = pnl_pct - self.config.round_trip_fee_pct;
                log::warn!("⚠️ 数据超时紧急平仓 | {} | {:?} | 入场: {:.2} | 当前: {:.2} | 净盈亏: {:.3}%",
                    self.config.symbol, state.position, state.entry_price, current_price, net_pnl_pct);
                
                state.position = Position::None;
                state.daily_pnl += net_pnl_pct;
                state.last_exit_pnl = net_pnl_pct;
                state.rsi_was_oversold = false;
                let snap = state.snapshot();
                let path = self.state_file.clone();
                drop(state);
                tokio::task::spawn_blocking(move || snap.save(&path));
                self.emit_signal(signal_type, current_price, now_ms).await;
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
        state.volume_ratio.add_trade(event.timestamp, event.quantity, event.is_buyer_maker);
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
            log::warn!("异常点差: {:.4}%，跳过本次信号 (bid={:.2}, ask={:.2})",
                spread_pct, event.best_bid, event.best_ask);
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
            let highest_pnl_pct = (state.highest_since_entry - state.entry_price) / state.entry_price * 100.0;

            let dynamic_sl = if highest_pnl_pct >= self.config.trailing_trigger_pct {
                if !state.trailing_active {
                    log::info!("📌 追踪激活 | {} | 最高: {:.2} | 追踪止损: {:.2} | 当前浮盈: {:.2}%",
                        self.config.symbol, state.highest_since_entry,
                        state.highest_since_entry * (1.0 - self.config.trailing_distance_pct / 100.0),
                        highest_pnl_pct);
                }
                state.trailing_active = true;
                state.highest_since_entry * (1.0 - self.config.trailing_distance_pct / 100.0)
            } else if highest_pnl_pct >= self.config.breakeven_trigger_pct {
                if !state.breakeven_active {
                    let be_price = state.entry_price * (1.0 + self.config.round_trip_fee_pct / 100.0);
                    log::info!("📌 保本激活 | {} | 止损上移至: {:.2}(含费) | 当前浮盈: {:.2}%",
                        self.config.symbol, be_price, highest_pnl_pct);
                }
                state.breakeven_active = true;
                // 费后保本：SL = 入场价 + 手续费，确保出场后真正不亏
                state.entry_price * (1.0 + self.config.round_trip_fee_pct / 100.0)
            } else {
                state.entry_price * (1.0 - self.config.stop_loss_pct / 100.0)
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
                    && !state.trailing_active {
                    "僵尸早退"
                } else if hold_secs >= self.config.max_hold_seconds {
                    "时间止损"
                } else {
                    "RSI超买"
                };

                let net_pnl_pct = pnl_pct - self.config.round_trip_fee_pct;
                log::info!("🔴 平多 | {} | 入场: {:.2} | 出场: {:.2} | 毛盈亏: {:+.3}% | 净盈亏: {:+.3}% | 最高浮盈: {:.2}% | 原因: {} | 持仓: {}s | 日累计: {:+.3}%",
                    self.config.symbol, state.entry_price, current_price, pnl_pct,
                    net_pnl_pct, highest_pnl_pct, reason, hold_secs, state.daily_pnl + net_pnl_pct);

                state.position = Position::None;
                state.daily_pnl += net_pnl_pct;
                state.last_exit_pnl = net_pnl_pct; // 记录本次出场累加的pnl，用于失败回滚
                state.last_trade_time = now_ms;
                state.rsi_was_oversold = false;

                // 连续止损熔断计数
                if reason == "止损" || reason == "时间止损" {
                    state.consecutive_stop_losses += 1;
                    let cooldown_ms = match state.consecutive_stop_losses {
                        2 => 1_200_000,     // 2连亏: 冷協20分钟
                        3 => 7_200_000,     // 3连亏: 暂停2小时
                        n if n >= 4 => 14_400_000, // 4+连亏: 暂停4小时
                        _ => 0,             // 第1次止损不额外冷却
                    };
                    if cooldown_ms > 0 {
                        state.loss_cooldown_until = now_ms + cooldown_ms;
                        log::warn!("⚠️ 连续{}次止损 | {} | 暂停交易{}min",
                            state.consecutive_stop_losses, self.config.symbol, cooldown_ms / 60_000);
                    }
                } else {
                    // 盈利出场（追踪/硬止盈/保本）重置计数
                    state.consecutive_stop_losses = 0;
                    state.loss_cooldown_until = 0;
                }

                let snap = state.snapshot();
                let path = self.state_file.clone();
                drop(state);
                tokio::task::spawn_blocking(move || snap.save(&path));
                self.emit_signal("SELL", current_price, now_ms).await;
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
            let lowest_pnl_pct = (state.entry_price - state.lowest_since_entry) / state.entry_price * 100.0;

            let dynamic_sl = if lowest_pnl_pct >= self.config.trailing_trigger_pct {
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
                    let be_price = state.entry_price * (1.0 - self.config.round_trip_fee_pct / 100.0);
                    log::info!("📌 空保本激活 | {} | 止损下移至: {:.2}(含费) | 当前浮盈: {:.2}%",
                        self.config.symbol, be_price, lowest_pnl_pct);
                }
                state.breakeven_active = true;
                // 费后保本：SL = 入场价 - 手续费，做空需价格低于此才真正保本
                state.entry_price * (1.0 - self.config.round_trip_fee_pct / 100.0)
            } else {
                state.entry_price * (1.0 + self.config.stop_loss_pct / 100.0)
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
                    && !state.trailing_active {
                    "空僵尸早退"
                } else {
                    "空超时"
                };

                let net_pnl_pct = pnl_pct - self.config.round_trip_fee_pct;
                log::info!("🔴 平空 | {} | 入场: {:.2} | 出场: {:.2} | 毛盈亏: {:+.3}% | 净盈亏: {:+.3}% | 最高浮盈: {:.2}% | 原因: {} | 持仓: {}s | 日累计: {:+.3}%",
                    self.config.symbol, state.entry_price, current_price, pnl_pct,
                    net_pnl_pct, lowest_pnl_pct, reason, hold_secs, state.daily_pnl + net_pnl_pct);

                state.position = Position::None;
                state.daily_pnl += net_pnl_pct;
                state.last_exit_pnl = net_pnl_pct; // 记录本次出场累加的pnl，用于失败回滚
                state.last_trade_time = now_ms;

                // 连续止损熔断计数
                if reason == "空止损" || reason == "空超时" {
                    state.consecutive_stop_losses += 1;
                    let cooldown_ms = match state.consecutive_stop_losses {
                        2 => 1_200_000,     // 2连亏: 冷協20分钟
                        3 => 7_200_000,     // 3连亏: 暂停2小时
                        n if n >= 4 => 14_400_000, // 4+连亏: 暂停4小时
                        _ => 0,             // 第1次止损不额外冷却
                    };
                    if cooldown_ms > 0 {
                        state.loss_cooldown_until = now_ms + cooldown_ms;
                        log::warn!("⚠️ 连续{}次止损 | {} | 暂停交易{}min",
                            state.consecutive_stop_losses, self.config.symbol, cooldown_ms / 60_000);
                    }
                } else {
                    // 盈利出场（追踪/硬止盈/保本）重置计数
                    state.consecutive_stop_losses = 0;
                    state.loss_cooldown_until = 0;
                }

                let snap = state.snapshot();
                let path = self.state_file.clone();
                drop(state);
                tokio::task::spawn_blocking(move || snap.save(&path));
                self.emit_signal("COVER", current_price, now_ms).await;
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

            // EMA21斜率：判断中期趋势动能方向
            let ema21_slope = if state.prev_ema_slow_5m > 0.0 {
                (ema_slow_5m - state.prev_ema_slow_5m) / state.prev_ema_slow_5m * 100.0
            } else { 0.0 };

            // === 做多入场条件 ===
            // EMA50宏观方向过滤：确认中期趋势上升
            let ema50_macro_rising = if state.ema50_history.len() >= 20 {
                ema_trend > state.ema50_history[0]  // 对比20根bar前（100分钟）
            } else { false };

            // 趋势环境确认：价格在EMA50之上至少0.15% + EMA21斜率非下降 + EMA50宏观上升
            let price_above_ema50_pct = if ema_trend > 0.0 {
                (current_price - ema_trend) / ema_trend * 100.0
            } else { 0.0 };
            let long_trend_env_ok = ema_trend > 0.0
                && price_above_ema50_pct > 0.15  // 至少高于EMA50 0.15%，避免边缘试探
                && ema21_slope > 0.03  // 斜率>+0.03% 确认上升动能
                && ema50_macro_rising;  // EMA50宏观方向必须上升

            // 条件1: 5分钟趋势向上 + 趋势强度过滤
            let trend_up = ema_fast_5m > ema_slow_5m;
            let trend_strength = if ema_slow_5m > 0.0 {
                (ema_fast_5m - ema_slow_5m) / ema_slow_5m * 100.0
            } else { 0.0 };
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
                let lookback_high = state.recent_highs[..state.recent_highs.len()-1]
                    .iter().copied().fold(f64::NEG_INFINITY, f64::max);
                let breakout_pct = (state.best_ask - lookback_high) / lookback_high * 100.0;
                // RSI < 65: 防止在RSI高位追高（RSI>=65时的突破信号可靠性差）
                breakout_pct > 0.05 && vol_ratio > self.config.volume_ratio_threshold && rsi < 65.0
            } else {
                false
            };

            // RSI反弹入场 或 突破入场（必须通过趋势环境确认）
            let trend_entry_signal = long_trend_env_ok
                && ((trend_up && trend_strong_enough && rsi_recovering && rsi_bounce_vr_ok && bid_support)
                    || (breakout_signal && trend_up && trend_strong_enough));

            // === 均值回归入场信号（超跌反弹，不需要趋势确认） ===
            let mean_revert_signal = if state.recent_highs.len() >= 20 {
                let recent_high = state.recent_highs.iter().copied().fold(f64::NEG_INFINITY, f64::max);
                let drop_pct = (recent_high - current_price) / recent_high * 100.0;
                // 从20bar高点跌>1.2% + RSI<38 + VR>2.5(强买盘) + 盘口支撑
                drop_pct > 1.2 && rsi < 38.0 && vol_ratio > 2.5 && bid_support
            } else { false };

            let entry_signal = trend_entry_signal || mean_revert_signal;

            if entry_signal {
                let entry_price = state.best_ask;
                // 优先判断RSI反弹路径：两种条件同时满足时RSI反弹更具体
                let entry_path = if mean_revert_signal { "均值回归" }
                    else if trend_up && rsi_recovering && rsi_bounce_vr_ok && bid_support { "RSI反弹" }
                    else { "突破" };
                let sl_price = entry_price * (1.0 - self.config.stop_loss_pct / 100.0);
                let tp_price = entry_price * (1.0 + self.config.take_profit_pct / 100.0);

                log::info!("🟢 做多 | {} @ {:.2} | RSI:{:.1} | 量比:{:.2} | EMA5m:{:.2}/{:.2}/{:.2} | 斜率:{:+.3}% | 路径:{} | SL:{:.2} | TP:{:.2}",
                    self.config.symbol, entry_price, rsi, vol_ratio,
                    ema_fast_5m, ema_slow_5m, ema_trend, ema21_slope, entry_path, sl_price, tp_price);

                state.position = Position::Long;
                state.entry_price = entry_price;
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
                self.emit_signal("BUY", entry_price, now_ms).await;
            } else if self.config.allow_short {
                // === 做空入场条件 ===
                // 趋势环境确认：价格在EMA50之下至少0.15% + EMA21斜率非上升
                let price_below_ema50_pct = if ema_trend > 0.0 {
                    (ema_trend - state.best_bid) / ema_trend * 100.0
                } else { 0.0 };
                let short_trend_env_ok = ema_trend > 0.0
                    && price_below_ema50_pct > 0.15  // 至少低于EMA50 0.15%
                    && ema21_slope < -0.03;  // 斜率<-0.03% 确认下降动能（回测验证）

                let trend_down = ema_fast_5m < ema_slow_5m;
                let short_trend_strength = if ema_slow_5m > 0.0 {
                    (ema_slow_5m - ema_fast_5m) / ema_slow_5m * 100.0
                } else { 0.0 };
                let short_trend_strong = short_trend_strength >= self.config.min_trend_strength_pct;
                let breakdown_signal = if state.recent_lows.len() >= 10 {
                    let lookback_low = state.recent_lows[..state.recent_lows.len()-1]
                        .iter().copied().fold(f64::INFINITY, f64::min);
                    let breakdown_pct = (lookback_low - state.best_bid) / lookback_low * 100.0;
                    // RSI > 40: 防止RSI已处于极度超卖时做空击穿（下跌动能已耗尽）
                    breakdown_pct > 0.05 && vol_ratio < (1.0 / self.config.volume_ratio_threshold) && rsi > 40.0
                } else {
                    false
                };

                let rsi_overbought_short = rsi > 70.0;
                let sell_pressure = vol_ratio < (1.0 / self.config.volume_ratio_threshold);

                let short_signal = short_trend_env_ok
                    && ((breakdown_signal && trend_down && short_trend_strong)
                        || (trend_down && short_trend_strong && rsi_overbought_short && sell_pressure));

                if short_signal {
                    let entry_price = state.best_bid; // 做空用bid
                    let short_path = if breakdown_signal && trend_down { "击穿" } else { "RSI超买" };
                    let sl_price = entry_price * (1.0 + self.config.stop_loss_pct / 100.0);
                    let tp_price = entry_price * (1.0 - self.config.take_profit_pct / 100.0);

                    log::info!("🟡 做空 | {} @ {:.2} | RSI:{:.1} | 量比:{:.2} | EMA5m:{:.2}/{:.2}/{:.2} | 斜率:{:+.3}% | 路径:{} | SL:{:.2} | TP:{:.2}",
                        self.config.symbol, entry_price, rsi, vol_ratio,
                        ema_fast_5m, ema_slow_5m, ema_trend, ema21_slope, short_path, sl_price, tp_price);

                    state.position = Position::Short;
                    state.entry_price = entry_price;
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
                    self.emit_signal("SHORT", entry_price, now_ms).await;
                }
            }
        }
    }

    /// 发布交易信号事件
    async fn emit_signal(&self, signal_type: &str, price: f64, timestamp: u64) {
        let signal = TradingSignalEvent {
            signal_id: format!("momentum_{}_{}", signal_type.to_lowercase(), timestamp),
            strategy_id: "momentum_scalp".to_string(),
            symbol: self.config.symbol.clone(),
            signal_type: signal_type.to_string(),
            strength: 1.0,
            suggested_price: price,
            suggested_quantity: Some(self.config.quantity_per_trade),
            stop_loss_price: match signal_type {
                "BUY" => Some(price * (1.0 - self.config.stop_loss_pct / 100.0)),
                "SHORT" => Some(price * (1.0 + self.config.stop_loss_pct / 100.0)),
                _ => None,
            },
            take_profit_price: match signal_type {
                "BUY" => Some(price * (1.0 + self.config.take_profit_pct / 100.0)),
                "SHORT" => Some(price * (1.0 - self.config.take_profit_pct / 100.0)),
                _ => None,
            },
            timestamp,
        };

        if let Err(e) = self.event_bus.publish(DomainEvent::TradingSignal(signal)).await {
            log::error!("发布交易信号失败: {}", e);
        }
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
                log::warn!("⚠️ 入场失败回滚 | {} | {:?} -> None | 入场价: {:.2} | 原因: {}",
                    self.config.symbol, state.position, state.entry_price, event.reason);
                state.position = Position::None;
                state.entry_price = 0.0;
                state.entry_time = 0;
                state.trailing_active = false;
                state.breakeven_active = false;
                // 回滚 daily_trades（入场时+1了，现在要-1）
                if state.daily_trades > 0 {
                    state.daily_trades -= 1;
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
                log::info!("  └─ 回滚 daily_pnl: 减去 {:+.3}% | 修正后: {:+.3}%",
                    state.last_exit_pnl, state.daily_pnl);
                state.last_exit_pnl = 0.0;
            }
                
            // 累加连续失败计数
            state.consecutive_exit_failures += 1;
                
            // 超过3次连续失败：强制放弃持仓，避免死循环
            if state.consecutive_exit_failures >= 3 {
                log::error!("❗ 连续{}次平仓失败 | {} | 强制清除持仓状态 | 入场价: {:.2} | 原因: {}",
                    state.consecutive_exit_failures, self.config.symbol, state.entry_price, event.reason);
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
                
            log::warn!("⚠️ 平仓失败回滚 | {} | 恢复{:?}持仓 | 入场价: {:.2} | 60s熔断({}/3) | 原因: {}",
                self.config.symbol, original_position, state.entry_price,
                state.consecutive_exit_failures, event.reason);
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
            EventType::OrderRejected,
        ]
    }
}
