//! 动量短线策略
//!
//! 基于多数据流（K线、成交流、盘口）的高频动量短线策略
//! 核心逻辑：趋势方向 + RSI超卖/超买回归 + 成交量确认 + 盘口强度

use crate::config::StrategyConfig;
use crate::error::EventBusError;
use crate::event_bus::{EventBus, EventHandler, EventType, TokioEventBus};
use crate::events::{
    AggTradeEvent, BookTickerEvent, DomainEvent, KlineCompletedEvent, TradingSignalEvent,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;

use super::indicators::{EMA, RSI, VolumeRatio};

const STATE_FILE: &str = "data/strategy_state.json";

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
    fn save(&self) {
        if let Some(parent) = std::path::Path::new(STATE_FILE).parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        match serde_json::to_string_pretty(self) {
            Ok(json) => {
                // 原子写入：先写临时文件，再rename，防止并发写入损坏
                let temp_file = format!("{}.tmp", STATE_FILE);
                if let Err(e) = std::fs::write(&temp_file, &json) {
                    log::error!("保存策略状态失败(写临时文件): {}", e);
                    return;
                }
                if let Err(e) = std::fs::rename(&temp_file, STATE_FILE) {
                    log::error!("保存策略状态失败(rename): {}", e);
                    let _ = std::fs::remove_file(&temp_file);
                }
            }
            Err(e) => log::error!("序列化策略状态失败: {}", e),
        }
    }
    
    fn load() -> Option<Self> {
        match std::fs::read_to_string(STATE_FILE) {
            Ok(json) => {
                match serde_json::from_str(&json) {
                    Ok(state) => {
                        log::info!("✅ 恢复策略状态成功");
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

    // RSI状态追踪（检测回升）
    rsi_was_oversold: bool, // RSI曾经低于超卖线
    rsi_oversold_bars: usize, // 超卖标志已持续的K线数（过期机制）

    // 预热计数
    kline_1m_count: usize,
    kline_5m_count: usize,
}

impl StrategyState {
    fn new() -> Self {
        // 尝试从文件恢复持仓状态
        let persisted = PersistentState::load();
        
        let mut state = Self {
            ema_fast_1m: EMA::new(7),
            ema_slow_1m: EMA::new(21),
            rsi_1m: RSI::new(14),
            ema_fast_5m: EMA::new(7),
            ema_slow_5m: EMA::new(21),
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
            rsi_was_oversold: false,
            rsi_oversold_bars: 0,
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

    /// 是否完成预热（需要足够的K线数据）
    fn is_warmed_up(&self) -> bool {
        self.kline_1m_count >= 21 && self.kline_5m_count >= 21
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
        }
    }
}

/// 动量短线策略
pub struct MomentumStrategy {
    config: StrategyConfig,
    event_bus: Arc<TokioEventBus>,
    state: Arc<Mutex<StrategyState>>,
}

impl MomentumStrategy {
    /// 创建新的动量策略
    pub fn new(config: StrategyConfig, event_bus: Arc<TokioEventBus>) -> Self {
        log::info!("动量策略 v{} ({}) 初始化:", env!("CARGO_PKG_VERSION"), env!("GIT_HASH"));
        log::info!("   交易对: {}", config.symbol);
        log::info!("   每笔数量: {} BTC", config.quantity_per_trade);
        log::info!("   止盈: {}% | 止损: {}%", config.take_profit_pct, config.stop_loss_pct);
        log::info!("   冷却: {}秒 | 日限: {}次", config.cooldown_seconds, config.max_daily_trades);

        Self {
            config,
            event_bus,
            state: Arc::new(Mutex::new(StrategyState::new())),
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
                log::warn!("⚠️ 数据超时紧急平仓 | {} | {:?} | 入场: {:.2} | 当前: {:.2} | 盈亏: {:.3}%",
                    self.config.symbol, state.position, state.entry_price, current_price, pnl_pct);
                
                state.position = Position::None;
                state.daily_pnl += pnl_pct;
                state.rsi_was_oversold = false;
                let snap = state.snapshot();
                drop(state);
                tokio::task::spawn_blocking(move || snap.save());
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
                    state.ema_fast_5m.update(event.close);
                    state.ema_slow_5m.update(event.close);
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

        // 预热未完成不交易
        if !state.is_warmed_up() {
            return;
        }

        let now_ms = event.timestamp;

        // 检查出场条件（阶梯式保护机制）
        if state.position == Position::Long && state.entry_price > 0.0 {
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
                    log::info!("📌 保本激活 | {} | 止损上移至: {:.2} | 当前浮盈: {:.2}%",
                        self.config.symbol, state.entry_price, highest_pnl_pct);
                }
                state.breakeven_active = true;
                state.entry_price
            } else {
                state.entry_price * (1.0 - self.config.stop_loss_pct / 100.0)
            };

            let should_exit = current_price <= dynamic_sl
                || pnl_pct >= self.config.take_profit_pct
                || hold_secs >= self.config.max_hold_seconds
                || rsi > self.config.rsi_overbought;

            if should_exit {
                let reason = if pnl_pct >= self.config.take_profit_pct {
                    "硬止盈"
                } else if state.trailing_active && current_price <= dynamic_sl {
                    "追踪止损"
                } else if state.breakeven_active && current_price <= dynamic_sl {
                    "保本止损"
                } else if current_price <= dynamic_sl {
                    "止损"
                } else if hold_secs >= self.config.max_hold_seconds {
                    "时间止损"
                } else {
                    "RSI超买"
                };

                log::info!("🔴 平多 | {} | 入场: {:.2} | 出场: {:.2} | 盈亏: {:+.3}% | 最高浮盈: {:.2}% | 原因: {} | 持仓: {}s | 日累计: {:+.3}%",
                    self.config.symbol, state.entry_price, current_price, pnl_pct,
                    highest_pnl_pct, reason, hold_secs, state.daily_pnl + pnl_pct);

                state.position = Position::None;
                state.daily_pnl += pnl_pct;
                state.last_trade_time = now_ms;
                state.rsi_was_oversold = false;
                let snap = state.snapshot();
                drop(state);
                tokio::task::spawn_blocking(move || snap.save());
                self.emit_signal("SELL", current_price, now_ms).await;
                return;
            }
        }

        // 做空出场检查
        if state.position == Position::Short && state.entry_price > 0.0 {
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
                    log::info!("📌 空保本激活 | {} | 止损下移至: {:.2} | 当前浮盈: {:.2}%",
                        self.config.symbol, state.entry_price, lowest_pnl_pct);
                }
                state.breakeven_active = true;
                state.entry_price
            } else {
                state.entry_price * (1.0 + self.config.stop_loss_pct / 100.0)
            };

            let should_exit = current_price >= dynamic_sl
                || pnl_pct >= self.config.take_profit_pct
                || hold_secs >= self.config.max_hold_seconds;

            if should_exit {
                let reason = if pnl_pct >= self.config.take_profit_pct {
                    "空止盈"
                } else if state.trailing_active && current_price >= dynamic_sl {
                    "空追踪止损"
                } else if state.breakeven_active && current_price >= dynamic_sl {
                    "空保本止损"
                } else if current_price >= dynamic_sl {
                    "空止损"
                } else {
                    "空超时"
                };

                log::info!("🔴 平空 | {} | 入场: {:.2} | 出场: {:.2} | 盈亏: {:+.3}% | 最高浮盈: {:.2}% | 原因: {} | 持仓: {}s | 日累计: {:+.3}%",
                    self.config.symbol, state.entry_price, current_price, pnl_pct,
                    lowest_pnl_pct, reason, hold_secs, state.daily_pnl + pnl_pct);

                state.position = Position::None;
                state.daily_pnl += pnl_pct;
                state.last_trade_time = now_ms;
                let snap = state.snapshot();
                drop(state);
                tokio::task::spawn_blocking(move || snap.save());
                self.emit_signal("COVER", current_price, now_ms).await;
                return;
            }
        }

        // 检查入场条件（空仓时）
        if state.position == Position::None {
            // 冷却检查
            if now_ms - state.last_trade_time < self.config.cooldown_seconds * 1000 {
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

            // === 做多入场条件 ===
            let ema_fast_5m = state.ema_fast_5m.value().unwrap_or(0.0);
            let ema_slow_5m = state.ema_slow_5m.value().unwrap_or(0.0);
            let rsi = state.rsi_1m.value().unwrap_or(50.0);
            let vol_ratio = state.volume_ratio.ratio();

            // 条件1: 5分钟趋势向上
            let trend_up = ema_fast_5m > ema_slow_5m;

            // 条件2: RSI从超卖回升（加天花板：RSI超过overbought时不入场）
            let rsi_recovering = state.rsi_was_oversold
                && rsi > (self.config.rsi_oversold + 5.0)
                && rsi < self.config.rsi_overbought;

            // 条件3: 买方成交量主导
            let buy_dominant = vol_ratio > self.config.volume_ratio_threshold;

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

            // RSI反弹入场 或 突破入场
            let entry_signal = (trend_up && rsi_recovering && buy_dominant && bid_support)
                || (breakout_signal && trend_up);

            if entry_signal {
                let entry_price = state.best_ask;
                // 优先判断RSI反弹路径：两种条件同时满足时RSI反弹更具体
                let entry_path = if trend_up && rsi_recovering && buy_dominant && bid_support { "RSI反弹" } else { "突破" };
                let sl_price = entry_price * (1.0 - self.config.stop_loss_pct / 100.0);
                let tp_price = entry_price * (1.0 + self.config.take_profit_pct / 100.0);

                log::info!("🟢 做多 | {} @ {:.2} | RSI:{:.1} | 量比:{:.2} | EMA5m:{:.2}/{:.2} | 路径:{} | SL:{:.2} | TP:{:.2}",
                    self.config.symbol, entry_price, rsi, vol_ratio,
                    ema_fast_5m, ema_slow_5m, entry_path, sl_price, tp_price);

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
                let snap = state.snapshot();
                drop(state);
                tokio::task::spawn_blocking(move || snap.save());
                self.emit_signal("BUY", entry_price, now_ms).await;
            } else if self.config.allow_short {
                // === 做空入场条件 ===
                let trend_down = ema_fast_5m < ema_slow_5m;
                let breakdown_signal = if state.recent_lows.len() >= 10 {
                    let lookback_low = state.recent_lows[..state.recent_lows.len()-1]
                        .iter().copied().fold(f64::INFINITY, f64::min);
                    let breakdown_pct = (lookback_low - state.best_bid) / lookback_low * 100.0;
                    // RSI > 35: 防止RSI已处于极度超卖时做空击穿（与做多突破rsi<65对称）
                    breakdown_pct > 0.05 && vol_ratio < (1.0 / self.config.volume_ratio_threshold) && rsi > 35.0
                } else {
                    false
                };

                let rsi_overbought_short = rsi > 70.0;
                let sell_pressure = vol_ratio < (1.0 / self.config.volume_ratio_threshold);

                let short_signal = (breakdown_signal && trend_down)
                    || (trend_down && rsi_overbought_short && sell_pressure);

                if short_signal {
                    let entry_price = state.best_bid; // 做空用bid
                    let short_path = if breakdown_signal && trend_down { "击穿" } else { "RSI超买" };
                    let sl_price = entry_price * (1.0 + self.config.stop_loss_pct / 100.0);
                    let tp_price = entry_price * (1.0 - self.config.take_profit_pct / 100.0);

                    log::info!("🟡 做空 | {} @ {:.2} | RSI:{:.1} | 量比:{:.2} | EMA5m:{:.2}/{:.2} | 路径:{} | SL:{:.2} | TP:{:.2}",
                        self.config.symbol, entry_price, rsi, vol_ratio,
                        ema_fast_5m, ema_slow_5m, short_path, sl_price, tp_price);

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
                    let snap = state.snapshot();
                    drop(state);
                    tokio::task::spawn_blocking(move || snap.save());
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
}

#[async_trait]
impl EventHandler for MomentumStrategy {
    async fn handle(&self, event: &DomainEvent) -> Result<(), EventBusError> {
        match event {
            DomainEvent::KlineCompleted(e) => self.on_kline(e).await,
            DomainEvent::AggTrade(e) => self.on_agg_trade(e).await,
            DomainEvent::BookTicker(e) => self.on_book_ticker(e).await,
            _ => {}
        }
        Ok(())
    }

    fn event_types(&self) -> Vec<EventType> {
        vec![
            EventType::KlineCompleted,
            EventType::AggTrade,
            EventType::BookTicker,
        ]
    }
}
