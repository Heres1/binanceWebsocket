//! V2 策略引擎 - 结构性优化
//!
//! 核心改进：
//! 1. Intra-bar 止盈止损：利用K线High/Low判断bar内是否触发TP/SL
//! 2. Trailing Stop（追踪止损）：让盈利奔跑
//! 3. 多策略类型：趋势跟随、突破、均值回归等
//! 4. 支持做空：下跌行情也能获利
//! 5. 放宽入场条件：移除冗余的bid_support

use crate::backtest::data_loader::BacktestKline;
use crate::backtest::report::{BacktestReport, TradeRecord};
use crate::strategies::indicators::{EMA, RSI, VolumeRatio};

/// 策略类型枚举
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StrategyType {
    /// A: 趋势动量（放宽版）- 5m趋势 + 成交量确认即入场
    TrendMomentum,
    /// B: EMA交叉 - 1m快线上穿慢线 + 5m趋势确认
    EmaCrossover,
    /// C: RSI反弹 - RSI超卖后反弹 + 趋势确认
    RsiBounce,
    /// D: 突破策略 - 价格突破N根K线高点 + 量能确认
    Breakout,
    /// E: 综合动量 - 多信号加权打分入场
    CompositeScore,
}

/// V2 策略参数配置
#[derive(Debug, Clone)]
pub struct StrategyV2Config {
    pub strategy_type: StrategyType,
    /// 止盈百分比
    pub take_profit_pct: f64,
    /// 止损百分比
    pub stop_loss_pct: f64,
    /// 追踪止损回撑百分比（从最高点回落X%平仓，0=不启用）
    pub trailing_stop_pct: f64,
    /// 最大持仓秒数
    pub max_hold_seconds: u64,
    /// 冷却秒数
    pub cooldown_seconds: u64,
    /// RSI超卖线
    pub rsi_oversold: f64,
    /// RSI超买线（出场用，设为100.0可禁用RSI出场）
    pub rsi_overbought: f64,
    /// 成交量比阈值
    pub volume_ratio_threshold: f64,
    /// 是否允许做空
    pub allow_short: bool,
    /// 突破策略：回看K线根数
    pub breakout_lookback: usize,
    /// 综合策略：最低入场得分(0-100)
    pub min_entry_score: f64,
    /// 每笔交易数量
    pub quantity_per_trade: f64,

    // === 做空专用参数（非对称）===
    /// 做空止盈百分比（下跌快而短，通常比多头小）
    pub short_take_profit_pct: f64,
    /// 做空止损百分比（反弹猛，通常比多头小）
    pub short_stop_loss_pct: f64,
    /// 做空追踪止损
    pub short_trailing_stop_pct: f64,
    /// 做空最大持仓秒数（下跌快，持仓时间短）
    pub short_max_hold_seconds: u64,
    /// 做空需要的最小下跌动量（EMA差值百分比，确认趋势强度）
    pub short_min_trend_strength: f64,
}

impl Default for StrategyV2Config {
    fn default() -> Self {
        Self {
            strategy_type: StrategyType::TrendMomentum,
            take_profit_pct: 1.5,
            stop_loss_pct: 0.8,
            trailing_stop_pct: 0.0,
            max_hold_seconds: 28800,
            cooldown_seconds: 600,
            rsi_oversold: 30.0,
            rsi_overbought: 100.0,
            volume_ratio_threshold: 1.5,
            allow_short: true,
            breakout_lookback: 20,
            min_entry_score: 60.0,
            quantity_per_trade: 0.00013,
            // 做空专用参数：下跌快而短，反弹猛
            short_take_profit_pct: 1.0,
            short_stop_loss_pct: 0.5,
            short_trailing_stop_pct: 0.3,
            short_max_hold_seconds: 14400, // 4小时
            short_min_trend_strength: 0.05, // EMA差值>0.05%
        }
    }
}

/// 持仓方向
#[derive(Debug, Clone, PartialEq)]
enum Direction {
    None,
    Long,
    Short,
}

/// V2 引擎状态
struct StateV2 {
    // 指标
    ema_fast_1m: EMA,
    ema_slow_1m: EMA,
    ema_fast_5m: EMA,
    ema_slow_5m: EMA,
    rsi_1m: RSI,
    volume_ratio: VolumeRatio,

    // EMA交叉检测
    prev_ema_fast_1m: f64,
    prev_ema_slow_1m: f64,

    // 持仓
    direction: Direction,
    entry_price: f64,
    entry_time: u64,
    highest_since_entry: f64,  // 入场后最高价（追踪止损用）
    lowest_since_entry: f64,   // 入场后最低价（做空追踪止损用）

    // 冷却与统计
    last_trade_time: u64,
    daily_trades: u32,
    daily_pnl: f64,
    last_day: u32,

    // RSI状态
    rsi_was_oversold: bool,
    rsi_was_overbought: bool,

    // 预热
    kline_1m_count: usize,
    kline_5m_count: usize,

    // 突破策略：记录最近N根K线的high/low
    recent_highs: Vec<f64>,
    recent_lows: Vec<f64>,
}

impl StateV2 {
    fn new(breakout_lookback: usize) -> Self {
        Self {
            ema_fast_1m: EMA::new(7),
            ema_slow_1m: EMA::new(21),
            ema_fast_5m: EMA::new(7),
            ema_slow_5m: EMA::new(21),
            rsi_1m: RSI::new(14),
            volume_ratio: VolumeRatio::new(60),
            prev_ema_fast_1m: 0.0,
            prev_ema_slow_1m: 0.0,
            direction: Direction::None,
            entry_price: 0.0,
            entry_time: 0,
            highest_since_entry: 0.0,
            lowest_since_entry: f64::MAX,
            last_trade_time: 0,
            daily_trades: 0,
            daily_pnl: 0.0,
            last_day: 0,
            rsi_was_oversold: false,
            rsi_was_overbought: false,
            kline_1m_count: 0,
            kline_5m_count: 0,
            recent_highs: Vec::with_capacity(breakout_lookback),
            recent_lows: Vec::with_capacity(breakout_lookback),
        }
    }

    fn is_warmed_up(&self) -> bool {
        self.kline_1m_count >= 21 && self.kline_5m_count >= 21
    }

    fn check_daily_reset(&mut self, timestamp_ms: u64) {
        let day = (timestamp_ms / 86400000) as u32;
        if day != self.last_day {
            self.last_day = day;
            self.daily_trades = 0;
            self.daily_pnl = 0.0;
        }
    }
}

/// V2 回测主函数
pub fn run_backtest_v2(
    klines_1m: &[BacktestKline],
    klines_5m: &[BacktestKline],
    config: &StrategyV2Config,
    initial_capital: f64,
    commission_rate: f64,
) -> BacktestReport {
    // 合并K线事件
    let mut events: Vec<(&BacktestKline, bool)> = Vec::new();
    for k in klines_1m {
        events.push((k, true));
    }
    for k in klines_5m {
        events.push((k, false));
    }
    events.sort_by_key(|(k, _)| k.close_time);

    let start_time = events.first().map(|(k, _)| k.open_time).unwrap_or(0);
    let end_time = events.last().map(|(k, _)| k.close_time).unwrap_or(0);

    let mut state = StateV2::new(config.breakout_lookback);
    let mut trades: Vec<TradeRecord> = Vec::new();
    let mut trade_id = 0;

    for (kline, is_1m) in &events {
        if kline.close <= 0.0 {
            continue;
        }
        let timestamp = kline.close_time;

        if *is_1m {
            state.kline_1m_count += 1;

            // 保存上一次的EMA值（用于交叉检测）
            state.prev_ema_fast_1m = state.ema_fast_1m.value().unwrap_or(0.0);
            state.prev_ema_slow_1m = state.ema_slow_1m.value().unwrap_or(0.0);

            state.ema_fast_1m.update(kline.close);
            state.ema_slow_1m.update(kline.close);
            let rsi = state.rsi_1m.update(kline.close);

            if rsi < config.rsi_oversold {
                state.rsi_was_oversold = true;
            }
            if rsi > config.rsi_overbought {
                state.rsi_was_overbought = true;
            }

            // 成交量比
            let buy_vol = kline.taker_buy_volume;
            let sell_vol = kline.volume - buy_vol;
            state.volume_ratio.add_trade(timestamp, buy_vol, false);
            state.volume_ratio.add_trade(timestamp, sell_vol, true);

            // 记录最近K线的high/low（突破策略用）
            state.recent_highs.push(kline.high);
            state.recent_lows.push(kline.low);
            if state.recent_highs.len() > config.breakout_lookback {
                state.recent_highs.remove(0);
                state.recent_lows.remove(0);
            }
        } else {
            state.kline_5m_count += 1;
            state.ema_fast_5m.update(kline.close);
            state.ema_slow_5m.update(kline.close);
            continue; // 5m K线只更新指标
        }

        state.check_daily_reset(timestamp);
        if !state.is_warmed_up() {
            continue;
        }

        // === 出场逻辑（使用Intra-bar High/Low） ===
        if state.direction != Direction::None {
            // 更新追踪价格
            if state.direction == Direction::Long {
                if kline.high > state.highest_since_entry {
                    state.highest_since_entry = kline.high;
                }
            } else {
                if kline.low < state.lowest_since_entry {
                    state.lowest_since_entry = kline.low;
                }
            }

            let exit_result = check_exit_v2(kline, &state, config, timestamp);

            if let Some((exit_price, reason)) = exit_result {
                let pnl_pct = if state.direction == Direction::Long {
                    (exit_price - state.entry_price) / state.entry_price * 100.0
                } else {
                    (state.entry_price - exit_price) / state.entry_price * 100.0
                };

                let hold_secs = (timestamp - state.entry_time) / 1000;
                let quantity = config.quantity_per_trade;
                let pnl_usdt = quantity * state.entry_price * pnl_pct / 100.0;
                let commission = quantity * (state.entry_price + exit_price) * commission_rate;

                trade_id += 1;
                trades.push(TradeRecord {
                    id: trade_id,
                    entry_time: state.entry_time,
                    exit_time: timestamp,
                    entry_price: state.entry_price,
                    exit_price,
                    quantity,
                    pnl_pct,
                    pnl_usdt,
                    commission,
                    hold_seconds: hold_secs,
                    exit_reason: reason,
                });

                state.direction = Direction::None;
                state.daily_pnl += pnl_pct;
                state.last_trade_time = timestamp;
                state.daily_trades += 1;
            }
        }

        // === 入场逻辑 ===
        if state.direction == Direction::None {
            // 冷却检查
            if timestamp - state.last_trade_time < config.cooldown_seconds * 1000 {
                continue;
            }
            if state.daily_trades >= 20 {
                continue;
            }
            if state.daily_pnl <= -3.0 {
                continue;
            }

            let entry = check_entry_v2(kline, &state, config);

            if let Some(dir) = entry {
                state.direction = dir;
                state.entry_price = kline.close;
                state.entry_time = timestamp;
                state.highest_since_entry = kline.high;
                state.lowest_since_entry = kline.low;
                state.rsi_was_oversold = false;
                state.rsi_was_overbought = false;
                state.last_trade_time = timestamp;
                state.daily_trades += 1;
            }
        }
    }

    // 回测结束强制平仓
    if state.direction != Direction::None {
        if let Some((last_kline, _)) = events.last() {
            let exit_price = last_kline.close;
            let pnl_pct = if state.direction == Direction::Long {
                (exit_price - state.entry_price) / state.entry_price * 100.0
            } else {
                (state.entry_price - exit_price) / state.entry_price * 100.0
            };
            let hold_secs = (last_kline.close_time - state.entry_time) / 1000;
            let quantity = config.quantity_per_trade;
            let pnl_usdt = quantity * state.entry_price * pnl_pct / 100.0;
            let commission = quantity * (state.entry_price + exit_price) * commission_rate;

            trade_id += 1;
            trades.push(TradeRecord {
                id: trade_id,
                entry_time: state.entry_time,
                exit_time: last_kline.close_time,
                entry_price: state.entry_price,
                exit_price,
                quantity,
                pnl_pct,
                pnl_usdt,
                commission,
                hold_seconds: hold_secs,
                exit_reason: "回测结束".to_string(),
            });
        }
    }

    BacktestReport::generate(trades, initial_capital, start_time, end_time)
}

/// Intra-bar 出场检查：利用K线High/Low判断bar内是否触发止盈止损
fn check_exit_v2(
    kline: &BacktestKline,
    state: &StateV2,
    config: &StrategyV2Config,
    timestamp: u64,
) -> Option<(f64, String)> {
    let hold_secs = (timestamp - state.entry_time) / 1000;

    if state.direction == Direction::Long {
        let tp_price = state.entry_price * (1.0 + config.take_profit_pct / 100.0);
        let sl_price = state.entry_price * (1.0 - config.stop_loss_pct / 100.0);

        // Trailing stop: 从最高点回落X%
        let trailing_price = if config.trailing_stop_pct > 0.0 {
            state.highest_since_entry * (1.0 - config.trailing_stop_pct / 100.0)
        } else {
            0.0 // 不启用
        };

        // 检查bar内是否触发止损（优先判断，保守估计）
        if kline.low <= sl_price {
            return Some((sl_price, "止损".to_string()));
        }

        // 检查trailing stop（bar内low <= trailing价格）
        if config.trailing_stop_pct > 0.0
            && state.highest_since_entry > state.entry_price * (1.0 + config.trailing_stop_pct / 100.0)
            && kline.low <= trailing_price
        {
            return Some((trailing_price, "追踪止损".to_string()));
        }

        // 检查bar内是否触发止盈
        if kline.high >= tp_price {
            return Some((tp_price, "止盈".to_string()));
        }

        // 超时平仓
        if hold_secs >= config.max_hold_seconds {
            return Some((kline.close, "超时".to_string()));
        }

        // RSI超买平仓
        let rsi = state.rsi_1m.value().unwrap_or(50.0);
        if rsi > config.rsi_overbought {
            return Some((kline.close, "RSI超买".to_string()));
        }
    } else if state.direction == Direction::Short {
        let tp_price = state.entry_price * (1.0 - config.short_take_profit_pct / 100.0);
        let sl_price = state.entry_price * (1.0 + config.short_stop_loss_pct / 100.0);

        // Trailing stop for short: 从最低点反弹X%
        let trailing_price = if config.short_trailing_stop_pct > 0.0 {
            state.lowest_since_entry * (1.0 + config.short_trailing_stop_pct / 100.0)
        } else {
            f64::MAX
        };

        // 止损（价格上涨过高）
        if kline.high >= sl_price {
            return Some((sl_price, "止损".to_string()));
        }

        // Trailing stop
        if config.short_trailing_stop_pct > 0.0
            && state.lowest_since_entry < state.entry_price * (1.0 - config.short_trailing_stop_pct / 100.0)
            && kline.high >= trailing_price
        {
            return Some((trailing_price, "追踪止损".to_string()));
        }

        // 止盈（价格下跌到目标）
        if kline.low <= tp_price {
            return Some((tp_price, "止盈".to_string()));
        }

        // 超时（做空使用独立的更短持仓时间）
        if hold_secs >= config.short_max_hold_seconds {
            return Some((kline.close, "超时".to_string()));
        }

        // RSI超卖平仓（空单 - 市场超卖可能反弹）
        let rsi = state.rsi_1m.value().unwrap_or(50.0);
        if rsi < 20.0 {
            return Some((kline.close, "RSI极度超卖".to_string()));
        }
    }

    None
}

/// 入场信号检查
fn check_entry_v2(
    kline: &BacktestKline,
    state: &StateV2,
    config: &StrategyV2Config,
) -> Option<Direction> {
    let ema_fast_5m = state.ema_fast_5m.value().unwrap_or(0.0);
    let ema_slow_5m = state.ema_slow_5m.value().unwrap_or(0.0);
    let ema_fast_1m = state.ema_fast_1m.value().unwrap_or(0.0);
    let ema_slow_1m = state.ema_slow_1m.value().unwrap_or(0.0);
    let rsi = state.rsi_1m.value().unwrap_or(50.0);
    let vol_ratio = state.volume_ratio.ratio();

    let trend_up_5m = ema_fast_5m > ema_slow_5m;
    let trend_down_5m = ema_fast_5m < ema_slow_5m;
    // 趋势强度：EMA差值百分比（用于做空确认趋势足够强）
    let trend_strength_5m = if ema_slow_5m > 0.0 {
        ((ema_fast_5m - ema_slow_5m) / ema_slow_5m * 100.0).abs()
    } else {
        0.0
    };

    match config.strategy_type {
        StrategyType::TrendMomentum => {
            // 做多：5m上升趋势 + 买量占优
            if trend_up_5m && vol_ratio > config.volume_ratio_threshold {
                return Some(Direction::Long);
            }
            // 做空：5m下降趋势 + 卖量占优 + 趋势强度足够
            if config.allow_short
                && trend_down_5m
                && vol_ratio < (1.0 / config.volume_ratio_threshold)
                && trend_strength_5m > config.short_min_trend_strength
                && ema_fast_1m < ema_slow_1m  // 1m也确认下跌
            {
                return Some(Direction::Short);
            }
        }

        StrategyType::EmaCrossover => {
            // 做多：1m快线从下方穿越慢线（金叉）+ 5m趋势向上
            let crossed_up = state.prev_ema_fast_1m <= state.prev_ema_slow_1m
                && ema_fast_1m > ema_slow_1m;
            if crossed_up && trend_up_5m {
                return Some(Direction::Long);
            }
            // 做空：1m快线从上方穿越慢线（死叉）+ 5m趋势向下 + 趋势强度确认
            if config.allow_short {
                let crossed_down = state.prev_ema_fast_1m >= state.prev_ema_slow_1m
                    && ema_fast_1m < ema_slow_1m;
                if crossed_down && trend_down_5m && trend_strength_5m > config.short_min_trend_strength {
                    return Some(Direction::Short);
                }
            }
        }

        StrategyType::RsiBounce => {
            // 做多：RSI曾经超卖且已恢复 + 5m趋势向上 + 量确认
            if state.rsi_was_oversold
                && rsi > (config.rsi_oversold + 5.0)
                && trend_up_5m
                && vol_ratio > 1.0
            {
                return Some(Direction::Long);
            }
            // 做空：RSI曾经超买且已回落 + 5m趋势向下
            if config.allow_short
                && state.rsi_was_overbought
                && rsi < (config.rsi_overbought - 5.0)
                && trend_down_5m
                && vol_ratio < 1.0
            {
                return Some(Direction::Short);
            }
        }

        StrategyType::Breakout => {
            if state.recent_highs.len() < config.breakout_lookback {
                return None;
            }
            // 做多：价格突破最近N根K线最高价 + 量确认
            let recent_high = state.recent_highs[..state.recent_highs.len() - 1]
                .iter()
                .cloned()
                .fold(f64::NEG_INFINITY, f64::max);
            if kline.close > recent_high && vol_ratio > config.volume_ratio_threshold {
                return Some(Direction::Long);
            }
            // 做空：价格跌破最近N根K线最低价
            if config.allow_short {
                let recent_low = state.recent_lows[..state.recent_lows.len() - 1]
                    .iter()
                    .cloned()
                    .fold(f64::INFINITY, f64::min);
                if kline.close < recent_low && vol_ratio < (1.0 / config.volume_ratio_threshold) {
                    return Some(Direction::Short);
                }
            }
        }

        StrategyType::CompositeScore => {
            // 综合打分制：多个信号加权
            let mut long_score: f64 = 0.0;
            let mut short_score: f64 = 0.0;

            // 趋势得分 (30分)
            if trend_up_5m { long_score += 30.0; }
            if trend_down_5m { short_score += 30.0; }

            // 1m EMA方向 (20分)
            if ema_fast_1m > ema_slow_1m { long_score += 20.0; }
            if ema_fast_1m < ema_slow_1m { short_score += 20.0; }

            // RSI得分 (25分)
            if rsi < 40.0 { long_score += 25.0; } // 偏低，有上涨空间
            else if rsi < 50.0 { long_score += 15.0; }
            if rsi > 60.0 { short_score += 25.0; } // 偏高，有下跌风险
            else if rsi > 50.0 { short_score += 15.0; }

            // 成交量得分 (25分)
            if vol_ratio > 1.5 { long_score += 25.0; }
            else if vol_ratio > 1.2 { long_score += 15.0; }
            if vol_ratio < 0.67 { short_score += 25.0; }
            else if vol_ratio < 0.83 { short_score += 15.0; }

            if long_score >= config.min_entry_score {
                return Some(Direction::Long);
            }
            if config.allow_short && short_score >= config.min_entry_score {
                return Some(Direction::Short);
            }
        }
    }

    None
}
