//! 回测引擎核心
//!
//! 加载历史数据，按时间序列驱动策略指标，模拟交易并记录结果

use crate::backtest::data_loader::{BacktestKline, DataLoader, RecordedEvent};
use crate::backtest::report::{BacktestReport, TradeRecord};
use crate::config::StrategyConfig;
use crate::strategies::indicators::{VolumeRatio, ADX, ATR, EMA, RSI};

const DEFAULT_POSITION_ALLOCATION_PCT: f64 = 0.985;
const DEFAULT_MIN_USDT_RESERVE: f64 = 2.0;

fn round_backtest_quantity(quantity: f64, symbol: &str) -> f64 {
    let decimals: i32 = match symbol {
        "BTCUSDT" => 5,
        "ETHUSDT" => 4,
        "SOLUSDT" => 2,
        _ => 5,
    };
    let factor = 10_f64.powi(decimals);
    ((quantity * factor) + 1e-9).floor() / factor
}

fn dynamic_entry_quantity(
    available_capital: f64,
    entry_price: f64,
    strategy: &StrategyConfig,
    position_allocation_pct: f64,
    min_usdt_reserve: f64,
) -> f64 {
    if available_capital <= 0.0 || entry_price <= 0.0 {
        return 0.0;
    }

    let alloc_cap = available_capital * position_allocation_pct;
    let reserve_cap = (available_capital - min_usdt_reserve).max(0.0);
    let quantity_cap = strategy.quantity_per_trade * entry_price;
    let deployable = alloc_cap.min(reserve_cap).min(quantity_cap);
    round_backtest_quantity(deployable / entry_price, &strategy.symbol)
}

/// 回测引擎配置
#[derive(Debug, Clone)]
pub struct BacktestConfig {
    pub symbol: String,
    pub initial_capital: f64,
    pub strategy: StrategyConfig,
    pub commission_rate: f64, // 手续费率，默认0.001 (0.1%)
    pub position_allocation_pct: f64,
    pub min_usdt_reserve: f64,
    pub spread_pct: f64,   // 模拟买卖点差%(全额)，入场/出场各承担一半
    pub slippage_pct: f64, // 模拟市价单滑点%(单边)
}

impl BacktestConfig {
    /// 单边成交成本%(点差一半 + 滑点)，用于把理想成交价修正为实际成交均价
    pub fn cost_per_side_pct(&self) -> f64 {
        self.spread_pct / 2.0 + self.slippage_pct
    }
}

/// 内部持仓状态
#[derive(Debug, Clone, PartialEq)]
enum Position {
    None,
    Long,
    Short,
}

/// 回测引擎内部状态
struct EngineState {
    // 指标
    ema_fast_1m: EMA,
    ema_slow_1m: EMA,
    rsi_1m: RSI,
    ema_fast_5m: EMA,
    ema_slow_5m: EMA,
    ema_trend_5m: EMA, // EMA50 - 趋势环境过滤
    volume_ratio: VolumeRatio,

    // 盘口
    best_bid: f64,
    best_bid_qty: f64,
    best_ask: f64,
    best_ask_qty: f64,

    // 持仓
    position: Position,
    entry_price: f64,
    entry_quantity: f64,
    entry_time: u64,
    available_capital: f64,

    // 追踪止损状态
    highest_since_entry: f64, // 入场后最高价
    trailing_active: bool,    // 追踪止损是否激活
    breakeven_active: bool,   // 保本止损是否激活

    // 冷却与统计
    last_trade_time: u64,
    daily_trades: u32,
    daily_pnl: f64,
    last_day: u32,

    // RSI状态
    rsi_was_oversold: bool,
    rsi_oversold_bars: usize,   // 超卖标志已持续的K线数（过期机制）
    rsi_was_overbought: bool,   // 做空对称：1m RSI曾超买
    rsi_overbought_bars: usize, // 超买标志已持续的K线数（过期机制）

    // 突破入场状态
    recent_highs: Vec<f64>,  // 最近20根K线最高价缓冲区
    recent_lows: Vec<f64>,   // 最近20根K线最低价缓冲区
    lowest_since_entry: f64, // 入场后最低价（用于做空追踪止损）

    // EMA斜率跟踪
    prev_ema_slow_5m: f64,   // 上一桩5mK线的EMA21值
    prev_ema_trend_5m: f64,  // 上一桩5mK线的EMA50值（判断EMA50方向）
    ema50_history: Vec<f64>, // EMA50历史值缓冲（最近20根=100分钟）

    // 止损后延长冷却
    last_exit_was_stoploss: bool,

    // 分批止盈状态（收益端结构优化）
    partial_tp_done: bool, // 本笔是否已执行分批止盈

    // 订单流指标：CVD累计成交量差（主动买-主动卖）
    cvd: f64,
    cvd_history: Vec<f64>, // 最近60根K线的CVD值（算净变化）

    // 资金费率（合约情绪指标，8h一条，前向填充对齐）
    funding_rates: Vec<(u64, f64)>, // (时间ms, 费率)
    funding_idx: usize,
    current_funding: f64, // 当前生效的资金费率

    // ATR/ADX指标
    atr_5m: ATR,
    adx_5m: ADX,

    // 入场ATR快照（防止ATR收缩导致止损过早触发）
    entry_atr: f64,

    // 预热
    kline_1m_count: usize,
    kline_5m_count: usize,
}

impl EngineState {
    fn new(initial_capital: f64) -> Self {
        Self {
            ema_fast_1m: EMA::new(7),
            ema_slow_1m: EMA::new(21),
            rsi_1m: RSI::new(14),
            ema_fast_5m: EMA::new(7),
            ema_slow_5m: EMA::new(21),
            ema_trend_5m: EMA::new(50),
            volume_ratio: VolumeRatio::new(60),
            best_bid: 0.0,
            best_bid_qty: 0.0,
            best_ask: 0.0,
            best_ask_qty: 0.0,
            position: Position::None,
            entry_price: 0.0,
            entry_quantity: 0.0,
            entry_time: 0,
            available_capital: initial_capital,
            highest_since_entry: 0.0,
            trailing_active: false,
            breakeven_active: false,
            last_trade_time: 0,
            daily_trades: 0,
            daily_pnl: 0.0,
            last_day: 0,
            rsi_was_oversold: false,
            rsi_oversold_bars: 0,
            rsi_was_overbought: false,
            rsi_overbought_bars: 0,
            recent_highs: Vec::with_capacity(20),
            recent_lows: Vec::with_capacity(20),
            lowest_since_entry: f64::MAX,
            prev_ema_slow_5m: 0.0,
            prev_ema_trend_5m: 0.0,
            ema50_history: Vec::with_capacity(20),
            last_exit_was_stoploss: false,
            partial_tp_done: false,
            cvd: 0.0,
            cvd_history: Vec::with_capacity(64),
            funding_rates: Vec::new(),
            funding_idx: 0,
            current_funding: 0.0,
            atr_5m: ATR::new(14, 100),
            adx_5m: ADX::new(14),
            entry_atr: 0.0,
            kline_1m_count: 0,
            kline_5m_count: 0,
        }
    }

    fn is_warmed_up(&self) -> bool {
        self.kline_1m_count >= 21 && self.kline_5m_count >= 50
    }

    fn is_bearish_continuation(&self, lookback: usize) -> bool {
        if lookback < 2 || self.recent_highs.len() <= lookback || self.recent_lows.len() <= lookback
        {
            return false;
        }

        let high_start = self.recent_highs.len() - lookback - 1;
        let low_start = self.recent_lows.len() - lookback - 1;
        let highs = &self.recent_highs[high_start..];
        let lows = &self.recent_lows[low_start..];
        let lower_highs = highs.windows(2).filter(|pair| pair[1] < pair[0]).count();
        let lower_lows = lows.windows(2).filter(|pair| pair[1] < pair[0]).count();
        let required = lookback.saturating_sub(1).max(1);

        lower_highs >= required && lower_lows >= required
    }

    fn has_bullish_reversal_confirmation(
        &self,
        current_price: f64,
        lookback: usize,
        min_break_pct: f64,
    ) -> bool {
        if lookback == 0 || self.recent_highs.len() < lookback {
            return false;
        }

        let start = self.recent_highs.len() - lookback;
        let recent_high = self.recent_highs[start..]
            .iter()
            .copied()
            .fold(f64::NEG_INFINITY, f64::max);

        recent_high.is_finite() && current_price > recent_high * (1.0 + min_break_pct / 100.0)
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

/// 回测引擎
pub struct BacktestEngine {
    config: BacktestConfig,
    data_loader: DataLoader,
}

impl BacktestEngine {
    pub fn new(config: BacktestConfig, data_dir: &str) -> Self {
        Self {
            config,
            data_loader: DataLoader::new(data_dir),
        }
    }

    /// 执行K线模式回测
    pub fn run_kline_backtest(&self) -> Result<BacktestReport, crate::error::DomainError> {
        let symbol = &self.config.symbol;

        // 加载1m和5m数据
        let klines_1m = self.data_loader.load_klines(symbol, "1m")?;
        let klines_5m = self.data_loader.load_klines(symbol, "5m")?;

        println!("\n🔄 开始K线回测...");
        println!("   1m K线: {} 根", klines_1m.len());
        println!("   5m K线: {} 根", klines_5m.len());

        // 合并并按时间排序
        let mut events: Vec<(&BacktestKline, bool)> = Vec::new(); // (kline, is_1m)
        for k in &klines_1m {
            events.push((k, true));
        }
        for k in &klines_5m {
            events.push((k, false));
        }
        events.sort_by_key(|(k, _)| k.close_time);

        let start_time = events.first().map(|(k, _)| k.open_time).unwrap_or(0);
        let end_time = events.last().map(|(k, _)| k.close_time).unwrap_or(0);

        let mut state = EngineState::new(self.config.initial_capital);
        // 资金费率数据（存在则加载，缺失则空向量=不过滤）
        state.funding_rates = self
            .data_loader
            .load_funding_rates(symbol)
            .unwrap_or_default();
        if self.config.strategy.use_funding_filter {
            println!("   资金费率: {} 条（过滤开启）", state.funding_rates.len());
        }
        let mut trades: Vec<TradeRecord> = Vec::new();
        let mut trade_id = 0;

        for (kline, is_1m) in &events {
            if kline.close <= 0.0 {
                continue;
            }

            let timestamp = kline.close_time;

            // 资金费率前向填充：推进到最近一条已结算费率
            while state.funding_idx < state.funding_rates.len()
                && state.funding_rates[state.funding_idx].0 <= timestamp
            {
                state.current_funding = state.funding_rates[state.funding_idx].1;
                state.funding_idx += 1;
            }

            if *is_1m {
                state.kline_1m_count += 1;
                state.ema_fast_1m.update(kline.close);
                state.ema_slow_1m.update(kline.close);
                let rsi = state.rsi_1m.update(kline.close);

                if rsi < self.config.strategy.rsi_oversold {
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

                // 做空对称：超买标志跟踪（同样30根过期，用做空独立阈值）
                if rsi > self.config.strategy.short_rsi_overbought {
                    state.rsi_was_overbought = true;
                    state.rsi_overbought_bars = 0;
                } else if state.rsi_was_overbought {
                    state.rsi_overbought_bars += 1;
                    if state.rsi_overbought_bars > 30 {
                        state.rsi_was_overbought = false;
                        state.rsi_overbought_bars = 0;
                    }
                }

                // 用K线收盘价模拟盘口（K线模式近似）
                state.best_bid = kline.close;
                state.best_ask = kline.close;
                // 使用真实的taker buy volume来模拟盘口深度
                let buy_vol = kline.taker_buy_volume;
                let sell_vol = (kline.volume - buy_vol).max(0.0); // 防止数据异常导致负值
                state.best_bid_qty = buy_vol;
                state.best_ask_qty = sell_vol;

                // 使用真实的taker buy volume计算买卖比
                // is_buyer_maker=false 表示买方主动成交（taker buy）
                state.volume_ratio.add_trade(timestamp, buy_vol, false);
                // is_buyer_maker=true 表示卖方主动成交
                state.volume_ratio.add_trade(timestamp, sell_vol, true);

                // CVD累计成交量差：delta = 主动买量 - 主动卖量 = 2×taker_buy - 总量
                let cvd_delta = 2.0 * buy_vol - kline.volume;
                state.cvd += cvd_delta;
                state.cvd_history.push(state.cvd);
                if state.cvd_history.len() > 60 {
                    state.cvd_history.remove(0);
                }

                // 更新最近20根K线最高价缓冲区
                state.recent_highs.push(kline.high);
                if state.recent_highs.len() > 20 {
                    state.recent_highs.remove(0);
                }
                state.recent_lows.push(kline.low);
                if state.recent_lows.len() > 20 {
                    state.recent_lows.remove(0);
                }
            } else {
                state.kline_5m_count += 1;
                state.prev_ema_slow_5m = state.ema_slow_5m.value().unwrap_or(0.0);
                state.prev_ema_trend_5m = state.ema_trend_5m.value().unwrap_or(0.0);
                state.ema_fast_5m.update(kline.close);
                state.ema_slow_5m.update(kline.close);
                state.ema_trend_5m.update(kline.close);
                // ATR和ADX更新（使用K线的high/low/close）
                state.atr_5m.update(kline.high, kline.low, kline.close);
                state.adx_5m.update(kline.high, kline.low, kline.close);
                // 记录EMA50历史值（保留最近20根=100分钟用于宏观方向判断）
                let new_ema50 = state.ema_trend_5m.value().unwrap_or(0.0);
                state.ema50_history.push(new_ema50);
                if state.ema50_history.len() > 20 {
                    state.ema50_history.remove(0);
                }
                continue; // 5m只更新指标不触发交易
            }

            state.check_daily_reset(timestamp);

            if !state.is_warmed_up() {
                continue;
            }

            // 检查出场（阶梯式保护机制 + Intra-bar模拟 + ATR动态止损）
            if state.position == Position::Long {
                let hold_secs = (timestamp - state.entry_time) / 1000;
                let rsi = state.rsi_1m.value().unwrap_or(50.0);

                // 用K线最高价更新入场后最高价（价格确实到达过该点）
                state.highest_since_entry = state.highest_since_entry.max(kline.high);
                let highest_pnl_pct =
                    (state.highest_since_entry - state.entry_price) / state.entry_price * 100.0;

                // === 分批止盈：触达第一目标位卖出部分仓位锁定盈利，剩余仓位保本继续奔跑 ===
                if !state.partial_tp_done
                    && self.config.strategy.partial_take_profit_pct > 0.0
                    && self.config.strategy.partial_take_profit_ratio > 0.0
                {
                    let partial_tp_price = state.entry_price
                        * (1.0 + self.config.strategy.partial_take_profit_pct / 100.0);
                    if kline.high >= partial_tp_price {
                        let sell_qty =
                            state.entry_quantity * self.config.strategy.partial_take_profit_ratio;
                        if sell_qty > 0.0 {
                            let exit_price =
                                partial_tp_price * (1.0 - self.config.cost_per_side_pct() / 100.0);
                            let pnl_pct =
                                (exit_price - state.entry_price) / state.entry_price * 100.0;
                            let pnl_usdt = sell_qty * state.entry_price * pnl_pct / 100.0;
                            let commission = sell_qty
                                * (state.entry_price + exit_price)
                                * self.config.commission_rate;
                            trade_id += 1;
                            trades.push(TradeRecord {
                                id: trade_id,
                                entry_time: state.entry_time,
                                exit_time: timestamp,
                                entry_price: state.entry_price,
                                exit_price,
                                quantity: sell_qty,
                                pnl_pct,
                                pnl_usdt,
                                commission,
                                hold_seconds: hold_secs,
                                exit_reason: "分批止盈".to_string(),
                            });
                            state.entry_quantity -= sell_qty;
                            state.available_capital += pnl_usdt - commission;
                            state.daily_pnl += pnl_pct;
                            state.partial_tp_done = true;
                            state.breakeven_active = true; // 剩余仓位保本（整笔已锁定盈利）
                        }
                    }
                }

                // ATR动态止损计算（使用max(当前ATR, 入场ATR)防止收缩）
                let current_atr = state.atr_5m.value().unwrap_or(0.0);
                let effective_atr = current_atr.max(state.entry_atr);
                let use_atr = self.config.strategy.use_atr_stops && effective_atr > 0.0;

                // 计算动态止损价
                let dynamic_sl = if use_atr {
                    // ATR基础止损价
                    let atr_sl = state.entry_price
                        - self.config.strategy.atr_stop_multiplier * effective_atr;
                    // ATR追踪止损：浮盈超过 N*ATR 后开启
                    let atr_trailing_trigger =
                        self.config.strategy.atr_trailing_multiplier * effective_atr;
                    let atr_trailing_dist =
                        self.config.strategy.atr_trailing_distance * effective_atr;
                    let profit_amount = state.highest_since_entry - state.entry_price;
                    if profit_amount >= atr_trailing_trigger {
                        state.trailing_active = true;
                        state.highest_since_entry - atr_trailing_dist
                    } else if state.breakeven_active
                        || highest_pnl_pct >= self.config.strategy.breakeven_trigger_pct
                    {
                        state.breakeven_active = true;
                        state.entry_price
                    } else {
                        atr_sl
                    }
                } else {
                    // 回退到固定%止损
                    if highest_pnl_pct >= self.config.strategy.trailing_trigger_pct {
                        state.trailing_active = true;
                        state.highest_since_entry
                            * (1.0 - self.config.strategy.trailing_distance_pct / 100.0)
                    } else if state.breakeven_active
                        || highest_pnl_pct >= self.config.strategy.breakeven_trigger_pct
                    {
                        state.breakeven_active = true;
                        state.entry_price
                    } else {
                        state.entry_price * (1.0 - self.config.strategy.stop_loss_pct / 100.0)
                    }
                };

                let tp_price =
                    state.entry_price * (1.0 + self.config.strategy.take_profit_pct / 100.0);

                // Intra-bar: 用low检测SL触发，用high检测TP触发
                let sl_hit = kline.low <= dynamic_sl;
                let tp_hit = kline.high >= tp_price;
                let timeout = hold_secs >= self.config.strategy.max_hold_seconds;
                let rsi_exit = rsi > self.config.strategy.rsi_overbought;
                let current_pnl = (kline.close - state.entry_price) / state.entry_price * 100.0;
                let stale_exit = hold_secs >= self.config.strategy.stale_exit_seconds
                    && current_pnl < self.config.strategy.stale_pnl_threshold_pct
                    && !state.trailing_active;

                // 趋势破坏提前认亏：浮亏且5m EMA死叉+跌破EMA50，立即出场不等硬止损
                let ema_fast_val = state.ema_fast_5m.value().unwrap_or(0.0);
                let ema_slow_val = state.ema_slow_5m.value().unwrap_or(0.0);
                let ema_trend_val = state.ema_trend_5m.value().unwrap_or(0.0);
                let trend_break = self.config.strategy.trend_break_exit
                    && current_pnl < -0.3
                    && !state.trailing_active
                    && !state.partial_tp_done
                    && ema_trend_val > 0.0
                    && ema_fast_val < ema_slow_val
                    && kline.close < ema_trend_val;

                let should_exit =
                    sl_hit || tp_hit || timeout || rsi_exit || stale_exit || trend_break;

                if should_exit {
                    // 确定出场价和原因（SL优先于TP，保守估计）
                    let (exit_price, reason) = if sl_hit && !tp_hit {
                        let r = if state.trailing_active {
                            "追踪止损"
                        } else if state.breakeven_active {
                            "保本止损"
                        } else {
                            "止损"
                        };
                        (dynamic_sl, r)
                    } else if tp_hit && !sl_hit {
                        (tp_price, "硬止盈")
                    } else if sl_hit && tp_hit {
                        // 同一根K线内同时触发，保守假设止损先触发
                        let r = if state.trailing_active {
                            "追踪止损"
                        } else if state.breakeven_active {
                            "保本止损"
                        } else {
                            "止损"
                        };
                        (dynamic_sl, r)
                    } else if trend_break {
                        (kline.close, "趋势破坏")
                    } else if stale_exit {
                        (kline.close, "僵尸早退")
                    } else if timeout {
                        (kline.close, "超时")
                    } else {
                        (kline.close, "RSI超买")
                    };

                    // 滑点/点差成本：卖出实际成交均价低于理想价
                    let exit_price = exit_price * (1.0 - self.config.cost_per_side_pct() / 100.0);
                    let pnl_pct = (exit_price - state.entry_price) / state.entry_price * 100.0;
                    let quantity = state.entry_quantity;
                    let pnl_usdt = quantity * state.entry_price * pnl_pct / 100.0;
                    let commission =
                        quantity * (state.entry_price + exit_price) * self.config.commission_rate;

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
                        exit_reason: reason.to_string(),
                    });

                    state.position = Position::None;
                    state.entry_quantity = 0.0;
                    state.available_capital += pnl_usdt - commission;
                    state.daily_pnl += pnl_pct;
                    state.last_trade_time = timestamp;
                    state.last_exit_was_stoploss =
                        reason == "止损" || reason == "超时" || reason == "趋势破坏";
                }
            }

            // 做空出场（对称的阶梯式保护 + ATR动态止损）
            if state.position == Position::Short {
                let hold_secs = (timestamp - state.entry_time) / 1000;

                state.lowest_since_entry = state.lowest_since_entry.min(kline.low);
                let lowest_pnl_pct =
                    (state.entry_price - state.lowest_since_entry) / state.entry_price * 100.0;

                // === 做空分批止盈（对称）：触达第一目标位买回部分仓位锁定盈利 ===
                if !state.partial_tp_done
                    && self.config.strategy.partial_take_profit_pct > 0.0
                    && self.config.strategy.partial_take_profit_ratio > 0.0
                {
                    let partial_tp_price = state.entry_price
                        * (1.0 - self.config.strategy.partial_take_profit_pct / 100.0);
                    if kline.low <= partial_tp_price {
                        let buy_qty =
                            state.entry_quantity * self.config.strategy.partial_take_profit_ratio;
                        if buy_qty > 0.0 {
                            let exit_price =
                                partial_tp_price * (1.0 + self.config.cost_per_side_pct() / 100.0);
                            let pnl_pct =
                                (state.entry_price - exit_price) / state.entry_price * 100.0;
                            let pnl_usdt = buy_qty * state.entry_price * pnl_pct / 100.0;
                            let commission = buy_qty
                                * (state.entry_price + exit_price)
                                * self.config.commission_rate;
                            trade_id += 1;
                            trades.push(TradeRecord {
                                id: trade_id,
                                entry_time: state.entry_time,
                                exit_time: timestamp,
                                entry_price: state.entry_price,
                                exit_price,
                                quantity: buy_qty,
                                pnl_pct,
                                pnl_usdt,
                                commission,
                                hold_seconds: hold_secs,
                                exit_reason: "空分批止盈".to_string(),
                            });
                            state.entry_quantity -= buy_qty;
                            state.available_capital += pnl_usdt - commission;
                            state.daily_pnl += pnl_pct;
                            state.partial_tp_done = true;
                            state.breakeven_active = true;
                        }
                    }
                }

                let current_atr = state.atr_5m.value().unwrap_or(0.0);
                let effective_atr = current_atr.max(state.entry_atr);
                let use_atr = self.config.strategy.use_atr_stops && effective_atr > 0.0;

                let dynamic_sl = if use_atr {
                    let atr_sl = state.entry_price
                        + self.config.strategy.atr_stop_multiplier * effective_atr;
                    let atr_trailing_trigger =
                        self.config.strategy.atr_trailing_multiplier * effective_atr;
                    let atr_trailing_dist =
                        self.config.strategy.atr_trailing_distance * effective_atr;
                    let profit_amount = state.entry_price - state.lowest_since_entry;
                    if profit_amount >= atr_trailing_trigger {
                        state.trailing_active = true;
                        state.lowest_since_entry + atr_trailing_dist
                    } else if state.breakeven_active
                        || lowest_pnl_pct >= self.config.strategy.breakeven_trigger_pct
                    {
                        state.breakeven_active = true;
                        state.entry_price
                    } else {
                        atr_sl
                    }
                } else if lowest_pnl_pct >= self.config.strategy.trailing_trigger_pct {
                    state.trailing_active = true;
                    state.lowest_since_entry
                        * (1.0 + self.config.strategy.trailing_distance_pct / 100.0)
                } else if state.breakeven_active
                    || lowest_pnl_pct >= self.config.strategy.breakeven_trigger_pct
                {
                    state.breakeven_active = true;
                    state.entry_price
                } else {
                    state.entry_price * (1.0 + self.config.strategy.stop_loss_pct / 100.0)
                };

                let tp_price =
                    state.entry_price * (1.0 - self.config.strategy.take_profit_pct / 100.0);

                let sl_hit = kline.high >= dynamic_sl;
                let tp_hit = kline.low <= tp_price;
                let timeout = hold_secs >= self.config.strategy.max_hold_seconds;
                let current_pnl = (state.entry_price - kline.close) / state.entry_price * 100.0;
                let stale_exit = hold_secs >= self.config.strategy.stale_exit_seconds
                    && current_pnl < self.config.strategy.stale_pnl_threshold_pct
                    && !state.trailing_active;

                // 做空趋势破坏提前认亏（对称）：浮亏且5m EMA金叉+站上EMA50
                let ema_fast_val_s = state.ema_fast_5m.value().unwrap_or(0.0);
                let ema_slow_val_s = state.ema_slow_5m.value().unwrap_or(0.0);
                let ema_trend_val_s = state.ema_trend_5m.value().unwrap_or(0.0);
                let trend_break = self.config.strategy.trend_break_exit
                    && current_pnl < -0.3
                    && !state.trailing_active
                    && !state.partial_tp_done
                    && ema_trend_val_s > 0.0
                    && ema_fast_val_s > ema_slow_val_s
                    && kline.close > ema_trend_val_s;

                let should_exit = sl_hit || tp_hit || timeout || stale_exit || trend_break;

                if should_exit {
                    let (exit_price, reason) = if sl_hit && !tp_hit {
                        let r = if state.trailing_active {
                            "空追踪止损"
                        } else if state.breakeven_active {
                            "空保本止损"
                        } else {
                            "空止损"
                        };
                        (dynamic_sl, r)
                    } else if tp_hit && !sl_hit {
                        (tp_price, "空止盈")
                    } else if sl_hit && tp_hit {
                        (dynamic_sl, "空止损")
                    } else if trend_break {
                        (kline.close, "空趋势破坏")
                    } else if stale_exit {
                        (kline.close, "空僵尸早退")
                    } else {
                        (kline.close, "空超时")
                    };

                    // 滑点/点差成本：买入平空实际成交均价高于理想价
                    let exit_price = exit_price * (1.0 + self.config.cost_per_side_pct() / 100.0);
                    let pnl_pct = (state.entry_price - exit_price) / state.entry_price * 100.0;
                    let quantity = state.entry_quantity;
                    let pnl_usdt = quantity * state.entry_price * pnl_pct / 100.0;
                    let commission =
                        quantity * (state.entry_price + exit_price) * self.config.commission_rate;

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
                        exit_reason: reason.to_string(),
                    });
                    state.position = Position::None;
                    state.entry_quantity = 0.0;
                    state.available_capital += pnl_usdt - commission;
                    state.daily_pnl += pnl_pct;
                    state.last_trade_time = timestamp;
                    state.last_exit_was_stoploss =
                        reason == "空止损" || reason == "空超时" || reason == "空趋势破坏";
                }
            }

            // 检查入场（多因子评分 + ADX过滤 + 波动率政权）
            if state.position == Position::None {
                // 止损后延长冷却：如果上一笔是止损出场，使用更长的冷却时间
                let effective_cooldown = if state.last_exit_was_stoploss
                    && self.config.strategy.post_stoploss_cooldown_seconds > 0
                {
                    self.config.strategy.post_stoploss_cooldown_seconds * 1000
                } else {
                    self.config.strategy.cooldown_seconds * 1000
                };
                if timestamp - state.last_trade_time < effective_cooldown {
                    continue;
                }
                if state.daily_trades >= self.config.strategy.max_daily_trades {
                    continue;
                }
                if state.daily_pnl <= -self.config.strategy.max_daily_loss_pct {
                    continue;
                }

                // === ADX过滤 ===
                let adx_val = state.adx_5m.value().unwrap_or(0.0);
                let adx_ready = state.adx_5m.is_ready();
                let plus_di = state.adx_5m.plus_di();
                let minus_di = state.adx_5m.minus_di();
                let adx_above_threshold =
                    !adx_ready || adx_val >= self.config.strategy.adx_min_threshold;
                let long_di_direction_ok = !adx_ready
                    || (plus_di > minus_di
                        && (plus_di - minus_di) >= self.config.strategy.min_adx_di_diff);
                let short_di_direction_ok = !adx_ready
                    || (minus_di > plus_di
                        && (minus_di - plus_di) >= self.config.strategy.min_adx_di_diff);

                // === 波动率政权过滤 ===
                let atr_pct = state.atr_5m.percentile();
                let volatility_normal = match atr_pct {
                    Some(p) => {
                        p >= self.config.strategy.atr_percentile_low
                            && p <= self.config.strategy.atr_percentile_high
                    }
                    None => true, // 数据不足时不过滤
                };

                let ema_fast_5m = state.ema_fast_5m.value().unwrap_or(0.0);
                let ema_slow_5m = state.ema_slow_5m.value().unwrap_or(0.0);
                let ema_trend = state.ema_trend_5m.value().unwrap_or(0.0);
                let rsi = state.rsi_1m.value().unwrap_or(50.0);
                let vol_ratio = state.volume_ratio.ratio();

                // EMA21斜率：判断中期趋势动能方向
                let ema21_slope = if state.prev_ema_slow_5m > 0.0 {
                    (ema_slow_5m - state.prev_ema_slow_5m) / state.prev_ema_slow_5m * 100.0
                } else {
                    0.0
                };

                // EMA50宏观方向
                let ema50_macro_rising = if state.ema50_history.len() >= 20 {
                    ema_trend > state.ema50_history[0]
                } else {
                    false
                };

                // === 趋势环境过滤（L2层）===
                let price_above_ema50_pct = if ema_trend > 0.0 {
                    (kline.close - ema_trend) / ema_trend * 100.0
                } else {
                    0.0
                };
                let long_structure_ok = !state
                    .is_bearish_continuation(self.config.strategy.downtrend_filter_lookback)
                    || state.has_bullish_reversal_confirmation(
                        kline.close,
                        self.config.strategy.downtrend_filter_lookback,
                        self.config.strategy.min_reversal_break_pct,
                    );

                let long_trend_env_ok = ema_trend > 0.0
                    && price_above_ema50_pct > self.config.strategy.min_price_above_ema50_pct
                    && price_above_ema50_pct < self.config.strategy.max_ema50_distance_pct
                    && ema21_slope > self.config.strategy.mean_revert_min_slope
                    && ema50_macro_rising
                    && long_di_direction_ok
                    && long_structure_ok;

                let trend_up = ema_fast_5m > ema_slow_5m;
                let trend_strength = if ema_slow_5m > 0.0 {
                    (ema_fast_5m - ema_slow_5m) / ema_slow_5m * 100.0
                } else {
                    0.0
                };
                let trend_strong_enough =
                    trend_strength >= self.config.strategy.min_trend_strength_pct;
                let rsi_recovering = state.rsi_was_oversold
                    && rsi > (self.config.strategy.rsi_oversold + 5.0)
                    && rsi < self.config.strategy.rsi_overbought;
                let rsi_bounce_vr_ok = vol_ratio > self.config.strategy.volume_ratio_threshold;
                // 盘口支撑：K线回测无法精确模拟实盘订单簿，默认不作为硬条件（配置可开启）
                let bid_support_raw = state.best_bid_qty > state.best_ask_qty * 1.5;
                let bid_support_ok = bid_support_raw || !self.config.strategy.require_bid_support;
                let slope_positive = ema21_slope > 0.03;

                // 突破入场条件
                let breakout_signal = if state.recent_highs.len() >= 10 {
                    let lookback_high = state.recent_highs[..state.recent_highs.len() - 1]
                        .iter()
                        .copied()
                        .fold(f64::NEG_INFINITY, f64::max);
                    let breakout_pct = (kline.close - lookback_high) / lookback_high * 100.0;
                    breakout_pct > 0.05
                        && vol_ratio > self.config.strategy.volume_ratio_threshold
                        && rsi < self.config.strategy.breakout_max_rsi
                } else {
                    false
                };

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
                if vol_ratio > self.config.strategy.volume_ratio_threshold {
                    entry_score += 15;
                }
                if bid_support_raw {
                    entry_score += 10;
                }
                if slope_positive {
                    entry_score += 10;
                }
                if volatility_normal {
                    entry_score += 10;
                }

                // 突破路径额外要求斜率>配置值
                let breakout_slope_ok = ema21_slope > self.config.strategy.breakout_min_slope;
                let rsi_bounce_not_late = rsi < self.config.strategy.rsi_bounce_max_rsi;

                // 趋势入场：评分达标 + 趋势环境确认
                let trend_entry_signal = long_trend_env_ok
                    && entry_score >= self.config.strategy.entry_score_threshold
                    && ((trend_up
                        && trend_strong_enough
                        && rsi_recovering
                        && rsi_bounce_vr_ok
                        && bid_support_ok
                        && rsi_bounce_not_late)
                        || (breakout_signal
                            && trend_up
                            && trend_strong_enough
                            && breakout_slope_ok));

                // === 均值回归入场信号（超跌反弹，不需要趋势确认） ===
                let mean_revert_signal = if state.recent_highs.len() >= 20 {
                    let recent_high = state
                        .recent_highs
                        .iter()
                        .copied()
                        .fold(f64::NEG_INFINITY, f64::max);
                    let drop_pct = (recent_high - kline.close) / recent_high * 100.0;
                    // 深跌反弹允许短暂跌破EMA50（原“必须仍在EMA50上方”与深跌条件自相矛盾，配置可恢复）
                    let above_ema50_ok = !self.config.strategy.mean_revert_require_above_ema50
                        || kline.close >= ema_trend;
                    drop_pct > self.config.strategy.mean_revert_min_drop_pct
                        && rsi < self.config.strategy.rsi_oversold
                        && vol_ratio > self.config.strategy.volume_ratio_threshold
                        && bid_support_ok
                        && ema21_slope > self.config.strategy.mean_revert_min_slope
                        && ema_trend > 0.0
                        && above_ema50_ok
                        && ema50_macro_rising
                        && long_di_direction_ok
                        && long_structure_ok
                        && volatility_normal // 波动率过滤也应用于均值回归
                } else {
                    false
                };

                // CVD确认过滤：近N根K线主动买盘净主导（CVD净上升）才允许入场
                let cvd_ok = !self.config.strategy.use_cvd_filter || {
                    let lookback = self.config.strategy.cvd_lookback;
                    state.cvd_history.len() > lookback
                        && state.cvd > state.cvd_history[state.cvd_history.len() - 1 - lookback]
                };

                // 资金费率过滤：费率过热（多头拥挤付费）时禁止做多入场
                let funding_ok = !self.config.strategy.use_funding_filter
                    || state.current_funding <= self.config.strategy.funding_long_block_pct / 100.0;

                let entry_signal =
                    (trend_entry_signal || mean_revert_signal) && cvd_ok && funding_ok;

                if entry_signal {
                    let entry_quantity = dynamic_entry_quantity(
                        state.available_capital,
                        kline.close,
                        &self.config.strategy,
                        self.config.position_allocation_pct,
                        self.config.min_usdt_reserve,
                    );
                    if entry_quantity <= 0.0 {
                        continue;
                    }
                    state.position = Position::Long;
                    // 滑点/点差成本：买入实际成交均价高于理想价
                    state.entry_price =
                        kline.close * (1.0 + self.config.cost_per_side_pct() / 100.0);
                    state.entry_quantity = entry_quantity;
                    state.entry_time = timestamp;
                    state.highest_since_entry = state.entry_price;
                    state.trailing_active = false;
                    state.breakeven_active = false;
                    state.partial_tp_done = false;
                    state.rsi_was_oversold = false;
                    state.rsi_oversold_bars = 0;
                    state.last_trade_time = timestamp;
                    state.daily_trades += 1;
                    state.entry_atr = state.atr_5m.value().unwrap_or(0.0);
                } else if self.config.strategy.allow_short {
                    // 做空入场（与做多对称：超买回落实路径 + 跌破路径）
                    let price_below_ema50_pct = if ema_trend > 0.0 {
                        (ema_trend - kline.close) / ema_trend * 100.0
                    } else {
                        0.0
                    };
                    // EMA50宏观下降（与多头ema50_macro_rising对称）
                    let ema50_macro_falling = if state.ema50_history.len() >= 20 {
                        ema_trend < state.ema50_history[0]
                    } else {
                        false
                    };
                    let short_trend_env_ok = ema_trend > 0.0
                        && price_below_ema50_pct > 0.15  // 至少低于EMA50 0.15%
                        && ema21_slope < -0.03 // 斜率<-0.03% 确认下降动能
                        && ema50_macro_falling;

                    let trend_down = ema_fast_5m < ema_slow_5m;
                    let short_trend_strength = if ema_slow_5m > 0.0 {
                        (ema_slow_5m - ema_fast_5m) / ema_slow_5m * 100.0
                    } else {
                        0.0
                    };
                    let short_trend_strong =
                        short_trend_strength >= self.config.strategy.min_trend_strength_pct;
                    let breakdown_signal = if state.recent_lows.len() >= 10 {
                        let lookback_low = state.recent_lows[..state.recent_lows.len() - 1]
                            .iter()
                            .copied()
                            .fold(f64::INFINITY, f64::min);
                        let breakdown_pct = (lookback_low - kline.close) / lookback_low * 100.0;
                        breakdown_pct > 0.05
                            && vol_ratio < (1.0 / self.config.strategy.volume_ratio_threshold)
                            && rsi > 40.0
                    } else {
                        false
                    };

                    // 与多头rsi_recovering对称：曾超买且回落（不再要求当前RSI>70，
                    // 旧条件在下降趋势中几乎不可能满足，180天仅触发2笔）
                    let rsi_falling_from_ob = state.rsi_was_overbought
                        && rsi < (self.config.strategy.short_rsi_overbought - 5.0)
                        && rsi > self.config.strategy.rsi_oversold;
                    let sell_pressure =
                        vol_ratio < (1.0 / self.config.strategy.volume_ratio_threshold);

                    // 资金费率做空过滤（与多头对称的逆向逻辑）：费率为负=空头拥挤付费，
                    // 不做空（容易被轧空）；只在费率 >= -阈值 时允许做空
                    let short_funding_ok = !self.config.strategy.use_funding_filter
                        || state.current_funding
                            >= -self.config.strategy.funding_long_block_pct / 100.0;

                    let short_signal = short_trend_env_ok
                        && adx_above_threshold
                        && short_di_direction_ok
                        && short_funding_ok
                        && ((breakdown_signal && trend_down && short_trend_strong)
                            || (trend_down
                                && short_trend_strong
                                && rsi_falling_from_ob
                                && sell_pressure));

                    if short_signal {
                        let entry_quantity = dynamic_entry_quantity(
                            state.available_capital,
                            kline.close,
                            &self.config.strategy,
                            self.config.position_allocation_pct,
                            self.config.min_usdt_reserve,
                        );
                        if entry_quantity <= 0.0 {
                            continue;
                        }
                        state.position = Position::Short;
                        // 滑点/点差成本：做空卖出开仓实际成交均价低于理想价
                        state.entry_price =
                            kline.close * (1.0 - self.config.cost_per_side_pct() / 100.0);
                        state.entry_quantity = entry_quantity;
                        state.entry_time = timestamp;
                        state.lowest_since_entry = state.entry_price;
                        state.trailing_active = false;
                        state.breakeven_active = false;
                        state.rsi_was_oversold = false;
                        state.rsi_oversold_bars = 0;
                        state.rsi_was_overbought = false;
                        state.rsi_overbought_bars = 0;
                        state.last_trade_time = timestamp;
                        state.daily_trades += 1;
                        state.entry_atr = state.atr_5m.value().unwrap_or(0.0);
                    }
                }
            }
        }

        // 如果回测结束时还有持仓，强制平仓
        if state.position == Position::Long || state.position == Position::Short {
            if let Some((last_kline, _)) = events.last() {
                // 强制平仓同样承担滑点/点差成本
                let current_price = if state.position == Position::Long {
                    last_kline.close * (1.0 - self.config.cost_per_side_pct() / 100.0)
                } else {
                    last_kline.close * (1.0 + self.config.cost_per_side_pct() / 100.0)
                };
                let pnl_pct = if state.position == Position::Long {
                    (current_price - state.entry_price) / state.entry_price * 100.0
                } else {
                    (state.entry_price - current_price) / state.entry_price * 100.0
                };
                let hold_secs = (last_kline.close_time - state.entry_time) / 1000;
                let quantity = state.entry_quantity;
                let pnl_usdt = quantity * state.entry_price * pnl_pct / 100.0;
                let commission =
                    quantity * (state.entry_price + current_price) * self.config.commission_rate;

                trade_id += 1;
                trades.push(TradeRecord {
                    id: trade_id,
                    entry_time: state.entry_time,
                    exit_time: last_kline.close_time,
                    entry_price: state.entry_price,
                    exit_price: current_price,
                    quantity,
                    pnl_pct,
                    pnl_usdt,
                    commission,
                    hold_seconds: hold_secs,
                    exit_reason: "回测结束".to_string(),
                });
            }
        }

        println!("✅ 回测完成: {} 笔交易", trades.len());

        Ok(BacktestReport::generate(
            trades,
            self.config.initial_capital,
            start_time,
            end_time,
        ))
    }

    /// 执行录制数据回放回测
    pub fn run_replay_backtest(
        &self,
        file_path: &str,
    ) -> Result<BacktestReport, crate::error::DomainError> {
        let events = self.data_loader.load_recorded_events(file_path)?;

        println!("\n🔄 开始回放回测...");
        println!("   事件数: {}", events.len());

        let mut state = EngineState::new(self.config.initial_capital);
        let mut trades: Vec<TradeRecord> = Vec::new();
        let mut trade_id = 0;
        let mut start_time = 0u64;
        let mut end_time = 0u64;

        for event in &events {
            match event {
                RecordedEvent::Kline(kline) => {
                    if kline.close <= 0.0 {
                        continue;
                    }

                    let ts = kline.close_time;
                    if start_time == 0 {
                        start_time = ts;
                    }
                    end_time = ts;

                    match kline.interval.as_str() {
                        "1m" => {
                            state.kline_1m_count += 1;
                            state.ema_fast_1m.update(kline.close);
                            state.ema_slow_1m.update(kline.close);
                            let rsi = state.rsi_1m.update(kline.close);
                            if rsi < self.config.strategy.rsi_oversold {
                                state.rsi_was_oversold = true;
                                state.rsi_oversold_bars = 0;
                            } else if state.rsi_was_oversold {
                                state.rsi_oversold_bars += 1;
                                if state.rsi_oversold_bars > 30 {
                                    state.rsi_was_oversold = false;
                                    state.rsi_oversold_bars = 0;
                                }
                            }
                            // 更新最近20根K线最高/最低价缓冲区
                            state.recent_highs.push(kline.high);
                            if state.recent_highs.len() > 20 {
                                state.recent_highs.remove(0);
                            }
                            state.recent_lows.push(kline.low);
                            if state.recent_lows.len() > 20 {
                                state.recent_lows.remove(0);
                            }
                        }
                        "5m" => {
                            state.kline_5m_count += 1;
                            state.ema_fast_5m.update(kline.close);
                            state.ema_slow_5m.update(kline.close);
                        }
                        _ => {}
                    }
                }
                RecordedEvent::AggTrade {
                    quantity,
                    is_buyer_maker,
                    timestamp,
                    ..
                } => {
                    if *quantity <= 0.0 {
                        continue;
                    }
                    state
                        .volume_ratio
                        .add_trade(*timestamp, *quantity, *is_buyer_maker);
                    if start_time == 0 {
                        start_time = *timestamp;
                    }
                    end_time = *timestamp;
                }
                RecordedEvent::BookTicker {
                    best_bid,
                    best_bid_qty,
                    best_ask,
                    best_ask_qty,
                    timestamp,
                    ..
                } => {
                    if *best_bid <= 0.0 || *best_ask <= 0.0 || best_bid >= best_ask {
                        continue;
                    }

                    state.best_bid = *best_bid;
                    state.best_bid_qty = *best_bid_qty;
                    state.best_ask = *best_ask;
                    state.best_ask_qty = *best_ask_qty;

                    if start_time == 0 {
                        start_time = *timestamp;
                    }
                    end_time = *timestamp;

                    state.check_daily_reset(*timestamp);

                    if !state.is_warmed_up() {
                        continue;
                    }

                    // 出场检查（阶梯式保护机制）
                    if state.position == Position::Long {
                        let current_price = state.best_bid;
                        let hold_secs = (timestamp - state.entry_time) / 1000;
                        let rsi = state.rsi_1m.value().unwrap_or(50.0);

                        // 更新入场后最高价
                        state.highest_since_entry = state.highest_since_entry.max(current_price);
                        let pnl_pct =
                            (current_price - state.entry_price) / state.entry_price * 100.0;
                        let highest_pnl_pct = (state.highest_since_entry - state.entry_price)
                            / state.entry_price
                            * 100.0;

                        // 计算动态止损价
                        let dynamic_sl = if highest_pnl_pct
                            >= self.config.strategy.trailing_trigger_pct
                        {
                            state.trailing_active = true;
                            state.highest_since_entry
                                * (1.0 - self.config.strategy.trailing_distance_pct / 100.0)
                        } else if highest_pnl_pct >= self.config.strategy.breakeven_trigger_pct {
                            state.breakeven_active = true;
                            state.entry_price
                        } else {
                            state.entry_price * (1.0 - self.config.strategy.stop_loss_pct / 100.0)
                        };

                        let should_exit = current_price <= dynamic_sl
                            || pnl_pct >= self.config.strategy.take_profit_pct
                            || hold_secs >= self.config.strategy.max_hold_seconds
                            || rsi > self.config.strategy.rsi_overbought;

                        if should_exit {
                            let reason = if pnl_pct >= self.config.strategy.take_profit_pct {
                                "硬止盈"
                            } else if state.trailing_active && current_price <= dynamic_sl {
                                "追踪止损"
                            } else if state.breakeven_active && current_price <= dynamic_sl {
                                "保本止损"
                            } else if current_price <= dynamic_sl {
                                "止损"
                            } else if hold_secs >= self.config.strategy.max_hold_seconds {
                                "超时"
                            } else {
                                "RSI超买"
                            };

                            let quantity = state.entry_quantity;
                            let pnl_usdt = quantity * state.entry_price * pnl_pct / 100.0;
                            let commission = quantity
                                * (state.entry_price + current_price)
                                * self.config.commission_rate;

                            trade_id += 1;
                            trades.push(TradeRecord {
                                id: trade_id,
                                entry_time: state.entry_time,
                                exit_time: *timestamp,
                                entry_price: state.entry_price,
                                exit_price: current_price,
                                quantity,
                                pnl_pct,
                                pnl_usdt,
                                commission,
                                hold_seconds: hold_secs,
                                exit_reason: reason.to_string(),
                            });

                            state.position = Position::None;
                            state.entry_quantity = 0.0;
                            state.available_capital += pnl_usdt - commission;
                            state.daily_pnl += pnl_pct;
                            state.last_trade_time = *timestamp;
                            continue;
                        }
                    }

                    // 做空出场检查（对称的阶梯式保护）
                    if state.position == Position::Short {
                        let current_price = state.best_ask; // 平空用ask
                        let hold_secs = (timestamp - state.entry_time) / 1000;

                        state.lowest_since_entry = state.lowest_since_entry.min(current_price);
                        let pnl_pct =
                            (state.entry_price - current_price) / state.entry_price * 100.0;
                        let lowest_pnl_pct = (state.entry_price - state.lowest_since_entry)
                            / state.entry_price
                            * 100.0;

                        let dynamic_sl = if lowest_pnl_pct
                            >= self.config.strategy.trailing_trigger_pct
                        {
                            state.trailing_active = true;
                            state.lowest_since_entry
                                * (1.0 + self.config.strategy.trailing_distance_pct / 100.0)
                        } else if lowest_pnl_pct >= self.config.strategy.breakeven_trigger_pct {
                            state.breakeven_active = true;
                            state.entry_price
                        } else {
                            state.entry_price * (1.0 + self.config.strategy.stop_loss_pct / 100.0)
                        };

                        let should_exit = current_price >= dynamic_sl
                            || pnl_pct >= self.config.strategy.take_profit_pct
                            || hold_secs >= self.config.strategy.max_hold_seconds;

                        if should_exit {
                            let reason = if pnl_pct >= self.config.strategy.take_profit_pct {
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

                            let quantity = state.entry_quantity;
                            let pnl_usdt = quantity * state.entry_price * pnl_pct / 100.0;
                            let commission = quantity
                                * (state.entry_price + current_price)
                                * self.config.commission_rate;

                            trade_id += 1;
                            trades.push(TradeRecord {
                                id: trade_id,
                                entry_time: state.entry_time,
                                exit_time: *timestamp,
                                entry_price: state.entry_price,
                                exit_price: current_price,
                                quantity,
                                pnl_pct,
                                pnl_usdt,
                                commission,
                                hold_seconds: hold_secs,
                                exit_reason: reason.to_string(),
                            });

                            state.position = Position::None;
                            state.entry_quantity = 0.0;
                            state.available_capital += pnl_usdt - commission;
                            state.daily_pnl += pnl_pct;
                            state.last_trade_time = *timestamp;
                            continue;
                        }
                    }

                    // 入场检查
                    if state.position == Position::None {
                        if timestamp - state.last_trade_time
                            < self.config.strategy.cooldown_seconds * 1000
                        {
                            continue;
                        }
                        if state.daily_trades >= self.config.strategy.max_daily_trades {
                            continue;
                        }
                        if state.daily_pnl <= -self.config.strategy.max_daily_loss_pct {
                            continue;
                        }

                        let ema_fast_5m = state.ema_fast_5m.value().unwrap_or(0.0);
                        let ema_slow_5m = state.ema_slow_5m.value().unwrap_or(0.0);
                        let rsi = state.rsi_1m.value().unwrap_or(50.0);
                        let vol_ratio = state.volume_ratio.ratio();

                        let trend_up = ema_fast_5m > ema_slow_5m;
                        let trend_strength = if ema_slow_5m > 0.0 {
                            (ema_fast_5m - ema_slow_5m) / ema_slow_5m * 100.0
                        } else {
                            0.0
                        };
                        let trend_strong_enough =
                            trend_strength >= self.config.strategy.min_trend_strength_pct;
                        let rsi_recovering = state.rsi_was_oversold
                            && rsi > (self.config.strategy.rsi_oversold + 5.0)
                            && rsi < self.config.strategy.rsi_overbought;
                        let buy_dominant = vol_ratio > self.config.strategy.volume_ratio_threshold;
                        let bid_support_raw = state.best_bid_qty > state.best_ask_qty * 1.5;
                        let bid_support =
                            bid_support_raw || !self.config.strategy.require_bid_support;

                        // 突破入场（replay模式用best_ask作为当前价）
                        let breakout_signal = if state.recent_highs.len() >= 10 {
                            let lookback_high = state.recent_highs[..state.recent_highs.len() - 1]
                                .iter()
                                .copied()
                                .fold(f64::NEG_INFINITY, f64::max);
                            let breakout_pct =
                                (state.best_ask - lookback_high) / lookback_high * 100.0;
                            // RSI < 65: 防止在RSI高位追高
                            breakout_pct > 0.05
                                && vol_ratio > self.config.strategy.volume_ratio_threshold
                                && rsi < self.config.strategy.breakout_max_rsi
                        } else {
                            false
                        };

                        let entry_signal = (trend_up
                            && trend_strong_enough
                            && rsi_recovering
                            && buy_dominant
                            && bid_support)
                            || (breakout_signal && trend_up && trend_strong_enough);

                        if entry_signal {
                            let entry_quantity = dynamic_entry_quantity(
                                state.available_capital,
                                state.best_ask,
                                &self.config.strategy,
                                self.config.position_allocation_pct,
                                self.config.min_usdt_reserve,
                            );
                            if entry_quantity <= 0.0 {
                                continue;
                            }
                            state.position = Position::Long;
                            state.entry_price = state.best_ask;
                            state.entry_quantity = entry_quantity;
                            state.entry_time = *timestamp;
                            state.highest_since_entry = state.best_ask;
                            state.trailing_active = false;
                            state.breakeven_active = false;
                            state.partial_tp_done = false;
                            state.rsi_was_oversold = false;
                            state.rsi_oversold_bars = 0;
                            state.last_trade_time = *timestamp;
                            state.daily_trades += 1;
                        } else if self.config.strategy.allow_short {
                            // 做空入场
                            let trend_down = ema_fast_5m < ema_slow_5m;
                            let short_trend_strength = if ema_slow_5m > 0.0 {
                                (ema_slow_5m - ema_fast_5m) / ema_slow_5m * 100.0
                            } else {
                                0.0
                            };
                            let short_trend_strong =
                                short_trend_strength >= self.config.strategy.min_trend_strength_pct;
                            let breakdown_signal = if state.recent_lows.len() >= 10 {
                                let lookback_low = state.recent_lows[..state.recent_lows.len() - 1]
                                    .iter()
                                    .copied()
                                    .fold(f64::INFINITY, f64::min);
                                let breakdown_pct =
                                    (lookback_low - state.best_bid) / lookback_low * 100.0;
                                // RSI > 35: 防止RSI极度超卖时做空击穿
                                breakdown_pct > 0.05
                                    && vol_ratio
                                        < (1.0 / self.config.strategy.volume_ratio_threshold)
                                    && rsi > 40.0
                            } else {
                                false
                            };

                            let rsi_overbought_short = rsi > 70.0;
                            let sell_pressure =
                                vol_ratio < (1.0 / self.config.strategy.volume_ratio_threshold);

                            let short_signal =
                                (breakdown_signal && trend_down && short_trend_strong)
                                    || (trend_down
                                        && short_trend_strong
                                        && rsi_overbought_short
                                        && sell_pressure);

                            if short_signal {
                                let entry_quantity = dynamic_entry_quantity(
                                    state.available_capital,
                                    state.best_bid,
                                    &self.config.strategy,
                                    self.config.position_allocation_pct,
                                    self.config.min_usdt_reserve,
                                );
                                if entry_quantity <= 0.0 {
                                    continue;
                                }
                                state.position = Position::Short;
                                state.entry_price = state.best_bid;
                                state.entry_quantity = entry_quantity;
                                state.entry_time = *timestamp;
                                state.lowest_since_entry = state.best_bid;
                                state.trailing_active = false;
                                state.breakeven_active = false;
                                state.rsi_was_oversold = false;
                                state.rsi_oversold_bars = 0;
                                state.last_trade_time = *timestamp;
                                state.daily_trades += 1;
                            }
                        }
                    }
                }
            }
        }

        // Replay回测结束强制平仓
        if state.position == Position::Long || state.position == Position::Short {
            let current_price = if state.position == Position::Long {
                state.best_bid
            } else {
                state.best_ask
            };
            if current_price > 0.0 && state.entry_price > 0.0 {
                let pnl_pct = if state.position == Position::Long {
                    (current_price - state.entry_price) / state.entry_price * 100.0
                } else {
                    (state.entry_price - current_price) / state.entry_price * 100.0
                };
                let hold_secs = (end_time - state.entry_time) / 1000;
                let quantity = state.entry_quantity;
                let pnl_usdt = quantity * state.entry_price * pnl_pct / 100.0;
                let commission =
                    quantity * (state.entry_price + current_price) * self.config.commission_rate;
                trade_id += 1;
                trades.push(TradeRecord {
                    id: trade_id,
                    entry_time: state.entry_time,
                    exit_time: end_time,
                    entry_price: state.entry_price,
                    exit_price: current_price,
                    quantity,
                    pnl_pct,
                    pnl_usdt,
                    commission,
                    hold_seconds: hold_secs,
                    exit_reason: "回测结束".to_string(),
                });
            }
        }

        println!("✅ 回放回测完成: {} 笔交易", trades.len());

        Ok(BacktestReport::generate(
            trades,
            self.config.initial_capital,
            start_time,
            end_time,
        ))
    }

    /// 获取数据加载器引用（用于下载数据）
    pub fn data_loader(&self) -> &DataLoader {
        &self.data_loader
    }

    /// 使用指定策略配置运行K线回测（不打印过程，用于参数优化）
    pub fn run_with_config(
        &self,
        strategy: &StrategyConfig,
    ) -> Result<BacktestReport, crate::error::DomainError> {
        let symbol = &self.config.symbol;
        let klines_1m = self.data_loader.load_klines(symbol, "1m")?;
        let klines_5m = self.data_loader.load_klines(symbol, "5m")?;
        Self::run_backtest_on_data(
            &klines_1m,
            &klines_5m,
            strategy,
            self.config.initial_capital,
            self.config.commission_rate,
            self.config.spread_pct,
            self.config.slippage_pct,
        )
    }

    /// 使用预加载数据运行回测（零IO，用于高速参数优化）
    pub fn run_backtest_on_data(
        klines_1m: &[BacktestKline],
        klines_5m: &[BacktestKline],
        strategy: &StrategyConfig,
        initial_capital: f64,
        commission_rate: f64,
        spread_pct: f64,
        slippage_pct: f64,
    ) -> Result<BacktestReport, crate::error::DomainError> {
        // 单边成交成本%(点差一半 + 滑点)，用于把理想成交价修正为实际成交均价
        let cost_pct = spread_pct / 2.0 + slippage_pct;
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

        let mut state = EngineState::new(initial_capital);
        let mut trades: Vec<TradeRecord> = Vec::new();
        let mut trade_id = 0;

        for (kline, is_1m) in &events {
            if kline.close <= 0.0 {
                continue;
            }
            let timestamp = kline.close_time;

            if *is_1m {
                state.kline_1m_count += 1;
                state.ema_fast_1m.update(kline.close);
                state.ema_slow_1m.update(kline.close);
                let rsi = state.rsi_1m.update(kline.close);
                if rsi < strategy.rsi_oversold {
                    state.rsi_was_oversold = true;
                    state.rsi_oversold_bars = 0;
                } else if state.rsi_was_oversold {
                    state.rsi_oversold_bars += 1;
                    if state.rsi_oversold_bars > 30 {
                        state.rsi_was_oversold = false;
                        state.rsi_oversold_bars = 0;
                    }
                }
                state.best_bid = kline.close;
                state.best_ask = kline.close;
                let buy_vol = kline.taker_buy_volume;
                let sell_vol = (kline.volume - buy_vol).max(0.0);
                state.best_bid_qty = buy_vol;
                state.best_ask_qty = sell_vol;
                state.volume_ratio.add_trade(timestamp, buy_vol, false);
                state.volume_ratio.add_trade(timestamp, sell_vol, true);

                // 更新最近20根K线最高/最低价缓冲区
                state.recent_highs.push(kline.high);
                if state.recent_highs.len() > 20 {
                    state.recent_highs.remove(0);
                }
                state.recent_lows.push(kline.low);
                if state.recent_lows.len() > 20 {
                    state.recent_lows.remove(0);
                }
            } else {
                state.kline_5m_count += 1;
                state.prev_ema_slow_5m = state.ema_slow_5m.value().unwrap_or(0.0);
                state.prev_ema_trend_5m = state.ema_trend_5m.value().unwrap_or(0.0);
                state.ema_fast_5m.update(kline.close);
                state.ema_slow_5m.update(kline.close);
                state.ema_trend_5m.update(kline.close);
                // ATR和ADX更新
                state.atr_5m.update(kline.high, kline.low, kline.close);
                state.adx_5m.update(kline.high, kline.low, kline.close);
                // 记录EMA50历史值（宏观方向过滤）
                let new_ema50 = state.ema_trend_5m.value().unwrap_or(0.0);
                state.ema50_history.push(new_ema50);
                if state.ema50_history.len() > 20 {
                    state.ema50_history.remove(0);
                }
                continue;
            }

            state.check_daily_reset(timestamp);
            if !state.is_warmed_up() {
                continue;
            }

            // 出场（阶梯式保护机制 + ATR动态止损）
            if state.position == Position::Long {
                let hold_secs = (timestamp - state.entry_time) / 1000;
                let rsi = state.rsi_1m.value().unwrap_or(50.0);

                state.highest_since_entry = state.highest_since_entry.max(kline.high);
                let highest_pnl_pct =
                    (state.highest_since_entry - state.entry_price) / state.entry_price * 100.0;

                let current_atr = state.atr_5m.value().unwrap_or(0.0);
                let effective_atr = current_atr.max(state.entry_atr);
                let use_atr = strategy.use_atr_stops && effective_atr > 0.0;

                let dynamic_sl = if use_atr {
                    let atr_sl = state.entry_price - strategy.atr_stop_multiplier * effective_atr;
                    let atr_trailing_trigger = strategy.atr_trailing_multiplier * effective_atr;
                    let atr_trailing_dist = strategy.atr_trailing_distance * effective_atr;
                    let profit_amount = state.highest_since_entry - state.entry_price;
                    if profit_amount >= atr_trailing_trigger {
                        state.trailing_active = true;
                        state.highest_since_entry - atr_trailing_dist
                    } else if highest_pnl_pct >= strategy.breakeven_trigger_pct {
                        state.breakeven_active = true;
                        state.entry_price
                    } else {
                        atr_sl
                    }
                } else if highest_pnl_pct >= strategy.trailing_trigger_pct {
                    state.trailing_active = true;
                    state.highest_since_entry * (1.0 - strategy.trailing_distance_pct / 100.0)
                } else if highest_pnl_pct >= strategy.breakeven_trigger_pct {
                    state.breakeven_active = true;
                    state.entry_price
                } else {
                    state.entry_price * (1.0 - strategy.stop_loss_pct / 100.0)
                };

                let tp_price = state.entry_price * (1.0 + strategy.take_profit_pct / 100.0);

                let sl_hit = kline.low <= dynamic_sl;
                let tp_hit = kline.high >= tp_price;
                let timeout = hold_secs >= strategy.max_hold_seconds;
                let rsi_exit = rsi > strategy.rsi_overbought;
                let current_pnl = (kline.close - state.entry_price) / state.entry_price * 100.0;
                let stale_exit = hold_secs >= strategy.stale_exit_seconds
                    && current_pnl < strategy.stale_pnl_threshold_pct
                    && !state.trailing_active;

                let should_exit = sl_hit || tp_hit || timeout || rsi_exit || stale_exit;

                if should_exit {
                    let (exit_price, reason) = if sl_hit && !tp_hit {
                        let r = if state.trailing_active {
                            "追踪止损"
                        } else if state.breakeven_active {
                            "保本止损"
                        } else {
                            "止损"
                        };
                        (dynamic_sl, r)
                    } else if tp_hit && !sl_hit {
                        (tp_price, "硬止盈")
                    } else if sl_hit && tp_hit {
                        let r = if state.trailing_active {
                            "追踪止损"
                        } else if state.breakeven_active {
                            "保本止损"
                        } else {
                            "止损"
                        };
                        (dynamic_sl, r)
                    } else if stale_exit {
                        (kline.close, "僵尸早退")
                    } else if timeout {
                        (kline.close, "超时")
                    } else {
                        (kline.close, "RSI超买")
                    };

                    // 滑点/点差成本：卖出实际成交均价低于理想价
                    let exit_price = exit_price * (1.0 - cost_pct / 100.0);
                    let pnl_pct = (exit_price - state.entry_price) / state.entry_price * 100.0;
                    let quantity = state.entry_quantity;
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
                        exit_reason: reason.to_string(),
                    });
                    state.position = Position::None;
                    state.entry_quantity = 0.0;
                    state.available_capital += pnl_usdt - commission;
                    state.daily_pnl += pnl_pct;
                    state.last_trade_time = timestamp;
                    state.last_exit_was_stoploss = reason == "止损" || reason == "超时";
                }
            }

            // 做空出场（对称的阶梯式保护 + ATR动态止损）
            if state.position == Position::Short {
                let hold_secs = (timestamp - state.entry_time) / 1000;

                state.lowest_since_entry = state.lowest_since_entry.min(kline.low);
                let lowest_pnl_pct =
                    (state.entry_price - state.lowest_since_entry) / state.entry_price * 100.0;

                let current_atr = state.atr_5m.value().unwrap_or(0.0);
                let effective_atr = current_atr.max(state.entry_atr);
                let use_atr = strategy.use_atr_stops && effective_atr > 0.0;

                let dynamic_sl = if use_atr {
                    let atr_sl = state.entry_price + strategy.atr_stop_multiplier * effective_atr;
                    let atr_trailing_trigger = strategy.atr_trailing_multiplier * effective_atr;
                    let atr_trailing_dist = strategy.atr_trailing_distance * effective_atr;
                    let profit_amount = state.entry_price - state.lowest_since_entry;
                    if profit_amount >= atr_trailing_trigger {
                        state.trailing_active = true;
                        state.lowest_since_entry + atr_trailing_dist
                    } else if lowest_pnl_pct >= strategy.breakeven_trigger_pct {
                        state.breakeven_active = true;
                        state.entry_price
                    } else {
                        atr_sl
                    }
                } else if lowest_pnl_pct >= strategy.trailing_trigger_pct {
                    state.trailing_active = true;
                    state.lowest_since_entry * (1.0 + strategy.trailing_distance_pct / 100.0)
                } else if lowest_pnl_pct >= strategy.breakeven_trigger_pct {
                    state.breakeven_active = true;
                    state.entry_price
                } else {
                    state.entry_price * (1.0 + strategy.stop_loss_pct / 100.0)
                };

                let tp_price = state.entry_price * (1.0 - strategy.take_profit_pct / 100.0);

                // 做空: high触发SL（价格上涨），low触发TP（价格下跌）
                let sl_hit = kline.high >= dynamic_sl;
                let tp_hit = kline.low <= tp_price;
                let timeout = hold_secs >= strategy.max_hold_seconds;
                let current_pnl = (state.entry_price - kline.close) / state.entry_price * 100.0;
                let stale_exit = hold_secs >= strategy.stale_exit_seconds
                    && current_pnl < strategy.stale_pnl_threshold_pct
                    && !state.trailing_active;

                let should_exit = sl_hit || tp_hit || timeout || stale_exit;

                if should_exit {
                    let (exit_price, reason) = if sl_hit && !tp_hit {
                        let r = if state.trailing_active {
                            "空追踪止损"
                        } else if state.breakeven_active {
                            "空保本止损"
                        } else {
                            "空止损"
                        };
                        (dynamic_sl, r)
                    } else if tp_hit && !sl_hit {
                        (tp_price, "空止盈")
                    } else if sl_hit && tp_hit {
                        (dynamic_sl, "空止损")
                    } else if stale_exit {
                        (kline.close, "空僵尸早退")
                    } else {
                        (kline.close, "空超时")
                    };

                    // 滑点/点差成本：买回实际成交均价高于理想价
                    let exit_price = exit_price * (1.0 + cost_pct / 100.0);
                    // 做空盈亏: (入场价 - 出场价) / 入场价
                    let pnl_pct = (state.entry_price - exit_price) / state.entry_price * 100.0;
                    let quantity = state.entry_quantity;
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
                        exit_reason: reason.to_string(),
                    });
                    state.position = Position::None;
                    state.entry_quantity = 0.0;
                    state.available_capital += pnl_usdt - commission;
                    state.daily_pnl += pnl_pct;
                    state.last_trade_time = timestamp;
                    state.last_exit_was_stoploss = reason == "空止损" || reason == "空超时";
                }
            }

            // 入场（多因子评分 + ADX过滤 + 波动率政权）
            if state.position == Position::None {
                // 止损后延长冷却
                let effective_cooldown = if state.last_exit_was_stoploss
                    && strategy.post_stoploss_cooldown_seconds > 0
                {
                    strategy.post_stoploss_cooldown_seconds * 1000
                } else {
                    strategy.cooldown_seconds * 1000
                };
                if timestamp - state.last_trade_time < effective_cooldown {
                    continue;
                }
                if state.daily_trades >= strategy.max_daily_trades {
                    continue;
                }
                if state.daily_pnl <= -strategy.max_daily_loss_pct {
                    continue;
                }

                // === ADX过滤 ===
                let adx_val = state.adx_5m.value().unwrap_or(0.0);
                let adx_ready = state.adx_5m.is_ready();
                let plus_di = state.adx_5m.plus_di();
                let minus_di = state.adx_5m.minus_di();
                let adx_above_threshold = !adx_ready || adx_val >= strategy.adx_min_threshold;
                let long_di_direction_ok = !adx_ready
                    || (plus_di > minus_di && (plus_di - minus_di) >= strategy.min_adx_di_diff);
                let short_di_direction_ok = !adx_ready
                    || (minus_di > plus_di && (minus_di - plus_di) >= strategy.min_adx_di_diff);

                // === 波动率政权过滤 ===
                let atr_pct = state.atr_5m.percentile();
                let volatility_normal = match atr_pct {
                    Some(p) => {
                        p >= strategy.atr_percentile_low && p <= strategy.atr_percentile_high
                    }
                    None => true,
                };

                let ema_fast_5m = state.ema_fast_5m.value().unwrap_or(0.0);
                let ema_slow_5m = state.ema_slow_5m.value().unwrap_or(0.0);
                let ema_trend = state.ema_trend_5m.value().unwrap_or(0.0);
                let rsi = state.rsi_1m.value().unwrap_or(50.0);
                let vol_ratio = state.volume_ratio.ratio();

                let ema21_slope = if state.prev_ema_slow_5m > 0.0 {
                    (ema_slow_5m - state.prev_ema_slow_5m) / state.prev_ema_slow_5m * 100.0
                } else {
                    0.0
                };

                let ema50_macro_rising = if state.ema50_history.len() >= 20 {
                    ema_trend > state.ema50_history[0]
                } else {
                    false
                };

                let price_above_ema50_pct = if ema_trend > 0.0 {
                    (kline.close - ema_trend) / ema_trend * 100.0
                } else {
                    0.0
                };
                let long_structure_ok = !state
                    .is_bearish_continuation(strategy.downtrend_filter_lookback)
                    || state.has_bullish_reversal_confirmation(
                        kline.close,
                        strategy.downtrend_filter_lookback,
                        strategy.min_reversal_break_pct,
                    );

                let long_trend_env_ok = ema_trend > 0.0
                    && price_above_ema50_pct > strategy.min_price_above_ema50_pct
                    && price_above_ema50_pct < strategy.max_ema50_distance_pct
                    && ema21_slope > strategy.mean_revert_min_slope
                    && ema50_macro_rising
                    && long_di_direction_ok
                    && long_structure_ok;

                let trend_up = ema_fast_5m > ema_slow_5m;
                let trend_strength = if ema_slow_5m > 0.0 {
                    (ema_fast_5m - ema_slow_5m) / ema_slow_5m * 100.0
                } else {
                    0.0
                };
                let trend_strong_enough = trend_strength >= strategy.min_trend_strength_pct;
                let rsi_recovering = state.rsi_was_oversold
                    && rsi > (strategy.rsi_oversold + 5.0)
                    && rsi < strategy.rsi_overbought;
                let rsi_bounce_vr_ok = vol_ratio > strategy.volume_ratio_threshold;
                // 盘口支撑：K线回测无法精确模拟实盘订单簿，默认不作为硬条件（配置可开启）
                let bid_support_raw = state.best_bid_qty > state.best_ask_qty * 1.5;
                let bid_support_ok = bid_support_raw || !strategy.require_bid_support;
                let slope_positive = ema21_slope > 0.03;

                let breakout_signal = if state.recent_highs.len() >= 10 {
                    let lookback_high = state.recent_highs[..state.recent_highs.len() - 1]
                        .iter()
                        .copied()
                        .fold(f64::NEG_INFINITY, f64::max);
                    let breakout_pct = (kline.close - lookback_high) / lookback_high * 100.0;
                    breakout_pct > 0.05
                        && vol_ratio > strategy.volume_ratio_threshold
                        && rsi < strategy.breakout_max_rsi
                } else {
                    false
                };

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
                if vol_ratio > strategy.volume_ratio_threshold {
                    entry_score += 15;
                }
                if bid_support_raw {
                    entry_score += 10;
                }
                if slope_positive {
                    entry_score += 10;
                }
                if volatility_normal {
                    entry_score += 10;
                }

                let breakout_slope_ok = ema21_slope > strategy.breakout_min_slope;
                let rsi_bounce_not_late = rsi < strategy.rsi_bounce_max_rsi;

                let trend_entry_signal = long_trend_env_ok
                    && entry_score >= strategy.entry_score_threshold
                    && ((trend_up
                        && trend_strong_enough
                        && rsi_recovering
                        && rsi_bounce_vr_ok
                        && bid_support_ok
                        && rsi_bounce_not_late)
                        || (breakout_signal
                            && trend_up
                            && trend_strong_enough
                            && breakout_slope_ok));

                // 均值回归入场信号
                // 深跌反弹允许价格短暂跌破EMA50（默认不要求在EMA50上方，配置可开启）
                let above_ema50_ok =
                    !strategy.mean_revert_require_above_ema50 || kline.close >= ema_trend;
                let mean_revert_signal = if state.recent_highs.len() >= 20 {
                    let recent_high = state
                        .recent_highs
                        .iter()
                        .copied()
                        .fold(f64::NEG_INFINITY, f64::max);
                    let drop_pct = (recent_high - kline.close) / recent_high * 100.0;
                    drop_pct > strategy.mean_revert_min_drop_pct
                        && rsi < strategy.rsi_oversold
                        && vol_ratio > strategy.volume_ratio_threshold
                        && bid_support_ok
                        && ema21_slope > strategy.mean_revert_min_slope
                        && ema_trend > 0.0
                        && above_ema50_ok
                        && ema50_macro_rising
                        && long_di_direction_ok
                        && long_structure_ok
                        && volatility_normal
                } else {
                    false
                };

                let entry_signal = trend_entry_signal || mean_revert_signal;

                if entry_signal {
                    let entry_quantity = dynamic_entry_quantity(
                        state.available_capital,
                        kline.close,
                        strategy,
                        DEFAULT_POSITION_ALLOCATION_PCT,
                        DEFAULT_MIN_USDT_RESERVE,
                    );
                    if entry_quantity <= 0.0 {
                        continue;
                    }
                    state.position = Position::Long;
                    // 滑点/点差成本：买入实际成交均价高于理想价
                    state.entry_price = kline.close * (1.0 + cost_pct / 100.0);
                    state.entry_quantity = entry_quantity;
                    state.entry_time = timestamp;
                    state.highest_since_entry = state.entry_price;
                    state.trailing_active = false;
                    state.breakeven_active = false;
                    state.partial_tp_done = false;
                    state.rsi_was_oversold = false;
                    state.rsi_oversold_bars = 0;
                    state.last_trade_time = timestamp;
                    state.daily_trades += 1;
                    state.entry_atr = state.atr_5m.value().unwrap_or(0.0);
                } else if strategy.allow_short {
                    // 做空入场（趋势环境过滤）
                    let price_below_ema50_pct = if ema_trend > 0.0 {
                        (ema_trend - kline.close) / ema_trend * 100.0
                    } else {
                        0.0
                    };
                    let short_trend_env_ok =
                        ema_trend > 0.0 && price_below_ema50_pct > 0.15 && ema21_slope < -0.03;

                    let trend_down = ema_fast_5m < ema_slow_5m;
                    let short_trend_strength = if ema_slow_5m > 0.0 {
                        (ema_slow_5m - ema_fast_5m) / ema_slow_5m * 100.0
                    } else {
                        0.0
                    };
                    let short_trend_strong =
                        short_trend_strength >= strategy.min_trend_strength_pct;
                    let breakdown_signal = if state.recent_lows.len() >= 10 {
                        let lookback_low = state.recent_lows[..state.recent_lows.len() - 1]
                            .iter()
                            .copied()
                            .fold(f64::INFINITY, f64::min);
                        let breakdown_pct = (lookback_low - kline.close) / lookback_low * 100.0;
                        breakdown_pct > 0.05
                            && vol_ratio < (1.0 / strategy.volume_ratio_threshold)
                            && rsi > 40.0
                    } else {
                        false
                    };

                    let rsi_overbought_short = rsi > 70.0;
                    let sell_pressure = vol_ratio < (1.0 / strategy.volume_ratio_threshold);

                    let short_signal = short_trend_env_ok
                        && adx_above_threshold
                        && short_di_direction_ok
                        && ((breakdown_signal && trend_down && short_trend_strong)
                            || (trend_down
                                && short_trend_strong
                                && rsi_overbought_short
                                && sell_pressure));

                    if short_signal {
                        let entry_quantity = dynamic_entry_quantity(
                            state.available_capital,
                            kline.close,
                            strategy,
                            DEFAULT_POSITION_ALLOCATION_PCT,
                            DEFAULT_MIN_USDT_RESERVE,
                        );
                        if entry_quantity <= 0.0 {
                            continue;
                        }
                        state.position = Position::Short;
                        // 滑点/点差成本：卖空实际成交均价低于理想价
                        state.entry_price = kline.close * (1.0 - cost_pct / 100.0);
                        state.entry_quantity = entry_quantity;
                        state.entry_time = timestamp;
                        state.lowest_since_entry = state.entry_price;
                        state.trailing_active = false;
                        state.breakeven_active = false;
                        state.rsi_was_oversold = false;
                        state.rsi_oversold_bars = 0;
                        state.last_trade_time = timestamp;
                        state.daily_trades += 1;
                        state.entry_atr = state.atr_5m.value().unwrap_or(0.0);
                    }
                }
            }
        }

        // 回测结束强制平仓
        if state.position == Position::Long || state.position == Position::Short {
            if let Some((last_kline, _)) = events.last() {
                // 强制平仓同样承担点差/滑点成本
                let current_price = if state.position == Position::Long {
                    last_kline.close * (1.0 - cost_pct / 100.0)
                } else {
                    last_kline.close * (1.0 + cost_pct / 100.0)
                };
                let pnl_pct = if state.position == Position::Long {
                    (current_price - state.entry_price) / state.entry_price * 100.0
                } else {
                    (state.entry_price - current_price) / state.entry_price * 100.0
                };
                let hold_secs = (last_kline.close_time - state.entry_time) / 1000;
                let quantity = state.entry_quantity;
                let pnl_usdt = quantity * state.entry_price * pnl_pct / 100.0;
                let commission = quantity * (state.entry_price + current_price) * commission_rate;
                trade_id += 1;
                trades.push(TradeRecord {
                    id: trade_id,
                    entry_time: state.entry_time,
                    exit_time: last_kline.close_time,
                    entry_price: state.entry_price,
                    exit_price: current_price,
                    quantity,
                    pnl_pct,
                    pnl_usdt,
                    commission,
                    hold_seconds: hold_secs,
                    exit_reason: "回测结束".to_string(),
                });
            }
        }

        Ok(BacktestReport::generate(
            trades,
            initial_capital,
            start_time,
            end_time,
        ))
    }
}
