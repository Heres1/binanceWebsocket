//! 回测引擎核心
//!
//! 加载历史数据，按时间序列驱动策略指标，模拟交易并记录结果

use crate::backtest::data_loader::{BacktestKline, DataLoader, RecordedEvent};
use crate::backtest::report::{BacktestReport, TradeRecord};
use crate::config::StrategyConfig;
use crate::strategies::indicators::{EMA, RSI, VolumeRatio};

/// 回测模式
#[derive(Debug, Clone)]
pub enum BacktestMode {
    /// K线模式：使用历史K线数据
    Kline,
    /// 回放模式：使用录制的实时数据
    Replay,
}

/// 回测引擎配置
#[derive(Debug, Clone)]
pub struct BacktestConfig {
    pub symbol: String,
    pub initial_capital: f64,
    pub strategy: StrategyConfig,
    pub commission_rate: f64, // 手续费率，默认0.001 (0.1%)
}

/// 内部持仓状态
#[derive(Debug, Clone, PartialEq)]
enum Position {
    None,
    Long,
}

/// 回测引擎内部状态
struct EngineState {
    // 指标
    ema_fast_1m: EMA,
    ema_slow_1m: EMA,
    rsi_1m: RSI,
    ema_fast_5m: EMA,
    ema_slow_5m: EMA,
    volume_ratio: VolumeRatio,

    // 盘口
    best_bid: f64,
    best_bid_qty: f64,
    best_ask: f64,
    best_ask_qty: f64,

    // 持仓
    position: Position,
    entry_price: f64,
    entry_time: u64,

    // 冷却与统计
    last_trade_time: u64,
    daily_trades: u32,
    daily_pnl: f64,
    last_day: u32,

    // RSI状态
    rsi_was_oversold: bool,

    // 预热
    kline_1m_count: usize,
    kline_5m_count: usize,
}

impl EngineState {
    fn new() -> Self {
        Self {
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
            position: Position::None,
            entry_price: 0.0,
            entry_time: 0,
            last_trade_time: 0,
            daily_trades: 0,
            daily_pnl: 0.0,
            last_day: 0,
            rsi_was_oversold: false,
            kline_1m_count: 0,
            kline_5m_count: 0,
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

        let mut state = EngineState::new();
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

                if rsi < self.config.strategy.rsi_oversold {
                    state.rsi_was_oversold = true;
                }

                // 用K线收盘价模拟盘口（K线模式近似）
                state.best_bid = kline.close;
                state.best_ask = kline.close;
                // 使用真实的taker buy volume来模拟盘口深度
                let buy_vol = kline.taker_buy_volume;
                let sell_vol = kline.volume - buy_vol;
                state.best_bid_qty = buy_vol;
                state.best_ask_qty = sell_vol;

                // 使用真实的taker buy volume计算买卖比
                // is_buyer_maker=false 表示买方主动成交（taker buy）
                state.volume_ratio.add_trade(timestamp, buy_vol, false);
                // is_buyer_maker=true 表示卖方主动成交
                state.volume_ratio.add_trade(timestamp, sell_vol, true);
            } else {
                state.kline_5m_count += 1;
                state.ema_fast_5m.update(kline.close);
                state.ema_slow_5m.update(kline.close);
                continue; // 5m只更新指标不触发交易
            }

            state.check_daily_reset(timestamp);

            if !state.is_warmed_up() {
                continue;
            }

            // 检查出场
            if state.position == Position::Long {
                let current_price = kline.close;
                let pnl_pct = (current_price - state.entry_price) / state.entry_price * 100.0;
                let hold_secs = (timestamp - state.entry_time) / 1000;
                let rsi = state.rsi_1m.value().unwrap_or(50.0);

                let should_exit = pnl_pct >= self.config.strategy.take_profit_pct
                    || pnl_pct <= -self.config.strategy.stop_loss_pct
                    || hold_secs >= self.config.strategy.max_hold_seconds
                    || rsi > self.config.strategy.rsi_overbought;

                if should_exit {
                    let reason = if pnl_pct >= self.config.strategy.take_profit_pct {
                        "止盈"
                    } else if pnl_pct <= -self.config.strategy.stop_loss_pct {
                        "止损"
                    } else if hold_secs >= self.config.strategy.max_hold_seconds {
                        "超时"
                    } else {
                        "RSI超买"
                    };

                    let quantity = self.config.strategy.quantity_per_trade;
                    let pnl_usdt = quantity * state.entry_price * pnl_pct / 100.0;
                    let commission = quantity * (state.entry_price + current_price) * self.config.commission_rate;

                    trade_id += 1;
                    trades.push(TradeRecord {
                        id: trade_id,
                        entry_time: state.entry_time,
                        exit_time: timestamp,
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
                    state.daily_pnl += pnl_pct;
                    state.last_trade_time = timestamp;
                    state.daily_trades += 1;
                }
            }

            // 检查入场
            if state.position == Position::None {
                if timestamp - state.last_trade_time < self.config.strategy.cooldown_seconds * 1000 {
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
                let rsi_recovering = state.rsi_was_oversold && rsi > (self.config.strategy.rsi_oversold + 5.0);
                let buy_dominant = vol_ratio > self.config.strategy.volume_ratio_threshold;
                let bid_support = state.best_bid_qty > state.best_ask_qty * 1.5;

                if trend_up && rsi_recovering && buy_dominant && bid_support {
                    state.position = Position::Long;
                    state.entry_price = kline.close;
                    state.entry_time = timestamp;
                    state.rsi_was_oversold = false;
                    state.last_trade_time = timestamp;
                    state.daily_trades += 1;
                }
            }
        }

        // 如果回测结束时还有持仓，强制平仓
        if state.position == Position::Long {
            if let Some((last_kline, _)) = events.last() {
                let current_price = last_kline.close;
                let pnl_pct = (current_price - state.entry_price) / state.entry_price * 100.0;
                let hold_secs = (last_kline.close_time - state.entry_time) / 1000;
                let quantity = self.config.strategy.quantity_per_trade;
                let pnl_usdt = quantity * state.entry_price * pnl_pct / 100.0;
                let commission = quantity * (state.entry_price + current_price) * self.config.commission_rate;

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
    pub fn run_replay_backtest(&self, file_path: &str) -> Result<BacktestReport, crate::error::DomainError> {
        let events = self.data_loader.load_recorded_events(file_path)?;

        println!("\n🔄 开始回放回测...");
        println!("   事件数: {}", events.len());

        let mut state = EngineState::new();
        let mut trades: Vec<TradeRecord> = Vec::new();
        let mut trade_id = 0;
        let mut start_time = 0u64;
        let mut end_time = 0u64;

        for event in &events {
            match event {
                RecordedEvent::Kline(kline) => {
                    if kline.close <= 0.0 { continue; }

                    let ts = kline.close_time;
                    if start_time == 0 { start_time = ts; }
                    end_time = ts;

                    match kline.interval.as_str() {
                        "1m" => {
                            state.kline_1m_count += 1;
                            state.ema_fast_1m.update(kline.close);
                            state.ema_slow_1m.update(kline.close);
                            let rsi = state.rsi_1m.update(kline.close);
                            if rsi < self.config.strategy.rsi_oversold {
                                state.rsi_was_oversold = true;
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
                RecordedEvent::AggTrade { quantity, is_buyer_maker, timestamp, .. } => {
                    if *quantity <= 0.0 { continue; }
                    state.volume_ratio.add_trade(*timestamp, *quantity, *is_buyer_maker);
                    if start_time == 0 { start_time = *timestamp; }
                    end_time = *timestamp;
                }
                RecordedEvent::BookTicker {
                    best_bid, best_bid_qty, best_ask, best_ask_qty, timestamp, ..
                } => {
                    if *best_bid <= 0.0 || *best_ask <= 0.0 || best_bid >= best_ask {
                        continue;
                    }

                    state.best_bid = *best_bid;
                    state.best_bid_qty = *best_bid_qty;
                    state.best_ask = *best_ask;
                    state.best_ask_qty = *best_ask_qty;

                    if start_time == 0 { start_time = *timestamp; }
                    end_time = *timestamp;

                    state.check_daily_reset(*timestamp);

                    if !state.is_warmed_up() {
                        continue;
                    }

                    // 出场检查
                    if state.position == Position::Long {
                        let current_price = state.best_bid;
                        let pnl_pct = (current_price - state.entry_price) / state.entry_price * 100.0;
                        let hold_secs = (timestamp - state.entry_time) / 1000;
                        let rsi = state.rsi_1m.value().unwrap_or(50.0);

                        let should_exit = pnl_pct >= self.config.strategy.take_profit_pct
                            || pnl_pct <= -self.config.strategy.stop_loss_pct
                            || hold_secs >= self.config.strategy.max_hold_seconds
                            || rsi > self.config.strategy.rsi_overbought;

                        if should_exit {
                            let reason = if pnl_pct >= self.config.strategy.take_profit_pct {
                                "止盈"
                            } else if pnl_pct <= -self.config.strategy.stop_loss_pct {
                                "止损"
                            } else if hold_secs >= self.config.strategy.max_hold_seconds {
                                "超时"
                            } else {
                                "RSI超买"
                            };

                            let quantity = self.config.strategy.quantity_per_trade;
                            let pnl_usdt = quantity * state.entry_price * pnl_pct / 100.0;
                            let commission = quantity * (state.entry_price + current_price) * self.config.commission_rate;

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
                            state.daily_pnl += pnl_pct;
                            state.last_trade_time = *timestamp;
                            state.daily_trades += 1;
                            continue;
                        }
                    }

                    // 入场检查
                    if state.position == Position::None {
                        if timestamp - state.last_trade_time < self.config.strategy.cooldown_seconds * 1000 {
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
                        let rsi_recovering = state.rsi_was_oversold && rsi > (self.config.strategy.rsi_oversold + 5.0);
                        let buy_dominant = vol_ratio > self.config.strategy.volume_ratio_threshold;
                        let bid_support = state.best_bid_qty > state.best_ask_qty * 1.5;

                        if trend_up && rsi_recovering && buy_dominant && bid_support {
                            state.position = Position::Long;
                            state.entry_price = state.best_ask;
                            state.entry_time = *timestamp;
                            state.rsi_was_oversold = false;
                            state.last_trade_time = *timestamp;
                            state.daily_trades += 1;
                        }
                    }
                }
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
    pub fn run_with_config(&self, strategy: &StrategyConfig) -> Result<BacktestReport, crate::error::DomainError> {
        let symbol = &self.config.symbol;
        let klines_1m = self.data_loader.load_klines(symbol, "1m")?;
        let klines_5m = self.data_loader.load_klines(symbol, "5m")?;
        Self::run_backtest_on_data(&klines_1m, &klines_5m, strategy, self.config.initial_capital, self.config.commission_rate)
    }

    /// 使用预加载数据运行回测（零IO，用于高速参数优化）
    pub fn run_backtest_on_data(
        klines_1m: &[BacktestKline],
        klines_5m: &[BacktestKline],
        strategy: &StrategyConfig,
        initial_capital: f64,
        commission_rate: f64,
    ) -> Result<BacktestReport, crate::error::DomainError> {

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

        let mut state = EngineState::new();
        let mut trades: Vec<TradeRecord> = Vec::new();
        let mut trade_id = 0;

        for (kline, is_1m) in &events {
            if kline.close <= 0.0 { continue; }
            let timestamp = kline.close_time;

            if *is_1m {
                state.kline_1m_count += 1;
                state.ema_fast_1m.update(kline.close);
                state.ema_slow_1m.update(kline.close);
                let rsi = state.rsi_1m.update(kline.close);
                if rsi < strategy.rsi_oversold {
                    state.rsi_was_oversold = true;
                }
                state.best_bid = kline.close;
                state.best_ask = kline.close;
                let buy_vol = kline.taker_buy_volume;
                let sell_vol = kline.volume - buy_vol;
                state.best_bid_qty = buy_vol;
                state.best_ask_qty = sell_vol;
                state.volume_ratio.add_trade(timestamp, buy_vol, false);
                state.volume_ratio.add_trade(timestamp, sell_vol, true);
            } else {
                state.kline_5m_count += 1;
                state.ema_fast_5m.update(kline.close);
                state.ema_slow_5m.update(kline.close);
                continue;
            }

            state.check_daily_reset(timestamp);
            if !state.is_warmed_up() { continue; }

            // 出场
            if state.position == Position::Long {
                let current_price = kline.close;
                let pnl_pct = (current_price - state.entry_price) / state.entry_price * 100.0;
                let hold_secs = (timestamp - state.entry_time) / 1000;
                let rsi = state.rsi_1m.value().unwrap_or(50.0);

                let should_exit = pnl_pct >= strategy.take_profit_pct
                    || pnl_pct <= -strategy.stop_loss_pct
                    || hold_secs >= strategy.max_hold_seconds
                    || rsi > strategy.rsi_overbought;

                if should_exit {
                    let reason = if pnl_pct >= strategy.take_profit_pct { "止盈" }
                        else if pnl_pct <= -strategy.stop_loss_pct { "止损" }
                        else if hold_secs >= strategy.max_hold_seconds { "超时" }
                        else { "RSI超买" };

                    let quantity = strategy.quantity_per_trade;
                    let pnl_usdt = quantity * state.entry_price * pnl_pct / 100.0;
                    let commission = quantity * (state.entry_price + current_price) * commission_rate;

                    trade_id += 1;
                    trades.push(TradeRecord {
                        id: trade_id, entry_time: state.entry_time, exit_time: timestamp,
                        entry_price: state.entry_price, exit_price: current_price, quantity,
                        pnl_pct, pnl_usdt, commission, hold_seconds: hold_secs,
                        exit_reason: reason.to_string(),
                    });
                    state.position = Position::None;
                    state.daily_pnl += pnl_pct;
                    state.last_trade_time = timestamp;
                    state.daily_trades += 1;
                }
            }

            // 入场
            if state.position == Position::None {
                if timestamp - state.last_trade_time < strategy.cooldown_seconds * 1000 { continue; }
                if state.daily_trades >= strategy.max_daily_trades { continue; }
                if state.daily_pnl <= -strategy.max_daily_loss_pct { continue; }

                let ema_fast_5m = state.ema_fast_5m.value().unwrap_or(0.0);
                let ema_slow_5m = state.ema_slow_5m.value().unwrap_or(0.0);
                let rsi = state.rsi_1m.value().unwrap_or(50.0);
                let vol_ratio = state.volume_ratio.ratio();

                let trend_up = ema_fast_5m > ema_slow_5m;
                let rsi_recovering = state.rsi_was_oversold && rsi > (strategy.rsi_oversold + 5.0);
                let buy_dominant = vol_ratio > strategy.volume_ratio_threshold;
                let bid_support = state.best_bid_qty > state.best_ask_qty * 1.5;

                if trend_up && rsi_recovering && buy_dominant && bid_support {
                    state.position = Position::Long;
                    state.entry_price = kline.close;
                    state.entry_time = timestamp;
                    state.rsi_was_oversold = false;
                    state.last_trade_time = timestamp;
                    state.daily_trades += 1;
                }
            }
        }

        // 回测结束强制平仓
        if state.position == Position::Long {
            if let Some((last_kline, _)) = events.last() {
                let current_price = last_kline.close;
                let pnl_pct = (current_price - state.entry_price) / state.entry_price * 100.0;
                let hold_secs = (last_kline.close_time - state.entry_time) / 1000;
                let quantity = strategy.quantity_per_trade;
                let pnl_usdt = quantity * state.entry_price * pnl_pct / 100.0;
                let commission = quantity * (state.entry_price + current_price) * commission_rate;
                trade_id += 1;
                trades.push(TradeRecord {
                    id: trade_id, entry_time: state.entry_time, exit_time: last_kline.close_time,
                    entry_price: state.entry_price, exit_price: current_price, quantity,
                    pnl_pct, pnl_usdt, commission, hold_seconds: hold_secs,
                    exit_reason: "回测结束".to_string(),
                });
            }
        }

        Ok(BacktestReport::generate(trades, initial_capital, start_time, end_time))
    }
}
