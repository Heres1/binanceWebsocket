//! 网格交易策略
//!
//! 在设定的价格区间内均匀布置网格，价格触及网格线时生成交易信号。
//! 本实现仅打印信号，不执行实际下单。

use crate::error::{DomainError, ServiceError};
use crate::event_bus::{EventBus, EventHandler, EventType, TokioEventBus};
use crate::events::{
    DomainEvent, GridAction, GridTriggerEvent, PriceUpdateEvent, TradingSignalEvent,
};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex;

/// 网格线
#[derive(Debug, Clone)]
pub struct GridLine {
    /// 网格级别（从0开始，0为最低网格）
    pub level: i32,
    /// 网格价格
    pub price: f64,
    /// 是否已触发（持有买入仓位）
    pub filled: bool,
}

/// 网格策略配置
#[derive(Debug, Clone)]
pub struct GridConfig {
    /// 策略唯一ID
    pub strategy_id: String,
    /// 订阅的交易对
    pub symbol: String,
    /// 网格区间下限
    pub lower_price: f64,
    /// 网格区间上限
    pub upper_price: f64,
    /// 网格数量
    pub grid_count: usize,
    /// 每格交易数量
    pub quantity_per_grid: f64,
    /// 利润阈值百分比（触发卖出信号）
    pub profit_threshold_pct: f64,
}

impl GridConfig {
    pub fn new(
        strategy_id: impl Into<String>,
        symbol: impl Into<String>,
        lower_price: f64,
        upper_price: f64,
        grid_count: usize,
        quantity_per_grid: f64,
    ) -> Self {
        Self {
            strategy_id: strategy_id.into(),
            symbol: symbol.into(),
            lower_price,
            upper_price,
            grid_count,
            quantity_per_grid,
            profit_threshold_pct: 0.5, // 默认0.5%利润触发
        }
    }

    /// 计算网格间距
    pub fn grid_spacing(&self) -> f64 {
        (self.upper_price - self.lower_price) / self.grid_count as f64
    }

    /// 生成所有网格线价格列表
    pub fn build_grid_lines(&self) -> Vec<GridLine> {
        let spacing = self.grid_spacing();
        (0..=self.grid_count)
            .map(|i| GridLine {
                level: i as i32,
                price: self.lower_price + spacing * i as f64,
                filled: false,
            })
            .collect()
    }
}

/// 网格策略运行时状态
struct GridState {
    /// 网格线列表
    grid_lines: Vec<GridLine>,
    /// 上一次价格
    last_price: Option<f64>,
    /// 触发信号计数
    signal_count: u64,
}

impl GridState {
    fn new(config: &GridConfig) -> Self {
        Self {
            grid_lines: config.build_grid_lines(),
            last_price: None,
            signal_count: 0,
        }
    }

    /// 当价格更新时检查是否触及网格，返回触发的信号列表
    fn check_price(
        &mut self,
        config: &GridConfig,
        price: f64,
        timestamp: u64,
    ) -> Vec<TradingSignalEvent> {
        let mut signals = Vec::new();
        let last = self.last_price.unwrap_or(price);

        for line in &mut self.grid_lines {
            // 价格从上方穿越网格线 → 买入信号
            if last > line.price && price <= line.price && !line.filled {
                line.filled = true;
                self.signal_count += 1;
                let signal = TradingSignalEvent {
                    signal_id: format!("{}_{}", config.strategy_id, self.signal_count),
                    strategy_id: config.strategy_id.clone(),
                    symbol: config.symbol.clone(),
                    signal_type: "BUY".to_string(),
                    strength: 1.0,
                    suggested_price: line.price,
                    suggested_quantity: Some(config.quantity_per_grid),
                    stop_loss_price: Some(line.price * (1.0 - 0.02)),
                    take_profit_price: Some(line.price * (1.0 + config.profit_threshold_pct / 100.0)),
                    timestamp,
                };
                signals.push(signal);
            }
            // 价格从下方穿越网格线 → 卖出信号（仅已持有仓位的网格）
            else if last < line.price && price >= line.price && line.filled {
                line.filled = false;
                self.signal_count += 1;
                let signal = TradingSignalEvent {
                    signal_id: format!("{}_{}", config.strategy_id, self.signal_count),
                    strategy_id: config.strategy_id.clone(),
                    symbol: config.symbol.clone(),
                    signal_type: "SELL".to_string(),
                    strength: 1.0,
                    suggested_price: line.price,
                    suggested_quantity: Some(config.quantity_per_grid),
                    stop_loss_price: None,
                    take_profit_price: None,
                    timestamp,
                };
                signals.push(signal);
            }
        }

        self.last_price = Some(price);
        signals
    }
}

/// 网格交易策略
///
/// 实现 `EventHandler` 监听价格事件，触发时生成交易信号并发布到事件总线。
pub struct GridStrategy {
    config: GridConfig,
    state: Arc<Mutex<GridState>>,
    event_bus: Arc<TokioEventBus>,
}

impl GridStrategy {
    /// 创建网格策略
    pub fn new(config: GridConfig, event_bus: Arc<TokioEventBus>) -> Self {
        let state = GridState::new(&config);
        log::info!(
            "网格策略初始化: {} | 区间 [{:.2}, {:.2}] | {}格 | 间距 {:.2}",
            config.strategy_id,
            config.lower_price,
            config.upper_price,
            config.grid_count,
            config.grid_spacing()
        );
        Self {
            state: Arc::new(Mutex::new(state)),
            config,
            event_bus,
        }
    }

    /// 获取配置
    pub fn config(&self) -> &GridConfig {
        &self.config
    }

    /// 处理价格更新
    async fn on_price_update(&self, event: &PriceUpdateEvent) -> Result<(), DomainError> {
        if event.symbol != self.config.symbol {
            return Ok(());
        }

        // 打印接收到的价格（调试用）
        log::debug!(
            "价格更新 | {} | {:.2} USDT | 24h涨跌: {:.2}%",
            event.symbol, event.price, event.price_change_pct_24h
        );

        let signals = {
            let mut state = self.state.lock().await;
            state.check_price(&self.config, event.price, event.timestamp)
        };

        for signal in &signals {
            let action = if signal.signal_type == "BUY" {
                GridAction::Buy
            } else {
                GridAction::Sell
            };

            let level = self.find_grid_level(signal.suggested_price);
            let grid_trigger = GridTriggerEvent::new(
                self.config.strategy_id.clone(),
                self.config.symbol.clone(),
                level,
                signal.suggested_price,
                action,
                self.config.quantity_per_grid,
            );

            // 打印信号
            log::info!(
                "[{}] {} 信号 | 价格: {:.2} | 数量: {} | 网格级别: {} | 止盈: {:.2}",
                signal.strategy_id,
                signal.signal_type,
                signal.suggested_price,
                self.config.quantity_per_grid,
                level,
                signal.take_profit_price.unwrap_or(0.0)
            );

            // 发布网格触发事件
            self.event_bus
                .publish(DomainEvent::GridTrigger(grid_trigger))
                .await
                .map_err(|e| {
                    DomainError::Service(ServiceError::Risk(format!("发布网格事件失败: {}", e)))
                })?;

            // 发布交易信号事件
            self.event_bus
                .publish(DomainEvent::TradingSignal(signal.clone()))
                .await
                .map_err(|e| {
                    DomainError::Service(ServiceError::Risk(format!("发布交易信号失败: {}", e)))
                })?;
        }

        Ok(())
    }

    /// 根据价格找到最近的网格级别
    fn find_grid_level(&self, price: f64) -> i32 {
        let spacing = self.config.grid_spacing();
        if spacing == 0.0 {
            return 0;
        }
        ((price - self.config.lower_price) / spacing).round() as i32
    }
}

#[async_trait]
impl EventHandler for GridStrategy {
    async fn handle(&self, event: &DomainEvent) -> Result<(), crate::error::EventBusError> {
        if let DomainEvent::PriceUpdate(price_event) = event {
            if let Err(e) = self.on_price_update(price_event).await {
                log::error!("网格策略处理价格失败: {}", e);
            }
        }
        Ok(())
    }

    fn event_types(&self) -> Vec<EventType> {
        vec![EventType::PriceUpdate]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_config() -> GridConfig {
        GridConfig::new("grid_test", "BTCUSDT", 48000.0, 52000.0, 4, 0.001)
    }

    #[test]
    fn test_grid_lines_count() {
        let config = make_config();
        let lines = config.build_grid_lines();
        // 4格 → 5条线 (0,1,2,3,4)
        assert_eq!(lines.len(), 5);
    }

    #[test]
    fn test_grid_spacing() {
        let config = make_config();
        // (52000 - 48000) / 4 = 1000
        assert_eq!(config.grid_spacing(), 1000.0);
    }

    #[test]
    fn test_grid_line_prices() {
        let config = make_config();
        let lines = config.build_grid_lines();
        assert_eq!(lines[0].price, 48000.0);
        assert_eq!(lines[1].price, 49000.0);
        assert_eq!(lines[2].price, 50000.0);
        assert_eq!(lines[3].price, 51000.0);
        assert_eq!(lines[4].price, 52000.0);
    }

    #[test]
    fn test_buy_signal_on_price_drop() {
        let config = make_config();
        let mut state = GridState::new(&config);

        // 价格从51000降到49500，应穿越50000网格线 → 买入信号
        let signals = state.check_price(&config, 51000.0, 1000);
        assert_eq!(signals.len(), 0); // 首次设置last_price

        let signals = state.check_price(&config, 49500.0, 2000);
        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].signal_type, "BUY");
        assert_eq!(signals[0].suggested_price, 50000.0);
    }

    #[test]
    fn test_sell_signal_on_price_rise() {
        let config = make_config();
        let mut state = GridState::new(&config);

        // 先触发买入
        state.check_price(&config, 51000.0, 1000);
        state.check_price(&config, 49500.0, 2000); // 穿越50000买入

        // 价格回升到50500，穿越50000 → 卖出信号
        let signals = state.check_price(&config, 50500.0, 3000);
        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].signal_type, "SELL");
        assert_eq!(signals[0].suggested_price, 50000.0);
    }

    #[test]
    fn test_no_duplicate_buy() {
        let config = make_config();
        let mut state = GridState::new(&config);

        state.check_price(&config, 51000.0, 1000);
        let s1 = state.check_price(&config, 49500.0, 2000); // 买入
        let s2 = state.check_price(&config, 49000.0, 3000); // 不重复买入

        assert_eq!(s1.len(), 1);
        // 49000穿越49000网格线，但此格未filled
        assert_eq!(s2.len(), 1); // 穿越49000线→买入
    }

    #[test]
    fn test_find_grid_level() {
        let config = make_config();
        let event_bus = Arc::new(TokioEventBus::new(100));
        let strategy = GridStrategy::new(config, event_bus);

        assert_eq!(strategy.find_grid_level(48000.0), 0);
        assert_eq!(strategy.find_grid_level(49000.0), 1);
        assert_eq!(strategy.find_grid_level(50000.0), 2);
        assert_eq!(strategy.find_grid_level(51000.0), 3);
        assert_eq!(strategy.find_grid_level(52000.0), 4);
    }

    #[tokio::test]
    async fn test_strategy_handles_price_event() {
        let config = make_config();
        let event_bus = Arc::new(TokioEventBus::new(100));
        let strategy = GridStrategy::new(config, event_bus);

        // 设置初始价格（无信号）
        let e1 = PriceUpdateEvent {
            symbol: "BTCUSDT".to_string(),
            price: 51000.0,
            price_change_pct_24h: 0.0,
            timestamp: 1000,
        };
        strategy.on_price_update(&e1).await.unwrap();

        // 价格下穿50000
        let e2 = PriceUpdateEvent {
            symbol: "BTCUSDT".to_string(),
            price: 49500.0,
            price_change_pct_24h: -2.5,
            timestamp: 2000,
        };
        strategy.on_price_update(&e2).await.unwrap();

        let state = strategy.state.lock().await;
        // level=2（50000那条线）应该已被标记为filled
        assert!(state.grid_lines[2].filled);
    }
}
