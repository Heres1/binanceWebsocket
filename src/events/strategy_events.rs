//! 策略事件
//! 
//! 包含交易信号、网格触发、网格状态变更等事件

use serde::{Deserialize, Serialize};

/// 交易信号事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradingSignalEvent {
    /// 信号 ID
    pub signal_id: String,
    /// 策略 ID
    pub strategy_id: String,
    /// 交易对符号
    pub symbol: String,
    /// 信号类型：BUY/SELL/HOLD
    pub signal_type: String,
    /// 信号强度：0.0-1.0
    pub strength: f64,
    /// 建议价格
    pub suggested_price: f64,
    /// 建议数量
    pub suggested_quantity: Option<f64>,
    /// 止损价格
    pub stop_loss_price: Option<f64>,
    /// 止盈价格
    pub take_profit_price: Option<f64>,
    /// 时间戳（毫秒）
    pub timestamp: u64,
}

/// 网格触发事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GridTriggerEvent {
    /// 网格 ID
    pub grid_id: String,
    /// 策略 ID
    pub strategy_id: String,
    /// 交易对符号
    pub symbol: String,
    /// 网格级别
    pub grid_level: i32,
    /// 触发价格
    pub trigger_price: f64,
    /// 操作：BUY/SELL
    pub action: GridAction,
    /// 数量
    pub quantity: f64,
    /// 预期利润
    pub expected_profit: f64,
    /// 时间戳（毫秒）
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GridAction {
    Buy,
    Sell,
}

/// 网格状态变更事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GridStateChangeEvent {
    /// 策略 ID
    pub strategy_id: String,
    /// 交易对符号
    pub symbol: String,
    /// 旧状态
    pub old_state: String,
    /// 新状态
    pub new_state: String,
    /// 变更原因
    pub reason: String,
    /// 当前持仓量
    pub current_position: f64,
    /// 当前未实现盈亏
    pub unrealized_pnl: f64,
    /// 时间戳（毫秒）
    pub timestamp: u64,
}

impl GridTriggerEvent {
    pub fn new(
        strategy_id: String,
        symbol: String,
        grid_level: i32,
        trigger_price: f64,
        action: GridAction,
        quantity: f64,
    ) -> Self {
        Self {
            grid_id: format!("grid_{}_{}", strategy_id, grid_level),
            strategy_id,
            symbol,
            grid_level,
            trigger_price,
            action,
            quantity,
            expected_profit: 0.0, // 待计算
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trading_signal_creation() {
        let event = TradingSignalEvent {
            signal_id: "signal_001".to_string(),
            strategy_id: "strategy_grid_1".to_string(),
            symbol: "BTCUSDT".to_string(),
            signal_type: "BUY".to_string(),
            strength: 0.8,
            suggested_price: 50000.0,
            suggested_quantity: Some(0.001),
            stop_loss_price: Some(49000.0),
            take_profit_price: Some(52000.0),
            timestamp: 1234567890000,
        };
        
        assert_eq!(event.signal_type, "BUY");
        assert_eq!(event.strength, 0.8);
        assert_eq!(event.suggested_price, 50000.0);
    }

    #[test]
    fn test_grid_trigger_creation() {
        let event = GridTriggerEvent::new(
            "grid_strategy_1".to_string(),
            "BTCUSDT".to_string(),
            5,
            49500.0,
            GridAction::Buy,
            0.001,
        );
        
        assert_eq!(event.grid_level, 5);
        assert_eq!(event.trigger_price, 49500.0);
        matches!(event.action, GridAction::Buy);
    }
}
