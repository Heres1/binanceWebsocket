//! 账户事件
//!
//! 包含余额更新、持仓变动等事件

use serde::{Deserialize, Serialize};

/// 余额更新事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BalanceUpdateEvent {
    /// 资产符号，如 "BTC", "USDT"
    pub asset: String,
    /// 可用余额
    pub available_balance: f64,
    /// 冻结余额
    pub locked_balance: f64,
    /// 总余额
    pub total_balance: f64,
    /// 时间戳（毫秒）
    pub timestamp: u64,
}

impl BalanceUpdateEvent {
    pub fn new(asset: String, available: f64, locked: f64) -> Self {
        Self {
            asset,
            available_balance: available,
            locked_balance: locked,
            total_balance: available + locked,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        }
    }
}

/// 持仓变动事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionChangeEvent {
    /// 交易对符号
    pub symbol: String,
    /// 持仓方向：LONG/SHORT
    pub side: String,
    /// 持仓数量
    pub quantity: f64,
    /// 开仓均价
    pub entry_price: f64,
    /// 未实现盈亏
    pub unrealized_pnl: f64,
    /// 杠杆倍数
    pub leverage: i32,
    /// 时间戳（毫秒）
    pub timestamp: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_balance_update_creation() {
        let event = BalanceUpdateEvent::new("USDT".to_string(), 10000.0, 500.0);

        assert_eq!(event.asset, "USDT");
        assert_eq!(event.available_balance, 10000.0);
        assert_eq!(event.locked_balance, 500.0);
        assert_eq!(event.total_balance, 10500.0);
    }

    #[test]
    fn test_position_change_serialization() {
        let event = PositionChangeEvent {
            symbol: "BTCUSDT".to_string(),
            side: "LONG".to_string(),
            quantity: 0.5,
            entry_price: 45000.0,
            unrealized_pnl: 2500.0,
            leverage: 10,
            timestamp: 1234567890000,
        };

        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("BTCUSDT"));
        assert!(json.contains("LONG"));
    }
}
