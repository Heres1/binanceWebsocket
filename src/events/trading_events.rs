//! 交易事件
//! 
//! 包含订单提交、成交、取消、拒绝等事件

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// 订单提交事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderSubmittedEvent {
    /// 订单 ID
    pub order_id: String,
    /// 交易对符号
    pub symbol: String,
    /// 订单方向：BUY/SELL
    pub side: String,
    /// 订单类型：LIMIT/MARKET
    pub order_type: String,
    /// 价格（市价单为 None）
    pub price: Option<f64>,
    /// 数量
    pub quantity: f64,
    /// 时间戳（毫秒）
    pub timestamp: u64,
}

impl OrderSubmittedEvent {
    pub fn new(
        symbol: String,
        side: String,
        order_type: String,
        price: Option<f64>,
        quantity: f64,
    ) -> Self {
        Self {
            order_id: Uuid::new_v4().to_string(),
            symbol,
            side,
            order_type,
            price,
            quantity,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        }
    }
}

/// 订单成交事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderFilledEvent {
    /// 订单 ID
    pub order_id: String,
    /// 成交 ID
    pub fill_id: String,
    /// 成交价格
    pub fill_price: f64,
    /// 成交数量
    pub fill_qty: f64,
    /// 手续费
    pub commission: f64,
    /// 手续费资产
    pub commission_asset: String,
    /// 是否卖方
    pub is_maker: bool,
    /// 时间戳（毫秒）
    pub timestamp: u64,
}

/// 订单取消事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderCancelledEvent {
    /// 订单 ID
    pub order_id: String,
    /// 交易对符号
    pub symbol: String,
    /// 取消原因
    pub reason: String,
    /// 时间戳（毫秒）
    pub timestamp: u64,
}

/// 订单拒绝事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderRejectedEvent {
    /// 订单 ID（如果有）
    pub order_id: Option<String>,
    /// 交易对符号
    pub symbol: String,
    /// 拒绝原因
    pub reason: String,
    /// 错误代码
    pub error_code: Option<i32>,
    /// 时间戳（毫秒）
    pub timestamp: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_order_submitted_creation() {
        let event = OrderSubmittedEvent::new(
            "BTCUSDT".to_string(),
            "BUY".to_string(),
            "LIMIT".to_string(),
            Some(50000.0),
            0.001,
        );
        
        assert_eq!(event.symbol, "BTCUSDT");
        assert_eq!(event.side, "BUY");
        assert!(!event.order_id.is_empty());
    }

    #[test]
    fn test_order_filled_serialization() {
        let event = OrderFilledEvent {
            order_id: "order_123".to_string(),
            fill_id: "fill_456".to_string(),
            fill_price: 50000.0,
            fill_qty: 0.001,
            commission: 0.5,
            commission_asset: "USDT".to_string(),
            is_maker: false,
            timestamp: 1234567890000,
        };
        
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("order_123"));
        assert!(json.contains("50000"));
    }
}
