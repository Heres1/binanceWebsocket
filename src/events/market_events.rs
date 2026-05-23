//! 市场数据事件
//! 
//! 包含价格更新、K 线完成、订单簿更新等事件

use serde::{Deserialize, Serialize};

/// 价格更新事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceUpdateEvent {
    /// 交易对符号，如 "BTCUSDT"
    pub symbol: String,
    /// 当前价格
    pub price: f64,
    /// 24 小时涨跌幅百分比
    pub price_change_pct_24h: f64,
    /// 时间戳（毫秒）
    pub timestamp: u64,
}

/// K 线完成事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KlineCompletedEvent {
    /// 交易对符号
    pub symbol: String,
    /// K 线间隔，如 "1m", "5m", "1h"
    pub interval: String,
    /// 开盘价
    pub open: f64,
    /// 最高价
    pub high: f64,
    /// 最低价
    pub low: f64,
    /// 收盘价
    pub close: f64,
    /// 成交量
    pub volume: f64,
    /// 收盘时间（毫秒）
    pub close_time: u64,
}

/// 订单簿更新事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBookUpdateEvent {
    /// 交易对符号
    pub symbol: String,
    /// 买一价
    pub best_bid: f64,
    /// 卖一价
    pub best_ask: f64,
    /// 买一量
    pub bid_qty: f64,
    /// 卖一量
    pub ask_qty: f64,
    /// 时间戳（毫秒）
    pub timestamp: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_price_event_serialization() {
        let event = PriceUpdateEvent {
            symbol: "BTCUSDT".to_string(),
            price: 50000.0,
            price_change_pct_24h: 2.5,
            timestamp: 1234567890000,
        };
        
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("BTCUSDT"));
        assert!(json.contains("50000"));
    }

    #[test]
    fn test_kline_event_creation() {
        let event = KlineCompletedEvent {
            symbol: "ETHUSDT".to_string(),
            interval: "1h".to_string(),
            open: 3000.0,
            high: 3050.0,
            low: 2980.0,
            close: 3020.0,
            volume: 10000.0,
            close_time: 1234567890000,
        };
        
        assert_eq!(event.symbol, "ETHUSDT");
        assert_eq!(event.interval, "1h");
        assert_eq!(event.close, 3020.0);
    }
}
