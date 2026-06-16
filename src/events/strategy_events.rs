//! 策略事件
//! 
//! 包含交易信号事件

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

