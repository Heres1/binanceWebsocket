//! 事件定义模块
//! 
//! 包含系统中所有领域事件的定义

mod market_events;
mod trading_events;
mod account_events;
mod strategy_events;
mod risk_events;

pub use market_events::*;
pub use trading_events::*;
pub use account_events::*;
pub use strategy_events::*;
pub use risk_events::*;

use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use uuid::Uuid;

/// 事件元数据 - 每个事件都包含的通用信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventMetadata {
    /// 事件唯一 ID
    pub event_id: String,
    /// 事件发生时间
    pub timestamp: DateTime<Utc>,
    /// 事件版本
    pub version: String,
    /// 关联 ID - 用于追踪相关事件
    pub correlation_id: Option<String>,
    /// 因果 ID - 触发此事件的上一个事件 ID
    pub causation_id: Option<String>,
}

impl Default for EventMetadata {
    fn default() -> Self {
        Self {
            event_id: Uuid::new_v4().to_string(),
            timestamp: Utc::now(),
            version: "1.0".to_string(),
            correlation_id: None,
            causation_id: None,
        }
    }
}

/// 统一事件枚举 - 系统能处理的所有事件类型
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event_type", content = "payload")]
pub enum DomainEvent {
    // ========== 市场数据事件 ==========
    /// 价格更新
    PriceUpdate(PriceUpdateEvent),
    /// K 线完成
    KlineCompleted(KlineCompletedEvent),
    /// 订单簿更新
    OrderBookUpdate(OrderBookUpdateEvent),
    /// 聚合成交
    AggTrade(AggTradeEvent),
    /// 最优买卖价
    BookTicker(BookTickerEvent),
    
    // ========== 交易事件 ==========
    /// 订单提交
    OrderSubmitted(OrderSubmittedEvent),
    /// 订单成交
    OrderFilled(OrderFilledEvent),
    /// 订单取消
    OrderCancelled(OrderCancelledEvent),
    /// 订单拒绝
    OrderRejected(OrderRejectedEvent),
    
    // ========== 账户事件 ==========
    /// 余额更新
    BalanceUpdate(BalanceUpdateEvent),
    /// 持仓变动
    PositionChange(PositionChangeEvent),
    
    // ========== 策略事件 ==========
    /// 交易信号
    TradingSignal(TradingSignalEvent),
    
    // ========== 风控事件 ==========
    /// 风控检查
    RiskCheck(RiskCheckEvent),
    /// 风控告警
    RiskAlert(RiskAlertEvent),
}

/// 可存储的事件 - 用于事件溯源
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorableEvent {
    /// 序列号 - 全局递增
    pub sequence: u64,
    /// 流 ID - 聚合根的 ID
    pub stream_id: String,
    /// 事件数据
    pub event: DomainEvent,
    /// 事件元数据
    pub metadata: EventMetadata,
}