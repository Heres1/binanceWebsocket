use crate::error::EventBusError;
use crate::event_bus::{EventHandler, EventType, TokioEventBus};
use crate::events::DomainEvent;
use async_trait::async_trait;
use std::sync::Arc;

/// 订单处理器
///
/// 负责处理订单相关事件：
/// - OrderSubmitted: 订单提交
/// - OrderFilled: 订单成交
/// - OrderCancelled: 订单取消
/// - OrderRejected: 订单拒绝
pub struct OrderHandler {
    #[allow(dead_code)]
    event_bus: Arc<TokioEventBus>,
}

impl OrderHandler {
    /// 创建新的订单处理器
    pub fn new(event_bus: Arc<TokioEventBus>) -> Self {
        Self { event_bus }
    }

    /// 处理订单提交事件
    async fn handle_order_submitted(&self, event: &crate::events::OrderSubmittedEvent) {
        log::debug!(
            "订单已提交: order_id={}, symbol={}, side={}, price={:?}, qty={}",
            event.order_id,
            event.symbol,
            event.side,
            event.price,
            event.quantity
        );
        // TODO: 持久化到数据库、更新订单状态等
    }

    /// 处理订单成交事件
    async fn handle_order_filled(&self, event: &crate::events::OrderFilledEvent) {
        log::debug!(
            "订单已成交: order_id={}, signal_id={}, symbol={}, side={}, fill_id={}, price={}, qty={}, amount={}, commission={} {}",
            event.order_id,
            event.signal_id,
            event.symbol,
            event.side,
            event.fill_id,
            event.fill_price,
            event.fill_qty,
            event.actual_quote_qty,
            event.commission,
            event.commission_asset
        );
        // TODO: 更新持仓、计算盈亏、触发后续策略等
    }

    /// 处理订单取消事件
    async fn handle_order_cancelled(&self, event: &crate::events::OrderCancelledEvent) {
        log::debug!(
            "订单已取消: order_id={}, symbol={}, reason={}",
            event.order_id,
            event.symbol,
            event.reason
        );
        // TODO: 更新订单状态、释放冻结资金等
    }

    /// 处理订单拒绝事件
    async fn handle_order_rejected(&self, event: &crate::events::OrderRejectedEvent) {
        log::warn!(
            "订单被拒绝: order_id={:?}, symbol={}, reason={:?}",
            event.order_id,
            event.symbol,
            event.reason
        );
        // TODO: 记录错误、通知用户、触发风控等
    }
}

#[async_trait]
impl EventHandler for OrderHandler {
    async fn handle(&self, event: &DomainEvent) -> Result<(), EventBusError> {
        match event {
            DomainEvent::OrderSubmitted(e) => self.handle_order_submitted(e).await,
            DomainEvent::OrderFilled(e) => self.handle_order_filled(e).await,
            DomainEvent::OrderCancelled(e) => self.handle_order_cancelled(e).await,
            DomainEvent::OrderRejected(e) => self.handle_order_rejected(e).await,
            _ => {
                // 忽略不感兴趣的事件
            }
        }
        Ok(())
    }

    fn event_types(&self) -> Vec<EventType> {
        vec![
            EventType::OrderSubmitted,
            EventType::OrderFilled,
            EventType::OrderCancelled,
            EventType::OrderRejected,
        ]
    }
}
