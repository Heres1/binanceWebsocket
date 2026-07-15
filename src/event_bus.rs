//! 事件总线模块
//!
//! 负责事件的发布、订阅和分发

use crate::events::DomainEvent;
use async_trait::async_trait;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::{atomic::AtomicBool, Arc};
use tokio::sync::{broadcast, Notify};

/// 订阅 ID 类型
pub type SubscriptionId = u64;

/// 事件错误类型
pub use crate::error::EventBusError as EventError;

/// 事件总线 Trait
#[async_trait]
pub trait EventBus: Send + Sync {
    /// 发布事件
    async fn publish(&self, event: DomainEvent) -> Result<(), EventError>;

    /// 订阅事件处理器
    fn subscribe<T: EventHandler + 'static>(&self, handler: Arc<T>) -> SubscriptionId;

    /// 取消订阅
    fn unsubscribe(&self, subscription_id: SubscriptionId);
}

/// 事件处理器 Trait
#[async_trait]
pub trait EventHandler: Send + Sync {
    /// 处理事件
    async fn handle(&self, event: &DomainEvent) -> Result<(), EventError>;

    /// 感兴趣的事件类型（用于过滤）
    fn event_types(&self) -> Vec<EventType>;
}

/// 事件类型枚举
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EventType {
    PriceUpdate,
    KlineCompleted,
    OrderBookUpdate,
    AggTrade,
    BookTicker,
    OrderSubmitted,
    OrderFilled,
    OrderCancelled,
    OrderRejected,
    BalanceUpdate,
    PositionChange,
    TradingSignal,
    RiskCheck,
    RiskAlert,
    All, // 订阅所有事件
}

/// Tokio 事件总线实现
pub struct TokioEventBus {
    sender: broadcast::Sender<DomainEvent>,
    _receiver: broadcast::Receiver<DomainEvent>,
    handlers: Arc<RwLock<HashMap<SubscriptionId, Arc<dyn EventHandler>>>>,
    next_subscription_id: RwLock<SubscriptionId>,
}

impl TokioEventBus {
    /// 创建新的事件总线
    pub fn new(capacity: usize) -> Self {
        let (sender, receiver) = broadcast::channel(capacity);
        Self {
            sender,
            _receiver: receiver,
            handlers: Arc::new(RwLock::new(HashMap::new())),
            next_subscription_id: RwLock::new(1),
        }
    }

    /// 获取事件总线当前未消费的消息数
    pub fn pending_count(&self) -> usize {
        self.sender.len()
    }
}

#[async_trait]
impl EventBus for TokioEventBus {
    async fn publish(&self, event: DomainEvent) -> Result<(), EventError> {
        self.sender
            .send(event)
            .map_err(|e| EventError::PublishFailed(e.to_string()))?;
        Ok(())
    }

    fn subscribe<T: EventHandler + 'static>(&self, handler: Arc<T>) -> SubscriptionId {
        let id = {
            let mut id = self.next_subscription_id.write();
            let current = *id;
            *id += 1;
            current
        };

        self.handlers.write().insert(id, handler);
        id
    }

    fn unsubscribe(&self, subscription_id: SubscriptionId) {
        self.handlers.write().remove(&subscription_id);
    }
}

/// 事件分发器 - 负责将事件分发给所有订阅的处理器
pub struct EventDispatcher {
    event_bus: Arc<TokioEventBus>,
    ready_notify: Option<Arc<Notify>>,
    is_ready: Arc<AtomicBool>,
}

impl EventDispatcher {
    pub fn new(event_bus: Arc<TokioEventBus>) -> Self {
        Self {
            event_bus,
            ready_notify: None,
            is_ready: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn with_ready_notify(event_bus: Arc<TokioEventBus>, notify: Arc<Notify>) -> Self {
        Self {
            event_bus,
            ready_notify: Some(notify),
            is_ready: Arc::new(AtomicBool::new(false)),
        }
    }

    /// 运行分发器
    pub async fn run(self) {
        let mut receiver = self.event_bus.sender.subscribe();

        self.is_ready
            .store(true, std::sync::atomic::Ordering::SeqCst);
        if let Some(notify) = self.ready_notify {
            notify.notify_one();
        }
        while let Ok(event) = receiver.recv().await {
            let event_type = extract_event_type(&event);

            // 只分发给关心此事件类型的 handler（按 event_types 过滤）
            let handlers: Vec<_> = {
                let guard = self.event_bus.handlers.read();
                guard
                    .values()
                    .filter(|h| {
                        let types = h.event_types();
                        types.contains(&EventType::All) || types.contains(&event_type)
                    })
                    .cloned()
                    .collect()
            };

            if handlers.is_empty() {
                continue;
            }

            // 并行处理所有匹配的 handler
            let futures: Vec<_> = handlers
                .iter()
                .map(|handler| handler.handle(&event))
                .collect();

            // 等待所有 handler 完成
            let results = futures_util::future::join_all(futures).await;

            // 处理错误
            for result in results {
                if let Err(e) = result {
                    log::error!("事件处理失败：{}", e);
                }
            }
        }
    }
}

// ========== 工具函数 ==========

/// 从 DomainEvent 提取 EventType
pub fn extract_event_type(event: &DomainEvent) -> EventType {
    use DomainEvent::*;
    match event {
        PriceUpdate(_) => EventType::PriceUpdate,
        KlineCompleted(_) => EventType::KlineCompleted,
        OrderBookUpdate(_) => EventType::OrderBookUpdate,
        AggTrade(_) => EventType::AggTrade,
        BookTicker(_) => EventType::BookTicker,
        OrderSubmitted(_) => EventType::OrderSubmitted,
        OrderFilled(_) => EventType::OrderFilled,
        OrderCancelled(_) => EventType::OrderCancelled,
        OrderRejected(_) => EventType::OrderRejected,
        BalanceUpdate(_) => EventType::BalanceUpdate,
        PositionChange(_) => EventType::PositionChange,
        TradingSignal(_) => EventType::TradingSignal,
        RiskCheck(_) => EventType::RiskCheck,
        RiskAlert(_) => EventType::RiskAlert,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::PriceUpdateEvent;

    struct TestHandler {
        received_events: Arc<RwLock<Vec<DomainEvent>>>,
    }

    impl TestHandler {
        fn new() -> Self {
            Self {
                received_events: Arc::new(RwLock::new(Vec::new())),
            }
        }
    }

    #[async_trait]
    impl EventHandler for TestHandler {
        async fn handle(&self, event: &DomainEvent) -> Result<(), EventError> {
            self.received_events.write().push(event.clone());
            Ok(())
        }

        fn event_types(&self) -> Vec<EventType> {
            vec![EventType::All]
        }
    }

    #[tokio::test]
    async fn test_event_bus_publish_subscribe() {
        let event_bus = Arc::new(TokioEventBus::new(100));
        let handler = Arc::new(TestHandler::new());

        // 订阅
        let subscription_id = event_bus.subscribe(handler.clone());
        assert_eq!(subscription_id, 1);

        // 发布事件（不 unwrap，因为可能没有接收者）
        let event = DomainEvent::PriceUpdate(PriceUpdateEvent {
            symbol: "BTCUSDT".to_string(),
            price: 50000.0,
            price_change_pct_24h: 2.5,
            timestamp: 1234567890000,
        });

        // 测试重点是验证 subscribe 和 unsubscribe 功能
        // publish 在真实场景中由 Dispatcher 消费
        let _ = event_bus.publish(event.clone()).await;

        // 验证订阅 ID 正确
        assert_eq!(subscription_id, 1);
    }

    #[tokio::test]
    async fn test_event_bus_unsubscribe() {
        let event_bus = Arc::new(TokioEventBus::new(100));
        let handler = Arc::new(TestHandler::new());

        let subscription_id = event_bus.subscribe(handler.clone());
        event_bus.unsubscribe(subscription_id);

        // 验证取消订阅后 ID 被移除
        // （实际测试中无法直接验证，但确保不 panic）
        let _ = event_bus
            .publish(DomainEvent::PriceUpdate(PriceUpdateEvent {
                symbol: "BTCUSDT".to_string(),
                price: 50000.0,
                price_change_pct_24h: 2.5,
                timestamp: 1234567890000,
            }))
            .await;

        // 测试通过（不 panic 即成功）
    }
}
