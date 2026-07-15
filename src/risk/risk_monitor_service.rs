//! 风控监控服务
//!
//! 实时监控风险指标，发布风控事件

use async_trait::async_trait;
use std::sync::Arc;

use crate::config::RiskConfig;
use crate::error::EventBusError;
use crate::event_bus::{EventBus, EventHandler, EventType, TokioEventBus};
use crate::events::DomainEvent;
use crate::risk::rules::RiskRules;

/// 风控监控服务
pub struct RiskMonitorService {
    pub rules: RiskRules,
    event_bus: Arc<TokioEventBus>,
}

impl Clone for RiskMonitorService {
    fn clone(&self) -> Self {
        Self {
            rules: self.rules.clone(), // 需要 RiskRules 实现 Clone
            event_bus: self.event_bus.clone(),
        }
    }
}

impl RiskMonitorService {
    /// 创建新的风控监控服务
    pub fn new(config: RiskConfig, event_bus: Arc<TokioEventBus>) -> Self {
        Self {
            rules: RiskRules::new(config),
            event_bus,
        }
    }

    /// 执行交易前风控检查
    pub async fn pre_trade_check(
        &self,
        order_amount: f64,
        available_balance: f64,
        current_position: f64,
    ) -> Result<(), crate::events::RiskAlertEvent> {
        let result = self
            .rules
            .pre_trade_check(order_amount, available_balance, current_position)
            .await;

        match result {
            Ok(_) => Ok(()),
            Err(alert) => {
                // 发布风控告警事件
                let event = DomainEvent::RiskAlert(alert.clone());
                if let Err(e) = self.event_bus.publish(event).await {
                    log::error!("发布风控告警事件失败: {}", e);
                }

                Err(alert)
            }
        }
    }

    /// 获取风控配置快照
    pub fn config(&self) -> RiskConfig {
        self.rules.config().clone()
    }

    /// 记录订单
    pub async fn record_order(&self, amount: f64) {
        self.rules.record_order(amount).await;
    }

    /// 更新亏损
    pub async fn update_loss(&self, loss: f64) {
        self.rules.update_loss(loss).await;
    }

    /// 更新持仓
    pub async fn update_position(&self, position: f64) {
        self.rules.update_position(position).await;
    }
}

#[async_trait]
impl EventHandler for RiskMonitorService {
    async fn handle(&self, event: &DomainEvent) -> Result<(), EventBusError> {
        match event {
            // 监听账户余额更新
            DomainEvent::BalanceUpdate(balance_event) => {
                log::debug!(
                    "风控监控: 余额更新 {} - 可用: {}, 冻结: {}",
                    balance_event.asset,
                    balance_event.available_balance,
                    balance_event.locked_balance
                );
            }

            // 监听订单成交
            DomainEvent::OrderFilled(fill_event) => {
                log::debug!(
                    "风控监控: 订单成交 #{} - 价格: {}, 数量: {}",
                    fill_event.order_id,
                    fill_event.fill_price,
                    fill_event.fill_qty
                );

                // TODO: 计算盈亏并更新风控状态
            }

            // 监听持仓变动
            DomainEvent::PositionChange(position_event) => {
                log::debug!(
                    "风控监控: 持仓变动 {} - 数量: {}, 未实现盈亏: {}",
                    position_event.symbol,
                    position_event.quantity,
                    position_event.unrealized_pnl
                );

                // 更新持仓
                self.update_position(position_event.quantity).await;
            }

            _ => {
                // 忽略不感兴趣的事件
            }
        }

        Ok(())
    }

    fn event_types(&self) -> Vec<EventType> {
        vec![
            EventType::BalanceUpdate,
            EventType::OrderFilled,
            EventType::PositionChange,
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RiskConfig;

    #[tokio::test]
    async fn test_risk_monitor_creation() {
        let event_bus = Arc::new(TokioEventBus::new(100));
        let config = RiskConfig {
            max_position_usdt: 1000.0,
            max_single_order_usdt: 100.0,
            max_daily_loss_usdt: 50.0,
            min_order_interval_secs: 10,
            position_allocation_pct: 0.985,
            min_usdt_reserve: 2.0,
        };

        let service = RiskMonitorService::new(config, event_bus);

        // 验证服务创建成功
        assert!(service.rules.get_state().await.daily_order_count == 0);
    }

    #[tokio::test]
    async fn test_pre_trade_check() {
        let event_bus = Arc::new(TokioEventBus::new(100));
        let config = RiskConfig {
            max_position_usdt: 1000.0,
            max_single_order_usdt: 100.0,
            max_daily_loss_usdt: 50.0,
            min_order_interval_secs: 10,
            position_allocation_pct: 0.985,
            min_usdt_reserve: 2.0,
        };

        let service = RiskMonitorService::new(config, event_bus);

        // 正常情况应该通过
        let result = service.pre_trade_check(50.0, 500.0, 200.0).await;
        assert!(result.is_ok());
    }
}
