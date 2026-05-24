//! 订单执行服务
//!
//! 接收交易信号，通过风控检查后调用 Binance API 下单

use std::sync::Arc;
use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::event_bus::{EventBus, EventHandler, EventType, TokioEventBus};
use crate::events::{
    DomainEvent, TradingSignalEvent, OrderFilledEvent, OrderRejectedEvent
};
use crate::error::{DomainError, EventBusError, ServiceError};
use crate::clients::BinanceClient;
use crate::risk::RiskMonitorService;

/// 账户余额缓存
#[derive(Debug, Clone)]
pub struct AccountBalance {
    pub available_usdt: f64,
    pub locked_usdt: f64,
    pub btc_free: f64,
    pub btc_locked: f64,
}

impl AccountBalance {
    pub fn new() -> Self {
        Self {
            available_usdt: 0.0,
            locked_usdt: 0.0,
            btc_free: 0.0,
            btc_locked: 0.0,
        }
    }
    
    /// 当前持仓价值估算 (BTC数量 * 当前价)
    pub fn position_value(&self, btc_price: f64) -> f64 {
        (self.btc_free + self.btc_locked) * btc_price
    }
}

/// 订单执行服务
pub struct OrderExecutionService {
    client: BinanceClient,
    risk_service: RiskMonitorService,
    event_bus: Arc<TokioEventBus>,
    symbol: String,
    balance: Arc<Mutex<AccountBalance>>,
}

impl OrderExecutionService {
    /// 创建新的订单执行服务
    pub fn new(
        client: BinanceClient,
        risk_service: RiskMonitorService,
        event_bus: Arc<TokioEventBus>,
        symbol: String,
    ) -> Self {
        Self {
            client,
            risk_service,
            event_bus,
            symbol,
            balance: Arc::new(Mutex::new(AccountBalance::new())),
        }
    }

    /// 启动时同步账户余额
    pub async fn sync_balance(&self) -> Result<(), DomainError> {
        match self.client.get_account().await {
            Ok(account) => {
                let mut bal = self.balance.lock().await;
                for b in &account.balances {
                    match b.asset.as_str() {
                        "USDT" => {
                            bal.available_usdt = b.free.parse().unwrap_or(0.0);
                            bal.locked_usdt = b.locked.parse().unwrap_or(0.0);
                        }
                        "BTC" => {
                            bal.btc_free = b.free.parse().unwrap_or(0.0);
                            bal.btc_locked = b.locked.parse().unwrap_or(0.0);
                        }
                        _ => {}
                    }
                }
                log::info!("账户余额同步 | USDT: {:.2} | BTC: {:.6}",
                    bal.available_usdt, bal.btc_free);
                Ok(())
            }
            Err(e) => {
                log::error!("账户余额同步失败: {}", e);
                Err(e)
            }
        }
    }

    /// 执行交易信号 - 使用市价单快速成交
    pub async fn execute_signal(
        &self,
        signal: &TradingSignalEvent,
    ) -> Result<(), DomainError> {
        log::info!("执行交易信号: {} | {} @ {:.2} | 数量: {:.6}",
            signal.signal_type, signal.symbol, signal.suggested_price,
            signal.suggested_quantity.unwrap_or(0.0));

        // 1. 计算订单金额
        let quantity = signal.suggested_quantity.ok_or_else(|| {
            ServiceError::Order("交易信号缺少数量".to_string())
        })?;
        
        let order_amount = quantity * signal.suggested_price;

        // 2. 获取真实账户余额
        let bal = self.balance.lock().await;
        let available_balance = bal.available_usdt;
        let current_position = bal.position_value(signal.suggested_price);
        drop(bal);

        // 3. 风控检查
        log::info!("风控检查: 余额={:.2} USDT, 持仓={:.2} USDT, 订单={:.2} USDT",
            available_balance, current_position, order_amount);
        if let Err(alert) = self.risk_service.pre_trade_check(
            order_amount,
            available_balance,
            current_position,
        ).await {
            log::warn!("风控拦截 | {} | {} | 金额: {:.2} | 原因: {}",
                signal.symbol, signal.signal_type, order_amount, alert.message);
            
            let reject_event = DomainEvent::OrderRejected(OrderRejectedEvent {
                order_id: Some(signal.signal_id.clone()),
                symbol: signal.symbol.clone(),
                reason: format!("风控拦截: {}", alert.message),
                error_code: None,
                timestamp: chrono::Utc::now().timestamp_millis() as u64,
            });
            
            self.event_bus.publish(reject_event).await
                .map_err(|e| ServiceError::Order(format!("发布事件失败: {}", e)))?;
            
            return Err(ServiceError::Order(format!("风控拦截: {}", alert.message)).into());
        }
        
        
        // 4. 调用 API 下单 - 使用市价单快速成交
        let side = if signal.signal_type == "BUY" { "BUY" } else { "SELL" };
        
        let order_result = self.client.place_order(
            &signal.symbol,
            side,
            "MARKET",  // 市价单快速成交
            quantity,
            None,       // 市价单不需要价格
            None,       // 市价单不需要TIF
        ).await?;

        // 5. 记录订单到风控
        self.risk_service.record_order(order_amount).await;

        // 6. 解析实际成交数据
        let fill_price = order_result.price.parse::<f64>()
            .unwrap_or(signal.suggested_price); // 市价单可能返回0，用信号价格备用
        let fill_qty = order_result.executed_qty.parse::<f64>()
            .unwrap_or(quantity);
        let actual_quote_qty = order_result.cummulative_quote_qty.parse::<f64>()
            .unwrap_or(order_amount); // 实际花费/收入的 USDT

        // 7. 更新本地余额缓存（使用实际成交金额）
        {
            let mut bal = self.balance.lock().await;
            if side == "BUY" {
                bal.available_usdt -= actual_quote_qty;
                bal.btc_free += fill_qty;
            } else {
                bal.available_usdt += actual_quote_qty;
                bal.btc_free -= fill_qty;
            }
        }

        // 8. 发布订单成交事件（市价单立即成交）
        let fill_event = DomainEvent::OrderFilled(OrderFilledEvent {
            order_id: order_result.order_id.to_string(),
            fill_id: format!("fill_{}", order_result.order_id),
            fill_price,
            fill_qty,
            commission: fill_qty * fill_price * 0.001, // Binance现货手续费0.1%
            commission_asset: "USDT".to_string(),
            is_maker: false,
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
        });
        
        self.event_bus.publish(fill_event).await
            .map_err(|e| ServiceError::Order(format!("发布事件失败: {}", e)))?;

        log::info!("市价单成交 | {} | {} | 价: {:.2} | 量: {:.6} | 手续费: {:.4} | order_id: {}",
            order_result.symbol, side, fill_price, fill_qty,
            fill_qty * fill_price * 0.001, order_result.order_id);

        Ok(())
    }
}

#[async_trait]
impl EventHandler for OrderExecutionService {
    async fn handle(&self, event: &DomainEvent) -> Result<(), EventBusError> {
        if let DomainEvent::TradingSignal(signal) = event {
            // 只处理当前交易对的信号
            if signal.symbol != self.symbol {
                return Ok(());
            }
            
            log::info!("订单执行服务收到交易信号");
            
            if let Err(e) = self.execute_signal(signal).await {
                log::error!("订单执行失败: {}", e);
                
                // 发布失败事件（可选）
                // ...
            }
        }
        
        Ok(())
    }

    fn event_types(&self) -> Vec<EventType> {
        vec![EventType::TradingSignal]
    }
}

// 为了在 tokio::spawn 中使用，需要实现 Clone
impl Clone for OrderExecutionService {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            risk_service: self.risk_service.clone(),
            event_bus: self.event_bus.clone(),
            symbol: self.symbol.clone(),
            balance: self.balance.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{BinanceConfig, RiskConfig};

    #[test]
    fn test_order_execution_service_creation() {
        // 这个测试需要真实的 API 客户端，暂时跳过
        println!("OrderExecutionService 创建测试（需要真实 API 密钥）");
    }
}
