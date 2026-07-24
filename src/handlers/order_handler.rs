use crate::error::EventBusError;
use crate::event_bus::{EventHandler, EventType, TokioEventBus};
use crate::events::DomainEvent;
use async_trait::async_trait;
use serde::Serialize;
use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;

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
    evidence_path: String,
    open_positions: Arc<Mutex<HashMap<String, OpenPositionEvidence>>>,
}

#[derive(Debug, Clone)]
struct OpenPositionEvidence {
    order_id: String,
    signal_id: String,
    fill_price: f64,
    fill_qty: f64,
    actual_quote_qty: f64,
    commission: f64,
    commission_asset: String,
    timestamp: u64,
}

#[derive(Debug, Serialize)]
struct OrderSubmittedEvidence<'a> {
    evidence_type: &'static str,
    order_id: &'a str,
    symbol: &'a str,
    side: &'a str,
    order_type: &'a str,
    price: Option<f64>,
    quantity: f64,
    timestamp: u64,
}

#[derive(Debug, Serialize)]
struct OrderFilledEvidence<'a> {
    evidence_type: &'static str,
    trade_role: &'static str,
    order_id: &'a str,
    signal_id: &'a str,
    symbol: &'a str,
    side: &'a str,
    fill_id: &'a str,
    fill_price: f64,
    fill_qty: f64,
    actual_quote_qty: f64,
    commission: f64,
    commission_asset: &'a str,
    is_maker: bool,
    timestamp: u64,
}

#[derive(Debug, Serialize)]
struct ClosedTradeEvidence<'a> {
    evidence_type: &'static str,
    symbol: &'a str,
    entry_order_id: &'a str,
    exit_order_id: &'a str,
    entry_signal_id: &'a str,
    exit_signal_id: &'a str,
    entry_time: u64,
    exit_time: u64,
    entry_price: f64,
    exit_price: f64,
    entry_qty: f64,
    exit_qty: f64,
    entry_quote_qty: f64,
    exit_quote_qty: f64,
    gross_pnl_usdt: f64,
    gross_pnl_pct: f64,
    net_pnl_usdt_est: f64,
    net_pnl_pct_est: f64,
    entry_commission: f64,
    entry_commission_asset: &'a str,
    exit_commission: f64,
    exit_commission_asset: &'a str,
}

#[derive(Debug, Serialize)]
struct OrderRejectedEvidence<'a> {
    evidence_type: &'static str,
    order_id: Option<&'a str>,
    symbol: &'a str,
    reason: &'a str,
    error_code: Option<i32>,
    timestamp: u64,
}

impl OrderHandler {
    /// 创建新的订单处理器
    pub fn new(event_bus: Arc<TokioEventBus>) -> Self {
        Self {
            event_bus,
            evidence_path: "data/trade_evidence.jsonl".to_string(),
            open_positions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn append_evidence<T>(&self, record: T)
    where
        T: Serialize,
    {
        let line = match serde_json::to_string(&record) {
            Ok(line) => line,
            Err(e) => {
                log::warn!("交易证据序列化失败: {}", e);
                return;
            }
        };
        let evidence_path = self.evidence_path.clone();
        let result = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            if let Some(parent) = Path::new(&evidence_path).parent() {
                fs::create_dir_all(parent)?;
            }
            let mut file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&evidence_path)?;
            writeln!(file, "{}", line)?;
            Ok(())
        })
        .await;

        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => log::warn!("交易证据写入失败: {}", e),
            Err(e) => log::warn!("交易证据写入任务失败: {}", e),
        }
    }

    fn commission_usdt_cash(commission: f64, commission_asset: &str) -> f64 {
        if commission_asset == "USDT" {
            commission
        } else {
            0.0
        }
    }

    /// 处理订单提交事件
    async fn handle_order_submitted(&self, event: &crate::events::OrderSubmittedEvent) {
        log::info!(
            "🧾 订单提交证据 | {} {} {} | 价格:{:?} | 数量:{:.8} | 本地ID:{}",
            event.symbol,
            event.side,
            event.order_type,
            event.price,
            event.quantity,
            event.order_id
        );
        self.append_evidence(OrderSubmittedEvidence {
            evidence_type: "order_submitted",
            order_id: &event.order_id,
            symbol: &event.symbol,
            side: &event.side,
            order_type: &event.order_type,
            price: event.price,
            quantity: event.quantity,
            timestamp: event.timestamp,
        })
        .await;
    }

    /// 处理订单成交事件
    async fn handle_order_filled(&self, event: &crate::events::OrderFilledEvent) {
        let trade_role = if event.side == "BUY" { "entry" } else { "exit" };
        log::info!(
            "🧾 成交证据 | {} {} | 角色:{} | 均价:{:.2} | 量:{:.8} | 成交额:{:.4}U | 手续费:{:.8} {} | 订单:{} | 信号:{}",
            event.side,
            event.symbol,
            trade_role,
            event.fill_price,
            event.fill_qty,
            event.actual_quote_qty,
            event.commission,
            event.commission_asset,
            event.order_id,
            event.signal_id
        );

        self.append_evidence(OrderFilledEvidence {
            evidence_type: "order_filled",
            trade_role,
            order_id: &event.order_id,
            signal_id: &event.signal_id,
            symbol: &event.symbol,
            side: &event.side,
            fill_id: &event.fill_id,
            fill_price: event.fill_price,
            fill_qty: event.fill_qty,
            actual_quote_qty: event.actual_quote_qty,
            commission: event.commission,
            commission_asset: &event.commission_asset,
            is_maker: event.is_maker,
            timestamp: event.timestamp,
        })
        .await;

        if event.side == "BUY" {
            let mut positions = self.open_positions.lock().await;
            positions.insert(
                event.symbol.clone(),
                OpenPositionEvidence {
                    order_id: event.order_id.clone(),
                    signal_id: event.signal_id.clone(),
                    fill_price: event.fill_price,
                    fill_qty: event.fill_qty,
                    actual_quote_qty: event.actual_quote_qty,
                    commission: event.commission,
                    commission_asset: event.commission_asset.clone(),
                    timestamp: event.timestamp,
                },
            );
            return;
        }

        let entry = {
            let mut positions = self.open_positions.lock().await;
            positions.remove(&event.symbol)
        };

        if let Some(entry) = entry {
            let gross_pnl_usdt = event.actual_quote_qty - entry.actual_quote_qty;
            let gross_pnl_pct = if entry.actual_quote_qty > 0.0 {
                gross_pnl_usdt / entry.actual_quote_qty * 100.0
            } else {
                0.0
            };
            let cash_commission_usdt =
                Self::commission_usdt_cash(entry.commission, &entry.commission_asset)
                    + Self::commission_usdt_cash(event.commission, &event.commission_asset);
            let net_pnl_usdt_est = gross_pnl_usdt - cash_commission_usdt;
            let net_pnl_pct_est = if entry.actual_quote_qty > 0.0 {
                net_pnl_usdt_est / entry.actual_quote_qty * 100.0
            } else {
                0.0
            };

            log::info!(
                "🧾 闭环成交证据 | {} | {:.2}→{:.2} | 毛:{:+.4}U({:+.3}%) | 净估:{:+.4}U({:+.3}%) | 入场单:{} 出场单:{}",
                event.symbol,
                entry.fill_price,
                event.fill_price,
                gross_pnl_usdt,
                gross_pnl_pct,
                net_pnl_usdt_est,
                net_pnl_pct_est,
                entry.order_id,
                event.order_id
            );

            self.append_evidence(ClosedTradeEvidence {
                evidence_type: "closed_trade",
                symbol: &event.symbol,
                entry_order_id: &entry.order_id,
                exit_order_id: &event.order_id,
                entry_signal_id: &entry.signal_id,
                exit_signal_id: &event.signal_id,
                entry_time: entry.timestamp,
                exit_time: event.timestamp,
                entry_price: entry.fill_price,
                exit_price: event.fill_price,
                entry_qty: entry.fill_qty,
                exit_qty: event.fill_qty,
                entry_quote_qty: entry.actual_quote_qty,
                exit_quote_qty: event.actual_quote_qty,
                gross_pnl_usdt,
                gross_pnl_pct,
                net_pnl_usdt_est,
                net_pnl_pct_est,
                entry_commission: entry.commission,
                entry_commission_asset: &entry.commission_asset,
                exit_commission: event.commission,
                exit_commission_asset: &event.commission_asset,
            })
            .await;
        } else {
            log::warn!(
                "🧾 平仓成交缺少本地入场证据 | {} | 出场单:{} | 可用 query_trades 从 Binance 回补",
                event.symbol,
                event.order_id
            );
        }
    }

    /// 处理订单取消事件
    async fn handle_order_cancelled(&self, event: &crate::events::OrderCancelledEvent) {
        log::info!(
            "🧾 订单取消证据 | {} | 订单:{} | 原因:{}",
            event.symbol,
            event.order_id,
            event.reason
        );
    }

    /// 处理订单拒绝事件
    async fn handle_order_rejected(&self, event: &crate::events::OrderRejectedEvent) {
        log::warn!(
            "🧾 拒单证据 | {} | order_id={:?} | code={:?} | 原因:{}",
            event.symbol,
            event.order_id,
            event.error_code,
            event.reason
        );
        self.append_evidence(OrderRejectedEvidence {
            evidence_type: "order_rejected",
            order_id: event.order_id.as_deref(),
            symbol: &event.symbol,
            reason: &event.reason,
            error_code: event.error_code,
            timestamp: event.timestamp,
        })
        .await;
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
