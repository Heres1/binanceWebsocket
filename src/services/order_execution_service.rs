//! 订单执行服务
//!
//! 接收交易信号，通过风控检查后调用 Binance API 下单

use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

use crate::clients::binance_client::OrderResponse;
use crate::clients::BinanceClient;
use crate::error::{DomainError, EventBusError, ServiceError};
use crate::event_bus::{EventBus, EventHandler, EventType, TokioEventBus};
use crate::events::{
    DomainEvent, OrderFilledEvent, OrderRejectedEvent, OrderSubmittedEvent, TradingSignalEvent,
};
use crate::risk::RiskMonitorService;

/// 账户余额缓存
#[derive(Debug, Clone)]
pub struct AccountBalance {
    pub available_usdt: f64,
    pub locked_usdt: f64,
    pub btc_free: f64,
    pub btc_locked: f64,
    pub eth_free: f64,
    pub eth_locked: f64,
    pub sol_free: f64,
    pub sol_locked: f64,
}

impl AccountBalance {
    pub fn new() -> Self {
        Self {
            available_usdt: 0.0,
            locked_usdt: 0.0,
            btc_free: 0.0,
            btc_locked: 0.0,
            eth_free: 0.0,
            eth_locked: 0.0,
            sol_free: 0.0,
            sol_locked: 0.0,
        }
    }

    /// 当前总持仓价值估算 (基于信号价格的粗略估算)
    pub fn position_value(&self, btc_price: f64) -> f64 {
        (self.btc_free + self.btc_locked) * btc_price
    }
}

/// 订单执行服务
pub struct OrderExecutionService {
    client: BinanceClient,
    risk_service: RiskMonitorService,
    event_bus: Arc<TokioEventBus>,
    symbols: Vec<String>,
    balance: Arc<Mutex<AccountBalance>>,
    // 成本端突破：限价单入场配置
    use_limit_entry: bool,         // 开仓BUY限价单优先，超时撤单转市价
    limit_entry_offset_pct: f64,   // 挂单价低于信号价的百分比
    limit_entry_wait_seconds: u64, // 限价单最长等待秒数
}

/// 按交易对 tickSize 向下取整价格（限价单挂单价必须符合 PRICE_FILTER）
fn round_price_tick(price: f64, symbol: &str) -> f64 {
    let decimals: u32 = match symbol {
        "BTCUSDT" => 2, // tick = 0.01
        "ETHUSDT" => 2, // tick = 0.01
        "SOLUSDT" => 3, // tick = 0.001
        _ => 2,
    };
    let factor = 10_f64.powi(decimals as i32);
    ((price * factor) + 1e-9).floor() / factor
}

/// 按交易对的 LOT_SIZE stepSize 向下取整
fn round_step_size(quantity: f64, symbol: &str) -> f64 {
    let decimals: u32 = match symbol {
        "BTCUSDT" => 5, // step = 0.00001
        "ETHUSDT" => 4, // step = 0.0001
        "SOLUSDT" => 2, // step = 0.01
        _ => 5,
    };
    let factor = 10_f64.powi(decimals as i32);
    // 加极小值避免浮点截断误差（如 0.3*100=29.999... 的情况）
    ((quantity * factor) + 1e-9).floor() / factor
}

impl OrderExecutionService {
    /// 创建新的订单执行服务
    pub fn new(
        client: BinanceClient,
        risk_service: RiskMonitorService,
        event_bus: Arc<TokioEventBus>,
        symbols: Vec<String>,
        use_limit_entry: bool,
        limit_entry_offset_pct: f64,
        limit_entry_wait_seconds: u64,
    ) -> Self {
        Self {
            client,
            risk_service,
            event_bus,
            symbols,
            balance: Arc::new(Mutex::new(AccountBalance::new())),
            use_limit_entry,
            limit_entry_offset_pct,
            limit_entry_wait_seconds,
        }
    }

    /// 发布订单提交证据事件
    async fn publish_submit_event(
        &self,
        signal: &TradingSignalEvent,
        side: &str,
        order_type: &str,
        quantity: f64,
        price: Option<f64>,
    ) {
        let submit_event = DomainEvent::OrderSubmitted(OrderSubmittedEvent {
            order_id: signal.signal_id.clone(),
            symbol: signal.symbol.clone(),
            side: side.to_string(),
            order_type: order_type.to_string(),
            price,
            quantity,
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
        });
        if let Err(e) = self.event_bus.publish(submit_event).await {
            log::warn!("订单提交证据事件发布失败: {}", e);
        }
    }

    /// 限价单优先 + 超时撤单转市价兜底（成本端突破：省去市价单滑点）
    /// 返回合成 OrderResponse：混合成交时合并限价段与市价段的数量/金额，供下游统一解析
    async fn place_limit_then_market(
        &self,
        signal: &TradingSignalEvent,
        quantity: f64,
    ) -> Result<OrderResponse, DomainError> {
        let limit_price = round_price_tick(
            signal.suggested_price * (1.0 - self.limit_entry_offset_pct / 100.0),
            &signal.symbol,
        );

        self.publish_submit_event(signal, "BUY", "LIMIT", quantity, Some(limit_price))
            .await;
        let placed = self
            .client
            .place_order(
                &signal.symbol,
                "BUY",
                "LIMIT",
                quantity,
                Some(limit_price),
                Some("GTC"),
            )
            .await?;
        log::info!(
            "📥 限价买单已挂 | {} | #{} @ {:.2} | 数量: {:.5} | 等待≤{}s",
            signal.symbol,
            placed.order_id,
            limit_price,
            quantity,
            self.limit_entry_wait_seconds
        );

        // 价格穿越导致立即成交：直接返回（响应含fills佣金）
        if placed.status == "FILLED" {
            log::info!(
                "✅ 限价单立即成交 @ {:.2}（省滑点≈{:.3}%）",
                limit_price,
                self.limit_entry_offset_pct
            );
            return Ok(placed);
        }

        // 轮询等待成交
        let wait_secs = self.limit_entry_wait_seconds;
        let poll_interval = 3u64;
        let mut waited = 0u64;
        while waited < wait_secs {
            tokio::time::sleep(Duration::from_secs(poll_interval)).await;
            waited += poll_interval;
            match self
                .client
                .query_order(&signal.symbol, Some(placed.order_id), None)
                .await
            {
                Ok(q) if q.status == "FILLED" => {
                    log::info!(
                        "✅ 限价单成交 | {} | #{} @ {:.2}（等待{}s，省滑点≈{:.3}%）",
                        signal.symbol,
                        placed.order_id,
                        limit_price,
                        waited,
                        self.limit_entry_offset_pct
                    );
                    return Ok(q);
                }
                Ok(q)
                    if q.status == "CANCELED"
                        || q.status == "EXPIRED"
                        || q.status == "REJECTED" =>
                {
                    log::warn!("限价单状态异常: {}，转市价兜底", q.status);
                    break;
                }
                Ok(_) => {} // NEW / PARTIALLY_FILLED 继续等待
                Err(e) => {
                    log::warn!("限价单查询失败: {}，继续等待", e);
                }
            }
        }

        // 超时未完全成交：撤单
        log::info!(
            "⏱️ 限价单{}s未完全成交，撤单转市价 | {} | #{}",
            wait_secs,
            signal.symbol,
            placed.order_id
        );
        if let Err(e) = self
            .client
            .cancel_order(&signal.symbol, Some(placed.order_id), None)
            .await
        {
            log::warn!("限价单撤单失败: {}（可能撤单瞬间已成交）", e);
        }
        // 撤单后确认限价段实际成交量
        let final_state = self
            .client
            .query_order(&signal.symbol, Some(placed.order_id), None)
            .await
            .ok();
        let limit_filled_qty = final_state
            .as_ref()
            .and_then(|q| q.executed_qty.parse::<f64>().ok())
            .unwrap_or(0.0);
        let limit_quote = final_state
            .as_ref()
            .and_then(|q| q.cummulative_quote_qty.parse::<f64>().ok())
            .unwrap_or(0.0);

        let remaining = round_step_size(quantity - limit_filled_qty, &signal.symbol);
        if remaining <= 0.0 {
            log::info!("✅ 撤单时限价单已完全成交 @ {:.2}", limit_price);
            return Ok(final_state.unwrap_or(placed));
        }

        // 剩余数量转市价
        self.publish_submit_event(signal, "BUY", "MARKET", remaining, None)
            .await;
        let market = self
            .client
            .place_order(&signal.symbol, "BUY", "MARKET", remaining, None, None)
            .await?;

        // 合并两笔成交
        let market_qty = market.executed_qty.parse::<f64>().unwrap_or(remaining);
        let market_quote = market.cummulative_quote_qty.parse::<f64>().unwrap_or(0.0);
        let total_qty = limit_filled_qty + market_qty;
        let total_quote = limit_quote + market_quote;
        log::info!(
            "🔀 混合成交 | 限价 {:.5} @ {:.2} + 市价 {:.5} @ {:.2} | 总量: {:.5}",
            limit_filled_qty,
            limit_price,
            market_qty,
            if market_qty > 0.0 {
                market_quote / market_qty
            } else {
                0.0
            },
            total_qty
        );
        // 以市价响应为载体合并总量；fills 仅含市价段，限价段佣金由下游按BNB费率补估
        let mut merged = market;
        merged.executed_qty = total_qty.to_string();
        merged.cummulative_quote_qty = total_quote.to_string();
        Ok(merged)
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
                        "ETH" => {
                            bal.eth_free = b.free.parse().unwrap_or(0.0);
                            bal.eth_locked = b.locked.parse().unwrap_or(0.0);
                        }
                        "SOL" => {
                            bal.sol_free = b.free.parse().unwrap_or(0.0);
                            bal.sol_locked = b.locked.parse().unwrap_or(0.0);
                        }
                        _ => {}
                    }
                }
                log::info!(
                    "账户余额初始化 | USDT: {:.2} | BTC: {:.6} | ETH: {:.4} | SOL: {:.2}",
                    bal.available_usdt,
                    bal.btc_free,
                    bal.eth_free,
                    bal.sol_free
                );
                Ok(())
            }
            Err(e) => {
                log::error!("账户余额同步失败: {}", e);
                Err(e)
            }
        }
    }

    /// 启动定期余额同步任务（带指数退避+日志防抖）
    pub fn start_balance_sync_task(&self) {
        let client = self.client.clone();
        let balance = self.balance.clone();

        tokio::spawn(async move {
            let base_interval = 60u64;
            let mut current_interval = base_interval;
            let max_interval = 300u64; // 失败时最大间隔 5 分钟
            let mut consecutive_failures: u32 = 0;

            // 等待第一个周期
            tokio::time::sleep(Duration::from_secs(base_interval)).await;

            loop {
                // 速率限制检查：如果客户端处于冷却期，直接跳过本次同步
                if client.is_rate_limited() {
                    let remaining = client.rate_limit_remaining_secs();
                    if consecutive_failures == 0 {
                        log::warn!("余额同步跳过: API冷却中，剩余{}s", remaining);
                    }
                    tokio::time::sleep(Duration::from_secs(remaining.max(60))).await;
                    continue;
                }

                match client.get_account().await {
                    Ok(account) => {
                        let mut bal = balance.lock().await;
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
                                "ETH" => {
                                    bal.eth_free = b.free.parse().unwrap_or(0.0);
                                    bal.eth_locked = b.locked.parse().unwrap_or(0.0);
                                }
                                "SOL" => {
                                    bal.sol_free = b.free.parse().unwrap_or(0.0);
                                    bal.sol_locked = b.locked.parse().unwrap_or(0.0);
                                }
                                _ => {}
                            }
                        }
                        log::debug!(
                            "定期余额同步 | USDT: {:.2} | BTC: {:.6}",
                            bal.available_usdt,
                            bal.btc_free
                        );
                        // 成功时重置退避
                        if consecutive_failures > 0 {
                            log::info!("余额同步恢复正常 | 之前连续失败{}次", consecutive_failures);
                        }
                        consecutive_failures = 0;
                        current_interval = base_interval;
                    }
                    Err(e) => {
                        consecutive_failures += 1;
                        // 日志防抖：前3次每次都写，之后每10次写一次
                        if consecutive_failures <= 3 || consecutive_failures % 10 == 0 {
                            log::warn!(
                                "定期余额同步失败(连续{}次): {} | 下次重试: {}s后",
                                consecutive_failures,
                                e,
                                current_interval * 2
                            );
                        }
                        // 指数退避: 60 → 120 → 240 → 300(封顶)
                        current_interval = (current_interval * 2).min(max_interval);
                    }
                }
                tokio::time::sleep(Duration::from_secs(current_interval)).await;
            }
        });
    }

    /// 执行交易信号 - 使用市价单快速成交
    pub async fn execute_signal(&self, signal: &TradingSignalEvent) -> Result<(), DomainError> {
        // 1. 判断交易方向与是否为平仓操作
        let is_exit = signal.signal_type == "SELL" || signal.signal_type == "COVER";
        let side = if signal.signal_type == "BUY" || signal.signal_type == "COVER" {
            "BUY"
        } else {
            "SELL"
        };
        // SELL=卖出资产(平多)，只有SELL需要检查资产余额
        let needs_asset_check = signal.signal_type == "SELL";

        let risk_config = self.risk_service.config();
        let (available_balance, current_position) = {
            let bal = self.balance.lock().await;
            (
                bal.available_usdt,
                bal.position_value(signal.suggested_price),
            )
        };

        let mut quantity = match signal.suggested_quantity {
            Some(q) if q > 0.0 => q,
            _ if signal.signal_type == "BUY" => {
                let alloc_cap = available_balance * risk_config.position_allocation_pct;
                let reserve_cap = (available_balance - risk_config.min_usdt_reserve).max(0.0);
                let deployable_usdt = alloc_cap
                    .min(reserve_cap)
                    .min(risk_config.max_single_order_usdt);
                if deployable_usdt <= 0.0 || signal.suggested_price <= 0.0 {
                    return Err(ServiceError::Order(format!(
                        "动态仓位无法下单: 可用 {:.2} USDT，缓冲 {:.2} USDT，价格 {:.2}",
                        available_balance, risk_config.min_usdt_reserve, signal.suggested_price
                    ))
                    .into());
                }
                let dynamic_qty = deployable_usdt / signal.suggested_price;
                log::info!(
                    "动态仓位计算 | {} | 可用:{:.2}U 使用率:{:.1}% 缓冲:{:.2}U 可投:{:.2}U 价格:{:.2} 数量:{:.6}",
                    signal.symbol,
                    available_balance,
                    risk_config.position_allocation_pct * 100.0,
                    risk_config.min_usdt_reserve,
                    deployable_usdt,
                    signal.suggested_price,
                    dynamic_qty
                );
                dynamic_qty
            }
            _ => return Err(ServiceError::Order("交易信号缺少数量".to_string()).into()),
        };

        if signal.signal_type == "BUY" {
            let alloc_cap = available_balance * risk_config.position_allocation_pct;
            let reserve_cap = (available_balance - risk_config.min_usdt_reserve).max(0.0);
            let deployable_usdt = alloc_cap
                .min(reserve_cap)
                .min(risk_config.max_single_order_usdt);
            let max_affordable_qty = if signal.suggested_price > 0.0 {
                deployable_usdt / signal.suggested_price
            } else {
                0.0
            };
            if quantity > max_affordable_qty {
                log::info!(
                    "动态仓位下调 | {} | 信号量:{:.6} -> 可负担量:{:.6} | 可用:{:.2}U 可投:{:.2}U",
                    signal.symbol,
                    quantity,
                    max_affordable_qty,
                    available_balance,
                    deployable_usdt
                );
                quantity = max_affordable_qty;
            }
        }

        // 2. 对卖出平仓(SELL)，使用实际持有量（扣除手续费后的真实余额）
        let actual_quantity = if needs_asset_check {
            let bal = self.balance.lock().await;
            let asset_free = match signal.symbol.as_str() {
                "BTCUSDT" => bal.btc_free,
                "ETHUSDT" => bal.eth_free,
                "SOLUSDT" => bal.sol_free,
                _ => quantity,
            };
            drop(bal);
            let q = quantity.min(asset_free);
            if q < quantity * 0.5 {
                log::error!(
                    "平仓异常 | {} | 信号量: {:.6} | 实际持有: {:.6}，跳过",
                    signal.symbol,
                    quantity,
                    asset_free
                );
                return Err(ServiceError::Order(format!(
                    "持有量不足: 需要 {:.6}，实际 {:.6}",
                    quantity, asset_free
                ))
                .into());
            }
            let rounded = round_step_size(q, &signal.symbol);
            if rounded <= 0.0 {
                log::error!(
                    "平仓异常 | {} | 取整后数量为0 | 原始: {:.6}",
                    signal.symbol,
                    q
                );
                return Err(ServiceError::Order(format!("取整后数量为0: 原始 {:.6}", q)).into());
            }
            if rounded < q {
                log::info!(
                    "平仓量调整 | {} | {:.6} -> {:.6} (LOT_SIZE取整)",
                    signal.symbol,
                    q,
                    rounded
                );
            }
            rounded
        } else {
            let rounded = round_step_size(quantity, &signal.symbol);
            if rounded <= 0.0 {
                return Err(ServiceError::Order(format!(
                    "下单数量过小: 原始 {:.8}，取整后为0",
                    quantity
                ))
                .into());
            }
            rounded
        };

        let order_amount = actual_quantity * signal.suggested_price;

        // 3. 仅对开仓操作做风控检查（平仓操作跳过，确保能及时止损/止盈）
        if !is_exit {
            log::debug!(
                "风控检查: 余额={:.2} USDT, 持仓={:.2} USDT, 订单={:.2} USDT",
                available_balance,
                current_position,
                order_amount
            );
            if let Err(alert) = self
                .risk_service
                .pre_trade_check(order_amount, available_balance, current_position)
                .await
            {
                log::warn!(
                    "风控拦截 | {} | {} | 金额: {:.2} | 原因: {}",
                    signal.symbol,
                    signal.signal_type,
                    order_amount,
                    alert.message
                );
                log::info!(
                    "本次信号已拒绝并通知策略回滚 | {} | {} | 可用资金不足时不会继续提交订单",
                    signal.symbol,
                    signal.signal_type
                );

                let reject_event = DomainEvent::OrderRejected(OrderRejectedEvent {
                    order_id: Some(signal.signal_id.clone()),
                    symbol: signal.symbol.clone(),
                    reason: format!("风控拦截: {}", alert.message),
                    error_code: None,
                    timestamp: chrono::Utc::now().timestamp_millis() as u64,
                });

                self.event_bus
                    .publish(reject_event)
                    .await
                    .map_err(|e| ServiceError::Order(format!("发布事件失败: {}", e)))?;

                return Ok(());
            }
        }

        // 4. 调用 API 下单
        // 开仓BUY且启用限价模式：限价单优先（省滑点），超时未成交撤单转市价兜底；
        // 平仓SELL始终市价单（止损止盈时效优先）
        let use_limit = side == "BUY" && !is_exit && self.use_limit_entry;
        let order_result = if use_limit {
            match self.place_limit_then_market(&signal, actual_quantity).await {
                Ok(r) => r,
                Err(e) => {
                    log::error!("限价优先下单失败，回退市价单: {}", e);
                    self.publish_submit_event(&signal, side, "MARKET", actual_quantity, None)
                        .await;
                    self.client
                        .place_order(&signal.symbol, side, "MARKET", actual_quantity, None, None)
                        .await?
                }
            }
        } else {
            self.publish_submit_event(&signal, side, "MARKET", actual_quantity, None)
                .await;
            self.client
                .place_order(
                    &signal.symbol,
                    side,
                    "MARKET", // 市价单快速成交
                    actual_quantity,
                    None, // 市价单不需要价格
                    None, // 市价单不需要TIF
                )
                .await?
        };

        // 5. 记录订单到风控
        self.risk_service.record_order(order_amount).await;

        // 6. 解析实际成交数据
        let fill_qty = order_result.executed_qty.parse::<f64>().unwrap_or(quantity);
        let actual_quote_qty = order_result
            .cummulative_quote_qty
            .parse::<f64>()
            .unwrap_or(order_amount); // 实际花费/收入的 USDT
                                      // 市价单实际成交均价 = 总成交额 / 总成交量
        let fill_price = if fill_qty > 0.0 {
            actual_quote_qty / fill_qty
        } else {
            signal.suggested_price
        };

        // 从API返回的fills中获取实际手续费
        // 注意：混合成交时fills仅含市价段，限价段佣金按BNB抵扣费率0.075%补估；
        // 限价单等待成交后query_order不含fills，同样按BNB费率估算
        let (commission, commission_asset) = if let Some(ref fills) = order_result.fills {
            let total_commission: f64 = fills
                .iter()
                .filter_map(|f| f.commission.parse::<f64>().ok())
                .sum();
            let asset = fills
                .first()
                .map(|f| f.commission_asset.clone())
                .unwrap_or_else(|| "USDT".to_string());
            // fills覆盖的成交额，差额部分（限价段）按BNB费率补估
            let fills_quote: f64 = fills
                .iter()
                .filter_map(|f| {
                    let p = f.price.parse::<f64>().ok()?;
                    let q = f.qty.parse::<f64>().ok()?;
                    Some(p * q)
                })
                .sum();
            let missing_quote = (actual_quote_qty - fills_quote).max(0.0);
            if missing_quote > 0.01 {
                log::info!(
                    "限价段佣金按BNB费率补估: 未覆盖成交额 {:.2} U → +{:.4} U",
                    missing_quote,
                    missing_quote * 0.00075
                );
            }
            (total_commission + missing_quote * 0.00075, asset)
        } else {
            // 无fills（限价单轮询成交）：按BNB抵扣费率0.075%估算
            (fill_qty * fill_price * 0.00075, "USDT".to_string())
        };

        // 7. 更新本地余额缓存（使用实际成交金额，按品种更新对应资产，并扣除实际手续费）
        {
            let mut bal = self.balance.lock().await;
            if side == "BUY" {
                bal.available_usdt -= actual_quote_qty;
                match signal.symbol.as_str() {
                    "BTCUSDT" => bal.btc_free += fill_qty,
                    "ETHUSDT" => bal.eth_free += fill_qty,
                    "SOLUSDT" => bal.sol_free += fill_qty,
                    _ => {}
                }
            } else {
                bal.available_usdt += actual_quote_qty;
                match signal.symbol.as_str() {
                    "BTCUSDT" => bal.btc_free = (bal.btc_free - fill_qty).max(0.0),
                    "ETHUSDT" => bal.eth_free = (bal.eth_free - fill_qty).max(0.0),
                    "SOLUSDT" => bal.sol_free = (bal.sol_free - fill_qty).max(0.0),
                    _ => {}
                }
            }

            if commission > 0.0 {
                match commission_asset.as_str() {
                    "USDT" => bal.available_usdt -= commission,
                    "BTC" => bal.btc_free = (bal.btc_free - commission).max(0.0),
                    "ETH" => bal.eth_free = (bal.eth_free - commission).max(0.0),
                    "SOL" => bal.sol_free = (bal.sol_free - commission).max(0.0),
                    _ => {}
                }
            }

            // 安全下限保护
            if bal.available_usdt < 0.0 {
                log::warn!("本地余额缓存异常: USDT={:.4}，强制置0", bal.available_usdt);
                bal.available_usdt = 0.0;
            }
        }

        // 8. 发布订单成交事件（市价单立即成交）
        let fill_event = DomainEvent::OrderFilled(OrderFilledEvent {
            order_id: order_result.order_id.to_string(),
            signal_id: signal.signal_id.clone(),
            symbol: order_result.symbol.clone(),
            side: side.to_string(),
            fill_id: format!("fill_{}", order_result.order_id),
            fill_price,
            fill_qty,
            actual_quote_qty,
            commission,
            commission_asset: commission_asset.clone(),
            is_maker: false,
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
        });

        self.event_bus
            .publish(fill_event)
            .await
            .map_err(|e| ServiceError::Order(format!("发布事件失败: {}", e)))?;

        log::info!(
            "✅ 成交 | {} {} | 均价: {:.2} | 量: {:.6} | 手续费: {:.6} {} | ID: {}",
            side,
            order_result.symbol,
            fill_price,
            fill_qty,
            commission,
            commission_asset,
            order_result.order_id
        );

        Ok(())
    }
}

#[async_trait]
impl EventHandler for OrderExecutionService {
    async fn handle(&self, event: &DomainEvent) -> Result<(), EventBusError> {
        if let DomainEvent::TradingSignal(signal) = event {
            // 只处理已配置交易对的信号
            if !self.symbols.contains(&signal.symbol) {
                return Ok(());
            }

            log::info!(
                "→ 执行信号: {} | {} @ {:.2} | 数量: {}",
                signal.signal_type,
                signal.symbol,
                signal.suggested_price,
                signal
                    .suggested_quantity
                    .map(|q| format!("{:.6}", q))
                    .unwrap_or_else(|| "动态".to_string())
            );

            if let Err(e) = self.execute_signal(signal).await {
                log::error!("订单执行失败: {}", e);

                // 所有失败都发布OrderRejected事件，通知策略回滚状态
                let reject_event = DomainEvent::OrderRejected(OrderRejectedEvent {
                    order_id: Some(signal.signal_id.clone()),
                    symbol: signal.symbol.clone(),
                    reason: format!("订单失败: {}", e),
                    error_code: None,
                    timestamp: chrono::Utc::now().timestamp_millis() as u64,
                });
                let _ = self.event_bus.publish(reject_event).await;
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
            symbols: self.symbols.clone(),
            balance: self.balance.clone(),
            use_limit_entry: self.use_limit_entry,
            limit_entry_offset_pct: self.limit_entry_offset_pct,
            limit_entry_wait_seconds: self.limit_entry_wait_seconds,
        }
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_order_execution_service_creation() {
        // 这个测试需要真实的 API 客户端，暂时跳过
        println!("OrderExecutionService 创建测试（需要真实 API 密钥）");
    }
}
