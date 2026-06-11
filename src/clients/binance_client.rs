//! Binance REST API 客户端
//!
//! 提供下单、撤单、查询订单、查询账户等功能

use hmac::{Hmac, Mac};
use sha2::Sha256;
use hex;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::{DomainError, ServiceError};

type HmacSha256 = Hmac<Sha256>;

/// Binance API 客户端
#[derive(Clone)]
pub struct BinanceClient {
    client: reqwest::Client,
    api_key: String,
    secret_key: String,
    base_url: String,
    recv_window: u64,  // 毫秒，默认 5000
    max_retries: u32,  // 最大重试次数
    /// 速率限制：在此时间戳(ms)之前暂停所有REST请求
    rate_limited_until_ms: Arc<AtomicU64>,
}

/// 订单响应
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OrderResponse {
    pub symbol: String,
    #[serde(rename = "orderId")]
    pub order_id: u64,
    #[serde(rename = "clientOrderId")]
    pub client_order_id: String,
    #[serde(rename = "transactTime")]
    pub transact_time: Option<u64>,
    pub price: String,
    #[serde(rename = "origQty")]
    pub orig_qty: String,
    #[serde(rename = "executedQty")]
    pub executed_qty: String,
    #[serde(rename = "cummulativeQuoteQty")]
    pub cummulative_quote_qty: String,
    pub status: String,
    #[serde(rename = "timeInForce")]
    pub time_in_force: Option<String>,
    #[serde(rename = "type")]
    pub order_type: String,
    pub side: String,
    #[serde(rename = "stopsPrice")]
    pub stop_price: Option<String>,
    #[serde(rename = "icebergQty")]
    pub iceberg_qty: Option<String>,
    pub fills: Option<Vec<OrderFill>>,
}

/// 订单成交明细
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OrderFill {
    pub price: String,
    pub qty: String,
    pub commission: String,
    #[serde(rename = "commissionAsset")]
    pub commission_asset: String,
    #[serde(rename = "tradeId")]
    pub trade_id: Option<u64>,
}

/// 撤单响应
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CancelOrderResponse {
    pub symbol: String,
    #[serde(rename = "origClientOrderId")]
    pub orig_client_order_id: String,
    #[serde(rename = "orderId")]
    pub order_id: u64,
    #[serde(rename = "clientOrderId")]
    pub client_order_id: String,
    pub price: String,
    #[serde(rename = "origQty")]
    pub orig_qty: String,
    #[serde(rename = "executedQty")]
    pub executed_qty: String,
    #[serde(rename = "cummulativeQuoteQty")]
    pub cummulative_quote_qty: String,
    pub status: String,
    #[serde(rename = "timeInForce")]
    pub time_in_force: Option<String>,
    #[serde(rename = "type")]
    pub order_type: String,
    pub side: String,
}

/// 账户信息
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AccountInfo {
    #[serde(rename = "makerCommission")]
    pub maker_commission: i64,
    #[serde(rename = "takerCommission")]
    pub taker_commission: i64,
    #[serde(rename = "buyerCommission")]
    pub buyer_commission: i64,
    #[serde(rename = "sellerCommission")]
    pub seller_commission: i64,
    #[serde(rename = "canTrade")]
    pub can_trade: bool,
    #[serde(rename = "canWithdraw")]
    pub can_withdraw: bool,
    #[serde(rename = "canDeposit")]
    pub can_deposit: bool,
    #[serde(rename = "updateTime")]
    pub update_time: Option<u64>,
    #[serde(rename = "accountType")]
    pub account_type: String,
    pub balances: Vec<Balance>,
}

/// 余额信息
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Balance {
    pub asset: String,
    pub free: String,
    pub locked: String,
}

/// Binance API 错误响应
#[derive(Debug, Deserialize)]
pub struct BinanceApiError {
    pub code: i64,
    pub msg: String,
}

/// K线数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KlineData {
    pub open_time: u64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub close_time: u64,
    pub trades_count: u64,
    pub taker_buy_volume: f64, // 主动买入量
}

impl BinanceClient {
    /// 创建新的 Binance 客户端
    pub fn new(api_key: String, secret_key: String, base_url: String) -> Self {
        use reqwest::header;
        
        // 如果是 SSH 隧道模式（localhost），需要禁用证书验证并设置正确的 Host 头
        let client = if base_url.contains("localhost") {
            let mut headers = header::HeaderMap::new();
            headers.insert(header::HOST, header::HeaderValue::from_static("api.binance.com"));
            
            reqwest::Client::builder()
                .danger_accept_invalid_certs(true)
                .default_headers(headers)
                .connect_timeout(std::time::Duration::from_secs(10))
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .unwrap_or_else(|_| reqwest::Client::new())
        } else {
            reqwest::Client::builder()
                .connect_timeout(std::time::Duration::from_secs(10))
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .unwrap_or_else(|_| reqwest::Client::new())
        };
        
        Self {
            client,
            api_key,
            secret_key,
            base_url,
            recv_window: 5000,  // 5秒
            max_retries: 2,     // 最多重试2次（共试3次）
            rate_limited_until_ms: Arc::new(AtomicU64::new(0)),
        }
    }

    /// 检查是否处于速率限制冷却期
    pub fn is_rate_limited(&self) -> bool {
        let until = self.rate_limited_until_ms.load(Ordering::Relaxed);
        if until == 0 { return false; }
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        now < until
    }

    /// 获取剩余冷却秒数（用于日志）
    pub fn rate_limit_remaining_secs(&self) -> u64 {
        let until = self.rate_limited_until_ms.load(Ordering::Relaxed);
        if until == 0 { return 0; }
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        if now >= until { return 0; }
        (until - now) / 1000
    }

    /// 创建带自定义 recv_window 的客户端
    pub fn with_recv_window(mut self, recv_window: u64) -> Self {
        self.recv_window = recv_window;
        self
    }

    /// HMAC-SHA256 签名
    fn sign(&self, query: &str) -> String {
        let mut mac = HmacSha256::new_from_slice(self.secret_key.as_bytes())
            .expect("HMAC can take key of any size");
        mac.update(query.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }

    /// 生成时间戳
    fn timestamp(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as u64
    }

    /// 构建带签名的查询字符串（使用 BTreeMap 保证参数顺序确定性）
    fn build_signed_query(&self, params: &HashMap<String, String>) -> String {
        let mut query: BTreeMap<String, String> = params.iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        query.insert("timestamp".to_string(), self.timestamp().to_string());
        query.insert("recvWindow".to_string(), self.recv_window.to_string());

        let query_string: String = query
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join("&");

        let signature = self.sign(&query_string);
        format!("{}&signature={}", query_string, signature)
    }

    /// 发送 GET 请求（带重试+速率限制检查）
    async fn get(&self, path: &str, params: &HashMap<String, String>) -> Result<serde_json::Value, DomainError> {
        // 速率限制检查：如果处于冷却期，直接返回错误而不发请求
        if self.is_rate_limited() {
            return Err(ServiceError::Order(format!(
                "API速率限制中，剩余冷却{}s", self.rate_limit_remaining_secs()
            )).into());
        }
        let mut last_err = None;
        
        for attempt in 0..=self.max_retries {
            if attempt > 0 {
                log::warn!("GET {} 重试 {}/{}", path, attempt, self.max_retries);
                tokio::time::sleep(std::time::Duration::from_millis(500 * attempt as u64)).await;
            }
            
            // 每次重试重新生成签名（timestamp会变）
            let query = self.build_signed_query(params);
            let url = format!("{}/api/v3{}?{}", self.base_url, path, query);
            
            match self.client
                .get(&url)
                .header("X-MBX-APIKEY", &self.api_key)
                .send()
                .await
            {
                Ok(response) => return self.handle_response(response).await,
                Err(e) => {
                    last_err = Some(ServiceError::MarketData(format!("HTTP 请求失败: {}", e)));
                }
            }
        }
        
        Err(last_err.unwrap().into())
    }

    /// 发送 POST 请求（带重试，仅网络层失败时重试）
    async fn post(&self, path: &str, params: &HashMap<String, String>) -> Result<serde_json::Value, DomainError> {
        if self.is_rate_limited() {
            return Err(ServiceError::Order(format!(
                "API速率限制中，剩余冷却{}s", self.rate_limit_remaining_secs()
            )).into());
        }
        let mut last_err = None;
        
        for attempt in 0..=self.max_retries {
            if attempt > 0 {
                log::warn!("POST {} 重试 {}/{}", path, attempt, self.max_retries);
                tokio::time::sleep(std::time::Duration::from_millis(500 * attempt as u64)).await;
            }
            
            let query = self.build_signed_query(params);
            let url = format!("{}/api/v3{}?{}", self.base_url, path, query);
            
            match self.client
                .post(&url)
                .header("X-MBX-APIKEY", &self.api_key)
                .send()
                .await
            {
                Ok(response) => return self.handle_response(response).await,
                Err(e) => {
                    last_err = Some(ServiceError::Order(format!("HTTP 请求失败: {}", e)));
                }
            }
        }
        
        Err(last_err.unwrap().into())
    }

    /// 发送 DELETE 请求（带重试）
    async fn delete(&self, path: &str, params: &HashMap<String, String>) -> Result<serde_json::Value, DomainError> {
        if self.is_rate_limited() {
            return Err(ServiceError::Order(format!(
                "API速率限制中，剩余冷却{}s", self.rate_limit_remaining_secs()
            )).into());
        }
        let mut last_err = None;
        
        for attempt in 0..=self.max_retries {
            if attempt > 0 {
                log::warn!("DELETE {} 重试 {}/{}", path, attempt, self.max_retries);
                tokio::time::sleep(std::time::Duration::from_millis(500 * attempt as u64)).await;
            }
            
            let query = self.build_signed_query(params);
            let url = format!("{}/api/v3{}?{}", self.base_url, path, query);
            
            match self.client
                .delete(&url)
                .header("X-MBX-APIKEY", &self.api_key)
                .send()
                .await
            {
                Ok(response) => return self.handle_response(response).await,
                Err(e) => {
                    last_err = Some(ServiceError::Order(format!("HTTP 请求失败: {}", e)));
                }
            }
        }
        
        Err(last_err.unwrap().into())
    }

    /// 处理 HTTP 响应
    async fn handle_response(&self, response: reqwest::Response) -> Result<serde_json::Value, DomainError> {
        let status = response.status();
        let text = response
            .text()
            .await
            .map_err(|e| ServiceError::MarketData(format!("读取响应失败: {}", e)))?;

        if !status.is_success() {
            // 解析 Binance 错误
            if let Ok(error) = serde_json::from_str::<BinanceApiError>(&text) {
                return Err(self.handle_api_error(error));
            }
            
            return Err(DomainError::Service(ServiceError::MarketData(format!(
                "HTTP 错误 {}: {}",
                status, text
            ))));
        }

        serde_json::from_str(&text)
            .map_err(|e| DomainError::Service(ServiceError::MarketData(format!("JSON 解析失败: {}, 响应: {}", e, text))))
    }

    /// 处理 Binance API 错误
    fn handle_api_error(&self, error: BinanceApiError) -> DomainError {
        let error_msg = format!("Binance API 错误 [{}]: {}", error.code, error.msg);
        
        // 根据错误码分类处理
        match error.code {
            -1003 => {
                // 速率限制或IP封禁 - 暂停所有REST请求
                // 尝试从消息中解析ban时间，否则默认暂停120秒
                let pause_ms = if error.msg.contains("IP banned until") {
                    // 解析: "...IP banned until 1781112351786..."
                    error.msg.split("banned until ").nth(1)
                        .and_then(|s| s.split('.').next())
                        .and_then(|s| s.trim().parse::<u64>().ok())
                        .unwrap_or_else(|| {
                            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64 + 300_000
                        })
                } else {
                    // 普通速率限制，暂停120秒
                    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64 + 120_000
                };
                self.rate_limited_until_ms.store(pause_ms, Ordering::Relaxed);
                log::error!("⚠️ API速率限制触发，暂停REST请求至冷却结束 | 原因: {}", error.msg);
                ServiceError::Order(format!("速率限制: {}", error.msg))
            }
            -1000 => ServiceError::Order(format!("未知错误: {}", error.msg)),
            -1013 => ServiceError::Order(format!("数量不符合过滤器: {}", error.msg)),
            -1021 => ServiceError::Order(format!("时间戳偏移: {}", error.msg)),
            -2010 => ServiceError::Order(format!("资金不足: {}", error.msg)),
            -2011 => ServiceError::Order(format!("订单取消: {}", error.msg)),
            _ => ServiceError::Order(error_msg),
        }.into()
    }

    /// 下单
    ///
    /// # 参数
    /// - `symbol`: 交易对，如 "BTCUSDT"
    /// - `side`: 方向，"BUY" 或 "SELL"
    /// - `order_type`: 类型，"LIMIT" 或 "MARKET"
    /// - `quantity`: 数量
    /// - `price`: 价格（限价单必需）
    /// - `time_in_force`: 有效方式，"GTC", "IOC", "FOK"（限价单必需）
    pub async fn place_order(
        &self,
        symbol: &str,
        side: &str,
        order_type: &str,
        quantity: f64,
        price: Option<f64>,
        time_in_force: Option<&str>,
    ) -> Result<OrderResponse, DomainError> {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.to_string());
        params.insert("side".to_string(), side.to_string());
        params.insert("type".to_string(), order_type.to_string());
        params.insert("quantity".to_string(), quantity.to_string());

        // 限价单需要价格和 TIF
        if order_type == "LIMIT" {
            if let Some(p) = price {
                params.insert("price".to_string(), format!("{}", p));
            } else {
                return Err(ServiceError::Order("限价单必须提供价格".to_string()).into());
            }

            if let Some(tif) = time_in_force {
                params.insert("timeInForce".to_string(), tif.to_string());
            } else {
                params.insert("timeInForce".to_string(), "GTC".to_string());
            }
        }

        let response = self.post("/order", &params).await?;
        let order: OrderResponse = serde_json::from_value(response)
            .map_err(|e| ServiceError::Order(format!("解析订单响应失败: {}", e)))?;

        log::debug!(
            "API响应: {} {} {} @ {} (数量: {})",
            order.symbol, order.side, order.order_type, order.price, order.orig_qty
        );

        Ok(order)
    }

    /// 撤单
    pub async fn cancel_order(
        &self,
        symbol: &str,
        order_id: Option<u64>,
        orig_client_order_id: Option<String>,
    ) -> Result<CancelOrderResponse, DomainError> {
        if order_id.is_none() && orig_client_order_id.is_none() {
            return Err(ServiceError::Order("必须提供 order_id 或 orig_client_order_id".to_string()).into());
        }

        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.to_string());

        if let Some(id) = order_id {
            params.insert("orderId".to_string(), id.to_string());
        }

        if let Some(client_id) = orig_client_order_id {
            params.insert("origClientOrderId".to_string(), client_id);
        }

        let response = self.delete("/order", &params).await?;
        let cancel_response: CancelOrderResponse = serde_json::from_value(response)
            .map_err(|e| ServiceError::Order(format!("解析撤单响应失败: {}", e)))?;

        log::info!("订单已撤销: {} #{}", cancel_response.symbol, cancel_response.order_id);

        Ok(cancel_response)
    }

    /// 查询订单状态
    pub async fn query_order(
        &self,
        symbol: &str,
        order_id: Option<u64>,
        orig_client_order_id: Option<String>,
    ) -> Result<OrderResponse, DomainError> {
        if order_id.is_none() && orig_client_order_id.is_none() {
            return Err(ServiceError::Order("必须提供 order_id 或 orig_client_order_id".to_string()).into());
        }

        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.to_string());

        if let Some(id) = order_id {
            params.insert("orderId".to_string(), id.to_string());
        }

        if let Some(client_id) = orig_client_order_id {
            params.insert("origClientOrderId".to_string(), client_id);
        }

        let response = self.get("/order", &params).await?;
        let order: OrderResponse = serde_json::from_value(response)
            .map_err(|e| ServiceError::Order(format!("解析订单查询响应失败: {}", e)))?;

        Ok(order)
    }

    /// 查询账户信息
    pub async fn get_account(&self) -> Result<AccountInfo, DomainError> {
        let params = HashMap::new();
        let response = self.get("/account", &params).await?;
        
        let account: AccountInfo = serde_json::from_value(response)
            .map_err(|e| ServiceError::Account(format!("解析账户信息失败: {}", e)))?;

        log::debug!("账户信息获取成功 | 可交易: {} | 资产数: {}", account.can_trade, account.balances.len());

        Ok(account)
    }

    /// 获取特定资产余额
    pub async fn get_balance(&self, asset: &str) -> Result<Balance, DomainError> {
        let account = self.get_account().await?;
        
        account.balances
            .into_iter()
            .find(|b| b.asset == asset)
            .ok_or_else(|| ServiceError::Account(format!("未找到资产: {}", asset)).into())
    }

    /// 获取历史K线数据（公开接口，无需签名）
    ///
    /// # 参数
    /// - `symbol`: 交易对，如 "BTCUSDT"
    /// - `interval`: K线间隔，如 "1m", "5m", "1h"
    /// - `start_time`: 起始时间戳（毫秒），可选
    /// - `end_time`: 结束时间戳（毫秒），可选
    /// - `limit`: 返回数量，最大1000，默认500
    pub async fn get_klines(
        &self,
        symbol: &str,
        interval: &str,
        start_time: Option<u64>,
        end_time: Option<u64>,
        limit: Option<u16>,
    ) -> Result<Vec<KlineData>, DomainError> {
        let mut url = format!(
            "{}/api/v3/klines?symbol={}&interval={}&limit={}",
            self.base_url,
            symbol,
            interval,
            limit.unwrap_or(1000)
        );

        if let Some(start) = start_time {
            url.push_str(&format!("&startTime={}", start));
        }
        if let Some(end) = end_time {
            url.push_str(&format!("&endTime={}", end));
        }

        let response = self.client
            .get(&url)
            .send()
            .await
            .map_err(|e| ServiceError::MarketData(format!("获取K线失败: {}", e)))?;

        let status = response.status();
        let text = response.text().await
            .map_err(|e| ServiceError::MarketData(format!("读取K线响应失败: {}", e)))?;

        if !status.is_success() {
            return Err(DomainError::Service(ServiceError::MarketData(
                format!("获取K线HTTP错误 {}: {}", status, text)
            )));
        }

        // Binance返回的是二维数组 [[open_time, open, high, low, close, volume, close_time, ...]]
        let raw: Vec<Vec<serde_json::Value>> = serde_json::from_str(&text)
            .map_err(|e| ServiceError::MarketData(format!("解析K线JSON失败: {}", e)))?;

        let klines: Vec<KlineData> = raw.iter().filter_map(|k| {
            if k.len() < 11 { return None; }
            Some(KlineData {
                open_time: k[0].as_u64()?,
                open: k[1].as_str()?.parse().ok()?,
                high: k[2].as_str()?.parse().ok()?,
                low: k[3].as_str()?.parse().ok()?,
                close: k[4].as_str()?.parse().ok()?,
                volume: k[5].as_str()?.parse().ok()?,
                close_time: k[6].as_u64()?,
                trades_count: k[8].as_u64()?,
                taker_buy_volume: k[9].as_str()?.parse().ok()?,
            })
        }).collect();

        Ok(klines)
    }

    /// 测试下单（不实际执行，用于验证参数）
    pub async fn test_place_order(
        &self,
        symbol: &str,
        side: &str,
        order_type: &str,
        quantity: f64,
        price: Option<f64>,
        time_in_force: Option<&str>,
    ) -> Result<(), DomainError> {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.to_string());
        params.insert("side".to_string(), side.to_string());
        params.insert("type".to_string(), order_type.to_string());
        params.insert("quantity".to_string(), quantity.to_string());

        if order_type == "LIMIT" {
            if let Some(p) = price {
                params.insert("price".to_string(), format!("{}", p));
            }
            if let Some(tif) = time_in_force {
                params.insert("timeInForce".to_string(), tif.to_string());
            }
        }

        // 使用测试端点
        let query = self.build_signed_query(&params);
        let url = format!("{}/api/v3/order/test?{}", self.base_url, query);

        let response = self.client
            .post(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await
            .map_err(|e| ServiceError::Order(format!("测试下单失败: {}", e)))?;

        if response.status().is_success() {
            log::info!("测试下单成功（参数验证通过）");
            Ok(())
        } else {
            let text = response.text().await.unwrap_or_default();
            Err(ServiceError::Order(format!("测试下单失败: {}", text)).into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hmac_signature() {
        let client = BinanceClient::new(
            "test_api_key".to_string(),
            "test_secret_key".to_string(),
            "https://testnet.binance.vision".to_string(),
        );

        // 签名应该是确定性的
        let signature1 = client.sign("symbol=BTCUSDT&side=BUY&type=LIMIT");
        let signature2 = client.sign("symbol=BTCUSDT&side=BUY&type=LIMIT");
        
        assert_eq!(signature1, signature2);
        assert_eq!(signature1.len(), 64); // SHA256 hex 长度
    }

    #[test]
    fn test_build_signed_query() {
        let client = BinanceClient::new(
            "test_api_key".to_string(),
            "test_secret_key".to_string(),
            "https://testnet.binance.vision".to_string(),
        );

        let mut params = HashMap::new();
        params.insert("symbol".to_string(), "BTCUSDT".to_string());
        params.insert("side".to_string(), "BUY".to_string());

        let query = client.build_signed_query(&params);
        
        assert!(query.contains("symbol=BTCUSDT"));
        assert!(query.contains("side=BUY"));
        assert!(query.contains("timestamp="));
        assert!(query.contains("recvWindow=5000"));
        assert!(query.contains("signature="));
    }

    #[test]
    fn test_timestamp_generation() {
        let client = BinanceClient::new(
            "test_api_key".to_string(),
            "test_secret_key".to_string(),
            "https://testnet.binance.vision".to_string(),
        );

        let ts1 = client.timestamp();
        let ts2 = client.timestamp();
        
        // 两次调用应该非常接近（毫秒级）
        assert!((ts2 - ts1) < 100);
    }
}
