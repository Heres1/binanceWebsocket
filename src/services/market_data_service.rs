//! 市场数据服务
//!
//! 负责连接Binance WebSocket，获取实时行情数据并发布事件
//! 支持通过环境变量 HTTPS_PROXY 配置代理，或直接连接

use crate::error::{DomainError, ServiceError};
use crate::event_bus::{EventBus, TokioEventBus};
use crate::events::{DomainEvent, PriceUpdateEvent, KlineCompletedEvent, AggTradeEvent, BookTickerEvent};
use futures_util::{SinkExt, StreamExt};
use std::env;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::time::{interval, timeout};
use tokio_rustls::{TlsConnector, client::TlsStream};
use rustls::{ClientConfig, OwnedTrustAnchor, RootCertStore};
use tokio_tungstenite::{tungstenite::Message, tungstenite::handshake::client::generate_key, tungstenite::http, WebSocketStream};
use url::Url;

/// 连接模式
#[derive(Debug, Clone, Copy)]
pub enum ConnectionMode {
    /// 自动检测（优先使用环境变量 HTTPS_PROXY）
    Auto,
    /// 强制直接连接（不使用代理）
    Direct,
    /// 强制使用代理
    Proxy,
}

/// 市场数据服务
pub struct MarketDataService {
    event_bus: Arc<TokioEventBus>,
    symbols: Vec<String>,
    ws_url: String,
    reconnect_interval: u64,
    max_reconnect_interval: u64,
    connect_timeout: u64,
    connection_mode: ConnectionMode,
}

/// 构建WebSocket URL
fn build_ws_url(host: &str, streams: &[String]) -> String {
    if streams.len() == 1 {
        format!("wss://{}/ws/{}", host, streams[0])
    } else {
        format!("wss://{}/stream?streams={}", host, streams.join("/"))
    }
}

impl MarketDataService {
    /// 创建新的市场数据服务
    pub fn new(event_bus: Arc<TokioEventBus>, symbols: Vec<String>) -> Self {
        // 订阅多个数据流: kline_1m, kline_5m, aggTrade, bookTicker
        let streams: Vec<String> = symbols
            .iter()
            .flat_map(|s| {
                let sym = s.to_lowercase();
                vec![
                    format!("{}@kline_1m", sym),
                    format!("{}@kline_5m", sym),
                    format!("{}@aggTrade", sym),
                    format!("{}@bookTicker", sym),
                ]
            })
            .collect();
        
        // 根据环境选择连接目标
        let ws_url = if env::var("USE_SSH_TUNNEL").is_ok() {
            build_ws_url("localhost:9443", &streams)
        } else {
            // 不带端口号，wss默认443，避免Host头带端口导致Binance不响应
            build_ws_url("stream.binance.com", &streams)
        };
        
        Self {
            event_bus,
            symbols,
            ws_url,
            reconnect_interval: 5,
            max_reconnect_interval: 60,
            connect_timeout: 30,
            connection_mode: ConnectionMode::Auto,
        }
    }
    
    /// 设置连接模式
    pub fn with_connection_mode(mut self, mode: ConnectionMode) -> Self {
        self.connection_mode = mode;
        self
    }
    
    /// 设置重连间隔
    pub fn with_reconnect_interval(mut self, seconds: u64) -> Self {
        self.reconnect_interval = seconds;
        self
    }
    
    /// 设置连接超时
    pub fn with_connect_timeout(mut self, seconds: u64) -> Self {
        self.connect_timeout = seconds;
        self
    }
    
    /// 启动服务（带指数退避重连）
    pub async fn start(&self) -> Result<(), DomainError> {
        log::info!("启动市场数据服务，订阅: {:?}", self.symbols);
        
        let mut retry_interval = self.reconnect_interval;
        
        loop {
            let connect_start = std::time::Instant::now();
            match self.connect_and_run().await {
                Ok(_) => {
                    // 收到Close帧或流结束，也需要重连（Binance会定期断连）
                    log::warn!("WebSocket连接关闭，{}秒后重连", self.reconnect_interval);
                    retry_interval = self.reconnect_interval;
                    tokio::time::sleep(Duration::from_secs(self.reconnect_interval)).await;
                }
                Err(e) => {
                    // 如果运行超过30秒，说明曾成功连接过，重置退避间隔
                    if connect_start.elapsed() > Duration::from_secs(30) {
                        retry_interval = self.reconnect_interval;
                    }
                    log::warn!("WebSocket连接失败: {}，{}秒后重连", e, retry_interval);
                    tokio::time::sleep(Duration::from_secs(retry_interval)).await;
                    // 指数退避：5s → 10s → 20s → 40s → 60s(封顶)
                    retry_interval = (retry_interval * 2).min(self.max_reconnect_interval);
                }
            }
        }
    }
    
    /// 连接WebSocket并运行
    async fn connect_and_run(&self) -> Result<(), DomainError> {
        log::info!("WebSocket连接: {}", self.ws_url);
        
        // SSH隧道模式：手动TCP+TLS+WS
        if env::var("USE_SSH_TUNNEL").is_ok() {
            log::info!("使用SSH隧道模式");
            let tls_stream = self.connect_direct_with_tls_host("127.0.0.1", 9443, "stream.binance.com").await?;
            let ws_request = self.build_ws_request()?;
            log::info!("WebSocket握手中... (SSH隧道)");
            let (ws_stream, response) = timeout(
                Duration::from_secs(self.connect_timeout),
                tokio_tungstenite::client_async(ws_request, tls_stream)
            ).await
            .map_err(|_| DomainError::Service(ServiceError::MarketData("WebSocket握手超时".to_string())))?
            .map_err(|e| DomainError::Service(ServiceError::MarketData(format!("WebSocket握手失败: {}", e))))?;
            log::info!("WebSocket连接成功 | 状态码: {} | 模式: SSH隧道", response.status());
            return self.handle_connection_generic(ws_stream).await;
        }
        
        // 代理模式：手动CONNECT+TLS+WS
        let use_proxy = match self.connection_mode {
            ConnectionMode::Direct => false,
            ConnectionMode::Proxy => true,
            ConnectionMode::Auto => env::var("HTTPS_PROXY").is_ok() || env::var("https_proxy").is_ok(),
        };
        
        if use_proxy {
            let proxy = env::var("HTTPS_PROXY").or_else(|_| env::var("https_proxy"))
                .map_err(|_| DomainError::Service(ServiceError::MarketData("代理模式但未设置 HTTPS_PROXY".to_string())))?;
            log::info!("使用HTTP代理: {}", proxy);
            let tls_stream = self.connect_via_proxy(&proxy, "stream.binance.com", 443).await?;
            let ws_request = self.build_ws_request()?;
            log::info!("WebSocket握手中... (代理模式)");
            let (ws_stream, response) = timeout(
                Duration::from_secs(self.connect_timeout),
                tokio_tungstenite::client_async(ws_request, tls_stream)
            ).await
            .map_err(|_| DomainError::Service(ServiceError::MarketData("WebSocket握手超时".to_string())))?
            .map_err(|e| DomainError::Service(ServiceError::MarketData(format!("WebSocket握手失败: {}", e))))?;
            log::info!("WebSocket连接成功 | 状态码: {} | 模式: 代理", response.status());
            return self.handle_connection_generic(ws_stream).await;
        }
        
        // 直连模式：使用 tokio-tungstenite 内置的 connect_async（最标准的方式）
        log::info!("直连模式 | 目标: {}", self.ws_url);
        let (ws_stream, response) = timeout(
            Duration::from_secs(self.connect_timeout),
            tokio_tungstenite::connect_async(&self.ws_url)
        ).await
        .map_err(|_| DomainError::Service(ServiceError::MarketData("WebSocket连接超时(30s)".to_string())))?
        .map_err(|e| DomainError::Service(ServiceError::MarketData(format!("WebSocket连接失败: {}", e))))?;
        
        log::info!("WebSocket连接成功 | 状态码: {} | 模式: 直连", response.status());
        self.handle_connection_generic(ws_stream).await
    }
    
    /// 手动构建 WebSocket 升级请求，确保 Host/Origin 头正确
    fn build_ws_request(&self) -> Result<http::Request<()>, DomainError> {
        let url = Url::parse(&self.ws_url).map_err(|e| {
            DomainError::Service(ServiceError::MarketData(format!("URL解析失败: {}", e)))
        })?;
        
        let host = url.host_str().unwrap_or("stream.binance.com");
        
        // URI 必须包含完整 wss:// 前缀，tungstenite 需要验证 scheme
        let request = http::Request::builder()
            .method("GET")
            .uri(self.ws_url.as_str())
            .header("Host", host)
            .header("Connection", "Upgrade")
            .header("Upgrade", "websocket")
            .header("Sec-WebSocket-Version", "13")
            .header("Sec-WebSocket-Key", generate_key())
            .header("Origin", "https://stream.binance.com")
            .body(())
            .map_err(|e| DomainError::Service(ServiceError::MarketData(format!("构建请求失败: {}", e))))?;
        
        log::info!("WebSocket请求头 | Host: {} | URI: {} | Origin: https://stream.binance.com", host, self.ws_url);
        
        Ok(request)
    }
    
    /// 直接连接，可指定TLS验证的主机名（用于SSH隧道）
    async fn connect_direct_with_tls_host(&self, connect_host: &str, port: u16, tls_host: &str) -> Result<TlsStream<TcpStream>, DomainError> {
        let tcp = timeout(
            Duration::from_secs(self.connect_timeout),
            TcpStream::connect(format!("{}:{}", connect_host, port))
        ).await
        .map_err(|_| DomainError::Service(ServiceError::MarketData("连接超时".to_string())))?
        .map_err(|e| DomainError::Service(ServiceError::MarketData(format!("TCP连接失败: {}", e))))?;
        
        self.tls_handshake(tls_host, tcp).await
    }
    
    /// TLS握手（使用rustls）
    async fn tls_handshake(&self, host: &str, tcp: TcpStream) -> Result<TlsStream<TcpStream>, DomainError> {
        // 创建根证书存储
        let mut root_cert_store = RootCertStore::empty();
        root_cert_store.add_trust_anchors(
            webpki_roots::TLS_SERVER_ROOTS.iter().map(|ta| {
                OwnedTrustAnchor::from_subject_spki_name_constraints(
                    ta.subject,
                    ta.spki,
                    ta.name_constraints,
                )
            })
        );
        
        // 创建TLS配置
        let config = ClientConfig::builder()
            .with_safe_defaults()
            .with_root_certificates(root_cert_store)
            .with_no_client_auth();
        
        let connector = TlsConnector::from(Arc::new(config));
        
        // 解析服务器名称
        let server_name = rustls::ServerName::try_from(host)
            .map_err(|_| DomainError::Service(ServiceError::MarketData(format!("无效的服务器名称: {}", host))))?;
        
        timeout(
            Duration::from_secs(self.connect_timeout),
            connector.connect(server_name, tcp)
        ).await
        .map_err(|_| DomainError::Service(ServiceError::MarketData("TLS握手超时".to_string())))?
        .map_err(|e| DomainError::Service(ServiceError::MarketData(format!("TLS握手失败: {}", e))))
    }
    
    /// 通过HTTP代理连接（CONNECT隧道）
    async fn connect_via_proxy(
        &self,
        proxy: &str,
        target_host: &str,
        target_port: u16,
    ) -> Result<TlsStream<TcpStream>, DomainError> {
        let proxy_url = Url::parse(proxy).map_err(|e| {
            DomainError::Service(ServiceError::MarketData(format!("代理URL解析失败: {}", e)))
        })?;
        
        let proxy_host = proxy_url.host_str().ok_or_else(|| {
            DomainError::Service(ServiceError::MarketData("代理缺少host".to_string()))
        })?;
        let proxy_port = proxy_url.port_or_known_default().unwrap_or(8080);
        
        log::info!("连接代理服务器: {}:{}", proxy_host, proxy_port);
        
        // 连接到代理服务器
        let mut tcp = timeout(
            Duration::from_secs(self.connect_timeout),
            TcpStream::connect(format!("{}:{}", proxy_host, proxy_port))
        ).await
        .map_err(|_| DomainError::Service(ServiceError::MarketData("代理连接超时".to_string())))?
        .map_err(|e| DomainError::Service(ServiceError::MarketData(format!("代理TCP连接失败: {}", e))))?;
        
        // 发送HTTP CONNECT请求
        let connect_req = format!(
            "CONNECT {}:{} HTTP/1.1\r\nHost: {}:{}\r\nProxy-Connection: Keep-Alive\r\n\r\n",
            target_host, target_port, target_host, target_port
        );
        
        log::info!("发送CONNECT请求...");
        tcp.write_all(connect_req.as_bytes()).await.map_err(|e| {
            DomainError::Service(ServiceError::MarketData(format!("发送CONNECT请求失败: {}", e)))
        })?;
        
        // 读取代理响应
        let mut reader = BufReader::new(&mut tcp);
        let mut response = String::new();
        
        timeout(
            Duration::from_secs(self.connect_timeout),
            reader.read_line(&mut response)
        ).await
        .map_err(|_| DomainError::Service(ServiceError::MarketData("读取代理响应超时".to_string())))?
        .map_err(|e| DomainError::Service(ServiceError::MarketData(format!("读取代理响应失败: {}", e))))?;
        
        log::info!("代理响应: {}", response.trim());
        
        if !response.contains("200") {
            return Err(DomainError::Service(ServiceError::MarketData(
                format!("代理CONNECT失败: {}", response)
            )));
        }
        
        // 读取剩余响应头
        loop {
            let mut line = String::new();
            reader.read_line(&mut line).await.map_err(|e| {
                DomainError::Service(ServiceError::MarketData(format!("读取响应头失败: {}", e)))
            })?;
            if line.trim().is_empty() {
                break;
            }
        }
        
        log::info!("CONNECT隧道建立成功");
        
        self.tls_handshake(target_host, tcp).await
    }
    
    /// 处理WebSocket连接（泛型，支持不同流类型）
    async fn handle_connection_generic<S>(
        &self,
        ws_stream: WebSocketStream<S>,
    ) -> Result<(), DomainError>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let (mut write, mut read) = ws_stream.split();
        
        // 发送订阅消息（组合流需要发送订阅）
        if self.symbols.len() > 1 {
            let subscribe_msg = serde_json::json!({
                "method": "SUBSCRIBE",
                "params": self.symbols.iter().map(|s| format!("{}@ticker", s.to_lowercase())).collect::<Vec<_>>(),
                "id": 1
            });
            write.send(Message::Text(subscribe_msg.to_string())).await.map_err(|e| {
                DomainError::Service(ServiceError::MarketData(format!("发送订阅消息失败: {}", e)))
            })?;
            log::info!("发送订阅消息: {}", subscribe_msg);
        }
        
        // 心跳超时检测：如果60秒内没有收到任何消息，认为连接已断
        let heartbeat_interval = Duration::from_secs(30);
        let heartbeat_timeout = Duration::from_secs(60);
        let mut last_message_time = std::time::Instant::now();
        let mut heartbeat_ticker = interval(heartbeat_interval);
        // 跳过第一次立即触发
        heartbeat_ticker.tick().await;
        
        // 主循环：接收消息
        loop {
            tokio::select! {
                // 接收WebSocket消息
                msg = read.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            last_message_time = std::time::Instant::now();
                            if let Err(e) = self.handle_message(&text).await {
                                log::error!("处理消息失败: {}", e);
                            }
                        }
                        Some(Ok(Message::Close(_))) => {
                            log::warn!("WebSocket连接关闭");
                            break;
                        }
                        Some(Ok(Message::Ping(data))) => {
                            last_message_time = std::time::Instant::now();
                            write.send(Message::Pong(data)).await.ok();
                        }
                        Some(Ok(Message::Pong(_))) => {
                            last_message_time = std::time::Instant::now();
                        }
                        Some(Err(e)) => {
                            log::error!("WebSocket错误: {}", e);
                            break;
                        }
                        None => {
                            log::warn!("WebSocket流结束");
                            break;
                        }
                        _ => {}
                    }
                }
                
                // 定时心跳 + 超时检测
                _ = heartbeat_ticker.tick() => {
                    // 检查是否超时（无消息时间超过60秒）
                    if last_message_time.elapsed() > heartbeat_timeout {
                        log::error!("心跳超时: {}s未收到任何消息，断开重连",
                            last_message_time.elapsed().as_secs());
                        break;
                    }
                    // 发送ping保持连接
                    if write.send(Message::Ping(vec![])).await.is_err() {
                        log::error!("Ping发送失败，连接可能已断");
                        break;
                    }
                }
            }
        }
        
        Ok(())
    }
    
    /// 处理WebSocket消息
    async fn handle_message(&self, text: &str) -> Result<(), DomainError> {
        // 解析JSON消息
        let json: serde_json::Value = serde_json::from_str(text).map_err(|e| {
            DomainError::Service(ServiceError::MarketData(format!("JSON解析失败: {}", e)))
        })?;
        
        // 处理组合流数据格式
        let (stream_name, data) = if let Some(stream) = json.get("stream").and_then(|v| v.as_str()) {
            // 组合流格式: {"stream": "btcusdt@kline_1m", "data": {...}}
            let data = json.get("data").cloned().unwrap_or(json.clone());
            (Some(stream.to_string()), data)
        } else {
            (None, json)
        };
        
        // 根据事件类型或stream名称分发
        let event_type = data.get("e").and_then(|v| v.as_str());
        
        match event_type {
            Some("kline") => {
                let event = self.parse_kline(&data)?;
                self.event_bus.publish(DomainEvent::KlineCompleted(event)).await.map_err(|e| {
                    DomainError::Service(ServiceError::MarketData(format!("发布事件失败: {}", e)))
                })?;
            }
            Some("aggTrade") => {
                let event = self.parse_agg_trade(&data)?;
                self.event_bus.publish(DomainEvent::AggTrade(event)).await.map_err(|e| {
                    DomainError::Service(ServiceError::MarketData(format!("发布事件失败: {}", e)))
                })?;
            }
            Some("24hrTicker") => {
                let event = self.parse_ticker(&data)?;
                self.event_bus.publish(DomainEvent::PriceUpdate(event)).await.map_err(|e| {
                    DomainError::Service(ServiceError::MarketData(format!("发布事件失败: {}", e)))
                })?;
            }
            _ => {
                // bookTicker没有"e"字段，通过stream名称判断
                if stream_name.as_ref().map_or(false, |s| s.contains("bookTicker")) 
                    || data.get("b").is_some() && data.get("a").is_some() && data.get("s").is_some() {
                    let event = self.parse_book_ticker(&data)?;
                    self.event_bus.publish(DomainEvent::BookTicker(event)).await.map_err(|e| {
                        DomainError::Service(ServiceError::MarketData(format!("发布事件失败: {}", e)))
                    })?;
                }
                // 其它消息忽略（如订阅确认）
            }
        }
        
        Ok(())
    }
    
    /// 解析K线数据
    fn parse_kline(&self, data: &serde_json::Value) -> Result<KlineCompletedEvent, DomainError> {
        let symbol = data.get("s")
            .and_then(|v| v.as_str())
            .ok_or_else(|| DomainError::Service(
                ServiceError::MarketData("缺少symbol字段".to_string())
            ))?;
        
        let k = data.get("k").ok_or_else(|| DomainError::Service(
            ServiceError::MarketData("缺少k线数据字段".to_string())
        ))?;
        
        let interval = k.get("i").and_then(|v| v.as_str()).unwrap_or("1m");
        let open = k.get("o").and_then(|v| v.as_str()).and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
        let high = k.get("h").and_then(|v| v.as_str()).and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
        let low = k.get("l").and_then(|v| v.as_str()).and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
        let close = k.get("c").and_then(|v| v.as_str()).and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
        let volume = k.get("v").and_then(|v| v.as_str()).and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
        let close_time = k.get("T").and_then(|v| v.as_u64()).unwrap_or(0);
        let is_closed = k.get("x").and_then(|v| v.as_bool()).unwrap_or(false);
        let trades_count = k.get("n").and_then(|v| v.as_u64()).unwrap_or(0);

        // 关键字段校验：close或open为0表示数据异常
        if close <= 0.0 || open <= 0.0 {
            log::warn!("K线数据异常: symbol={} interval={} open={} close={}，跳过",
                symbol, interval, open, close);
            return Err(DomainError::Service(
                ServiceError::MarketData(format!("K线价格异常: open={}, close={}", open, close))
            ));
        }
        
        Ok(KlineCompletedEvent {
            symbol: symbol.to_string(),
            interval: interval.to_string(),
            open,
            high,
            low,
            close,
            volume,
            close_time,
            is_closed,
            trades_count,
        })
    }
    
    /// 解析聚合成交数据
    fn parse_agg_trade(&self, data: &serde_json::Value) -> Result<AggTradeEvent, DomainError> {
        let symbol = data.get("s")
            .and_then(|v| v.as_str())
            .ok_or_else(|| DomainError::Service(
                ServiceError::MarketData("缺少symbol字段".to_string())
            ))?;
        
        let price = data.get("p")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        
        let quantity = data.get("q")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        
        let is_buyer_maker = data.get("m")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        
        let timestamp = data.get("T")
            .and_then(|v| v.as_u64())
            .unwrap_or_else(|| chrono::Utc::now().timestamp_millis() as u64);
        
        Ok(AggTradeEvent {
            symbol: symbol.to_string(),
            price,
            quantity,
            is_buyer_maker,
            timestamp,
        })
    }
    
    /// 解析最优买卖价数据
    fn parse_book_ticker(&self, data: &serde_json::Value) -> Result<BookTickerEvent, DomainError> {
        let symbol = data.get("s")
            .and_then(|v| v.as_str())
            .ok_or_else(|| DomainError::Service(
                ServiceError::MarketData("缺少symbol字段".to_string())
            ))?;
        
        let best_bid = data.get("b")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        
        let best_bid_qty = data.get("B")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        
        let best_ask = data.get("a")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        
        let best_ask_qty = data.get("A")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        
        // 注意: Binance bookTicker的"u"字段是updateId(序列号)，不是时间戳
        // 必须使用系统时间作为事件时间戳
        let timestamp = chrono::Utc::now().timestamp_millis() as u64;
        
        Ok(BookTickerEvent {
            symbol: symbol.to_string(),
            best_bid,
            best_bid_qty,
            best_ask,
            best_ask_qty,
            timestamp,
        })
    }
    
    /// 解析ticker数据(兼容旧流)
    fn parse_ticker(&self, data: &serde_json::Value) -> Result<PriceUpdateEvent, DomainError> {
        let symbol = data.get("s")
            .and_then(|v| v.as_str())
            .ok_or_else(|| DomainError::Service(
                ServiceError::MarketData("缺少symbol字段".to_string())
            ))?;
        
        let price = data.get("c")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .ok_or_else(|| DomainError::Service(
                ServiceError::MarketData("缺少price字段".to_string())
            ))?;
        
        let price_change_pct = data.get("P")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        
        let timestamp = data.get("E")
            .and_then(|v| v.as_u64())
            .unwrap_or_else(|| chrono::Utc::now().timestamp_millis() as u64);
        
        Ok(PriceUpdateEvent {
            symbol: symbol.to_string(),
            price,
            price_change_pct_24h: price_change_pct,
            timestamp,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event_bus::TokioEventBus;
    
    #[test]
    fn test_parse_kline() {
        let event_bus = Arc::new(TokioEventBus::new(100));
        let service = MarketDataService::new(event_bus, vec!["BTCUSDT".to_string()]);
        
        let json = serde_json::json!({
            "e": "kline",
            "s": "BTCUSDT",
            "k": {
                "i": "1m",
                "o": "74800.00",
                "h": "74850.00",
                "l": "74780.00",
                "c": "74820.00",
                "v": "10.5",
                "T": 1234567890000u64,
                "x": true,
                "n": 150u64
            }
        });
        
        let event = service.parse_kline(&json).unwrap();
        assert_eq!(event.symbol, "BTCUSDT");
        assert_eq!(event.interval, "1m");
        assert_eq!(event.close, 74820.0);
        assert!(event.is_closed);
    }
    
    #[test]
    fn test_parse_agg_trade() {
        let event_bus = Arc::new(TokioEventBus::new(100));
        let service = MarketDataService::new(event_bus, vec!["BTCUSDT".to_string()]);
        
        let json = serde_json::json!({
            "e": "aggTrade",
            "s": "BTCUSDT",
            "p": "74800.50",
            "q": "0.5",
            "m": false,
            "T": 1234567890000u64
        });
        
        let event = service.parse_agg_trade(&json).unwrap();
        assert_eq!(event.symbol, "BTCUSDT");
        assert_eq!(event.price, 74800.5);
        assert_eq!(event.quantity, 0.5);
        assert!(!event.is_buyer_maker);
    }
    
    #[test]
    fn test_parse_book_ticker() {
        let event_bus = Arc::new(TokioEventBus::new(100));
        let service = MarketDataService::new(event_bus, vec!["BTCUSDT".to_string()]);
        
        let json = serde_json::json!({
            "s": "BTCUSDT",
            "b": "74800.00",
            "B": "5.0",
            "a": "74801.00",
            "A": "3.0",
            "u": 123456u64
        });
        
        let event = service.parse_book_ticker(&json).unwrap();
        assert_eq!(event.symbol, "BTCUSDT");
        assert_eq!(event.best_bid, 74800.0);
        assert_eq!(event.best_ask, 74801.0);
    }
}
