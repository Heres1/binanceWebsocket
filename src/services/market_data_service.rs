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
use std::convert::TryFrom;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::{interval, timeout};
use tokio_rustls::{TlsConnector, client::TlsStream};
use rustls::{ClientConfig, OwnedTrustAnchor, RootCertStore};
use tokio_tungstenite::{tungstenite::Message, WebSocketStream};
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
    
    /// 启动服务（带自动重连）
    pub async fn start(&self) -> Result<(), DomainError> {
        println!("启动市场数据服务，订阅: {:?}", self.symbols);
        
        loop {
            match self.connect_and_run().await {
                Ok(_) => {
                    println!("WebSocket连接正常关闭");
                    break;
                }
                Err(e) => {
                    println!("WebSocket连接失败: {}，{}秒后重连", e, self.reconnect_interval);
                    tokio::time::sleep(Duration::from_secs(self.reconnect_interval)).await;
                }
            }
        }
        
        Ok(())
    }
    
    /// 连接WebSocket并运行（支持HTTP代理）
    async fn connect_and_run(&self) -> Result<(), DomainError> {
        log::info!("WebSocket连接: {}", self.ws_url);
        
        // 解析WebSocket URL
        let ws_url = Url::parse(&self.ws_url).map_err(|e| {
            DomainError::Service(ServiceError::MarketData(format!("URL解析失败: {}", e)))
        })?;
        
        let host = ws_url.host_str().ok_or_else(|| {
            DomainError::Service(ServiceError::MarketData("缺少host".to_string()))
        })?;
        let port = ws_url.port_or_known_default().unwrap_or(443);
        
        // SSH隧道模式：连接localhost，但TLS验证stream.binance.com
        let (connect_host, connect_port, tls_host) = if env::var("USE_SSH_TUNNEL").is_ok() {
            ("127.0.0.1", 9443, "stream.binance.com")
        } else {
            (host, port, host)
        };
        
        // 根据连接模式决定是否使用代理
        // SSH隧道模式下强制直连，忽略代理设置
        let use_proxy = if env::var("USE_SSH_TUNNEL").is_ok() {
            false
        } else {
            match self.connection_mode {
                ConnectionMode::Direct => false,
                ConnectionMode::Proxy => true,
                ConnectionMode::Auto => env::var("HTTPS_PROXY").is_ok() || env::var("https_proxy").is_ok(),
            }
        };
        
        let tls_stream = if use_proxy {
            match env::var("HTTPS_PROXY").or_else(|_| env::var("https_proxy")) {
                Ok(proxy) => {
                    println!("使用HTTP代理: {}", proxy);
                    self.connect_via_proxy(&proxy, tls_host, connect_port).await?
                }
                Err(_) => {
                    return Err(DomainError::Service(ServiceError::MarketData(
                        "代理模式但未设置 HTTPS_PROXY 环境变量".to_string()
                    )));
                }
            }
        } else {
            println!("直接连接（无代理）到 {}:{}", connect_host, connect_port);
            self.connect_direct_with_tls_host(connect_host, connect_port, tls_host).await?
        };
        
        // WebSocket握手
        log::info!("WebSocket握手中...");
        let (ws_stream, _) = timeout(
            Duration::from_secs(self.connect_timeout),
            tokio_tungstenite::client_async(&self.ws_url, tls_stream)
        ).await
        .map_err(|_| DomainError::Service(ServiceError::MarketData("WebSocket握手超时".to_string())))?
        .map_err(|e| DomainError::Service(ServiceError::MarketData(format!("WebSocket握手失败: {}", e))))?;
        
        println!("WebSocket连接成功: {}", self.ws_url);
        
        self.handle_connection(ws_stream).await
    }
    
    /// 直接连接（无代理）
    async fn connect_direct(&self, host: &str, port: u16) -> Result<TlsStream<TcpStream>, DomainError> {
        self.connect_direct_with_tls_host(host, port, host).await
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
        
        println!("连接代理服务器: {}:{}", proxy_host, proxy_port);
        
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
        
        println!("发送CONNECT请求...");
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
        
        println!("代理响应: {}", response.trim());
        
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
        
        println!("CONNECT隧道建立成功");
        
        self.tls_handshake(target_host, tcp).await
    }
    
    /// 处理WebSocket连接
    async fn handle_connection(
        &self,
        ws_stream: WebSocketStream<TlsStream<TcpStream>>,
    ) -> Result<(), DomainError> {
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
            println!("📤 发送订阅消息: {}", subscribe_msg);
        }
        
        // 启动心跳检测
        let (heartbeat_tx, mut heartbeat_rx) = mpsc::channel(1);
        let heartbeat_handle = tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(30));
            loop {
                ticker.tick().await;
                if heartbeat_tx.send(()).await.is_err() {
                    break;
                }
            }
        });
        
        // 主循环：接收消息
        loop {
            tokio::select! {
                // 接收WebSocket消息
                msg = read.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            if let Err(e) = self.handle_message(&text).await {
                                log::error!("处理消息失败: {}", e);
                            }
                        }
                        Some(Ok(Message::Close(_))) => {
                            log::warn!("WebSocket连接关闭");
                            break;
                        }
                        Some(Ok(Message::Ping(data))) => {
                            // 自动回复pong
                            write.send(Message::Pong(data)).await.ok();
                        }
                        Some(Err(e)) => {
                            log::error!("WebSocket错误: {}", e);
                            break;
                        }
                        _ => {}
                    }
                }
                
                // 心跳检测
                _ = heartbeat_rx.recv() => {
                    // 发送ping保持连接
                    write.send(Message::Ping(vec![])).await.ok();
                }
            }
        }
        
        heartbeat_handle.abort();
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
        
        let timestamp = data.get("u")
            .and_then(|v| v.as_u64())
            .unwrap_or_else(|| chrono::Utc::now().timestamp_millis() as u64);
        
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
