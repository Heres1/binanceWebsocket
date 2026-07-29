//! 数据加载模块
//!
//! 支持从Binance API下载历史K线数据，以及从本地录制文件加载回放数据

use crate::clients::binance_client::{BinanceClient, KlineData};
use crate::error::DomainError;
use serde::{Deserialize, Serialize};
use std::path::Path;

/// 回测用的K线数据（带interval标记）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestKline {
    pub symbol: String,
    pub interval: String,
    pub open_time: u64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub close_time: u64,
    pub trades_count: u64,
    pub taker_buy_volume: f64,
}

impl BacktestKline {
    pub fn from_kline_data(kline: &KlineData, symbol: &str, interval: &str) -> Self {
        Self {
            symbol: symbol.to_string(),
            interval: interval.to_string(),
            open_time: kline.open_time,
            open: kline.open,
            high: kline.high,
            low: kline.low,
            close: kline.close,
            volume: kline.volume,
            close_time: kline.close_time,
            trades_count: kline.trades_count,
            taker_buy_volume: kline.taker_buy_volume,
        }
    }
}

/// 录制事件数据格式
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum RecordedEvent {
    Kline(BacktestKline),
    AggTrade {
        symbol: String,
        price: f64,
        quantity: f64,
        is_buyer_maker: bool,
        timestamp: u64,
    },
    BookTicker {
        symbol: String,
        best_bid: f64,
        best_bid_qty: f64,
        best_ask: f64,
        best_ask_qty: f64,
        timestamp: u64,
    },
}

/// 数据加载器
pub struct DataLoader {
    data_dir: String,
}

impl DataLoader {
    pub fn new(data_dir: &str) -> Self {
        Self {
            data_dir: data_dir.to_string(),
        }
    }

    /// 从Binance下载历史K线数据（分批下载，每次1000根）
    pub async fn download_klines(
        &self,
        client: &BinanceClient,
        symbol: &str,
        interval: &str,
        start_time: u64,
        end_time: u64,
    ) -> Result<Vec<BacktestKline>, DomainError> {
        let dir = format!("{}/history", self.data_dir);
        std::fs::create_dir_all(&dir).ok();

        let mut all_klines: Vec<BacktestKline> = Vec::new();
        let mut current_start = start_time;

        println!("📥 下载 {} {} K线数据...", symbol, interval);

        loop {
            if current_start >= end_time {
                break;
            }

            let klines = client
                .get_klines(
                    symbol,
                    interval,
                    Some(current_start),
                    Some(end_time),
                    Some(1000),
                )
                .await?;

            if klines.is_empty() {
                break;
            }

            let last_time = klines.last().unwrap().close_time;

            for k in &klines {
                all_klines.push(BacktestKline::from_kline_data(k, symbol, interval));
            }

            println!(
                "   已下载 {} 根K线 (总计: {})",
                klines.len(),
                all_klines.len()
            );

            // 下一批从最后一根K线的close_time+1开始
            current_start = last_time + 1;

            // 避免API限频
            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

            if klines.len() < 1000 {
                break; // 已到末尾
            }
        }

        // 保存到文件
        let file_path = format!("{}/{}_{}.json", dir, symbol, interval);
        let json = serde_json::to_string(&all_klines)
            .map_err(|e| crate::error::ServiceError::MarketData(format!("序列化K线失败: {}", e)))?;
        std::fs::write(&file_path, &json)
            .map_err(|e| crate::error::InfrastructureError::io_with_operation("保存K线数据", e))?;

        println!("✅ K线数据已保存: {} ({} 根)", file_path, all_klines.len());

        Ok(all_klines)
    }

    /// 从本地文件加载K线数据
    pub fn load_klines(
        &self,
        symbol: &str,
        interval: &str,
    ) -> Result<Vec<BacktestKline>, DomainError> {
        let file_path = format!("{}/history/{}_{}.json", self.data_dir, symbol, interval);

        if !Path::new(&file_path).exists() {
            return Err(crate::error::ServiceError::MarketData(format!(
                "K线数据文件不存在: {} (请先运行下载)",
                file_path
            ))
            .into());
        }

        let content = std::fs::read_to_string(&file_path)
            .map_err(|e| crate::error::InfrastructureError::io_with_operation("读取K线文件", e))?;

        let klines: Vec<BacktestKline> = serde_json::from_str(&content).map_err(|e| {
            crate::error::ServiceError::MarketData(format!("解析K线文件失败: {}", e))
        })?;

        println!("📂 加载K线: {} {} ({} 根)", symbol, interval, klines.len());
        Ok(klines)
    }

    /// 从录制文件加载事件数据
    pub fn load_recorded_events(&self, file_path: &str) -> Result<Vec<RecordedEvent>, DomainError> {
        if !Path::new(file_path).exists() {
            return Err(crate::error::ServiceError::MarketData(format!(
                "录制文件不存在: {}",
                file_path
            ))
            .into());
        }

        let content = std::fs::read_to_string(file_path)
            .map_err(|e| crate::error::InfrastructureError::io_with_operation("读取录制文件", e))?;

        let mut events = Vec::new();
        for line in content.lines() {
            if line.trim().is_empty() {
                continue;
            }
            match serde_json::from_str::<RecordedEvent>(line) {
                Ok(event) => events.push(event),
                Err(e) => {
                    eprintln!("跳过无效行: {}", e);
                }
            }
        }

        println!("📂 加载录制数据: {} 条事件", events.len());
        Ok(events)
    }

    /// 检查本地是否已有数据
    pub fn has_local_data(&self, symbol: &str, interval: &str) -> bool {
        let file_path = format!("{}/history/{}_{}.json", self.data_dir, symbol, interval);
        Path::new(&file_path).exists()
    }

    /// 下载资金费率历史（fapi公共接口，无需签名）
    /// use_tunnel=true 时通过 SSH 隧道 10443 端口访问，并强制 SNI=fapi.binance.com
    pub async fn download_funding_rates(
        &self,
        symbol: &str,
        start_time: u64,
        end_time: u64,
        use_tunnel: bool,
    ) -> Result<Vec<FundingRateRecord>, DomainError> {
        let dir = format!("{}/history", self.data_dir);
        std::fs::create_dir_all(&dir).ok();

        let mut builder = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .connect_timeout(std::time::Duration::from_secs(10))
            .timeout(std::time::Duration::from_secs(30));
        if use_tunnel {
            // SSH 隧道：把 fapi.binance.com 解析到本地 10443（保持 SNI 正确）
            let addr: std::net::SocketAddr = "127.0.0.1:10443".parse().map_err(|e| {
                crate::error::ServiceError::MarketData(format!("隧道地址解析失败: {}", e))
            })?;
            builder = builder.resolve("fapi.binance.com", addr);
        }
        let client = builder.build().map_err(|e| {
            crate::error::ServiceError::MarketData(format!("HTTP客户端构建失败: {}", e))
        })?;

        let mut all: Vec<FundingRateRecord> = Vec::new();
        let mut current_start = start_time;
        println!("📥 下载 {} 资金费率数据...", symbol);

        loop {
            if current_start >= end_time {
                break;
            }
            let url = format!(
                "https://fapi.binance.com{}/fapi/v1/fundingRate?symbol={}&startTime={}&endTime={}&limit=1000",
                if use_tunnel { ":10443" } else { "" },
                symbol,
                current_start,
                end_time
            );
            let batch: Vec<FundingRateRecord> = client
                .get(&url)
                .send()
                .await
                .map_err(|e| {
                    crate::error::ServiceError::MarketData(format!("获取资金费率失败: {}", e))
                })?
                .json()
                .await
                .map_err(|e| {
                    crate::error::ServiceError::MarketData(format!("解析资金费率失败: {}", e))
                })?;

            if batch.is_empty() {
                break;
            }
            let last_time = batch.last().unwrap().funding_time;
            all.extend(batch);
            current_start = last_time + 1;
            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
            if all.len() % 1000 != 0 {
                break; // 不足整批，已到末尾
            }
        }

        let file_path = format!("{}/{}_funding.json", dir, symbol);
        let json = serde_json::to_string(&all).map_err(|e| {
            crate::error::ServiceError::MarketData(format!("序列化资金费率失败: {}", e))
        })?;
        std::fs::write(&file_path, &json)
            .map_err(|e| crate::error::InfrastructureError::io_with_operation("保存资金费率", e))?;
        println!("✅ 资金费率已保存: {} ({} 条)", file_path, all.len());
        Ok(all)
    }

    /// 加载资金费率历史，返回 (时间ms, 费率f64) 按时间升序
    pub fn load_funding_rates(&self, symbol: &str) -> Result<Vec<(u64, f64)>, DomainError> {
        let file_path = format!("{}/history/{}_funding.json", self.data_dir, symbol);
        let content = std::fs::read_to_string(&file_path)
            .map_err(|e| crate::error::InfrastructureError::io_with_operation("读取资金费率", e))?;
        let records: Vec<FundingRateRecord> = serde_json::from_str(&content)
            .map_err(|e| crate::error::ServiceError::MarketData(format!("解析资金费率: {}", e)))?;
        let mut out: Vec<(u64, f64)> = records
            .into_iter()
            .filter_map(|r| {
                r.funding_rate
                    .parse::<f64>()
                    .ok()
                    .map(|v| (r.funding_time, v))
            })
            .collect();
        out.sort_by_key(|r| r.0);
        Ok(out)
    }

    pub fn has_local_funding(&self, symbol: &str) -> bool {
        let file_path = format!("{}/history/{}_funding.json", self.data_dir, symbol);
        Path::new(&file_path).exists()
    }
}

/// 资金费率记录（Binance合约公共数据 fapi/v1/fundingRate）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FundingRateRecord {
    #[serde(rename = "fundingTime")]
    pub funding_time: u64,
    #[serde(rename = "fundingRate")]
    pub funding_rate: String, // API返回字符串如 "0.00006689"
}
