//! 资金费率服务（实盘）
//!
//! 后台轮询 Binance 合约公共接口 fapi/v1/fundingRate（无需签名），
//! 为策略入场提供合约情绪过滤：费率极端为正 = 多头拥挤付费，不做多。
//!
//! 网络策略与回测下载器一致：USE_SSH_TUNNEL=1 时经 SSH 隧道 10443 端口，
//! 并强制 SNI=fapi.binance.com；否则直连。

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

/// symbol -> (最新资金费率, 成功拉取的本地时间ms)
pub type FundingRateCache = Arc<RwLock<HashMap<String, (f64, u64)>>>;

pub fn new_cache() -> FundingRateCache {
    Arc::new(RwLock::new(HashMap::new()))
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[derive(serde::Deserialize)]
struct FundingRateResp {
    #[serde(rename = "fundingRate")]
    funding_rate: String,
}

fn build_client(use_tunnel: bool) -> Result<reqwest::Client, reqwest::Error> {
    let mut builder = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(15));
    if use_tunnel {
        // SSH 隧道：把 fapi.binance.com 解析到本地 10443（保持 SNI 正确）
        if let Ok(addr) = "127.0.0.1:10443".parse::<std::net::SocketAddr>() {
            builder = builder.resolve("fapi.binance.com", addr);
        }
    }
    builder.build()
}

async fn fetch_latest(client: &reqwest::Client, symbol: &str, use_tunnel: bool) -> Option<f64> {
    let url = format!(
        "https://fapi.binance.com{}/fapi/v1/fundingRate?symbol={}&limit=1",
        if use_tunnel { ":10443" } else { "" },
        symbol
    );
    let resp = client.get(&url).send().await.ok()?;
    let rows: Vec<FundingRateResp> = resp.json().await.ok()?;
    rows.last()?.funding_rate.parse::<f64>().ok()
}

/// 启动后台轮询任务（每10分钟一次；费率8h才结算，10分钟足够新鲜）
pub fn spawn_poller(symbols: Vec<String>, cache: FundingRateCache) {
    let use_tunnel = std::env::var("USE_SSH_TUNNEL").is_ok();
    tokio::spawn(async move {
        let client = match build_client(use_tunnel) {
            Ok(c) => c,
            Err(e) => {
                log::warn!("⚠️ 资金费率客户端构建失败: {}，过滤功能停用", e);
                return;
            }
        };
        log::info!(
            "💰 资金费率轮询启动 | 品种: {:?} | 通道: {}",
            symbols,
            if use_tunnel {
                "SSH隧道10443"
            } else {
                "直连"
            }
        );
        loop {
            for sym in &symbols {
                match fetch_latest(&client, sym, use_tunnel).await {
                    Some(rate) => {
                        if let Ok(mut guard) = cache.write() {
                            guard.insert(sym.clone(), (rate, now_ms()));
                        }
                        log::debug!("💰 {} 资金费率更新: {:+.4}%", sym, rate * 100.0);
                    }
                    None => {
                        log::warn!("⚠️ {} 资金费率拉取失败（沿用旧值）", sym);
                    }
                }
            }
            tokio::time::sleep(Duration::from_secs(600)).await;
        }
    });
}

/// 读取当前费率。数据缺失或超过3小时未成功刷新时返回 None（调用方应按“放行”处理，
/// 避免网络故障期间完全冻结交易）。
pub fn current_rate(cache: &FundingRateCache, symbol: &str) -> Option<f64> {
    let guard = cache.read().ok()?;
    let (rate, fetched_at) = guard.get(symbol).copied()?;
    const STALE_MS: u64 = 3 * 3600 * 1000;
    if now_ms().saturating_sub(fetched_at) > STALE_MS {
        return None;
    }
    Some(rate)
}
