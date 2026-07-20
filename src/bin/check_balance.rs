use hmac::{Hmac, Mac};
use reqwest::header;
use rust_binance_event_driven::config::AppConfig;
use serde::Deserialize;
use sha2::Sha256;
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Deserialize)]
struct PriceResponse {
    price: String,
}

#[derive(Debug, Deserialize)]
struct AccountInfo {
    #[serde(rename = "canTrade")]
    can_trade: bool,
    balances: Vec<Balance>,
}

#[derive(Debug, Deserialize)]
struct Balance {
    asset: String,
    free: String,
    locked: String,
}

fn sign(secret_key: &str, query: &str) -> String {
    let mut mac =
        HmacSha256::new_from_slice(secret_key.as_bytes()).expect("HMAC can take key of any size");
    mac.update(query.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

fn build_signed_query(secret_key: &str, params: &BTreeMap<String, String>) -> String {
    let query = params
        .iter()
        .map(|(key, value)| format!("{}={}", key, value))
        .collect::<Vec<_>>()
        .join("&");
    let signature = sign(secret_key, &query);
    format!("{}&signature={}", query, signature)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn round_btc_step(quantity: f64) -> f64 {
    ((quantity * 100_000.0) + 1e-9).floor() / 100_000.0
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = AppConfig::load("config/default.toml")?;
    let base_url = if config.binance.testnet {
        "https://testnet.binance.vision".to_string()
    } else if std::env::var("USE_SSH_TUNNEL").is_ok() {
        "https://localhost:8443".to_string()
    } else {
        "https://api.binance.com".to_string()
    };

    let proxy_url = std::env::var("HTTPS_PROXY")
        .or_else(|_| std::env::var("HTTP_PROXY"))
        .ok();
    let mut client_builder = reqwest::Client::builder();
    if base_url.contains("localhost") {
        let mut headers = header::HeaderMap::new();
        headers.insert(
            header::HOST,
            header::HeaderValue::from_static("api.binance.com"),
        );
        client_builder = client_builder
            .danger_accept_invalid_certs(true)
            .default_headers(headers);
    }
    if let Some(proxy_url) = proxy_url.as_deref() {
        client_builder = client_builder.proxy(reqwest::Proxy::all(proxy_url)?);
    }
    let http_client = client_builder.build()?;

    let mut params = BTreeMap::new();
    params.insert("recvWindow".to_string(), "5000".to_string());
    params.insert("timestamp".to_string(), now_ms().to_string());
    let signed_query = build_signed_query(&config.binance.secret_key, &params);
    let account_url = format!("{}/api/v3/account?{}", base_url, signed_query);
    let account: AccountInfo = http_client
        .get(account_url)
        .header("X-MBX-APIKEY", &config.binance.api_key)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let get_balance = |asset: &str| -> (f64, f64) {
        account
            .balances
            .iter()
            .find(|balance| balance.asset == asset)
            .map(|balance| {
                (
                    balance.free.parse::<f64>().unwrap_or(0.0),
                    balance.locked.parse::<f64>().unwrap_or(0.0),
                )
            })
            .unwrap_or((0.0, 0.0))
    };

    let (usdt_free, usdt_locked) = get_balance("USDT");
    let (btc_free, btc_locked) = get_balance("BTC");
    let (eth_free, eth_locked) = get_balance("ETH");
    let (sol_free, sol_locked) = get_balance("SOL");

    let price_url = format!("{}/api/v3/ticker/price?symbol=BTCUSDT", base_url);
    let price_resp: PriceResponse = http_client
        .get(price_url)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let btc_price = price_resp.price.parse::<f64>()?;

    let btc_value = (btc_free + btc_locked) * btc_price;
    let total_btc_equiv_usdt = usdt_free + usdt_locked + btc_value;
    let reserve_usdt = 1.0_f64.max(usdt_free * 0.015);
    let deployable_usdt = (usdt_free - reserve_usdt).max(0.0);
    let suggested_quantity = round_btc_step(deployable_usdt / btc_price);
    let suggested_order_usdt = suggested_quantity * btc_price;

    println!("账户余额检查（只读，不下单）");
    println!("账户可交易: {}", account.can_trade);
    println!("BTCUSDT当前价: {:.2} USDT", btc_price);
    println!("USDT: 可用 {:.4}, 锁定 {:.4}", usdt_free, usdt_locked);
    println!(
        "BTC:  可用 {:.8}, 锁定 {:.8}, 折合 {:.2} USDT",
        btc_free, btc_locked, btc_value
    );
    println!("ETH:  可用 {:.8}, 锁定 {:.8}", eth_free, eth_locked);
    println!("SOL:  可用 {:.8}, 锁定 {:.8}", sol_free, sol_locked);
    println!("现货可估总额(USDT+BTC): {:.2} USDT", total_btc_equiv_usdt);
    println!("建议保留缓冲: {:.2} USDT", reserve_usdt);
    println!(
        "建议BTC下单量: {:.5} BTC，约 {:.2} USDT",
        suggested_quantity, suggested_order_usdt
    );
    println!(
        "建议风控: 单笔上限 ≥ {:.2} USDT，持仓上限 ≥ {:.2} USDT",
        suggested_order_usdt * 1.08,
        total_btc_equiv_usdt * 1.08
    );

    Ok(())
}
