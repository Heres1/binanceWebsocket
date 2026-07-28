use chrono::{DateTime, Local, NaiveDateTime, TimeZone, Utc};
use rust_binance_event_driven::clients::{BinanceClient, BinanceTrade};
use rust_binance_event_driven::config::AppConfig;
use std::env;

fn print_usage() {
    println!("只读查询 Binance 历史成交/订单，用于补全实盘日志证据链");
    println!("用法:");
    println!("  cargo run --bin query_trades -- --symbol BTCUSDT --start \"2026-07-20 00:00:00\" --end \"2026-07-21 08:00:00\"");
    println!("  USE_SSH_TUNNEL=1 cargo run --bin query_trades -- --start-ms 1784520000000 --end-ms 1784630400000 --orders");
    println!("参数:");
    println!("  --symbol SYMBOL       交易对，默认读取策略配置");
    println!("  --start TIME          起始时间，支持毫秒时间戳、RFC3339、YYYY-MM-DD HH:MM:SS");
    println!("  --end TIME            结束时间，格式同 --start");
    println!("  --start-ms MS         起始毫秒时间戳");
    println!("  --end-ms MS           结束毫秒时间戳");
    println!("  --limit N             最大返回数量，默认 1000");
    println!("  --orders              同时查询 allOrders");
}

fn parse_time_ms(value: &str) -> Result<u64, String> {
    if let Ok(ms) = value.parse::<u64>() {
        return Ok(ms);
    }

    if let Ok(dt) = DateTime::parse_from_rfc3339(value) {
        return Ok(dt.timestamp_millis() as u64);
    }

    let naive = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S")
        .map_err(|e| format!("时间格式无法解析: {} ({})", value, e))?;
    let local_dt = Local
        .from_local_datetime(&naive)
        .single()
        .ok_or_else(|| format!("本地时间存在歧义或不存在: {}", value))?;
    Ok(local_dt.timestamp_millis() as u64)
}

fn format_ms(ms: u64) -> String {
    match Utc.timestamp_millis_opt(ms as i64).single() {
        Some(dt) => dt
            .with_timezone(&Local)
            .format("%Y-%m-%d %H:%M:%S")
            .to_string(),
        None => format!("{}", ms),
    }
}

fn trade_side(trade: &BinanceTrade) -> &'static str {
    if trade.is_buyer {
        "BUY"
    } else {
        "SELL"
    }
}

fn parse_f64(value: &str) -> f64 {
    value.parse::<f64>().unwrap_or(0.0)
}

fn quote_asset(symbol: &str) -> &str {
    if symbol.ends_with("USDT") {
        "USDT"
    } else {
        ""
    }
}

fn base_asset(symbol: &str) -> &str {
    symbol.strip_suffix("USDT").unwrap_or(symbol)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        print_usage();
        return Ok(());
    }

    let config = AppConfig::load("config/default.toml")?;
    let mut symbol = config.strategy.symbol.clone();
    let mut start_ms: Option<u64> = None;
    let mut end_ms: Option<u64> = None;
    let mut limit: u16 = 1000;
    let mut include_orders = false;

    let mut index = 1;
    while index < args.len() {
        match args[index].as_str() {
            "--symbol" => {
                index += 1;
                symbol = args.get(index).ok_or("--symbol 缺少值")?.to_string();
            }
            "--start" => {
                index += 1;
                start_ms = Some(parse_time_ms(args.get(index).ok_or("--start 缺少值")?)?);
            }
            "--end" => {
                index += 1;
                end_ms = Some(parse_time_ms(args.get(index).ok_or("--end 缺少值")?)?);
            }
            "--start-ms" => {
                index += 1;
                start_ms = Some(args.get(index).ok_or("--start-ms 缺少值")?.parse()?);
            }
            "--end-ms" => {
                index += 1;
                end_ms = Some(args.get(index).ok_or("--end-ms 缺少值")?.parse()?);
            }
            "--limit" => {
                index += 1;
                limit = args
                    .get(index)
                    .ok_or("--limit 缺少值")?
                    .parse::<u16>()?
                    .min(1000);
            }
            "--orders" => {
                include_orders = true;
            }
            other => {
                return Err(format!("未知参数: {}，可用 --help 查看用法", other).into());
            }
        }
        index += 1;
    }

    if start_ms.is_none() {
        start_ms = Some((Utc::now().timestamp_millis() - 24 * 60 * 60 * 1000) as u64);
    }
    if end_ms.is_none() {
        end_ms = Some(Utc::now().timestamp_millis() as u64);
    }

    let base_url = if config.binance.testnet {
        "https://testnet.binance.vision".to_string()
    } else if env::var("USE_SSH_TUNNEL").is_ok() {
        "https://localhost:8443".to_string()
    } else {
        "https://api.binance.com".to_string()
    };

    let client = BinanceClient::new(
        config.binance.api_key.clone(),
        config.binance.secret_key.clone(),
        base_url.clone(),
    );

    println!("历史成交查询（只读，不下单）");
    println!(
        "网络: {}",
        if base_url.contains("localhost") {
            "SSH隧道"
        } else {
            "直连"
        }
    );
    println!("交易对: {}", symbol);
    println!(
        "时间段: {} -> {}",
        format_ms(start_ms.unwrap()),
        format_ms(end_ms.unwrap())
    );

    let trades = client
        .get_my_trades(&symbol, start_ms, end_ms, Some(limit))
        .await?;

    println!("\n成交明细: {} 条", trades.len());
    let base_asset = base_asset(&symbol);
    let quote_asset = quote_asset(&symbol);
    let mut buy_quote = 0.0;
    let mut sell_quote = 0.0;
    let mut quote_cash_flow = 0.0;
    let mut base_position_delta = 0.0;
    let mut quote_commission = 0.0;
    let mut base_commission = 0.0;
    let mut last_trade_price = 0.0;
    let mut other_commissions: Vec<String> = Vec::new();

    for trade in &trades {
        let price = parse_f64(&trade.price);
        let qty = parse_f64(&trade.qty);
        let quote = parse_f64(&trade.quote_qty);
        let commission = parse_f64(&trade.commission);
        last_trade_price = price;

        if trade.is_buyer {
            buy_quote += quote;
            quote_cash_flow -= quote;
            base_position_delta += qty;
        } else {
            sell_quote += quote;
            quote_cash_flow += quote;
            base_position_delta -= qty;
        }

        if trade.commission_asset == quote_asset {
            quote_commission += commission;
            quote_cash_flow -= commission;
        } else if trade.commission_asset == base_asset {
            base_commission += commission;
            base_position_delta -= commission;
        } else if commission > 0.0 {
            other_commissions.push(format!("{:.8} {}", commission, trade.commission_asset));
        }

        println!(
            "{} | {} | order:{} trade:{} | 价:{:.2} 数量:{:.8} 成交额:{:.4}U | 手续费:{} {} | maker:{}",
            format_ms(trade.time),
            trade_side(trade),
            trade.order_id,
            trade.id,
            price,
            qty,
            quote,
            trade.commission,
            trade.commission_asset,
            trade.is_maker
        );
    }

    let gross_pnl = sell_quote - buy_quote;
    let residual_base_value = if last_trade_price > 0.0 {
        base_position_delta * last_trade_price
    } else {
        0.0
    };
    let net_cash_flow = quote_cash_flow;
    let net_asset_pnl_est = net_cash_flow + residual_base_value;
    println!("\n区间汇总（按成交方向聚合）");
    println!("买入成交额: {:.4} USDT", buy_quote);
    println!("卖出成交额: {:.4} USDT", sell_quote);
    println!("毛盈亏: {:+.4} USDT", gross_pnl);
    println!(
        "{}手续费: {:.8} {}",
        quote_asset, quote_commission, quote_asset
    );
    if base_commission > 0.0 {
        println!(
            "{}手续费: {:.8} {}，已从残余{}仓位中扣除",
            base_asset, base_commission, base_asset, base_asset
        );
    }
    println!(
        "残余{}变化: {:+.8} {}，按末笔价估值: {:+.4} USDT",
        base_asset, base_position_delta, base_asset, residual_base_value
    );
    if !other_commissions.is_empty() {
        println!("其他手续费: {}", other_commissions.join(", "));
        println!("提示: 其他币种手续费未折算进净资产估算，需要额外按对应币种价格折算。");
    }
    println!("净现金流: {:+.4} USDT", net_cash_flow);
    println!("净资产盈亏估算: {:+.4} USDT", net_asset_pnl_est);

    if include_orders {
        let orders = client
            .get_all_orders(&symbol, start_ms, end_ms, Some(limit))
            .await?;
        println!("\n订单明细: {} 条", orders.len());
        for order in &orders {
            println!(
                "{} | {} {} {} | order:{} client:{} | 状态:{} | 原始量:{} 成交量:{} 成交额:{}U",
                format_ms(order.time),
                order.side,
                order.symbol,
                order.order_type,
                order.order_id,
                order.client_order_id,
                order.status,
                order.orig_qty,
                order.executed_qty,
                order.cummulative_quote_qty
            );
        }
    }

    Ok(())
}
