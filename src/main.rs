use log::LevelFilter;
use rust_binance_event_driven::clients::BinanceClient;
use rust_binance_event_driven::config::{AppConfig, TradingPairConfig};
use rust_binance_event_driven::event_bus::{EventBus, EventDispatcher, TokioEventBus};
use rust_binance_event_driven::handlers::order_handler::OrderHandler;
use rust_binance_event_driven::infrastructure::logging::{AsyncLogger, LogFormat, LoggerConfig};
use rust_binance_event_driven::risk::RiskMonitorService;
use rust_binance_event_driven::services::{
    ConnectionMode, MarketDataService, OrderExecutionService,
};
use rust_binance_event_driven::strategies::MomentumStrategy;
use std::sync::Arc;
use tokio::sync::Notify;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n动量短线交易系统启动");

    // ==========================================
    // 0. 加载配置文件
    // ==========================================
    let config = AppConfig::load("config/default.toml")?;

    // 初始化日志系统
    let log_level = match config.logging.level.as_str() {
        "trace" => LevelFilter::Trace,
        "debug" => LevelFilter::Debug,
        "info" => LevelFilter::Info,
        "warn" => LevelFilter::Warn,
        "error" => LevelFilter::Error,
        _ => LevelFilter::Info,
    };
    let log_format = match config.logging.format.as_str() {
        "json" => LogFormat::Json,
        _ => LogFormat::Text,
    };
    let rotate_size = config.logging.rotate_size_mb.map(|mb| mb * 1024 * 1024);
    let max_files = config.logging.max_files.unwrap_or(5);

    let logger_config = LoggerConfig::new(
        log_level,
        log_format,
        config.logging.file_path.clone(),
        rotate_size,
        max_files,
    );

    if let Err(e) = AsyncLogger::init(logger_config) {
        eprintln!("⚠️ 日志系统初始化失败: {}，使用默认输出", e);
    }
    println!(
        "配置加载完成 | 日志: {} (每次启动新建文件)",
        config.logging.file_path.as_deref().unwrap_or("stdout")
    );
    println!(
        "   Binance: {} (测试网: {})",
        if config.binance.testnet {
            "测试网"
        } else {
            "实盘"
        },
        config.binance.testnet
    );
    println!("   交易对: {}", config.strategy.symbol);
    println!(
        "   策略: 动量短线 | 止盈: {}% | 止损: {}%",
        config.strategy.take_profit_pct, config.strategy.stop_loss_pct
    );
    println!("   网络连接: {}", config.network.connection_mode);

    // ==========================================
    // 1. 创建事件总线
    // ==========================================
    let event_bus = Arc::new(TokioEventBus::new(2000));

    // 2. 启动事件分发器
    let ready_notify = Arc::new(Notify::new());
    let dispatcher = EventDispatcher::with_ready_notify(event_bus.clone(), ready_notify.clone());
    tokio::spawn(dispatcher.run());
    ready_notify.notified().await;

    // ==========================================
    // 3. 创建 Binance REST API 客户端
    // ==========================================
    let base_url = if config.binance.testnet {
        "https://testnet.binance.vision".to_string()
    } else if std::env::var("USE_SSH_TUNNEL").is_ok() {
        // SSH 隧道模式：REST API 走本地 8443 端口转发
        "https://localhost:8443".to_string()
    } else {
        "https://api.binance.com".to_string()
    };
    let binance_client = BinanceClient::new(
        config.binance.api_key.clone(),
        config.binance.secret_key.clone(),
        base_url,
    );
    println!("Binance API 客户端已创建");

    // ==========================================
    // 3.5 验证 API 连通性
    // ==========================================
    match binance_client.get_account().await {
        Ok(account) => {
            let assets: Vec<String> = account
                .balances
                .iter()
                .filter(|b| {
                    b.free.parse::<f64>().unwrap_or(0.0) > 0.0
                        || b.locked.parse::<f64>().unwrap_or(0.0) > 0.0
                })
                .map(|b| format!("{}:{}", b.asset, b.free))
                .collect();
            println!("API 连接成功 | 账户资产: {}", assets.join(", "));
        }
        Err(e) => {
            println!("API 连接失败: {} | 请检查密钥/IP白名单/网络", e);
            return Ok(());
        }
    }

    // ==========================================
    // 4. 创建风控服务
    // ==========================================
    let risk_service = RiskMonitorService::new(config.risk.clone(), event_bus.clone());
    event_bus.subscribe(Arc::new(risk_service.clone()));

    // ==========================================
    // 4.5 解析多品种配置
    // ==========================================
    let trading_pairs = if config.trading_pairs.is_empty() {
        // 向后兼容：如果未配置trading_pairs，使用strategy单品种配置
        vec![TradingPairConfig {
            symbol: config.strategy.symbol.clone(),
            quantity_per_trade: config.strategy.quantity_per_trade,
            allow_short: config.strategy.allow_short,
        }]
    } else {
        config.trading_pairs.clone()
    };

    let symbols: Vec<String> = trading_pairs.iter().map(|p| p.symbol.clone()).collect();

    // 现货模式安全检查：现货API不支持裸卖空，强制禁用allow_short
    let is_spot_mode = !config.binance.testnet; // 实盘连接api.binance.com即现货
    if is_spot_mode {
        for pair in &trading_pairs {
            if pair.allow_short {
                log::error!("\u{26a0}\u{fe0f} 配置错误: {} 启用了allow_short=true，但当前连接的是现货API(api.binance.com)，现货不支持裸卖空！已强制禁用做空。", pair.symbol);
            }
        }
    }
    // 强制覆盖: 现货模式下all_short必须为false
    let trading_pairs: Vec<TradingPairConfig> = trading_pairs
        .into_iter()
        .map(|mut p| {
            if is_spot_mode && p.allow_short {
                p.allow_short = false;
            }
            p
        })
        .collect();

    log::info!(
        "启动多品种交易 | 品种数: {} | {}",
        symbols.len(),
        symbols.join(", ")
    );

    // ==========================================
    // 5. 创建订单执行服务
    // ==========================================
    let order_execution = Arc::new(OrderExecutionService::new(
        binance_client.clone(),
        risk_service.clone(),
        event_bus.clone(),
        symbols.clone(),
        config.strategy.use_limit_entry,
        config.strategy.limit_entry_offset_pct,
        config.strategy.limit_entry_wait_seconds,
    ));
    // 启动时同步真实账户余额
    order_execution.sync_balance().await?;
    // 启动定期余额同步（每60秒与交易所对账）
    order_execution.start_balance_sync_task();
    event_bus.subscribe(order_execution.clone());

    // ==========================================
    // 6. 注册订单处理器
    // ==========================================
    let order_handler = Arc::new(OrderHandler::new(event_bus.clone()));
    event_bus.subscribe(order_handler);

    // ==========================================
    // 7. 注册动量短线策略（多品种）
    // ==========================================
    // 资金费率过滤：启用时启动后台轮询并注入策略（合约情绪指标）
    let funding_cache = if config.strategy.use_funding_filter {
        let cache = rust_binance_event_driven::services::new_funding_cache();
        rust_binance_event_driven::services::spawn_funding_poller(
            trading_pairs.iter().map(|p| p.symbol.clone()).collect(),
            cache.clone(),
        );
        Some(cache)
    } else {
        None
    };
    for pair in &trading_pairs {
        let mut strategy_config = config.strategy.clone();
        strategy_config.symbol = pair.symbol.clone();
        strategy_config.quantity_per_trade = pair.quantity_per_trade;
        strategy_config.allow_short = pair.allow_short;

        let strategy = Arc::new(MomentumStrategy::new(
            strategy_config,
            event_bus.clone(),
            funding_cache.clone(),
        ));
        event_bus.subscribe(strategy);
    }

    // ==========================================
    // 8. 启动市场数据服务（持续运行）
    // ==========================================

    // 根据配置设置连接模式
    let connection_mode = match config.network.connection_mode.as_str() {
        "auto" => ConnectionMode::Auto,
        "direct" => ConnectionMode::Direct,
        "proxy" => ConnectionMode::Proxy,
        "ssh_tunnel" => ConnectionMode::Direct,
        _ => ConnectionMode::Auto,
    };

    let market_data_service = MarketDataService::new(event_bus.clone(), symbols.clone())
        .with_connection_mode(connection_mode);

    println!("\n=========================================");
    println!("  动量短线交易系统已启动");
    println!(
        "  品种: {} | 每笔: {} BTC",
        symbols.join(", "),
        config.strategy.quantity_per_trade
    );
    println!(
        "  止盈: {}% | 止损: {}% | 持仓上限: {}s",
        config.strategy.take_profit_pct,
        config.strategy.stop_loss_pct,
        config.strategy.max_hold_seconds
    );
    println!(
        "  连接: {:?} | 冷却: {}s | 日限: {}次",
        connection_mode, config.strategy.cooldown_seconds, config.strategy.max_daily_trades
    );
    println!("=========================================\n");

    // 持续运行，直到用户按 Ctrl+C
    market_data_service.start().await?;

    Ok(())
}
