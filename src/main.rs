use std::sync::Arc;
use rust_binance_event_driven::event_bus::{EventBus, TokioEventBus, EventDispatcher};
use rust_binance_event_driven::services::{ConnectionMode, MarketDataService, OrderExecutionService};
use rust_binance_event_driven::strategies::MomentumStrategy;
use rust_binance_event_driven::config::AppConfig;
use rust_binance_event_driven::risk::RiskMonitorService;
use rust_binance_event_driven::clients::BinanceClient;
use rust_binance_event_driven::handlers::order_handler::OrderHandler;
use rust_binance_event_driven::infrastructure::logging::{AsyncLogger, LoggerConfig, LogFormat};
use log::LevelFilter;
use tokio::sync::Notify;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("动量短线交易系统启动");
    
    // ==========================================
    // 0. 加载配置文件
    // ==========================================
    println!("\n=== 加载配置文件 ===");
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
    } else {
        println!("✅ 日志系统已初始化");
        if let Some(ref path) = config.logging.file_path {
            println!("   日志文件: {}", path);
        }
    }
    println!("✅ 配置加载成功");
    println!("   Binance: {} (测试网: {})", 
        if config.binance.testnet { "测试网" } else { "实盘" },
        config.binance.testnet
    );
    println!("   交易对: {}", config.strategy.symbol);
    println!("   策略: 动量短线 | 止盈: {}% | 止损: {}%", config.strategy.take_profit_pct, config.strategy.stop_loss_pct);
    println!("   网络连接: {}", config.network.connection_mode);
    
    // ==========================================
    // 1. 创建事件总线
    // ==========================================
    let event_bus = Arc::new(TokioEventBus::new(100));
    
    // 2. 启动事件分发器
    let ready_notify = Arc::new(Notify::new());
    let dispatcher = EventDispatcher::with_ready_notify(event_bus.clone(), ready_notify.clone());
    tokio::spawn(dispatcher.run());
    ready_notify.notified().await;
    println!("✅ 事件分发器已就绪");
    
    // ==========================================
    // 3. 创建 Binance REST API 客户端
    // ==========================================
    println!("\n=== 初始化 Binance API 客户端 ===");
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
    println!("✅ Binance API 客户端已创建");
    
    // ==========================================
    // 3.5 验证 API 连通性
    // ==========================================
    println!("\n=== 验证 Binance REST API 连通性 ===");
    match binance_client.get_account().await {
        Ok(account) => {
            println!("✅ API 连接成功！账户信息:");
            for balance in &account.balances {
                let free: f64 = balance.free.parse().unwrap_or(0.0);
                let locked: f64 = balance.locked.parse().unwrap_or(0.0);
                if free > 0.0 || locked > 0.0 {
                    println!("   {} | 可用: {} | 冻结: {}", 
                        balance.asset, balance.free, balance.locked);
                }
            }
        }
        Err(e) => {
            println!("❌ API 连接失败: {}", e);
            println!("   请检查: 1) API密钥是否正确  2) IP白名单是否包含当前出口IP  3) 网络是否可达 api.binance.com");
            return Ok(());
        }
    }
    
    // ==========================================
    // 4. 创建风控服务
    // ==========================================
    println!("\n=== 初始化风控服务 ===");
    let risk_service = RiskMonitorService::new(
        config.risk.clone(),
        event_bus.clone(),
    );
    event_bus.subscribe(Arc::new(risk_service.clone()));
    println!("✅ 风控服务已注册");
    println!("   最大持仓: {} USDT", config.risk.max_position_usdt);
    println!("   单笔限额: {} USDT", config.risk.max_single_order_usdt);
    println!("   日亏损限额: {} USDT", config.risk.max_daily_loss_usdt);
    
    // ==========================================
    // 5. 创建订单执行服务
    // ==========================================
    println!("\n=== 初始化订单执行服务 ===");
    let order_execution = Arc::new(OrderExecutionService::new(
        binance_client.clone(),
        risk_service.clone(),
        event_bus.clone(),
        config.strategy.symbol.clone(),
    ));
    // 启动时同步真实账户余额
    order_execution.sync_balance().await?;
    event_bus.subscribe(order_execution.clone());
    println!("✅ 订单执行服务已注册（市价单模式）");
    
    // ==========================================
    // 6. 注册订单处理器
    // ==========================================
    let order_handler = Arc::new(OrderHandler::new(event_bus.clone()));
    event_bus.subscribe(order_handler);
    println!("✅ 订单处理器已注册");
    
    // ==========================================
    // 7. 注册动量短线策略
    let momentum_strategy = Arc::new(MomentumStrategy::new(
        config.strategy.clone(),
        event_bus.clone(),
    ));
    event_bus.subscribe(momentum_strategy);
    println!("✅ 动量短线策略已注册");
    println!("   止盈: {}% | 止损: {}% | 最大持仓: {}s",
        config.strategy.take_profit_pct, config.strategy.stop_loss_pct, config.strategy.max_hold_seconds);
    println!("   RSI超卖: {} | RSI超买: {} | 量比阈值: {}",
        config.strategy.rsi_oversold, config.strategy.rsi_overbought, config.strategy.volume_ratio_threshold);

    // ==========================================
    // 8. 启动市场数据服务（持续运行）
    // ==========================================
    println!("\n=== 启动动量交易监听 ===");
    
    // 根据配置设置连接模式
    let connection_mode = match config.network.connection_mode.as_str() {
        "auto" => ConnectionMode::Auto,
        "direct" => ConnectionMode::Direct,
        "proxy" => ConnectionMode::Proxy,
        "ssh_tunnel" => ConnectionMode::Direct,
        _ => ConnectionMode::Auto,
    };
    
    let market_data_service = MarketDataService::new(
        event_bus.clone(),
        vec![config.strategy.symbol.clone()]
    ).with_connection_mode(connection_mode);
    
    println!("🚀 动量短线交易系统已启动");
    println!("   交易对: {}", config.strategy.symbol);
    println!("   每笔交易: {} BTC", config.strategy.quantity_per_trade);
    println!("   日最大交易: {} 次 | 冷却: {}s", config.strategy.max_daily_trades, config.strategy.cooldown_seconds);
    println!("   连接模式: {:?}", connection_mode);
    println!("   按 Ctrl+C 停止程序");
    println!("=========================================\n");
    
    // 持续运行，直到用户按 Ctrl+C
    market_data_service.start().await?;
    
    Ok(())
}