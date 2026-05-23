use std::sync::Arc;
use rust_binance_event_driven::commands::{Command, order_commands::PlaceOrderCommand};
use rust_binance_event_driven::command_bus::CommandBus;
use rust_binance_event_driven::event_bus::{EventBus, TokioEventBus, EventDispatcher};
use rust_binance_event_driven::services::{ConnectionMode, MarketDataService};
use rust_binance_event_driven::strategies::{GridStrategy};
use rust_binance_event_driven::strategies::grid_strategy::GridConfig;
use tokio::sync::Notify;
use tokio::time::Duration;
#[tokio::main]
async fn main(){
    println!("事件驱动交易系统启动");
    
    // 1.创建事件总线
    let event_bus = Arc::new(TokioEventBus::new(100));
    
    // 2.启动分发器
    let ready_notify = Arc::new(Notify::new());
    let dispatcher = EventDispatcher::with_ready_notify(event_bus.clone(), ready_notify.clone());
    tokio::spawn(dispatcher.run());
    ready_notify.notified().await;
    println!("事件分发器已就绪");
    
    // 5. 测试命令总线
    println!("\n=== 开始测试命令总线 ===");
    let command_bus = CommandBus::new(event_bus.clone());
    
    // 创建下单命令
    let place_order_cmd = PlaceOrderCommand::buy_limit("BTCUSDT", 50000.0, 0.001);
    let command = Command::PlaceOrder(place_order_cmd);
    
    // 发送命令
    let result = command_bus.send(command).await;
    println!("命令执行结果: {:?}", result);
    
    if result.is_success() {
        println!("✅ 命令执行成功!");
    } else if result.is_failure() {
        println!("❌ 命令执行失败!");
    }
    
    // 6. 注册网格策略（订阅价格事件，生成交易信号）
    println!("\n=== 初始化网格策略 ===");
    let grid_config = GridConfig::new(
        "grid_btc_1",
        "BTCUSDT",
        80000.0,   // 区间下限
        100000.0,  // 区间上限
        10,        // 10个网格
        0.001,     // 每格 0.001 BTC
    );
    let spacing = grid_config.grid_spacing();
    println!(
        "网格配置: 区间 [{:.0}, {:.0}] | {}格 | 间距 {:.0} USDT",
        grid_config.lower_price, grid_config.upper_price,
        grid_config.grid_count, spacing
    );
    let grid_strategy = Arc::new(GridStrategy::new(grid_config, event_bus.clone()));
    event_bus.subscribe(grid_strategy);
    println!("✅ 网格策略已注册，等待价格事件...");

    // 7. 启动市场数据服务（测试45秒）
    println!("\n=== 开始测试市场数据服务 ===");
    
    // 连接模式配置：
    // - ConnectionMode::Auto: 自动检测（优先使用 HTTPS_PROXY 环境变量）
    // - ConnectionMode::Direct: 强制直接连接（服务器环境推荐）
    // - ConnectionMode::Proxy: 强制使用代理（必须设置 HTTPS_PROXY）
    let connection_mode = ConnectionMode::Auto; // 改为 Direct 可在服务器直连
    
    let market_data_service = MarketDataService::new(
        event_bus.clone(),
        vec!["BTCUSDT".to_string()]
    ).with_connection_mode(connection_mode);
    
    println!("连接模式: {:?}", connection_mode);
    
    // 运行45秒后停止
    let market_handle = tokio::spawn(async move {
        println!("🚀 正在启动市场数据服务...");
        if let Err(e) = market_data_service.start().await {
            eprintln!("❌ 市场数据服务错误: {}", e);
        }
    });
    
    // 等待45秒接收实时数据，观察网格信号
    println!("正在接收Binance实时数据，持续45秒（观察网格交易信号）...");
    tokio::time::sleep(Duration::from_secs(45)).await;
    
    // 取消市场数据服务
    market_handle.abort();
    println!("\n程序已运行完成，即将退出");
}