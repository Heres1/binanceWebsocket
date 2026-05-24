//! 领域服务模块
//!
//! 包含核心业务服务：
//! - 市场数据服务：连接交易所获取实时行情
//! - 订单执行服务：处理下单、撤单等操作
//! - 账户同步服务：同步账户余额和持仓
//! - 风控监控服务：实时监控风险指标

pub mod market_data_service;
pub mod order_execution_service;
// TODO: 用户数据流服务（需要在 BinanceClient 中添加 listen key 管理）
// pub mod user_stream_service;

pub use market_data_service::{ConnectionMode, MarketDataService};
pub use order_execution_service::OrderExecutionService;
// pub use user_stream_service::UserDataStreamService;
