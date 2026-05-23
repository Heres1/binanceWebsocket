//! 领域服务模块
//!
//! 包含核心业务服务：
//! - 市场数据服务：连接交易所获取实时行情
//! - 订单执行服务：处理下单、撤单等操作
//! - 账户同步服务：同步账户余额和持仓
//! - 风控监控服务：实时监控风险指标

pub mod market_data_service;

pub use market_data_service::{ConnectionMode, MarketDataService};
