//! 回测系统模块
//!
//! 提供历史数据回测功能，支持K线回测和实时数据回放两种模式

pub mod data_loader;
pub mod engine;
pub mod recorder;
pub mod report;
pub mod strategy_v2;

pub use data_loader::DataLoader;
pub use engine::BacktestEngine;
pub use report::BacktestReport;
pub use strategy_v2::{run_backtest_v2, StrategyType, StrategyV2Config};
