//! 回测系统模块
//!
//! 提供历史数据回测功能，支持K线回测和实时数据回放两种模式

pub mod data_loader;
pub mod engine;
pub mod report;
pub mod recorder;
pub mod strategy_v2;

pub use engine::BacktestEngine;
pub use report::BacktestReport;
pub use data_loader::DataLoader;
pub use strategy_v2::{run_backtest_v2, StrategyV2Config, StrategyType};
