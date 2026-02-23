//! 日志系统模块
//!
//! 负责处理应用程序的日志记录功能

pub mod logger;
pub use logger::{LogFormat, LoggerConfig, AsyncLogger};
