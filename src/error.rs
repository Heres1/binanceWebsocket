//! 统一错误处理模块
//! 
//! 提供全系统统一的错误类型和便捷宏
//! 
//! # 设计原则
//! - 分层错误：每层有自己的错误类型
//! - 自动转换：通过 From trait 自动向上转换
//! - 统一出口：对外暴露 DomainError

use thiserror::Error;

/// 系统统一错误类型
#[derive(Error, Debug)]
pub enum DomainError {
    /// 基础设施错误（IO、配置、日志等）
    #[error("基础设施错误: {0}")]
    Infrastructure(#[from] InfrastructureError),
    
    /// 事件总线错误
    #[error("事件总线错误: {0}")]
    EventBus(#[from] EventBusError),
    
    /// 领域服务错误
    #[error("服务错误: {0}")]
    Service(#[from] ServiceError),
    
    /// 未知错误
    #[error("未知错误: {0}")]
    Unknown(String),
}

/// 基础设施层错误
#[derive(Error, Debug)]
pub enum InfrastructureError {
    #[error("配置错误: {context} - {details}")]
    Config { context: String, details: String },
    
    #[error("IO错误在'{operation}'中: {source}")]
    Io { 
        operation: String, 
        #[source] 
        source: std::io::Error 
    },
    
    #[error("日志错误: {operation} - {reason}")]
    Log { operation: String, reason: String },
    
    #[error("数据库错误: {operation} - {reason}")]
    Database { operation: String, reason: String },
    
    #[error("日志初始化错误: {0}")]
    LoggerInit(#[from] log::SetLoggerError),
}

impl InfrastructureError {
    /// 创建配置错误（带上下文）
    pub fn config_with_context(context: impl Into<String>, details: impl Into<String>) -> Self {
        InfrastructureError::Config {
            context: context.into(),
            details: details.into(),
        }
    }
    
    /// 创建 IO 错误（带操作信息）
    pub fn io_with_operation(operation: impl Into<String>, source: std::io::Error) -> Self {
        InfrastructureError::Io {
            operation: operation.into(),
            source,
        }
    }
    
    /// 创建日志错误
    pub fn log_operation(operation: impl Into<String>, reason: impl Into<String>) -> Self {
        InfrastructureError::Log {
            operation: operation.into(),
            reason: reason.into(),
        }
    }
}

/// 事件总线错误
#[derive(Error, Debug)]
pub enum EventBusError {
    #[error("发布失败: {0}")]
    PublishFailed(String),
    
    #[error("订阅失败: {0}")]
    SubscribeFailed(String),
    
    #[error("处理失败: handler={handler}, error={error}")]
    HandleFailed { handler: String, error: String },
    
    #[error("通道关闭")]
    ChannelClosed,
}

/// 领域服务错误
#[derive(Error, Debug)]
pub enum ServiceError {
    #[error("订单服务错误: {0}")]
    Order(String),
    
    #[error("账户服务错误: {0}")]
    Account(String),
    
    #[error("风控服务错误: {0}")]
    Risk(String),
    
    #[error("市场数据错误: {0}")]
    MarketData(String),
}

/// 统一结果类型
pub type Result<T> = std::result::Result<T, DomainError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_infrastructure_error_creation() {
        let err = InfrastructureError::config_with_context("配置文件加载", "文件不存在");
        assert!(err.to_string().contains("配置错误"));
        assert!(err.to_string().contains("配置文件加载"));
    }

    #[test]
    fn test_event_bus_error_creation() {
        let err = EventBusError::PublishFailed("channel full".to_string());
        assert!(err.to_string().contains("发布失败"));
    }

    #[test]
    fn test_domain_error_from_infrastructure() {
        let infra_err = InfrastructureError::config_with_context("test", "test details");
        let domain_err: DomainError = infra_err.into();
        assert!(matches!(domain_err, DomainError::Infrastructure(_)));
    }

    #[test]
    fn test_service_error_creation() {
        let err = ServiceError::Order("API调用失败".to_string());
        assert!(err.to_string().contains("订单服务错误"));
    }
}

