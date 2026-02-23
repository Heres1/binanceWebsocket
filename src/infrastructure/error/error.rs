use thiserror::Error;
pub type Result<T> = std::result::Result<T, InfrastructureError>;

#[derive(Error, Debug)]
pub enum InfrastructureError {
    /// 配置错误
    #[error("配置错误: {0}")]
    ConfigError(String),
    
}
