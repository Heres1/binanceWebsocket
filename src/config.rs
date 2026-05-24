//! 配置管理模块
//!
//! 负责从 TOML 配置文件加载系统配置

use serde::Deserialize;
use std::fs;
use crate::error::{DomainError, InfrastructureError};

/// 应用主配置
#[derive(Deserialize, Clone, Debug)]
pub struct AppConfig {
    pub logging: LoggingConfig,
    pub binance: BinanceConfig,
    pub strategy: StrategyConfig,
    pub risk: RiskConfig,
    pub network: NetworkConfig,
}

/// 日志配置
#[derive(Deserialize, Clone, Debug)]
pub struct LoggingConfig {
    pub level: String,             // 日志级别: trace, debug, info, warn, error
    pub format: String,            // 日志格式: text, json
    pub file_path: Option<String>, // 日志文件路径
    pub rotate_size_mb: Option<u64>, // 轮转大小(MB)
    pub max_files: Option<usize>,  // 最大备份文件数
}

/// Binance API 配置
#[derive(Deserialize, Clone, Debug)]
pub struct BinanceConfig {
    pub api_key: String,
    pub secret_key: String,
    pub testnet: bool,  // true=测试网, false=实盘
}

/// 动量策略配置
#[derive(Deserialize, Clone, Debug)]
pub struct StrategyConfig {
    pub symbol: String,
    pub quantity_per_trade: f64,       // 每笔交易数量(BTC)
    pub take_profit_pct: f64,          // 止盈百分比
    pub stop_loss_pct: f64,            // 止损百分比
    pub max_hold_seconds: u64,         // 最大持仓时间(秒)
    pub cooldown_seconds: u64,         // 交易冷却时间(秒)
    pub max_daily_trades: u32,         // 日最大交易次数
    pub max_daily_loss_pct: f64,       // 日最大亏损百分比
    pub rsi_oversold: f64,             // RSI超卖线
    pub rsi_overbought: f64,           // RSI超买线
    pub volume_ratio_threshold: f64,   // 买卖量比阈值
    #[serde(default)]
    pub allow_short: bool,             // 是否允许做空
    #[serde(default = "default_strategy_type")]
    pub strategy_type: String,         // 策略类型
    #[serde(default)]
    pub short: Option<ShortConfig>,    // 做空配置
}

fn default_strategy_type() -> String {
    "TrendMomentum".to_string()
}

/// 做空配置
#[derive(Deserialize, Clone, Debug)]
pub struct ShortConfig {
    pub take_profit_pct: f64,
    pub stop_loss_pct: f64,
    #[serde(default)]
    pub trailing_stop_pct: f64,
    pub max_hold_seconds: u64,
    #[serde(default)]
    pub min_trend_strength: f64,
}

/// 风控配置
#[derive(Deserialize, Clone, Debug)]
pub struct RiskConfig {
    pub max_position_usdt: f64,       // 最大持仓金额
    pub max_single_order_usdt: f64,   // 单笔最大金额
    pub max_daily_loss_usdt: f64,     // 日亏损上限
    pub min_order_interval_secs: u64, // 最小下单间隔（秒）
}

/// 网络配置
#[derive(Deserialize, Clone, Debug)]
pub struct NetworkConfig {
    pub connection_mode: String,      // "auto", "direct", "proxy", "ssh_tunnel"
    pub proxy_url: Option<String>,
}

impl AppConfig {
    /// 从 TOML 文件加载配置，环境变量可覆盖敏感字段
    pub fn load(config_path: &str) -> Result<Self, DomainError> {
        let content = fs::read_to_string(config_path)
            .map_err(|e| InfrastructureError::io_with_operation("读取配置文件", e))?;
        
        let mut config: AppConfig = toml::from_str(&content)
            .map_err(|e| InfrastructureError::config_with_context(
                "解析 TOML 配置",
                e.to_string()
            ))?;
        
        // 环境变量覆盖 API 密钥（优先级高于配置文件）
        if let Ok(key) = std::env::var("BINANCE_API_KEY") {
            config.binance.api_key = key;
        }
        if let Ok(secret) = std::env::var("BINANCE_SECRET_KEY") {
            config.binance.secret_key = secret;
        }
        
        // 验证配置
        config.validate()?;
        
        Ok(config)
    }
    
    /// 验证配置有效性
    fn validate(&self) -> Result<(), DomainError> {
        // 验证 Binance 配置
        if self.binance.api_key.is_empty() {
            return Err(DomainError::Infrastructure(
                InfrastructureError::config_with_context(
                    "Binance API Key",
                    "不能为空".to_string()
                )
            ));
        }
        
        if self.binance.secret_key.is_empty() {
            return Err(DomainError::Infrastructure(
                InfrastructureError::config_with_context(
                    "Binance Secret Key",
                    "不能为空".to_string()
                )
            ));
        }
        
        // 验证策略配置
        if self.strategy.quantity_per_trade <= 0.0 {
            return Err(DomainError::Infrastructure(
                InfrastructureError::config_with_context(
                    "每笔交易数量",
                    "必须大于0".to_string()
                )
            ));
        }
        
        if self.strategy.take_profit_pct <= 0.0 || self.strategy.stop_loss_pct <= 0.0 {
            return Err(DomainError::Infrastructure(
                InfrastructureError::config_with_context(
                    "止盈止损",
                    "必须大于0".to_string()
                )
            ));
        }
        
        // 验证风控配置
        if self.risk.max_position_usdt <= 0.0 {
            return Err(DomainError::Infrastructure(
                InfrastructureError::config_with_context(
                    "最大持仓金额",
                    "必须大于0".to_string()
                )
            ));
        }
        
        if self.risk.max_single_order_usdt <= 0.0 {
            return Err(DomainError::Infrastructure(
                InfrastructureError::config_with_context(
                    "单笔最大金额",
                    "必须大于0".to_string()
                )
            ));
        }
        
        // 验证网络配置
        match self.network.connection_mode.as_str() {
            "auto" | "direct" | "proxy" | "ssh_tunnel" => {},
            other => {
                return Err(DomainError::Infrastructure(
                    InfrastructureError::config_with_context(
                        "网络连接模式",
                        format!("不支持的模式: {} (可选: auto, direct, proxy, ssh_tunnel)", other)
                    )
                ));
            }
        }
        
        Ok(())
    }
    
    /// 获取 Binance API 基础 URL
    pub fn get_binance_base_url(&self) -> &str {
        if self.binance.testnet {
            "https://testnet.binance.vision"
        } else {
            "https://api.binance.com"
        }
    }
    
    /// 获取 WebSocket URL
    pub fn get_ws_base_url(&self) -> &str {
        if self.binance.testnet {
            "wss://testnet.binance.vision"
        } else {
            "wss://stream.binance.com"
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_config_validation_success() {
        let config_content = r#"
            [logging]
            level = "info"
            format = "text"
            file_path = "logs/test.log"
            rotate_size_mb = 50
            max_files = 5

            [binance]
            api_key = "test_api_key"
            secret_key = "test_secret_key"
            testnet = true
            
            [strategy]
            symbol = "BTCUSDT"
            quantity_per_trade = 0.00013
            take_profit_pct = 0.4
            stop_loss_pct = 0.25
            max_hold_seconds = 300
            cooldown_seconds = 60
            max_daily_trades = 15
            max_daily_loss_pct = 2.0
            rsi_oversold = 35.0
            rsi_overbought = 65.0
            volume_ratio_threshold = 1.5
            
            [risk]
            max_position_usdt = 1000.0
            max_single_order_usdt = 100.0
            max_daily_loss_usdt = 50.0
            min_order_interval_secs = 10
            
            [network]
            connection_mode = "ssh_tunnel"
        "#;
        
        let config: AppConfig = toml::from_str(config_content).unwrap();
        assert!(config.validate().is_ok());
        assert!(config.binance.testnet);
        assert_eq!(config.get_binance_base_url(), "https://testnet.binance.vision");
    }
    
    #[test]
    fn test_config_validation_empty_api_key() {
        let config_content = r#"
            [logging]
            level = "info"
            format = "text"
            rotate_size_mb = 50
            max_files = 5

            [binance]
            api_key = ""
            secret_key = "test_secret_key"
            testnet = true
            
            [strategy]
            symbol = "BTCUSDT"
            quantity_per_trade = 0.00013
            take_profit_pct = 0.4
            stop_loss_pct = 0.25
            max_hold_seconds = 300
            cooldown_seconds = 60
            max_daily_trades = 15
            max_daily_loss_pct = 2.0
            rsi_oversold = 35.0
            rsi_overbought = 65.0
            volume_ratio_threshold = 1.5
            
            [risk]
            max_position_usdt = 1000.0
            max_single_order_usdt = 100.0
            max_daily_loss_usdt = 50.0
            min_order_interval_secs = 10
            
            [network]
            connection_mode = "direct"
        "#;
        
        let config: AppConfig = toml::from_str(config_content).unwrap();
        assert!(config.validate().is_err());
    }
    
    #[test]
    fn test_config_validation_invalid_network_mode() {
        let config_content = r#"
            [logging]
            level = "info"
            format = "text"
            rotate_size_mb = 50
            max_files = 5

            [binance]
            api_key = "test_key"
            secret_key = "test_secret"
            testnet = true
            
            [strategy]
            symbol = "BTCUSDT"
            quantity_per_trade = 0.00013
            take_profit_pct = 0.4
            stop_loss_pct = 0.25
            max_hold_seconds = 300
            cooldown_seconds = 60
            max_daily_trades = 15
            max_daily_loss_pct = 2.0
            rsi_oversold = 35.0
            rsi_overbought = 65.0
            volume_ratio_threshold = 1.5
            
            [risk]
            max_position_usdt = 1000.0
            max_single_order_usdt = 100.0
            max_daily_loss_usdt = 50.0
            min_order_interval_secs = 10
            
            [network]
            connection_mode = "invalid_mode"
        "#;
        
        let config: AppConfig = toml::from_str(config_content).unwrap();
        assert!(config.validate().is_err());
    }
    
    #[test]
    fn test_get_binance_urls() {
        let config_content = r#"
            [logging]
            level = "info"
            format = "text"
            rotate_size_mb = 50
            max_files = 5

            [binance]
            api_key = "test_key"
            secret_key = "test_secret"
            testnet = true
            
            [strategy]
            symbol = "BTCUSDT"
            quantity_per_trade = 0.00013
            take_profit_pct = 0.4
            stop_loss_pct = 0.25
            max_hold_seconds = 300
            cooldown_seconds = 60
            max_daily_trades = 15
            max_daily_loss_pct = 2.0
            rsi_oversold = 35.0
            rsi_overbought = 65.0
            volume_ratio_threshold = 1.5
            
            [risk]
            max_position_usdt = 1000.0
            max_single_order_usdt = 100.0
            max_daily_loss_usdt = 50.0
            min_order_interval_secs = 10
            
            [network]
            connection_mode = "direct"
        "#;
        
        let config: AppConfig = toml::from_str(config_content).unwrap();
        assert_eq!(config.get_binance_base_url(), "https://testnet.binance.vision");
        assert_eq!(config.get_ws_base_url(), "wss://testnet.binance.vision");
        
        let mut config_testnet = config.clone();
        config_testnet.binance.testnet = false;
        assert_eq!(config_testnet.get_binance_base_url(), "https://api.binance.com");
    }
}
