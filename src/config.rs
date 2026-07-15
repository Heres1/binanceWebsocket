//! 配置管理模块
//!
//! 负责从 TOML 配置文件加载系统配置

use crate::error::{DomainError, InfrastructureError};
use serde::Deserialize;
use std::fs;

/// 应用主配置
#[derive(Deserialize, Clone, Debug)]
pub struct AppConfig {
    pub logging: LoggingConfig,
    pub binance: BinanceConfig,
    pub strategy: StrategyConfig,
    pub risk: RiskConfig,
    pub network: NetworkConfig,
    #[serde(default)]
    pub trading_pairs: Vec<TradingPairConfig>,
}

/// 交易对配置（多品种支持）
#[derive(Deserialize, Clone, Debug)]
pub struct TradingPairConfig {
    pub symbol: String,
    pub quantity_per_trade: f64,
    #[serde(default)]
    pub allow_short: bool,
}

/// 日志配置
#[derive(Deserialize, Clone, Debug)]
pub struct LoggingConfig {
    pub level: String,               // 日志级别: trace, debug, info, warn, error
    pub format: String,              // 日志格式: text, json
    pub file_path: Option<String>,   // 日志文件路径
    pub rotate_size_mb: Option<u64>, // 轮转大小(MB)
    pub max_files: Option<usize>,    // 最大备份文件数
}

/// Binance API 配置
#[derive(Deserialize, Clone, Debug)]
pub struct BinanceConfig {
    pub api_key: String,
    pub secret_key: String,
    pub testnet: bool, // true=测试网, false=实盘
}

/// 动量策略配置
#[derive(Deserialize, Clone, Debug)]
pub struct StrategyConfig {
    pub symbol: String,
    pub quantity_per_trade: f64,     // 每笔交易数量(BTC)
    pub take_profit_pct: f64,        // 硬止盈百分比（上限）
    pub stop_loss_pct: f64,          // 初始止损百分比
    pub max_hold_seconds: u64,       // 最大持仓时间(秒)
    pub cooldown_seconds: u64,       // 交易冷却时间(秒)
    pub max_daily_trades: u32,       // 日最大交易次数
    pub max_daily_loss_pct: f64,     // 日最大亏损百分比
    pub rsi_oversold: f64,           // RSI超卖线
    pub rsi_overbought: f64,         // RSI超买线
    pub volume_ratio_threshold: f64, // 买卖量比阈值
    #[serde(default = "default_breakeven_trigger")]
    pub breakeven_trigger_pct: f64, // 触发保本止损的浮盈百分比
    #[serde(default = "default_trailing_trigger")]
    pub trailing_trigger_pct: f64, // 触发追踪止损的浮盈百分比
    #[serde(default = "default_trailing_distance")]
    pub trailing_distance_pct: f64, // 追踪止损回撤距离百分比
    #[serde(default = "default_round_trip_fee")]
    pub round_trip_fee_pct: f64, // 往返手续费百分比(默认0.1%)
    #[serde(default)]
    pub allow_short: bool, // 是否允许做空
    #[serde(default = "default_strategy_type")]
    pub strategy_type: String, // 策略类型
    #[serde(default)]
    pub short: Option<ShortConfig>, // 做空配置
    #[serde(default = "default_stale_exit_seconds")]
    pub stale_exit_seconds: u64, // 僵尸仓位早退时间(秒) - 浮盈不足时提前退出
    #[serde(default = "default_stale_pnl_threshold")]
    pub stale_pnl_threshold_pct: f64, // 僵尸判定门槛% - 浮盈低于此值视为僵尸
    #[serde(default = "default_min_trend_strength")]
    pub min_trend_strength_pct: f64, // EMA趋势强度最低门槛%
    #[serde(default = "default_max_ema50_distance")]
    pub max_ema50_distance_pct: f64, // 价格距离EMA50的最大百分比(趋势过度延伸过滤)
    #[serde(default = "default_post_stoploss_cooldown")]
    pub post_stoploss_cooldown_seconds: u64, // 止损后的延长冷却时间(秒)
    #[serde(default = "default_mean_revert_min_slope")]
    pub mean_revert_min_slope: f64, // 均值回归最低斜率门槛(%)，低于此值禁止入场
    #[serde(default = "default_breakout_min_slope")]
    pub breakout_min_slope: f64, // 突破路径最低斜率门槛(%)
    // ATR动态止损
    #[serde(default = "default_atr_stop_multiplier")]
    pub atr_stop_multiplier: f64, // 止损距离 = N * ATR
    #[serde(default = "default_atr_trailing_multiplier")]
    pub atr_trailing_multiplier: f64, // 追踪止损触发 = N * ATR profit
    #[serde(default = "default_atr_trailing_distance")]
    pub atr_trailing_distance: f64, // 追踪止损距离 = N * ATR
    #[serde(default = "default_use_atr_stops")]
    pub use_atr_stops: bool, // 是否启用ATR止损
    // ADX过滤
    #[serde(default = "default_adx_min_threshold")]
    pub adx_min_threshold: f64, // ADX最低门槛(低于=震荡市)
    #[serde(default = "default_adx_strong_trend")]
    pub adx_strong_trend: f64, // 强趋势阈值(入场加分)
    // 波动率政权过滤
    #[serde(default = "default_atr_percentile_low")]
    pub atr_percentile_low: f64, // ATR百分位下限
    #[serde(default = "default_atr_percentile_high")]
    pub atr_percentile_high: f64, // ATR百分位上限
    // 多因子评分
    #[serde(default = "default_entry_score_threshold")]
    pub entry_score_threshold: u32, // 入场最低分数(满分100)
}

fn default_strategy_type() -> String {
    "TrendMomentum".to_string()
}

fn default_breakeven_trigger() -> f64 {
    0.3
}

fn default_trailing_trigger() -> f64 {
    0.5
}

fn default_trailing_distance() -> f64 {
    0.3
}

fn default_round_trip_fee() -> f64 {
    0.1
}

fn default_stale_exit_seconds() -> u64 {
    14400 // 4小时
}

fn default_stale_pnl_threshold() -> f64 {
    0.3 // 浮盈不足0.3%视为僵尸
}

fn default_min_trend_strength() -> f64 {
    0.0 // 默认0不过滤，向后兼容
}

fn default_max_ema50_distance() -> f64 {
    99.0 // 默认99.0=不过滤，向后兼容
}

fn default_post_stoploss_cooldown() -> u64 {
    0 // 默认0=使用普通冷却时间，向后兼容
}

fn default_mean_revert_min_slope() -> f64 {
    -0.05 // EMA21斜率必须 > -0.05% 才允许均值回归入场
}

fn default_breakout_min_slope() -> f64 {
    0.05 // 突破路径要求EMA21斜率 > 0.05%
}

fn default_atr_stop_multiplier() -> f64 {
    2.0
}

fn default_atr_trailing_multiplier() -> f64 {
    2.5
}

fn default_atr_trailing_distance() -> f64 {
    1.5
}

fn default_use_atr_stops() -> bool {
    false // 默认关闭，向后兼容
}

fn default_adx_min_threshold() -> f64 {
    20.0
}

fn default_adx_strong_trend() -> f64 {
    30.0
}

fn default_atr_percentile_low() -> f64 {
    20.0
}

fn default_atr_percentile_high() -> f64 {
    80.0
}

fn default_entry_score_threshold() -> u32 {
    55
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
    #[serde(default = "default_position_allocation_pct")]
    pub position_allocation_pct: f64, // 动态仓位资金使用率
    #[serde(default = "default_min_usdt_reserve")]
    pub min_usdt_reserve: f64, // 最小USDT缓冲，避免价格波动导致资金不足
}

fn default_position_allocation_pct() -> f64 {
    0.985
}

fn default_min_usdt_reserve() -> f64 {
    2.0
}

/// 网络配置
#[derive(Deserialize, Clone, Debug)]
pub struct NetworkConfig {
    pub connection_mode: String, // "auto", "direct", "proxy", "ssh_tunnel"
    pub proxy_url: Option<String>,
}

impl AppConfig {
    /// 从 TOML 文件加载配置，环境变量可覆盖敏感字段
    pub fn load(config_path: &str) -> Result<Self, DomainError> {
        let content = fs::read_to_string(config_path)
            .map_err(|e| InfrastructureError::io_with_operation("读取配置文件", e))?;

        let mut config: AppConfig = toml::from_str(&content).map_err(|e| {
            InfrastructureError::config_with_context("解析 TOML 配置", e.to_string())
        })?;

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
                InfrastructureError::config_with_context("Binance API Key", "不能为空".to_string()),
            ));
        }

        if self.binance.secret_key.is_empty() {
            return Err(DomainError::Infrastructure(
                InfrastructureError::config_with_context(
                    "Binance Secret Key",
                    "不能为空".to_string(),
                ),
            ));
        }

        // 验证策略配置
        if self.strategy.quantity_per_trade <= 0.0 {
            return Err(DomainError::Infrastructure(
                InfrastructureError::config_with_context("每笔交易数量", "必须大于0".to_string()),
            ));
        }

        if self.strategy.take_profit_pct <= 0.0 || self.strategy.stop_loss_pct <= 0.0 {
            return Err(DomainError::Infrastructure(
                InfrastructureError::config_with_context("止盈止损", "必须大于0".to_string()),
            ));
        }

        // 参数层级约束: breakeven < trailing_trigger < take_profit
        if self.strategy.breakeven_trigger_pct >= self.strategy.trailing_trigger_pct {
            return Err(DomainError::Infrastructure(
                InfrastructureError::config_with_context(
                    "参数层级错误",
                    format!(
                        "breakeven_trigger({:.2}%) 必须 < trailing_trigger({:.2}%)",
                        self.strategy.breakeven_trigger_pct, self.strategy.trailing_trigger_pct
                    ),
                ),
            ));
        }
        if self.strategy.trailing_trigger_pct >= self.strategy.take_profit_pct {
            return Err(DomainError::Infrastructure(
                InfrastructureError::config_with_context(
                    "参数层级错误",
                    format!("trailing_trigger({:.2}%) 必须 < take_profit({:.2}%)，否则追踪止损永远不会触发",
                        self.strategy.trailing_trigger_pct, self.strategy.take_profit_pct)
                )
            ));
        }
        if self.strategy.volume_ratio_threshold <= 0.0 {
            return Err(DomainError::Infrastructure(
                InfrastructureError::config_with_context("量比阈值", "必须大于0".to_string()),
            ));
        }

        // 验证风控配置
        if self.risk.max_position_usdt <= 0.0 {
            return Err(DomainError::Infrastructure(
                InfrastructureError::config_with_context("最大持仓金额", "必须大于0".to_string()),
            ));
        }

        if self.risk.max_single_order_usdt <= 0.0 {
            return Err(DomainError::Infrastructure(
                InfrastructureError::config_with_context("单笔最大金额", "必须大于0".to_string()),
            ));
        }

        if self.risk.max_single_order_usdt > self.risk.max_position_usdt {
            return Err(DomainError::Infrastructure(
                InfrastructureError::config_with_context(
                    "风控配置冲突",
                    format!(
                        "单笔最大金额({:.2})不应超过最大持仓金额({:.2})",
                        self.risk.max_single_order_usdt, self.risk.max_position_usdt
                    ),
                ),
            ));
        }

        if self.risk.max_daily_loss_usdt <= 0.0 {
            return Err(DomainError::Infrastructure(
                InfrastructureError::config_with_context("日亏损上限", "必须大于0".to_string()),
            ));
        }

        if self.risk.position_allocation_pct <= 0.0 || self.risk.position_allocation_pct > 1.0 {
            return Err(DomainError::Infrastructure(
                InfrastructureError::config_with_context(
                    "动态仓位资金使用率",
                    "必须在(0, 1]之间".to_string(),
                ),
            ));
        }

        if self.risk.min_usdt_reserve < 0.0 {
            return Err(DomainError::Infrastructure(
                InfrastructureError::config_with_context("USDT缓冲", "不能为负数".to_string()),
            ));
        }

        // 验证网络配置
        match self.network.connection_mode.as_str() {
            "auto" | "direct" | "proxy" | "ssh_tunnel" => {}
            other => {
                return Err(DomainError::Infrastructure(
                    InfrastructureError::config_with_context(
                        "网络连接模式",
                        format!(
                            "不支持的模式: {} (可选: auto, direct, proxy, ssh_tunnel)",
                            other
                        ),
                    ),
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
            take_profit_pct = 2.0
            stop_loss_pct = 1.5
            max_hold_seconds = 300
            cooldown_seconds = 60
            max_daily_trades = 15
            max_daily_loss_pct = 2.0
            rsi_oversold = 35.0
            rsi_overbought = 65.0
            volume_ratio_threshold = 1.5
            breakeven_trigger_pct = 0.5
            trailing_trigger_pct = 1.5
            trailing_distance_pct = 0.5
            
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
        assert_eq!(
            config.get_binance_base_url(),
            "https://testnet.binance.vision"
        );
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
        assert_eq!(
            config.get_binance_base_url(),
            "https://testnet.binance.vision"
        );
        assert_eq!(config.get_ws_base_url(), "wss://testnet.binance.vision");

        let mut config_testnet = config.clone();
        config_testnet.binance.testnet = false;
        assert_eq!(
            config_testnet.get_binance_base_url(),
            "https://api.binance.com"
        );
    }
}
