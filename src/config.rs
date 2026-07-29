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
    #[serde(default)]
    pub rotation: RotationConfig,
}

/// 日级动量轮动策略配置（3年跨周期回测验证：年化+49.4%）
#[derive(Deserialize, Clone, Debug)]
pub struct RotationConfig {
    #[serde(default = "default_rotation_symbols")]
    pub symbols: Vec<String>, // 品种池
    #[serde(default = "default_rotation_lookback")]
    pub momentum_lookback_days: usize, // 动量回看天数
    #[serde(default = "default_rotation_ma")]
    pub ma_filter_days: usize, // 趋势过滤均线天数
    #[serde(default = "default_rotation_rebal")]
    pub rebalance_interval_days: u64, // 调仓间隔天数
    #[serde(default = "default_rotation_check")]
    pub check_interval_seconds: u64, // 定时检查间隔（秒）
    #[serde(default = "default_rotation_min_usdt")]
    pub min_usdt_value: f64, // 低于此USDT价值视为无持仓
    #[serde(default = "default_rotation_dry_run")]
    pub dry_run: bool, // 干跑模式：只打印信号不下单
    #[serde(default = "default_rotation_rebal_on_start")]
    pub rebalance_on_start: bool, // 首次运行（无状态文件）是否立即按信号调仓对齐仓位，默认false等满一个周期
}

impl Default for RotationConfig {
    fn default() -> Self {
        Self {
            symbols: default_rotation_symbols(),
            momentum_lookback_days: default_rotation_lookback(),
            ma_filter_days: default_rotation_ma(),
            rebalance_interval_days: default_rotation_rebal(),
            check_interval_seconds: default_rotation_check(),
            min_usdt_value: default_rotation_min_usdt(),
            dry_run: default_rotation_dry_run(),
            rebalance_on_start: default_rotation_rebal_on_start(),
        }
    }
}

fn default_rotation_symbols() -> Vec<String> {
    vec![
        "BTCUSDT".to_string(),
        "ETHUSDT".to_string(),
        "SOLUSDT".to_string(),
    ]
}
fn default_rotation_lookback() -> usize {
    90
}
fn default_rotation_ma() -> usize {
    50
}
fn default_rotation_rebal() -> u64 {
    30
}
fn default_rotation_check() -> u64 {
    14400 // 4小时
}
fn default_rotation_min_usdt() -> f64 {
    10.0
}
fn default_rotation_dry_run() -> bool {
    true // 默认干跑，安全优先
}
fn default_rotation_rebal_on_start() -> bool {
    false // 默认首次运行不立即调仓，等满一个周期
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
    pub quantity_per_trade: f64, // 每笔交易数量(BTC)
    pub take_profit_pct: f64,    // 硬止盈百分比（上限）
    pub stop_loss_pct: f64,      // 初始止损百分比
    pub max_hold_seconds: u64,   // 最大持仓时间(秒)
    pub cooldown_seconds: u64,   // 交易冷却时间(秒)
    pub max_daily_trades: u32,   // 日最大交易次数
    pub max_daily_loss_pct: f64, // 日最大亏损百分比
    pub rsi_oversold: f64,       // RSI超卖线
    pub rsi_overbought: f64,     // RSI超买线
    #[serde(default = "default_short_rsi_overbought")]
    pub short_rsi_overbought: f64, // 做空专用RSI超买线（rsi_overbought常被设为100以禁用多头超买离场，做空需独立阈值，与rsi_oversold=40镜像）
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
    #[serde(default = "default_min_adx_di_diff")]
    pub min_adx_di_diff: f64, // +DI/-DI方向差最低门槛，避免ADX强但方向反的弱反弹入场
    // 波动率政权过滤
    #[serde(default = "default_atr_percentile_low")]
    pub atr_percentile_low: f64, // ATR百分位下限
    #[serde(default = "default_atr_percentile_high")]
    pub atr_percentile_high: f64, // ATR百分位上限
    // 多因子评分
    #[serde(default = "default_entry_score_threshold")]
    pub entry_score_threshold: u32, // 入场最低分数(满分100)
    #[serde(default = "default_breakout_max_rsi")]
    pub breakout_max_rsi: f64, // 突破路径RSI上限，防止反弹末端追高
    #[serde(default = "default_mean_revert_min_drop_pct")]
    pub mean_revert_min_drop_pct: f64, // 均值回归最小回撤幅度，过滤浅跌假反弹
    #[serde(default = "default_downtrend_filter_lookback")]
    pub downtrend_filter_lookback: usize, // 下跌中继结构检测窗口
    #[serde(default = "default_min_reversal_break_pct")]
    pub min_reversal_break_pct: f64, // 连续下移结构中的反转突破确认幅度
    // === 入场过滤开关（路线B：松绑过滤、用风控替代过滤） ===
    #[serde(default = "default_require_bid_support")]
    pub require_bid_support: bool, // 是否要求盘口买一量>1.5倍卖一量（实盘订单簿条件，K线回测无法精确模拟，默认关闭）
    #[serde(default = "default_rsi_bounce_max_rsi")]
    pub rsi_bounce_max_rsi: f64, // RSI反弹路径RSI上限（防止追入反弹末端）
    #[serde(default = "default_min_price_above_ema50_pct")]
    pub min_price_above_ema50_pct: f64, // 做多要求价格在EMA50上方的最小百分比
    #[serde(default = "default_mean_revert_require_above_ema50")]
    pub mean_revert_require_above_ema50: bool, // 均值回归是否要求价格仍在EMA50上方（默认关闭：深跌反弹允许短暂跌破）

    // === 执行层（成本端突破：限价单省滑点） ===
    #[serde(default = "default_use_limit_entry")]
    pub use_limit_entry: bool, // 开仓BUY限价单优先（挂低于信号价的买单），超时未成交撤单转市价
    #[serde(default = "default_limit_entry_offset_pct")]
    pub limit_entry_offset_pct: f64, // 限价挂单价低于信号价的百分比（默认0.05≈典型点差）
    #[serde(default = "default_limit_entry_wait_seconds")]
    pub limit_entry_wait_seconds: u64, // 限价单最长等待秒数，超时撤单转市价兑底

    // === 趋势判断日志（为合约多空积累经验） ===
    #[serde(default = "default_log_trend_snapshot")]
    pub log_trend_snapshot: bool, // 每根5m K线收盘打印趋势环境快照（多空对称指标）
    #[serde(default = "default_log_virtual_short")]
    pub log_virtual_short: bool, // 现货模式下评估并跟踪虚拟做空信号（纯日志不交易）

    // === 收益端结构优化（修复小赢大亏） ===
    #[serde(default = "default_partial_take_profit_pct")]
    pub partial_take_profit_pct: f64, // 分批止盈第一目标位%：触达后卖出部分仓位锁定盈利（0=禁用）
    #[serde(default = "default_partial_take_profit_ratio")]
    pub partial_take_profit_ratio: f64, // 分批止盈卖出比例（剩余仓位保本+继续奔跑）
    #[serde(default = "default_trend_break_exit")]
    pub trend_break_exit: bool, // 趋势破坏提前认亏：浮亏且EMA死叉+跌破EMA50时立即出场，不等硬止损

    // === 订单流信号（新数据源：CVD累计成交量差） ===
    #[serde(default = "default_use_cvd_filter")]
    pub use_cvd_filter: bool, // CVD确认过滤：入场要求近N根K线主动买盘净主导（过滤价涨但卖盘主导的假信号）
    #[serde(default = "default_cvd_lookback")]
    pub cvd_lookback: usize, // CVD净变化计算窗口（K线根数）

    // === 资金费率信号（新数据源：合约情绪指标） ===
    #[serde(default = "default_use_funding_filter")]
    pub use_funding_filter: bool, // 资金费率过滤：费率过热（多头拥挤付费）时禁止做多入场
    #[serde(default = "default_funding_long_block_pct")]
    pub funding_long_block_pct: f64, // 做多禁入资金费率阈值%（如0.03=0.03%，每8h结算）
}

fn default_strategy_type() -> String {
    "TrendMomentum".to_string()
}

fn default_short_rsi_overbought() -> f64 {
    60.0
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

fn default_min_adx_di_diff() -> f64 {
    1.2
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

fn default_breakout_max_rsi() -> f64 {
    60.0
}

fn default_mean_revert_min_drop_pct() -> f64 {
    1.5
}

fn default_downtrend_filter_lookback() -> usize {
    4
}

fn default_min_reversal_break_pct() -> f64 {
    0.05
}

fn default_require_bid_support() -> bool {
    false
}

fn default_rsi_bounce_max_rsi() -> f64 {
    60.0
}

fn default_min_price_above_ema50_pct() -> f64 {
    0.05
}

fn default_mean_revert_require_above_ema50() -> bool {
    false
}

fn default_use_limit_entry() -> bool {
    true
}

fn default_limit_entry_offset_pct() -> f64 {
    0.05
}

fn default_limit_entry_wait_seconds() -> u64 {
    30
}

fn default_log_trend_snapshot() -> bool {
    true
}

fn default_log_virtual_short() -> bool {
    true
}

fn default_partial_take_profit_pct() -> f64 {
    0.6
}

fn default_partial_take_profit_ratio() -> f64 {
    0.5
}

fn default_trend_break_exit() -> bool {
    true
}

fn default_use_cvd_filter() -> bool {
    false
}

fn default_cvd_lookback() -> usize {
    20
}

fn default_use_funding_filter() -> bool {
    false
}

fn default_funding_long_block_pct() -> f64 {
    0.03
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

        if self.strategy.breakout_max_rsi <= 0.0 || self.strategy.breakout_max_rsi > 100.0 {
            return Err(DomainError::Infrastructure(
                InfrastructureError::config_with_context(
                    "突破RSI上限",
                    "必须在0到100之间".to_string(),
                ),
            ));
        }

        if self.strategy.mean_revert_min_drop_pct <= 0.0 {
            return Err(DomainError::Infrastructure(
                InfrastructureError::config_with_context(
                    "均值回归最小回撤",
                    "必须大于0".to_string(),
                ),
            ));
        }

        if self.strategy.downtrend_filter_lookback < 2 {
            return Err(DomainError::Infrastructure(
                InfrastructureError::config_with_context(
                    "下跌中继检测窗口",
                    "必须至少为2".to_string(),
                ),
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
