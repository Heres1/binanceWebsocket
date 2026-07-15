//! 风控规则定义
//!
//! 实现各种风控检查规则

use chrono::{DateTime, Utc};
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::config::RiskConfig;
use crate::events::{RiskAlertEvent, RiskLevel};

/// 风控规则引擎
pub struct RiskRules {
    config: RiskConfig,
    state: Arc<Mutex<RiskState>>,
}

impl Clone for RiskRules {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            state: self.state.clone(),
        }
    }
}

/// 风控状态
#[derive(Debug, Clone)]
pub struct RiskState {
    /// 当前持仓金额 (USDT)
    pub current_position_usdt: f64,
    /// 当日亏损金额 (USDT)
    pub daily_loss_usdt: f64,
    /// 上次下单时间
    pub last_order_time: Option<DateTime<Utc>>,
    /// 当日订单数量
    pub daily_order_count: u32,
    /// 日期（用于重置日统计）
    pub trading_date: String,
}

/// 风控检查结果
#[derive(Debug)]
pub enum RiskCheckResult {
    /// 通过检查
    Pass,
    /// 警告（可以交易，但有风险）
    Warning(String),
    /// 拒绝（禁止交易）
    Rejected(String),
}

impl RiskRules {
    /// 创建新的风控规则引擎
    pub fn new(config: RiskConfig) -> Self {
        let today = Utc::now().format("%Y-%m-%d").to_string();

        Self {
            config,
            state: Arc::new(Mutex::new(RiskState {
                current_position_usdt: 0.0,
                daily_loss_usdt: 0.0,
                last_order_time: None,
                daily_order_count: 0,
                trading_date: today,
            })),
        }
    }

    /// 资金充足性检查
    pub async fn check_capital(
        &self,
        available_balance: f64,
        order_amount: f64,
    ) -> Result<(), RiskAlertEvent> {
        if available_balance < order_amount {
            let alert = RiskAlertEvent::new(
                RiskLevel::High,
                "CAPITAL_INSUFFICIENT".to_string(),
                format!(
                    "资金不足: 可用 {:.2} USDT，需要 {:.2} USDT",
                    available_balance, order_amount
                ),
                "available_balance >= order_amount".to_string(),
            );
            return Err(alert);
        }

        // 资金紧张警告
        if available_balance < order_amount * 2.0 {
            log::warn!(
                "资金紧张，可用 {:.2} USDT，订单需要 {:.2} USDT",
                available_balance,
                order_amount
            );
        }

        Ok(())
    }

    /// 持仓限额检查
    pub async fn check_position_limit(
        &self,
        current_position: f64,
        order_amount: f64,
    ) -> Result<(), RiskAlertEvent> {
        let new_position = current_position + order_amount;

        if new_position > self.config.max_position_usdt {
            let alert = RiskAlertEvent::new(
                RiskLevel::Critical,
                "POSITION_LIMIT_EXCEEDED".to_string(),
                format!(
                    "持仓超限: 当前 {:.2} + 订单 {:.2} = {:.2} USDT，超过限制 {:.2} USDT",
                    current_position, order_amount, new_position, self.config.max_position_usdt
                ),
                "total_position <= max_position".to_string(),
            );
            return Err(alert);
        }

        Ok(())
    }

    /// 单笔金额检查
    pub async fn check_single_order_limit(&self, order_amount: f64) -> Result<(), RiskAlertEvent> {
        if order_amount > self.config.max_single_order_usdt {
            let alert = RiskAlertEvent::new(
                RiskLevel::High,
                "SINGLE_ORDER_LIMIT_EXCEEDED".to_string(),
                format!(
                    "单笔金额超限: {:.2} USDT，超过限制 {:.2} USDT",
                    order_amount, self.config.max_single_order_usdt
                ),
                "order_amount <= max_single_order".to_string(),
            );
            return Err(alert);
        }

        // 金额较大警告
        if order_amount > self.config.max_single_order_usdt * 0.8 {
            log::warn!(
                "单笔金额较大: {:.2} USDT (限制: {:.2} USDT)",
                order_amount,
                self.config.max_single_order_usdt
            );
        }

        Ok(())
    }

    /// 日亏损检查
    pub async fn check_daily_loss(&self) -> Result<(), RiskAlertEvent> {
        let state = self.state.lock().await;

        if state.daily_loss_usdt >= self.config.max_daily_loss_usdt {
            let alert = RiskAlertEvent::new(
                RiskLevel::Critical,
                "DAILY_LOSS_LIMIT_EXCEEDED".to_string(),
                format!(
                    "日亏损超限: 已亏损 {:.2} USDT，超过限制 {:.2} USDT",
                    state.daily_loss_usdt, self.config.max_daily_loss_usdt
                ),
                "daily_loss <= max_daily_loss".to_string(),
            )
            .with_suggested_action("建议停止今日交易".to_string());

            return Err(alert);
        }

        // 亏损接近上限警告
        if state.daily_loss_usdt > self.config.max_daily_loss_usdt * 0.8 {
            log::warn!(
                "日亏损接近上限: {:.2} / {:.2} USDT",
                state.daily_loss_usdt,
                self.config.max_daily_loss_usdt
            );
        }

        Ok(())
    }

    /// 下单频率检查
    pub async fn check_order_frequency(&self) -> Result<(), RiskAlertEvent> {
        let state = self.state.lock().await;

        if let Some(last_time) = state.last_order_time {
            let now = Utc::now();
            let elapsed = (now - last_time).num_seconds() as u64;

            if elapsed < self.config.min_order_interval_secs {
                let alert = RiskAlertEvent::new(
                    RiskLevel::Medium,
                    "ORDER_TOO_FREQUENT".to_string(),
                    format!(
                        "下单过于频繁: 距上次下单 {} 秒，最小间隔 {} 秒",
                        elapsed, self.config.min_order_interval_secs
                    ),
                    "order_interval >= min_interval".to_string(),
                );
                return Err(alert);
            }
        }

        Ok(())
    }

    /// 统一风控检查（交易前调用）—— 单次加锁完成所有检查
    pub async fn pre_trade_check(
        &self,
        order_amount: f64,
        available_balance: f64,
        current_position: f64,
    ) -> Result<RiskCheckResult, RiskAlertEvent> {
        let mut state = self.state.lock().await;

        // 0. 重置日统计
        let today = Utc::now().format("%Y-%m-%d").to_string();
        if state.trading_date != today {
            log::info!("新的一天，重置风控日统计");
            state.daily_loss_usdt = 0.0;
            state.daily_order_count = 0;
            state.trading_date = today;
        }

        // 1. 检查下单频率
        if let Some(last_time) = state.last_order_time {
            let now = Utc::now();
            let elapsed = (now - last_time).num_seconds() as u64;
            if elapsed < self.config.min_order_interval_secs {
                return Err(RiskAlertEvent::new(
                    RiskLevel::Medium,
                    "ORDER_TOO_FREQUENT".to_string(),
                    format!(
                        "下单过于频繁: 距上次下单 {} 秒，最小间隔 {} 秒",
                        elapsed, self.config.min_order_interval_secs
                    ),
                    "order_interval >= min_interval".to_string(),
                ));
            }
        }

        // 2. 检查单笔金额
        if order_amount > self.config.max_single_order_usdt {
            return Err(RiskAlertEvent::new(
                RiskLevel::High,
                "SINGLE_ORDER_LIMIT_EXCEEDED".to_string(),
                format!(
                    "单笔金额超限: {:.2} USDT，超过限制 {:.2} USDT",
                    order_amount, self.config.max_single_order_usdt
                ),
                "order_amount <= max_single_order".to_string(),
            ));
        }

        // 3. 检查资金充足性
        if available_balance < order_amount {
            return Err(RiskAlertEvent::new(
                RiskLevel::High,
                "CAPITAL_INSUFFICIENT".to_string(),
                format!(
                    "资金不足: 可用 {:.2} USDT，需要 {:.2} USDT",
                    available_balance, order_amount
                ),
                "available_balance >= order_amount".to_string(),
            ));
        }

        // 4. 检查持仓限额
        let new_position = current_position + order_amount;
        if new_position > self.config.max_position_usdt {
            return Err(RiskAlertEvent::new(
                RiskLevel::Critical,
                "POSITION_LIMIT_EXCEEDED".to_string(),
                format!(
                    "持仓超限: 当前 {:.2} + 订单 {:.2} = {:.2} USDT，超过限制 {:.2} USDT",
                    current_position, order_amount, new_position, self.config.max_position_usdt
                ),
                "total_position <= max_position".to_string(),
            ));
        }

        // 5. 检查日亏损
        if state.daily_loss_usdt >= self.config.max_daily_loss_usdt {
            return Err(RiskAlertEvent::new(
                RiskLevel::Critical,
                "DAILY_LOSS_LIMIT_EXCEEDED".to_string(),
                format!(
                    "日亏损超限: 已亏损 {:.2} USDT，超过限制 {:.2} USDT",
                    state.daily_loss_usdt, self.config.max_daily_loss_usdt
                ),
                "daily_loss <= max_daily_loss".to_string(),
            )
            .with_suggested_action("建议停止今日交易".to_string()));
        }

        // 所有检查通过
        Ok(RiskCheckResult::Pass)
    }

    /// 更新订单记录
    pub async fn record_order(&self, amount: f64) {
        let mut state = self.state.lock().await;
        state.last_order_time = Some(Utc::now());
        state.daily_order_count += 1;

        log::debug!(
            "风控记录: 订单金额 {:.2} USDT，今日第 {} 单",
            amount,
            state.daily_order_count
        );
    }

    /// 更新亏损
    pub async fn update_loss(&self, loss: f64) {
        let mut state = self.state.lock().await;
        state.daily_loss_usdt += loss;

        log::debug!(
            "风控更新: 今日亏损 {:.2} USDT / 限制 {:.2} USDT",
            state.daily_loss_usdt,
            self.config.max_daily_loss_usdt
        );
    }

    /// 更新持仓
    pub async fn update_position(&self, position: f64) {
        let mut state = self.state.lock().await;
        state.current_position_usdt = position;
    }

    /// 获取风控配置快照
    pub fn config(&self) -> &RiskConfig {
        &self.config
    }

    /// 获取当前风控状态
    pub async fn get_state(&self) -> RiskState {
        self.state.lock().await.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> RiskConfig {
        RiskConfig {
            max_position_usdt: 1000.0,
            max_single_order_usdt: 100.0,
            max_daily_loss_usdt: 50.0,
            min_order_interval_secs: 10,
            position_allocation_pct: 0.985,
            min_usdt_reserve: 2.0,
        }
    }

    #[tokio::test]
    async fn test_capital_check_success() {
        let rules = RiskRules::new(create_test_config());

        // 资金充足
        assert!(rules.check_capital(500.0, 100.0).await.is_ok());
    }

    #[tokio::test]
    async fn test_capital_check_failure() {
        let rules = RiskRules::new(create_test_config());

        // 资金不足
        let result = rules.check_capital(50.0, 100.0).await;
        assert!(result.is_err());

        let alert = result.unwrap_err();
        assert_eq!(alert.level, RiskLevel::High);
        assert!(alert.message.contains("资金不足"));
    }

    #[tokio::test]
    async fn test_position_limit_check() {
        let rules = RiskRules::new(create_test_config());

        // 未超限
        assert!(rules.check_position_limit(500.0, 100.0).await.is_ok());

        // 超限
        let result = rules.check_position_limit(950.0, 100.0).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_single_order_limit_check() {
        let rules = RiskRules::new(create_test_config());

        // 未超限
        assert!(rules.check_single_order_limit(50.0).await.is_ok());

        // 超限
        let result = rules.check_single_order_limit(150.0).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_order_frequency_check() {
        let rules = RiskRules::new(create_test_config());

        // 首次下单，无间隔限制
        assert!(rules.check_order_frequency().await.is_ok());

        // 记录订单
        rules.record_order(100.0).await;

        // 立即再次检查，应该失败
        let result = rules.check_order_frequency().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_daily_loss_check() {
        let rules = RiskRules::new(create_test_config());

        // 亏损未超限
        rules.update_loss(30.0).await;
        assert!(rules.check_daily_loss().await.is_ok());

        // 亏损超限
        rules.update_loss(30.0).await;
        let result = rules.check_daily_loss().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_pre_trade_check_all_pass() {
        let rules = RiskRules::new(create_test_config());

        let result = rules
            .pre_trade_check(
                50.0,  // 订单金额
                500.0, // 可用余额
                200.0, // 当前持仓
            )
            .await;

        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), RiskCheckResult::Pass));
    }

    #[tokio::test]
    async fn test_pre_trade_check_capital_fail() {
        let rules = RiskRules::new(create_test_config());

        // 资金不足
        let result = rules
            .pre_trade_check(
                500.0, // 订单金额（超过余额）
                100.0, // 可用余额
                0.0,   // 当前持仓
            )
            .await;

        assert!(result.is_err());
    }
}
