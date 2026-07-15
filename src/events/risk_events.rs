//! 风控事件
//!
//! 包含风控检查、风控告警等事件

use serde::{Deserialize, Serialize};

/// 风控检查事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskCheckEvent {
    /// 检查 ID
    pub check_id: String,
    /// 检查类型
    pub check_type: RiskCheckType,
    /// 检查结果：PASS/FAIL
    pub result: RiskCheckResult,
    /// 风险等级：LOW/MEDIUM/HIGH/CRITICAL
    pub risk_level: RiskLevel,
    /// 风险分数：0.0-1.0
    pub risk_score: f64,
    /// 详细信息
    pub details: String,
    /// 时间戳（毫秒）
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RiskCheckType {
    /// 资金充足性检查
    CapitalAdequacy,
    /// 持仓限额检查
    PositionLimit,
    /// 止损检查
    StopLoss,
    /// 市场波动检查
    MarketVolatility,
    /// 流动性检查
    Liquidity,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RiskCheckResult {
    Pass,
    Fail(String),
    Warning(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum RiskLevel {
    Low,
    Medium,
    High,
    Critical,
}

/// 风控告警事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskAlertEvent {
    /// 告警 ID
    pub alert_id: String,
    /// 告警级别
    pub level: RiskLevel,
    /// 告警类型
    pub alert_type: String,
    /// 告警消息
    pub message: String,
    /// 触发的规则
    pub triggered_rule: String,
    /// 建议操作
    pub suggested_action: Option<String>,
    /// 时间戳（毫秒）
    pub timestamp: u64,
}

impl RiskAlertEvent {
    pub fn new(
        level: RiskLevel,
        alert_type: String,
        message: String,
        triggered_rule: String,
    ) -> Self {
        use uuid::Uuid;

        Self {
            alert_id: Uuid::new_v4().to_string(),
            level,
            alert_type,
            message,
            triggered_rule,
            suggested_action: None,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        }
    }

    pub fn with_suggested_action(mut self, action: String) -> Self {
        self.suggested_action = Some(action);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_risk_alert_creation() {
        let event = RiskAlertEvent::new(
            RiskLevel::High,
            "POSITION_LIMIT".to_string(),
            "持仓超过限制".to_string(),
            "max_position_btc <= 1.0".to_string(),
        );

        assert_eq!(event.level, RiskLevel::High);
        assert_eq!(event.alert_type, "POSITION_LIMIT");
        matches!(event.level, RiskLevel::High);
    }

    #[test]
    fn test_risk_check_serialization() {
        let event = RiskCheckEvent {
            check_id: "check_001".to_string(),
            check_type: RiskCheckType::CapitalAdequacy,
            result: RiskCheckResult::Pass,
            risk_level: RiskLevel::Low,
            risk_score: 0.2,
            details: "资金充足".to_string(),
            timestamp: 1234567890000,
        };

        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("CapitalAdequacy"));
        assert!(json.contains("资金充足"));
    }
}
