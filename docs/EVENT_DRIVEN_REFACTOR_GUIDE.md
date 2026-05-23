# Rust-Binance 事件驱动架构重构开发指南

## 文档信息

- **版本**: v1.0.0
- **创建时间**: 2026-02-23
- **目标**: 将函数调用驱动的系统重构为事件驱动架构
- **预计周期**: 8-10 周
- **难度**: ⭐⭐⭐⭐☆

---

## 目录

1. [项目概述](#一项目概述)
2. [架构设计](#二架构设计)
3. [技术栈](#三技术栈)
4. [开发环境准备](#四开发环境准备)
5. [开发路线图](#五开发路线图)
6. [模块开发详解](#六模块开发详解)
7. [测试策略](#七测试策略)
8. [迁移方案](#八迁移方案)
9. [性能优化](#九性能优化)
10. [常见问题](#十常见问题)

---

## 一、项目概述

### 1.1 当前系统问题

通过实盘日志分析发现：
- **实时性差**: 10 秒轮询间隔，无法及时响应市场变化
- **订单成功率低**: 仅 21.05%，大量订单因资金不足失败
- **资源浪费**: 轮询时空等 CPU 周期过多
- **扩展困难**: 新增策略需修改多处代码
- **风控滞后**: 无法实时监控和拦截风险

### 1.2 重构目标

| 指标 | 当前 | 目标 | 提升 |
|------|------|------|------|
| 响应延迟 | 10 秒 | <100 毫秒 | 100 倍 |
| 订单成功率 | 21% | >80% | 4 倍 |
| CPU 利用率 | 30-40% | 60-70% | 优化 |
| 策略数量 | 1-2 个 | 10+ 个 | 5 倍 |
| 风控实时性 | 事后 | 实时 | 质的飞跃 |

### 1.3 核心设计理念

#### 事件驱动架构
```
事件生产者 → 发布事件 → 事件总线 → 订阅者处理
     ↓                                      ↓
  市场变化                              策略决策
  订单成交                              风控检查
  账户变动                              执行交易
```

#### CQRS 模式
- **命令 (Command)**: 改变状态的操作 (如：下单、撤单)
- **查询 (Query)**: 读取状态的操作 (如：查余额、查订单)
- **优势**: 读写分离，提高性能和可扩展性

#### 事件溯源
- 保存所有状态变更事件
- 通过重放事件重建状态
- 完整的审计日志

---

## 二、架构设计

### 2.1 整体架构图

```
┌─────────────────────────────────────────────────────────┐
│                    应用层                                │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐     │
│  │ 网格策略    │  │ 趋势策略    │  │ 套利策略    │     │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘     │
└─────────┼────────────────┼────────────────┼────────────┘
          │                │                │
┌─────────▼────────────────▼────────────────▼────────────┐
│                  事件总线                               │
│  ┌──────────────────────────────────────────────────┐  │
│  │  Tokio Broadcast Channel + Event Dispatcher      │  │
│  └──────────────────────────────────────────────────┘  │
└─────────┬──────────────────────────────────────────────┘
          │
┌─────────▼──────────────────────────────────────────────┐
│                  领域服务层                             │
│  ┌───────────┐  ┌───────────┐  ┌───────────┐         │
│  │订单服务   │  │账户服务   │  │风控服务   │         │
│  └─────┬─────┘  └─────┬─────┘  └─────┬─────┘         │
└────────┼──────────────┼──────────────┼────────────────┘
         │              │              │
┌────────▼──────────────▼──────────────▼────────────────┐
│                  基础设施层                            │
│  ┌───────────┐  ┌───────────┐  ┌───────────┐        │
│  │Binance API│  │数据库     │  │日志监控   │        │
│  └───────────┘  └───────────┘  └───────────┘        │
└───────────────────────────────────────────────────────┘
```

### 2.2 目录结构

```
src/
├── events/                    # 【新建】事件定义
│   ├── mod.rs
│   ├── market_events.rs      # 市场数据事件
│   ├── trading_events.rs     # 交易事件
│   ├── account_events.rs     # 账户事件
│   ├── strategy_events.rs    # 策略事件
│   └── risk_events.rs        # 风控事件
│
├── commands/                  # 【新建】命令定义
│   ├── mod.rs
│   ├── order_commands.rs     # 订单命令
│   ├── strategy_commands.rs  # 策略命令
│   └── risk_commands.rs      # 风控命令
│
├── event_bus.rs              # 【新建】事件总线
├── command_bus.rs            # 【新建】命令总线
├── event_store.rs            # 【新建】事件存储
│
├── services/                  # 【新建】领域服务
│   ├── mod.rs
│   ├── market_data_service.rs
│   ├── order_execution_service.rs
│   ├── risk_monitor_service.rs
│   └── account_sync_service.rs
│
├── strategies/                # 【新建】策略引擎
│   ├── mod.rs
│   ├── grid_strategy_engine.rs
│   └── trend_strategy_engine.rs
│
├── processors/                # 【新建】事件处理器
│   ├── mod.rs
│   ├── order_processor.rs
│   ├── risk_processor.rs
│   └── strategy_processor.rs
│
├── binance/                   # 【保留】现有 Binance API
├── core/                      # 【逐步迁移】现有业务逻辑
├── infrastructure/            # 【增强】基础设施
│   ├── config/
│   ├── logging/
│   └── database/
│
└── main.rs                    # 【重构】应用入口
```

### 2.3 核心事件类型

#### 市场数据事件
```rust
PriceUpdateEvent       // 价格更新
KlineCompletedEvent    // K 线完成
OrderBookUpdateEvent   // 订单簿更新
```

#### 交易事件
```rust
OrderSubmittedEvent    // 订单提交
OrderFilledEvent       // 订单成交
OrderCancelledEvent    // 订单取消
OrderRejectedEvent     // 订单拒绝
```

#### 策略事件
```rust
TradingSignalEvent     // 交易信号
GridTriggerEvent       // 网格触发
GridStateChangeEvent   // 网格状态变更
```

#### 风控事件
```rust
RiskCheckEvent         // 风控检查
RiskAlertEvent         // 风控告警
```

#### 账户事件
```rust
BalanceUpdateEvent     // 余额更新
PositionChangeEvent    // 持仓变动
```

---

## 三、技术栈

### 3.1 核心依赖

在 `Cargo.toml` 中添加：

```toml
[dependencies]
# ============ 异步运行时 ============
tokio = { version = "1.35", features = ["full"] }
tokio-util = "0.7"
async-trait = "0.1"

# ============ 序列化 ============
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
rmp-serde = "1.1"  # MessagePack 二进制格式

# ============ 时间处理 ============
chrono = { version = "0.4", features = ["serde"] }

# ============ 日志和追踪 ============
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }

# ============ 数据库 ============
sqlx = { version = "0.7", features = ["runtime-tokio-rustls", "sqlite"] }

# ============ 工具库 ============
uuid = { version = "1.6", features = ["v4", "serde"] }
thiserror = "1.0"
anyhow = "1.0"
parking_lot = "0.12"  # 高性能锁

# ============ 监控指标 ============
prometheus = "0.13"

# ============ 通道 ============
async-channel = "2.1"
crossbeam-channel = "0.5"

# ============ 已有的依赖 (保留) ============
reqwest = { version = "0.11", features = ["json"] }
binance = "0.18"  # 如有使用
dotenv = "0.15"
```

### 3.2 开发工具

```bash
# 代码格式化
cargo install rustfmt

# Lint 检查
cargo install clippy

# 自动编译
cargo install cargo-watch

# 快速测试
cargo install cargo-nextest

# 测试覆盖率
cargo install cargo-llvm-cov
```

---

## 四、开发环境准备

### 4.1 系统要求

- **操作系统**: Linux / macOS / Windows (WSL2)
- **Rust 版本**: 1.75+
- **内存**: 至少 8GB
- **磁盘**: 至少 20GB 可用空间

### 4.2 安装 Rust

```bash
# 安装 rustup
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# 安装稳定版
rustup install stable
rustup default stable

# 验证
rustc --version
cargo --version
```

### 4.3 设置项目

```bash
# 进入项目目录
cd /Users/zcx/work/rust-binance

# 创建开发分支
git checkout -b feature/event-driven-refactor

# 更新依赖
cargo fetch

# 运行现有测试确保环境正常
cargo test --lib
```

### 4.4 配置开发环境

创建 `.env.development`:

```bash
# 日志级别
RUST_LOG=debug,hyper=info,tower=info
RUST_BACKTRACE=1

# 事件总线配置
EVENT_BUS_CAPACITY=10000

# 数据库配置
DATABASE_URL=sqlite://data/events.db

# 功能开关
ENABLE_EVENT_STORE=true
ENABLE_NEW_ARCH=false  # 初期设为 false，逐步切换
```

### 4.5 验证清单

- [ ] Rust 工具链安装成功
- [ ] 项目可以正常编译
- [ ] 现有测试全部通过
- [ ] Git 分支创建成功

---

## 五、开发路线图

### 阶段 1: 基础架构搭建 (第 1-2 周)

**目标**: 实现事件驱动的基础设施

#### 第 1 周：事件定义和事件总线

**任务 1.1**: 创建事件模块 (2 天)
- [ ] 创建 `src/events/mod.rs`
- [ ] 创建 `src/events/market_events.rs`
- [ ] 创建 `src/events/trading_events.rs`
- [ ] 创建 `src/events/account_events.rs`
- [ ] 创建 `src/events/strategy_events.rs`
- [ ] 创建 `src/events/risk_events.rs`
- [ ] 编写单元测试

**任务 1.2**: 实现事件总线 (3 天)
- [ ] 创建 `src/event_bus.rs`
- [ ] 实现 `EventBus` trait
- [ ] 实现基于 Tokio channel 的事件总线
- [ ] 实现事件分发器
- [ ] 实现订阅管理器
- [ ] 编写集成测试

**交付物**:
- 完整的事件类型定义
- 可运行的事件总线原型
- 测试覆盖率 > 80%

#### 第 2 周：命令总线和事件存储

**任务 2.1**: 实现命令总线 (2 天)
- [ ] 创建 `src/commands/mod.rs`
- [ ] 创建 `src/commands/order_commands.rs`
- [ ] 创建 `src/command_bus.rs`
- [ ] 实现 `Command` trait
- [ ] 实现 `CommandHandler` trait
- [ ] 实现命令分发器

**任务 2.2**: 实现事件存储 (3 天)
- [ ] 创建 `src/event_store.rs`
- [ ] 设计数据库 schema
- [ ] 实现事件保存接口
- [ ] 实现事件查询接口
- [ ] 实现快照机制
- [ ] 编写持久化测试

**交付物**:
- 完整的命令定义
- 可持久化的事件存储
- 测试覆盖率 > 80%

### 阶段 2: 核心服务实现 (第 3-5 周)

**目标**: 实现事件驱动的核心业务服务

#### 第 3 周：市场数据服务

**任务 3.1**: 市场数据服务 (3 天)
- [ ] 创建 `src/services/market_data_service.rs`
- [ ] 实现 WebSocket 连接管理
- [ ] 实现价格事件发布
- [ ] 实现订单簿事件发布
- [ ] 添加技术指标计算

**任务 3.2**: 集成测试 (2 天)
- [ ] 模拟 Binance WebSocket
- [ ] 测试价格更新流程
- [ ] 测试订单簿更新流程
- [ ] 性能基准测试

**交付物**:
- 可运行的市场数据服务
- 实时价格推送功能
- 测试覆盖率 > 85%

#### 第 4 周：订单执行服务

**任务 4.1**: 订单执行服务 (3 天)
- [ ] 创建 `src/services/order_execution_service.rs`
- [ ] 实现订单命令处理器
- [ ] 集成现有订单管理器
- [ ] 实现订单状态跟踪
- [ ] 发布订单生命周期事件

**任务 4.2**: 订单处理器 (2 天)
- [ ] 创建 `src/processors/order_processor.rs`
- [ ] 实现订单事件处理逻辑
- [ ] 更新本地订单缓存
- [ ] 通知账户同步服务

**交付物**:
- 完整的订单执行流程
- 事件驱动的订单管理
- 测试覆盖率 > 85%

#### 第 5 周：风控监控服务

**任务 5.1**: 风控服务 (3 天)
- [ ] 创建 `src/services/risk_monitor_service.rs`
- [ ] 实现实时风险监控
- [ ] 集成现有风控规则
- [ ] 发布风控告警事件
- [ ] 实现风控拦截机制

**任务 5.2**: 风控处理器 (2 天)
- [ ] 创建 `src/processors/risk_processor.rs`
- [ ] 实现风控事件处理
- [ ] 添加风控规则引擎
- [ ] 实现动态参数调整

**交付物**:
- 实时风险监控体系
- 自动风控拦截功能
- 测试覆盖率 > 90%

### 阶段 3: 策略引擎实现 (第 6-7 周)

**目标**: 实现事件驱动的策略引擎

#### 第 6 周：网格策略引擎

**任务 6.1**: 策略基类 (2 天)
- [ ] 创建 `src/strategies/base_strategy.rs`
- [ ] 定义策略接口
- [ ] 实现策略状态管理
- [ ] 实现策略生命周期

**任务 6.2**: 网格策略 (3 天)
- [ ] 创建 `src/strategies/grid_strategy_engine.rs`
- [ ] 迁移现有网格逻辑
- [ ] 实现事件驱动的网格触发
- [ ] 添加动态网格参数调整
- [ ] 集成趋势过滤

**交付物**:
- 可运行的网格策略引擎
- 支持多交易对同时运行
- 测试覆盖率 > 85%

#### 第 7 周：策略管理和工具

**任务 7.1**: 策略管理器 (2 天)
- [ ] 创建策略注册表
- [ ] 实现策略热加载
- [ ] 添加策略监控
- [ ] 实现策略统计

**任务 7.2**: 策略处理器 (3 天)
- [ ] 创建 `src/processors/strategy_processor.rs`
- [ ] 实现策略信号处理
- [ ] 添加信号过滤机制
- [ ] 实现信号强度计算

**交付物**:
- 完整的策略管理框架
- 支持动态添加/删除策略
- 测试覆盖率 > 85%

### 阶段 4: 辅助服务完善 (第 8 周)

**目标**: 完善辅助服务和工具

#### 第 8 周：账户同步和其他服务

**任务 8.1**: 账户同步服务 (2 天)
- [ ] 创建 `src/services/account_sync_service.rs`
- [ ] 实现定时同步任务
- [ ] 发布账户变动事件
- [ ] 添加余额预警

**任务 8.2**: 日志和监控 (2 天)
- [ ] 实现结构化日志
- [ ] 集成 Prometheus
- [ ] 添加关键指标采集
- [ ] 配置 Grafana 仪表盘

**任务 8.3**: 配置管理 (1 天)
- [ ] 实现热重载配置
- [ ] 添加环境变量支持
- [ ] 实现配置验证

**交付物**:
- 完整的账户同步机制
- 完善的监控体系
- 灵活的配置管理

### 阶段 5: 集成测试和优化 (第 9-10 周)

**目标**: 全面测试和性能优化

#### 第 9 周：集成测试

**任务 9.1**: 端到端测试 (3 天)
- [ ] 完整交易流程测试
- [ ] 异常场景测试
- [ ] 故障恢复测试
- [ ] 边界条件测试

**任务 9.2**: 压力测试 (2 天)
- [ ] 高并发测试
- [ ] 大数据量测试
- [ ] 长时间运行测试
- [ ] 资源泄漏检测

**交付物**:
- 完整的测试套件
- 性能基准报告
- 问题修复清单

#### 第 10 周：性能优化和文档

**任务 10.1**: 性能优化 (2 天)
- [ ] 瓶颈分析
- [ ] 内存优化
- [ ] 并发优化
- [ ] 序列化优化

**任务 10.2**: 文档完善 (3 天)
- [ ] API 文档
- [ ] 架构文档
- [ ] 运维手册
- [ ] 开发者指南

**交付物**:
- 性能优化报告
- 完整的文档体系
- 上线检查清单

---

## 六、模块开发详解

### 6.1 事件模块开发

#### 步骤 1: 创建基础事件结构

文件：`src/events/mod.rs`

```rust
//! 事件定义模块

mod market_events;
mod trading_events;
mod account_events;
mod strategy_events;
mod risk_events;

pub use market_events::*;
pub use trading_events::*;
pub use account_events::*;
pub use strategy_events::*;
pub use risk_events::*;

use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use uuid::Uuid;

/// 事件元数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventMetadata {
    pub event_id: String,
    pub timestamp: DateTime<Utc>,
    pub version: String,
    pub correlation_id: Option<String>,
    pub causation_id: Option<String>,
}

impl Default for EventMetadata {
    fn default() -> Self {
        Self {
            event_id: Uuid::new_v4().to_string(),
            timestamp: Utc::now(),
            version: "1.0".to_string(),
            correlation_id: None,
            causation_id: None,
        }
    }
}

/// 统一事件枚举
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event_type", content = "payload")]
pub enum DomainEvent {
    // 市场数据
    PriceUpdate(PriceUpdateEvent),
    KlineCompleted(KlineCompletedEvent),
    
    // 交易
    OrderSubmitted(OrderSubmittedEvent),
    OrderFilled(OrderFilledEvent),
    OrderCancelled(OrderCancelledEvent),
    
    // 账户
    BalanceUpdate(BalanceUpdateEvent),
    
    // 策略
    TradingSignal(TradingSignalEvent),
    GridTrigger(GridTriggerEvent),
    
    // 风控
    RiskCheck(RiskCheckEvent),
    RiskAlert(RiskAlertEvent),
}

/// 可存储的事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorableEvent {
    pub sequence: u64,
    pub stream_id: String,
    pub event: DomainEvent,
    pub metadata: EventMetadata,
}
```

#### 步骤 2: 实现具体事件类型

文件：`src/events/market_events.rs`

```rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceUpdateEvent {
    pub symbol: String,
    pub price: f64,
    pub price_change_pct_24h: f64,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KlineCompletedEvent {
    pub symbol: String,
    pub interval: String,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub close_time: u64,
}
```

#### 步骤 3: 编写测试

```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_price_event_serialization() {
        let event = PriceUpdateEvent {
            symbol: "BTCUSDT".to_string(),
            price: 50000.0,
            price_change_pct_24h: 2.5,
            timestamp: 1234567890,
        };
        
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("BTCUSDT"));
    }
}
```

### 6.2 事件总线开发

#### 步骤 1: 定义 Event Bus Trait

文件：`src/event_bus.rs`

```rust
use async_trait::async_trait;
use std::sync::Arc;

#[async_trait]
pub trait EventBus: Send + Sync {
    async fn publish(&self, event: DomainEvent) -> Result<(), EventError>;
    fn subscribe<T: EventHandler>(&self, handler: Arc<T>) -> SubscriptionId;
    fn unsubscribe(&self, subscription_id: SubscriptionId);
}

#[async_trait]
pub trait EventHandler: Send + Sync {
    async fn handle(&self, event: &DomainEvent) -> Result<(), EventError>;
    fn event_types(&self) -> Vec<EventType>;
}

#[derive(Debug)]
pub enum EventError {
    PublishFailed(String),
    SubscribeFailed(String),
}
```

#### 步骤 2: 实现 Tokio Event Bus

```rust
use tokio::sync::broadcast;
use std::sync::Arc;
use parking_lot::RwLock;
use std::collections::HashMap;

pub struct TokioEventBus {
    sender: broadcast::Sender<DomainEvent>,
    handlers: Arc<RwLock<HashMap<SubscriptionId, Arc<dyn EventHandler>>>>,
    next_subscription_id: AtomicU64,
}

impl TokioEventBus {
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self {
            sender,
            handlers: Arc::new(RwLock::new(HashMap::new())),
            next_subscription_id: AtomicU64::new(1),
        }
    }
}

#[async_trait]
impl EventBus for TokioEventBus {
    async fn publish(&self, event: DomainEvent) -> Result<(), EventError> {
        self.sender.send(event)
            .map_err(|e| EventError::PublishFailed(e.to_string()))?;
        Ok(())
    }
    
    fn subscribe<T: EventHandler>(&self, handler: Arc<T>) -> SubscriptionId {
        let id = self.next_subscription_id.fetch_add(1, Ordering::SeqCst);
        self.handlers.write().insert(id, handler);
        id
    }
    
    fn unsubscribe(&self, subscription_id: SubscriptionId) {
        self.handlers.write().remove(&subscription_id);
    }
}
```

#### 步骤 3: 实现事件分发器

```rust
pub struct EventDispatcher {
    event_bus: Arc<TokioEventBus>,
}

impl EventDispatcher {
    pub fn new(event_bus: Arc<TokioEventBus>) -> Self {
        Self { event_bus }
    }
    
    pub async fn run(self) {
        let mut receiver = self.event_bus.sender.subscribe();
        
        while let Ok(event) = receiver.recv().await {
            let handlers = self.event_bus.handlers.read();
            
            // 并行处理所有 handler
            let futures: Vec<_> = handlers.values()
                .map(|handler| handler.handle(&event))
                .collect();
            
            // 等待所有 handler 完成
            let results = futures::future::join_all(futures).await;
            
            // 处理错误
            for result in results {
                if let Err(e) = result {
                    tracing::error!("事件处理失败：{:?}", e);
                }
            }
        }
    }
}
```

### 6.3 市场数据服务开发

#### 步骤 1: 创建服务结构

文件：`src/services/market_data_service.rs`

```rust
use crate::event_bus::EventBus;
use std::sync::Arc;

pub struct MarketDataService {
    event_bus: Arc<dyn EventBus>,
    symbols: Vec<String>,
}

impl MarketDataService {
    pub fn new(event_bus: Arc<dyn EventBus>, symbols: Vec<String>) -> Self {
        Self { event_bus, symbols }
    }
    
    pub async fn start(&self) -> Result<(), ServiceError> {
        // 连接 Binance WebSocket
        // 订阅价格更新
        // 发布 PriceUpdateEvent
        
        Ok(())
    }
}
```

#### 步骤 2: 实现 WebSocket 连接

```rust
use tokio_tungstenite::{connect_async, tungstenite::Message};
use futures_util::{StreamExt, SinkExt};

impl MarketDataService {
    async fn connect_websocket(&self, stream_url: &str) -> Result<(), ServiceError> {
        let (ws_stream, _) = connect_async(stream_url).await?;
        let (_, mut read) = ws_stream.split();
        
        while let Some(msg) = read.next().await {
            match msg? {
                Message::Text(text) => {
                    // 解析价格数据
                    let price_data = self.parse_price_data(&text)?;
                    
                    // 发布事件
                    let event = DomainEvent::PriceUpdate(price_data);
                    self.event_bus.publish(event).await?;
                }
                _ => {}
            }
        }
        
        Ok(())
    }
}
```

### 6.4 网格策略引擎开发

#### 步骤 1: 定义策略接口

文件：`src/strategies/base_strategy.rs`

```rust
use async_trait::async_trait;
use crate::events::{DomainEvent, TradingSignalEvent};

#[async_trait]
pub trait Strategy: Send + Sync {
    fn name(&self) -> &str;
    fn symbols(&self) -> Vec<String>;
    
    async fn on_event(&self, event: &DomainEvent) -> Result<Option<TradingSignalEvent>, StrategyError>;
    
    fn get_state(&self) -> StrategyState;
    async fn update_state(&self, event: &DomainEvent);
}

pub struct StrategyState {
    pub is_active: bool,
    pub last_update: u64,
    pub metrics: HashMap<String, f64>,
}
```

#### 步骤 2: 实现网格策略

文件：`src/strategies/grid_strategy_engine.rs`

```rust
pub struct GridStrategyEngine {
    strategy_id: String,
    symbols: Vec<String>,
    grid_config: GridConfig,
    state: RwLock<GridState>,
    event_bus: Arc<dyn EventBus>,
}

impl GridStrategyEngine {
    pub fn new(
        strategy_id: String,
        symbols: Vec<String>,
        grid_config: GridConfig,
        event_bus: Arc<dyn EventBus>,
    ) -> Self {
        Self {
            strategy_id,
            symbols,
            grid_config,
            state: RwLock::new(GridState::default()),
            event_bus,
        }
    }
    
    async fn check_grid_trigger(&self, price_event: &PriceUpdateEvent) -> Option<GridTriggerEvent> {
        let state = self.state.read();
        let current_price = price_event.price;
        
        // 检查是否触及网格线
        for (level, grid_price) in state.grid_levels.iter().enumerate() {
            let threshold = grid_price * 0.001; // 0.1% 容差
            
            if (current_price - grid_price).abs() < threshold {
                return Some(GridTriggerEvent {
                    grid_id: format!("grid_{}", level),
                    strategy_id: self.strategy_id.clone(),
                    symbol: price_event.symbol.clone(),
                    grid_level: level as i32,
                    trigger_price: current_price,
                    action: if current_price < state.base_price {
                        GridAction::Buy
                    } else {
                        GridAction::Sell
                    },
                    quantity: self.calculate_quantity(level, current_price),
                    expected_profit: self.calculate_expected_profit(level, current_price),
                });
            }
        }
        
        None
    }
}

#[async_trait]
impl Strategy for GridStrategyEngine {
    fn name(&self) -> &str {
        "GridStrategy"
    }
    
    fn symbols(&self) -> Vec<String> {
        self.symbols.clone()
    }
    
    async fn on_event(&self, event: &DomainEvent) -> Result<Option<TradingSignalEvent>, StrategyError> {
        if let DomainEvent::PriceUpdate(price_event) = event {
            if let Some(grid_trigger) = self.check_grid_trigger(price_event).await {
                // 发布网格触发事件
                self.event_bus.publish(DomainEvent::GridTrigger(grid_trigger)).await?;
            }
        }
        
        Ok(None)
    }
    
    fn get_state(&self) -> StrategyState {
        let state = self.state.read();
        StrategyState {
            is_active: true,
            last_update: state.last_update_time,
            metrics: state.get_metrics(),
        }
    }
    
    async fn update_state(&self, event: &DomainEvent) {
        // 根据事件更新网格状态
    }
}
```

---

## 七、测试策略

### 7.1 测试金字塔

```
           /\
          /  \
         / E2E \        端到端测试 (10%)
        /______\
       /        \
      / Integration\    集成测试 (30%)
     /______________\
    /                \
   /    Unit Tests    \  单元测试 (60%)
  /____________________\
```

### 7.2 单元测试

每个模块都要有完整的单元测试：

```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_basic_functionality() {
        // 测试基本功能
    }
    
    #[test]
    fn test_edge_cases() {
        // 测试边界情况
    }
    
    #[test]
    fn test_error_handling() {
        // 测试错误处理
    }
}
```

### 7.3 集成测试

测试模块间交互：

```rust
// tests/integration_test.rs

#[tokio::test]
async fn test_price_to_order_flow() {
    // 1. 发布价格更新
    // 2. 策略生成信号
    // 3. 风控检查通过
    // 4. 订单执行
    // 5. 验证结果
}
```

### 7.4 压力测试

```rust
#[tokio::test]
async fn test_high_concurrency() {
    // 同时发布大量事件
    // 验证系统处理能力
    // 检查资源使用情况
}
```

### 7.5 测试检查清单

- [ ] 所有公共函数都有单元测试
- [ ] 关键路径有集成测试
- [ ] 错误场景有覆盖
- [ ] 性能基准测试完成
- [ ] 测试覆盖率 > 85%

---

## 八、迁移方案

### 8.1 渐进式迁移策略

#### 阶段 1: 双轨运行 (第 1-4 周)
- 保留现有系统
- 新系统并行开发
- 通过适配器兼容

#### 阶段 2: 逐步切换 (第 5-8 周)
- 先将市场数据切换到新系统
- 然后切换订单管理
- 最后切换策略引擎

#### 阶段 3: 完全替换 (第 9-10 周)
- 关闭旧系统
- 新系统独立运行
- 保留回滚能力

### 8.2 兼容性保证

```rust
// 使用装饰器模式保持兼容
pub struct LegacyAdapter {
    legacy_system: LegacySystem,
}

impl EventHandler for LegacyAdapter {
    async fn handle(&self, event: &DomainEvent) -> Result<(), EventError> {
        // 将事件转换为旧系统格式
        // 调用旧系统接口
        Ok(())
    }
}
```

### 8.3 回滚方案

1. **功能开关**: 可通过配置快速切换
2. **数据备份**: 每日备份关键数据
3. **监控告警**: 异常时自动告警
4. **快速回滚**: 5 分钟内可回退到旧版本

---

## 九、性能优化

### 9.1 优化方向

#### 内存优化
- 使用对象池复用事件对象
- 避免不必要的克隆
- 使用 Arc 共享数据

#### 并发优化
- 使用无锁数据结构
- 细粒度锁分区
- 异步 IO 操作

#### 序列化优化
- 使用二进制格式 (MessagePack)
- 减少嵌套层次
- 压缩大字段

### 9.2 性能指标

| 指标 | 目标值 | 测量方法 |
|------|--------|----------|
| 事件延迟 | <10ms | P99 延迟 |
| 吞吐量 | >10000 TPS | 每秒处理事件数 |
| 内存占用 | <500MB | RSS 内存 |
| CPU 使用 | <70% | 平均使用率 |

---

## 十、常见问题

### Q1: 如何保证事件不丢失？

**A**: 使用事件持久化 + 确认机制
```rust
async fn publish_with_ack(&self, event: DomainEvent) -> Result<(), EventError> {
    // 1. 先保存到事件存储
    self.event_store.save(&event).await?;
    
    // 2. 发布到事件总线
    self.event_bus.publish(event).await?;
    
    // 3. 等待消费者确认
    // ...
    
    Ok(())
}
```

### Q2: 如何处理事件乱序？

**A**: 使用时间戳 + 序列号
```rust
pub struct OrderedEvent {
    pub sequence: u64,
    pub timestamp: u64,
    pub event: DomainEvent,
}

// 消费者按 sequence 排序处理
```

### Q3: 如何调试事件流？

**A**: 使用分布式追踪
```rust
use tracing::{span, Level};

let span = span!(Level::INFO, "order_flow", order_id = %order.order_id);
let _enter = span.enter();

// 所有相关日志都会包含 trace ID
```

### Q4: 如何测试事件驱动系统？

**A**: 使用虚拟时间 + 模拟器
```rust
#[tokio::test]
async fn test_grid_strategy() {
    let (event_bus, mock) = MockEventBus::create();
    let strategy = GridStrategy::new(event_bus);
    
    // 发送模拟价格事件
    mock.send(PriceUpdateEvent { ... }).await;
    
    // 验证策略反应
    assert_eq!(mock.receive().await, ExpectedSignal::Buy);
}
```

### Q5: 性能瓶颈如何排查？

**A**: 使用性能分析工具
```bash
# CPU 分析
cargo install flamegraph
cargo flamegraph --example your_example

# 内存分析
cargo install heaptrack
heaptrack target/debug/your_binary

# 阻塞分析
tokio-console
```

---

## 附录

### A. 参考资源

- [Tokio 官方文档](https://tokio.rs/)
- [事件驱动架构模式](https://www.enterpriseintegrationpatterns.com/patterns/messaging/toc.html)
- [CQRS 模式](https://martinfowler.com/bliki/CQRS.html)
- [事件溯源](https://martinfowler.com/eaaDev/EventSourcing.html)

### B. 检查清单

#### 开发阶段检查
- [ ] 每日代码审查
- [ ] 每周进度同步
- [ ] 阶段性演示

#### 上线前检查
- [ ] 所有测试通过
- [ ] 性能达标
- [ ] 文档完整
- [ ] 监控就绪
- [ ] 回滚方案测试

#### 运维检查
- [ ] 日志采集正常
- [ ] 指标监控正常
- [ ] 告警配置正确
- [ ] 备份策略生效

### C. 关键联系人

- 架构师：[待填写]
- 技术负责人：[待填写]
- 运维负责人：[待填写]

---

## 更新日志

### v1.0.0 (2026-02-23)
- 初始版本
- 完整的开发路线图
- 详细的模块开发指南
- 测试和优化策略

---

**文档结束**

祝你开发顺利！如有任何问题，请随时查阅本文档或寻求团队帮助。
