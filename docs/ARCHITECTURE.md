# 系统架构全景图

## 目录
1. [整体架构](#整体架构)
2. [分层架构详解](#分层架构详解)
3. [数据流时序图](#数据流时序图)
4. [核心设计模式](#核心设计模式)
5. [组件关系图](#组件关系图)
6. [模块职责对照表](#模块职责对照表)

---

## 整体架构

```
+-----------------------------------------------------------------------------+
|                              外部系统层                                       |
|  +--------------+  +--------------+  +--------------+                        |
|  |   Binance    |  |   SSH隧道     |  |   配置文件    |                        |
|  |  交易所API    |  |  (开发环境)   |  |  环境变量     |                        |
|  |              |  |  localhost:9443|  |  USE_SSH_TUNNEL|                       |
|  +------+-------+  +------+-------+  +--------------+                        |
+--------+---------+---------+------------------------------------------------+
         |                 |
         |  WebSocket连接   |
         v                 v
+-----------------------------------------------------------------------------+
|                            基础设施层 (Infrastructure)                        |
|                                                                              |
|  +--------------+  +--------------+  +--------------+  +--------------+     |
|  |  TCP连接     |  |  TLS加密     |  |  WebSocket   |  |  异步运行时   |     |
|  |  tokio::net  |  |  tokio-rustls|  |  tokio-      |  |  tokio       |     |
|  |  TcpStream   |  |              |  |  tungstenite |  |              |     |
|  +--------------+  +--------------+  +--------------+  +--------------+     |
+-----------------------------------------------------------------------------+
                                      |
                                      v
+-----------------------------------------------------------------------------+
|                            应用核心层                                         |
|                                                                              |
|  +-------------------------------------------------------------------------+ |
|  |                        事件总线 (Event Bus)                              | |
|  |                                                                         | |
|  |  +-----------------+        +-----------------+                        | |
|  |  |  TokioEventBus  |<------>| broadcast::     |                        | |
|  |  |                 |        | Channel(100)    |                        | |
|  |  |  publish()      |------->| 事件广播频道     |                        | |
|  |  |  subscribe()    |        +-----------------+                        | |
|  |  |  unsubscribe()  |                                                 | |
|  |  |                 |        +-----------------+                        | |
|  |  |  handlers:      |        |  HashMap<ID,    |                        | |
|  |  |  Arc<RwLock<    |        |  Arc<dyn        |                        | |
|  |  |  HashMap<...>>> |        |  EventHandler>> |                        | |
|  |  +-----------------+        +-----------------+                        | |
|  |                                                                         | |
|  |  +-----------------------------------------------------------------+   | |
|  |  |                    EventDispatcher (分发器)                     |   | |
|  |  |                                                                 |   | |
|  |  |  while let Ok(event) = receiver.recv().await {                  |   | |
|  |  |      let handlers = event_bus.handlers.read();                  |   | |
|  |  |      join_all(handlers.handle(event)).await;                    |   | |
|  |  |  }                                                              |   | |
|  |  +-----------------------------------------------------------------+   | |
|  +-------------------------------------------------------------------------+ |
|                                      ^                                      |
|                                      |                                      |
|                    +-----------------+---------------+                    |
|                    |                 |               |                    |
|                    | publish(event)  |               | subscribe(handler) |
|                    |                 |               |                    |
|                    v                 v               v                    |
|  +---------------------+  +-----------------+  +-----------------+         |
|  |    服务层 (Services) |  |  策略层          |  |  处理器层        |         |
|  |                     |  |  (Strategies)    |  |  (Handlers)     |         |
|  | +-----------------+ |  |                 |  |                 |         |
|  | | MarketDataService| |  | +-----------+  |  | +-----------+  |         |
|  | |                 | |  | |GridStrategy|  |  | |OrderHandler|  |         |
|  | | 连接Binance     | |  | |           |  |  | |           |  |         |
|  | | 解析JSON        |-+--+>| 接收价格   |  |  | | 接收订单   |  |         |
|  | | 构建事件        | |  | | 检查网格   |  |  | | 事件      |  |         |
|  | | 调用publish()   | |  | | 生成信号   |-+--+>| 记录日志   |  |         |
|  | +-----------------+ |  | +-----------+  |  | +-----------+  |         |
|  +---------------------+  +-----------------+  +-----------------+         |
|                                                                              |
|  +-------------------------------------------------------------------------+ |
|  |                        命令总线 (Command Bus)                            | |
|  |                                                                         | |
|  |  Command::PlaceOrder ---> OrderCommandHandler.handle() ---> Binance API  | |
|  |                                                                         | |
|  +-------------------------------------------------------------------------+ |
+-----------------------------------------------------------------------------+
                                      |
                                      v
+-----------------------------------------------------------------------------+
|                              错误处理层                                       |
|  +-------------------------------------------------------------------------+ |
|  |  DomainError                                                            | |
|  |  +--- InfrastructureError (IO、配置)                                      | |
|  |  +--- EventBusError (发布失败)                                            | |
|  |  +--- CommandBusError (执行失败)                                          | |
|  |  +--- ServiceError (市场数据错误)                                         | |
|  +-------------------------------------------------------------------------+ |
+-----------------------------------------------------------------------------+
```

---

## 分层架构详解

### Layer 4: 入口层 (Presentation)

**文件**: `main.rs`

**职责**:
- 组装所有组件
- 启动系统
- 生命周期管理

**核心代码**:
```rust
async fn main() {
    // 1. 创建事件总线
    let event_bus = Arc::new(TokioEventBus::new(100));
    
    // 2. 启动分发器
    let dispatcher = EventDispatcher::with_ready_notify(event_bus.clone(), ...);
    tokio::spawn(dispatcher.run());
    
    // 3. 注册策略
    let grid_strategy = Arc::new(GridStrategy::new(config, event_bus.clone()));
    event_bus.subscribe(grid_strategy);
    
    // 4. 启动市场数据服务
    let market_data_service = MarketDataService::new(event_bus.clone(), symbols);
    tokio::spawn(market_data_service.start());
}
```

---

### Layer 3: 应用层 (Application)

#### 3.1 事件总线 (Event Bus)

**文件**: `event_bus.rs`

**核心组件**:

| 组件 | 类型 | 职责 |
|------|------|------|
| TokioEventBus | struct | 存储处理器列表，提供publish/subscribe接口 |
| EventDispatcher | struct | 从broadcast频道接收事件，分发给所有处理器 |
| EventBus | trait | 接口定义：publish, subscribe, unsubscribe |
| EventHandler | trait | 处理器接口：handle, event_types |

**关键数据结构**:
```rust
pub struct TokioEventBus {
    sender: broadcast::Sender<DomainEvent>,           // 广播发送端
    _receiver: broadcast::Receiver<DomainEvent>,     // 保持频道存活
    handlers: Arc<RwLock<HashMap<SubscriptionId, Arc<dyn EventHandler>>>>,
    next_subscription_id: RwLock<SubscriptionId>,
}
```

#### 3.2 命令总线 (Command Bus)

**文件**: `command_bus.rs`

**职责**: 接收命令，路由到对应的处理器执行

```rust
pub struct CommandBus {
    order_handler: OrderCommandHandler,
}

impl CommandBus {
    pub async fn send(&self, command: Command) -> CommandResult {
        match command {
            Command::PlaceOrder(cmd) => self.order_handler.handle(cmd).await,
        }
    }
}
```

---

### Layer 2: 领域层 (Domain)

#### 2.1 服务层 (Services)

**文件**: `services/market_data_service.rs`

**MarketDataService**:
- 连接Binance WebSocket
- 解析JSON数据
- 构建PriceUpdateEvent
- 调用event_bus.publish()

**连接流程**:
```
main.rs: tokio::spawn(market_data_service.start())
    |
    v
MarketDataService::start()
    |
    +---> loop {
    |       connect_and_run()
    |       |
    |       +---> TCP连接 (localhost:9443 或 stream.binance.com:9443)
    |       +---> TLS握手 (证书验证)
    |       +---> WebSocket握手 (HTTP Upgrade)
    |       +---> 接收数据帧
    |       +---> 解析JSON
    |       +---> 构建PriceUpdateEvent
    |       +---> event_bus.publish(event)
    |       |
    |       +---> 如果断开: 等待5秒重连
    |   }
```

#### 2.2 策略层 (Strategies)

**文件**: `strategies/grid_strategy.rs`

**GridStrategy**:
- 接收PriceUpdateEvent
- 检查价格是否穿越网格线
- 生成TradingSignalEvent

**网格数学**:
```
区间: [lower_price, upper_price] = [80000, 100000]
网格数: grid_count = 10
间距: spacing = (100000 - 80000) / 10 = 2000

网格线:
  Level 0: 80000
  Level 1: 82000
  Level 2: 84000
  ...
  Level 10: 100000
```

**穿越检测逻辑**:
```rust
// 价格从上方穿越网格线 -> 买入信号
if last_price > line.price && current_price <= line.price && !line.filled {
    line.filled = true;
    generate_buy_signal();
}
```

#### 2.3 处理器层 (Handlers)

**文件**: `handlers/order_handler.rs`

**OrderHandler**: 监听订单事件，记录日志
**OrderCommandHandler**: 执行下单命令

---

### Layer 1: 基础设施层 (Infrastructure)

| 组件 | 库 | 用途 |
|------|-----|------|
| TCP连接 | `tokio::net::TcpStream` | 建立网络连接 |
| TLS加密 | `tokio-rustls`, `rustls` | 加密通信 |
| WebSocket | `tokio-tungstenite` | WebSocket协议 |
| 异步运行时 | `tokio` | 任务调度 |
| 日志 | `log`, `env_logger` | 日志记录 |

---

## 数据流时序图

### 场景: 价格更新触发网格交易

```
时间线 --------------------------------------------------------------------->

T0: main.rs 初始化
    |
    +-- 创建 TokioEventBus::new(100)
    |       |
    |       +-- broadcast::channel(100) --> 创建广播频道
    |
    +-- 创建 EventDispatcher
    |       |
    |       +-- dispatcher.subscribe() --> 订阅广播频道
    |
    +-- tokio::spawn(dispatcher.run()) --> 后台启动分发器
    |
    +-- dispatcher 标记 is_ready = true
    |       |
    |       +-- notify.notify_one() --> 通知main线程
    |
    +-- ready_notify.notified().await <--- 等待就绪通知
    |       |
    |       +-- [阻塞] 直到收到通知
    |
    +-- println!("事件分发器已就绪")

T1: 注册策略
    |
    +-- GridConfig::new("grid_btc_1", "BTCUSDT", 80000.0, 100000.0, 10, 0.001)
    |       |
    |       +-- 网格线: [80000, 82000, 84000, ..., 100000]
    |
    +-- GridStrategy::new(config, event_bus.clone())
    |       |
    |       +-- 内部状态: GridState { grid_lines, last_price: None }
    |
    +-- event_bus.subscribe(grid_strategy)
            |
            +-- handlers.insert(1, grid_strategy)
                |
                +-- 订阅ID = 1

T2: 启动市场数据服务
    |
    +-- MarketDataService::new(event_bus, ["BTCUSDT"])
    |       |
    |       +-- ws_url = "wss://localhost:9443/ws/btcusdt@ticker"
    |
    +-- tokio::spawn(market_data_service.start())
            |
            +-- 后台线程启动

T3: 连接Binance WebSocket
    |
    +-- MarketDataService.start()
    |       |
    |       +-- loop { connect_and_run() }
    |               |
    |               +-- TCP连接: localhost:9443
    |               +-- TLS握手: 验证stream.binance.com证书
    |               +-- WebSocket握手: HTTP Upgrade
    |               +-- 连接成功
    |
    +-- 等待Binance推送数据

T4: 收到价格数据 (每1秒)
    |
    +-- Binance 推送: {"e":"24hrTicker","s":"BTCUSDT","c":"85000.00",...}
    |
    +-- MarketDataService 解析JSON
    |       |
    |       +-- PriceUpdateEvent {
    |               symbol: "BTCUSDT",
    |               price: 85000.0,
    |               price_change_pct_24h: 2.5,
    |               timestamp: 1774365484026
    |           }
    |
    +-- event_bus.publish(PriceUpdateEvent)
            |
            +-- sender.send(event) --> 放入广播频道
                |
                +-- 通知所有接收者

T5: EventDispatcher 分发事件
    |
    +-- receiver.recv().await <--- 收到 PriceUpdateEvent
    |
    +-- handlers.read() --> 获取所有处理器
    |       |
    |       +-- [GridStrategy#1]
    |
    +-- handlers.iter().map(|h| h.handle(event)).collect()
    |       |
    |       +-- 创建Future列表 (但不执行)
    |
    +-- join_all(futures).await --> 并行执行所有处理器
            |
            +-- GridStrategy::handle(&PriceUpdateEvent)
                    |
                    +-- match event {
                    |   DomainEvent::PriceUpdate(e) => {
                    |       self.on_price_update(e).await
                    |   }
                    |   _ => Ok(())
                    | }
                    |
                    +-- on_price_update(e)
                            |
                            +-- 获取当前价格: 85000.0
                            +-- 获取上一次价格: None (第一次)
                            +-- last_price = Some(85000.0)
                            +-- 无穿越 --> 无信号

T6: 价格继续下跌到 81000
    |
    +-- (重复T4-T5)
    |
    +-- GridStrategy::on_price_update(81000.0)
            |
            +-- last_price = Some(85000.0)
            +-- 当前价格 = 81000.0
            |
            +-- 检查网格线 #0: 80000
            |       last(85000) > 80000 && price(81000) <= 80000? No
            |
            +-- 检查网格线 #1: 82000
            |       last(85000) > 82000 && price(81000) <= 82000? Yes!
            |       |
            |       +-- line.filled = true
            |       +-- signal_count = 1
            |       +-- 生成信号:
            |               TradingSignalEvent {
            |                   action: Buy,
            |                   price: 82000.0,
            |                   quantity: 0.001,
            |                   reason: "网格穿越: 82000"
            |               }
            |
            +-- event_bus.publish(TradingSignalEvent)
                    |
                    +-- sender.send(event) --> 广播频道

T7: EventDispatcher 再次分发
    |
    +-- receiver.recv().await <--- 收到 TradingSignalEvent
    |
    +-- handlers.read()
    |       |
    |       +-- [GridStrategy#1] (OrderHandler忽略此事件)
    |
    +-- GridStrategy::handle(TradingSignalEvent)
            |
            +-- match event {
            |   DomainEvent::TradingSignal(_) => {
            |       // 策略不处理自己的信号
            |       // 实际应由风控模块或执行模块处理
            |   }
            |   _ => {}
            | }
            |
            +-- (将来: RiskManager 会接收并检查风险)

T8: 后续处理 (将来实现)
    |
    +-- RiskManager 检查通过
    |       |
    |       +-- 生成 Command::PlaceOrder
    |
    +-- CommandBus::send(command)
    |       |
    |       +-- match command {
    |       |   Command::PlaceOrder(cmd) => {
    |       |       order_handler.handle(cmd).await
    |       |   }
    |       | }
    |
    +-- OrderCommandHandler::handle(cmd)
            |
            +-- 参数验证: quantity > 0? Yes
            +-- 调用Binance API (POST /api/v3/order)
            +-- 收到响应: order_id = "123456"
            +-- event_bus.publish(OrderSubmittedEvent { order_id, ... })
            +-- 返回 CommandResult::success("订单已提交")

T9: OrderHandler 记录日志
    |
    +-- EventDispatcher 分发 OrderSubmittedEvent
    |
    +-- OrderHandler::handle(OrderSubmittedEvent)
            |
            +-- log::info!("订单已提交: order_id=123456...")
            |
            +-- (将来: 更新数据库、更新持仓等)
```

---

## 核心设计模式

### 1. 发布-订阅模式 (Pub-Sub)

```
+--------------------------+
|    发布者 (Producer)      |
|  MarketDataService       |
|       |                  |
|       | publish(PriceUpdateEvent)
|       v                  |
|  +-----------+           |
|  |  EventBus |           |
|  | (广播频道) |           |
|  +-----+-----+           |
|        |                 |
|  +-----+-----+           |
|  v     v     v           |
| +--+ +--+ +--+           |
| |GS| |RM| |LH|           |
| +--+ +--+ +--+           |
| 消费者 (Consumer)         |
+--------------------------+

优点:
1. 解耦: 发布者不知道谁在听
2. 扩展: 新增消费者不影响现有组件
3. 灵活: 消费者可以选择订阅哪些事件
```

### 2. CQRS (命令查询职责分离)

```
+--------------------------+
|   查询/事件流 (Query/Event)|
|                          |
| Binance --> MarketDataService
|                |         |
|                v publish()|
|            EventBus      |
|                |         |
|                v         |
|          GridStrategy    |
|                |         |
|                v publish()|
|            EventBus      |
|                |         |
|                v         |
|          OrderHandler (日志)
|                          |
+--------------------------+

+--------------------------+
|    命令流 (Command)       |
|                          |
| GridStrategy --> Command::PlaceOrder
|                    |     |
|                    v send()|
|                CommandBus |
|                    |     |
|                    v     |
|           OrderCommandHandler
|                    |     |
|                    v     |
|            Binance API (POST)
|                          |
+--------------------------+

为什么分离:
- 事件流是"通知发生了什么"（不可变、广播）
- 命令流是"要求做某事"（有副作用、点对点）
```

### 3. 分层架构

```
+--------------------------+
| Layer 4: 入口层           |
| main.rs                  |
| - 组装组件               |
| - 启动系统               |
| - 生命周期管理           |
+--------------------------+
            |
            v
+--------------------------+
| Layer 3: 应用层           |
| EventBus + CommandBus    |
| - 协调业务流程           |
| - 不实现具体业务逻辑      |
+--------------------------+
            |
            v
+--------------------------+
| Layer 2: 领域层           |
| Services + Strategies    |
| + Handlers               |
| - 实现核心业务逻辑        |
+--------------------------+
            |
            v
+--------------------------+
| Layer 1: 基础设施层       |
| TCP + TLS + WebSocket    |
| + Logger                 |
| - 与外部系统交互          |
| - 提供技术能力            |
+--------------------------+
```

---

## 组件关系图

```
                    +-------------+
                    |   main.rs   |
                    |   (入口)     |
                    +------+------+
                           |
       +-------------------+-------------------+
       |                   |                   |
       v                   v                   v
+-------------+    +-------------+    +-------------+
| TokioEventBus|    |EventDispatcher|   | CommandBus  |
|             |    |             |    |             |
| broadcast:: |    | handlers:   |    | OrderCmd    |
|  Channel    |    | HashMap     |    |  Handler    |
+------+------+    +------+------+    +------+------+
       |                   |                   |
       | subscribe()       | dispatch          | handle()
       |                   |                   |
+------+------+    +------+------+    +------+------+
|             |    |             |    |             |
v             |    v             |    v             |
+---------+   |  +---------+   |  +---------+   |
|GridStrat|   |  |OrderHdlr|   |  |MarketDt |   |
|         |   |  |         |   |  |Service  |   |
|接收价格  |---+  |接收订单  |   |  |         |   |
|生成信号  |      |事件日志  |   |  |连接Binance|   |
+----+----+      +---------+   |  +----+----+   |
     |                         |       |        |
     | publish()               |       | publish()|
     +-------------------------+       |        |
                                       |        |
                    +------------------+        |
                    |                             |
                    v                             |
             +-------------+                      |
             |   Binance   |<---------------------+
             |   交易所     |
             +-------------+
```

---

## 模块职责对照表

| 模块 | 文件路径 | 职责 | 输入 | 输出 |
|------|---------|------|------|------|
| **main.rs** | `src/main.rs` | 系统组装和启动 | 无 | 运行中的系统 |
| **TokioEventBus** | `src/event_bus.rs` | 事件发布和订阅管理 | DomainEvent | 广播到所有订阅者 |
| **EventDispatcher** | `src/event_bus.rs` | 事件分发 | broadcast频道 | 调用所有handler.handle() |
| **MarketDataService** | `src/services/market_data_service.rs` | 连接交易所获取数据 | WebSocket帧 | PriceUpdateEvent |
| **GridStrategy** | `src/strategies/grid_strategy.rs` | 生成交易信号 | PriceUpdateEvent | TradingSignalEvent |
| **CommandBus** | `src/command_bus.rs` | 命令路由 | Command | CommandResult |
| **OrderCommandHandler** | `src/handlers/order_handler.rs` | 执行下单 | PlaceOrderCommand | CommandResult + OrderSubmittedEvent |
| **OrderHandler** | `src/handlers/order_handler.rs` | 监听订单事件 | OrderSubmittedEvent | 日志记录 |
| **Error模块** | `src/error.rs` | 统一错误处理 | 各种错误 | DomainError |

---

## 关键代码对应关系

| 运行日志 | 代码文件 | 函数 |
|---------|---------|------|
| `事件驱动交易系统启动` | `main.rs:12` | `println!` |
| `事件分发器已就绪` | `main.rs:22` | `ready_notify` 收到通知 |
| `=== 开始测试命令总线 ===` | `main.rs:25` | `CommandBus::send()` |
| `命令执行结果: Success` | `command_bus.rs:16` | `order_handler.handle()` |
| `=== 初始化网格策略 ===` | `main.rs:43` | `GridConfig::new()` |
| `网格配置: 区间 [80000, 100000]` | `main.rs:53` | `grid_config.grid_spacing()` |
| `网格策略已注册` | `main.rs:60` | `event_bus.subscribe()` |
| `正在启动市场数据服务...` | `main.rs:81` | `tokio::spawn` |
| `启动市场数据服务，订阅交易对: ["BTCUSDT"]` | `market_data_service.rs:98` | `println!` |
| `连接Binance WebSocket: wss://...` | `market_data_service.rs:118` | `connect_and_run()` |
| `WebSocket连接成功` | `market_data_service.rs` | WebSocket握手完成 |
| `Binance原始数据: {...}` | `market_data_service.rs` | 收到WebSocket消息 |
| `价格更新 | BTCUSDT | 85000.00` | `market_data_service.rs` | 解析并打印 |

---

## 文件结构

```
src/
├── main.rs                 # 入口层: 系统组装和启动
├── lib.rs                  # 模块导出
│
├── event_bus.rs            # 应用层: 事件总线 + 分发器
│   ├── trait EventBus      #   接口定义
│   ├── trait EventHandler  #   处理器接口
│   ├── struct TokioEventBus #   实现
│   └── struct EventDispatcher # 分发器
│
├── command_bus.rs          # 应用层: 命令总线
│   └── struct CommandBus   #   命令路由
│
├── events/                 # 领域层: 事件定义
│   ├── mod.rs              #   事件汇总 (DomainEvent枚举)
│   ├── market_events.rs    #   市场事件 (PriceUpdateEvent)
│   ├── trading_events.rs   #   交易事件 (OrderSubmittedEvent)
│   ├── account_events.rs   #   账户事件 (BalanceUpdateEvent)
│   ├── strategy_events.rs  #   策略事件 (TradingSignalEvent)
│   └── risk_events.rs      #   风控事件 (RiskCheckEvent)
│
├── commands/               # 领域层: 命令定义
│   ├── mod.rs              #   命令汇总 (Command枚举)
│   └── order_commands.rs   #   订单命令 (PlaceOrderCommand)
│
├── strategies/             # 领域层: 策略实现
│   ├── mod.rs
│   └── grid_strategy.rs    #   网格策略
│       ├── struct GridConfig    # 配置
│       ├── struct GridStrategy  # 策略实现
│       └── struct GridState     # 运行时状态
│
├── handlers/               # 领域层: 处理器
│   ├── mod.rs
│   └── order_handler.rs    #   订单处理器
│       ├── struct OrderHandler         # 事件处理器
│       └── struct OrderCommandHandler  # 命令处理器
│
├── services/               # 领域层: 服务
│   ├── mod.rs
│   └── market_data_service.rs # 市场数据服务
│       ├── struct MarketDataService
│       └── enum ConnectionMode
│
├── error.rs                # 基础设施层: 错误处理
│   └── enum DomainError    #   统一错误类型
│
└── infrastructure/         # 基础设施层
    ├── mod.rs
    ├── error/              #   错误相关
    └── logging/            #   日志相关
```
