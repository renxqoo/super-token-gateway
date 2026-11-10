# 全新网关架构图集

8 张图均为新系统目标设计，当前尚未实现。各运行面均为 Bun + TypeScript 独立进程，运行细节见 [Bun 运行规范](bun-runtime.md)。返回[总体架构](architecture.md)。

## 1. 总体架构

执行面处理字节流；金融面处理权威状态。初期可以单物理资金组，聚合/额度身份从一开始支持扩组。

```mermaid
flowchart TB
    C[客户] --> LB[入口负载均衡 / 在途与公平许可]
    LB --> E[Bun + TypeScript Execution Node]
    E --> AUTH[鉴权、意图、产品与最终请求准备]
    AUTH --> R[Router / 能力、健康与加权选择]
    R -->|金融控制 RPC| F[Finance Service / Bun + TS]
    F --> P[(PG 资金组 / 原子命令、账户与许可)]
    R -->|只有新许可可发送| X[Provider Connector / 单次调用]
    X --> U[供应商]
    U --> S[有界流管线 / 增量计量]
    S -->|SealEvidence 屏障| F
    S -->|正常完成前等待证据提交| C
    W[Financial Worker / Bun + TS / PG 耐久任务] --> P
    CFG[Control API / Bun + TS / 配置与预算] --> AUTH
    CFG --> F
    REDIS[(Redis / 软限流、健康、只读缓存)] --> R
    P --> O[加密证据对象引用 / 派生分析]
```

## 2. 授权、发送与收费时序

首次同步请求只有一次合并的发送前金融事务；异步排队的预留/认领是显式变体。重放命令不重复返回发送能力。

```mermaid
sequenceDiagram
    autonumber
    participant C as 客户
    participant E as Bun Execution Node
    participant F as Bun Finance Service
    participant P as PG 资金组
    participant U as Provider
    participant W as Financial Worker
    C->>E: Idempotency-Key / 业务请求
    E->>E: 新 ingressId / 鉴权、旧意图查询与最终载荷准备
    E->>F: AdmitAndPermit / owner / 版本与载荷摘要
    F->>P: 一个事务：意图、客户预留、供应商预留、调用许可
    P-->>F: 耐久提交确认
    F-->>E: CREATED_PERMIT / 一次发送 token
    Note over E,F: 同命令重放只返回 OBSERVE_ONLY
    E->>U: 一次物理发送
    U-->>E: 数据片段
    E-->>C: 有界背压输出正文
    U-->>E: 最终用量与结束边界
    E->>F: SealEvidence / 最小原始证据与 finality
    F->>P: 同事务：evidence、READY、CAPTURE 工作
    P-->>F: 耐久确认
    F-->>E: SEALED_BILLABLE
    E-->>C: 正常完成标记 / 最终成功响应
    W->>P: 认领 / 幂等 Capture 与 journal、effects、终态
    Note over E,P: 提交未知不重发；上游执行未知有预算地恢复
```

## 3. 用户收费决策

用户收费、调用执行和供应商成本是三种状态。NO_CHARGE 不等于供应商已取消。

```mermaid
stateDiagram-v2
    [*] --> RESERVED: 金融命令预留客户额度
    RESERVED --> READY: 截止前封存合格最终证据
    READY --> CAPTURED: 原子 Capture / 收费至多一次
    RESERVED --> RELEASED: 可靠未执行与撤销证明
    RESERVED --> NO_CHARGE: 未知截止 / 平台永久免收
    RESERVED --> REVIEW: 原始证据或金额冲突
    READY --> REVIEW: 确定性异常
    REVIEW --> READY: 合格证据与审批
    REVIEW --> NO_CHARGE: 平台承担用户费用
    CAPTURED --> [*]
    RELEASED --> [*]
    NO_CHARGE --> [*]
    note right of NO_CHARGE
        晚到证据只能补成本/审计
        原始用户收费不复活
        供应商 HELD 不返还 free
    end note
```

## 4. 智能路由与调用责任

每次费用性物理发送独立占预算；没有隐藏 SDK 重试。错误码不自动证明未执行。

```mermaid
flowchart TD
    A[产品/客户许可/最终请求] --> B[硬过滤：模型语义、地域、能力、费用上界]
    B --> C[优先级分组 / 有界因子加权 / 亲和]
    C --> D{软配额与运行许可允许?}
    D -->|否| N{有其他允许候选?}
    N -->|是| C
    N -->|否| STOP[未发送拒绝 / 已有订单按状态结束]
    D -->|是| Q{全请求调用数与 deadline 允许?}
    Q -->|否| STOP
    Q -->|是| F[Finance 原子预留并授予新调用许可]
    F --> G{返回 CREATED_PERMIT?}
    G -->|否| OBS[查询旧订单 / 不发送]
    G -->|是| SEND[按摘要单次调用 / 内层无自动重试]
    SEND --> R{事实结果}
    R -->|最终合格事实| SEAL[证据屏障 / 异步收费]
    R -->|证明未执行且载荷可重放| CLOSE[关闭本调用 / 释放该成本预留]
    CLOSE --> N
    R -->|执行未知或已输出中断| UNKNOWN[UNKNOWN / 保留供应商责任]
    UNKNOWN --> REC[查询与截止政策 / 不透明换渠道]
```

## 5. 核心数据关系

图是逻辑实体关系，具体复合外键、资产与命令约束见数据库设计。预算发行与资金组之间没有跨库本地事务。

```mermaid
erDiagram
    billing_accounts ||--o{ spending_sources : funds
    spending_sources ||--|{ money_accounts : allocates
    billing_accounts ||--o{ spending_credentials : permits
    billing_accounts ||--o{ inference_orders : buys
    inference_orders ||--o{ provider_invocations : invokes
    inference_orders ||--o| fund_reservations : reserves
    fund_reservations ||--|{ reservation_slices : splits
    money_accounts ||--o{ reservation_slices : holds
    money_accounts ||--o{ account_effects : changes
    command_receipts ||--o{ account_effects : proves
    command_receipts ||--o{ journal_headers : posts
    journal_headers ||--|{ journal_lines : balances
    money_accounts ||--o{ journal_lines : records
    provider_invocations ||--o{ metering_evidence : proves
    inference_orders ||--o| capture_records : captures_once
    inference_orders ||--o{ financial_work : schedules
    provider_allowances ||--o{ provider_invocations : budgets
    inference_orders ||--o{ financial_adjustments : corrects
    payment_orders ||--o{ payout_orders : refunds
```

## 6. 供应商额度划拨

pool 不逐请求更新；本地额度申请/归还采用唯一身份和保守的在途本金，不依赖响应是否收到推断资金回滚。

```mermaid
sequenceDiagram
    participant B as 预算发行者
    participant R as 发行 PG
    participant L as 目标资金组
    participant P as 本地 PG
    B->>R: 原子扣 free、增 issued、写 grant/outbox
    R-->>B: grantId 耐久
    B->>L: grantId / 固定目的 epoch / 来源证明
    L->>P: 去重 + 增 allowance.granted/free
    P-->>L: 耐久接收
    L-->>B: 接收确认
    Note over B,P: 确认丢失重投相同 grant，不再发放本金
    L->>P: 退还事务先扣 free/granted，写 return/outbox
    L->>B: returnId / 已扣除的来源证明
    B->>R: 去重 return，减 issued、增 free
    R-->>B: 已归还
    B-->>L: 确认
    Note over L,R: 归还确认丢失不再次加回，按 returnId 查询
```

## 7. 未知结果与客户免收

用户可以按政策结束账单，平台的上游责任仍有本金覆盖。没有真实 usage 就不伪造精确客户费用。

```mermaid
flowchart TD
    A[已有许可 / 执行或最终用量未知] --> B[UNKNOWN / 保留客户预留与供应商 HELD]
    B --> Q{能力允许最终查询/发票关联?}
    Q -->|是| R[有界恢复 / 不重新推理]
    R --> E{截止前合格最终证据?}
    E -->|是| READY[封存 READY / 客户原始收费一次]
    E -->|否| DEAD[按 resolution_deadline 判定]
    Q -->|否| DEAD
    DEAD --> NC[原子 NO_CHARGE / 客户 reserved 释放]
    NC --> HOLD[供应商 HELD 保留 / 或最坏成本 PROVISIONED]
    HOLD --> LATE[晚到结果/账单只补成本与审计]
    LATE --> COST[应计、准备与真实发票差异核对]
    HOLD --> CAP{未知敞口达到平台上限?}
    CAP -->|是| STOP[停止相关新调用 / 调查与降载]
    CAP -->|否| WAIT[有限恢复 / 不增加无预算风险]
```

## 8. 部署与容量边界

执行节点与金融池分开，各自使用独立 Bun 进程/容器；同一镜像可以有不同启动入口与资源配置。物理资金组数量由完整旅程基准决定。消费单元是已入账本金分配，不是复制缓存余额。

```mermaid
flowchart TB
    LB[LB / 入口短队列] --> E[Bun Execution Nodes / 独立进程与长流]
    E --> RUN[租户公平与上游在途许可]
    E --> MAP[稳定付款聚合定位 / placement epoch]
    MAP --> F1[Bun Finance Service / 资金组 A]
    MAP --> FN[Bun Finance Service / 资金组 N]
    F1 --> P1[(PG A 主 / root、cells、许可与账本)]
    FN --> PN[(PG N 主 / root、cells、许可与账本)]
    P1 --> S1[安全同步候选 A]
    PN --> SN[安全同步候选 N]
    W[Bun Financial Workers / 独立进程与预算] --> P1 & PN
    I[预算发行与配置 / 低频] --> F1 & FN
    RC[(Redis / 软状态)] --> E
    P1 & PN --> BK[WAL、备份与恢复核账]
    E --> U[供应商 / 实际配额和费用契约]
```
