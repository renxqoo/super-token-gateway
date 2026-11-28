# Super Token Gateway 系统架构图

## 系统总览

```mermaid
flowchart LR
    InferenceClient["推理客户端"]
    Customer["Console 客户"]
    Operator["平台管理调用方"]
    Gateway["Gateway<br/>推理、资源与私网管理"]
    Console["Console API<br/>客户、平台管理与用量"]
    Worker["Worker<br/>恢复、维护与事件投递"]
    LedgerDB[("Gateway PostgreSQL<br/>资金、配置与任务")]
    ConsoleDB[("Console PostgreSQL<br/>身份、目标配置与投影")]
    Redis[("Redis<br/>配额与集群通知")]
    Providers["上游模型供应商"]

    InferenceClient -->|公开推理 API| Gateway
    Customer -->|客户 API| Console
    Operator -->|私网管理| Console
    Operator -->|私网管理| Gateway
    Console -->|私网 HTTP| Gateway
    Gateway -->|进程内 Ledger| LedgerDB
    Worker -->|选主、恢复与领批| LedgerDB
    Console --> ConsoleDB
    Gateway <-->|配额与通知| Redis
    Gateway <-->|推理请求与响应| Providers
    Worker -.->|可选 Outbox 推送| Console

    classDef app fill:#e8efff,stroke:#4667b0,color:#172844
    classDef store fill:#e8f5ef,stroke:#43836b,color:#173d32
    class Gateway,Console,Worker app
    class LedgerDB,ConsoleDB,Redis store
```

实线表示主要调用、读写或交互，箭头标明方向。虚线表示可选路径或图中标注的异步通知。Ledger 是 Gateway 与 Worker 各自在进程内使用的账本库。账务状态以 Gateway PostgreSQL 为准，Console 的用量来自已提交的事件。本地单机模式可把 Worker 运行在 Gateway 进程内。 [系统架构与数据归属](architecture.md) · [运行与恢复](reliability-and-operations.md)

- 推理与账务 · [网关内部](#网关内部架构) · [推理与切换](#推理请求与故障切换) · [定价与资金](#定价与资金账本) · [视频任务](#异步视频任务)
- 管理与恢复 · [Console 同步](#console-管理与配置同步) · [模型上架](#新模型上架与价格预览) · [Outbox 与告警](#outbox用量投影和告警) · [Worker](#worker-恢复和维护)
- 插件与部署 · [插件执行](#插件执行与宿主边界) · [插件发布](#插件发布和配置传播) · [生产](#生产部署与可观测性) · [本地](#本地开发与单机验证)

## 网关内部架构

```mermaid
flowchart TB
    Client["推理客户端"]
    Ops["Console API / 私网管理调用方"]

    subgraph App["Gateway 进程"]
        direction TB
        Public["公开监听 · Bun HTTP<br/>推理、模型、视频任务、健康检查"]
        Admin["私网监听 · Hono<br/>管理角色校验、写操作审计"]
        Ingress["入口准入<br/>节点容量、可选 IP 限额、请求体预算"]
        Auth["Key、计费账户与模型权限<br/>Console 组织和最新 Key 策略复核"]
        Proxy["推理编排<br/>报价、预扣、派发与收口"]
        Ranking["候选排序<br/>优先级、协议、健康和自适应权重"]
        Failover["失败切换策略<br/>尝试上限、时限和执行证据"]
        Identity["请求身份<br/>当前版本指纹与已有请求查询"]
        Snap["目录快照<br/>模型、路由、价格、凭据和插件修订"]
        Route["Runtime<br/>运行配额、路由健康、熔断与生命周期"]
        Plug["Plugin Engine · 内容策略与供应商适配"]
        Proto["Protocol · 请求/响应转换、SSE 和 Usage"]
        Schema["Contracts · 输入校验、错误契约"]
        Transport["Runtime 出站 · 鉴权 / 签名、DNS 固定、连接池"]
        Catalog["Ledger · 目录和管理仓储、账务查询"]
        Accounting["Ledger · 预扣、切换与结算"]
        Rating["Kernel · 精确金额与计价"]
        Tasks["视频任务查询和取消"]
    end

    DB[("Gateway PostgreSQL")]
    Redis[("Redis<br/>配置共享模式时")]
    Upstream["上游供应商"]

    Client --> Public --> Ingress --> Auth --> Proxy
    Public --> Tasks
    Ops --> Admin --> Catalog
    Admin --> Schema
    Auth --> Snap
    Auth -->|组织与 Key 复核| Catalog
    Proxy --> Snap
    Proxy --> Ranking
    Proxy --> Failover
    Proxy --> Identity
    Ranking --> Snap
    Ranking --> Route
    Identity -->|读已有请求状态| DB
    Proxy --> Route
    Proxy --> Plug
    Proxy --> Proto
    Proxy --> Transport
    Proxy --> Accounting
    Proxy -->|预扣报价| Rating
    Accounting -->|结算金额| Rating
    Tasks --> Catalog
    Tasks --> Transport
    Snap -->|加载和节点确认| Catalog
    Route <--> Redis
    Catalog --> DB
    Accounting --> DB
    Transport <--> Upstream
```

数据、快照和管理连接池在生产环境指向同一个账本数据库的写入口。管理连接使用独立数据库角色。Gateway 可通过独立队列将抽样诊断 Span 写入可选的 Trace PostgreSQL。 [Gateway 运行时](apps/gateway/src/app.ts) · [推理入口](apps/gateway/src/proxy.ts) · [管理 API](apps/gateway/src/internal-api.ts) · [Trace 队列](apps/gateway/src/trace-queue.ts)

## 推理请求与故障切换

```mermaid
sequenceDiagram
    participant C as 推理客户端
    participant G as Gateway
    participant L as Ledger / Gateway PostgreSQL
    participant U as 上游供应商

    C->>G: 发起推理请求
    G->>G: 认证与准入
    opt 请求携带幂等键
        G->>L: 查询版本化请求身份
        break 命中已有请求或身份冲突
            L-->>G: 返回已有请求状态与身份匹配结果
            G-->>C: 返回对应的 409
        end
    end
    G->>G: 排序候选、执行内容策略并取得双侧报价
    G->>L: 提交幂等预扣和首次供应商尝试
    alt 预扣失败或命中幂等保护
        L-->>G: 资金、冲突或已有请求状态
        G-->>C: 返回对应错误
    else 预扣已提交
        L-->>G: 返回已登记的请求和尝试
        G->>U: 派发首个尝试
        opt 交付前故障且允许切换
            G->>G: 按执行证据、剩余尝试与时限判定，再为下一候选报价
            G->>L: 原子转换供应商尝试，必要时补足客户预扣
            L-->>G: 旧尝试按证据释放或保留待核账
            G->>U: 向下一渠道派发
        end
        U-->>G: 响应、流事件与可获得的用量证据
        opt SSE 内容增量可交付
            G-->>C: 按策略审核和背压转发增量
        end
        alt 满足计价证据或有界估算规则
            G->>L: commitTerminal 提交账单、分录和 Outbox
            alt 确认事务提交
                L-->>G: 结算已持久化
                G-->>C: 返回成功 JSON 或按协议结束成功 SSE
            else 提交失败或结果待确认
                G-->>C: 以错误收口
            end
        else 证据证明未执行
            G->>L: 释放预扣
            G-->>C: 以错误收口
        else 已可能执行且缺少证据
            G->>L: 将预扣保留为 UNKNOWN
            G-->>C: 以错误收口
        end
    end
```

切换受发送状态、执行证据、尝试和时间预算约束。已经向客户端交付内容的 SSE 继续沿当前尝试收口。结算提交结果不明时按原请求 ID 核对。 [路由与执行](routing-and-execution.md) · [推理切换实现](apps/gateway/src/inference/route-attempts.ts) · [账务编排实现](apps/gateway/src/inference/accounting.ts)

## 定价与资金账本

```mermaid
flowchart TB
    Catalog["版本化定价目录<br/>基础价、分层修饰器、汇率版本"]
    Quote["Kernel 计算双侧报价和预扣上界<br/>标准化定价冻结生效价卡"]
    Customer["客户资金<br/>按计费账户和币种分钱包<br/>现金、赠额、授信与钱包租约"]
    Supplier["供应商资金<br/>按账户和币种分池<br/>资金分片与供应商租约"]
    Hold[("持久化预扣<br/>billing_holds 和供应商尝试")]
    Evidence["数字用量、终态和结算依据"]
    Select{"满足普通租约结算条件？"}
    Lease["租约结算 · 批量更新租约用量"]
    Direct["直接结算 · 按账本当前状态分配资金<br/>适用于标准化快照、视频登记等"]
    Tx[("PostgreSQL 原子提交<br/>billing_charges、平衡分录、Outbox")]
    Sweep["租约关闭时归并余额<br/>Worker 回收过期租约"]
    Recovery["UNKNOWN 保留预扣<br/>Worker 处理收据、释放、结算或受限豁免"]

    Catalog --> Quote --> Hold
    Customer -->|客户预扣及来源| Hold
    Supplier -->|实际尝试预扣| Hold
    Hold --> Select
    Evidence -->|证据足以计价| Select
    Hold -->|派发结果未明| Recovery
    Recovery -->|取得足够的核账证据| Direct
    Select -->|是| Lease --> Tx
    Select -->|否| Direct --> Tx
    Lease -.->|租约关闭时归并| Sweep
```

每个逻辑请求可有多个供应商尝试，客户按最终选定的尝试结算。旧尝试的供应商成本独立核账。结算与 Outbox 在 PostgreSQL 同事务提交，客户余额的租约归并可延后。充值、调账、退款和欠款通过资金操作路径保留审计。实际支付、发票及原路现金退款由外部业务系统处理。 [资金与定价](funds-and-billing.md) · [账本表和事务](database.md) · [结算分派实现](packages/ledger/src/settlement-dispatch.ts)

## 异步视频任务

```mermaid
flowchart TB
    Create["POST /v1/videos"]
    Slot["准入校验并预占视频创建名额"]
    Dispatch["提交推理预扣并调用视频上游"]
    Commit[("直接结算事务<br/>写账单、登记 async_tasks、转换名额")]
    Response["返回 gateway_task_id"]
    Query["GET /v1/videos/:id"]
    Cancel["POST /v1/videos/:id/cancel"]
    Access["账户、Key、任务作用域与当前策略校验"]
    Done{"已保存终态？"}
    Poll["按任务绑定的上游轮询<br/>同节点合并在途查询"]
    State[("Gateway PostgreSQL<br/>上游任务标识、状态与结果")]
    LocalCancel["更新本地取消状态"]
    Refund["符合配置的系统故障<br/>通过账本发起幂等自动退款"]
    Sweep["Worker 清理过期名额及陈旧任务"]
    Result["返回任务结果或取消状态"]

    Create --> Slot --> Dispatch --> Commit --> Response
    Commit --> State
    Query --> Access
    Cancel --> Access
    Access -->|查询时读取已登记任务| Done
    Done -->|终态| Result
    Done -->|待完成| Poll --> Result
    Poll -->|保存状态和结果| State
    Poll -->|命中退款条件| Refund
    Access -->|取消时重新校验写入权限| LocalCancel --> Result
    LocalCancel --> State
    Sweep --> State
```

查询从 Gateway 按任务绑定的渠道读取供应商状态。取消更新本地任务状态。普通 Key 按轮换链隔离任务，账户 ADMIN 受当前 Key 策略约束。 [视频处理实现](apps/gateway/src/async-tasks.ts) · [任务仓储](packages/ledger/src/async-tasks.ts) · [个人与团队计费账户](docs/decisions/0013-personal-and-team-billing-accounts.md)

## Console 管理与配置同步

```mermaid
flowchart TB
    Customer["客户"]
    Admin["平台管理员"]

    subgraph App["Console API 进程"]
        ClientAPI["客户监听 /api/v1<br/>登录、组织与成员、计费账户、Key、市场与账务"]
        AdminAPI["管理监听 /admin/v1<br/>平台授权、目录、资金、运营与上游用量监控"]
        Auth["客户或平台会话与角色校验"]
        Sync["目录与组织准入同步<br/>提交 revision，跟踪确认状态"]
        Retry["后台补偿<br/>Console PostgreSQL 锁选出一名执行者"]
        GatewayClient["GatewayClient · 按操作角色选择私网凭证"]
    end

    CDB[("Console PostgreSQL<br/>身份、会话、组织、目标配置、同步进度与投影")]
    PrivateAPI["Gateway /internal/v1<br/>目录、准入、资金操作及查询"]
    GDB[("Gateway PostgreSQL<br/>生效配置、Key 验证材料与权威账务")]

    Customer --> ClientAPI --> Auth
    Admin --> AdminAPI --> Auth
    Auth -->|读写 Console 业务数据| CDB
    Auth -->|管理目录与准入配置| Sync
    Auth -->|实时报价、钱包、账单、资金操作| GatewayClient
    Retry -->|找出待同步 revision| CDB
    Retry --> Sync
    Sync -->|记录目标和已确认版本| CDB
    Sync --> GatewayClient --> PrivateAPI --> GDB
```

个人注册和团队创建分别开通计费账户，签发时将 Key 固定到付款账户。Gateway 使用已持久化的组织准入状态，Console 跟踪上次确认的 revision。待同步配置由 Console 保存并按可恢复条件补偿，待重新提供的渠道凭据由管理员再次提交。 [Console API 契约](docs/console-api.md) · [Console 运行时](apps/console-api/src/app.ts) · [补偿任务](apps/console-api/src/sync-reconciler.ts)

## 新模型上架与价格预览

```mermaid
sequenceDiagram
    participant A as 平台管理员
    participant C as Console
    participant G as Gateway 管理面
    participant L as Ledger / Gateway PostgreSQL
    participant N as 推理节点

    opt 试算草稿
        A->>C: 指定部署、币种、账户与计量
        C->>G: 用审计凭证请求价格预览
        G-->>C: 返回双侧报价、预扣与毛利
        C-->>A: 展示报价
    end
    A->>C: 发布模型和部署
    C->>C: 持久化意图并预留隐藏草稿
    C->>G: 按发布 ID 和目录纪元提交
    G->>L: 同一事务写模型、全部部署、发布记录和新纪元
    G-->>N: 通知刷新
    N->>L: 记录已应用的纪元
    C->>G: 回读提交、当前配置、来源版本和节点确认
    alt 配置仍匹配且无需等待节点
        C->>G: 通过审计凭证读取所绑定客户价卡
        C->>G: 确认价卡读取后的配置与节点状态
        C->>C: 一起提交模型和部署投影
        C-->>A: 市场投影已提交
    else 等待生效或配置有变
        C-->>A: 返回待确认状态
    end
```

Gateway 的模型列表和推理都读取已提交快照。Console 市场在投影事务后公开模型。 [上架接口与恢复](docs/console-api.md#新模型上架与价格预览)

## Outbox、用量投影和告警

```mermaid
flowchart TB
    Tx[("Gateway PostgreSQL<br/>结算与 Outbox 同事务写入")]
    Claim["按消费方和分片认领事件<br/>返回批次租约"]
    Pull["Console 定时经 Gateway 私网 API 拉取"]
    Push["Worker 可选推送<br/>Console 私网 /internal/outbox/events"]
    Store[("Console PostgreSQL 事务<br/>event_id 去重、写用量明细和汇总<br/>持久化客户告警评估任务")]
    Ack["提交后 ACK<br/>拉取方或推送方推进各自消费游标"]
    Eval["客户告警评估<br/>低余额、当日支出"]
    Balance["Gateway 私网查询实时余额"]
    Webhook["客户告警 Webhook<br/>投递状态持久化，可重试"]
    Monitor["可选平台监控<br/>扫描已结算用量，评估极值与持续偏移"]
    Findings[("监控窗口、覆盖状态与异常记录")]
    Metrics["Console 私网 /metrics"]
    Ops["Prometheus → Alertmanager → 平台运维"]

    Tx --> Claim
    Claim --> Pull --> Store
    Claim -.-> Push --> Store
    Store --> Ack
    Ack -->|保存游标| Tx
    Store --> Eval --> Webhook
    Eval -->|低余额评估| Balance
    Store -.->|后台扫描已提交数据| Monitor --> Findings
    Findings --> Metrics --> Ops
```

Outbox 至少一次投递。处理失败或确认未完成时，原批次可在租约释放后重投，Console 按 `event_id` 去重。平台用量异常由运维复核，相关资金操作沿用管理 API。 [消费及客户告警实现](apps/console-api/src/outbox-consumer.ts) · [上游用量监控](docs/decisions/0014-upstream-usage-monitoring.md) · [告警配置](infra/monitoring/prometheus.yml)

## Worker 恢复和维护

```mermaid
flowchart TB
    Workers["Worker 实例<br/>独立进程或 Gateway 内置 Runner"]
    Lock{"在 Gateway PostgreSQL 写库<br/>持有会话级 advisory lock？"}
    Standby["待命并重新检查选主"]
    Sweep["清扫过期预扣和钱包/供应商租约"]
    Reconcile["轮询可配置的核账来源<br/>处理收据和豁免"]
    Maintain["处理过期资金批次与欠款<br/>再平衡供应商分片、清理视频名额与陈旧任务"]
    Partition["准备当前和下一月分区<br/>修剪符合条件的过期配置节点"]
    Outbox["可选业务事件推送<br/>领批、投递，成功后 ACK"]
    Observe["采集积压、选主与恢复指标"]
    DB[("Gateway PostgreSQL")]
    Provider["供应商核账来源"]
    Receiver["Console API 或配置的业务 Webhook"]

    Workers --> Lock
    Lock -->|否| Standby --> Lock
    Lock -->|是| Sweep & Reconcile & Maintain & Partition & Outbox & Observe
    Sweep & Reconcile & Maintain & Partition & Outbox & Observe --> DB
    Reconcile -.->|配置连接器时| Provider
    Outbox -.-> Receiver
```

Worker 主节点负责执行恢复与维护任务。独立 Worker 可另外启用私网健康监听。失去锁后，其他 Worker 可以在稳定写库上接管。 [Worker Runner](apps/worker/src/runner.ts) · [核账轮询](apps/worker/src/reconciliation-poll.ts) · [独立进程入口](apps/worker/src/main.ts)

## 插件执行与宿主边界

```mermaid
flowchart TB
    Route["当前路由与配置快照"]
    Ingress["入口内容链<br/>审核或改写请求"]
    Policy["请求字段策略与协议转换"]
    ProviderPlugin["Provider 修订<br/>声明式执行器 / Provider WASM 执行池"]
    Host["Gateway 宿主<br/>校验有效性、注入凭据与签名、安全传输"]
    Upstream["上游供应商"]
    Decode["供应商响应或流帧适配<br/>协议终态与 Usage 提取"]
    Egress["出口内容链<br/>按响应、流增量和结束阶段处理"]
    Settle["结算及响应收口<br/>内容处置、计量和插件审计"]

    ContentWasm["Content WASM 执行池<br/>可加载内置 pii-guard"]
    ContentWebhook["Content Webhook 执行器"]
    Sandbox["Rust Wasmtime 沙箱<br/>独立执行池、Fuel 和内存预算"]
    Service["外部内容审核服务"]

    Route --> Ingress --> Policy --> ProviderPlugin --> Host --> Upstream
    Upstream --> Decode --> Egress --> Settle
    Decode -->|需要自定义适配时| ProviderPlugin
    Ingress & Egress --> ContentWasm
    Ingress & Egress --> ContentWebhook
    ProviderPlugin -->|WASM 形态| Sandbox
    ContentWasm --> Sandbox
    ContentWebhook -->|受控出站| Service
```

渠道固定 Provider 修订，路由固定有序的 Content 修订。宿主保管供应商秘密，并校验插件产出的用量。回程内容处理影响输出与计费处置。 [供应商插件执行器](packages/plugin-engine/src/providers/executor.ts) · [内容流水线](packages/plugin-engine/src/pipeline.ts) · [原生沙箱](native/plugin-wasmtime/src/lib.rs) · [插件架构决策](docs/decisions/0010-provider-plugin-architecture.md)

## 插件发布和配置传播

```mermaid
flowchart TB
    Admin["平台管理调用方 · Gateway 私网"]
    Draft["上传或安装内置插件<br/>创建不可变修订草稿"]
    Check["预检目标绑定、签名与 ABI<br/>WASM Provider 使用样例试跑"]
    Release["发布 / 回滚<br/>比较预期纪元、校验绑定变更"]
    Revoke["紧急撤销修订"]
    DB[("Gateway PostgreSQL<br/>修订、绑定、发布审计、撤销与 config_epoch")]
    Publisher["接受管理写入的 Gateway<br/>提交后广播并刷新本节点"]
    Redis[("Redis · 可用时广播纪元")]
    Nodes["各 Gateway 节点<br/>加载目录、校验世代与插件完整性"]
    Load["应用新的内存快照"]
    Ack["记录节点应用确认与心跳"]
    Inflight["撤销后中止相关在途工作<br/>按派发证据释放或转入 UNKNOWN"]

    Admin --> Draft --> DB
    Admin --> Check --> Release --> DB
    Admin --> Revoke --> DB
    DB -->|返回提交纪元| Publisher
    Publisher -.-> Redis
    Publisher -->|刷新本节点| Nodes
    Redis -.->|触发刷新| Nodes
    DB -->|定期检查纪元并读目录| Nodes
    Nodes --> Load --> Ack --> DB
    Load -->|发现已撤销修订| Inflight
```

发布事务保存绑定、身份审计、纪元及提交时预期节点。Redis 加速传播，节点通过 PostgreSQL 轮询补偿并上报实际应用状态。普通发布允许在途使用原修订。紧急撤销在节点看到新快照后生效。 [发布 API](apps/gateway/src/internal-plugin-routes.ts) · [快照实现](apps/gateway/src/snapshot.ts) · [发布和撤销契约](docs/decisions/0012-plugin-revisions-and-provider-verification.md)

## 生产部署与可观测性

```mermaid
flowchart TB
    Internet["公网客户端"]
    Admin["私网管理调用方"]
    Ops["值班运维"]
    Providers["上游供应商"]

    subgraph Edge["公网入口"]
        GE["Gateway Caddy<br/>公开 API 与健康路由"]
        CE["Console Caddy<br/>客户 API 与健康路由"]
    end

    subgraph Private["应用与监控私网"]
        G["Gateway 多副本<br/>每个实例均有公开与私网监听"]
        Mgmt["Gateway 私网 HTTP 地址"]
        Con["Console API 副本<br/>客户与管理监听"]
        W["独立 Worker<br/>PostgreSQL 选主"]
        Prom["Prometheus"]
        AM["Alertmanager"]
    end

    GW[("Gateway PostgreSQL<br/>稳定写入口")]
    GStandby[("Gateway PostgreSQL 备库")]
    CW[("独立 Console PostgreSQL 写入口")]
    Redis[("共享 Redis<br/>稳定主节点入口 / Sentinel")]
    Trace[("可选 Trace PostgreSQL")]
    Backup[("各应用库备份与 WAL")]

    Internet --> GE & CE
    GE -->|负载均衡到公开监听| G
    CE -->|客户监听| Con
    Admin --> Mgmt
    Admin -->|管理监听| Con
    Con -->|配置与账务| Mgmt --> G
    G -->|stg_data / stg_control| GW
    W -->|stg_worker| GW
    GW -->|WAL 复制| GStandby
    G <--> Redis
    G --> Providers
    Con --> CW
    G -.->|抽样 Span| Trace
    W -.->|配置事件推送时| Con
    Prom -->|抓取私网指标| G & W & Con
    Prom --> AM --> Ops
    GW & CW --> Backup
```

仓库提供 Caddy、systemd 和监控模板。部署环境提供 PostgreSQL 写入口切换、Redis 故障转移和故障域划分。生产指标从私网监听采集，Gateway 与 Console 各自迁移和备份。 [生产运行手册](reliability-and-operations.md) · [Gateway 入口](infra/Caddyfile) · [Console 入口](infra/console.Caddyfile) · [数据库角色](infra/postgres/roles.sql)

## 本地开发与单机验证

```mermaid
flowchart TB
    Developer["本地客户端"]
    Dev["bun run dev"]
    Infra["bun run infra:up"]
    Mini["单机 Compose 示例"]

    subgraph App["Gateway 进程 · all-in-one"]
        Gateway["公开推理和私网管理"]
        Worker["同进程 Worker Runner"]
    end

    Mock["apps/mock-providers<br/>独立上游协议替身"]
    Ledger[("本地 Gateway PostgreSQL")]
    Redis[("本地 Redis<br/>按配置启用")]
    Console["Console API<br/>按需单独启动"]
    ConsoleDB[("独立的 Console PostgreSQL")]

    Developer --> Gateway
    Dev --> Gateway & Mock
    Infra --> Ledger & Redis
    Mini -->|同一运行模式| Gateway
    Mini --> Ledger
    Gateway & Worker --> Ledger
    Gateway -.->|配置共享配额时| Redis
    Gateway -->|开发模式使用的上游| Mock
    Console -->|私网 HTTP| Gateway
    Console --> ConsoleDB
```

本地开发脚本同时启动 Gateway 和 Mock。仓库的 Gateway Compose 示例包含账本 PostgreSQL 和内置 Worker 的 Gateway。Console 可用独立命令或自身 Compose 启动。 [开发启动脚本](scripts/dev.ts) · [Gateway Compose](infra/docker-compose.yml) · [Console Compose](infra/console.compose.yml) · [Mock 协议覆盖](apps/mock-providers/README.md)
