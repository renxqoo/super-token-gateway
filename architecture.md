# 系统架构与数据归属

当前系统采用 [ADR 0009](docs/decisions/0009-enterprise-gateway-worker-architecture.md) 确立的 Gateway + Worker 架构。Gateway 在进程内调用 `@gateway/ledger`，直接访问 PostgreSQL 完成预扣和结算。Worker 负责恢复、维护与 Outbox 投递。Console API 使用独立数据库，通过 Gateway 私网 HTTP 接口同步配置、查询账务并消费事件。运行拓扑与请求时序见 [架构图](diagrams.md)。

## 模块边界

| 模块                     | 职责                                                                                                            |
| ------------------------ | --------------------------------------------------------------------------------------------------------------- |
| `apps/gateway`           | 公开推理和资源接口、私网 `/internal/v1` 管理 API、配置快照、准入、选路和响应编排                                |
| `apps/worker`            | 使用 PostgreSQL advisory lock 选主，清扫过期预扣和租约、处理核账和欠款、维护分区、清理过期视频名额、投递 Outbox |
| `apps/console-api`       | 客户与平台管理接口，管理账号、会话、组织和成员、计费账户业务配置、用量投影及告警                                |
| `apps/mock-providers`    | 可独立运行的开发与测试供应商替身                                                                                |
| `packages/kernel`        | 精确金额、用量维度、费率规则、预扣报价和结算计算                                                                |
| `packages/contracts`     | 共享输入校验、HTTP 数据契约和错误表示                                                                           |
| `packages/protocol`      | 请求与响应转换、SSE 解析、用量提取和精确数字处理                                                                |
| `packages/plugin-engine` | 内容插件、供应商插件、声明式适配、Webhook 与 Wasmtime 执行                                                      |
| `packages/runtime`       | 安全出站传输、云认证、限流、路由健康、背压与生命周期管理                                                        |
| `packages/ledger`        | PostgreSQL 迁移、配置目录、客户与供应商账本、预扣、结算、对账、异步任务和 Outbox                                |
| `native/`                | Rust Wasmtime 沙箱、内置 PII 防护 WASM 和供应商适配 WASM 示例                                                   |

Gateway 负责何时执行账务操作，Kernel 决定金额，Ledger 执行事务并保存权威状态。Worker 与 Gateway 共用 Ledger 实现。模块边界和推理阶段依赖由 [架构测试](tests/unit/architecture.test.ts) 核对。供应商协议适配与修订发布见 [ADR 0010](docs/decisions/0010-provider-plugin-architecture.md) 和 [ADR 0012](docs/decisions/0012-plugin-revisions-and-provider-verification.md)。

## 数据归属

| 位置                  | 数据                                                                                                                                |
| --------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| Gateway PostgreSQL    | 生效目录、Key 验证材料、加密渠道凭据、价格与汇率版本、钱包和资金批次、预扣及供应商尝试、账单和平衡分录、核账证据、视频任务和 Outbox |
| Console PostgreSQL    | 用户、会话、组织与成员、目录或准入的目标配置和同步进度、已消费事件、用量投影和告警                                                  |
| Redis                 | 已配置共享模式时保存运行配额和并发许可，分发目录纪元与熔断变化                                                                      |
| Gateway 进程内存      | 目录快照、路由统计、在途请求、连接及流缓冲、微批以及租约的本地工作状态                                                              |
| 可选 Trace PostgreSQL | 采样诊断 Span，与账务数据库分开                                                                                                     |

Console 按 revision 同步管理与准入配置。Gateway 持有推理使用的配置和组织准入记录。Console 的用量视图由 Outbox 投影，余额、授信、扣费、退款和核账以 Gateway 账本为准。实际支付、发票和原路现金退款由外部业务系统处理。Gateway 内部的退款操作归还技术钱包额度。见 [Console API](docs/console-api.md) 和 [账本数据模型](database.md)。

常规推理的提示词、回答、工具参数及媒体正文在有界内存中处理。账单和 Outbox 保存计数、金额、标识与受控审计，不作为响应缓存。视频任务另由 `async_tasks.result_json` 保存创建或轮询结果，用于查询任务状态。诊断数据由独立 Trace 配置管理。

## 请求与账务数据流

1. 公网监听执行节点与可选 IP 准入，验证 Key、计费账户、模型与操作，再按账户获取运行配额。Console 管理的账号在 Gateway 数据库复核组织和最新 Key 策略。
2. Gateway 从目录快照取候选，按渠道执行内容策略和协议准备，确定客户售价、供应商成本及预扣上界。账本以请求 ID 和幂等指纹建立客户预扣与首次供应商尝试。支持预建连接的路径可先准备连接，实际派发等待预扣确认。
3. 请求由受控传输发往上游。失败是否允许切换取决于发送状态、执行证据、路由预算和是否已交付正文。切换沿用原客户预扣，原子转换供应商尝试，必要时补足预扣。未明的供应商成本继续待核账。
4. Gateway 从响应提取用量，通过 `commitTerminal` 提交结算。PostgreSQL 提交成功后才返回成功 JSON 或 SSE 成功终态。可背压转发的 SSE 内容增量可以先送出。标准化定价使用预扣时冻结的费率快照。正常结算通过账单和 Outbox 保存用量与费用。常规租约结算可把余额归并延后到租约关闭。
5. 派发后缺少足以结算的证据时，账本保留 `UNKNOWN` 预扣。对话流可按配置，以可观测文本和已有用量估算，并以两侧预扣作为上界。Worker 根据已配置的核账证据、超时和资金规则推进恢复。视频任务登记、初始结算与名额转换可在同一 PostgreSQL 事务完成。
6. Console 通过内部事件接口拉取 Outbox，也可接收 Worker 推送。事件至少一次投递。Console 按事件 ID 在自己的 PostgreSQL 事务内去重、更新用量投影并保存告警任务，提交后确认。

[路由、重试和流式收口](routing-and-execution.md) · [资金与定价](funds-and-billing.md)

## 部署拓扑

| 模式           | 进程和依赖                                                                                                                                                                               |
| -------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 本地或单机验证 | `--mode=all-in-one` 在 Gateway 进程内运行 Worker，共用账本 PostgreSQL。Redis 可选。仓库的 Gateway Compose 示例采用此模式                                                                 |
| 多副本和生产   | Gateway 用 `--mode=gateway` 独立运行，Worker 单独部署。Gateway 副本与 Worker 指向稳定的 PostgreSQL 写入口。生产 Gateway 使用共享 Redis、独立的数据与管理数据库角色、独立的公开与私网监听 |
| Console        | 客户和管理监听位于同一 Console 进程，分别暴露 `/api/v1` 与 `/admin/v1`。私网管理监听也接收 Outbox 推送。Console 连接独立数据库，并通过私网 HTTP 接入 Gateway                             |

公网 Caddy 只转发 Gateway 公开业务和健康路由，Console 的公网模板只转发客户 API 与健康路由。Gateway 和 Console 的管理监听及生产指标保持私网可达。Redis 通过稳定写入口或 Sentinel 接入，资金恢复以 PostgreSQL 备份和 WAL 为依据。生产角色、连接及恢复步骤见 [运行手册](reliability-and-operations.md) 和 [ADR 0011](docs/decisions/0011-production-operations.md)。

## 容量扩展的验证条件

扩容前以相同混合负载分别观察 Gateway 的请求 CPU、节点与 Redis 配额、上游连接、PostgreSQL 预扣与结算延迟、租约归并和 Outbox 积压。独立记录热点钱包和共享供应商分片上的锁竞争，核对成功响应的唯一结算与账本守恒。当前实现以单账本数据库为权威。如果测试证明共享资金库成为上限，可再评估按计费账户划分持久化，连同迁移、查询和幂等边界验证。现有基准入口见 [README](README.md#验证与基准) 与 [容量及对标契约](docs/performance/capacity-contract.md)。
