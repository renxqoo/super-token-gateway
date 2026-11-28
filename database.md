# PostgreSQL 模型与事务

Gateway 和 Worker 通过 `@gateway/ledger` 连接同一账本 PostgreSQL。Console 连接独立数据库，配置同步与事件消费使用 Gateway 私网 API。[系统架构](architecture.md) · [ADR 0009](docs/decisions/0009-enterprise-gateway-worker-architecture.md) · [Console 数据归属](docs/console-api.md)

## 账本数据

| 表或数据                                                         | 职责与约束                                                                |
| ---------------------------------------------------------------- | ------------------------------------------------------------------------- |
| `billing_accounts`、`api_keys`、`console_organization_admission` | 账单账户、Key 验证材料、权限与用量限额、Console 组织准入的已生效状态      |
| `models`、`routes`、`channels`、`upstream_accounts`              | 技术目录、渠道和供应商账户。渠道凭据加密存储，插件绑定使用明确修订        |
| `price_versions`、`pricing_modifiers`、`fx_rate_versions`        | 版本化基础费率、产品与商业修饰器、汇率。预扣可冻结客户和供应商的有效价卡  |
| `wallets`、`wallet_lots`、`wallet_leases`                        | 按账单账户、币种管理客户余额、赠额、授信及运行租约                        |
| `supplier_root_balances`、`supplier_cells`、`supplier_leases`    | 供应商资金及分片租约                                                      |
| `billing_holds`                                                  | 以请求 ID 记录幂等、资金分配、预扣和供应商尝试，尝试写入 `supplier_holds` |
| `billing_charges`、`ledger_entries`                              | 每个已结算逻辑请求的唯一客户账单、数字用量及双方平衡分录                  |
| `billing_reconciliation_*`、`billing_resolutions`                | 待核账任务、收据、受限豁免和人工处置                                      |
| `async_task_admissions`、`async_tasks`                           | 视频创建名额、已登记任务、上游任务标识和查询结果                          |
| `outbox_events`、`outbox_consumers`、`outbox_consumer_shards`    | 已提交业务事件、消费方和带租约的分片游标                                  |

配置发布使用 `config_releases`、`config_release_acks` 与 `gateway_config_nodes`。插件、路由调整和新模型上架分别保存发布类型和配置，按同一纪元机制确认。新模型与全部部署共享一个客户价卡引用，每项部署记录各自的供应商价卡。Console 的 `route_release_requests` 保存两类目录发布意图和拒绝原因，通过模型及路由上的待发布 ID 协调单项编辑和提交投影。 [上架与投影契约](docs/console-api.md#新模型上架与价格预览)

类型定义见 [账本 schema](packages/ledger/src/schema.ts)，运行时以 [SQL 迁移清单](packages/ledger/src/migrations/manifest.json) 为准。

## 事务与恢复

首次派发前，账本提交请求预扣与首次供应商尝试。切换沿用请求 ID，原子登记新尝试和客户补足额。已确认不会执行的旧尝试可释放供应商预扣。未明成本保留在旧尝试供 Worker 核账。

Gateway 在成功 JSON 或 SSE 成功终态前等待结算提交。常规租约结算走集合写入，账单、数字用量、分录及 Outbox 先提交，余额的租约归并可延迟。其他请求走直接结算，按数据库当前预扣和资金状态计算。重复结算通过请求 ID 和数据库唯一约束收敛。带视频任务的结算在同一直接事务登记任务、消费名额。Worker 回收失效租约和预扣，处理已提交的核账凭据及人工处置。

金额在数据库使用 `NUMERIC(38,0)`，TypeScript 用精确整数和有理数计算。账单保留适用的客户价格及供应商成本，账本记录来源分配和不可变平衡分录。退款和调整通过新增资金操作保存审计历史。普通推理结果正文不进入这些账务与投影表。详细规则见 [资金与定价](funds-and-billing.md)。

## 迁移与角色

`bun run db:migrate` 与构建产物 `bun dist/tools/migrate.js` 都从 `DATABASE_URL` 取得明确库名。工具在 PostgreSQL advisory lock 内校验清单，接受空库初始化或从已知版本顺序应用后续迁移。启动时运行进程核对完整已应用清单与校验和。部署不同版本前先检查目标迁移和兼容窗口。历史 v2 迁移资料见 [旧版本执行记录](docs/migrations/enterprise-v2.md)。

生产由迁移身份持有应用对象。在相应数据库应用 [账本角色脚本](infra/postgres/roles.sql)，再将 `stg_data`、`stg_control`、`stg_worker`、`stg_audit` 分别授予对应服务登录。Gateway 分开配置数据与管理登录，Worker 使用独立登录，归档使用审计登录。Console 单独执行 `bun run console:migrate`，应用其 [角色脚本](infra/postgres/console-roles.sql)。

## 分区、归档与留存

`ledger_entries` 按 `created_at` 的 UTC 月份分区，含默认分区。`prepare_ledger_month` 只允许准备当前和下一 UTC 月份，可事务搬移默认分区中落入该月的数据。Worker 按配置维护分区。账单和事件使用各自的表与索引。

使用归档登录导出指定 UTC 月份的账单与分录。导出在同一个重复读只读快照中分页生成 gzip NDJSON，清单记录各文件条数和 SHA-256。目标目录必须由运行用户持有且为私有目录。可独立复核清单，源数据继续在线可查。

```sh
bun run db:archive export YYYY-MM /absolute/private/archive-directory
bun run db:archive verify /absolute/private/archive-directory/manifest.json
```

`bun run db:archive` 读取 `.env.local`。部署环境可对照 [归档脚本](scripts/archive.ts)，用审计连接执行 `bun --no-env-file dist/tools/archive.js`。财务灾难恢复使用 PostgreSQL 逻辑或物理备份及 WAL。月度导出适用于归档复核和审计，数据范围由导出时快照决定。
