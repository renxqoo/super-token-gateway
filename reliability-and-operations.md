# 发布、容量与故障恢复

[系统架构](architecture.md) · [ADR 0011 生产运行与恢复](docs/decisions/0011-production-operations.md) · [Console 部署与验收](docs/console-api.md#部署与验收)

## 本地环境

`bun run setup:local` 生成受保护的 `.env.local` 和内部凭证文件。首次生成的示例使用 PostgreSQL 55442、Redis 56382、Gateway 8088 和独立 mock 9088。实际端口以本机配置为准。`bun run infra:up` 使用 `PG_TOOLS_DIRECTORY` 和 `DURABILITY_REDIS_SERVER` 指定的工具，启动仓库拥有的本地 PostgreSQL 与 Redis。服务由 `bun run infra:status` 和 `bun run infra:down` 管理。端口已被其他进程占用时脚本拒绝接管。

`bun run db:migrate` 初始化或升级账本，`bun run dev` 启动 mock 与内置 Worker 的 Gateway。Console 单独使用 `bun run console:migrate` 和 `bun run console`，并配置自己的数据库和 Gateway 私网 API 凭证。[本地配置脚本](scripts/setup-local.ts) · [开发启动脚本](scripts/dev.ts)

## 生产发布

1. 按 [.bun-version](.bun-version) 使用锁定的 Bun，执行 `bun install --frozen-lockfile`。在目标平台准备原生 Wasmtime 库，再执行 `bun run verify` 生成和验证构建产物。验证依赖见 [交付手册](delivery-and-validation.md)。
2. 准备 PostgreSQL 稳定写入口和专用库。使用对象所有者连接运行 `bun dist/tools/migrate.js`，按 [账本迁移与角色](database.md#迁移与角色) 配置授权。Console 使用独立库及 [Console 角色脚本](infra/postgres/console-roles.sql)。升级前核对迁移清单和备份。
3. 生产 Gateway 采用 `--mode=gateway`。用 `DATABASE_URL` 和 `INTERNAL_DATABASE_URL` 配置同一写入口和数据库的两个独立登录，准备共享 Redis、`INTERNAL_AUTH_FILE` 和 `PLUGIN_SIGNING_PUBLIC_KEY_PEM`。远端 PostgreSQL 连接要求 `sslmode=verify-full`，回环环境例外见 [ADR 0011](docs/decisions/0011-production-operations.md#管理身份和数据库授权)。独立 Worker 用自己的数据库登录。样例见 [Gateway](infra/gateway.example.env)、[Worker](infra/worker.example.env) 和 [Console](infra/console.example.env)。
4. Linux 单元从 `/opt/super-token-gateway` 运行 `dist`，环境文件放在 `/etc/super-token-gateway/`。按实机配置校验 [Gateway](infra/gateway@.service)、[Worker](infra/worker.service) 和 [Console](infra/console-api@.service) 单元，给每个 Gateway 副本分配独立监听地址或端口。
5. [Gateway Caddy 模板](infra/Caddyfile) 代理公开推理及健康路由，示例用 8080 和 8082 作为两路公开端口。用 [Console Caddy 模板](infra/console.Caddyfile) 转发客户 API。管理监听、Outbox 推送入口和生产指标仅在私网开放。以 `/readyz` 验收实例后接入流量，摘除旧节点后再发送 SIGTERM 排空。
6. 验证一笔实际结算、Worker 选主和 Outbox 消费。启用 Console 时再验证组织和 Key 策略同步、账务查询和用量投影。涉及不兼容数据库升级时按对应迁移说明协调旧进程退出和新进程启动。

`infra/docker-compose.yml` 使用单库、单 Gateway 与内置 Worker，适合单机验证。生产拓扑使用独立 Worker、数据库及最小权限角色，并由部署平台提供 PostgreSQL 和 Redis 故障转移。

## 可用性与故障行为

| 故障                                    | 当前行为与核对点                                                                                                                                           |
| --------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| PostgreSQL 准入失败或写入口只读         | 新请求得不到预扣，不派发。Gateway 的 `/readyz` 检查写入口。                                                                                                |
| PostgreSQL 结算失败或无法确认提交       | 响应以失败收口，SSE 不发成功终态。按原请求 ID 核对已提交账单和待核账预扣，重放沿用原幂等约束。                                                             |
| 共享 Redis 不可用                       | 已配置共享配额的请求失败关闭，Gateway 就绪返回失败。Redis 切换可丢失尚未复制的配额计数，资金继续由 PostgreSQL 决定。                                       |
| 上游超时、错误或输出后断流              | 由发送状态、执行证据和路由预算决定是否切换。已输出的流不拼接替代响应。缺证据的请求保留预扣，对话流的有界估算见 [资金规则](funds-and-billing.md#请求链路)。 |
| Gateway SIGTERM 或 SIGKILL              | SIGTERM 停止接纳新请求并有界排空。SIGKILL 后由 Worker 清扫过期预扣及租约，已派发且无法确认的请求进入核账。                                                 |
| Worker 主节点不可用或 Outbox 接收方中断 | 由其他 Worker 获取 PostgreSQL 选主锁。已提交事件保留在 Outbox，消费方在提交本地事务后确认，失败按租约和游标重试。                                          |

Redis 可通过托管主节点写入口或 `REDIS_SENTINEL_URLS` + `REDIS_SENTINEL_MASTER` 发现主节点。PG 主备切换先隔离旧主，再校验 WAL 位置和新写入口。PostgreSQL 恢复后核对资金、新写入、Worker 选主及 Outbox 游标。更多细节见 [ADR 0011 的故障切换与 PITR](docs/decisions/0011-production-operations.md#故障切换与-pitr-操作)。

## 智能路由与尝试预算

候选按优先级、模型策略和渠道健康排序。切换复用同一逻辑请求，最终只向客户结算选中尝试。供应商未明成本独立待核账。客户端已经收到内容后继续沿当前流收口。尝试数、首包时限、结果不明重试及自适应排序见 [路由与执行](routing-and-execution.md) 和 [自适应路由方案](docs/adaptive-provider-routing.md)。

## 监控、备份与容量

- 生产 Gateway 从私网管理监听采集 `/metrics`。独立 Worker 配置健康端口后提供 `/livez`、`/readyz` 和 `/metrics`。Console 管理监听提供用量监控指标。观察就绪、结算失败、未明账龄、过期租约、选主、Outbox 积压、配置分叉和共享配额错误。通过独立探针验证公网入口。[Prometheus 配置](infra/monitoring/prometheus.yml) · [告警规则](infra/monitoring/alerts.yml)
- Gateway 账本和 Console 各自保留备份与恢复记录。按业务切点协调两库的 PITR，核对 Console 组织准入、事件 ID 去重、Gateway 消费游标和账本守恒。恢复到更旧的目录世代后按 ADR 0011 的分叉程序处理。按月导出账单与分录见 [数据库归档](database.md#分区归档与留存)。
- `bun run test:artifact` 核验独立产物、账户与推理链路、Console 及账本归档。`bun run test:durability` 检查进程强杀、幂等重放和逻辑备份恢复。`bun run test:production` 使用隔离进程演练 PostgreSQL 主备、PITR 和 Redis Sentinel，目标环境仍需独立完成部署验收。
- 测量前记录每个 Gateway 的数据、快照和管理池预算，并加上 Worker、Console 及滚动扩容实例占用。当前基准以 [README 的验证与基准](README.md#验证与基准) 为入口。比较时同时核对尾延迟、账本守恒、未明预扣和 Outbox 投递进度。
