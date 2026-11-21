# ADR 0011 生产运行与恢复

生产 Gateway 的数据面和管理面使用两个 PostgreSQL 登录身份。Worker、迁移和只读审计各使用独立身份。运行身份继承相应的无登录角色，迁移身份持有应用对象，部署管理员负责引导数据库与授予角色。Worker 通过限定月份的数据库函数建分区。管理 API 使用逐身份的凭证散列和显式角色。变更先记录审计意图，再执行；成功或失败记录结果。出站事件消费者只能操作该身份授权的游标。

Gateway 使用稳定的 PostgreSQL 写入口，切换前应隔离旧主连接。生产写入口由数据库部署提供。Redis 通过托管写入口或 Sentinel 发现主节点，已配置共享配额时依赖不可用即拒绝准入。Redis 复制允许损失尚未复制的限额计数，应按容量和风险评估。资金状态始终由 PostgreSQL 决定。

恢复演练只在独立进程和临时数据目录中执行。PostgreSQL 使用同步复制验证已确认资金交易的保存，PITR 从基础备份和 WAL 恢复到明确目标。Redis 使用一个主节点、一个副本及三个 Sentinel 进行真实选举。记录丢失的已确认操作、最早丢失操作距离故障的时间，以及注入故障至应用恢复并确认写入的时间。单机结果用于验证恢复行为和复现方法。

## 管理身份和数据库授权

| 管理角色   | 控制面权限                                         |
| ---------- | -------------------------------------------------- |
| `ops`      | 内部健康及渠道状态                                 |
| `catalog`  | 项目、模型、渠道、路由、上游账号及渠道预览         |
| `security` | API Key、内容和供应商插件的管理与试运行            |
| `finance`  | 客户和供应商价目、充值、授信、退款、调账和人工核账 |
| `auditor`  | 余额、收费、分录、预扣、请求账务与守恒检查         |
| `events`   | 指定消费者的 Outbox claim 和 ACK                   |

每个身份持有独立的至少 32 字节随机令牌，编码为无填充 Base64URL 或十六进制。使用私网 HTTPS 管理入口。将令牌保存在调用方密钥存储，Gateway 的 `INTERNAL_AUTH_FILE` 只包含令牌的 SHA-256 十六进制散列。多个角色可以赋予同一身份。替换或撤销凭证后滚动重启 Gateway。生产模式禁用共享 `ADMIN_TOKEN`，需要明确配置两个 PostgreSQL 登录和共享 Redis。

```json
[
  {
    "id": "finance-service",
    "token_sha256": "<64 lowercase hexadecimal characters>",
    "roles": ["finance"]
  },
  {
    "id": "billing-service",
    "token_sha256": "<different 64-character digest>",
    "roles": ["events"],
    "consumers": ["business-billing"]
  }
]
```

管理变更返回 `x-admin-request-id`。审计分开持久化请求意图和处理结果，记录身份、路由、路径资源、状态码及时间。认证后的核账身份来自 Gateway。数据库中的历史审计记录对运行账号只可追加。操作已提交、结果审计持久化失败时保留待查记录并触发告警。操作员使用请求 ID 和业务幂等键核查原操作，再决定重试。

### 建库和迁移顺序

1. 为账本使用独立数据库。用部署管理员创建专用迁移登录 `stg_migration` 和四个新的无额外权限的运行登录。迁移登录持有数据库应用对象。使用数据库管理员或秘密管理系统为登录设置密码。
2. 新库由迁移登录建对象。存量库先做可恢复备份并停止写入，再以部署管理员运行 `psql -X "$DBA_URL" -v migration_role=stg_migration -f infra/postgres/adopt-ownership.sql`，仅转移 Gateway 应用对象。以迁移身份执行 `bun dist/tools/migrate.js`，确认 schema 为 `2026112201`。
3. 在专用数据库执行 `psql -X "$DBA_URL" -f infra/postgres/roles.sql`。此脚本收回 `PUBLIC` 对当前数据库和公共 schema 的默认访问，按实际 SQL 授予权限。将 `stg_data`、`stg_control`、`stg_worker`、`stg_audit` 分别授给对应的专用登录。表权限集中授予父分区表。Worker 只可通过限定月份的函数建分区。
4. 配置生产 Gateway 的 `DATABASE_URL` 和 `INTERNAL_DATABASE_URL` 指向同一个写入口和数据库，分别使用数据和控制登录。独立 Worker 使用自己的登录。归档使用只读审计登录。运行身份须为独立登录，Gateway 和 Worker 启动会检查当前身份与所属角色。生产网络还应限制运行登录来源与 SQL 入口。 [角色授权](../../infra/postgres/roles.sql) · [对象转属](../../infra/postgres/adopt-ownership.sql) · [Gateway 样例](../../infra/gateway.example.env) · [Worker 样例](../../infra/worker.example.env)

## 监控与告警

生产 Gateway 在私网管理监听提供 `/metrics`，公开监听提供业务 `/readyz`。Worker 在健康端口提供 `/metrics`。用相同的 `stg_cluster` 标签聚合共享一个账本的实例。采集所有 Gateway 和 Worker 实例，以独立外部探针检查负载均衡地址。 [Prometheus 配置](../../infra/monitoring/prometheus.yml)

规则覆盖就绪、5xx、P99、快照、共享配额、结算失败、行政审计、Worker 选主、账务清扫、过期预扣与租约、未明账、Outbox 与采集缺样本。未明账按原预扣到期时间估算账龄。Outbox 以启用的业务 Webhook 消费者游标之后的首个事件时间作为积压指标。告警阈值是随仓库提供的初值，生产使用业务批准的 SLO 并在发布前验证 Alertmanager 接收端收到测试告警。 [规则](../../infra/monitoring/alerts.yml) · [行为测试](../../infra/monitoring/alerts.test.yml) · [Prometheus 告警规则文档](https://prometheus.io/docs/prometheus/latest/configuration/alerting_rules/)

## TCP 建连突发

TCP 监听队列先于 HTTP 准入。Bun 1.4.4 为 Gateway 请求 512 个监听队列槽位，内核会按上限截断。macOS 压测曾观察到两路监听均达到 128，客户端在收到 HTTP 响应之前得到 `ECONNRESET`。单独调整 `MAX_IN_FLIGHT` 无法改变该容量。 [Bun 监听实现](https://github.com/oven-sh/bun/blob/bun-v1.4.4/packages/bun-usockets/src/bsd.c#L1079-L1109) · [XNU 队列上限](https://github.com/apple-oss-distributions/xnu/blob/main/bsd/kern/uipc_socket.c#L841-L858)

- Linux 宿主机部署将 [队列配置](../../infra/sysctl.d/90-super-token-gateway.conf) 安装到 `/etc/sysctl.d`，通过 `sudo sysctl -p /etc/sysctl.d/90-super-token-gateway.conf` 应用后滚动重启入口和 Gateway。使用 `sysctl net.core.somaxconn` 检查命名空间上限，使用 `ss -lnt` 检查各监听端口的 Send-Q。Compose 在 Gateway 自己的网络命名空间显式设置 4096。单个 Bun 1.4.4 监听队列仍取它请求的 512。复测前后用 `nstat -az TcpExtListenOverflows TcpExtListenDrops` 比较计数。 [Linux TCP 配置](https://docs.kernel.org/networking/ip-sysctl.html#tcp-variables)
- macOS 同条件直连压测应先记录 `sysctl -n kern.ipc.somaxconn`，由管理员临时用 `sudo sysctl -w kern.ipc.somaxconn=512` 解除截断，再启动两个 Gateway。用 `netstat -Lan` 核对其公有端口的 `maxqlen`，压测结束后恢复原值。若原值已达 512 或更高，可直接验证端口并运行。

先执行 `bun run build`。将 `DATABASE_URL` 指向有权新建和删除测试库的隔离 PostgreSQL，必要时通过 `DURABILITY_REDIS_SERVER` 指定 Redis 可执行文件。以 `bun --no-env-file scripts/bench-prod.ts --sse-burst` 复测原四档，每档一次性发出请求并沿用连接池。以 `bun --no-env-file scripts/bench-prod.ts --sse-burst-fresh` 只跑 4,000 路，每路 120 秒，使用空连接池并验证全部新建。两种模式都校验 SSE 尾帧、逐请求结算和金额、共享配额回收及账本守恒。 [压测入口](../../scripts/bench-prod.ts) · [长 SSE 验收](../../scripts/bench-sse-burst.ts)

### 2026-11-22 建连与结算实测

macOS 原始四档在最后一档完成 3,926 路，另外 74 路在 HTTP 响应前报告 `connect ECONNRESET`。本机最小复现在连接突发期间采到两路实际监听队列均达到 `128/128`，原生 Node.js 客户端也能重现建连重置。隔离 Linux 的 4,000 路、20 秒对照中，将 `somaxconn` 从 128 提到 4096 后，`ListenOverflows`、`ListenDrops` 分别从 775 降到 0，首个内容帧 P99 从 1,297.55 毫秒降到 358.26 毫秒。两档 Linux 对照的请求最终都成功。

隔离 Linux 使用两个 Gateway、真实 PostgreSQL 17.11 和 Redis 7.4.11、独立 Worker 与模拟上游，压测客户端直连 Gateway。原四档按 500 路 45 秒、1,000 路 70 秒、2,000 路 90 秒、4,000 路 120 秒依次执行，含预热共 7,700 笔成功响应及唯一结算。最后一档新建和复用各 2,000 条 TCP 连接，4,000 路同时活跃的采样跨度为 118.26 秒，内核监听溢出和丢弃计数均为 0。

冷建连验收一次性打开 4,000 条新 TCP 连接。每路持续 120 秒，建连失败和断流均为 0。两台 Gateway 各采到 2,000 路，全部 4,000 路同时活跃的采样跨度为 118.22 秒。两轮 4,000 路验收均有 160,000 个内容帧及 4,000 笔带不同请求 ID 的唯一结算，客户合计 740,000 原子单位、供应商合计 148,000 原子单位，金额差异和资金守恒异常均为 0。测试结束后活跃流、共享许可、活跃租约和未明账均为 0。

macOS 原机升级队列后的复测需管理员授权；本次已完成上述 Linux 回环验证。

## 故障切换与 PITR 操作

- **PostgreSQL**。在负载均衡和写入口先隔离旧主及旧连接，确认备库已追到可接受的 WAL 位置，再提升并切换写入口。同步确认模式取决于目标复制策略和延迟。确认 Gateway 能完成实际结算，检查旧主保持隔离、仅一个 Worker 持有选主锁，随后读取账本守恒和未明账。 [PostgreSQL 17 流复制](https://www.postgresql.org/docs/17/warm-standby.html)
- **Redis**。托管服务提供稳定主节点地址。使用 Sentinel 时，设置 `REDIS_SENTINEL_URLS` 和 `REDIS_SENTINEL_MASTER`，`REDIS_URL` 提供数据服务的 TLS 和认证参数。生产配置至少三个独立 Sentinel。Gateway 取地址多数并验证 Redis 角色。Redis TLS 部署应确保 Sentinel 返回的主机名匹配数据端证书。异步复制可能丢失配额计数，活着的旧主需要由部署侧隔离。对必须跨故障严格不超限的配额，需单独验证共享配额的持久性与隔离。 [Redis Sentinel](https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/) · [客户端发现](https://redis.io/docs/latest/develop/reference/sentinel-clients/)
- **PITR**。保持基础备份以及连续 WAL 归档，定期验证备份和保留窗口。事故后冻结对旧主的写入，选择目标时刻并恢复到独立实例，重放连续 WAL 到目标，核对关键交易、账本守恒、角色、消费游标与后续写入。验证通过后切换写入口。对恢复目标之后已确认的业务操作单独对账，通过原幂等键处置。 [PostgreSQL 17 PITR](https://www.postgresql.org/docs/17/continuous-archiving.html)

### 复现与计时

`bun run test:production` 启动回环实验室。使用 PostgreSQL 17 的 `initdb`、`pg_ctl`、`pg_basebackup`、`pg_verifybackup`、`psql` 及 Redis 7.4 的 `redis-server`。依赖路径可用 `PG_TOOLS_DIRECTORY` 和 `DURABILITY_REDIS_SERVER` 指定。脚本使用生成的测试凭证和临时目录，不读取项目的 `.env.local` 或已部署服务。失败时保留隔离证据目录。

PostgreSQL 计时从切断写入口起算，分别记录首笔充值、首笔推理结算、Worker 首次完成清扫的恢复时间。Redis 断开复制连接后继续记入一笔主节点确认的额度，再强杀主节点，由 Sentinel 提升副本。分别记录首次恢复共享配额与首笔成功推理结算。PITR 使用已准备并通过校验的本地基础备份，从模拟错误写入已提交后开始计时，至恢复实例通过守恒审计并接受新写入。远端下载、报障响应、真实写入口变更、跨域链路及实际数据量均应在生产拓扑中测量。

## 2026-11-22 本机验收

环境为 Bun 1.4.4、PostgreSQL 17.11、Redis 7.4.12、Prometheus 3.5.0 和 Alertmanager 0.28.1。数据库和 Redis 使用真实进程与回环 TCP，推理上游使用测试响应。

| 场景                    | 样本 RPO                                                                       | 实测 RTO                                                              | 验收结果                                                                                                         |
| ----------------------- | ------------------------------------------------------------------------------ | --------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| PostgreSQL 同步备库提升 | 0 毫秒，8 笔已确认资金交易全部保留                                             | 充值 5,561.28 毫秒，推理结算 5,585.82 毫秒，Worker 清扫 5,640.98 毫秒 | 旧主停止，故障窗口 readiness 返回 503。新主恢复结算及唯一 Worker 选主锁，账本守恒。同期样本采用 `remote_apply`。 |
| Redis Sentinel 提升     | 101.26 毫秒的样本恢复窗口。复制断开后确认的 1 笔配额更新丢失，已复制标记保留。 | 共享配额 2,397.65 毫秒，推理结算 2,398.10 毫秒                        | 三个 Sentinel 完成选举，Gateway 撤销旧主许可，接入新主并完成账本结算。                                           |
| PostgreSQL PITR         | 目标点以前的确认样本丢失 0 笔。模拟故障比目标点晚 89.33 毫秒。                 | 191 毫秒                                                              | 基础备份校验通过。WAL 重放保留目标前充值，排除目标后写入。守恒审计通过，受限角色完成新写入。                     |

受限身份完成 13 原子单位的应计业务结算。验证控制面越权拦截、审计身份和目标资源、指定 Outbox 消费者，以及 7 项数据库越权操作返回 PostgreSQL `42501`。旧对象转属、只读账务访问及 Worker 分区函数在独立数据库通过验证。

`promtool` 校验 17 条规则，并通过 4 组告警行为测试。Alertmanager 示例通过配置校验。使用 `PROMETHEUS_BIN` 和 `ALERTMANAGER_BIN` 指定已安装的二进制，执行 `bun run test:alerts`。本次演练的模拟结算失败由 Prometheus 检测，Alertmanager 将严重告警送达回环 HTTP 接收器。测试启动到接收耗时 11.01 秒。生产需配置真实接收端、独立入口探针及实际私网采集目标。 [Alertmanager 示例](../../infra/monitoring/alertmanager.example.yml) · [送达演练](../../scripts/verify-alert-delivery.ts) · [Alertmanager 配置文档](https://prometheus.io/docs/alerting/latest/configuration/)

代码回归通过 88 项单测和 27 项真实 PG、Redis、HTTP 集成测试。隔离构建生成 7 个产物，独立启动及归档通过。强杀和逻辑备份恢复演练回收 16 笔预扣，恢复后守恒检查零异常。

本次提供了运行配置和可重跑的单机验收。生产放行仍需要目标环境的旧主隔离、跨故障域切换、远端备份恢复、实际负载和接收端送达的记录。Redis 异步复制的配额损失须由生产方接受或由更严格的配额存储策略解决。
