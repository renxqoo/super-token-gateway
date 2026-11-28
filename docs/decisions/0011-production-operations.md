# ADR 0011 生产运行与恢复

常规预扣继续按固定顺序批量加锁。需换分片时优先选择同交易分组的一条有余额的分片。锁竞争导致事务确认回滚或首选分片无可用余额时，相关请求进入独立重试队列。每笔重试只锁定一个有足够余额的供应商分片，取到锁后检查客户额度的有效期。事务确认回滚后的竞争可返回 `503 ledger_busy`，提交结果不明确时保留既有故障恢复语义。

普通和租约预扣都对事务体内的 PostgreSQL 查询取消使用相同的独立重试。租约重试保留原内存预扣，重试事务确认回滚时才归还。提交阶段失败按原有未知结果处理。

直接结算事务内的查询取消允许在原有拆批窗口内隔离重试。首次拆分的两个子批可并行，避免单笔阻塞拖住健康请求。租约结算遇到同类 SQLSTATE 时转入按原请求 ID 幂等的直接结算路径。重试复用已取得的用量和原价格版本。无法确认结算时保留待核账资金。

生产 Gateway 的数据面和管理面使用两个 PostgreSQL 登录身份。Worker、迁移和只读审计各使用独立身份。运行身份继承相应的无登录角色，迁移身份持有应用对象，部署管理员负责引导数据库与授予角色。Worker 通过限定月份的数据库函数建分区。管理 API 使用逐身份的凭证散列和显式角色。变更先记录审计意图，再执行；成功或失败记录结果。出站事件消费者只能操作该身份授权的游标。

Outbox 领批与 ACK 先锁定消费方记录，同一消费方的游标更新由该行锁串行化。新领批只给实际返回的事件所在分片写入租约，确认只更新该租约的分片。消费方初始化、部分确认和旧版迁移的租约沿用同一锁顺序。

Gateway 使用稳定的 PostgreSQL 写入口，切换前应隔离旧主连接。生产写入口由数据库部署提供。Redis 通过托管写入口或 Sentinel 发现主节点，已配置共享配额时依赖不可用即拒绝准入。Redis 复制允许损失尚未复制的限额计数，应按容量和风险评估。资金状态始终由 PostgreSQL 决定。

恢复演练只在独立进程和临时数据目录中执行。PostgreSQL 使用同步复制验证已确认资金交易的保存，PITR 从基础备份和 WAL 恢复到明确目标。Redis 使用一个主节点、一个副本及三个 Sentinel 进行真实选举。记录丢失的已确认操作、最早丢失操作距离故障的时间，以及注入故障至应用恢复并确认写入的时间。单机结果用于验证恢复行为和复现方法。

## 管理身份和数据库授权

| 管理角色   | 控制面权限                                         |
| ---------- | -------------------------------------------------- |
| `ops`      | 内部健康及渠道状态                                 |
| `catalog`  | 计费账户、模型、渠道、路由、上游账号及渠道预览     |
| `security` | API Key、内容和供应商插件的管理与试运行            |
| `finance`  | 客户和供应商价目、充值、授信、退款、调账和人工核账 |
| `auditor`  | 余额、收费、分录、预扣、请求账务与守恒检查         |
| `events`   | 指定消费者的 Outbox claim 和 ACK                   |

每个身份持有独立的至少 32 字节随机令牌，编码为无填充 Base64URL 或十六进制。使用私网 HTTPS 管理入口。将令牌保存在调用方密钥存储，Gateway 的 `INTERNAL_AUTH_FILE` 只包含令牌的 SHA-256 十六进制散列。多个角色可以赋予同一身份。替换或撤销凭证后滚动重启 Gateway。所有环境使用 `INTERNAL_AUTH_FILE`。生产环境还需配置两个 PostgreSQL 登录、共享 Redis 和插件验签公钥 `PLUGIN_SIGNING_PUBLIC_KEY_PEM`（启动期校验公钥合法性及原生 `wasmtime` 沙箱自检）。

Gateway 数据面、控制面和独立 Worker 的生产 PostgreSQL 连接使用 `sslmode=verify-full`，由 Bun 校验服务器证书和主机名。与受控本地数据库或本地 TLS 代理通信时可使用字面回环地址。远端连接拒绝禁用证书校验，连接地址由 URL 主体指定。账本归档应写入由运行用户持有的私有目录，数据与清单仅向运行用户开放。现有归档和分发副本的权限由运维核对。

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
2. 由迁移登录连接新建空库，执行 `bun dist/tools/migrate.js` 初始化对象并校验版本与构建产物清单一致。
3. 在专用数据库执行 `psql -X "$DBA_URL" -f infra/postgres/roles.sql`。此脚本收回 `PUBLIC` 对当前数据库和公共 schema 的默认访问，按实际 SQL 授予权限。将 `stg_data`、`stg_control`、`stg_worker`、`stg_audit` 分别授给对应的专用登录。表权限集中授予父分区表。Worker 只可通过限定月份的函数建分区。
4. 配置生产 Gateway 的 `DATABASE_URL` 和 `INTERNAL_DATABASE_URL` 指向同一个写入口和数据库，分别使用数据和控制登录。独立 Worker 使用自己的登录。归档使用只读审计登录。运行身份须为独立登录，Gateway 和 Worker 启动会检查当前身份与所属角色。生产网络还应限制运行登录来源与 SQL 入口。 [角色授权](../../infra/postgres/roles.sql) · [Gateway 样例](../../infra/gateway.example.env) · [Worker 样例](../../infra/worker.example.env)

## 监控与告警

生产 Gateway 在私网管理监听提供 `/metrics`，公开监听提供业务 `/readyz`。Worker 在健康端口提供 `/metrics`。用相同的 `stg_cluster` 标签聚合共享一个账本的实例。采集所有 Gateway 和 Worker 实例，以独立外部探针检查负载均衡地址。 [Prometheus 配置](../../infra/monitoring/prometheus.yml)

同一 Gateway 节点的就绪与指标请求共用一次在途的 PostgreSQL 写入口探测。探测结束后复用结果 250 毫秒，限制探针流量给数据库带来的查询压力。排空、配置快照不可用或 Redis 断连即时反映在就绪结果中。

规则覆盖就绪、5xx、P99、快照、共享配额、结算失败、行政审计、Worker 选主、账务清扫、过期预扣与租约、未明账、Outbox 与采集缺样本。未明账按原预扣到期时间估算账龄。Outbox 以启用的业务 Webhook 消费者游标之后的首个事件时间作为积压指标。告警阈值是随仓库提供的初值，生产使用业务批准的 SLO 并在发布前验证 Alertmanager 接收端收到测试告警。 [规则](../../infra/monitoring/alerts.yml) · [行为测试](../../infra/monitoring/alerts.test.yml) · [Prometheus 告警规则文档](https://prometheus.io/docs/prometheus/latest/configuration/alerting_rules/)

## 公网路由和视频任务准入

Caddy 公网模板按 HTTP 方法转发 Gateway 的公开路径，包括音乐生成和视频创建、查询、取消。管理接口和指标路径保持在私网监听。已有 `caddy:2-alpine` Docker 镜像时，运行 `bun --no-env-file scripts/verify-gateway-ingress.ts`，通过临时 Caddy 和本地上游夹具核对 OpenAPI 公开路径的转发及私网路径的拒绝。 [Caddy 配置](../../infra/Caddyfile)

视频任务查询和取消验证项目凭证与组织状态后，在读取任务之前先取得项目配额。正常查询、未找到的查询、已完成任务的查询以及取消操作使用同一个项目的 RPM、TPM 和并发额度，与推理共享配额。超限返回 `429`，共享配额不可用返回 `503`。跨项目请求在取得配额后仍返回 `404`。

视频创建使用非流式响应，在派发前由 PostgreSQL 按账单账户串行预留一个名额，同时计入在途创建、已持久化的活跃任务和取消冷却中的任务。创建成功时，同一事务写入任务、账单和 Outbox 并消费预留。任务登记失败会撤回该事务，网关保留名额和待核账预扣。确定上游未执行时可以重试或归还名额。结果不明确时保留名额至到期，由 Worker 定期清理。部署新版前先执行账本迁移并应用角色授权，避免旧节点和新版节点同时处理视频创建。

Console 管理的密钥在推理准入时复核当前数据库中的凭据和权限。快照与当前策略不一致时返回 503，等待节点同步。资金预占依据数据库中的当前密钥额度，租约快路径只使用无密钥额度的请求。

公网业务入口在读取请求体前检查节点压力和 IP 配额。配置可信代理时，Gateway 从连接对端和可信转发链解析地址。生产仅应信任实际连接 Gateway 的代理及由它验证的上游转发链。健康路径和私网监听不消耗业务 IP 配额。

IP 限流由 `INGRESS_IP_MODE` 控制，支持 `off`、`observe` 和 `enforce`。启用时需要配置 `REDIS_URL`、`INGRESS_IP_WINDOW_MS`、`INGRESS_IP_MAX_REQUESTS`、`INGRESS_IP_NODE_MAX_REQUESTS`、`INGRESS_IP_MAX_PENDING` 和 `INGRESS_IP_TIMEOUT_MS`。节点有界准入后，Redis 原子计数并返回窗口剩余时间。超额返回 `429 ip_rate_limited`，Redis 不可用、等待超时或节点入口饱和返回 `503`。观测模式只记录这些决策。入口配额使用独立连接，在等待超时时关闭该连接并释放待处理额度，后续请求可在已确认的主节点上重连。主备切换可能丢失尚未复制的计数。在 `enforce` 模式下，对于已同步鉴权通过且声明体积不超过 `65536` 字节的非 multipart 推理 POST 请求，Gateway 可将 IP 配额与账户共享配额合并到单次原子 Redis Lua 批量校验中（IP 校验严格先于账户配额扣除，触发前置错误时仍先执行独立 IP 配额校验并保留 `429`/`503` 语义）。

资源保护由 `INGRESS_PRESSURE_MODE` 控制，沿用相同的三个模式。启用时显式配置 `INGRESS_PRESSURE_SAMPLE_MS`、`INGRESS_PRESSURE_STALE_MS`、`INGRESS_PRESSURE_HIGH_SAMPLES`、`INGRESS_PRESSURE_RECOVERY_SAMPLES`、`INGRESS_CPU_HIGH_PERCENT`、`INGRESS_CPU_RECOVERY_PERCENT`、`INGRESS_MEMORY_HIGH_PERCENT`、`INGRESS_MEMORY_RECOVERY_PERCENT` 和 `INGRESS_MEMORY_CRITICAL_PERCENT`。从 Linux cgroup v2 采样。超限时阻止新业务准入，放行既有请求收尾。缺少有效限额时保留固定并发基线，已经触发的保护需要有效恢复样本才能解除。采样状态与拒绝原因可通过私网指标观察。

公开业务仅接受未编码或单独标记为 `identity` 的请求体，其他 `Content-Encoding` 返回 `415 unsupported_content_encoding` 及 `Accept-Encoding: identity`。`MAX_BODY_BYTES` 和 `BODY_READ_TIMEOUT_MS` 继续负责单请求上限。设置 `INGRESS_BODY_BUFFER_BYTES` 后，推理请求在读取前按请求体字节数预留节点总缓冲额度，未知长度按单请求上限预留。超时或断连时取消读取，处理结束时归还额度。流式请求在流终态时释放。预算用尽返回 `503 request_body_capacity_unavailable`。预算衡量原始请求体字节，解析和派生对象的内存由资源压力监控覆盖。

访问日志只记录已通过权限和目录检查的模型名称。超过 16 KiB 的记录仅输出时间、请求 ID、Trace ID、操作、状态和截断标记。

## 容量配比与 PostgreSQL 存储调优

### 在途并发与节点资源配比（`MAX_IN_FLIGHT`）

单节点在真实 LLM 上游时延下的吞吐上限遵循 Little's Law：`吞吐上限 ≈ MAX_IN_FLIGHT / 平均上游时延（秒）`。默认 `MAX_IN_FLIGHT=2000` 适用于短时延或低并发起步，承载真实长时延请求或长 SSE 流时必须按目标吞吐、平均上游时延和突发系数（建议 `1.25–1.5×`）配置节点数与在途上限：

| 目标集群吞吐 | 平均上游时延 / 流长 | 稳态集群总在途 | 推荐拓扑示例（每节点 `MAX_IN_FLIGHT`） | 单节点估算 RSS（基线 150 MiB + ~73 KiB/在途） | 单节点最低 FD（`ulimit -n`） |
| ------------ | ------------------- | -------------: | -------------------------------------- | --------------------------------------------: | ---------------------------: |
| 3,000 RPS    | 1 s                 |          3,000 | 2 节点 × 2,500                         |                                      ~330 MiB |                        8,192 |
| 3,000 RPS    | 5 s                 |         15,000 | 4 节点 × 5,000（或 2 节点 × 10,000）   |                       ~515 MiB（或 ~880 MiB） |                       16,384 |
| 3,000 RPS    | 30 s                |         90,000 | 8 节点 × 15,000                        |                                    ~1,245 MiB |                       65,536 |
| 3,000 RPS    | 120 s               |        360,000 | 16 节点 × 30,000                       |                                    ~2,340 MiB |                      131,072 |

- **上游连接池与临时端口**：Gateway 按 `clamp(MAX_IN_FLIGHT, 1024, 65535)` 分配上游连接池槽位。单机对同一 `(上游 IP, 端口)` 受 `ip_local_port_range` 限制（默认约 28,000 端口，建议调宽至 `10240 65535`）；单节点对单一上游并发超过 20,000 时应通过多节点或多上游 IP 分摊，并在压测时将负载发生器部署在独立主机，避免回环端口耗尽。
- **慢速请求体与多租户隔离**：所有 JSON 与 multipart 请求体读取均受 `BODY_READ_TIMEOUT_MS`（默认 `15000` ms，超时返回 `408 request_timeout`）约束，并在读取请求体之前执行节点本地每项目 `concurrency` 预检（超限立即返回 `429 rate_limit_concurrency`），防止单个持有有效密钥的慢速上传租户耗尽全局 `MAX_IN_FLIGHT` 槽位。
- **Linux 多进程端口复用**：在 Linux 生产主机上，若在同一监听端口启动多个 Gateway 进程，可显式开启 `GATEWAY_REUSE_PORT=true`（内核按连接哈希分发）。macOS XNU 的 `SO_REUSEPORT` 不会跨进程均衡分发，横向扩展应使用独立端口加前置负载均衡（如 Caddy / Envoy / Nginx）。

### PostgreSQL WAL 与 Checkpoint 调优

在保持 `fsync=on`、`synchronous_commit=on`、`full_page_writes=on` 全开的前提下，短请求每笔约产生 3.5–8.8 KiB WAL（3,000 RPS 对应约 13–26 MB/s，约 1.1–2.2 TB/天）。默认或过小的 `max_wal_size`（例如 1–4 GiB）会在持续高吞吐下每 1.5–3 分钟触发一次 WAL-driven checkpoint，引入全页写放大与刷盘尾延迟尖刺。

- 生产 PostgreSQL 17 建议配置：`max_wal_size = 32GB`（高吞吐场景 `64GB`）、`min_wal_size = 8GB`、`checkpoint_timeout = 15min`、`checkpoint_completion_target = 0.9`、`wal_compression = lz4`（或 `zstd`），并启用 `log_checkpoints = on`。
- 持续监控 `pg_stat_checkpointer` 的 `num_requested`、`write_time`、`sync_time`、`buffers_written`：稳态运行期间应以 timed checkpoint 为主，`num_requested` 接近 0，且 checkpoint 刷盘不引起 `commit_leased_batch` P99 明显劣化。

## TCP 建连突发

出口连接复用和新建连接分别管理。按已校验的物理目标 IP 与端口限制新建连接速率及并行握手，每节点默认 2000 次/秒、16 个并行握手，分别由 `UPSTREAM_DIAL_RPS` 和 `UPSTREAM_DIAL_CONCURRENCY` 控制。TLS 连接复用仍按 SNI 隔离。等待同时受首字节预算、节点活跃交换容量、`UPSTREAM_DIAL_QUEUE_WAIT_MS` 和 `UPSTREAM_DIAL_QUEUE_BYTES` 限制。队列满或耗尽出口等待预算时返回 `503 upstream_capacity` 和 `Retry-After: 1`。多个节点指向同一端点时按端点合计拨号能力分配预算。

交换槽队列与拨号队列分别应用等待时长和字节上限。推理出口准入还传入绝对截止时间，由首字节期限和本次尝试期限中较早的一项约束两个阶段。绝对请求期限先到时按未派发超时处理。

同步适配后即可确定上游目标的推理请求在持久化资金预扣前取得出口准入，先完成资金预扣，再向上游写入请求。带幂等 Key 的首次尝试先通过账本的原子重放检查，保留进行中和已完成的 409 语义。幂等路径、需要异步适配的请求和控制面出站调用在发送阶段使用同一连接调度。取消待拨号的请求立即归还队列资源，已开始的底层握手保留名额至完成或明确关闭。已确认没有发送的请求可以释放预扣，发送不确定的请求仍按既有规则进入待对账。

推理幂等指纹改为带版本前缀的规范化请求指纹。更新共享同一账本的 Gateway 时，将公网流量一起切换至支持新指纹的节点。新节点能按历史算法识别已有记录。旧节点处理已由新节点写入的幂等键时会返回冲突。

本地容量、未派发取消和单节点拨号失败不会广播渠道熔断。真正的上游 HTTP 故障和限流继续调用共享路由状态机。熔断和半开探测按代次处理，相同一轮的旧响应不会改写后继状态。

滚动更新期间，新节点继续处理旧版广播的限流时长。旧版的无代次 OPEN/CLOSED 只由旧节点处理。完成所有 Gateway 节点更新后，集群统一使用带代次的熔断事件。

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
- **PITR**。保持基础备份以及连续 WAL 归档，定期验证备份和保留窗口。事故后冻结对旧主的写入，选择目标时刻并恢复到独立实例，重放连续 WAL 到目标，核对关键交易、账本守恒、角色、消费游标、`plugin_revocations` 撤销清单与后续写入。验证通过后切换写入口。若某节点检测到数据库 `config_epoch` 回退、同 epoch 的 `lineage_token` 分叉或撤销清单回退，节点会立即置 `gateway_snapshot_diverged=1`、保留本地已撤销集合并使 `/readyz` 返回 `503 config_diverged`，待运维核验撤销记录并滚动重启后恢复。对恢复目标之后已确认的业务操作单独对账，通过原幂等键处置。 [PostgreSQL 17 PITR](https://www.postgresql.org/docs/17/continuous-archiving.html)

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
