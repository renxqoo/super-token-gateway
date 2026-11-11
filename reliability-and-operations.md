# 发布、容量与故障恢复

## 本地环境

`bun run setup:local` 生成并保护配置；`bun run infra:up` 仅使用已有 Docker 创建此项目专用 PG/Redis，或直接使用配置好的本机实例。它不安装 Docker、不清空共享服务。开发库与测试库隔离；生产禁止运行 mock seed。

开发端口：Gateway 8088、独立 mock 9088、PG 55442、准入 Redis 56382、耐久队列 Redis 56383、worker health 8099。`infra/compose.yaml` 是本地依赖配置，不是跨主机 HA 集群。不要运行 `down -v` 删除仍有用途的 usage 证据。

## 生产发布

1. 使用 Bun 1.4.4 和锁文件执行 `bun install --frozen-lockfile`、`bun run verify`，生成 dist 与 manifest；保留上一个可回滚版本。
2. 准备专用 PG、准入 Redis、耐久队列 Redis、数据库迁移 owner 和两个独立运行登录账号。用迁移账号运行 `bun dist/tools/migrate.js`，再执行 `infra/runtime-role.sql` 把 `gateway_runtime` 授予 HTTP 账号、`gateway_worker` 授予独立 worker 账号。`MIGRATION_DATABASE_URL` 可独立于 `DATABASE_URL`。
3. 发布 dist 到 `/opt/llm-gateway/releases/<版本>/`，将 current 指向该目录。进程必须只读，密钥由 `/etc/llm-gateway/<实例>.env` 或外部 secret manager 注入，文件权限 0600，禁止打入镜像。
4. 安装 `infra/gateway@.service` 和必需的 `gateway-worker.service`，使用独立低权限用户、Bun 的绝对路径。实例 1/2 分别监听 127.0.0.1:8088/8089，共享同一 Gateway DB、Redis namespace、KEY_PEPPER 与 ENCRYPTION_KEY。
5. 配置 `NODE_ENV=production`、`ALLOW_PRIVATE_UPSTREAM=false`、已验证的 PG/Redis TLS 连接及公网 HTTPS 供应商地址。配置业务系统与 Gateway 的独立访问凭证。
6. 修改 `infra/Caddyfile` 的真实域名和实例地址，运行 `caddy validate`，切入已通过 `/readyz` 的新实例，再逐个 SIGTERM 旧实例。公开路由只代理 /v1 和 /v1beta；管理面不暴露公网。

可用 `docker build -f infra/Dockerfile -t <镜像> .` 构建另一种发布成品。运行时建议 `--read-only --tmpfs /tmp:size=64m --cap-drop ALL --security-opt no-new-privileges`、非 root 和内存限制；使用 `bun run test:container` 验证真实依赖、独立 worker、JSON/SSE、精确账单与 SIGTERM。镜像包含 Bun 1.4.4 与独立 bundle，不需要生产安装 node_modules。

只运行当前单一 schema 与队列契约。初始化接受空库或精确当前版本；schema2026111006 / 2026111007 / 2026111008 / 2026111009 / 2026111010 / 2026111101 / 2026111102 通过显式 `db:migrate -- --upgrade --quiesced` 离线转换到 2026111201。执行前停止所有客户端、排空旧证据作业并备份，具体步骤及恢复限制见 database.md。应用修订回退必须使用同一数据库契约并保留已确认队列、账本及费率证据。

## 高可用依赖

应用层可多实例运行，支持共享幂等、分布式并发和恢复租约；本次双实例 HTTP 测试已验证只分派一次。实际 PG 主从/仲裁、Redis 故障转移和跨机房可用性由部署平台提供，不能用本地单容器证明。

PG 使用高可用主端点、同步耐久提交、备份与 PITR。准入 Redis 的丢失会重置部分速率窗口；它不保存权威资金。

耐久队列 Redis 保存尚未入 PG 的证据，必须独立部署：Redis ≥7.2、appendonly=yes、appendfsync=always、no-appendfsync-on-rewrite=no、aof-load-truncated=no、maxmemory-policy=noeviction。生产 QUEUE_REQUIRED_REPLICAS 至少 1，且等于所有可晋升副本数；客户端启动及准入探针检查 Redis 主角色、online/connected 副本数与配置完全一致，并重查持久化配置。所有连接副本都按可晋升候选管理，所有候选均须 AOF 落盘确认。成员变动需先停止准入、排空/核对、重新配置确认数，禁止自动降为 0。生产推荐独立主节点与两个 AOF 副本、三个故障域及外部仲裁/隔离。

同连接 WAITAOF 证明此前写入达到指定落盘副本数量，不提供任意故障下的强一致性。晋升必须隔离旧主，并只选择未丢失已确认偏移的落盘副本；无法证明则停止准入，不能以可用性为由晋升落后的节点。不要从空白主库启动复制覆盖存量副本。当前客户端通过稳定故障转移端点或代理连接，不直接发现 Sentinel。参见 [Redis 官方 WAITAOF 保证](https://redis.io/docs/latest/commands/waitaof/)。

本地 `bun run test:durability` 默认使用独立临时Docker主副本与AOF卷；当前Mac可加 `-- --native` 使用本轮拥有的redis-server子进程及独立AOF目录，控制前核对进程PID。两者使用相同临时PG及断言：事务未提交/已提交未 ACK 时 SIGKILL、两节点硬重启、在主库保持停机时独立恢复副本并晋升、缺副本/错误持久化配置拒绝准入。它不模拟机房掉电或仲裁失效。

UPSTREAM_POOL_SIZE 默认1024，限制每种协议同时持有连接的请求数；共享空闲连接另有 min(512, floor(UPSTREAM_POOL_SIZE/2)) 的上限，30秒无使用则回收。等待连接的请求可被取消，取消发生在获得连接前时记录明确未执行。停机时拒绝等待请求并销毁连接。连接耗尽指标采用固定错误类别；本机端口或文件描述符不足且尚未取得响应时，记录明确未执行、释放预扣并返回503，不触发供应商熔断。

DB_POOL_SIZE 默认24；多个实例/worker 的连接总数必须低于 PG 预算并留出迁移/运维余量。容量测试单独记录各进程连接总量。增加实例前先确认 PG WAL/磁盘、Redis CPU、上游限制与负载发生器都没有成为瓶颈。

准入与熔断脚本优先使用 EVALSHA，减少每个请求重复发送 Lua 正文。只有 Redis 明确返回 NOSCRIPT 时才在同一命令期限内发送脚本；超时、断连和执行结果未知继续关闭本次准入。脚本缓存被清理后按同一规则恢复。

TPM 桶在首次创建时设置固定窗口到期时间，已有桶的零 token 预留只读取现有计数。后续预留仍累加精确计数，最终调整仍作用于原桶，减少重复的零值写入及相同到期时间写入。

RPM 和 active 集合按剩余 TTL 合并过期时间写入，保留至少完整滚动窗口及租约预算，空闲键最多额外保留 30 秒。短租约准入保留已有长租约的集合寿命。实际准入仍按 Redis TIME 和成员 score 清理过期项。成功健康观察在一次 HSET 中更新失败数、延迟和熔断时间。

QUEUE_BATCH_DELAY_MS 允许 0 至 10 毫秒，缺省 1 毫秒。容量测试记录实际窗口、作业大小、worker 并发和数据库合批上限。

TPM 是按输入字节/输出上限保守估算的准入，完成后用最终 token 事实修正；多模态、远端文件与异步任务不能依靠它获得精确财务上限，应配合供应商配额和项目并发。财务硬约束由 PG 预扣与钱包承担，不能由技术窗口代替。

## 智能路由与尝试预算

模型缺省使用 WEIGHTED。候选需匹配模型与操作，模型、路由、供应商均启用，并通过共享供应商熔断检查。先按 priority 从高到低排序，同层的 WEIGHTED 按权重随机排序，ROUND_ROBIN 通过共享 Redis 计数轮换，LEAST_LATENCY 按供应商共享延迟升序选择，延迟相同时优先较大权重。延迟样本以旧值 80%、新值 20% 平滑。路由和模型偏好缓存 1 秒；派发前仍在 PG 校验权限、凭证有效期及渠道硬配置。

MAX_ATTEMPTS 缺省 3，允许 1 至 5，整次逻辑请求共享物理尝试预算。候选未通过供应商限流或探测准入时可以继续找其他候选；实际尝试预算及总循环次数都有界。TOTAL_TIMEOUT_MS 缺省 180,000 ms，HEADERS_TIMEOUT_MS 与 STREAM_IDLE_MS 缺省 30,000 ms。

HTTP 408、409、429、500、502、503、504、529 允许重试或切换，401、403、404 也允许尝试其他渠道。已识别的 400、422 终止本次调用。连接拒绝、头部超时、坏 JSON 及首个有效流事件前失败，可以在尚未交付结果时切换。已交付流内容、完整结果的本地终结失败，以及本机连接资源耗尽，按各自证据结束本次请求。发生重试时保留每个物理 attempt 的独立事实。

同供应商再尝试时同时遵守 Retry-After 的数字秒或 HTTP 日期，以及 30 ms 起的指数退避与最多 30 ms 随机抖动。单次等待上限一小时，整次请求仍受总期限及客户端取消约束，等待不增加尝试预算。连续三次有效健康失败开路 10 秒；冷却后共享状态只允许一个半开探测，成功恢复闭合，失败重新开路。

客户账单采用最终选中 attempt 的原始客户费率版本，所有其他物理调用的上游成本独立保留。全部尝试明确未执行时释放预扣；结果未知或缺少收费维度时继续保留预扣，等待可信补证或显式人工对账。多渠道边界与金额回归通过 `bun --env-file=.env.local test tests/integration/routing.test.ts --timeout 30000` 执行。

## 故障行为

推理请求取得熔断准入时绑定本次观察身份与健康代次。首次准入及熔断重开使用唯一身份设置代次，健康记录过期后重新初始化也获得新代次。正常闭合状态下开始的调用只更新同代次且仍处于闭合状态的健康记录；半开探测还要求当前 token 且租约有效。获取半开租约时延长健康记录寿命，覆盖完整租约及健康保留窗口。租约过期、被替换或健康代次改变后，迟到观察保留该物理调用的数字证据与资金结果。有效探测提交健康结果后释放探测身份，其他实例共享相同准入状态。

| 故障                             | 行为与处置                                                                                         |
| -------------------------------- | -------------------------------------------------------------------------------------------------- |
| PG 无法准入/worker 提交          | 准入失败不发上游；worker 失败保留队列作业与预扣，重试并告警；恢复后幂等结算                        |
| 准入 Redis 断连/黑洞             | 准入失败关闭，命令1秒内返回503；连接重建后新请求恢复，不自动重放可能已执行的 Lua                   |
| 上游超时/5xx/输出前失败          | 有限切换/重试；保留每个物理尝试及未知成本                                                          |
| 输出后断流/取消                  | 不透明切换；提交 PARTIAL/UNKNOWN；终止上游并回收租约                                               |
| 供应商已有最终证据、原流随后断开 | 保留 FINAL 优先，不允许后来部分证据降级计量                                                        |
| SIGTERM                          | readiness关闭，等待宽限期，再取消在途；最多30秒清理，等待计量后关闭DB/Redis                        |
| SIGKILL                          | 数据库保留分派意图；重启按过期阈值追加 UNKNOWN；不重放输入                                         |
| 异步状态完成但缺usage            | PENDING_USAGE，指数退避最多1小时继续补证，不能当作零成本                                           |
| Batch 缺根聚合usage              | 纯内存逐行扫描上游结果文件，只提取数字；每文件256MiB、50,000行、每行受JSON预算限制；超界保留待补证 |
| 客户端下载停滞                   | 有背压队列及停滞/总时限；取消上游，正文不落盘                                                      |

恢复 worker 有界循环认领小批资源，主进程也每10秒恢复；数据库租约避免多实例重复认领。poll失败退避，全部并发任务完成后才结束本轮。若供应商没有查询/补证能力，UNKNOWN 长期保留并进入运营对账，不自动免收、不自行生成最终证据。

## 监控与告警

私网 `GET /admin/metrics` 提供固定基数计数器、请求时长直方图、在途量、原始缓冲量、进程 RSS 与 CPU。至少告警：readiness失败、usage_queue_failures_total、usage_consumer_errors_total、`GET /admin/queue` 的 failed/waiting/active 队列数量（准入门控 QUEUE_MAX_PENDING_JOBS 统计 waiting/active/delayed；failed 作业保留待人工重试，单独按同一上限计数，未达上限不关闭全部租户准入）、用量入库延迟、持续UNKNOWN/PENDING_USAGE、恢复/事件积压、429/503、RSS/缓冲预算、PG连接/锁/WAL/磁盘、Redis内存与延迟。

`billing_settled_total` 等消费回调计数描述完成作业的返回结果。作业内部分提交、忙项重试和已提交作业重投可能使回调时间晚于实际 PG 提交，窗口内账务验收使用已提交收据及实际 ACK。

`scripts/bench-system.ts --online-capacity --seconds 60 --upstream precomputed --rate 6150 --target-rps 6000` 使用预序列化无延迟 Chat 上游，保留在线 worker、资金预扣和外部消费。需提供测试 owner 的 DATABASE_URL、准入 Redis、独立的 AOF always 证据主节点及落盘副本，设置 QUEUE_REQUIRED_REPLICAS=1。该入口创建并清理独立测试数据库和命名空间；REDIS_EXTRA_URLS 的所有端点参与清理。完整命令与本机配置、结果见 validation.md。

严格窗口验收按 request ID 求有效 HTTP 完成与已确认收据 ACK 的交集。每页 ACK 前逐对核对 request_id、evidence_id、数字用量及金额，最后再核对全量预扣、扣款、流水与队列。`--record-request-ids` 将技术身份和完成时刻写入有背压的TSV文件；外部ACK单独记录，运行后用64MiB排序预算逐行合并核对。严格在线验收支持60秒的整数倍、最长3600秒，每分钟完成交集和对应发出窗口的全部响应延迟均须达标，尾批包含在延迟预算内；普通吞吐测试使用固定大小直方图。

不得在网关、代理、APM 或错误采集中记录请求/响应正文或认证头；不要启用响应缓存、body dump、磁盘请求缓冲或 swap/core dump 内容采集。技术ID和数字计量可保留。S3并非本 Gateway 的输出存储依赖。

## 备份与恢复

PG 与耐久队列共同构成恢复集。PG 单独备份不包含已经落盘确认但未入 PG 的证据。计划备份应停止所有 HTTP 准入、等待所有生产者退出、队列 waiting/active/delayed/failed 为零，核对 PG 预扣与证据后，再使用一致性快照进行 pg_dump/WAL 备份。未知执行结果的预扣可以保留；不要自动释放。若无法排空，必须保全对应队列 AOF 及复制状态，与 PG 协调恢复并通过幂等消费重放。

恢复到全新数据库和同一当前队列契约，核对 schema、usage、钱包/资金批次/预扣/账单、平衡 journal 和不可变触发器，并重新执行 infra/runtime-role.sql；备份不保存 ACL。加密密钥与 KEY_PEPPER 独立备份。确认无证据缺口后再恢复准入。不要删除 failed 作业来让备份门禁通过。

本地 `bun run test:backup -- --quiesced` 只针对专用 PG，要求先停准入并排空当前 namespace，生成 0600 PG 备份、恢复到随机临时库并校验。备份在 `.local/backups/`，不进入 Git。

供应商 API Key 通过管理 PATCH 轮换；master ENCRYPTION_KEY 不可直接覆盖，需先受控迁移全部密文并保留恢复窗口。KEY_PEPPER 改变后旧 Key verifier 无法使用，需要先发放新凭证并完成迁移。

当前没有提供目标服务器，因此未执行公网发布、跨主机故障转移或10,000 RPS持续60分钟集群验收。当前版本的6000 RPS及持续验收状态见 validation.md；不得将单机短测称为完整生产验收。

## 本地容量配置

Compose 为当前约12GiB Docker VM提供2GiB shared_buffers、6GiB effective_cache_size、4GiB max_wal_size 和15分钟checkpoint配置，保留fsync、full_page_writes、synchronous_commit=on。这些是本地测量配置；其他主机应按实际内存、磁盘、连接与WAL保留预算调整，不能直接照搬为通用最优值。参见[PG17内存说明](https://www.postgresql.org/docs/17/runtime-config-resource.html)与[WAL说明](https://www.postgresql.org/docs/17/wal-configuration.html)。

## 本地多副本演练与热点验收

`docker compose --env-file .env.local -p super-token-gateway -f infra/compose.yaml -f infra/compose.ha.yaml up -d --wait` 启动本地两个独立 AOF 副本；以 `QUEUE_REQUIRED_REPLICAS=2` 验证三节点写入。该 overlay 是同机演练模板；生产仍需独立故障域、稳定端点、旧主隔离和明确的晋升控制，不能由本地 Compose 推导99.99% SLO。

完整账务压测支持 `--wallets 1/10/1000` 与 `--hot-share 0.9`，报告每钱包负载、PG 锁等待、活动查询和停压排空。使用 `QUEUE_REQUIRED_REPLICAS=1` 或目标生产副本数实测，不以零副本成绩代替生产容量。修改 schema/队列后重新执行 `verify`、`test:durability`、`test:container`，备份恢复需在停止准入并排空队列后执行。

使用冷 tablespace 的生产恢复需先准备对应 tablespace 或显式用 pg_restore 的 --no-tablespaces 恢复到目标默认存储，再核对数据与权限。原生 PG 工具的本地备份复验可通过 PG_TOOLS_DIRECTORY 指定同主版本工具目录；密码经子进程环境传递，不放入命令行或输出。

## 当前容量验收入口

完整口径见 [容量契约](docs/performance/capacity-contract.md)。`scripts/bench-system.ts --online-capacity` 支持60秒整数倍窗口，`--max-p99-ms`默认1500，只能收紧，超过1500的参数直接拒绝且验收判定不通过；全量和每个发出分钟的尾批P99均进入门槛。默认按5000字节/计划请求另加4GiB实验空间、4GiB系统保留量做磁盘预检，长测空间不足时在创建数据库前拒绝。该估计来自历史数量级，不是磁盘占用上限；测试期间继续检查剩余空间，不足时中止并判失败。不要通过关闭耐久性或删除权威身份降低空间预算。
