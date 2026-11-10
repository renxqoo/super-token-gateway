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

只运行当前单一 schema 与队列契约，不保留旧代码或旧库兼容。初始化只接受空库或精确当前版本；旧数据库不修改，需以新的专用库启动。应用修订回退必须使用同一当前契约；不可切回已经删除的 PG usage/outbox 路径，不删除已确认队列、账本或费率证据。

## 高可用依赖

应用层可多实例运行，支持共享幂等、分布式并发和恢复租约；本次双实例 HTTP 测试已验证只分派一次。实际 PG 主从/仲裁、Redis 故障转移和跨机房可用性由部署平台提供，不能用本地单容器证明。

PG 使用高可用主端点、同步耐久提交、备份与 PITR。准入 Redis 的丢失会重置部分速率窗口；它不保存权威资金。

耐久队列 Redis 保存尚未入 PG 的证据，必须独立部署：Redis ≥7.2、appendonly=yes、appendfsync=always、no-appendfsync-on-rewrite=no、aof-load-truncated=no、maxmemory-policy=noeviction。生产 QUEUE_REQUIRED_REPLICAS 至少 1；应等于所有可晋升副本数，所有候选均须 AOF 落盘确认。成员变动需先停止准入、排空/核对、重新配置确认数，禁止自动降为 0。生产推荐独立主节点与两个 AOF 副本、三个故障域及外部仲裁/隔离。

同连接 WAITAOF 证明此前写入达到指定落盘副本数量，不提供任意故障下的强一致性。晋升必须隔离旧主，并只选择未丢失已确认偏移的落盘副本；无法证明则停止准入，不能以可用性为由晋升落后的节点。不要从空白主库启动复制覆盖存量副本。当前客户端通过稳定故障转移端点或代理连接，不直接发现 Sentinel。参见 [Redis 官方 WAITAOF 保证](https://redis.io/docs/latest/commands/waitaof/)。

本地 `bun run test:durability` 使用独立临时主副本、各自 AOF 卷和临时 PG：事务未提交/已提交未 ACK 时 SIGKILL、两节点硬重启、在主库保持停机时独立恢复副本并晋升、缺副本/错误持久化配置拒绝准入。它不模拟机房掉电或仲裁失效。

DB_POOL_SIZE 默认24；多个实例/worker 的连接总数必须低于 PG 预算并留出迁移/运维余量。容量测试单独记录各进程连接总量。增加实例前先确认 PG WAL/磁盘、Redis CPU、上游限制与负载发生器都没有成为瓶颈。

TPM 是按输入字节/输出上限保守估算的准入，完成后用最终 token 事实修正；多模态、远端文件与异步任务不能依靠它获得精确财务上限，应配合供应商配额和项目并发。财务硬约束由 PG 预扣与钱包承担，不能由技术窗口代替。

## 故障行为

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

私网 `GET /admin/metrics` 提供固定基数计数器、请求时长直方图、在途量、原始缓冲量、进程 RSS 与 CPU。至少告警：readiness失败、usage_queue_failures_total、usage_consumer_errors_total、failed/waiting/active 队列数量、用量入库延迟、持续UNKNOWN/PENDING_USAGE、恢复/事件积压、429/503、RSS/缓冲预算、PG连接/锁/WAL/磁盘、Redis内存与延迟。

不得在网关、代理、APM 或错误采集中记录请求/响应正文或认证头；不要启用响应缓存、body dump、磁盘请求缓冲或 swap/core dump 内容采集。技术ID和数字计量可保留。S3并非本 Gateway 的输出存储依赖。

## 备份与恢复

PG 与耐久队列共同构成恢复集。PG 单独备份不包含已经落盘确认但未入 PG 的证据。计划备份应停止所有 HTTP 准入、等待所有生产者退出、队列 waiting/active/delayed/failed 为零，核对 PG 预扣与证据后，再使用一致性快照进行 pg_dump/WAL 备份。未知执行结果的预扣可以保留；不要自动释放。若无法排空，必须保全对应队列 AOF 及复制状态，与 PG 协调恢复并通过幂等消费重放。

恢复到全新数据库和同一当前队列契约，核对 schema、usage、钱包/资金批次/预扣/账单、平衡 journal 和不可变触发器，并重新执行 infra/runtime-role.sql；备份不保存 ACL。加密密钥与 KEY_PEPPER 独立备份。确认无证据缺口后再恢复准入。不要删除 failed 作业来让备份门禁通过。

本地 `bun run test:backup -- --quiesced` 只针对专用 PG，要求先停准入并排空当前 namespace，生成 0600 PG 备份、恢复到随机临时库并校验。备份在 `.local/backups/`，不进入 Git。

供应商 API Key 通过管理 PATCH 轮换；master ENCRYPTION_KEY 不可直接覆盖，需先受控迁移全部密文并保留恢复窗口。KEY_PEPPER 改变后旧 Key verifier 无法使用，需要先发放新凭证并完成迁移。

当前没有提供目标服务器，因此未执行公网发布、跨主机故障转移或10,000 RPS持续60分钟集群验收。当前吞吐仍未达到目标，验收进度见 validation.md；不得将单机短测称为完整生产验收。

## 本地容量配置

Compose 为当前约12GiB Docker VM提供2GiB shared_buffers、6GiB effective_cache_size、4GiB max_wal_size 和15分钟checkpoint配置，保留fsync、full_page_writes、synchronous_commit=on。这些是本地测量配置；其他主机应按实际内存、磁盘、连接与WAL保留预算调整，不能直接照搬为通用最优值。参见[PG17内存说明](https://www.postgresql.org/docs/17/runtime-config-resource.html)与[WAL说明](https://www.postgresql.org/docs/17/wal-configuration.html)。
