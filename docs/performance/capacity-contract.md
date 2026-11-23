# 容量及对标契约

本文档定义 `super-token-gateway` 当前 v4 架构（`apps/gateway` + `apps/worker` + PostgreSQL 17 + 可选 Redis 7.4）的容量验收标准、强耐久边界、可复现压测入口与外部对标口径。历史三进程执行 Core、`scripts/bench-system.ts` 与 Redis AOF 证据队列（`QUEUE_REDIS_URL` / `QUEUE_REQUIRED_REPLICAS`）已随 v4 架构演进退役，历史报告见同目录按日期归档的文档，不再作为当前架构的达标依据。

## 架构与强耐久边界

1. **PostgreSQL 17 强耐久账本**：
   - `fsync = on`、`synchronous_commit = on`、`full_page_writes = on` 始终开启；禁止为压测成绩放宽耐久参数。
   - 生产高可用与主备验收启用物理流式同步备库（`synchronous_standby_names` 非空且 `pg_stat_replication.sync_state = 'sync'`），事务提交在主库与同步备库均落盘后返回。
   - 发送上游前必须持久化预扣（`billing_holds`）；终态在同一数据库事务内原子提交客户扣款与供应成本（`billing_charges`）、借贷平衡流水（`ledger_entries`）及 16 分片外发事件（`outbox_events`）。
   - 压测结束后必须满足：`auditConservation().ok === true`（`unbalanced_journals = 0`、`broken_lots = 0`、`wallet_mismatches = 0`、`reservation_mismatches = 0`、`open_holds = 0`、`unknown_holds = 0`）、`duplicate_request_charges = 0`、`active_leases_after_idle = 0`、`post_drain_in_flight = 0`。
2. **Redis 7.4 职责边界**：
   - 仅承担多网关集群共享限流（RPM / TPM / 并发租约）、熔断状态广播与配置 epoch 推送；不承担资金证据队列职责。
   - `--no-redis` 仅用于单节点或无跨节点共享限流场景的对照测量，报告中必须明确标注 `redis_enabled: false`。
3. **资源保护与在途配比**：
   - 网关在读取请求体前即执行单节点每项目并发保护（`429 rate_limit_concurrency`），并对 JSON 与 multipart 请求体读取强制执行有界超时（`BODY_READ_TIMEOUT_MS`，默认 `15000` ms，超时返回 `408 request_timeout`）。
   - 真实上游时延下的吞吐上限受 `MAX_IN_FLIGHT / 平均上游时延` 约束，详细的内存、FD、端口与 `MAX_IN_FLIGHT` 规划见 [生产运维决策](../decisions/0011-production-operations.md)。

## 标准压测入口与溯源字段

仓库内所有正式基准均从独立构建产物 `dist/` 启动（`bun run build`），并在 JSON 报告中强制输出：

- `provenance`：`git_commit`、`dist_manifest_sha256`、`bun_version`、`host`（CPU 型号/核数、内存、OS/arch）、`pg_version`、`pg_settings`（`fsync`、`synchronous_commit`、`full_page_writes`、`shared_buffers`、`max_wal_size`、`checkpoint_completion_target`、`checkpoint_timeout`、`synchronous_standby_names`）。
- `pg_checkpointer_delta`：压测前后 `pg_stat_checkpointer` 差分（`num_timed`、`num_requested`、`write_time_ms`、`sync_time_ms`、`buffers_written`）。

### 脚本与参数

| 脚本 / 命令                                             | 适用场景                                                                                                       | 关键环境变量 / 标志                                                                                                                                                                                                          |
| ------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `bun run bench:single`（`scripts/bench-single.ts`）     | 单网关节点极限吞吐、真实上游时延、HTTPS TLS + DNS、访问日志开销                                                | `BENCH_PROJECTS`、`BENCH_JSON_CONCURRENCY`、`BENCH_SECONDS`、`BENCH_MAX_IN_FLIGHT`、`BENCH_UPSTREAM_DELAY_MS`、`BENCH_UPSTREAM_TLS`、`BENCH_ACCESS_LOG`、`BENCH_GW_LOG`、`BENCH_FAST_CLIENT`                                 |
| `bun run bench:prod --limit`（`scripts/bench-prod.ts`） | 多网关 + Worker + Redis 7.4 + PG 17 极限吞吐与同步备库压测                                                     | `BENCH_GATEWAY_NODES`（1–8）、`BENCH_MOCK_NODES`（1–8）、`BENCH_PROJECTS`（1–5000）、`BENCH_JSON_CONCURRENCY`、`BENCH_LIMIT_SECONDS`、`BENCH_TENANT_AFFINITY`、`BENCH_SYNC_STANDBY`、`BENCH_VERIFY_OUTBOX_ACK`、`--no-redis` |
| `bun run bench:prod --mixed-soak`                       | 固定到达率混合负载（JSON 50% + SSE 25% + Wasm 插件 12.5% + Webhook 插件 12.5%）与逐笔账务/余额/Outbox ACK 验收 | `BENCH_PROJECTS=1000`、`BENCH_SOAK_SECONDS`、`BENCH_SOAK_RPS=3000`、`BENCH_SOAK_CONCURRENCY`、`BENCH_VERIFY_OUTBOX_ACK=1`、`BENCH_SYNC_STANDBY`                                                                              |
| `bun run bench:prod --sse-burst` / `--sse-burst-fresh`  | 高并发 SSE 突发、客户端取消、Redis 并发租约回收与活跃租约归零                                                  | 需要 Redis，自动校验 `stg_bench:rl:conc:*` 归零                                                                                                                                                                              |

## 验收门槛

### 1. 均匀 1000 钱包混合固定到达验收（3000 RPS）

- 负载：均匀 1000 钱包（`BENCH_PROJECTS=1000`）、正费率混合工作负载（JSON + SSE + Wasm + Webhook）、外部消费者在线调用 `/internal/v1/events/claim` 与 `/internal/v1/events/ack`（`BENCH_VERIFY_OUTBOX_ACK=1`）。
- 门槛：
  - 目标到达率 `≥ 3000 RPS`，`errors = 0`，`unsent = 0`。
  - 无延迟上游下端到端 `P99 ≤ 1500 ms`（含调度滞后与服务耗时）。
  - `exact_ledger` 全部模型 `wrong_atoms_or_usage = 0`、`wrong_plugin = 0`；`exact_balances` 三项偏差均为 `0`；`conservation.ok = true`。
  - `outbox_ack.acked_events` 覆盖全部结算事件且 `backlog_after_drain = 0`。
  - 停发后活跃租约在 `5000 ms` 预算内归零，整轮排空在 `120 s` 内完成。
- 60 分钟持续验收：在独立压测环境先连续两轮通过 60 s 同口径验收，再执行 `BENCH_SOAK_SECONDS=3600`；运行前按实测 WAL 速率（约 3.7–4.3 KiB/req）预检磁盘余量，并全量记录每分钟窗口与 `pg_checkpointer_delta`。禁止用缩短时长替代 60 分钟长测结论。

### 2. 真实上游时延与长流验收

- 真实 LLM 上游（平均时延 1–30 s，长流 5 / 30 / 120 s）容量按 `节点数 × MAX_IN_FLIGHT / 平均上游时延` 规划；零时延上游 RPS 仅反映网关与账本控制面极限，不得直接外推为长流在途容量。
- 单机发生器受 `ulimit -n`、`kern.ipc.somaxconn` 与临时端口数限制；6000 以上并发在途验收必须在多机分离部署发生器、网关与数据库后执行。

### 3. 外部项目对标规则

- 外部对标必须如实记录测试硬件、版本、产物哈希与契约差异；若对标对象不具备强耐久预扣、复式账本、Outbox 或跨节点 Redis 限流，必须在表中显式单列契约差异，禁止隐瞒功能差距直接对比。

## 2026-11-23 实测基线与对标结果

### 环境溯源

- 硬件与 OS：Apple M6（12 核，24 GiB RAM），macOS `darwin arm64`（27.0.0）
- 运行时与数据层：Bun `1.4.5`、PostgreSQL `17.11`（`fsync=on`、`synchronous_commit=on`、`full_page_writes=on`、`shared_buffers=2GB`、`max_wal_size=4GB`、`checkpoint_completion_target=0.9`）、Redis `7.4`
- 资金核验：下表所有本轮实测均满足 `total_errors = 0`、`duplicate_request_charges = 0`、`conservation.ok = true`、`active_leases_after_idle = 0`、`post_drain_in_flight = 0`

### 优化前后吞吐与延迟对比（零时延短 JSON，32 钱包，全量 PG 17 强耐久账本）

| 场景                                            | `1360133` 历史基线 | `fca0b08` 优化前 |              当前版本实测 | 延迟（P50 / P99）                 | WAL / req | 备注                                                 |
| ----------------------------------------------- | -----------------: | ---------------: | ------------------------: | --------------------------------- | --------: | ---------------------------------------------------- |
| 1 节点 @ 32c                                    |                  — |      4,365.0 RPS | **12,679.1–13,866.1 RPS** | 2.25–2.44 ms / 3.93–4.33 ms       |   5,421 B | 低并发延迟降幅 3×                                    |
| 1 节点 @ 128c                                   |                  — |                — | **19,363.0–22,654.5 RPS** | 5.53–5.93 ms / 9.68–15.63 ms      |   4,641 B | 单节点 RSS ~215 MB                                   |
| 1 节点 @ 512c                                   |                  — |     23,001.7 RPS | **25,366.1–26,924.0 RPS** | 18.55–19.49 ms / 27.75–29.73 ms   |   3,836 B |                                                      |
| 1 节点 @ 1024c                                  |       14,214.7 RPS |     22,793.1 RPS | **26,601.8–29,111.2 RPS** | 34.26–37.41 ms / 48.35–54.60 ms   |   4,087 B | 较 `1360133` 提升 **2.0×**                           |
| 2 节点 + Redis 7.4 @ 1024c                      |  12,180–16,302 RPS |     30,001.7 RPS |          **32,931.8 RPS** | 30.56 ms / 43.26 ms               |   3,905 B | 较 `1360133` 提升 **2.0–2.7×**                       |
| 2 节点 + Redis 7.4 @ 2048c                      |                  — |                — |          **34,065.6 RPS** | 58.82 ms / 77.31 ms               |   3,745 B | 504.5 MB 双节点总 RSS                                |
| 3 节点 + Redis 7.4 @ 2048c                      |                  — |                — |          **37,514.4 RPS** | 53.05 ms / 72.61 ms               |   3,856 B | 轮询分发，0 错误，全量守恒                           |
| 3 节点 + Redis 7.4 @ 3072c                      |                  — |                — | **37,526.2–37,927.6 RPS** | 78.39–79.15 ms / 105.53–105.88 ms |   3,762 B | 轮询分发，0 错误，全量守恒                           |
| 3 节点 `--no-redis` @ 2048c–3072c               |                  — |                — | **40,257.1–41,715.3 RPS** | 49.48–70.87 ms / 71.05–98.86 ms   |   3,758 B | PG 17 强耐久账本全开                                 |
| 2 节点 + Redis + **PG 17 物理同步备库** @ 1024c |                  — |                — |          **27,969.4 RPS** | 35.81 ms / 51.73 ms               |   3,918 B | `synchronous_standby_names='*'`, `sync_state='sync'` |

### 与 `agentgateway` 公开性能数据对标（契约透明对比）

| 项目 / 形态                                          |     吞吐（RPS / QPS） | 持久预扣 & 复式账本    | Outbox 事件       | 跨节点 Redis 限流   | 说明                                                                |
| ---------------------------------------------------- | --------------------: | ---------------------- | ----------------- | ------------------- | ------------------------------------------------------------------- |
| `agentgateway`（公开基线 @ 32c，1 KiB mock JSON）    |            36,933 QPS | 否                     | 否                | 否                  | 多核 Rust 纯代理，无数据库与账务持久化                              |
| `agentgateway`（公开基线 @ 512c）                    |            30,000 QPS | 否                     | 否                | 否                  | 多核 Rust 纯代理，无数据库与账务持久化                              |
| `super-token-gateway`（3 节点 + Redis 7.4 + PG 17）  | **37,514–37,928 RPS** | **是（PG 17 强耐久）** | **是（16 分片）** | **是（Redis 7.4）** | 全量金融账本与集群限流下仍超越 `agentgateway` 峰值                  |
| `super-token-gateway`（3 节点 `--no-redis` + PG 17） | **40,257–41,715 RPS** | **是（PG 17 强耐久）** | **是（16 分片）** | 否（本地限流）      | 较 `agentgateway` 32c 峰值高 **+9%–+13%**，较 512c 高 **+34%–+39%** |

### 热路径专项与契约功能验证实测（2026-11-23）

| 专项验证                                        | 命令参数摘要                                                                                                                 | 实测结果                                                                                                                                           | 核验状态                                       |
| ----------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------- |
| 1000 钱包 3000 RPS 混合 + 在线 Outbox ACK（F4） | `BENCH_PROJECTS=1000 BENCH_GATEWAY_NODES=2 BENCH_SOAK_SECONDS=15 BENCH_SOAK_RPS=3000 BENCH_VERIFY_OUTBOX_ACK=1 --mixed-soak` | `45,000/45,000` 完成，0 错误，0 漏发，**P99 56 ms**（JSON 47 ms / SSE 66 ms / Wasm 48 ms / Webhook 66 ms），`outbox_ack = 45,200`（`backlog = 0`） | 全部通过（短窗诊断；60 min 长测需独立环境）    |
| 真实上游 1000 ms 时延（F2）                     | `BENCH_UPSTREAM_DELAY_MS=1000 BENCH_MAX_IN_FLIGHT=4000 BENCH_JSON_CONCURRENCY=1000,2000`                                     | 1000c：**949.2 RPS**（P50 1003.6 ms，P99 1042.6 ms）；2000c：**1,807.2 RPS**（P50 1002.45 ms，P99 1023.1 ms，RSS 237.3 MB）                        | 全部通过，控制面增量开销 `< 4 ms`              |
| HTTPS TLS 复用池 + `localhost` DNS 缓存（F7）   | `BENCH_UPSTREAM_TLS=1 BENCH_JSON_CONCURRENCY=128,512`                                                                        | 128c：**20,307.4 RPS**（P99 14.34 ms）；512c：**25,123.7 RPS**（P50 19.96 ms，P99 28.31 ms，RSS 248.9 MB）                                         | 全部通过，达到明文 HTTP 同档吞吐的 **~95–99%** |
| 访问日志落盘（F7）                              | `BENCH_ACCESS_LOG=1 BENCH_GW_LOG=... BENCH_JSON_CONCURRENCY=512`                                                             | 512c：**23,996.4 RPS**（P50 20.70 ms，P99 34.08 ms）                                                                                               | 全部通过，开销约 5–8%                          |
| 租户亲和路由对照（3 节点）                      | `BENCH_TENANT_AFFINITY=1 BENCH_GATEWAY_NODES=3` @ 1024c                                                                      | +Redis：**30,000.7 RPS**（P99 **59.96 ms**，较轮询 97.95 ms 降低 39%）；`--no-redis`：**35,469.5 RPS**（P99 **49.66 ms**）                         | 全部通过，显著降低跨节点行锁尾延迟             |

## 未执行项与局限

- 本机单机 12 核同时运行全部网关、Worker、Mock 上游、Redis 7.4、PostgreSQL 17 及发生器，尚未执行 60 分钟持续长测、多机分机压测、真实外部 LLM 供应商 30 s / 120 s 满载长流以及跨故障域 Redis Sentinel / PG 备库晋升切换压测；相关结论不由此推导。
