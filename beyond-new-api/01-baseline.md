# 01 基线与对标证据

2026-11-17。本文只记录已经发生的实测与调查，不含建议。建议见 [02-architecture.md](02-architecture.md) 与 [03-server-design.md](03-server-design.md)。

## 1. 环境

| 项目     | 值                                                                                                                       |
| -------- | ------------------------------------------------------------------------------------------------------------------------ |
| 硬件     | Apple M6，12 核，24 GiB；Darwin 27.0.0 arm64                                                                             |
| PG       | 原生 PostgreSQL 17.11，`127.0.0.1:55452`；fsync/synchronous_commit/full_page_writes 开启                                 |
| PG 参数  | shared_buffers 256 MiB，max_wal_size 1 GiB，checkpoint_timeout 300 s，max_connections 200，wal_sync_method open_datasync |
| 运行时   | Bun 1.4.4（`.local/toolchain/bun-1.4.4`），Redis 7.4.12（`.local/toolchain/redis74`）                                    |
| 磁盘余量 | 38 GiB（2026-11-17 `df`）                                                                                                |

两边共用同一个 PG 实例、同一个负载发生器 `scripts/bench.ts`、同一个上游替身 `apps/mock-providers`（零延迟短 JSON；P0 核得上游为 368/370 字节，见 §8）。

## 2. 本项目（T17，提交 9a8db8d）

来源：[capacity-2026-11-16.md](../docs/performance/capacity-2026-11-16.md)、[capacity-improvement-plan.md](../docs/performance/capacity-improvement-plan.md)。本次未重跑。

| 指标                                              | 值                                                                              |
| ------------------------------------------------- | ------------------------------------------------------------------------------- |
| 按容量契约的最大稳定负载（均匀 1000 钱包短 JSON） | **35 RPS**（连续两轮，窗口交集 35.78 / 35.77，P99 745 / 770 ms）                |
| 16 供应账户、worker 并发 64                       | 50 RPS 一轮有效（P99 754 ms），未完成连续两轮                                   |
| 80 RPS 时 HTTP 吞吐                               | 79.2 RPS，但窗口交集 18.1（1 账户）/ 3.8（16 账户），积压线性增长               |
| 每请求 CPU（80 RPS，1 账户）                      | PG 19.6、插件 helper 10.4、Gateway 7.1、Core API 3.0、Redis 1.1，合计约 41 ms   |
| 每请求 PG（50 RPS，16 账户）                      | 顶层语句约 57 ms；规范 JSON 摘要约 80 次；调用方身份重核共 28806 次 / 40 s 窗口 |

“窗口交集”指同一 request ID 同时满足 HTTP 有效成功、PG 证据与账单已提交、外部消费者已 ACK，且完成时间落在 60 秒到达窗口内（`scripts/capacity-evidence.ts:64`、`scripts/benchmark-lib.ts:107`）。

## 3. new-api（ce8fc49e4）

### 3.1 构建与配置

- 源码：`git clone --no-hardlinks /Users/wr/work/new-api` 到 `.local/reference/new-api-ce8fc49`，HEAD `ce8fc49e4b1a874aa917dce4f1a8416c52ec8ec0`，工作区干净。
- 构建：Docker `golang:1.26.10-alpine` 交叉编译 darwin/arm64，`CGO_ENABLED=0`，官方 PGO（`github.com/MoeForge/moejs/default.pgo`）与 Dockerfile 相同的 `-ldflags`。产物在宿主上原生运行。
- 运行：`GIN_MODE=release`，`SQL_MAX_OPEN_CONNS=64`，独立 Redis（56400，无持久化，new-api 只作缓存），独立库 `newapi_bench_1794879563902`。
- 数据：1000 个用户，各一个非无限额令牌（`unlimited_quota=false`），一个 OpenAI 类型渠道指向替身（HTTP），一条 ability。
- 每轮：重置 `pg_stat_statements`，按 PID 每 500 ms 采样 `ps` 累计 CPU（覆盖退出的 PG 后端），压测后核对 `logs`（type=2）条数与用户、令牌、渠道 `used_quota` 累计。

### 3.2 结果

| 轮次                          | 结果                                         | P50 / P99 ms         | 账务                          |
| ----------------------------- | -------------------------------------------- | -------------------- | ----------------------------- |
| 并发 64，30 s                 | 3944.99 RPS，118400/118400                   | 13.27 / 55.71        | 消费日志 122032 = 请求 122032 |
| 到达 3000，60 s（默认配置）   | 2999.81，180000/180000，通过                 | 0.68 / 8.11          | 精确                          |
| 到达 3000，60 s（默认配置）   | 2999.91，180000/180000，通过                 | 0.68 / 2.92          | 精确                          |
| 到达 3500，60 s               | 3498，丢弃 75，失败                          | 1.95 / 637           | 精确                          |
| 到达 4000 / 5000，60 s        | 饱和约 3640，丢弃 19557 / 79366，失败        | 505 / 840；533 / 972 | 精确                          |
| 到达 3000，关闭信任旁路，三轮 | 一过（P99 519 ms）两败（丢弃 50392 / 24621） | 0.78–0.84 / 519–3743 | 精确                          |
| 到达 2500，关闭信任旁路，两轮 | 2499.93 / 2499.88，全部通过                  | 0.61 / 2.19、2.11    | 精确                          |

“丢弃”是发生器在途上限 2048 时未能发出的计划请求，按容量契约记为失败。

### 3.3 每请求成本（3000 RPS 第二轮，按 PID 采样）

| 进程     | CPU ms/请求 |
| -------- | ----------- |
| new-api  | 0.79        |
| PG       | 0.49        |
| Redis    | 0.11        |
| **合计** | **约 1.4**  |

关闭信任旁路的 2500 RPS 两轮为 0.77 / 0.47 / 0.11。PG：每请求 16.0 次语句调用（6 条业务语句 + 5 组 begin/commit），执行时间 0.24 ms，WAL 约 2468 字节、21.8 条记录。6 条业务语句为：

```text
UPDATE users SET quota=quota-$1 WHERE id=$2
UPDATE users SET request_count=request_count+$1, used_quota=used_quota+$2 WHERE id=$3
UPDATE tokens SET accessed_time=$1, remain_quota=remain_quota-$2, used_quota=used_quota+$3 WHERE id=$4
UPDATE channels SET used_quota=used_quota+$1 WHERE id=$2
INSERT INTO logs (...)
SELECT count(*) FROM user_subscriptions WHERE user_id=$1 AND status=$2 AND end_time>$3
```

令牌与用户认证、渠道选择不访问 PG（Redis 与内存缓存）。并发 64 时 `channels` 单行锁等待占 PG 执行时间的 99%（7.85 / 7.94 ms），但单条自动提交很短，不限制吞吐。

### 3.4 检查点

3000 RPS 时 PG 日志显示约每 60 秒一次 `checkpoint starting: wal`，每次距离约 540 MB。关闭旁路的失败轮与检查点时段重合；本次未区分失败来自预扣开销还是检查点时机。

## 4. new-api 的语义（源码核对）

| 行为         | 证据                                                                                                                                                     |
| ------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 信任旁路     | `service/billing_session.go:329` `shouldTrust`：钱包与令牌余额均超过 `TrustQuotaUSD`（默认 10，`setting/operation_setting/quota_setting.go:20`）时不预扣 |
| 非原子计费   | `service/quota.go:442` 起逐条 `DecreaseUserQuota`、`DecreaseTokenQuota`，各自提交；`billing_session.go` 注释“资金来源已提交，令牌调整失败只能记录日志”   |
| 进程内结算态 | `BillingSession` 在内存；`relay/compatible_handler.go:194` 在上游响应写出后同步 `PostTextConsumeQuota`，无持久化的执行记录                               |
| 退款异步     | `billing_session.go:112` `gopool.Go` 中退款，失败只写日志                                                                                                |
| 供应侧       | 仅 `channels.used_quota` 计数，无上游资金账本或预付硬闸                                                                                                  |

## 5. v2 每请求成本的来源（代码与设计记录）

| 成本                                                    | 代码位置                                                                                               | 设计记录                                                                                               |
| ------------------------------------------------------- | ------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------ |
| 成功路径 5 次 Core RPC                                  | `apps/gateway/src/chat.ts`：context(311)、admit(583)、begin(960)、quota OBSERVE(769)、quota CLOSE(802) | R01/R02（`docs/migrations/enterprise-v2.md`）                                                          |
| 每次 RPC 读 startup session + Ed25519 验签              | `apps/core-api/src/identity/internal-listener.ts:372–391`                                              | `enterprise-v2-internal-identity.md`                                                                   |
| “批”逐项执行完整命令，各开子事务                        | `packages/storage/src/sql/core-execution.sql:508–520` `core_execution_batch_v1`                        | `enterprise-v2-core-execution.md:280` 微批规则                                                         |
| 资金事务内逐项身份重核，不用缓存                        | `core_caller_v1`                                                                                       | `enterprise-v2-internal-identity.md:55`、`:211`（吊销线性化）                                          |
| 候选摘要在 SQL 内重算                                   | `core_canonical_v1`、`core_digest_v1`、`core_candidates_v1`                                            | `enterprise-v2-core-execution.md:147`（无通用 revision 列）、`enterprise-v2-financial-admission.md:25` |
| admit 前单独 preflight                                  | `apps/core-api/src/execution/handler.ts:393`                                                           | —                                                                                                      |
| 准入、begin、结算都锁供应余额行                         | `core-execution.sql:311`、`:374`；`core-settlement.sql:106`、`:209`                                    | R08                                                                                                    |
| 结算锁 `upstream_accounts` 行（NO KEY UPDATE）          | `core-settlement.sql:105`、`:207`                                                                      | —                                                                                                      |
| 每次插件调用新建 QuickJS-Wasm 实例，两级 stdio + base64 | `packages/plugin-host/native/README.md`“Every call instantiates a fresh guest”                         | `docs/performance/plugin-runtime-probe-2026-11-12.md:13`（复用 guest 泄漏正文）                        |
| 结算不跨调用合批                                        | `packages/storage/src/settlement/index.ts`（每批 ≤128 组）                                             | `enterprise-v2-legacy-removal.md:199`                                                                  |

11-11 单体版在原子预扣、信用、WAITAOF + 副本、外部 ACK、PG 同步提交下，窗口完整链路为 2000–3951 RPS（[funds-capacity-2026-11-11.md](../docs/performance/funds-capacity-2026-11-11.md)），环境与今天不同，未复现。

## 6. 需更正的旧结论

[historical-validation-2026-11-10.md](../docs/performance/historical-validation-2026-11-10.md) 记录 new-api 32 并发 330.88 RPS，并得出“本项目约为 new-api 的 7.85 倍”。今天原生 PG 上 new-api 64 并发为 3944.99 RPS，该结论不成立。旧轮次 new-api 连接 `127.0.0.1:55442`（OrbStack 容器内 PG，见 `.local/reference/new-api-runtime/state.json`），差异原因推测与此有关，未验证。

## 7. 限制

- 本项目数字来自 11-16 报告，new-api 数字为 11-17 实测，两者未在同一天同一构建对照重跑。
- new-api 上游为 HTTP，本项目为 TLS。
- new-api 没有外部投递与 ACK，交集项以“消费日志与各处 quota 累计等于请求数”替代。
- 发生器与被测服务同机。
- 本次压测脚本位于会话临时目录，未入库；[04-spec.md](04-spec.md) 的 SR-01 要求将其固化为仓库脚本。

## 8. P0 同日复测与更正（2026-11-17）

本节是 P0 新取证，前文保留历史轮次；完整命令、成功与失败审计、采样及检查结果见 [P0 报告](../docs/performance/p0-2026-11-17.md)。当前源码 HEAD `3535f4a`，运行时代码与 `9a8db8d` 相同，未实施 P1–P7。

- 本项目到达 36 / target 35，连续两轮完整通过：P99 683.61 / 930.81 ms，窗口交集 35.90 / 35.82 RPS。40、45 RPS 都出现不能连续通过的轮次，50、60 失败。
- 按 PID 每 500 ms 累加，在上述两轮每请求 CPU：PG 23.50 / 22.19 ms，插件 9.51 / 9.29，Gateway 6.83 / 6.77，Core API 3.57 / 3.45，worker 1.23 / 1.19，准入+证据+副本 Redis 1.71 / 1.68。worker“约 0.1 ms”不是新采样支持的事实。CPU 包含预热和排空，分母为其对应全部已处理请求；PG 共享后台单列，发生器和上游不计入服务端合计。
- Gateway 一次约 40 RPS 的采样发现：15116 个样本中，约 59.3% 在 supervisor.load 的完整 Wasm SHA-256，约 4.9% 在包快照 structuredClone。模块大小 2518186 字节。RPC 签名约 1.1%，schema 约 8.9%，HTTP/2 约 4.2%。这些是样本比例；不能把 profiler 的 wall-time 归因直接当 CPU。绝对分摊与 7.1 ms 历史归一化估计在报告中注明方法和限制。
- new-api 原始 rate=target=3000 轮为 180000/180000、无丢弃，但窗口内 179992，包含尾批吞吐 2999.86，因此严格容量判失败。§3.2 历史表中“通过”不能当作严格 target=rate 的容量证明（2500 轮同理）；新的连续档用显式 offered/target 分别报告。没有放宽 P99、交集、排空或计划请求规则。
- **关键语义更正：** `trust_quota_usd=0` 关闭信任旁路，不保证正额预扣。本短提示词在默认费率下的预扣估算转整数后为 0；新日志显示每请求 pre-consumed 为 0，且没有 needs-pre-consume 分支。源码 `relay/helper/price.go:94–126` 仅估算输入成本，`common/quota_math.go` 转整数；`service/billing_session.go:209–229` 只在 effectiveQuota>0 时扣令牌。因此 §3.2–3.3 的“强制预扣”应读作“关闭信任旁路”，2500 RPS 尚不能作为**正额预扣**稳定档的证据。默认计费每请求原生 quota=2，用户已确认保留费率，并用原始累计=2×请求数及 quota/2 折算同时核对。
- 用户随后授权追加：相同请求及默认费率，`trust_quota_usd=0` 加 `pre_consume_multiplier`，以日志证明每个请求确实正额预扣；与只关闭信任的轮次单列。结果以报告和原始审计为准，不修改参考产品源码。

旧 §7 中“未同日对照”“临时装置未入库”是旧轮次限制；本次 P0 已提供正式脚本和同日对照。HTTP/TLS 上游差异、new-api 无外部 ACK 与同机发生器的限制仍存在。P0 不构成 G4 故障对照或 G1 60 分钟验收。

P0 另核对响应尺寸：同一 mock 源码对 `synthetic` / `gpt-4o-mini` 默认 Chat JSON 分别为 368 / 370 字节（UTF-8正文，不含HTTP头/TLS；通过createMockApp公开入口实测）。原“约1KiB”描述偏粗，不作为本轮尺寸事实。客户端经过v2协议转写的正文另有差异，未用该上游字节数冒充客户端尺寸。

追加正额预扣实测已完成：`trust_quota_usd=0, pre_consume_multiplier=100`、offered3010/target3000连续两轮通过，P99为77.52/109.03ms；含预热183661/183665个请求均进入正额预扣分支，无信任旁路或零额预扣结算。每请求CPU合计2.081/2.120ms（new-api1.114/1.115、PG0.810/0.846、Redis0.157/0.158）；PG顶层22.022次调用、1.263/1.949ms执行、2642/2658B WAL。3500目标轮丢弃34471个计划请求，失败。默认3000稳定，纯trust=0/倍率1在2500稳定、3000有过有败。所有费率和请求正文保持不变；这是对“实际正额预扣”成本的新证据，不把配置变化归因成默认代码回归。
