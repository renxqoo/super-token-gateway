# super-token-gateway 与 new-api 同口径性能对标（2026-11-23）

## 结论

同机、同 PG 17、同 mock 上游、同负载模型、同发生器下：

| 维度                                 | super-token-gateway（本仓 `main` + 未提交工作区，`dist/` 实时构建）                    | new-api（`ce8fc49e4`，原生 Go Mach-O arm64，PGO）                                                   | 结论                                                                                 |
| ------------------------------------ | -------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------ |
| 单实例峰值吞吐（短 JSON，1000 钱包） | **18,409 RPS @1024c**，且随并发单调上升                                                | 默认配置 922 RPS 后**随并发下降**至 712；开官方批量模式 6,417 RPS @512c 后回落                      | gateway 峰值 **2.9×**（对最优 new-api 配置）、**20×**（对默认配置）                  |
| 低并发延迟                           | 32c P50 12.7ms / P99 23.7ms                                                            | 批量模式 32c P50 5.5ms / P99 10.3ms；默认 32c P50 25.7ms / P99 140ms                                | new-api 批量模式低并发延迟更优（无持久预扣）；gateway 比默认配置低并发 P99 低 **6×** |
| SSE 流式                             | 6,706 RPS @1024c，P99 248ms                                                            | 5,479 RPS @128c，512c 抖动至 3,753，1024c 5,379                                                     | gateway 高并发 SSE 占优，低并发 new-api 批量模式占优                                 |
| 资金不变量（压测后）                 | **每轮 `conservation.ok=true`、`charges = settled = uniq = 成功响应数`、活动租约归零** | 默认模式账务精确；批量模式**停机丢失最多一个批处理窗口**（本轮 users.used_quota 落后 30–56%，见下） | 资金正确性 gateway 严格更强                                                          |
| CPU 形态                             | 单节点 93% 单核为主（可横向扩展）                                                      | 批量模式 400%+（4 核吃满后不再扩展）                                                                | 形态不同，gateway 横向更优、new-api 单机吃核更多                                     |

**"哪个更优"的答案取决于契约**：若只看"轻路径流式直通 + 非持久计费"，new-api 批量模式在 32c 延迟更低；若要求**持久预扣 + 复式账本 + 每请求守恒 + 停机零丢失 + 高并发单调扩展**，gateway 在吞吐（2.9×）、P99 稳定性和资金正确性上全面更优。new-api 批量模式的 5.6k RPS 是以**进程内攒账 + 5 s 刷盘**为代价换来的。

---

## 环境与口径

- 硬件：Apple M6（12 核，24 GiB），macOS 27.0.0 arm64。
- 数据层：PostgreSQL 17.11（Docker `postgres:17`），`fsync=on`、`synchronous_commit=on`、`full_page_writes=on`、`shared_buffers=2GB`、`max_wal_size=4GB`、`checkpoint_timeout=15min`。每轮独立建库、结束 drop。
- Redis：gateway 自带压测用独立 Redis 7.4（`appendonly=no`）；new-api 用独立 Redis 7.4（`appendonly=no`，仅缓存）。
- 发生器：同一 `scripts/bench-http.ts` 的 `createFastHttp1BenchClient`（Bun 原生 HTTP/1.1 keep-alive），同机 loopback。
- 上游：同一 `dist/mock-providers/main.js`，输出 `Hello, 世界 🌍`，`prompt_tokens=10 / cached=3 / completion_tokens=5`。
- 请求体：`{model, stream, messages:[{role:user,content:"hi"}], max_tokens:32}`，网关侧 `model=gpt-4o-mini`，new-api 侧同名（默认 `model_ratio=0.075`、`completion_ratio=1`、`group_ratio=1`，即 2 quota/请求）。
- 钱包：两侧各 1000 个，均匀轮询；每钱包初始额度充足（gateway `1e12 atoms`，new-api `1e9 quota`）。
- 并发阶梯：32 / 128 / 512 / 1024c，每档 8 s，前有 2 s 预热。
- new-api 关闭了 `GLOBAL_API_RATE_LIMIT` / `GLOBAL_WEB_RATE_LIMIT`（默认 360/180 s 单 IP 全局限流会在 1000+ RPS 下砍请求；这是单 IP 发生器的装置产物，与网关侧 `rpm=10M` 项目限额放开口径一致）。
- new-api `SQL_MAX_OPEN_CONNS=64`，与历史 p0 报告一致。

### 契约差异（不可隐瞒）

| 能力                | gateway                                                      | new-api（本版本）                                                                         |
| ------------------- | ------------------------------------------------------------ | ----------------------------------------------------------------------------------------- |
| 发送上游前持久预扣  | 是（`billing_holds`，PG 事务）                               | 默认 `trust_quota_usd=10`：钱包 > $10 时**旁路预扣**；`trust=0` 时走 Redis Lua 或 DB 预扣 |
| 终态原子性          | 同一事务原子提交客户扣款、供应成本、借贷流水、16 分片 Outbox | 消费日志 + 分表 quota 增量；默认模式逐请求同步写，批量模式攒内存 5 s 刷盘                 |
| 复式账本 / 守恒审计 | `auditConservation()` 每轮校验                               | 无等价审计                                                                                |
| 停机语义            | SIGTERM 后账务已全部落库                                     | 批量模式下 SIGTERM 到达时若下一轮刷盘未完成，内存增量丢失                                 |
| 跨节点共享限流      | Redis 7.4（RPM/TPM/并发租约）                                | 无                                                                                        |
| 热路径单行热点      | 无（钱包按项目分散，供应商用 leased settlement）             | `UPDATE channels SET used_quota=used_quota+$1 WHERE id=$2` 每请求锁同一渠道行             |

---

## 实测数据

### 短 JSON 并发阶梯（1000 钱包）

| 并发  | gateway RPS / P50 / P99      | new-api 默认（trust 旁路） | new-api 批量模式（trust 旁路） | new-api 批量模式（trust=0 强制预扣） |
| ----- | ---------------------------- | -------------------------- | ------------------------------ | ------------------------------------ |
| 32c   | **2,428** / 12.7 / 23.7 ms   | 922 / 25.7 / 140.5 ms      | 5,591 / **5.5** / **10.3** ms  | 5,619 / 5.6 / 9.3 ms                 |
| 128c  | **4,461** / 28.8 / 45.7 ms   | 844 / 132.2 / 429.8 ms     | **6,176** / 19.6 / 31.4 ms     | 3,702 / 26.5 / 231.8 ms              |
| 512c  | **13,463** / 29.4 / 119.2 ms | 749 / 609.1 / 1,620.6 ms   | 6,417 / 78.9 / 90.1 ms         | 5,434 / 82.6 / 304.5 ms              |
| 1024c | **18,409** / 51.3 / 157.9 ms | 713 / 1,189.4 / 3,495.7 ms | 4,936 / 175.3 / 510.8 ms       | 5,430 / 162.4 / 404.5 ms             |

gateway 每档 0 错误；`conservation.ok=true`；`charges=settled=uniq=315,635`；活动租约 0。

### SSE 并发阶梯（1000 钱包）

| 并发  | gateway RPS / P50 / P99      | new-api 批量模式（trust=0）   |
| ----- | ---------------------------- | ----------------------------- |
| 32c   | 2,185 / 14.0 / 26.9 ms       | **4,599** / **6.4** / 18.1 ms |
| 128c  | 4,444 / 28.6 / 51.2 ms       | **5,479** / 21.8 / 68.7 ms    |
| 512c  | **5,513** / 84.9 / 200.1 ms  | 3,753 / 94.5 / 528.2 ms       |
| 1024c | **6,706** / 146.1 / 248.2 ms | 5,379 / 181.0 / 271.6 ms      |

gateway 每档 0 错误；`conservation.ok=true`；`charges=settled=uniq=156,466`。

### 多节点（gateway 2 节点 + Worker + Redis，`bench:prod --limit`，含 `SNAPSHOT_POLL_MS=2000`）

| 并发（总） | RPS / P50 / P99          | 备注                                  |
| ---------- | ------------------------ | ------------------------------------- |
| 512c       | 5,989 / 80.2 / 157.3 ms  | 每节点 256c，未推满                   |
| 1024c      | 8,148 / 122.3 / 203.7 ms | 每节点 512c，未推满；0 错误、守恒通过 |

这是不同拓扑（含 Worker、Redis、生产快照轮询），不能与单节点 1024c 直接比较；列出仅供横向扩展参考。完整单节点→3 节点阶梯见 [capacity-contract.md](capacity-contract.md)。

### 资源

| 指标       | gateway @1024c       | new-api 批量 @1024c  | new-api 默认 @1024c |
| ---------- | -------------------- | -------------------- | ------------------- |
| 进程 RSS   | 252 MB（SSE 326 MB） | 236 MB               | 233 MB              |
| CPU        | 93%（单核为主）      | 151%（512c 时 443%） | 110%                |
| WAL / 请求 | 5,832 B              | 2,073 B              | 2,764 B             |

gateway 的 WAL/请求更高，是因为每请求持久写预扣 + 终态 + 流水 + Outbox；new-api 批量模式 WAL 低是因为大部分 quota 增量在内存里。

---

## new-api 批量模式的账务缺口（如实记录）

三轮批量模式（`BATCH_UPDATE_ENABLED=true, BATCH_UPDATE_INTERVAL=5, MEMORY_CACHE_ENABLED=true`）停机后 DB 核对：

| 轮次                     | 消费日志 quota 总和 | users.used_quota 落库 | 落后比例  | users.request_count 落库 | 预期    |
| ------------------------ | ------------------- | --------------------- | --------- | ------------------------ | ------- |
| json-batch（trust 旁路） | 398,218             | 276,204               | **30.7%** | 138,102                  | 199,109 |
| json-trust0-batch        | 349,062             | 154,102               | **55.8%** | 77,051                   | 174,531 |
| sse-trust0-batch         | 330,626             | 173,622               | **47.5%** | 86,811                   | 165,313 |

new-api 日志证据（json-trust0-batch）：`batch update started 06:44:04 → finished 06:44:22`（一轮刷盘 18 s），`received signal: terminated 06:44:25`，之后无新一轮刷盘即退出。即**优雅 SIGTERM 下仍丢失了一个批处理窗口**。消费日志（`logs` 表）本身是逐条精确的；缺口在 `users/tokens/channels` 的 quota 累计列。

默认非批量模式（json-default）账务精确：`consume_logs=29,468`，`users.used_quota = tokens.used_quota = channels.used_quota = 58,936 = 2 × 29,468`，`valid=true`。

---

## 深度集成测试（gateway，当前工作区）

| 套件                                          | 结果                             | 覆盖                                                                                                                                                      |
| --------------------------------------------- | -------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `tests/integration/ledger-v4.test.ts`         | **26 pass / 0 fail**，345 expect | 100 并发热钱包零 55P03、200 并发租约结算、退款/债务同锁序、Worker 回收死节点租约、Outbox 无死锁/不跳晚提交、锁超时不丢结算、约束错误不回滚同批其他请求    |
| `tests/integration/gateway-v4.test.ts`        | **11 pass / 0 fail**，600 expect | 动态 failover（503/429/401/ECONNREFUSED）+ 熔断恢复 + 单笔扣款、SSE 原子 `[DONE]`、幂等 HELD 守卫、供应商 UNKNOWN 对账、SSRF 拦截、真实 WASM 插件流式审计 |
| `tests/integration/cluster-and-media.test.ts` | **7 pass / 0 fail**，121 expect  | Redis 跨节点限流/熔断/epoch 同步、并发租约 TTL 可见性、批量许可 flush                                                                                     |

未运行：`console-api.test.ts`、`platform-foundation.test.ts`（与本次性能对标无关）、60 分钟长测、多机分机压测、真实外部 LLM 长流。

---

## 原始证据

- 装置：`.local/compare-newapi-20261123/bench-newapi.ts`、`bench-gateway.ts`（不入库，`.local/` 被 gitignore）。
- 逐轮 `audit.json`：`.local/compare-newapi-20261123/{gw-json,gw-sse,newapi-json-default,newapi-json-batch,newapi-json-trust0-batch,newapi-sse-trust0-batch}-*/audit.json`；汇总 `all-audits.json`。
- new-api 二进制 SHA-256：`60b6a0263d88f49b88678cb3fd8c7c2b1c5c3590457fc6ed5e81e772faed72c6`（`.local/reference/new-api-ce8fc49/new-api-bench-darwin`，与 `/Users/wr/work/new-api` HEAD `ce8fc49e4` 一致）。
- gateway `dist/manifest.json` SHA-256：`e9de65426dc4ab3885bbfa808425a227f6dec836a6535abe2078652ff183b2dd`（`git_commit=5a0e27b` + 未提交工作区，构建于 2026-11-23T06:03Z）。
- 每轮 new-api 完整日志（含 SLOW SQL）保留在对应目录的 `newapi.log`。

## 局限

- 单机 12 核同时跑发生器、两侧网关、mock、PG、Redis；1024c 以上未测。
- new-api 仅测 1 个渠道、1 个模型、`gpt-4o-mini` 默认费率；多渠道/多模型下的渠道选择与 `abilities` 查询开销未覆盖。
- new-api 批量模式的账务缺口是在 SIGTERM 下观测的；生产环境若通过 `/api` 触发最终刷盘或等待自然周期可能收敛，本轮未验证。
- 未做故障注入对照（PG 强杀、Redis 失联）下的两侧恢复行为对比。
