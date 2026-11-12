# Core 准入、单次发送许可与双方资金事务

状态：金融实施设计已接纳；T04.b.2a已完成双路独立复查、精确main集成与主树单次完整verify（unit284、integration463、全部产物），v2026111206冻结。T04.b.2b签名数字事实/双方结算与T04.c三业务进程仍待验收。以下保留2026-11-12设计修订1技术正文；“当前行为”、行号及早期未接纳状态是当时事实，使用前核对已接纳源码。原稿SHA256为 `dfa76dd607387731163be1a0aed405abf54894eb32643e1b30ec27be116e22e9`。

实施优先级：本次已确认业务规则及 [迁移主规格](enterprise-v2.md) → [内部身份设计](enterprise-v2-internal-identity.md) → 本文金融事务设计。身份设计的 Core 私有认证投影、历史收证撤销屏障及严格 `DispatchProjectionV1` 替代本文早期可信 session 与完整 dispatch 传入 Core 的形状。完整上游 target、headers 与 body 仅留 Gateway 宿主内存；Core 只接收白名单技术摘要，不能因本文早期字段建议恢复原文传输或持久化。

已交付前置包括精确上游费用算法、上游充值存储、插件 SDK/宿主与目录发布绑定，分别见同目录 math、funding、plugin-sdk、plugin-catalog 文档及主规格真实证据；本文 §8.2a 的 bootstrap 必须复用这些实际入口，不重建另一条 builtin 执行路径。身份修复1已通过最终独立复查、精确 main 集成和主树单次完整 verify（unit277、integration396及全部产物），v2026111205冻结；金融 T04.b.2a 的前置依赖已满足。

金融实施分两单元：T04.b.2a 先交付真实 context/准入/重试/一次 begin/abort/命令恢复、双方预留与 quota 协议；T04.b.2b 随后交付数字证据身份及 RAW/CANONICAL/DISPUTED、双方结算/流水/事件同事务与历史 producer 验证。两者分别拥有追加迁移并独立审查；T04.c 才装配三业务进程和双 Gateway 完整 JSON/SSE/耐久故障验收。拆分仅安排实施顺序，不删减本文任何不变量或最终接口功能。版本号由协调者按已接纳迁移顺序分配，正文旧“建议下一版本”不授予改写已冻结 SQL 的权限。

新增管理资金入口必须沿已接纳 b.1 的私有平台凭证投影，在同一资金外层事务持有 credential/admin SHARE 锁并复核当前启用、期限与逐动作权限，直至资金、回执和审计提交。现有 funding repository 的 actor 字符串仅用于归属审计，不能作为认证凭据或替代这一撤销屏障。项目 ADMIN 不获得平台权限；普通充值不恢复冻结。

## 0. Request 与 attempt 的配额归属补充

State: revision2 accepted after bounded independent design review; R-Q1 closed, not product acceptance. This addendum overrides the old financial plan's universal attempt/command derivation where it conflicts with logical request quotas. Based on actual main apps/gateway/src/execution.ts:146–190,333 and104–128, apps/gateway/src/config.ts:73 and usage.ts:42: customer-level scopes are acquired once outside retry; provider scope once per physical attempt; TPM summary aggregates all attempts. Preserve these observable semantics.

Organization/principal/project/key RPM and concurrency belong to the logical request. First successful admission fixes each common lease's creator caller/kind/command/body digest, request ID, owner session/epoch and original bucket; next refers to that exact same common lease, never reacquires or charges common RPM/concurrency. Channel quota belongs to each physical attempt and its creating admission/next command. Immutable common provenance may retain first attempt_id, but that does not make its authority/liveness depend on whether the first attempt later aborts or becomes UNKNOWN. Query/recovery finds common descriptors by request_id and current channel descriptors by attempt_id; no copied common rows or alias ownership.

Execution budget captures the actual Core configuration max_attempts=1..5, default3, original total deadline and finite aggregate token budget. Do not hardcode16 or silently extend existing allowed configuration. Common estimate initially covers captured max_attempts times the first verified finite token upper bound. Each admitted physical attempt has an immutable estimate; next validates cumulative estimates against the original common token budget. Exceeding it is a typed persistent rejection with no new physical attempt/supplier hold; do not enlarge original common allocation or borrow from another window to make it pass. All counters obey existing finite safe numeric limits, no rounding/capping unknown amounts.

Shared lease cleanup F02 remains unchanged: pre-PG partial failure, BUSY, rollback, unknown commit, NOT_FOUND, late finally and process exit confer no delete authority. A next command's confirmed REJECTED can only authorize its own unadopted channel draft whose creator tuple/digest match; it cannot release first-admission common descriptors. Committed request/common release barriers or individual attempt/channel release barriers provide the other cleanup authority; no generic release closure.

Logical closure is separate from financial state and immutable budget. Use one explicit monotonic request lifecycle marker (for example execution_closed_at outside execution_budget), never mutate captured limits/deadline/token budget. A strict current-original-owner/session-bound CLOSE with finite reason COMPLETED/FAILED/CANCELLED/DRAINED sets this marker and affected descriptors RELEASE_PENDING in one PG transaction following the existing sorted locks. The only independent whole-request close branch atomically verifies under those same locks that every permit has never been granted and all RESERVED permits are aborted in that transaction; it sets logical closure before any later begin can win. Pure financial TERMINAL, successful settlement, accepted FINAL evidence or a selected customer charge is not transport-closure proof and cannot independently authorize early common release. Other Core instances may only recover an already committed closure/release barrier, not manufacture a new closure from a financial terminal state. Subsequent next/begin reject regardless of residual attempt budget; only after confirmed commit may any Core release those exact Redis leases and mark RELEASED. The barrier never returns open/ACTIVE. Replays recover the same safe receipt; original command/owner/epoch and cleanup proof are not replaced.

CLOSE is valid for ordinary success/failure/cancel/drain and safe whole-request abandonment before send. It is not restricted to all-RESERVED aborts; the worker may not have settled money when the logical transport has ended. It releases active concurrency, not funds and not RPM history. All SEND_GRANTED/UNKNOWN customer/supplier financial responsibility remains exactly unchanged. A per-attempt RESERVED abort alone does not close the request or free common quota while further retries are allowed; if it also releases the final customer reservation, the same committed logical closure must prevent later next/begin.

The trusted Gateway host must first synchronously invalidate unconsumed local grants and stop/await all owned transport for the request before asking CLOSE; T04.c implements and proves that host lifecycle. A begin committed before CLOSE but delivered later cannot be consumed after local closure. Core CLOSE blocks fresh next/begin, not retroactively changes an old committed grant or proves a supplier never executed. This stage's RPC/lifecycle tests are not proof of the future actual host behavior. Worker/technical recovery may finish already committed release barriers from PG descriptors without owner transfer or new sending rights.

Common TPM uses all physical attempts, including failed/UNKNOWN attempts, rather than the selected customer-billed attempt. Only complete accepted numeric evidence permits the derived aggregate target; an unknown component retains the original estimate. Apply an idempotent target delta to the captured original Redis bucket only; expiry/generation loss never moves refund/charge to a new window or revives a released active lease. Closing concurrency does not refund RPM or invent a complete token summary. T04.b.2b derives full aggregate targets from the sole canonical evidence chain; T04.b.2a does not expose an owner-supplied arbitrary-target RECONCILE success RPC. Runtime narrow idempotent original-bucket port is provided for b2b.

Required actual later vectors: one request retryA→B counts commonRPM1/concurrency1 with each channel charged once; first-attempt abort can retain common and retry safely; next rejected/BUSY/late cleanup cannot delete common; common close vs next/begin both PG lock orders; post-close and late-grant zero new sends; normal successful transport frees common concurrency before delayed worker settlement while money UNKNOWN unchanged; complete tokens sum all physical attempts and unknown estimate retained; original bucket expiry no new-window effect. Design models cannot substitute for actual PG/Redis/host tests in b2a/b2b/c.

### 0.1 未发送许可的技术恢复

This implements the accepted financial design's separate cleanup after revocation. It does not authorize a new sending owner or historical cost inference. The actual supported strict command is execution_abort_recovery_v1 (DTO/path/function definitions belong the implementation and must be documented from actual source).

Only a genuinely authenticated current core-worker/core-api may propose this recovery. Gateway is denied. The target is an existing Core request/permit whose immutable original owner and context are read from PG, not supplied as a replacement identity. Under the existing global identity→auth/catalog→wallet/account→request/hold/attempt/permit lock order, re-read the original captured execution deadline or actual stored revocation/expiry of its recorded workload/customer authority. Caller booleans, stale pre-read state, financial terminal, lease timeout, NOT_FOUND or inability to reach Gateway cannot establish eligibility. A healthy unexpired target rejects recovery.

Only a CAS of still-RESERVED, never-granted permit may become ABORTED/NOT_EXECUTED. Its supplier hold release, paired journal, corresponding channel release barrier, audit and safe recovery receipt commit together. Every SEND_GRANTED/UNKNOWN permit and related customer/supplier responsibility is retained regardless of time, revocation, route changes or process death. A concurrent begin holds the same request/permit locks: begin first prevents recovery; recovery first prevents begin. No grant, AuthMaterial or old-owner current identity is returned or minted.

A retry-safe per-attempt recovery need not close the whole request. Only if all request permits have never been granted and are all ABORTED in the same transaction may it atomically mark execution_closed_at, return the final customer reservation under original cash/gift/credit/expiry rules and set common release barriers. Never refund the last customer hold and leave next open. Any older UNKNOWN means customer responsibility stays; no financial terminal shortcut authorizes common early release. Recovery of a committed quota RELEASE_PENDING barrier remains separate, rebuildable from PG and incapable of touching money.

Every current technical-caller authorization is locked through the outer commit; final eligibility is observed under the required target locks. Concurrent revoke/re-enable cannot turn an earlier pre-read into authority after it is no longer true. NOWAIT/BUSY, dependency/internal audit errors and unknown COMMIT are not stable business rejection; same command safe-state query recovers without grant or duplicated journals. Owner/epoch/creator immutable fields never transfer. Manual/general settlement or reconciliation is not added by this command.

Owned CLOSE is a pure technical lifecycle operation: it requires the currently valid original workload owner/session and actual request binding plus the future host invalidate/stop-await precondition. It must not require the customer key to remain enabled merely to stop work and release already-owned concurrency; customer revocation still blocks every new admission/next/begin. A workload identity itself security-revoked cannot issue a new CLOSE. Other Core callers can complete previously committed barriers or this proved-never-granted branch, never manufacture a close from completed money alone.

Actual regression requirements: expired and revoked RESERVED release exactly once; healthy/no-proof and Gateway role deny without side effects; both begin/recovery lock orders; original UNKNOWN remains unchanged; all-never-granted atomic customer release+closure, retry-safe abort retains it; actual audit failure full rollback; commit-response loss safe recovery; original source gift expiry not revived; current technical caller revoked while competing authorization follows valid lock linearization. These are product tests to execute, not completed by this clarification.

## 1. 现有事实与最小复用面

下列相对路径均以本工作树为根，定义及行号可用同名绝对路径复核。JSON 记录关键输入文件 SHA-256。

| 现有定义                                                                                                                                                           | 当前真实行为                                                                                                                                                                                                                          | T04.b 处理                                                                                                                                              |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `packages/kernel/src/cost.ts:17 calculateCost`；`packages/storage/src/sql/functions.sql:330 exact_billing_price`                                                   | BigInt/NUMERIC 有理数按全部维度累加，末尾只 ceil 一次；超出 38 位拒绝；缺 quantity 的数学缺省为 0                                                                                                                                     | 原样复用数值运算，调用前单独证明维度已知/ceiling 齐全；数学缺省不等于业务上缺值免费                                                                     |
| `packages/kernel/src/reservation.ts:15 quoteReservation`                                                                                                           | 取候选客户价最大额；requests=1；输出/字符/图片在 ceiling 内替换；返回每 provider 的客户价格版本                                                                                                                                       | 原样复用客户报价规则，输入来自 Core 锁定的真实 price 行与数字 manifest；不接受 Gateway 自报金额                                                         |
| `packages/storage/src/funds.ts:140 quote`                                                                                                                          | provider-specific tariff 优先于通用；当前有 1 秒本地缓存                                                                                                                                                                              | SQL 查询选择规则复用；context 可做建议缓存，admission/next/begin 权威复核不能复用缓存决定授权                                                           |
| `packages/storage/src/sql/functions.sql:39 admit_gateway_batch`                                                                                                    | 钱包 `FOR NO KEY UPDATE SKIP LOCKED`，128 项，现金/赠款按策略分配、信用差额、来源流水；request/hold/首 attempt 原子。request=RUNNING，attempt=DISPATCHING；首 attempt 在 PG 内生成                                                    | 追加版本提取一个私有分配内核、增加显式 request/attempt UUID 输入并同步 Core/旧 adapter；外层改 ADMITTED、建 RESERVED，不假装已有硬闸/commands/permits   |
| 同函数 `:92–98/:121–129`；`schema/existing.ts:821`                                                                                                                 | 意图唯一范围是 `(project_id,key_id,operation,intent_digest)`，不是目标 project+意图                                                                                                                                                   | 追加项目级唯一约束和外层仲裁；fingerprint 必须含币种等语义，不能沿用旧 body-only 指纹                                                                   |
| `packages/storage/src/store.ts:460 admitInitial/:525 startAttempt`                                                                                                 | 批次适配及 55P03 重试；retry 直接 INSERT attempt，复核部分授权/route，但没有单次 begin、上游 hold 或全局序号预算仲裁                                                                                                                  | 新 admission repository 取代入口；不能继续把 startAttempt 当执行许可                                                                                    |
| `sql/functions.sql:583 record_usage_evidence`                                                                                                                      | source 旧版去重、同 FINAL 重叠数值矛盾记 usage_conflicts、FINAL/PARTIAL 排序、MERGE_REQUIRED 丰富；更新 evidence_id                                                                                                                   | 复用现有 usage/index/payload/conflicts/deliveries 链；按 §4.1a 拆 RAW、累计 CANONICAL、DISPUTED 身份，替换 source 唯一键及旧 SQL 仲裁。不能直接原样调用 |
| `sql/functions.sql:234 commit_usage_evidence`                                                                                                                      | 更新 request、资源账单结果；SUCCESS 分支会无条件设置 selected_attempt_id                                                                                                                                                              | 新 Core 路径不用此函数选择交付者；用 `selected_attempt_id IS NULL OR = 当前` 的写入条件，旧 attempt 迟到只能结自己的供应成本                            |
| `sql/functions.sql:397 ingest_initial_usage/:489 persist_usage_batch`                                                                                              | 快路径写 usage/event deliveries，再调用客户 settle；普通路径逐事件捕获错误；证据可能先保留、客户结算返回 ERROR 后重试                                                                                                                 | 新双资金路径不套用此边界；同一输入组内证据、双方财务与事件都在外层子事务，检查旧 settle 返回结果，ERROR 必须抛回外层回滚整组                            |
| `sql/functions.sql:688 settle_covered_requests/:753 settle_requests`                                                                                               | 客户 selected attempt、billing outcome、captured billing price；完整 PROVIDER_USAGE 等待 FINAL 全量维度；字符/按请求 basis 沿现有成功规则；failed 仍有 DISPATCHING/UNKNOWN/PARTIAL 时不释放；超 hold 完整形成应收、来源/债务/过期守恒 | 原样作为客户结算内核复用，外层先录齐同组所有事实/争议，再调用一次；不得让快路径跳过新上游入账                                                           |
| `sql/functions.sql:300 credit_wallet/:274 configure_wallet_credit/:204 collect_wallet_debt/:354 expire_wallet/:626 refund_billing_charge/:2 adjust_billing_charge` | 现金不限定模型/到期，赠款作用域和三选用策略；现金偿债、赠款不偿旧债；信用含债务和在途；退款先减债/原来源逆序归还、过期 gift 转 expired；稳定 external_ref 指纹                                                                        | 金融规则原样复用，T04 不重写；Core adapter 消除 SQLSTATE 泄漏。完整 API 迁移/管理审计补齐归 T05，不能删旧有效回归                                       |
| `sql/functions.sql:548 post_money_journals/:992 validate_money_lines`；`sql/enterprise-invariants.sql:1`                                                           | 客户 journal 2–2048 合法 legs、同钱包/币种且平衡；上游 journal 延迟约束要求至少两 leg 总和 0、immutable                                                                                                                               | 客户原样复用；上游新增私有 posting helper 及账户/币种/attempt 归属校验，不能只有 sum=0                                                                  |
| `schema/enterprise.ts:26 executionCommands/:55 executionPermits/:137 upstreamHolds/:167 upstreamCosts/:194 upstreamCostHeads`                                      | 已有基础形状，无执行函数；commands 没有安全 receipt，hold 没有 remaining_reserved，cost 只有总额/complete/digest，没有 evidence FK                                                                                                    | 追加必需元数据，使用现有核心表，不另造钱包/许可系统                                                                                                     |
| `runtime/src/limits.ts:203 enter`                                                                                                                                  | 返回带闭包的 release/adjustTokens，token bucket 由 Redis TIME 产生；released/reconciled 仅在内存；现 Lua 同 ID 重进仍可重复 INCRBY                                                                                                    | 必须新增可重建 descriptor、Redis 幂等 acquire/release/renew/adjust，不能把 closure 跨 Core 使用                                                         |
| `runtime/src/batcher.ts:2 Batcher`；`store.ts:117–146`                                                                                                             | 有界队列、按 weight 保留输入组、不拆单组、批次/并发上限                                                                                                                                                                               | 复用；不先造通用平台，不扩大已知容量承诺                                                                                                                |
| `runtime/src/usage-queue.ts:222/:238/:286`；`apps/gateway/src/billing-worker.ts:6`                                                                                 | Producer 同连接 queue.add→WAITAOF，失败保留同 jobId 重试；worker process promise 成功后 BullMQ 才完成任务                                                                                                                             | 复用耐久顺序并升级严格数字 envelope；Core PG commit 返回之后才完成 process；外部 event ACK 与 BullMQ ACK 独立                                           |

现有基础 schema 的 `cost_versions.ceilings={}` 与 `attempts.binding_id=NULL` 为旧行为过渡形状，已有 foundation 文档明确它们不满足新执行前置条件。`2026111202` 及其四个 SQL/checksum **不可改写**；新增 DDL/函数放追加版本，版本号由协调者分配，建议下一空闲 `2026111203`，不在本文写回 manifest。

## 2. 不变量、状态与各自责任

1. 客户 request、customer hold、首 attempt、供应 hold、RESERVED permit、命令安全回执共同提交或共同消失。后续每个物理发送新增自己的 attempt/供应 hold；不同 attempt 的 UNKNOWN 不相互抵消。
2. `execution_permits` 为**发送状态权威**。为复用现有结算，`attempts.state` 仍沿用现有枚举；RESERVED 期间 attempt 可以是 DISPATCHING，但这绝不表示已发出。恢复器必须联查 permit，禁止只看 DISPATCHING 就判断发送过。
3. 归属 `(gateway_instance_id,incarnation_id,owner_session_id,permit_epoch)` 创建后固定。epoch 初始十进制 `"1"`；单 attempt 不转移 owner、不因租约过期换 epoch。任何新物理发送必须新 attempt。
4. 客户只按一次选定交付 attempt 的现有最终交付规则计费；全部物理 attempt 的供应成本分别结算。选定交付身份通过 CAS 固定；后来的失败/取消/更早 attempt 事实不能换掉它。
5. lease 是配额/执行活性信息，不能释放任何资金。只有 RESERVED abort、可信 NOT_EXECUTED 或足够完整的成本/人工裁决可以减敞口。
6. 全部资金为币种内 atoms；客户币种和供应成本币种独立。available upstream=`balance_atoms-reserved_atoms`，不换汇、不跨币种净额。
7. 新执行读取权威授权/资金/版本。旧 snapshot 用来结算和只读补证，不能被当前配置悄悄改写。

| permit 前态          | 动作及前置条件                                                                         | 后态与资金                                                                                                                             |
| -------------------- | -------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| 不存在               | admission/next 成功                                                                    | RESERVED；两边 hold 已提交，无发送权                                                                                                   |
| RESERVED             | 原 owner 有效 session、digest/epoch/版本一致、当前授权及硬闸仍有效、期限内；首次 begin | SEND_GRANTED；granted_at 固定；资金保留；只有这次首次成功调用有临时 grant                                                              |
| RESERVED             | 原 owner abort 或撤销后的独立清理事务；与 begin 同行 CAS                               | ABORTED；attempt=NOT_EXECUTED；释放本 attempt 供应占用；若请求无其他责任/交付，走客户零额 RELEASE；有其他 UNKNOWN 时客户 hold 不被清掉 |
| SEND_GRANTED         | 首次 grant 回执丢失、owner 死亡、执行超时而无最终证据                                  | UNKNOWN；不再给 grant，保留各自敞口；恢复器可保守标记 UNKNOWN                                                                          |
| SEND_GRANTED/UNKNOWN | 数字 PARTIAL                                                                           | permit 可保持 UNKNOWN；attempt=PARTIAL；只结已知累计成本并保留未知敞口                                                                 |
| SEND_GRANTED/UNKNOWN | 完整最终成本且无争议/可信未发送事实                                                    | TERMINAL；对应供应 hold SETTLED/RELEASED；客户结果按原规则                                                                             |
| TERMINAL             | 迟到更低等级取消/中断                                                                  | 只保留事实，不降低 FINAL/已选交付、不释放真实费用                                                                                      |
| 任意                 | 矛盾 FINAL                                                                             | 持久争议、账户 RECONCILE_REQUIRED，客户 wallet_disputes 冻结；已记费用不暗改，未决敞口保留                                             |

grant 只表示 Core 已允许**一次**发送，不是已发送事实；重放命令回执只能看状态，不能取回发送权。

## 3. 稳定内部 DTO/RPC（建议）

建议新增 `packages/contracts/src/core-execution.ts`，独立于 SDK；只依赖 contracts 现有 `Operation/Usage/PluginBinding/CommandEnvelope` 与 kernel 纯值。SDK 与此子模块只对接 operation、必要数字 manifest、immutable IDs/digests，不传 prompt、tools 内容、媒体、响应、任意 metadata、完整 URL 或 secret。

### 3.1 可信调用者与每次启动 session

`TrustedGatewaySession` 是 T04.c 内部认证 middleware 传给 application/repository 的参数，**不是 JSON 可填写的 DTO**：

```ts
type TrustedGatewaySession = {
  service_identity: string;
  gateway_instance_id: string;
  incarnation_id: string;
  owner_session_id: string;
  expires_at: string;
};
```

mTLS 静态证书只证明服务/instance，body 自报 `caller_incarnation` 不能证明启动归属。T04.c 必须落实每次启动 session 绑定：例如 Gateway 启动生成不持久化的临时密钥，以 mTLS 换取 Core 签名的短期启动 session；请求携 session 与当前连接/临时密钥持有证明，所有 Core 副本可验证。Core 签名的 instance/incarnation/session 才能进入 TrustedGatewaySession；重启不读取旧启动私钥或 session。签发、过期、轮换、重放挑战的具体实现归 T04.c/T14，不在本文搭第二套身份系统。合法旧进程可收证到截止，不能由新进程冒用旧 incarnation。

SQL 的 `p_trusted_*` 来自上述 middleware 的窄接口；数据库只把受限 Core role 视为可信调用者，禁止将公开 body 直接展开成 trusted 参数。`caller_incarnation` 必须等于认证 session；owner/epoch 不作为权限证明。worker 则是专用受限服务身份；队列 origin 需按 T04.c 的可信生产者认证方案验证，不能把普通用户填的 gateway UUID 当授权。命令查询也验证原调用方与安全归属。

### 3.2 数字 manifest 与候选

```ts
type BillableManifestV1 = {
  version: 1;
  operation: Operation;
  input_characters?: string;
  images?: string;
  output_tokens_bound?: string;
  quantities: Usage;
  resource_ids: string[];
};
type CandidateRefV1 = {
  route_id: string;
  channel_id: string;
  binding_id: string;
  plugin_version_id: string;
  package_digest: string;
  config_revision: string;
  config_digest: string;
  cost_version_id: string;
  customer_price_id: string;
  candidate_digest: string;
};
```

以上是 Core 消费的业务形状，不宣称 SDK 已导出这些字段。strict schema 限长/数量、仅允许既有维度和稳定 ID，禁止未知键。单请求 requests ceiling 按显式成本配置为至少 1，实际计费 request 何时已知由宿主事实决定；quantities 中缺字段仍未知，显式 `"0"` 才是数值零。数量采用十进制字符串；只有现有有理数 duration 的 seconds 可有小数；其他计数均整数。现有 quote 的 images/outputBound 需要 Number 时先验证安全整数，不能直接 Number 大数。T04.a 未定字段先做明确 adapter，不能读取它的在写文件推断 ABI。

当前 route/model 无通用 revision 列，不能臆造读取 `route.revision`。候选摘要由 Core 对锁定行的语义字段规范化计算：model/operations/output cap、route ID/provider/upstream model/request policy/price pointer、channel account/binding/config revision/网络与凭证版本引用、plugin publication/quarantine 状态、客户价/成本 immutable ID。上下文快照变化即 admission 拒绝陈旧候选；begin 若对应当前绑定/可执行语义改变也拒绝，不能默默切新版。已 grant 结算沿原 binding。凭证本体不进入该 JSON，T04.c 定义受 permit 约束的密封执行句柄。

### 3.2a prepare 前身份与捕获预估向量（拟定，A/B 接点裁决）

宿主在 prepare **之前**生成 `proposed_request_id` 和 `proposed_attempt_id`；request ID 可沿现入口 `apps/gateway/src/app.ts:31` 的技术 requestId，attempt 必须为本次物理尝试的新 UUID。创建真实 prep session 时即固定 tenant/project、可信 instance/incarnation/session、proposed request/attempt、plugin digest、binding/config revision、operation 与有界本地输入句柄。session ID 另由宿主生成，不用占位 attempt，不在 prepare 后任意重绑。next 保持已提交 request_id，只预生成新的 proposed_attempt_id 与新的 prep session。

admission 的 command digest 包含这两个 proposed IDs、prep-session opaque ID/摘要、tenant 归属、候选/manifest/final dispatch digest；next 包含已有 request_id 与 proposed_attempt_id。Core 仅在验证服务启动 session、当前 project/key 归属、command/intent 仲裁且两个 UUID 尚未占用后采纳它们；同命令重放返回已提交的**同 UUID**。主键/唯一约束并发兜底；不同命令复用已占 UUID 为持久业务冲突，不认作拿到原链授权。UUID 和 session_id 仅是关联，不是认证或访问许可。Core 不需要也不能读取正文来证明 prep；T04.c 的可信宿主对绑定与最终 digest 负责，未经验证服务身份不能走此路径。

宿主收 RESERVED 必须逐项核对返回 request/attempt 与原 prep session 相同；不一致则 fail closed，按已有 command 查询/原 RESERVED abort 收敛，不能把结果改绑到另一 session。prepare 后正文、计划、manifest、binding/config 或最终 digest 变化，必须 dispose 旧未消费 session；若已 RESERVED，先按其状态 abort 或保留 UNKNOWN，再申请新的 command/attempt，不能在原准入下悄悄重新 prepare。begin 只接受仍未消费、未取消且 digest 不变的原 session。T04.a 的**实际**字段名、public exports 和 session API 待接纳后写一份明确 adapter 表并做契约验证；上述为 Core 拟定字段，不能引用在写 SDK 当已接纳接口。

每 attempt 的供应向量拟定持久化在 `upstream_holds.estimated_quantities`（strict Usage、收费维度齐全）和 `estimate_manifest_digest`；`estimated_quantities_digest` 覆盖版本化规范化向量。permit.manifest 是宿主最终数字 manifest 的 Core 规范化捕获，二者与 captured cost_version 一起 immutable。唯一构造规则：对每个 rates 中的维度 d，要求配置有合法 `ceilings[d]`；Core 以已验证最终 manifest 的准确量/硬上界收窄时，先验证它不超过配置 ceiling，再令 e[d] 为该准确量/硬上界；未得到该维度更窄可靠上界时 e[d]=配置 ceiling。requests 对单次执行必须显式且 e≥1；无法证明某收费维度有界则拒绝，不以缺值0或 `min(actual,ceiling)` 掩盖超限。manifest 只给未定准确量的临时估算不得收窄 e。

**E 和 U 必须读取这一份 e**：E=`ceil(Σ e[d]×rate[d])`；以后 unknown 维度也使用持久 `estimated_quantities[d]`，绝不回读较大配置 ceiling。重建时对 captured manifest 和 immutable cost ceilings 按同版本规则重算 e，与持久向量/digest/E 完全相等才允许 begin/结算；不等即冻结并拒绝新 grant。比如配置 input ceiling100、output50，最终 manifest 可验证上界60/30，rate均1/3，则 e=60/30、E30；初始 U30/R30，不能用100/50得到 U50。若真实已final量超 e，即使未超原配置，也按本 attempt 超预估冻结。配置 ceilings 保留做来源校验，持久 e 才是该 attempt 原始承诺。

### 3.3 RPC 输入、返回与幂等

所有 POST 命令使用现有 CommandEnvelope 字段，body_digest 由 Core 重算。caller_identity 是认证结果中的稳定 service+gateway_instance 身份，不是所有 Gateway 共用一个可伪造字符串；不同 instance 的同 UUID 不串用。context 引用应是跨 Core 可验证、有界有效期的 Core 签名授权上下文（或等价持久引用），固定 key/project/IP/operation/model；不能仅依赖某 Core 进程内 Map。admission 的身份引用必须匹配它，仍重读当前权限。digest=SHA-256(版本化 canonical 业务体)，含所有会影响资金/执行的字段与最终 dispatch digest，不含 trace_id、重试网络信息；deadline_at 是首次意图的一部分，重试不得换期限延长授权。金额/币种大写验证后、集合排序去重、十进制规范化后再 digest，不能只比较客户端传来的 digest。owner 的可信 instance/incarnation/session/epoch 属执行语义：与已存命令逐项比较，不能由新启动改 body UUID 后继承旧 owner；新的 session 可查询原安全状态，但任何写入重放都不能换 owner 或取得旧发送权。

| RPC                                             | 业务输入（另加 envelope）                                                                                                                                                                                                           | 安全输出                                                                                                                       |
| ----------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `POST /_internal/core/v1/context`               | 短时客户 token、可信 transport IP、operation/model/request fingerprint；不含 prompt                                                                                                                                                 | 有界授权上下文标识、客户币种、候选顺序/immutable IDs/数字策略。无 request/hold/grant                                           |
| `POST /_internal/core/v1/admissions`            | context 引用、project/key 身份引用、nullable intent key hash、原始语义 fingerprint、customer_currency、选定 CandidateRef、manifest、最终 dispatched_digest、资源 IDs、proposed_request_id/proposed_attempt_id、prep-session ID/摘要 | COMMITTED + 同 proposed request/attempt/ordinal、permit tuple/RESERVED/deadline、捕获金额/币种/IDs、配额 lease IDs；没有 grant |
| `POST /_internal/core/v1/attempts`              | request_id、expected ordinal、前一 attempt ID、候选/manifest/dispatched_digest、同一请求 owner tuple、proposed_attempt_id、prep-session ID/摘要                                                                                     | 同 proposed UUID 的新 RESERVED attempt；旧 UNKNOWN 自己保留。无可见输出/已选成功交付后禁止重试                                 |
| `POST /_internal/core/v1/attempts/{id}/begin`   | attempt_id、permit_epoch、binding/candidate/dispatch digest、命令 ID；宿主尚未消费/未取消的本地上下文摘要                                                                                                                           | 首次 CAS 独占响应可含 `send_grant`；同命令 replay/另一 begin/查询不含 grant                                                    |
| `GET /_internal/core/v1/commands/{id}?kind=...` | 路径及原 caller 的认证 session                                                                                                                                                                                                      | NOT_FOUND/COMMITTED/REJECTED + 持久安全 receipt/current permit state；永不含 send_grant                                        |
| `POST .../{id}/abort-before-send`               | attempt/epoch、有限枚举 reason                                                                                                                                                                                                      | ABORTED 或冲突的 SEND_GRANTED/UNKNOWN 状态；重复安全重放                                                                       |
| `POST .../{id}/lease`                           | attempt/epoch、RENEW 或 RELEASE、期限                                                                                                                                                                                               | 仅配额租约结果；无发送权/资金释放/owner 转移                                                                                   |

admission 示例：Core 为 `command_id=A`、USD 客户、HKD 成本计算 customer hold `"100"` 与 supplier hold `"60"`，返回 request=Q、attempt=X、RESERVED、epoch=`"1"`。同 A 同体恢复仍是 Q/X 和这两个 hold；A 改成 CNY 或换 dispatch digest 返回 409 `command_conflict`，原 receipt 不变。另一 Gateway 用新命令 B、同 project+intent 调用时，自己的 B 持久 REJECTED/409 `already_accepted`，附原 Q/state，不共享执行链。相同 intent 不同 fingerprint 为 409 `idempotency_conflict`。这些码除旧公开码外是建议按规格新增。

内部错误结构沿规格 `{code,category,retryability,command_state,request_id?,attempt_id?,message}`，storage 层翻译 SQLSTATE。客户不足仍 402 `insufficient_funds`；上游不足/冻结按规格新增 503 `upstream_funds_unavailable`；相同命令不同体 409 `command_conflict`；BUSY/PG/Redis 暂不可用是可重试依赖错误且**没有确定持久命令结果**。业务拒绝为 REJECTED 固定回执，充值或配置变化后必须用新命令重新提出意图。未认证/无法解析有效 envelope 的请求不要求落命令表；不可泄漏 SQLSTATE/堆栈/secret。

## 4. 追加 schema 与 SQL 接口

### 4.1 必需增量（以下均拟定）

| 对象                                                | 追加内容与约束                                                                                                                                                                                                                                                                                                                                                               |
| --------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `execution_commands`                                | `receipt jsonb NOT NULL`，严格安全回执，无 grant/secret/正文；保留 `(kind,caller_identity,command_id)` UNIQUE；记录验证的 incarnation/session，COMMITTED/REJECTED 插入即 immutable 终态，不持久 PENDING。REJECTED receipt 可作为对应共享 quota lease 的清理凭据                                                                                                              |
| `execution_permits`                                 | route FK、candidate/manifest digests、strict manifest、owner_session_id、prep_session_id/prep_binding_digest；既有 binding/dispatch digest/epoch/owner 不可变；只许状态列受控更新。command_id 仍指 admission/next 命令主键                                                                                                                                                   |
| `gateway_requests`                                  | 项目级非空 `(project_id,intent_digest)` UNIQUE，captured execution_budget 记录全局 attempts/总 deadline；主键必须为 Core 验证后采纳的 proposed_request_id                                                                                                                                                                                                                    |
| `upstream_holds`                                    | immutable `estimated_quantities jsonb NOT NULL`、`estimated_quantities_digest`、`estimate_manifest_digest`、estimate_algorithm_version=1；新 Core hold 每个 rates 维度齐全，E 可重算；不得改 estimated_atoms 充当剩余占用。旧 hold 无可证向量时不伪造，迁移须拒绝活跃且不可映射记录                                                                                          |
| `usage_event_index / usage_payloads / usage_events` | 按 §4.1a 扩展角色、snapshot身份与 provenance，删除旧 source 唯一键；同一事实链仍保留原 id/created_at payload FK、seq、归属和 immutable。增加 final_dimensions，受 strict 跨 payload 校验                                                                                                                                                                                     |
| `upstream_costs`                                    | evidence_id FK 指同 attempt 的 CANONICAL（disputed=false）或 DISPUTED（true），币种与 hold一致；digest 覆盖该 snapshot、F、captured cost/e IDs；head 不得指 disputed。此前表须为空或有可证明映射，否则拒绝迁移，不假造历史金额/FK                                                                                                                                            |
| `core_evidence_receipts`（新）                      | §4.1a 的原始事件身份/幂等元数据与 raw/canonical/disputed/conflict FK；strict numeric_fault 只记无法表示金额的数字异常，不存任意 metadata。不是第二套用量/费用事实表                                                                                                                                                                                                          |
| `execution_quota_leases`（新）                      | PK `(attempt_id,scope_kind,scope_id)`，stable lease_id UNIQUE；command FK、已验证 owner/epoch、Redis namespace/config_generation/shard alias、bucket ID/expiry、tokens、lease_revision、draft/live expiry；状态 ADOPTION_PENDING/ACTIVE/RELEASE_PENDING/RELEASED/EXPIRED。RELEASE_PENDING/RELEASED/EXPIRED 不能 begin；Redis 路由来自受控 generation，禁止 arbitrary key/URL |
| plugin bootstrap                                    | §8.2a 的 `plugin_validation_runs`、`plugin_publication_receipts`，以及既有 audit_events 的 nullable actor/details 扩展；新 bootstrap/plugin action 强制非空严格 actor/details。plugin_versions 身份字段 immutable，状态/校验结果仅受控动作更新                                                                                                                               |
| 索引                                                | upstream_holds `(account_id,currency,state,attempt_id)`；permits `(state,expires_at,attempt_id)`；usage `(attempt_id,snapshot_revision)` 新唯一与语义 source 检索索引；receipts 两类身份 claim 唯一及 attempt/source/revision 检索；validation 一版本至多一个 active run                                                                                                     |

旧 request intent constraint 是当前 `admit_gateway_batch` 的 ON CONFLICT 目标。只在旧 adapter 尚依赖它时暂保留，同时所有新 Core 入口先做项目级仲裁并有更强 UNIQUE。追加迁移先检查历史重复项目 intent；发现跨 key/operation重复就拒绝，不自动挑赢家。T04.c 切换撤旧准入权限；T16 改私有内核旧查询/ON CONFLICT为项目范围并删除冗余 constraint/adapter，不永久保留双轨语义。

### 4.1a F01：同 source 的原始事实、累计 snapshot 与争议身份

选定一个结构：继续使用唯一 `usage_event_index → usage_payloads` 数字事实链；**source 永远只是业务语义，不编码身份后缀**。新Core source白名单确定为 `execution`、`execution_outcome`、`recovery_interruption`、`async_partial`、`async_final`、`batch_completed`、`batch_cancelled`、`batch_failed`、`provider_invoice`；依据当前metering/usage/recovery/resources生产者及既有invoice回归。provider_invoice只允许被授权补证/对账身份，不能由普通插件/用户声明来源。新增语义按合同升级，不能接受任意字符串。SDK adapter 映射应逐项确认，不能猜其字段。RAW/CANONICAL/DISPUTED 角色与 source 正交；`execution` 的多个 revision 仍精确等于 `execution`。

拟定给 index 追加：`record_kind` 为 LEGACY/RAW/CANONICAL/DISPUTED，旧行默认 LEGACY；`snapshot_revision numeric(38,0)`（新 Core 必填≥1，旧行 NULL）；`snapshot_digest`；`final_dimensions text[]`；`parent_evidence_id` 和 `input_fact_id`。新 canonical/disputed 的 parent 是计算时已接受的前一 CANONICAL（初始可空），input_fact 指这次 RAW。RAW 两个引用均空。`UNIQUE(attempt_id,snapshot_revision)` 仅新行非空；在锁住 attempt 后分配严格递增序号，**不等于 source_revision 或 upstream cost revision**。新增 `(id,attempt_id)` UNIQUE 后两类自引用采用 composite FK，同 attempt；writer/约束触发器校验角色、parent revision更小、归属及 payload/F，防环与跨 attempt。id 主键继续是每条 snapshot 身份。index/payload 不允许 UPDATE/DELETE，只有追加。

显式 `DROP CONSTRAINT usage_events_attempt_id_source_key`，以主键、snapshot revision 唯一及 receipt身份仲裁替代；不可仅添加 receipt然后保留旧唯一。cost_atoms 拟改 nullable：RAW 必须 NULL；CANONICAL 必须为可重算累计 C；DISPUTED 可计算时存候选 C，溢出则 NULL 且有 numeric_fault；旧 LEGACY 保持原非空。NULL 表示本记录不是已post成本，不能解释成0。cost_complete 对 RAW=false，对 canonical/disputed来自 priced F完整性。snapshot finality 和 supplier complete 仍分开。view追加这些元数据，不复制用量；通过公共查询只暴露 LEGACY/CANONICAL/DISPUTED，内部 writer 用 index/payload读取 RAW。

`core_evidence_receipts` 是幂等/关联记录：id PK，origin_identity/event_id、origin incarnation/session、attempt_id、source、source_revision（无损整数）、body_digest、`event_claim boolean`、`revision_claim boolean`、outcome、strict attempt_state/request_state/error_code/provider_request_id（只存既定状态/错误枚举和安全opaque ID，用来与RAW的Usage/F重建完整body digest）、raw_evidence_id NOT NULL、canonical_evidence_id nullable、disputed_evidence_id nullable、conflict_id nullable、risk_dimensions、numeric_fault、created_at；immutable。所有 evidence 引用通过 `(evidence_id,attempt_id)` FK，并验证角色。body digest 是规范化业务内容（attempt/owner/source/revision/F/Usage/状态/错误/provider-request-ID），不含 event_id、trace 或投递job，便于识别同一 revision 换event ID的重复。

明确的唯一性：`UNIQUE(origin_identity,event_id,body_digest)` 保证同投递内容只一 receipt；partial UNIQUE `(origin_identity,event_id) WHERE event_claim` 唯一认领原 event；partial UNIQUE `(attempt_id,origin_identity,source,source_revision) WHERE revision_claim` 唯一认领原 revision。两种 claim 分别仲裁，不把同 event 不同内容默认为重放。namespace event 与 revision advisory keys 先排序 try-lock，再由唯一键兜底。新 event+新 revision 认领两者；换event但同revision同内容的新receipt认领event、不再认领revision，指原 RAW及原canonical，outcome=REVISION_REPLAY，零新snapshot/钱/交付。完全同event同内容直接返回原receipt。

身份预检先拒绝event ID跨attempt/owner/session重用：这是合同/归属错误，保留受限数字审计但不返回其他tenant资料，不把错误subject当本attempt财务事实。以下event换体均指同一已验证subject。

已认领 event 或 revision 出现不同 body：保留原 receipt与其 claim，不 UPDATE它；新receipt只认领尚未存在的event（若该event已存在则event_claim=false），revision_claim=false，outcome=CONFLICT。插入独立 RAW，再插入把 baseline final维度与该输入合成的 DISPUTED；同组首次冲突无baseline时各输入各有disputed snapshot。原 RAW、DISPUTED、receipts 与 usage_conflicts 同事务保存，已final相冲维度在 disputed 中表示该输入主张，未冲突已final维度从 baseline继承；不把相互矛盾数值揉成一个较小 snapshot。`usage_conflicts` 拟加 incoming_evidence_id、disputed_evidence_id、incoming_receipt_id FK；existing_event_id 指被挑战的 canonical，若无先前head则指同组对方 RAW。既有 usage副本由 writer严格验证等于incoming RAW，不作为新的权威用量来源；fingerprint 覆盖 subject、原/新证据ID和规范化主张，不能只hash usage误合并不同revision。旧 conflict列允许新增FK为NULL，仅旧记录适用。conflict与receipt相互引用的FK采用DEFERRABLE INITIALLY DEFERRED，并在组完成强制IMMEDIATE检查；其余引用按锁序插入，避免靠悬空关联提交。

正常可接受的新事实revision保留RAW，并追加CANONICAL：parent=上个accepted canonical，input_fact=RAW，按 source_revision和逐维final规则merge；receipt指 raw和新canonical，disputed为空。ESTIMATED只保留RAW，receipt关联当前canonical或NULL，不建新的cost head、不取代真实费用事实；STALE partial保留RAW，receipt指raw和当时accepted canonical（若无则空），不推进head。CONFLICT receipt的canonical是未动的baseline（可空），disputed是本输入候选，conflict_id必填；OVERFLOW同样保留RAW及可表达的disputed snapshot，无法表示金额为NULL/numeric_fault，不创建伪造0的upstream_costs revision；保留原head/hold、冻结并暴露未结清责任。新的已确认成本 revision 指**merged CANONICAL**，而不是直接指原RAW；争议成本revision指DISPUTED。head只选non-disputed cost revision。这样每次费用计算的实际累计量、raw来源、前驱都能沿FK重建。

同组先归一、查全部身份和FINAL矛盾，再创建 accepted链/记账；不能把组内先到事实收费后才看矛盾。accepted链每一步一个RAW，全部仍在同组事务。只给新CANONICAL/DISPUTED创建event_deliveries；RAW/重复投递不创建外部交付。外部事件注明 `attempt_snapshot`/`disputed_snapshot`、snapshot与cost revision及是否已post，消费者不累加累计成本。现 Store.requestReceipt/events/createConsumer replay/claim（store.ts:522/612/709/719/735）及所有按seq回放查询必须同步过滤RAW；consumer cursor跨RAW的空隙仍单调推进，不能泄漏RAW或卡页。既有交付链保留，不新增长期outbox。

追加版本必须同时替换下列**仍存活 SQL**，不能删唯一键后留旧arbiter：

| 当前位置                                                                 | 拟定迁移结果                                                                                                                                                                                                                                                                                                                |
| ------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| record_usage_evidence functions.sql:597 SELECT original                  | 过渡旧签名仅允许没有Core permit的旧attempt；查该attempt/source/LEGACY，显式 `ORDER BY seq DESC LIMIT 1`；同source的非final可修订、只有已确认FINAL重叠矛盾走旧争议，不能沿旧original存在就把所有partial差异判冲突；按实际event主键/digest恢复重放，不再假定source只有一行。Core直接用receipts claim仲裁                      |
| 同函数:613 suffix                                                        | 删除函数suffix生成；同步迁移resources.ts:273–282当前async_partial_<usage-hash>生产者，在Core路径发稳定async_partial+独立event/revision。旧caller过渡adapter仅识别该精确受控格式为async_partial，非final差异按观察更新不按已final矛盾判争议；旧adapter只用于无permit的LEGACY且T16删除。历史后缀原文不改、不靠猜后缀回填final |
| 同函数:615–616 ON CONFLICT(attempt_id,source)                            | 改为明确主键 `ON CONFLICT(id)`，碰撞后核对attempt/source/payload，不同内容必须冲突，不能静默忽略。Core使用receipt唯一键及snapshot主键                                                                                                                                                                                       |
| ingest_initial_usage:435、insert_usage_view:470、persist_usage_batch:517 | 过渡路径显式写LEGACY并拒绝有Core permit的attempt；不推断旧final_dimensions。Core evidence groups不调用该fastpath；view trigger仅供过渡调用，新writer直写一份index/payload                                                                                                                                                   |
| commit_usage_evidence:255–266                                            | 旧签名只操作旧attempt；batch/async匹配保持精确source，过滤RAW/DISPUTED。新terminal controller只根据accepted CANONICAL及受信request outcome决定，selected CAS，不读任意最新seq raw                                                                                                                                           |
| settle_covered_requests:709 / settle_requests:847                        | execution、batch_cancelled仍精确匹配稳定source；Core只允许accepted CANONICAL且客户所需维度final；旧LEGACY沿原规则。batch/async终态不得以RAW或DISPUTED存在作为成功/取消证明                                                                                                                                                  |
| usage view/事件DTO/查询/归档                                             | 同步追加元数据/NULL成本合同和对RAW的内部隔离，更新payload保留策略；任何receipt/cost/conflict引用仍在保留期时不得先删其payload，索引不可悬空                                                                                                                                                                                 |

旧函数签名确有过渡需求：T04.b追加版本替换其实现，所有旧caller/测试同步迁移适配，且新Core attempts由permit存在性强制隔离；T04.c三进程切换撤旧Gateway/worker入口EXECUTE、禁止直PG。T16删除旧record/commit/ingest/persist写签名与无使用的view INSERT trigger/adapter；历史LEGACY只读与保留策略继续属于同事实表，不是永久别名或第二份证据。T04.b迁移测试必须验证新增版本重放、旧ON CONFLICT失效点已消除，不能声称只改Drizzle声明就足够。

### 4.2 SQL 签名（新增建议，不是已存在函数）

```sql
admit_core_batch_v1(p_items jsonb) RETURNS jsonb
next_core_attempt_v1(p_trusted_identity text, p_trusted_instance text,
  p_trusted_incarnation uuid, p_trusted_session uuid, p_command jsonb) RETURNS jsonb
begin_core_attempt_v1(p_trusted_identity text, p_trusted_instance text,
  p_trusted_incarnation uuid, p_trusted_session uuid, p_command jsonb) RETURNS jsonb
abort_core_attempt_v1(p_trusted_identity text, p_trusted_instance text,
  p_trusted_incarnation uuid, p_trusted_session uuid, p_command jsonb) RETURNS jsonb
lookup_core_command_v1(p_trusted_identity text, p_kind text, p_command_id uuid) RETURNS jsonb
record_core_usage_v1(p_origin text, p_event jsonb) RETURNS jsonb
persist_core_evidence_groups_v1(p_groups jsonb) RETURNS jsonb
post_upstream_journal_v1(p_account uuid, p_currency text, p_channel uuid,
  p_attempt uuid, p_operation uuid, p_reference text, p_kind text, p_lines jsonb) RETURNS uuid
register_upstream_recharge_v1(p_trusted_actor text, p_account uuid, p_command jsonb) RETURNS jsonb
allocate_customer_batch_v1(p_items jsonb) RETURNS jsonb
register_bundled_plugin_v1(p_trusted_bootstrap text, p_command jsonb) RETURNS jsonb
start_plugin_validation_v1(p_trusted_bootstrap text, p_command jsonb) RETURNS jsonb
finish_plugin_validation_v1(p_trusted_worker text, p_run uuid, p_result jsonb) RETURNS jsonb
publish_plugin_version_v1(p_trusted_bootstrap text, p_command jsonb) RETURNS jsonb
bind_channel_plugin_v1(p_trusted_actor text, p_channel uuid, p_command jsonb) RETURNS jsonb
```

`allocate_customer_batch_v1` 是追加版本从原 `admit_gateway_batch` 提取出的**唯一**私有客户来源分配内核，保留现金/赠款/授信/过期/流水算法，输入显式 request_id 与 attempt_id，删除内核无条件 `attempt_id=ledger_uuid()` 分配。Core wrapper传验证采纳的proposed IDs；next的INSERT同样使用proposed_attempt_id。原 `admit_gateway_batch(jsonb)` 在T04.c切换前仅为旧caller适配：它为缺失attempt_id的旧请求生成UUID、映射旧JSON后调用同一内核，不能给Core permit重绑ID，不复制一套金融算法。Core role只可EXECUTE外层admit/next，不可直接调用内核或旧签名；旧caller和测试的调用形状/返回值同步更新。T04.c撤旧入口权限，T16无使用后删除该旧适配及冗余intent constraint，不保留永久可选ID双轨。新attempt主键冲突必须整组回滚/稳定拒绝，不以随机重试UUID静默改变已prepare身份。

batch items 仅由 Core API repository 内部构造，严格区分 `{trusted_session,validated_command}`，HTTP body 无 `trusted_session`。SECURITY DEFINER 固定 `search_path=pg_catalog,public,pg_temp`，全表限定或受控路径，PUBLIC 撤权；Core API/worker 分别只授入口 EXECUTE，内部 helper 无公开授权。DDL 与普通索引先在 Drizzle TS 声明、生成新 snapshot/migration，原子 SQL/触发器/ACL 放新版本 custom SQL，所有资产唯一 manifest owner。现有 `database.ts:28 migrate` 支持已知 manifest 前缀追加，不需再发明运行时函数刷新。

## 5. 全局锁序、撤销屏障与微批

基础隔离为 READ COMMITTED；当前 `database.ts:12–20` 仅设 statement/lock timeout，未声明其他 isolation。实施时用隔离集成 fixture 核验实际事务设置，不从连接默认推断高隔离保证。全部新 writer 按下列次序取自己所需子集，IDs 在每类内排序；禁止拿了后级再回取前级。

| 顺序 | 锁/对象                                                                                                                                                                                     | 要点                                                                                                                                                                                                                                                                                                             |
| ---- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 0    | command/intent/event 的 namespace-separated transaction advisory keys，按数值排序，try-lock                                                                                                 | 同命令及 project+intent 串行；hash 碰撞只增加保守串行，不替代真实唯一键。重复 receipt 普通读取；所有 try 失败返回 BUSY，不持久业务拒绝                                                                                                                                                                           |
| 1    | mutable auth/catalog：organizations → principals → projects → api_keys → models → routes → billing_tariffs → supplier_brands → channels → plugin_versions → upstream_accounts；每类 ID 排序 | admission/next/begin `FOR SHARE`。锁完重读并核对权威 parent/归属/权限/作用域/CIDR/到期/当前 pointer；预读父链若已变化，退出该组重试/重取context，不在后级锁持有时回头锁新父链。共享锁与 enabled/role/绑定变更的 UPDATE 冲突；不能用 KEY SHARE 代替。worker 不需要无关 key 鉴权，但取涉及账户状态行的最强模式见下 |
| 2    | immutable billing_prices/cost_versions/channel_plugin_bindings/plugin_packages（有需要的 package identity），按表与 ID 排序                                                                 | 普通 MVCC 读取及 FK KEY SHARE 足够，因为有 immutable 触发器；plugin_versions 的可变 quarantine 在上一级，不混淆。不在热资金事务取大 bytea                                                                                                                                                                        |
| 3    | customer wallets，ID 排序 `FOR NO KEY UPDATE`                                                                                                                                               | 保留现有 FK KEY SHARE 读者不把钱包误报忙的性质。首次不存在 wallet 不现场创建空钱包当资金充足，按规则拒绝；充值按 project/currency 唯一键建立                                                                                                                                                                     |
| 4    | upstream_account_balances，(account_id,currency) 排序 `FOR NO KEY UPDATE`                                                                                                                   | 共享账户跨 channel 共同行仲裁；不同币种绝不互相借款                                                                                                                                                                                                                                                              |
| 5    | gateway_requests → billing_holds → attempts（request_id,ordinal,id）→ execution_permits → existing customer charges/allocations → upstream_holds/cost heads                                 | request/hold 预取 FOR UPDATE NOWAIT，其余 mutable 行用 FOR NO KEY UPDATE NOWAIT；已有 settle_requests 会重新取自己已持有的 request/hold/wallet 锁，可安全重入；不预拿 attempt 再去钱包                                                                                                                           |
| 6    | commands/receipts 插入、quota descriptor、usage/index/payload、两侧 journal、audit/event delivery                                                                                           | 引用的父行已锁/是自己新行。命令行先经 advisory 仲裁，再以唯一键兜底，不靠晚期无界 ON CONFLICT 等待去重                                                                                                                                                                                                           |

供应成本 writer 可能把账户置 RECONCILE_REQUIRED，因此它在第 1 级**一开始就对涉及的 upstream_accounts 取 FOR NO KEY UPDATE**，不先 FOR SHARE 再升级。两 worker 对同账户各持 SHARE 后升级会死锁；禁止这种实现。admission/begin/recharge（不解冻）只需 account SHARE。代价是同账户不同币种的结算暂会因账户级冻结状态串行；这是现有 account.state 语义的最小正确实现，容量留 T17 实测，不宣称币种间完全无争用。

FOR SHARE 才阻止非键 UPDATE，KEY SHARE 不阻止 enabled/role UPDATE；依据 [PG17 row-level locks](https://www.postgresql.org/docs/17/explicit-locking.html#LOCKING-ROWS)。不可变/FK 读取保留 KEY SHARE 的理由与“撤销后无新 grant”是两件事。

管理员 revoke/disable/rebind：只按第 1 级既定次序改授权/目录及审计，提交后才回答成功；**不在同事务持 auth 锁再扫描/释放钱包或 permits**。RESERVED 释放另开事务重取全序。begin 若先获 SHARE，管理员等待它提交，grant 线性化在撤销应答前；若 revoke 先提交，begin 在获得 SHARE 后读到新值并拒绝。不能以 HTTP 回包到达先后证明旧 grant 无效；旧 grant 的 committed_at/owner/期限确定其责任。wallet manual freeze 只锁 wallet，不再反拿 auth；account disable 只改 account，不内联资金清理。所有管理员多对象命令遵循同表排序；服务 drain 不持数据库锁等待 Gateway/Redis。

微批沿用 Batcher：admission 上限 128、evidence 总权重上限 256，单 queue 输入组最多 128（当前真实 schema）。整组不能拆开后各自收费。一次 DB batch 中每个独立 group 用子事务，所有可能争用的 row 用 NOWAIT/advisory try-lock；忙则回滚该组已取锁并返回可重试 BUSY，继续独立钱包/账户。不能先阻塞锁热 wallet 再处理整批，不把 SKIP LOCKED 未取到的行误认无账户。若 group 包含多个账户/钱包，预锁全部再写；同 request 的同组矛盾 FINAL 必须一起判决。成功组在同一外层事务暂持锁，后组仍只 try、不等待，避免跨组反向等待；未知隐式 FK 写者通过并发集成测试核验。

旧函数返回 ERROR 不自动触发 rollback。外层发现任何 ERROR、审计/流水失败、余额守恒异常必须在包含该组**全部**证据+钱+回执的内层块 raise，捕获后整组数据库副作用已回滚；PL/pgSQL 局部变量不会自动回滚，输出/临时数组在失败后必须丢弃重建，不能再 flush 失败组（[PG17 exception blocks](https://www.postgresql.org/docs/17/plpgsql-control-structures.html#PLPGSQL-ERROR-TRAPPING)）。预期业务失败在已回滚块外插 REJECTED receipt；PG连接断开/死锁/锁超时/内部审计异常不是稳定业务拒绝，保留命令未知并由同 ID 查询恢复。延迟 upstream journal 触发器在 commit 前强制检查（组完成后切所涉约束 IMMEDIATE 再恢复 DEFERRED）；提交级错误也不能被当成功。`SKIP LOCKED` 只用于工作选择/忙检测，余额读取始终是在实际获锁的权威行上（[PG17 SELECT locking](https://www.postgresql.org/docs/17/sql-select.html#SQL-FOR-UPDATE-SHARE)）。

## 6. 具体事务步骤

### 6.1 admission

1. Middleware 验证启动 session，Core strict parse/canonicalize/recompute command digest，核对 envelope incarnation/session。context 只是一份候选，不能代替当前授权。
2. 先做安全command查询（不是授权捷径），PG事务外取得组织/principal/project/key/channel的有界quota draft leases。stable identity由caller+kind+command_id+scope派生，参数含body digest；重复acquire返回同一lease，不是新owner、不重复扣RPM/TPM且不延长draft。任何中途失败、BUSY、依赖异常、rollback、commit未知都**不删除**已取共享lease；只有提交REJECTED receipt或RELEASE_PENDING屏障才有清理权，详见§6.4。Redis未知只用同identity查，不能换ID重扣。
3. PG group 按第 5 节锁序取得 command 与 project+intent 仲裁；去重结果先暂存，返回旧request/receipt前必须完成相应当前服务归属及客户授权校验，不能仅凭body里的project_id读别人的原意图。已有同 command、同 digest 返回原安全 receipt，无新副作用；不同 digest 冲突。已有项目 intent 则记录本命令 REJECTED，原 Q/state 给公开 already_accepted；fingerprint 不同为 idempotency conflict。指纹含协议 endpoint/operation/model、原始规范化精确正文摘要、customer_currency、资源语义与其他执行相关输入，不保存正文。
4. FOR SHARE 锁定并复核 auth/catalog 当前值；核对 Core context 引用归属、可信 IP、candidate/config/binding/published/quarantine、路由/操作/输出 bound；价格/成本版本 currency 与 captured IDs 一致。新成本配置必须非空 rates，每个收费维度有显式 ceiling（rate=0 也显式），不能默认 `{}` 为零；按§3.2a构造并持久同一向量e，不能取 `min(actual,ceiling)` 掩盖超限。预计输入/输出无法有界则拒绝。核对proposed IDs未占用及session tenant/owner/plugin/config绑定，ID不构成授权。
5. Core 从锁定真实价行使用 quoteReservation 计算客户上界（对最终兼容候选集合取最大值）；供应 E=exact_billing_price(本 candidate 的持久预估向量e,cost.rates)，以后U也仅取同一e。锁 wallet、account/currency 后检查客户 freeze/debt/credit 和供应 account.state=ACTIVE、balance-reserved≥E。T04 不接受未知 mode 或缺余额行；新账户不造充值，缺币种余额行拒绝；存在零额余额行仍严格按balance-reserved≥E判断，E>0自然不足，显式零费率E=0不被误判成有正额费用。
6. 在该组内调用新增私有 `allocate_customer_batch_v1([Core构造input含proposed request/attempt IDs])` 分配客户资金，传入真正 captured quote/price/cost IDs，检查 error/created，任何非预期都 raise 回滚。它的 request RUNNING/attempt DISPATCHING 只是中间事务状态；补 binding，request=ADMITTED，写 RESERVED permit、manifest/digests/owner/session/deadline、upstream hold E 与 reserve journal，账户 reserved+=E。命令COMMITTED安全receipt和ADOPTION_PENDING quota descriptors同事务插入；request/attempt使用原prep的同UUID，保存e及其digest。无网络/Redis 调用发生在资金锁内。
7. 事务 commit 之后才返回 RESERVED；commit/连接结果未知时 API 不声称失败未准入，调用者用原 command_id 查。任何失败不凭本次调用结果删除共享lease。确定REJECTED可按receipt清理；COMMITTED后由任意Core据持久descriptor完成adoption/renew，begin前必须转ACTIVE并验证安全期限；若draft已过期，禁止grant，通过RESERVED abort与RELEASE_PENDING屏障释放对应资金/quota；不能重建同lease假装原draft仍活。commit未知先查原命令，不删除，不释放可能已提交款项。

### 6.2 next attempt 与客户 quote 上界

同 request 锁下要求 expected ordinal=max(ordinal)+1、未超 captured max attempts/总截止、没有 selected success、尚无可见输出的可信宿主事实、原 owner/session 仍有效。旧 UNKNOWN 不删、不释放。每条物理尝试先用新proposed_attempt_id建立独立prep session，Core采纳同UUID；写独立供应hold、绑定/成本版本和按§3.2a捕获的e。next采用与admission相同quota清理权/adoption规则。

T04.b 采用规格允许的最小拒绝策略：重试只能使用首次 quote 已捕获的客户候选价。重新计算最终 manifest 在捕获价下的客户上界，若大于现有 hold，或新候选/新价格不在原 quote.prices，持久有类型 REJECTED，不建新 attempt/供应 hold。正常首次 quote 已覆盖全部最终兼容候选的最大额，常规切换不需要重复客户 hold。客户原 quote、价格 map、input_characters 不在 retry 中覆盖；每个 attempt 固定自己的 billing_pricing_id。旧 UNKNOWN 及客户原 hold 全部保留。

这样不假装现有 BillingQuote.prices 的 provider_id 到 price_id 单值映射能保存同 channel 多个历史版本，也不破坏 FundsStore.resolve 只接受原 captured 价格的规则。未来若 T05 需要放宽到新增候选/调高客户上界，必须先追加能表达全部捕获版本的正式契约与来源分配事务，再原子增加 funded/credit hold。T04 没有虚假的补足成功路径。该拒绝策略是本设计明确实现选择，协调者接纳后才成为 T04.b 执行依据。

### 6.3 begin 与 abort

begin 先恢复同 command receipt；重放即使原状态还 SEND_GRANTED 也不给 grant。锁 auth/catalog→wallet/account balance→request/hold/attempt/permit 后，复核可信 owner/session、固定 epoch/digests、RESERVED、未到期、当前撤销/普通停用/插件隔离/客户及供应 freeze。供应检查自己的 hold 仍计入 reserved、整个账户承诺安全；不是重复再预留 E。begin须按§6.4先在事务外adopt/renew并获取有界Redis存活证明，再在PG锁内核对所有descriptor为ACTIVE、相同lease_revision/config_generation/adopted attempt、未被RELEASE_PENDING撤销且安全期限仍足够；grant本地可消费期限不得超过这些proof的保守共同期限。Redis未知、draft/live TTL过期或PG adoption未确定时不得grant；不能在资金锁内等待Redis。任何成本超额冻结先提交后，后续 begin 拒绝，即使已有 RESERVED。

唯一 `UPDATE execution_permits SET state='SEND_GRANTED',granted_at=clock_timestamp() WHERE state='RESERVED' AND ... RETURNING ...` 胜者，在同事务写 begin COMMITTED 安全 receipt。SQL 返回 `fresh_transition=true` 仅给该次 repository 调用内存；它不存入安全 receipt，不从查询重造。PG commit 确认后，Core handler 最多写一次临时 send_grant（绑定 attempt/owner/session/epoch/digests/deadline，T04.c 验证）。重复 POST/GET、驱动未知提交重试均只状态；不得有 RPC 框架自动重放缓存的含 grant HTTP body。Gateway 宿主在本地 atomic registry 将 attempt 从未消费改已消费，随后才 network send；晚到、重复、取消后或过期响应不能消费。

begin commit 后响应丢失，Core无法知道是否收到/消费，资金责任立即视为可能启动；持久 permit 可先保留 SEND_GRANTED，在状态查询标明 unknown delivery，恢复到 UNKNOWN 不能重新发权。两者都禁止 abort-before-send。后续原 owner 的可信 NOT_DISPATCHED 数字证明可以经耐久证据结零；不能按 begin超时/HTTP4xx/leaseexpiry猜未发。

abort 取同样锁并 CAS RESERVED→ABORTED，与 begin 只有一个赢家。把该 attempt 记 NOT_EXECUTED、供应 hold RELEASED、对应 reserved 归零且 journal 成对；客户若无任何其他 UNKNOWN/交付则沿原 settle 的 FAILED/全 NOT_EXECUTED 路径零额 release，过期赠款归 expired，不复活。begin 已赢时 abort 持久 REJECTED，Gateway 可中断网络但只能走已知/未知证据，不直接释放资金。

### 6.4 F02：共享 quota lease 的清理权与跨 Core 恢复（拟定）

新增runtime窄端口 `acquireLease/adoptLease/renewLease/releaseLease/reconcileTokens`，均返回可重建descriptor。旧Limits.enter closure只作有退出点的本地adapter，不能跨Core承担清理权限。stable lease ID由验证caller+kind+command UUID+scope派生；重复acquire仅取得同一引用，**不取得独占owner**。Redis同scope Lua同槽原子创建lease record、active成员及原RPM/TPM bucket关联；参数含规范化command digest/owner/session/config generation，不同参数拒绝。

**明确期限：** DRAFT active有效期固定为首次Redis TIME起30,000ms与command deadline的较早者；重复acquire不续draft。过期active被清除，但幂等tombstone保留至该命令deadline后30,000ms，之后命令已过期，acquire直接拒绝，不能把同ID当新请求再扣RPM/TPM。tombstone与active均有限TTL；ADOPTED幂等tombstone保留到captured execution deadline后30,000ms。ADOPTED的live lease每次续至 `min(Redis TIME+30,000ms, captured execution deadline)`，活跃宿主以10,000ms周期请求续租；adopt/renew各有稳定operation ID和预期lease_revision，重复同operation返回相同期限/revision而不再次延长，迟到旧revision不能倒退PG descriptor；最多到原执行总deadline，不能靠续租延长它。数值是本设计的有限技术租约参数，必须配置/契约化并做真实时序验收，不能用测试虚时宣称已验证网络时钟。任何generation丢失/Redis重启不重新解释旧lease为新owner，T07/T14恢复前新grant fail closed。

**只有两条删除授权：**

1. 本command的终态 **REJECTED已确认提交**，safe receipt的caller/kind/command/body digest与lease完全一致，且无对应adopted attempt。REJECTED immutable且同键永不能后来COMMITTED，才能向Redis发有界幂等cleanup。跨Core重试前也必须查PG终态；不能依据应用打算拒绝、HTTP 4xx、函数局部结果或未确认commit清理。Lua再次核对lease command/digest和未ADOPTED；发现已adopted则拒绝并告警，回PG查证，绝不盲删。
2. 已关联PG/已adopted lease，按§5全锁序在permit/descriptor下将其标 **RELEASE_PENDING并确认commit**；该屏障使begin不能通过。Core/worker据持久descriptor在commit后幂等release，Lua核对generation/lease/attempt/epoch并写RELEASED tombstone后ZREM active；最后标PG RELEASED。Redis回执未知时只重试同清理权/同ID，不新发资金许可。RELEASE_PENDING不回ACTIVE，旧renew迟到也不能撤销屏障或复活RELEASED。

除此之外，pre-PG部分scope失败、try-lock BUSY、依赖异常、rollback、commit未知、进程退出、旧caller延迟finally，全部**不调用删除active的cleanup**，保留draft有限TTL；API不能返回一个裸release闭包让失败调用随意使用。若能够在新PG事务中完成命令仲裁并持久业务REJECTED，可走第1条；仅有NOT_FOUND不足以证明别的同命令调用不会提交。已持久COMMITTED即使还没Redis adopt，也只能走第2条。Redis拥有的adopt标志是额外防误删，PG receipt/permit屏障才是清理权来源，不假设PG与Redis原子。

**adoption与begin：** admission提交ADOPTION_PENDING descriptor后，以PG查得的COMMITTED命令和attempt向Redis发adopt；Lua只接受未过期DRAFT、相同generation/command/digest/owner，原子固定adopted attempt且延长有限live期限。之后另开按permit锁序的PG事务，仅在descriptor仍ADOPTION_PENDING、permit仍RESERVED且无RELEASE_PENDING时写ACTIVE/live expiry/lease_revision。Redis成功PG失败/未知，仍不grant也不pre-PG删；同descriptor查询/重试收敛。若PG屏障已成立，adopt/renew即便网络迟到，PG不会重新ACTIVE，已授权cleanup会最终释放。

begin之前在资金事务外完成必要续租和读取各scope的live proof；Redis返回该记录剩余TTL，Core以接收时monotonic时间加 `remaining_ttl - 本次完整RPC耗时 - 1,000ms安全裕量` 计算proof截止（≤0则失败）；本次handler在事务及响应前再次检查该截止，grant可消费截止≤各scope的最短proof截止且≤原execution deadline。不得用不同机器未校准墙钟的绝对expiry直接当安全证明；T04.c将有界剩余期限绑定进grant并扣除RPC耗时。PG锁内重新检查descriptor state/revision/generation、permit/owner和proof仍未过期；release barrier先提交则begin拒绝，begin先提交则旧pre-PG失败调用没有删除权。adopt或renew回执未知、lease已过期/丢失时都不给grant，不复活同lease；仍RESERVED可提交abort及RELEASE_PENDING释放资金，已SEND_GRANTED则UNKNOWN资金责任保留。租约过期可能结束并发活性占用，**不证明未发送，也不释放任何钱**；T04.c必须停止过期本地上下文的消费/网络并清理任务。

**复现交错及排除：** A/B同command同scope acquire L，只扣一次。A持command try-lock，B BUSY且其finally延迟。A提交COMMITTED+descriptor，adopt/renew并begin成功。B迟到后仍无REJECTED receipt和RELEASE_PENDING，故没有release请求，L保持active。若B曾在更早时真正提交REJECTED，则A必读到这个immutable终态，不能COMMITTED/adopt/begin；若有RELEASE_PENDING，则begin必拒绝。三者由同command/permit仲裁互斥，不借助跨PG/Redis事务。

RPM历史永不退款。token reconcile以lease已计target作差 `new_target-old_target`，重复目标0，只能改原Redis返回的bucket身份且bucket仍存在；已过期则不补扣/退款到新窗口，不减别人额度。UNKNOWN token未知保留原估量到原桶自然过期。采用generation、可信bucket映射和Redis Lua权限限制，客户端不能任填key。资金仍只由PG决定。清理/续租失败留有界重试工作与指标，不制造无TTL key、不无界后台任务；任何Core/worker可按PG descriptor重建清理，不依赖旧进程closure。

## 7. 供应累计成本与剩余占用的确定算法

### 7.1 快照与精度

每次 evidence 是**attempt 的累计观察快照**，不是新增消耗。新增严格 envelope 含 stable event_id、attempt、origin/session/epoch、source 枚举、source_revision 十进制序号、finality、Usage、`final_dimensions`、状态/错误枚举和可选安全 provider_request_id；exact source enum/SDK mapping 在接纳T04.a后确认。event_id 相同不同 body 是持久冲突，不能依赖现 record_usage 的“相同source且子集”就重放。receipt按§4.1a的event/revision claim和body digest仲裁，每次正常/冲突投递明确指向RAW与merged/disputed snapshot。同 origin/attempt/source 的 source_revision 单调产生；相同 revision 不同 body 也冲突。晚到较旧 PARTIAL 可记录 STALE receipt/观察事实而不推进head、不制造争议；非final/provisional计数可被较新观察修正，不因此反向冲销费用；已final维度下调或上调才构成财务矛盾。跨 source 不凭到达顺序覆盖已final维度。source_revision不是数据库分配的cost revision，两者不混用。

同 attempt 先 merge 已接受快照：未出现的已final维度保留；未final/provisional观察可由较新source revision修正，旧revision仅留事实不覆盖；可信FINAL可覆盖此前未final的PARTIAL（包括向下修正），因为此前没有把该维度记为已花费用。已final维度的数值不能被新来源或更晚revision修改，冲突即争议。final_dimensions只来自有类型的完整定价事实，不能按字段存在、全局FINAL标签或数值精度推断；例如input_tokens还依赖未提供的缓存分区归一化时，不能把原始token总量冒充已定价的input维度。缺TTL分区仍未知，不用aggregate猜拆分。FINAL输入只对其明确出现且可完整定价的维度赋final，缺失仍未知。

requests=1的已知费用可以在token未知时独立final；begin/owner死亡本身不能证明request费用发生。原响应成功的measureResponse当前明确加requests=1（connectors/src/measurement.ts:24），迁移宿主应保持这一事实；由正式计费语义和受控宿主响应/账单事实证明该请求费已经发生时保留requests，不能丢成空usage。仅“已调用socket发送”不自动证明供应商已计费，具体来源能力需T04.a/c核验。NOT_EXECUTED只有受控宿主明确未dispatch事实，费率各维度置0/final；HTTP状态码不构成证明。T04不引入“已证实monotone lower-bound”能力；所有普通nonfinal计数均不进入expense。

写入 usage_event_index 的 snapshot finality 不把继承的PARTIAL quantity变成FINAL：新输入为可信FINAL时，仅在客户PROVIDER_USAGE所需维度都已明确final后才给原settle可用的FINAL快照，否则该聚合快照仍PARTIAL；客户不收费的纯数字观察不要求等待不收费维度final。客户非PROVIDER_USAGE的既有成功/basis规则继续由billing_outcome决定。供应complete始终独立要求其**全部priced维度**已final，即使客户快照已FINAL仍可能供应incomplete；记录中保留final_dimensions，新的writer不再只按全局finality排序选择供应权威快照。仅有PARTIAL输入即使所有出现的request维度已final，也不伪造客户最终交付成功。ESTIMATED只作数值观察/诊断，不作为已知真实成本head；UNKNOWN不能降低之前的PARTIAL/FINAL或抹去已知request费用。

对已接受权威 snapshot s，令每个收费维度rate=n_d/d_d、captured estimated_quantities=e_d、完整且已final的实际量=q_d，final集合=F。非final/provisional数值只留观察证据，不代入已知成本；用精确有理数：

```text
K(s) = Σ over d in F (q_d * rate_d)
u_d(s) = 0                           if d in F
         e_d                         otherwise
U(s) = Σ u_d(s) * rate_d
C(s) = ceil(K(s))
R_base(s) = ceil(K(s) + U(s)) - C(s)
complete(s) = every priced dimension is in F
```

实现可直接复用现有exact_billing_price两次：known_usage只选F中的已完整定价维度，C=exact_billing_price(known_usage,rates)；exposure_usage对每个priced维度取final实际量或未知维度的同一持久e，R=exact_billing_price(exposure_usage,rates)-C。禁止把含provisional值的原usage直接传给C计算，也不能把unknown的数学缺省0当作释放依据。kernel calculateCost用同样两个独立输入映射，人工期望不复用这些函数生成。

新hold无head时C=0、R=E=ceil(Σe_d*rate_d)。每个尚未final的维度保留其完整ceiling，不能减掉provisional观察值而释放风险；该维度有了完整final证据后，才把实际费用转入K并释放其未用上界。complete时U=0、R_base=0；无未解决争议时R也归0（§7.1a风险维度已final即退出未知风险集合）；可信NOT_EXECUTED为C=R=0，且不能覆盖已final费用矛盾。全部维度有理数求和后ceil一次，R用联合敞口ceil减已post成本；不能简单E-C，也不能按维度分别ceil。一个已final维度实际超ceiling时，其他未知维度仍占完整ceiling，因此不会被总额差抵消。任何C+R_base>E、已final的priced quantity超captured ceiling、或最终C>E，记录overrun并置RECONCILE_REQUIRED。可信PARTIAL超过e时按§7.1a立即冻结并扩保守风险占用；它仍不是已确定费用，不进入K/expense。ESTIMATED不冒充实际cost。

### 7.1a 可信 PARTIAL 超界风险与 final 成本严格分开（拟定）

只告警不足以保护新grant：如果可信响应观察已显示未知维度超e，继续按原E占用且放新attempt会扩大风险。因此采用保守异常冻结，不新增lower-bound能力：经T04.a/c宿主来源、计数、归一化与PARTIAL类型校验的 priced维度 q_obs>e 时，receipt记录risk_dimensions并关联RAW，账户在同一证据资金事务置RECONCILE_REQUIRED，阻止此后admit/next/begin。账户原为DISABLED/QUARANTINED时保留更严格状态并追加待对账事实/审计，不把风险状态写入降级为可读资源权限；任何恢复ACTIVE动作须复核所有未解决风险/overrun/安全状态，不能普通enable绕过。插件自行声明“可信”无效；ESTIMATED仅作诊断，不据此post费用，也不伪装真实PARTIAL。

K、C、U、R_base完全不变。对当前head仍未final的维度，额外风险上界量 `e_risk[d]=max(e[d],所有可信超界PARTIAL RAW该维度观察量)`；已final维度仍取该final实际量。`X_obs=ceil(K(head)+Σ_{d不在F} e_risk[d]×rate[d])`，`R_obs=max(0,X_obs-C_head)`。实际R取R_base、R_obs及§7.3争议R的最大值。这是**保守风险保留**，不是主张PARTIAL量已发生费用，不把它放进upstream_costs.cost_atoms或expense；有增加R时只记available→reserved的平衡分录。即使余额无法覆盖新增保留，也完整记风险占用并冻结，负available如实暴露，不能宣称风险已有充足担保。初始E仍为原e算出的E，绝不覆写。

较新PARTIAL向下修正可更新观察快照，但不自动抹掉原超界风险，也不解冻；在该维度取得完整可信FINAL后，这个维度从未知风险集合移除，按final实际量首次进入K，必要时可释放此前多留的风险占用。receipt/canonical provenance保留被final覆盖的RAW证据，资金审计记录风险reserve的delta；若final也超e，转为已确认overrun，仍冻结。全部风险维度final可使R归0，**账户state仍不自动ACTIVE**，须T06对明确风险/争议来源追加reconciliation与审计恢复；充值/普通低价PARTIAL也不解冻。矛盾FINAL不能用此规则自动解决，仍走§7.3。

人工例D：input/output rates均1，e20/20，E40、充值50。可信PARTIAL input25未final，K0/C0、U40/R_base40，X_obs45/R45；B50/S0/A5并冻结，只增加reserved5，不记expense25。随后完整FINAL input10/output15，K25/C25、U0/R0，B25/S25/A25，普通cost_delta=25；最终低于临时25没有退款，因为此前C0。state仍RECONCILE_REQUIRED直至明确审计恢复。若同样input25是ESTIMATED，则只留诊断，C0/R40、不自动花25。

### 7.2 head、hold 与余额更新

`upstream_costs(attempt,revision)` 是完整维度已确认费用的累计 C/complete/evidence digest/FK，不是 delta；`upstream_cost_heads`指向唯一当前**未争议**权威revision，revision在已锁attempt下max+1，无Number转换。`upstream_holds.estimated_atoms`永远是原E，不随partial改写；`final_cost_atoms`只在完整无争议最终时记C；state PARTIAL/UNKNOWN是供应责任未结束，SETTLED/RELEASED才闭合。

**不新增 remaining_reserved 列。** 当前新增的evidence FK与final_dimensions使 `head→CANONICAL snapshot→captured rates/estimated_quantities及receipt风险RAW`足以精确派生R；缺少这两个关联时现有cost总额本身不够派生。一个SQL纯计算helper/查询负责此派生，各writer调用同一明确定义；应用独立人工算例验证，不能读取 `final_cost_atoms`当partial已扣总额。无head→E；非争议head→R_base并叠加§7.1a风险保留；冲突处理见下。若未来为性能缓存R，须追加独立版本+与head一致的DB约束，不能在T04悄悄让estimated列兼任remaining。

在account/currency与attempt/head锁内，对旧C/R与新C/R求：

```text
cost_delta = C_new - C_old
reserve_delta = R_new - R_old
balance_new = balance_old - cost_delta
spent_new = spent_old + cost_delta
reserved_new = reserved_old + reserve_delta
available_new = balance_new - reserved_new
```

同一revision重放delta=0；不能SUM所有revision.cost_atoms。T04普通证据推进只增加已final维度集合，cost_delta必须非负；FINAL低于此前provisional不产生退款，因为该provisional从未进入C。已final费用若需下调，必须走争议与T06明确裁决/追加调整，不能伪装成普通partial修订自动反向expense。完整真实成本不截断、不因可用额不足拒绝入账；可出现负balance，仍完整spent并冻结。NUMERIC38范围内进行BigInt/NUMERIC运算，字符串传输；超出可表示金额时不能cap/存0冒充成本：落core_evidence_receipts的严格numeric_fault+OVERFLOW争议、freeze、保持原head/hold、告警并保留人工处置责任；不同钱包组仍可提交。无可表达金额就不伪造金额流水，故不能声称该争议已财务结清。

供应journal用独立币种平衡子账：`account:balance`表示未花余额、`account:reserved`占用、`account:available`可用、`system:funding`/`system:expense`对侧；建议统一只用可重算的`available/reserved/expense/funding`分录（不同时重复记balance和available）。充值：funding -X/available +X；reserve：available -E/reserved +E；一次更新：reserved +(R_new-R_old)、available +(-cost_delta-reserve_delta)、expense +cost_delta，总和0。balance=available+reserved，spent=expense净累计。所有命名/allowed legs由新增posting helper白名单定义，现基础表未定义这些account文本语义。zero cost和reserve delta同时为0无journal；纯风险reserve变化使用`risk:<attempt>:<snapshot_revision>`稳定reference（expense=0），其他reference如`cost:<attempt>:<revision>`与`hold:<attempt>`；在同事务唯一约束避免重投重复。

按每个account/currency独立重算并验收：spent=所有attempt当前已post head的C之和，reserved=所有hold派生R之和，balance=RECHARGE+受保护TEST_OPENING+signed ADJUSTMENT-spent，available=balance-reserved。journal中available/reserved/expense净变动必须分别重现这些缓存；有争议时head保留原已post金额，不能把disputed revision另加进spent。客户侧仍验证每lot issued=available+reserved+spent+expired、wallet各缓存等于lot对应和、debt等于各charge未偿债和、credit_reserved等于未闭合hold.credit_atoms和，以及charge amount=funded+debt+debt_refunded。跨币种不做资产加总。这些是可从独立表重建的不变量，不是仅断言当前缓存等于自己计算的临时变量。

### 7.3 矛盾 FINAL 与迟到事实

先把同一输入组全部原始事实归一并检测矛盾，再变更head/财务。矛盾FINAL按现有usage_conflicts模式持久记录、customer wallet_disputes保持冻结，并把supplier account置RECONCILE_REQUIRED。新record_core_usage_v1同时写该条严格数字冲突snapshot的usage index/payload（不选为attempt权威证据），使disputed upstream_cost revision可通过evidence FK重算；原record_usage_evidence只写conflict不写这条index，故不能直接承担此新职责。每条输入按§4.1a关联RAW与DISPUTED并由receipt引用；head继续保留之前未争议权威C，不拿先到/金额较小者自动裁决。若同组初次两条FINAL已互相矛盾，不先收费再报争议；原hold仍保留，customer不建charge。

冲突后剩余保守占用 `R=max(R_base(head), R_obs(head), max(0, max_disputed_exposure-C_head))`，其中无head时R_base=原E，C_head=0；有未解决冲突时不自动推进head，因此基准敞口不因晚到低价事实减少。disputed_exposure 对每个可计算且尚未解决的冲突snapshot按`ceil(K+U)`求上界；未能表示的溢出只能保留原R并冻结人工处理，不能伪称担保足够。既有完整C=30/R=0，迟到矛盾FINAL=40时不暗增expense，增reserve=10并冻结；FINAL=25时不自动退5。争议resolution必须以追加有审计的明确选定revision/调整来解，不删除冲突。T04.b仅关闭新许可并暴露争议/receipt，不提供自动解冻；T06实现reconciliations与每个争议revision的解决关联后，派生R只排除明确resolved记录。

已选成功attempt X之后，旧attempt Y的晚到FINAL只能改变Y供应费用和必要争议，不能把q.selected_attempt_id改Y。取消/UNKNOWN/PARTIAL不能降低已接受FINAL，资源取消也不能让真实最终账单被释放。当前resource_billing_state与commit_usage_evidence不能直接用于这一选择；新writer显式CAS，T11资源同规则。迟到未争议FINAL可把原UNKNOWN/部分责任结清；不因q已取消丢弃供应费用。

### 7.4 三组独立人工算例（实现必须以这些常数为期望）

**A：request费已知、token临时计数不入expense，partial→final。** USD账户已充值1000，rate requests=2 atoms、input_tokens=1/3、output_tokens=1/3；ceilings分别1/60/30，E=2+20+10=32。reserve后balance1000/reserved32/spent0/available968。收到PARTIAL requests=1(final)、input=30(nonfinal)：K只含request费2，U仍为input完整20+output完整10=30，C2/R30；cost_delta2、reserve_delta=-2，balance998/reserved30/spent2/available968。临时input30没有形成10 atoms费用，也未减少未知token敞口。最终requests1/input31/output2全final：K=2+31/3+2/3=13，C13/R0；只再扣11，balance987/reserved0/spent13/available987。不能把2+13重复算成15，不能把临时input费用提前记12再退款；两个token维度分别ceil也会错误收费14。若最终input不是31而是低于临时30的数值，只以该final事实首次入K，不产生供应退款。

**B：一个维度超上界且另一个未知，最终超额、负余额与充值不复活。** HKD账户充值50；rate input=1、output=1，ceilings20/20，E40。reserve后B50/R40/S0/A10。partial input30(final)，output缺失：C30，U20，R20，balance20/reserved20/spent30/available0，联合敞口50>40立即RECONCILE_REQUIRED；错误的E-C仅留10会少担保10。最终output25全final：C55/R0，新增费用25，balance=-5/reserved0/spent55/available=-5，完整记55，overrun15，不截成40或50。之后真实充值100：balance95/spent55/reserved0，但state仍RECONCILE_REQUIRED，未明确对账审计不得begin/next/admit。

**C：重试成功不释放前一UNKNOWN，客户只按交付attempt收费。** 同一USD供应账户充值500，attempt A E100后grant丢失，无已知成本：B500/R100/S0/A400。允许新attempt B E80：B500/R180/S0/A320。B的捕获价为request费20、output单价1且ceiling60；已收到完整request费事实requests1(final)，output仍未知，故partial C20且R60：B480/R160/S20/A320；B最终output30也final，C=20+30=50/R0，只追加30：B450/R100/S50/A350。客户单次hold120，B交付账单70：客户available返回50、spent70，A成本未知不另收客户。A迟到完整成本30：供应B420/R0/S80/A420；供应总成本80=30+50，不能把B各revision20+50再加A30算100。若A永无证据，其R100永留，不能因B成功/leaseexpiry/停用释放；A晚到也不能改客户selected B。

基础精度另外保留：1/3+1/3=ceil(2/3)=1；125ms×8/1000=1；9007199254740993 atoms精确字符串往返，客户CNY与成本USD不能相减。

## 8. worker 原子证据组与首次资金链

### 8.1 worker 顺序

1. BullMQ读取严格数字组（同job可多request，但组是不可拆的原子输入），验证可信origin/session/permit绑定与event digest。重复组/事件按receipt原样收敛，不能因更换jobId重复收费。
2. 按第5节取account状态的最强锁、wallet/balance/request/attempt/permit/head锁；同attempt/origin/source按source_revision归一全部事件与final flags，先确定同组争议（矛盾FINAL不因排序消失）及risk，再按§4.1a写RAW、merged CANONICAL或DISPUTED及receipts/conflicts。终态控制器可以标记request交付，但selected只CAS一次。
3. 每个physical attempt计算权威snapshot cost revision及reserve delta，写balance/hold/head/供应平衡journal/overrun审计；客户随后沿旧settle_requests结选定交付。检查其返回值，ERROR使本组所有变更回滚，WAITING/CONFLICT是可提交的业务状态。不能先commit证据再另transaction补供应钱。
4. 同事务插event_deliveries及必要数字事件、资源状态、双方账务审计；事件语义写明attempt_snapshot/cost revision，消费者不能累加每一快照当新增费用。RAW不外发，canonical/disputed按明确snapshot语义沿已有usage_event_index/event_deliveries交付机制，不造第二套兼容outbox。外部只看commit后的数据。
5. commit确认后才resolve worker process，BullMQ再ACK；PGcommit已成功但ACK丢失，重投receipt/head/unique journal reference保证零新增财务副作用。跨事件的外部消费者ACK保持独立持久状态。

若锁忙/PG不可用，job保留并按现有有界重试/失败队列告警，不以mock成功消掉。已admitted事实不能因新admission backlog gate而被拒绝收证。WAITAOF失败时Gateway不得输出正常JSON/terminal/EOF；任务可能已经在队列或PG中，真实费用照结，传输成功与财务事实不同。

### 8.2 T04 最小真实创建/充值/回执/审计

T04首链必须能通过Core API创建实际brand/account/channel、配置真实binding与显式客户价/成本ceiling，登记**已完成**的充值事实，再查询receipt/balance/journal；不能让测试直接UPDATE余额假装产品入口可用。外公开`/admin/providers`继续是唯一channel集合；外部provider_id=内部channels.id，没有`/admin/channels`别名。旧kind/base_url/api_key/rpm/concurrency/enabled输入沿规格保留，创建默认brand/零账户并返回新account/brand IDs；共享账户参数需同brand。无钱就是不能执行。

最小新增管理端点沿规格：`POST /admin/upstream-accounts`、`POST /admin/upstream-accounts/{id}/recharges`、`GET .../recharges`、`GET .../balances`、凭证上传/receipt安全查询；命令用受信管理员身份与精确权限，不能匿名fixture RPC。T04可先只有单brand/单account/currency的最小管理纵切；查询必须读真实提交，不返回合成成功对象。

充值strict输入：external_ref、payment_currency/payment_atoms、credited_currency/credited_atoms、voucher_id/voucher_digest、completed_at、reason；两金额均正、规范化十进制，币种分别保存，完成时间在允许的事实范围内，不执行支付、不推导FX。voucher在Core PG有界bytea（现schema≤5MiB，png/jpeg/pdf），校验实际bytes SHA-256、actor权限/可引用性/到期未引用状态；前置上传只暂存。资金事务在账户/币种锁后，查`(account,RECHARGE,external_ref)`与规范化全payload digest，核对voucher，balance+=credited，写funding operation稳定receipt、funding/available平衡journal、actor audit同事务。重复原payload返回原receipt；换付款币种/金额、credited币种/金额、凭证内容摘要、完成时间或理由均409；不能仅比较有没有voucher。FK已引用voucher的删除必须失败，清理只删过期且确定未引用对象，不要求为了标linked改immutable voucher内容。

余额不足/对账冻结时充值仍可登记事实，但不把account.state改ACTIVE、不清UNKNOWN；state更新只由明确安全/对账动作。audit故障必须整笔rollback，receipt/余额/流水/操作都不能留下。负调整、完整凭证管理/对账/报表与恢复ACTIVE归T06，T04不开放未实现调账成功路径。

TEST_OPENING只能是独立测试配置+角色双重保护的显式入口/fixture能力，journal.kind=TEST_OPENING并标记隔离测试账户；生产role永远不能执行。T04的充值验收用真实RECHARGE事实命令与凭证的HTTP fixture，不能只用TEST_OPENING替代；不把fixture事实声称真实供应商支付。T19三币种各100单位opening沿主规格另验证。

### 8.2a F03：T04.b 真实受控 builtin bootstrap 交付（拟定）

**owner为T04.b**，与admission/结算同一纵切交付。可扩展当前 `scripts/seed.ts`，但它目前只是开发mock seed，不是已存在安装器；新增受控 `install-bundled-release` 子命令及真实Core catalog application入口。只接收构建时枚举的 `release_id`、显式目标provider ID/config/credential reference/expected revision和稳定command ID；release_id选择打包进发行物的**接纳T04.a immutable OpenAI包**，不接任意server文件路径、下载URL、编译脚本或本地替代WASM。T04.a源码/包/ABI尚未接纳时此路径不得返回成功，不能拿native合成testdata充当OpenAI包。

**信任与签名责任：** 必须配置由部署/发行管理方审查并随受控Core发行物锁定的外部trust policy，映射signing_key_id→受信公钥、允许的release身份/package digest、ABI、config schema digest和validation policy版本。包内自带public key不是trust anchor。T04.a负责可复现immutable包及合成fixtures；发行负责人负责在接纳后用发行系统持有的私钥签署规范化release descriptor（name/version/package digest/文件长度与摘要/ABI/schema/fixture digest/策略），bootstrap只有验签能力，不取得私钥。签名是包内容digest之外的detached release descriptor签名，避免把签名/自指digest再纳入自身摘要；Core仍保存完整不可变signature/key字段。没有mandatory签名、未知key/过期撤销policy、digest或ABI/schema不匹配全部失败；不回退unsigned或开发默认值。实际key ID/摘要由接纳发行物明确提供，本设计不编造其值。

bootstrap调用身份是部署事先授予的受信技术operator/workload，拥有仅限bundled allowlist的bootstrap install/publish/bind权限；从验证middleware进入repository，不能body填`actor=bootstrap`。生产不能用普通项目ADMIN/INVOKER，也不能以测试PG超级用户绕过。T04.c提供实际mTLS/启动证明集成；T04.b的产品验收须用受验证bootstrap身份与失败身份，不能用假UUID标已通过。

流程固定如下，所有成功对象/回执来自真实提交。catalog步骤复用execution_commands的kind/caller/command唯一性和safe receipt；新增server白名单catalog命令类型，request/attempt FK为NULL，caller来自已验证bootstrap/operator session而非伪造gateway身份；执行命令仍严格要求TrustedGatewaySession。stage body_digest覆盖release/config/expected revision。

1. **注册UPLOADED。** 在事务前验证bundled bytes与mandatory外部trust policy，严格解包/长度/摘要/ABI/schema/fixtures限额；Core在注册事务再次核对已验证描述与bytes摘要，插plugin_packages immutable bytes/manifest/signature/signing_key_id，插plugin_versions UPLOADED及actor审计。相同name/version+相同全部immutable字段返回原对象/receipt；同name/version不同digest、同digest不同bytes/manifest/signature/key/ABI/schema均409，不覆盖。现有immutable_plugin_packages禁止任何UPDATE/DELETE，不能用`ON CONFLICT DO UPDATE signature`或先存空签名后补。采用INSERT ON CONFLICT DO NOTHING后SELECT精确比较；校验出的config schema digest也必须是受信release中签名值。
2. **VALIDATING真实任务。** 新增最小 `plugin_validation_runs`：id PK、version FK、package/fixture/policy/runtime digests、requesting actor/command、state(PENDING/RUNNING/CLEANUP_PENDING/SUCCEEDED/FAILED)、lease owner/epoch/expiry、bounded result digest/error、开始/结束时间；同version在PENDING/RUNNING/CLEANUP_PENDING至多一active，run的(actor,command_id)唯一且body_digest不可变。start事务在expected revision下UPLOADED（或明确新命令重验REJECTED）→VALIDATING，分配持久run、audit并提交。Core worker领取有界lease后，通过与未来T08相同validator入口加载**public `runPluginFixtures` 真helper路径**（实际export/signature待T04.a接纳后adapter核验），不手写第二份模拟fixtures、不调用旧connectors。校验全部合成fixtures及正常/错误/截断/取消/无usage/精确数字、资源声明，模块memory/imports/预算、ABI/config schema、digest/signature任一失败都不能VALIDATED。OpenAI包未声明的能力不能以空fixtures声称覆盖。
3. **VALIDATED。** worker在无凭证/无网络/无用户内容的受限helper完成，所有子进程停止并wait/reap、临时目录/失败半产物清理、引用/租约释放后，提交finish：验证run lease/epoch和固定package/policy，写严格result及digest，VALIDATING→VALIDATED并actor审计（worker与发起actor均可追溯）。helper超时/trap/取消写有类型失败并阻止发布；若尚未完成stop/wait/reap/临时物清理，run保持CLEANUP_PENDING及恢复租约，plugin可标REJECTED但任务不标完成，不允许新run覆盖它。恢复器只回收本run登记且验证归属的子进程/临时对象，清理确认后才FAILED并释放active slot；持续失败暴露健康故障/待处理责任，不无限创建新任务或删别的目录。每次CLI调用至多新建一个校验run，无无界重试；同命令恢复同run/result。REJECTED重验需新显式command、新run与审计，包bytes仍不可变；VALIDATING中的崩溃恢复先清理再新epoch/重新校验，不把旧未知结果当成功。
4. **PUBLISHED。** 受信bootstrap经同一publish application入口提交稳定publish command，锁version并复核VALIDATED、最新成功run、当前trust policy/digest/ABI/schema未变、not quarantined；同事务CAS→PUBLISHED、递增revision、插immutable `plugin_publication_receipts`（id、version/package、validation_run/result digest、schema/ABI/policy、actor/command、receipt、created_at）与actor audit；审计或receipt失败整笔回滚，不产生PUBLISHED。publication receipt按 `(actor,command_id)` unique、同digest replay原receipt，换体409；发布后再次同意图不制造新发布。状态转换不能靠fixture SQL UPDATE。
5. **binding事务。** 只有发布receipt确认提交后，通过真实 `PUT /admin/providers/{id}/plugin-binding` 所用的同一`bind_channel_plugin_v1` 入口：锁channel和version并校验brand/account、current expected config_revision、PUBLISHED/not quarantined、包receipt、config严格schema和secret reference归属。创建不可变channel_plugin_bindings新revision/config_digest并更新channel.binding_id/config_revision，写old/new digest/actor/reason审计与稳定绑定receipt，同事务提交。相同command同体重放不多revision，不同体409；expected revision过时冲突，不能更新历史binding。失败不建binding、不合成成功；发布成功绑定失败可独立同command恢复，不能回滚/删除已发布包假装全部没发生。

新 `plugin_publication_receipts` 与run结果是有限发布流程证据，不是执行ABI或用量系统。审计复用既有audit_events，追加actor/details字段（历史可空，新插件动作必须strict details，含action/old/new state、digests/schema/fixture result/run/reason/command）；保留immutable audit触发器。plugin_versions新增guard：name/version/package_digest/abi/config_schema身份不可UPDATE，状态和validation_result只有受控状态函数能变。新bootstrap role不获表UPDATE，只获窄入口EXECUTE。验证result包含数字/错误类别/摘要，不持久真实请求样本或任意helper stdout。

**首链其余真实管理：** `/admin/providers`创建brand/account/channel时可以尚无binding，但返回真实IDs且账户为0；无binding不能成为context可执行候选或准入。`PUT .../plugin-binding`实际持久上述revision。`/admin/models/{model}/routes`现createRoute只写currency/rates，T04.b必须保留route现有pricing字段并新增同层ceilings，将二者交给已存在 `upstreamCostConfigurationSchema` 的 `{pricing,ceilings}` strict校验并真实INSERT cost_versions.ceilings；rates每维即使0也需显式ceiling，缺配置拒绝，不靠fixture后补UPDATE。同步POST /admin/models/{model}/routes、PATCH /admin/routes/{id}、routeSchema/RouteInput/Store.createRoute/updateRoute/routeDefinitions和查询；PATCH更换pricing时必须同时提交该版本完整ceilings并新建immutable cost_version，不沿用不匹配旧ceilings或UPDATE旧版本。同步candidate捕获cost_version及ceilings；客户价用真实FundsStore setTariff/其Core管理入口持久billing_prices/ceilings，不把供应价假装客户价。实际字段路由以现有admin定义核对，不能另造/admin/channels别名。

从零首链是：受验证管理员真实建account/channel/model/customer tariff/cost ceilings → 受信bootstrap注册/真helper验证/发布receipt → 真binding事务 → 有凭证的真实RECHARGE事实命令及audit → context/prepare/admission/begin/mock HTTP Chat/耐久数字证据/双方账本。缺任何前置只失败，不替测试填PUBLISHED/binding/ceiling/余额。充值与voucher规则完全沿§8.2，TEST_OPENING不替代充值。

T08负责线上任意管理员包上传、通用异步任务队列/进度UI、完整发布/停用/隔离/回滚运维；必须复用本register/validator/public runPluginFixtures/publish/bind入口和同执行ABI，扩上传来源/任务装配，不建立第二套builtin shortcut。T04.c使用持久PUBLISHED包由Core按digest分发，缺包节点拒绝，不回退任意本地默认。

## 9. Gateway 执行器实际需要与后续边界

T04.c只依赖：context/metadata、admission或next的RESERVED回执、begin首次临时grant、command安全查询、abort、lease与可信证据发布。Gateway local state是dispatch digest+immutable package/config IDs、未消费attempt registry、deadline/AbortSignal、内存正文；资金金额仅用于可核对回执，不在Gateway重算/修改钱包或路由权限。密封凭证与受控transport由T04.c衔接，插件不拿secret。Gateway不import storage、不配置DATABASE_URL/pepper，不把core-client做全Store转发代理。

| 切片  | 此次必须完成的责任                                                                                                                                                                                                                                              | 明确后续                                                                                                                                                  |
| ----- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| T04.b | Core admission/next/begin/abort/command查询、双资金hold、partial/final delta结算、受信builtin注册/真helper验证/发布/绑定、显式成本ceiling管理、最小充值/receipt/audit、带清理权/adoption的可重建quota接口、真实PG/Redis事务故障验收；定义Core消费的稳定数字合同 | 不以本只读plan代替实现；实际依赖T04.a接纳                                                                                                                 |
| T04.c | 三进程/多Gateway、mTLS+启动session、core-client、真实helper/网络发送、同连接WAITAOF、JSON/SSE终态、取消/drain、产物无PG Gateway                                                                                                                                 | 不用合成grant/假helper响应替代                                                                                                                            |
| T05   | 迁移客户资金所有公开管理/查询/退款/授信/债务/争议/人工裁决，完整旧回归与最小权限                                                                                                                                                                                | T04复用金融SQL不代表这些API全迁完                                                                                                                         |
| T06   | 完整brand/account/channel管理、双币种充值/凭证生命周期、调整/人工对账/恢复、报告/余额观察及争议resolution关联                                                                                                                                                   | 观测不改账；充值不复活；不再新增第二channel集合                                                                                                           |
| T07   | 身份目录所有API、所有路由策略、层级配额/健康/Redis丢失恢复、即时撤销全面验收                                                                                                                                                                                    | T04不能把begin的授权屏障或shared-account硬闸留空等待T07                                                                                                   |
| T11   | 资源permit/补证调度、固定原channel/plugin/config的只读与变更动作                                                                                                                                                                                                | 只读补证即使余额0、冻结、普通停用仍允许有界执行，不走“新推理资金许可”；安全quarantine按隔离规则拒绝。cancel/delete另申请一次授权，不因HTTP DELETE自认幂等 |
| T16   | 删除旧Store/execute/connectors/worker旁路、冗余兼容约束/fixtures，目标依赖测试通过                                                                                                                                                                              | 删除本稿过渡admit/record/commit/ingest/persist签名及view写trigger；历史LEGACY只读保留，不保留两个生产准入/结算入口                                        |

资源未来契约：原resource owner/tenant、稳定remote ID、binding/config/cost版本、已定义action只读能力、task lease/epoch、期限与数字预算。只读查询是继续证明既有责任，不新增推理资金承诺；不能用当前余额/普通disabled挡死补证。只读仍受访问权限、受控网络/安全隔离与独立并发上限约束。变更action需要原Gateway/incarnation的单次grant，丢失即action UNKNOWN、不重派。下载URI/签名query只在Gateway内存，Core/PG/Redis/审计只有白名单稳定身份。本文不实现T11。

## 10. 可观察故障/并发验收表（设计推演，尚未实跑）

每个场景用真实Core HTTP/PG/Redis及mock-provider HTTP计数，固定request/attempt/event/journal/ACK逐ID审计，期望来自本表和手算。事务装置允许故障注入，但不能让伪成功代替真实副作用。

| 场景/交错                                                | 期望状态与金额                                                                             | 物理副作用次数/入口                                                           |
| -------------------------------------------------------- | ------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------- |
| 两Gateway同project+intent同指纹，含不同Key               | 一request/首attempt/双方hold；败者自己的命令稳定REJECTED already_accepted；无内容重放      | 至多一grant、一物理发送；新增Core integration                                 |
| 同intent换fingerprint或customer currency                 | 409冲突，原request/价格/币种/hold不变；不得多建另币种hold                                  | 新发送0；保留/纠正旧`billing.test.ts:2258`缺币种指纹fixture                   |
| 同kind/caller/command ID换digest或币种/manifest          | 409command_conflict，原receipt不变；不同caller同UUID不串用                                 | 副作用总计一次；command integration                                           |
| admission commit后切断响应                               | 同command查到原RESERVED Q/X/两个hold；未确认RESERVED不能send                               | 查询/重试新增hold0，后续首次begin最多send1                                    |
| PGcommit前断开/SQL错误                                   | 查询NOT_FOUND或真实已提交receipt，不猜；失败事务无request/双方hold/receipt碎片             | send0；同ID恢复                                                               |
| 两并发begin（同/异命令）                                 | 仅一CAS，首次成功调用有grant，其余只状态/冲突；资金不变                                    | provider计数≤1；重放含grant响应也被宿主registry拒绝                           |
| begin commit成功、grant响应丢失/迟到截止之后             | SEND_GRANTED/UNKNOWN，双方责任保留，不凭命令查重获grant                                    | **send0**（该故障fixture拦住首次响应）；无新attempt自动重发                   |
| grant前abort与begin竞争                                  | abort先赢→ABORTED/NOT_EXECUTED、供应R0/客户按条件RELEASE；begin先赢→abort拒绝/UNKNOWN      | 前者send0，后者≤1；不能两边都宣称成功                                         |
| grant后owner死亡/重启新incarnation                       | 不转移、不重取grant，UNKNOWN仍占用；新启动旧session证明无效                                | 不新增物理发送；恢复器无重发                                                  |
| 撤销/disabled/rebind与begin并发                          | SHARE/UPDATE提交点可解释；管理员响应之后新begin无grant；旧grant保留责任                    | 检验审计时间与事务commit交错；FOR KEY SHARE对照探针不能作为屏障成功           |
| FOR KEY SHARE读wallet与新admission并发                   | 钱包NOKEYUPDATE可继续，不误报资金不足；独立钱包不被热wallet阻塞                            | 保留billing:955/lifecycle:237有效断言；不拿此测试证明撤销                     |
| shared account两channel并发E60、余额100                  | 同(account,currency)最多成功1、另一稳定上游不足；客户失败组无hold；reserved60              | 一grant/发送；无各channel预算超卖                                             |
| 缺成本价/缺任何priced ceiling/缺binding/必要量不有界     | 有类型拒绝，客户与供应hold均0；显式零费率+显式ceiling才可0成本                             | provider调用0；不能用空rates成功                                              |
| partial→final、多个attempt含UNKNOWN                      | 完全符合A/B/C，delta不能sum revision；旧UNKNOWN保留，客户只selected                        | 每attempt≤1send；用SQL独立查head与journal净额                                 |
| request费已知但无tokenusage                              | 已知request费进入spent，未知token有R；request-only价可在有充分最终request事实时complete    | 不能把空usage当全免费/全部最终                                                |
| FINAL后取消/更早attempt迟到FINAL                         | 选定交付不被替换、完整费用不降级；更早attempt单独供应成本                                  | 客户charge一次；每attempt成本一次                                             |
| 同组矛盾FINAL/结算后迟到矛盾FINAL                        | 同组不先charge；已post则保留原C、增保守R/争议，双方freeze                                  | 无第二客户charge；持久conflict/审计；不能忽略矛盾                             |
| 完整成本超E/超余额后充值                                 | 全C入spent，允许balance负，RECONCILE_REQUIRED；充值不改state、其他UNKNOWN仍R               | 正确平衡journal；新admit/next/begin拒绝                                       |
| recharge同external_ref，换voucher bytes/payment currency | 相同payload原receipt；改变任何语义409；审计归属真实actor                                   | balance只加一次，不发生支付调用                                               |
| 充值或结算audit/posting故障                              | 本组evidence/钱/receipt/event同时rollback；其他独立组可提交；失败job不ACK                  | journal/余额/receipt不能半份存在                                              |
| PGcommit后BullMQ ACK丢失、换job重复同event               | 原receipt/head/ledger/event delivery不变，允许任务重投成功ACK                              | 财务新增0；外部event ACK仍独立                                                |
| worker暂停，Gateway证据WAITAOF成功                       | Gateway可正常terminal，队列积压、PG暂未结；恢复worker后双账/event收敛再ACK                 | 发送一次；没有把worker在线误当终态前提                                        |
| worker暂停且WAITAOF失败/连接代次变化                     | Gateway无正常JSON/terminal/EOF，保留UNKNOWN/已知证据责任；job可能实际已持久，恢复后照结    | 不因返回错误自动同intent重发；真实队列/产物测试                               |
| Core节点取quota后死亡、另一Core release/adjust           | PG已提交descriptor可重建；prePG失败无清理权仅draft30s；同lease有权重复release/adjust零追加 | active归零，TPM原bucket只修正一次；资金无释放                                 |
| 账户冻结/普通停用时只读资源补证                          | 新推理拒绝，但原绑定query可继续收FINAL、成本与R收敛；mutate无单次permit拒绝                | T11边界测试，不在T04假实现资源成功                                            |
| 金额计算/余额累计38位溢出与独立peer                      | overflow durable争议/冻结或准入拒绝，不cap；peer正常receipt/账本                           | 不污染同批peer；无无意义常量测试                                              |
| 同source PARTIAL rev1→FINAL rev2、再晚到rev1             | source始终execution；RAW与各CANONICAL为不同id/revision；旧rev仅STALE，不改head             | 追加迁移真实PG/事件查询；无source唯一错误、无后缀；重复receipt新增财务0       |
| 同组同source两条矛盾FINAL、event/revision换体            | 两个RAW/DISPUTED及receipt/conflict FK完整，先判争议不先收费；head不动、保留/扩R、冻结      | 测cost evidence均可沿同一usage链重算；RAW不外发，不误触async/batch终态        |
| A/B共享lease，B BUSY cleanup延迟到A begin后              | B无清理权不发ZREM；A L仍active，新command不能占其并发槽                                    | 真实Redis延迟注入和PG begin；同scope RPM/TPM只扣1次                           |
| 部分scope失败/rollback/commit未知，Redis acquire回执丢失 | 不cleanup、原ID查询；draft到30s/命令deadline失活；不能重建同ID再扣bucket                   | TTL/tombstone与原bucket验证；不得用NOT_FOUND当删除授权                        |
| PG COMMITTED后draft过期或adopt/renew回执未知             | 不grant；同descriptor查询恢复，已过期不能复活；RESERVED可abort+RELEASE_PENDING             | 已grant则UNKNOWN资金保留，租约expire不NOT_EXECUTED                            |
| REJECTED清理与旧caller迟到renew/release                  | REJECTED不能后来COMMITTED；已adopted仅PG屏障可删，RELEASE_PENDING不回ACTIVE                | 真实并发，先commit屏障再Redis，禁止跨系统原子假设                             |
| prepare的proposed IDs被占用/跨tenant/新incarnation       | 身份与归属失败不adopt；已准入重放同UUID，禁止换body/digest重绑                             | public Core/真host session，UUID知晓不授权；next同样                          |
| 配置100/50而捕获e60/30，rates1/3                         | E=U=R30；unknown不退回配置50；捕获digest或E不符不得begin                                   | 真实持久重建e；不从被测算法生成期望                                           |
| 可信PARTIAL input25 nonfinal超过e20、后final10/15        | 人工D C0/R45冻结→C25/R0，B25；不先expense25；最终不自动ACTIVE                              | PARTIAL和ESTIMATED分开；risk RAW与reserve journal可追溯                       |
| 首次builtin安装、签名伪造/缺anchor/fixture失败           | 真实状态依序提交；任一失败不PUBLISHED/不binding；不能信包自带key                           | 真public runPluginFixtures/helper；无任意路径，query读取持久run/audit/receipt |
| 同release重试、同name/version不同bytes、不可变signature  | 同内容同回执，换内容409，无UPDATE包；中断重试清理任务/子进程后收敛                         | 真实Core入口；绑定expected revision竞争唯一新revision                         |
| route缺ceilings/未发布binding/充值audit失败              | 无准入；完整配置与RECHARGE/voucher/audit全真实持久后才可首Chat                             | 不直接SQL补齐前置，不用TEST_OPENING代替充值                                   |

复用已有资金断言的真实位置：`tests/integration/billing.test.ts` 257/289/322/360/575（信用/UNKNOWN/超额）、723/759/798（批次/独立wallet）、991/1109/1146/1167（来源/过期/退款/策略）、1183/1189（有理数/TTL）、1313/1334/1472/1516（交付retry/FINAL争议/迟到资源）、1933/2037/2058/2305（溢出/同组矛盾/原子组）。不要弱化旧断言，只把入口改成真实Core/worker。`tests/integration/schema.test.ts:134`保留延迟上游平衡约束，271保留RECHARGE NULL拒绝。`runtime/usage-queue.ts`同连接耐久和`verify-durability.ts --native`真实停机/恢复继续作为T04.c验收入口；此文未执行它们。

## 11. 最小文件职责与实施顺序

新增路径建议，实施者需按已接纳T04.a实际package exports核对，不能把本文路径当已存在导出：

- `packages/contracts/src/core-execution.ts`：strict RPC/安全receipt/数字manifest/数字证据；可按当前package结构导出，SDK不依赖Core业务代码。
- `packages/storage/src/admission/`：窄repository与领域失败映射；真实Core functions调用、trusted参数与组结果检查，不import Redis。
- `packages/storage/src/upstream-funds/`：充值/receipt/余额与journal读取，实际事务；不代理支付。
- `scripts/seed.ts`或新受控bundled installer、Core catalog/worker validation入口：只消费接纳发行物/外部trust policy，真helper验证、审计发布与binding；T08复用，不能另起执行ABI。
- `packages/storage/src/schema/{enterprise,existing,catalog}.ts`：上文新增字段/表/约束；追加migration snapshot/SQL+manifest及对应schema漂移测试，原2026111202字节不变。
- 新version custom SQL：command arbitration、核心事务wrapper、minimal record_core_usage、supplier delta posting、权限；客户金融算法继续用现有函数。所有资金helper只受入口调用。
- `apps/core-api/src/admission/`及最小identity/catalog/upstream-funds装配：trusted middleware接口、quota前置/cleanup、事务调用、typedHTTP映射。T04.c安装mTLS启动session真实实现，T04.b拒绝没有verified session的真实公网调用。
- `apps/core-worker/src/settlement/`：BullMQ numeric group→一次PG原子提交→ACK，恢复只负责状态/收证，不重复生成；与API共享PG/相同schema。
- `packages/runtime/src/limits.ts`及所属`__test__/`：可重建lease Lua与窄端口；已有Batcher复用。真实Redis并发在integration验证，不能用closure mock当完成证据。
- `tests/integration/core-admission.test.ts`/`core-settlement.test.ts`/`upstream-funds.test.ts`（建议名）：上表真实PG/Redis/HTTP场景；`tests/support/`复用公开fixture，不跨模块私有`__test__`；必要脚本将新目录纳入实际test扫描。

先接纳T04.a并核对明确adapter，再追加schema/约束及纯数值人工例；交付最小bootstrap真实发布/绑定及成本配置，使首链前置可通过产品入口得到；再实现command+admission、next/上界拒绝、begin/abort/lease，最后组证据+双journal+事件、真实最小充值和故障装置。每步先相关检查；T04.b里程碑按package真实脚本跑check/lint/fmt/test/integration/build/artifact，T04.c完整三进程产物与native durability。不要在T04.b提前宣称Gateway已无PG/文本全协议已迁完。后续需要改变本文设计时先由协调者更新权威spec相关决定，不能静默改已接纳baseline。

## 12. 真正待实施对接核验与本轮验证限度

不再向用户确认已定的资金/UNKNOWN/服务划分。以下是实现前必须与已接纳代码核对的技术接点：

1. T04.a实际BillableManifest字段、public runPluginFixtures/helper exports、外部受信signed OpenAI发行物、最终dispatch digest覆盖范围、prepare前session/attempt绑定与输出上界的方式；Core合同只消费必要数字/immutable IDs。需确认partial/final每维权威事实怎样由宿主产生，不能让插件自报一个布尔值绕过成本缺口。
2. T04.c启动session/mTLS方案及跨Core验证、密封credential handle、Gateway本地消费registry、RPC库禁止自动重发grant响应；队列numeric envelope的可信origin证明/Redis ACL方案需一并核验。静态cert加自报incarnation不合格。
3. 当前Bun SQL驱动事务返回与连接丢失的行为、NOWAIT子事务及约束切换方式，需真实PG17并发fixture验证；现代码没显式isolation设置，不能仅靠设计推演宣称无死锁。
4. 新增私有allocator/旧适配对proposed IDs及旧intent constraint的同步、§4.1a全部SQL和事件查询迁移、T04.c撤权/T16删除的实际调用清单，须由集成者以源码与真实PG核验。对任何历史project+intent重复或已有无法映射的upstream_costs，迁移应拒绝并交协调者按事实处置，不编造回填。
5. 新version编号/文件owner需协调者分配；schema包exports、test脚本扫描与最小权限role名称按当前实际实现定义。旧`gateway_runtime/gateway_worker`角色当前权限事实不能直接充当新Core最小权限结论。
6. T06争议resolution如何关联每一disputed revision和PARTIAL风险来源并审计恢复ACTIVE；T04.b保守冻结/receipt完整交付，不开放自动恢复或假对账。

本轮仅静态读源、核对官方PG17锁/异常/ON CONFLICT/FK保证及独立Fraction/有限状态设计推演，未运行产品测试、集成、构建、artifact、container、durability、backup或容量；产品用例执行数**0**。现有主工作树115单测/286集成/artifact9/native durability8仅先前foundation/native基础，不能作为本设计通过证据。有限模型不执行Lua/SQL/HTTP，不证明网络时钟、真实锁行为或helper产品流程通过。修订处置与实际设计核对结果见 `.local/T04b-design-fix1-report.md`；未决接点不是新业务问题，本稿不自行解锁实施。

详细来源hash、决策摘要、未决技术对接和未执行项见同目录 `T04b-design-fixed.json`。
