# Core 签名数字证据与双方结算实现单元

本记录对应 T04.b.2b，占用追加版本 `2026111207`、`2026111208`（④ 认证列及⑥审查修复）与 `2026111209`（⑦复核修复），在本机 main 工作区按里程碑推进（⑤ 依主规格 T04.b.2b 边界澄清追加）。设计依据为 [Core 准入、单次发送许可与双方资金事务](enterprise-v2-core-execution.md) §4.1a、§7、§8；本文只记录实现中对该设计的具体落点和修订，不是整个企业迁移的完成声明。

## 里程碑

| 里程碑 | 范围                                                                                                 | 状态   |
| ------ | ---------------------------------------------------------------------------------------------------- | ------ |
| ①      | v2026111207 schema：证据角色与 snapshot 谱系、`core_evidence_receipts`、供应成本证据外键、旧路径隔离 | 已实现 |
| ②      | 供应累计成本 C 与派生 R、供应平衡 journal，§7.4 人工算例 A/B/C/D                                     | 已实现 |
| ③      | `persist_core_evidence_groups_v1`：归一、争议、风险冻结、客户 selected 结算、事件交付及对外 DTO      | 已实现 |
| ④      | Core client 数字证据签名、producer 撤销屏障、worker 提交后 ACK                                       | 已实现 |
| ⑤      | Bun plugin-host 观察桥；真实 HTTP→插件/宿主→数字观察→签名→worker 首链                                | 已实现 |
| ⑥      | 独立审查发现修复：v07 复原、v08 追加、最终性/重放/争议结清/NOWAIT/拒绝审计/告警/观察桥维度           | 已实现 |
| ⑦      | 复核发现修复：v2026111209 组内重放锚定、观察桥缓存写入维度；v07 推送历史登记                         | 已实现 |

## ① schema 落点

`usage_event_index` 追加 `record_kind`、`snapshot_revision`、`snapshot_digest`、`final_dimensions`、`parent_evidence_id`、`input_fact_id`，`cost_atoms` 改为可空，并新增 `(id,attempt_id)` 唯一键供同 attempt 复合外键引用。`usage_record_kind` CHECK 固定各角色的列形状：迁移前 Gateway 直写行必须保留成本且无 snapshot 字段；RAW 无成本、不完整；CANONICAL 必有成本；DISPUTED 可在溢出时无成本但此时不得标记完整。`usage_snapshot_revision_unique` 只约束非空 revision。

`evidence_lineage` 触发器在插入时校验：有 Core permit 的 attempt 只能写签名 snapshot，没有 permit 的 attempt 只能写 Gateway 直写行；source 必须在 §4.1a 白名单内，不接受后缀；`final_dimensions` 无重复且只含已定义的计量维度；CANONICAL/DISPUTED 的 input_fact 必须是同 attempt 更早 revision 的 RAW，parent 若存在必须是同 attempt 更早 revision 的 CANONICAL。

`core_evidence_receipts` 按 §4.1a 建立，三种唯一性分别是投递内容、event 认领和 revision 认领。`origin_identity` 沿内部身份 `service:instance_id` 形式，只接受 `gateway`。receipt 与 `usage_conflicts` 的互相引用使用 `DEFERRABLE INITIALLY DEFERRED` 外键；receipt 不可 UPDATE/DELETE。`usage_conflicts` 追加 incoming/disputed evidence 与 incoming receipt，三者全空（历史冲突）或全有。

`upstream_costs` 追加非空 `evidence_id` 与 `cost_version_id`，证据通过 `(evidence_id,attempt_id)` 复合外键限定同 attempt，revision 从 1 开始。迁移首句检查该表为空，非空时以 `upstream_costs_history_unmapped` 整体回滚，不伪造历史证据。

`usage_events` 视图以 `CREATE OR REPLACE` 追加 `record_kind`、`snapshot_revision`、`final_dimensions` 并过滤 RAW，保留原有触发器、列默认值和角色授权；`Store.usage`、`Store.request` 与事件消费者 replay 都经该视图读取，因此不会暴露 RAW，消费者 cursor 跨过 RAW 的 seq 空隙仍单调推进。

## 对设计的修订

1. 迁移前 Gateway 直写行的 `record_kind` 命名为 `GATEWAY`，不使用设计稿中的 `LEGACY`。仓库架构约束禁止任何迁移 SQL 出现 `LEGACY` 字样，用以阻止旧升级路径回流；该约束保持不变。
2. 旧 `usage_events_attempt_id_source_key` 已删除，替换为只覆盖 `record_kind='GATEWAY'` 的部分唯一索引 `usage_gateway_source_unique`。`record_usage_evidence` 的仲裁改为 `ON CONFLICT(attempt_id,source) WHERE record_kind='GATEWAY'`，迁移前 Gateway 路径的同 source 唯一、后缀与冲突判定逐字保持；签名 snapshot 只受 revision 唯一和 receipt 认领约束。删除该过渡索引、后缀生成以及 `resources.ts` 的 `async_partial_<hash>` 生产者迁移，随 T11 资源迁移和 T16 旧入口删除一起执行。
3. `commit_usage_evidence`、`settle_*` 中直接读 `usage_event_index` 的 source 匹配，在 ① 中由 `evidence_lineage` 保证迁移前 attempt 只有 Gateway 直写行，行为不变；Core attempt 只接受 accepted CANONICAL 的过滤随 ③ 的结算调用一起实现。
4. 对外 usage DTO 的 `disputed_snapshot` 语义、可空成本和 snapshot revision 字段属于公开契约变更，随 ③ 首次产生 DISPUTED 行时同步 OpenAPI 与测试。

## ① 验证

| 检查                                                       | 结果                                                                        |
| ---------------------------------------------------------- | --------------------------------------------------------------------------- |
| `tests/unit/architecture.test.ts`                          | 3 通过，含 Drizzle 声明与 0005 snapshot 无漂移、迁移 SQL 禁用词             |
| `tests/integration/core-execution-upgrade.test.ts`         | 6 通过，新增 v6→v7 保留 Gateway 用量和同 source 唯一、历史成本拒绝升级      |
| `tests/integration/core-settlement-schema.test.ts`（新增） | 1 通过 / 11 断言：Core attempt 拒绝直写、谱系非法组合拒绝、RAW 不出公共查询 |

既有升级测试中写死的已应用版本列表、迁移计数和 `usage_events` 整行快照，因新增版本和视图列属于目标行为变化而同步更新；新增列另以显式断言验证历史行全部为 `GATEWAY` 且无 snapshot 字段。

里程碑全量门禁（本机，Go 1.27.4 位于 `.local/toolchain`，依赖经 `bun install --frozen-lockfile` 恢复）：

| 命令                     | 结果                                                                           |
| ------------------------ | ------------------------------------------------------------------------------ |
| check / lint / fmt:check | 222 个文件无类型错误；0 错误/警告；325 个文件已格式化                          |
| test:native              | Go 测试通过                                                                    |
| test                     | 284 通过 / 4063 断言 / 40 文件                                                 |
| test:integration         | 453 通过 / 13 失败 / 10371 断言 / 28 文件；13 项全部为 native-durability，见下 |
| build                    | 成功                                                                           |
| test:plugin-artifact     | 15 场景 / 24 步骤 / pending 0                                                  |
| test:artifact            | Gateway 9、catalog 7、identity 7、financial 9 项通过                           |

13 项 native-durability 失败在改动前基线（450 通过 / 13 失败）中同样存在，报错为 `PG_TOOLS_DIRECTORY and DURABILITY_REDIS_SERVER are required absolute native tool paths`：本机 PostgreSQL/Redis 运行在 OrbStack 容器内，宿主没有原生 `initdb`/`redis-server`，属于环境缺口，未在本里程碑处理。`test:durability`、`test:backup`、`test:container` 未运行。

## ② 供应成本与剩余占用

`core_supplier_exposure_v1(rates,estimated,usage,finals,risk)` 是纯计算函数，按 §7.1/§7.1a 两次复用 `exact_billing_price`：已 final 维度的实际量求 C；联合敞口对已 final 维度取实际量、其余维度取 `max(e, 可信超界 PARTIAL 观察)` 后一次 ceil，减 C 得 R。同时返回仅以 e 计算的 `base_reserve_atoms`、`complete`、`overrun`（任一 final 量超 e，或以 e 计的联合敞口超 E）和超界风险维度。捕获的 e 缺任一费率维度、或 final 维度缺实际量，一律拒绝。

`core_supplier_risk_v1(attempt)` 从该 attempt 的 receipt 风险维度及其关联 RAW 取各维度最大观察量，不新增缓存列。

`core_supplier_settle_v1(attempt,snapshot,reference)` 在账户→余额→hold 锁序下记一次供应 delta：旧 C 取 head，旧 R 取该 attempt 全部 `upstream:reserved` 分录净额（RESERVE 时写入的 E 及之后各次调整），新 R 取推导值与争议下限的较大者；争议下限在函数内由该 attempt 全部有成本的 DISPUTED 快照按 `C+R−head C` 取最大值得出（③ 起不再由调用方传入）。存在 DISPUTED 或数值溢出 receipt 时不移动 head，只调整占用。传入 CANONICAL 时追加 `upstream_costs` revision（attempt 内 max+1）并移动 head；传 NULL 时只按当前 head 重算风险占用。cost_delta 为负直接拒绝，不产生自动退款。非零 delta 写一条平衡 journal：`upstream:available` −(Δcost+Δreserve)、`upstream:reserved` +Δreserve、`upstream:expense` +Δcost，零值腿省略；余额缓存 balance −Δcost、spent +Δcost、reserved +Δreserve，允许 balance 为负。complete 且 R=0 时 hold 为 SETTLED 并记 final_cost_atoms，否则为 PARTIAL。overrun、超界风险、争议或数值溢出时，ACTIVE 账户置 RECONCILE_REQUIRED（更严格状态保持不变）并写 `upstream_audit_events`；不提供自动恢复 ACTIVE。

账户行在该函数中以 `FOR NO KEY UPDATE` 先锁，使冻结更新不违反账户→余额的锁序；代价是同账户的结算与准入（账户 `FOR SHARE`）串行。是否改为仅在需要冻结时升级锁，留待 T17 容量实测给出证据后再决定。

| 检查                                                               | 结果                                                                                                                                           |
| ------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| `tests/integration/core-supplier-exposure.test.ts`（新增）         | 1 通过 / 14 断言：§7.4 A/B/C/D 各阶段 C、R、complete、overrun 及风险维度，1/3+1/3、125ms×8/1000、9007199254740993 精度，缺 e 与缺 final 量拒绝 |
| 受影响集成（schema、升级 4 个文件、core-execution、新增 2 个文件） | 40 通过 / 0 失败 / 312 断言                                                                                                                    |
| check / lint / fmt:check / architecture                            | 223 文件无类型错误；0 警告；326 文件已格式化；3 通过                                                                                           |

`core_supplier_settle_v1` 的余额、journal、head 与冻结效果需要真实证据组驱动，按 §7.4 的余额序列在 ③ 通过 `persist_core_evidence_groups_v1` 验证，② 不单独以私有函数调用冒充公开路径验收。

## ③ 证据组写入、客户结算与对外 DTO

`persist_core_evidence_groups_v1(groups)`（`SECURITY DEFINER`，仅经 `@gateway/storage/settlement` 的 `createSettlementStore(db).persist` 调用）每组一个子事务：组内失败只回滚该组，返回 `REJECTED`（P0400/P0403，带错误名）或 `ERROR`（只带 SQLSTATE），不影响同批其它组。组结构由 `coreEvidenceGroupV1Schema`（≤64 事件、≤128 组）在应用层校验，SQL 侧 `core_evidence_validate_v1` 再做同等校验，不信任调用方。

单组处理顺序：

1. 校验并按 attempt/origin/source/revision 排序；body digest 为 `core_digest_v1(event − event_id)`。
2. 以 event 认领与 revision 认领键取事务级 advisory 锁（NOWAIT 语义，取不到即 `core_busy`），再按 准入锁序 账户→客户钱包→供应余额→请求→attempt→permit→供应 hold→客户 hold 预锁，后续 `core_supplier_settle_v1` 与 `settle_requests` 只是重入已持有的锁。
3. 主体校验：permit 必须处于 SEND_GRANTED/UNKNOWN/TERMINAL，`owner_service:gateway_instance_id`、owner session、incarnation 与 permit epoch 与事件 origin 全等，否则整组 `evidence_subject_invalid`。③ 不改 permit 状态。
4. NOT_EXECUTED 归一为 FINAL 且全部计费维度为 0 的快照。
5. 先全组预扫描再记账：同 body 重投为 DUPLICATE（返回原 receipt，不写新行）；同 event 异 body、同 revision 异 body 为 CONFLICT；同 revision 同 body 新 event 为 REVISION_REPLAY；与当前 CANONICAL 或组内其它事件对同一 final 维度给出不同量时，双方都标为争议。未争议事件先处理，争议事件后处理，保证组内先到事实不会先收费再发现矛盾。
6. 每个非重复事件写一个 RAW；ACCEPTED 再写 CANONICAL（合并已 final 维度，旧 final 不被覆盖），争议写 DISPUTED 与 `usage_conflicts`、置 `attempts.metering_conflict`（既有 `register_wallet_dispute` 冻结客户钱包）；数值溢出写无成本 DISPUTED 与 `numeric_fault='COST_OVERFLOW'`；ESTIMATED 与 STALE（同 origin/source 已接受更高 revision）只留 RAW 与 receipt。CANONICAL 的 finality 按 §345 由客户口径决定：输入 FINAL，且客户价格为 `PROVIDER_USAGE` 时其全部计价维度均已 final，才为 FINAL，否则为 PARTIAL（无数量时 UNKNOWN）；非 `PROVIDER_USAGE` 价格不等待用量维度。供应 `complete` 独立判定，可能在客户快照 FINAL 时仍为 false。
7. 只为 CANONICAL/DISPUTED 写 `event_deliveries`；RAW 与重复投递不外发。
8. 每个产生资金影响的 attempt 调一次 `core_supplier_settle_v1`；请求终态只在 `selected_attempt_id` 为空且 billing_outcome 为 PENDING/UNKNOWN 时 CAS 一次，随后对已终态且无 charge 的请求调 `settle_requests`，任一 ERROR 回滚整组。客户结算读 `attempts.evidence_id`，它只指向 accepted CANONICAL，因此 Core attempt 不会按 RAW/DISPUTED 收费，修订 3 的过滤由此落地。

对外 usage DTO（`Store.usage`、`Store.request().usage`、事件消费者 claim 与 OpenAPI `UsageEvent`）：`semantics` 为 `attempt_snapshot` 或 `disputed_snapshot`；新增 `snapshot_revision`（迁移前 Gateway 行为 null）与 `cost_revision`（由该快照入账的供应成本 revision，未入账为 null）；`cost_atoms` 仅在溢出的 disputed_snapshot 上为 null。`docs/openapi.json` 由 `bun run openapi` 重新生成。

### ③ 验证

`tests/integration/core-settlement.test.ts`（新增，6 用例），期望值全部按 fixture 常量手算（供应费率 input/output 各 1/3 atom，捕获 e=60/30，E=30，充值 1000；客户按请求完成计 100）：

| 用例                               | 手算要点                                                                                                                                                                                                       |
| ---------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| PARTIAL→FINAL、重投、迟到、重放    | input 31 final：C=⌈31/3⌉=11，R=⌈61/3⌉−11=10 → 989/10/11；FINAL 31+2：C=11，R=0，hold SETTLED，charge 100 一次；重投 DUPLICATE、更低 revision STALE、同 revision 新 event REVISION_REPLAY，余额与 charge 均不变 |
| 超 E 的 final                      | 120+60：C=60，余额 50−60=−10，账户 RECONCILE_REQUIRED，审计 `supplier_cost_overrun`                                                                                                                            |
| 可信超界 PARTIAL、ESTIMATED、final | input 90 非 final：R=⌈(90+30)/3⌉=40，冻结；ESTIMATED 500/500 不动账；final 30+15：C=15，R=0 → 985/0/15，冻结保持                                                                                               |
| 组内与跨组矛盾                     | 同组两 FINAL 30 vs 31：均 CONFLICT，不收费、不入账，DTO 为两条 disputed_snapshot（成本 15/16）；已入账 15 后来 33+15：head 不动，争议下限 ⌈48/3⌉−15=1                                                          |
| 重试与迟到旧 attempt               | 两 attempt 各占 30；新 attempt final 15 → 985/30/15 并 selected；旧 attempt 迟到 60+30：C=30 → 955/0/45；selected 不变，charge 仍一条 100                                                                      |
| 伪造 owner session                 | 该组 REJECTED 且无 receipt；同批 NOT_EXECUTED 组 ACCEPTED，供应占用全额释放、客户 hold RELEASED                                                                                                                |

未覆盖（③ 当时）：`OVERFLOW` 路径。⑥ 已用 38 位费率用例覆盖。

`tests/integration/lifecycle.test.ts` 在 v2 基线上原用当前 `Store.claimEvents` 制造“升级前已 ACK”的历史状态；该查询现读取 `record_kind`，而生产 Gateway 启动时要求 schema 恰为 `SCHEMA_VERSION`，不会以新代码读旧 schema。因此升级前的 lease 改为只用 v2 表的 SQL，`ackEvents` 返回 1 及升级后 replay/claim 的断言保持不变。

里程碑全量门禁：

| 命令                     | 结果                                                                                      |
| ------------------------ | ----------------------------------------------------------------------------------------- |
| check / lint / fmt:check | 225 个文件无类型错误；0 错误/警告；328 个文件已格式化                                     |
| test:native              | 通过                                                                                      |
| test                     | 284 通过 / 4065 断言 / 40 文件                                                            |
| test:integration         | 460 通过 / 13 失败 / 10480 断言 / 30 文件；失败集合与基线逐项一致，均为 native-durability |
| build                    | 成功                                                                                      |
| test:plugin-artifact     | 15 场景 / 24 步骤 / pending 0                                                             |
| test:artifact            | Gateway 9、catalog 7、identity 7、financial 9 项通过                                      |

首轮全量集成中 `business dependency load failure closes the preflight fixture before any service starts`（native-durability）失败一次，单独重跑与第二轮全量均通过，该用例不涉及本单元改动的表或函数，记为负载下的时序波动。`test:durability`、`test:backup`、`test:container` 未运行。

## ④ 签名证据、producer 撤销屏障与提交后 ACK

### 签名

`signedCoreEvidenceEventV1Schema` 为事件加 `signature`（Ed25519，base64url 64 字节）。签名域为 `JSON.stringify(evidenceProofFields(event, sha256(canonical(event))))`，字段为 `stg.internal.evidence`、`1`、origin identity、origin session、origin incarnation 与事件规范化摘要；与命令证明分属不同域，不能互相重放。`createCoreRpcClient().signEvidence(body)` 只在 startup 后、且服务为 `gateway` 时可用，origin 三字段由当前启动回执填入，调用方不能自报。

`createSettlementStore(db).persist` 在事务外按 `origin_session_id` 读取不可变的 `public_key_spki` 验签（ID-R2 第 1 步）。组内任一事件验签失败或 session 不存在时，该组整体为 `REJECTED evidence_signature_invalid`，不进 SQL、不写 RAW/receipt、不认领 event/revision；同批其它组照常处理。验签通过后向 SQL 传入签名 hex、签名域摘要与 session 公钥摘要；SQL 再核对公钥摘要等于 session 与 permit 记录、session 的 instance/incarnation/certificate 与 permit 一致。新 session（含同证书重启）替原 owner 签名的事实以 `evidence_subject_invalid` 拒绝。

### 撤销屏障

`core_evidence_group_v1` 在 advisory 锁之后、任何资金锁之前，按 instance（service,instance_id）→ certificate → session 的全局序对组内全部 producer 身份行 `FOR SHARE NOWAIT`，锁持至外层事务结束。撤销函数 `internal_identity_change_v1` 以 `FOR UPDATE` 取同序，因此：撤销未提交时收证得到 `55P03`，组回滚为 `ERROR`、worker 不 ACK；撤销提交后重试读到新值。

处置：精确重投（同 event、同 body、同签名域摘要与公钥）先于身份处置判定，返回原 receipt，不因后续撤销而改判。未锚事实若 instance/certificate/session 任一 `security_revoked_at` 非空，记为 `QUARANTINED / SECURITY_REVOKED`；permit 仍为 RESERVED/ABORTED 且从未 grant 时记为 `QUARANTINED / UNGRANTED`。隔离事实只写 RAW 与 receipt，不认领 revision，不进入 canonical、成本 head、任一侧流水、请求终态或敞口释放。session 自然过期、普通 disable/drain、证书 RETIRE 不是隔离谓词。

`core_evidence_receipts` 增加 `authentication_disposition`、`authentication_reason`、`producer_key_sha256`、`signature_version`（=1）、`signature`（64 字节）、`signature_payload_digest`，均 NOT NULL 且不可变；`core_receipt_authentication` CHECK 固定 VERIFIED⇔NORMAL、QUARANTINED⇔outcome QUARANTINED 及隔离行不得指向 canonical/disputed/conflict。

### worker

`packages/runtime/src/usage-queue.ts` 的耐久 producer/consumer 增加 `QueueChannel` 参数：`usageChannel` 保持原队列名 `usage-v1` 与前缀 `{ns:usage}`，在途任务不受影响；`evidenceChannel` 使用 `core-evidence-v1`、`{ns:core-evidence}`，载荷为 `{version:1,groups}`。生产侧沿用同一 WAITAOF 耐久确认。

`apps/core-api/src/settlement/worker.ts` 的 `createSettlementWorker` 消费 evidenceChannel：processor 等 `persist` 的外层 PG 事务提交后才 resolve，BullMQ 随后 ACK。任一组 `ERROR`（锁忙、PG 不可用）则抛错，整个 job 不 ACK 并按既有有界退避重试，已提交的组在重投时按 receipt 收敛为 DUPLICATE；`REJECTED` 组 ACK 并经 `onRejected` 告警，不无限重投。

### 对设计的修订（④）

5. ~~认证字段并入 v2026111207~~（已被⑥修订 11 取代）：④ 曾把认证列并入已推送的 v07 并改变其校验和；独立审查判定违反迁移不可变规则，⑥ 已把 v07 复原为 `30218b0` 内容，认证列改由 v2026111208 追加。
6. 设计稿拟加的 `producer_session_id` 不单独建列，复用已有、带外键的 `origin_session_id`；producer 证书与 instance 由该 session 不可变派生。
7. 签名在 storage 入口以 Node Ed25519 验证，PG 无原生 Ed25519；SQL 只接受该入口传入的已验证签名域摘要，并独立复核身份绑定。该入口仅供受信 Core 进程调用，函数对 PUBLIC 撤权。
8. worker 模块暂置于 `apps/core-api/src/settlement/`；独立 core-worker 进程入口、角色与部署随 T04.c 三进程切分落地。
9. ③ 中组内重复投递若排在被判争议的首份之前处理，会读不到首份 receipt 而返回空 receipt_id；④ 改为重复投递在全组其余事件之后处理，并以回归用例覆盖。
10. ③ 推送版本把 CANONICAL 的 FINAL 判定绑在供应 complete 上，与 §345 不符：请求完成计价下，供应维度尚未 final 的 FINAL 证据被记成 PARTIAL。④ 改按客户口径判定，并以两条回归用例覆盖（请求完成计价一条修复前失败、修复后通过；按量计价一条覆盖 PARTIAL 等待与随后 FINAL 收费 30×1+15×2=60）。测试 fixture `bootstrap` 新增 `customerBasis` 参数以构造按量计价的客户价格。

### ④ 验证

| 检查                                                             | 结果                                                                                                                                                                                                                                                                                                            |
| ---------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `tests/integration/core-evidence-authentication.test.ts`（新增） | 5 通过：篡改签名整组拒绝且不占 event，原签名随后照常结算（989/10/11）；session 撤销后已锚事实精确重投返回原 ACCEPTED、新事实 QUARANTINED 且双方资金与请求不变；证书撤销未提交时收证 `55P03` 且无 receipt，提交后重试 QUARANTINED；未 grant permit 的用量 QUARANTINED/UNGRANTED 且占用 30 保持；组内重复投递回归 |
| `tests/integration/core-settlement-worker.test.ts`（新增）       | 1 通过：身份锁被占时 job 进入退避、无 receipt、余额 1000/30/0；释放后 ACK 且结算 985/0/15；同组换 job 重投 receipt 仍 1 条、charge 仍一条 100；篡改组 ACK 并告警 1 次                                                                                                                                           |
| 组内重复投递回归                                                 | 恢复旧处理顺序时该用例因 receipt_id 为空被 schema 拒绝而失败；修复后通过                                                                                                                                                                                                                                        |
| 受影响集成（9 个文件）                                           | 38 通过 / 0 失败 / 358 断言                                                                                                                                                                                                                                                                                     |

既有 `tests/integration/core-settlement.test.ts` 改为由真实 core-client 签名，伪造用例改为同证书新 session 替原 owner 签名。`tests/integration/billing.test.ts` 中一处 `InstanceType<typeof UsageConsumer>` 类型标注因 consumer 变为泛型改为直接引用默认实例类型，断言未变。

未覆盖（④ 当时）：ingester 先取得 SHARE、撤销者等待的顺序；instance 级撤销；多 owner 交叉组的锁序。⑥ 已全部补齐：在测试控制的事务中调用 SQL 入口持锁。

## ⑤ plugin-host 观察桥与首链

主规格 T04.b.2b 边界澄清要求：签名对象是宿主验证后的归一化数字观察，不在 core-client 另写协议解析；真实 HTTP→插件/宿主→观察→签名→worker 首链在本单元可测。

`packages/plugin-host/src/evidence.ts` 的 `observeAttempt(attempt, { attemptId, permitEpoch, record })` 包装宿主 attempt 的 `invoke`/`transformJSON`：只观察宿主已按 ABI 与 capabilities 校验过的 `output`/`failure` 结果，不接受外部 JSON。累计 usage 快照或 finality 变化时生成一条 `source=execution`、revision 单调递增的观察；FINAL 时 `final_dimensions` 取本次报告中可完整定价的 kernel 定价维度（不含 `reasoning_tokens`；`cache_write_tokens` 聚合须两个 TTL 分区都报告），否则为空（⑥ 修订 13）；failure 携带的 usage 记为 UNKNOWN。`finish(outcome)` 以最后快照附 attempt/request 终态追加一条，只能调用一次。`record` 由调用方注入（签名与入队），观察在宿主结果返回调用方之前写入，写入失败即向调用方抛错，不先放出输出。plugin-host 不依赖 core-client，Go/WASM/SDK ABI 未变。

首链测试只在测试装配中组合：Core admit→begin 取得 transient send，真实 WASM openai-chat 插件 prepare，经本地 HTTP 上游发送（校验收到的 bearer 为 begin 下发的供应凭证、模型映射与输出上限），response 经宿主转换为 FINAL 观察，`client.signEvidence` 签名后经 evidenceChannel 入队，settlement worker 提交后 ACK。Gateway 进程内的实际装配随 T04.c 三进程切换。

### ⑤ 验证

| 检查                                                         | 结果                                                                                                                                                                                                                            |
| ------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `packages/plugin-host/src/__test__/evidence.test.ts`（新增） | 4 通过：JSON FINAL 观察与终态追加（revision 1/2、event 不同）；SSE 只在累计快照变化时记录且末帧前不为 FINAL；无用量终态为 UNKNOWN 且拒绝二次 finish；record 失败时宿主结果不返回                                                |
| `tests/integration/core-first-chain.test.ts`（新增）         | 1 通过 / 9 断言：上游恰收 1 次、凭证 `Bearer synthetic-upstream-secret`；两条 receipt 均 ACCEPTED/VERIFIED；供应 985/0/15、charge 一条 100、请求 SUCCEEDED/SUCCESS 且 selected 为该 attempt；公开 usage 为 FINAL 30/15、成本 15 |

首链测试的 worker 并发设为 1：两条观察分属两个 job，并发处理时若 revision 2 先提交，revision 1 按设计记为 STALE（已由 `core-settlement` 的 STALE 用例覆盖），资金结果相同；为使该用例期望确定而固定并发，不代表生产并发配置。

未覆盖：settlement 独立产物检查（无独立 core-worker 进程入口，随 T04.c）。SSE 端到端首链已于 ⑥ 补齐。

④⑤ 合计全量门禁（⑤ 完成后一次运行）：check 231 文件无类型错误；lint 0；fmt 334 文件；test:native 通过；test 288 通过 / 4073 断言 / 41 文件；test:integration 469 通过 / 13 失败 / 10534 断言 / 33 文件，失败集合与基线逐项一致（native-durability 环境缺口）；build 成功；test:plugin-artifact 15/24、pending 0；test:artifact Gateway 9、catalog 7、identity 7、financial 9。`test:durability`、`test:backup`、`test:container` 未运行。

## ⑥ 独立审查与修复

全新上下文审查者只读审查 `f05f703..3f2e1e0`，发现经实现者逐条对照代码核实。每条缺陷先写回归用例，再把修复前的函数体与约束临时换回验证其失败，最后恢复修复版确认通过。这次替换只在本地进行，复原后的校验和与 manifest 一致。

| #   | 发现                                                                                                           | 处置                                                                                                                                                                                                           | 回归证据                                                                                                                 |
| --- | -------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| 1   | FINAL 之后的 UNKNOWN/PARTIAL 输入把 CANONICAL 降为 PARTIAL，按量计价客户永不收费（违反 §7.1）                  | 快照最终性改由“本输入为 FINAL 或该 attempt 已有 ACCEPTED 的 FINAL/NOT_EXECUTED 收据”判定，再叠加 §345 客户口径                                                                                                 | `core-settlement-regressions`：修复前快照为 PARTIAL、无扣费；修复后 FINAL/FINAL、扣 60、客户 hold SETTLED、供应 985/0/15 |
| 2A  | 原收据为 ESTIMATED/OVERFLOW/无头 STALE 时，同版本同内容重放违反 `core_receipt_outcome` 约束（23514），job 毒化 | v08 约束只要求 ACCEPTED 有 canonical；REVISION_REPLAY 精确指向原 RAW 与原 canonical（可空，§4.1a/字段定义允许），不再回落到当前头                                                                              | 修复前 23514；修复后 ESTIMATED→REVISION_REPLAY，原 RAW、canonical 空、无新快照                                           |
| 2B  | 组内同版本两份副本、首份被判争议时，第二份按重放处理却读不到认领（23502）                                      | 预扫描后把组内副本的争议与隔离状态继承自首份，并按新事实处理；数据库中仍无锚时以可重试 BUSY 报错，不写半成品                                                                                                   | 修复前 23502；修复后两份均 CONFLICT、账户 RECONCILE_REQUIRED                                                             |
| 3   | 证据组资金锁阻塞等待，违反 §5“只 try 不等待”                                                                   | 账户、钱包、余额、请求、客户 hold、attempt、permit、供应 hold 全部 NOWAIT，并按 §5 第 5 级把 billing_holds 移到 attempts 之前；身份锁排序加 `COLLATE "C"`                                                      | 修复前约 2016ms 后才超时；修复后立即返回 55P03，未写收据，释放后重投正常结算                                             |
| 4   | 观察桥按字段出现加全局 FINAL 推断 `final_dimensions`（违反 §7.1）                                              | 只取 kernel 定价维度：`reasoning_tokens` 不计入；`cache_write_tokens` 聚合仅在两个 TTL 分区都报告时计入                                                                                                        | `evidence.test.ts` 新增 2 条，修复前失败                                                                                 |
| 4b  | 记录失败后去重标记已更新，同一观察再也不会被记录                                                               | 记录成功后才更新去重标记                                                                                                                                                                                       | 新增 1 条，修复前失败                                                                                                    |
| 5   | 已推送 v07 的校验和被改                                                                                        | v07 复原为 `30218b0` 内容；v2026111208 追加认证列、拒绝审计表与全部函数修复。升级前若已有 `core_evidence_receipts` 行（只可能来自未签名的旧 v07）则以 `core_evidence_receipts_unsigned_history` 拒绝并整体回滚 | `core-execution-upgrade` 新增 2 条：空表升级保留前 6 版校验和；有旧收据时拒绝且回滚                                      |
| 6   | 争议未决时供应 hold 仍被标为 SETTLED 并写 final_cost（违反 §7.2）                                              | SETTLED 与 final_cost 追加“无争议、无溢出”条件                                                                                                                                                                 | 修复前保持 SETTLED/15；修复后 PARTIAL、final_cost 空、RECONCILE_REQUIRED                                                 |
| 7   | 组内“30、31、30”三方主张中，第三份被接受为 canonical                                                           | 组内争议维度标记为 contested，之后的同维度主张一律判争议                                                                                                                                                       | 修复前出现 CANONICAL；修复后三份 CONFLICT、无 CANONICAL                                                                  |
| 8   | 签名无效/主体不符没有技术审计；UNGRANTED 隔离无告警；身份锁未按 ASCII 序                                       | 新表 `core_evidence_rejections`（仅标识字段，单批 ≤8192 行，不可变，对 PUBLIC 撤权），由 SQL 入口在同一事务内写入；worker 新增 `onQuarantined`；排序 `COLLATE "C"`                                             | 审计 1 条（修复前失败）；隔离告警由 worker 用例覆盖                                                                      |
| 9   | 任务重试时 REJECTED 组重复告警；队列不传 channel 时静默落到 usage-v1（隐式兼容层）                             | 拒绝告警只在本次确认（ACK）时发出；隔离告警只计非重复收据；`channel` 改为必填，24 处旧调用显式传 `usageChannel`                                                                                                | worker 新增 1 条：同一 job 重试两次，拒绝告警只发一次（修复前失败）                                                      |
| 10  | 供应敞口任何异常都被记为数值溢出                                                                               | 只捕获 `numeric_value_out_of_range` 与 `exact_billing_price` 的溢出信号（`price exceeds amount range`），其他异常让整组报错重试；另检查 C+R 不超过 38 位                                                       | 38 位费率用例：OVERFLOW、`COST_OVERFLOW`、RAW+DISPUTED、账户冻结。非溢出异常经公开入口不可达，只经代码审阅               |

### 对设计的修订（⑥）

11. 本单元占用 v2026111207 与 v2026111208，偏离主规格“只追加 v07”的表述。原因：v07 已推送，不可再改；v08 的升级保护只接受空收据表。
12. 规格 §4.1a 的“必须同时替换”（修订 2：旧 `(attempt_id,source)` 部分唯一索引、旧 ON CONFLICT 仲裁和旧 producer 保留到 T11/T16）按审查建议提交用户裁决；用户于 2026-11-13 指示按推荐方案处理，接纳推迟，并登记于主规格。
13. 插件 ABI 只有整份结果级的 finality，没有逐维度最终性。观察桥按“定价维度 + 完整分区”规则推导，是在不改 ABI 前提下能满足 §7.1 的最小实现。逐维度 final 事实需要 ABI 扩展，移交 T08/T09 的 SDK 演进，不在本单元修改 Go/WASM/SDK ABI。
14. 拒绝审计在重试时可能为同一拒绝组写多行（每次尝试一行，最多 10 次），仍然有界；不为去重另建状态。
15. ② 记录曾称 §7.4 算例经 ③ 的 `persist` 验证，实际只验证到私有暴露函数；⑥ 补齐 A、B、C 三例，按原文常数从签名收证一路验证到余额与 journal。C 例因 chat 准入要求输入上界，加入显式零费率 `input_tokens`（§7.3 允许），E 不变。fixture `bootstrap` 新增可选 `supplier` 参数，用于指定供应价格与上界。

### ⑥ 验证（新增与改动用例）

| 用例                                                            | 结果                                                                                                                                                                                                                                                                      |
| --------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `tests/integration/core-settlement-regressions.test.ts`（新增） | 11 通过：发现 1、2A、2B、3、6、7、8 各 1 条（共 7 条，修复前全部失败）；§7.4 A（1000/32/0→998/30/2→987/0/13）、B（50/40/0→20/20/30 冻结→-5/0/55→充值后 95 仍冻结）、C（500/100→500/180→480/160/20→450/100/50→420/0/80，selected 为重试 attempt、客户只扣一次）；38 位溢出 |
| `tests/integration/core-evidence-authentication.test.ts`        | 7 通过（新增 2）：ingester 先持身份 SHARE 时 instance 撤销等待其提交，之后的精确重投返回原 ACCEPTED、新事实 SECURITY_REVOKED 隔离（989/10/11）；两个 producer 同组，任一证书行忙则整组 55P03 回滚无收据（1000/60/0），重试后两条均 ACCEPTED（970/0/30）                   |
| `tests/integration/core-settlement-worker.test.ts`              | 2 通过（新增 1，修复前失败）                                                                                                                                                                                                                                              |
| `tests/integration/core-first-chain.test.ts`                    | 2 通过：JSON 与真实 SSE（分 97 字节切片喂入，上游收到 `stream:true` 与 `include_usage`），两路结算均为 985/0/15、扣 100                                                                                                                                                   |
| `tests/integration/core-execution-upgrade.test.ts`              | 10 通过（新增 2；两处“已应用版本数”由 6 改为 7，原因是追加了 v08）                                                                                                                                                                                                        |
| `packages/plugin-host/src/__test__/evidence.test.ts`            | 7 通过（新增 3，修复前失败）                                                                                                                                                                                                                                              |

共享测试辅助（`granted`、`event`、`supplier` 等）从 `core-settlement.test.ts` 移至公共的 `tests/support/core-settlement.ts`。

仍未覆盖：单批 128 组在 `statement_timeout=8000` 下的整体超时（NOWAIT 后不再因锁等待放大，未单独压测，留 T17）；非溢出的供应敞口异常（经公开入口不可达）。

⑥ 全量门禁：check 233 文件无类型错误；lint 0；fmt 337 文件；test:native 通过；test 291 通过 / 4083 断言 / 41 文件；build 成功；test:plugin-artifact 15/24、pending 0；test:artifact Gateway、catalog、identity 7、financial 9 全部通过；test:durability 通过（本机原生 PostgreSQL 17.11 / Redis 7.4.12，位于 `.local/toolchain`）。test:integration 第一次运行 498 通过 / 1 失败：`plugin-catalog-upgrade` 中重复迁移后的版本数仍断言为 6，追加 v08 后应为 7。修正后整套重跑：499 通过 / 0 失败 / 10754 断言 / 34 文件，原 13 项 native-durability 环境失败已随原生工具就位而通过。`test:backup` 未通过：开发库 `DATABASE_URL` 仍停在 2026111006，脚本第一步版本核对即失败，且脚本读取的是旧单账本表，需在 T12/T14 迁移，本单元未改动开发库。`test:container` 未运行。

## ⑦ 复核与修复

全新上下文复核者只读检查 `78aa247`：⑥ 的 11 行处置中 7 行确认修复、4 行部分修复，另有新发现。复核期间 OrbStack 停止，PG 不可用，因此部分结论来自代码追踪。实现者逐条对照代码与 git 记录核实如下。

| #        | 发现                                                                                                                                                                     | 处置                                                                                                                                                                                                                                              |
| -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| N1       | v07 先后以两种内容推送：`30218b0`（14:57，无认证列）、`3f2e1e0`（15:31，认证列并入 v07），`78aa247`（18:06 推送）又复原为前者并追加 v08                                  | 已推送版本一律不再修改，以最新推送 `78aa247` 为准：v07 = `30218b0` 内容，v08 = 认证列与⑥修复。按 `3f2e1e0` 迁移过的数据库会在校验和核对时拒绝启动，须重建；这类库只可能是该窗口内的本地测试/开发库，没有生产环境。⑦ 的 SQL 修复只追加 v2026111209 |
| N2       | ⑥ 观察桥要求 TTL 分区齐全才把 `cache_write_tokens` 记为 final。按聚合费率计价时（`validatePricing` 二选一），只报聚合值的 FINAL 响应永远不完整，按量客户永不收费         | 撤回分区条件：本次报告的 kernel 定价维度即为 final；未报告的 TTL 分区自然保持未知（§7.1“缺TTL分区仍未知”）。原用例断言的是错误行为，已替换为修复前失败的回归用例                                                                                  |
| N3       | 组内首份是“未认领 revision 的旧 CONFLICT”的 DUPLICATE 时，后一份同内容新事件被判为 REPLAY，找不到锚点，返回 P0503 BUSY，job 反复重试直至耗尽；结果还取决于 event_id 排序 | v09：组内重放只锚定会认领 revision 的首份（mode 为 NEW/REPLAY）；DUPLICATE、CONFLICT、QUARANTINED 首份不作锚，后一份按新事实经组内主张判定                                                                                                        |
| N4       | 首份仅因 event_id 被占用而判为 CONFLICT 时，后一份新事件继承争议，revision 是否被接受取决于排序                                                                          | 同上：只继承首份因数值主张产生的争议，event_id 冲突不传递                                                                                                                                                                                         |
| N5       | ⑥ 新增的 C+R 38 位检查不可能触发（`exact_billing_price` 已限定总额）；账户级余额加减越界仍会以未分类 22003 报错重试                                                      | v09 删除不可达的检查。账户级越界需单笔成本接近 10^38，只登记为已知限制，不另加分类                                                                                                                                                                |
| ⑥#9 补充 | 批次一直 ERROR 直到重试耗尽时，`onRejected` 不会触发，但审计行已写入                                                                                                     | 登记：此时 job 进入失败集，由现有失败告警覆盖                                                                                                                                                                                                     |

设计修订 11 更正为：本单元占用 v07–v09；判定依据是“推送后不改，以最新推送为准”，而不是“复原到最早推送”。

⑦ 回归证据：N2 新用例在已推送观察桥上失败，修复后通过；N3/N4 两条组内顺序用例在 v08 函数体上失败（前者正是 P0503），换成 v09 后通过；`core-settlement-regressions` 共 13 条全部通过。

⑦ 全量门禁：check 233 文件无类型错误；lint 0；fmt 337 文件；test:native 通过；test 291 通过 / 4086 断言 / 41 文件；test:integration 501 通过 / 0 失败 / 10754 断言 / 34 文件；build 成功；test:plugin-artifact 15/24；test:artifact 四项产物全部通过；test:durability 通过。test:backup 原因同 ⑥，未通过；test:container 未运行。
