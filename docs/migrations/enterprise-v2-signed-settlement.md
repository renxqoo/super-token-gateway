# Core 签名数字证据与双方结算实现单元

本记录对应 T04.b.2b、追加版本 `2026111207`，在本机 main 工作区按四个里程碑推进，未提交。设计依据为 [Core 准入、单次发送许可与双方资金事务](enterprise-v2-core-execution.md) §4.1a、§7、§8；本文只记录实现中对该设计的具体落点和修订，不是整个企业迁移的完成声明。

## 里程碑

| 里程碑 | 范围                                                                                                 | 状态   |
| ------ | ---------------------------------------------------------------------------------------------------- | ------ |
| ①      | v2026111207 schema：证据角色与 snapshot 谱系、`core_evidence_receipts`、供应成本证据外键、旧路径隔离 | 已实现 |
| ②      | 供应累计成本 C 与派生 R、供应平衡 journal，§7.4 人工算例 A/B/C/D                                     | 已实现 |
| ③      | `persist_core_evidence_groups_v1`：归一、争议、风险冻结、客户 selected 结算、事件交付及对外 DTO      | 已实现 |
| ④      | Core client 数字证据签名、producer 撤销屏障、worker 提交后 ACK                                       | 待执行 |

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
6. 每个非重复事件写一个 RAW；ACCEPTED 再写 CANONICAL（合并已 final 维度，旧 final 不被覆盖），争议写 DISPUTED 与 `usage_conflicts`、置 `attempts.metering_conflict`（既有 `register_wallet_dispute` 冻结客户钱包）；数值溢出写无成本 DISPUTED 与 `numeric_fault='COST_OVERFLOW'`；ESTIMATED 与 STALE（同 origin/source 已接受更高 revision）只留 RAW 与 receipt。CANONICAL 的 finality 仅在输入 FINAL 且供应成本 complete 时为 FINAL。
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

未覆盖：`OVERFLOW` 路径需要单价使成本越过 38 位 atoms，而当前 fixture 路由费率固定为 1/3，本里程碑未构造该场景。

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
