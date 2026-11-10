# 数据库设计

全新数据模型，面向 PostgreSQL 17+。本文件定义建库时必须落地的字段、约束、事务与索引；不是已经执行的 DDL。金额一律为资产的整数 atoms，`NUMERIC(38,0)`，不继承任何其他项目字段。

## 1. 三类存储与分片

| 存储 | 数据 | 写入者 |
| --- | --- | --- |
| 配置/预算发行库 | 产品、版本价格、供应商/能力、全局 pool/grant、配置发布 | Control API、预算发行命令 |
| PG 资金组 | 付款聚合、身份消费许可、账户/预留/账本、订单、调用、证据、支付、任务 | Finance Service/Worker 的受控命令 |
| Redis/对象存储/分析库 | 软状态、加密原件、派生报表 | 各自适配器；不能绕过资金命令 |

一个 `billing_account` 下所有共享现金、权益、成员限额和订单在同一个资金组。`placements(virtual_partition, group_id, epoch)` 固定定位，新增物理组不改变已有聚合归属。跨组转账有独立业务流程，不能在应用里开两个事务假装原子。

所有实体使用服务端 UUID；客户/资金字段使用资产 ID。时间为 UTC timestamptz，金额通过字符串传输。分片定位、命令和供应商 allowance 绑定 epoch，旧目的地不能收同一 grant 再增加一次本金。

### 1.1 Bun.SQL 与精确编解码

首版 PG 适配器使用 Bun.SQL；具体 Bun 版本在实施前锁定。所有金融事务只在 `sql.begin(async tx => ...)` 提供的同一事务连接内执行，禁止混入全局池查询；回调必须等待其全部语句，事务完成后不能继续使用 tx。授权回执只有在外层事务 Promise 获得 COMMIT 确认后才可返回。

金额 numeric、序号 bigint 与超出安全整数范围的计量均使用十进制字符串/封装 bigint；numeric 出库按驱动字符串解析，入库绑定校验后的十进制字符串并显式转换为目标类型。SQL 参数绑定，不拼接客户输入；不配置自动转 number 的类型解析器。时间截止规则在 DB 中比较；Date 仅用于允许毫秒精度的展示，微秒级游标保留数据库精确表示。

连接断开/超时只说明客户端结果未知，不能据此认定 COMMIT 回滚。按 commandId 查询或重放幂等命令；`CREATED_PERMIT` 规则不因驱动重连而改变。独立连接池、DB statement/lock timeout、死锁重试边界与验收详见 [Bun 运行规范](bun-runtime.md)。

## 2. 配置与授权

| 表 | 核心字段 | 约束 |
| --- | --- | --- |
| assets | asset_id、code、atom_exponent=18、display_scale、status | code 唯一；发布后精度不修改 |
| products | product_id、public_model、capability_class、status | 对外模型身份不含内部供应商别名 |
| price_versions | price_id、product_id、customer_asset_id、version、metering_schema、rational_rates、rounding、policy、hash、published_at | `(product,asset,version)` 唯一；不可变 |
| provider_profiles | provider_id、provider_account_scope、credential_ref、endpoint_ref、billing_asset_id、status | 不存明文凭证；配额/账单域不按 URL 盲拆 |
| connector_versions | connector_id/version、provider、upstream_model、capabilities、bound_policy、cost_rates、recovery_capabilities、hash | 不可变；每次调用固定引用/快照 |
| routing_versions | route_id/version、product、allowed_candidates、priority/weights、region/policy、hash | 不可变；发布前验证价格/能力/上界完整 |
| billing_accounts | account_id、owner_type/owner_id、placement_epoch、admission_status、settlement_policy、policy_version | 付款与权限聚合；不逐请求累加总余额 |
| spending_credentials | credential_id、account_id、kind、secret_verifier、scopes、status、policy_version、expires_at | 属于一个付款聚合；Finance 最终校验 |
| account_memberships | account_id、principal_id、roles、status、version | 组织成员消费范围在本聚合权威确认 |

跨数据库没有伪造的 FK。资金组接收经过认证的不可变配置发布件，并按版本/摘要确认可用；缺配置或摘要不匹配就拒绝新授权，不能回落到随机价格。策略变更的金融生效点在资金组命令提交，控制面投影可稍晚。

## 3. 账户、来源与双分录

### 3.1 spending_sources

字段：source_id、billing_account_id、asset_id、source_type(CASH/PROMOTIONAL/SUBSCRIPTION_CREDIT)、restriction_policy、period_start/end、valid_until、status、version。

CASH 不自动过期；非现金来源限制可提现性、产品/用途和周期。已占用份额按原政策完成或归还，来源到期只处理没有被合法预留的部分。首发可以只启用 CASH，不允许对未实现来源返回假成功。

### 3.2 money_accounts

| 字段 | 类型/约束 | 用途 |
| --- | --- | --- |
| money_account_id | uuid PK | 一条实际余额 |
| billing_account_id / source_id | uuid FK，可按 kind 允许平台账户为空 | 客户资金归属 |
| asset_id | uuid NOT NULL | 禁止跨资产混账 |
| kind | ROOT/SPEND/PLATFORM_REVENUE/EXTERNAL_CLEARING/EXPENSE/PAYABLE 等 | 金融命令验证合法科目 |
| cell_no / allocation_version | integer/bigint | 稳定消费单元与收入条带路由 |
| posted_atoms | numeric(38,0) NOT NULL | 已过账余额 |
| reserved_atoms | numeric(38,0) NOT NULL DEFAULT 0 | 有效资金占用 |
| account_seq | bigint NOT NULL | 账户不可变变动链序号 |
| status | OPEN/NO_NEW_RESERVATIONS/LEGAL_HOLD/CLOSED | 独立于已提交外部义务 |

客户 ROOT/SPEND 强制 `posted>=reserved>=0`；平台内部科目根据类型允许带符号余额。`(source_id,kind,cell_no,allocation_version)` 唯一。客户账户 owner、source 与资产通过复合 FK/建户命令固定，不能用一个只检查 SUM=0 的 journal 从他人账户转钱。

新账户从 posted=reserved=0、seq=0 创建；正余额只能通过具有 journal/effect 的合法入账产生，不能在初始化或缓存重建时直接设置一份可花本金。

### 3.3 journal_headers / journal_lines

header：journal_id、command_id、asset_id、kind、business_ref、created_at；`(command_id,asset_id,journal_kind)` 唯一。

line：journal_id、line_no、money_account_id、asset_id、delta_atoms。主键 `(journal_id,line_no)`；FK 确保账户资产与 journal 一致；普通金融 journal 至少两条非零腿、同资产合计为零。原子费用为零时不生成零 journal。

所有已提交 header/line 不可 UPDATE/DELETE，也不允许向旧 journal 补腿。通过追加反向/差异 journal 纠错。预算 allowance 是风险额度，不混入客户现金 journal 当成银行资金。

### 3.4 account_effects：本次命令的余额证明

字段：effect_id、command_id、money_account_id、account_seq、posted_before/after、reserved_before/after、posted_delta、reserved_delta。

约束：`after=before+delta`；`UNIQUE(money_account_id,account_seq)`；`UNIQUE(command_id,money_account_id)`。它是每个账户一次命令的不可变变化证明，不替代 journal。

提交期校验设计：

1. 账户行加锁，插入 effect 时 before/seq 必须衔接当前账户缓存；更新缓存为 after。
2. effect.posted_delta 必须等于本命令该账户 journal lines 的合计。
3. effect.reserved_delta 必须等于本命令打开 slices 的金额减本命令关闭 slices 的原金额。
4. 最终缓存等于最后一个 effect 的 after，且客户账户仍满足 posted>=reserved。
5. slice 只能打开一次、关闭一次，capture/release 与关闭命令身份必须匹配。

这样按“本命令触及的少量行”校验，避免每次提交扫描整个历史账本/所有活跃 hold。全量 `reserved=SUM(active slices)` 仍是周期对账不变量。需要真正的约束触发器/受控写函数及真实 PG 验收；普通 CHECK 无法表达跨行 SUM。[D1]

## 4. inference_orders：稳定业务意图

| 字段 | 类型/约束 | 含义 |
| --- | --- | --- |
| order_id | uuid PK | 稳定业务请求 |
| billing_account_id / principal_id / credential_id | uuid | 所有权与认证来源 |
| endpoint / product_id | text/uuid | 客户实际购买的操作 |
| client_key_digest / canonical_version / payload_hash | text/integer/text | 业务幂等，不是 ingressId |
| price_id / price_snapshot / policy_snapshot | uuid/jsonb | 不可变客户报价、计量和未知处理政策 |
| customer_asset_id / max_customer_atoms | uuid/numeric >=0 | 原始授权上界 |
| execution_phase | ADMITTED/EXECUTING/UNKNOWN/TERMINAL | 与收费状态分开 |
| charge_state | RESERVED/READY/CAPTURED/RELEASED/NO_CHARGE/REVIEW | 用户资金决策 |
| selected_invocation_id / selected_evidence_id | uuid NULL | 唯一获选计费来源 |
| next_invocation_no / invocation_limit | integer | 整个请求的总物理发送预算 |
| resolution_deadline | timestamptz NULL | 未知客户处理期限，DB 时钟 |
| revision / created_at / updated_at | bigint/timestamptz | CAS、审计 |

幂等唯一键：`(billing_account_id, credential_id, endpoint, client_key_digest)`；有未结清/未恢复财务责任时不能删除该绑定。无客户键时创建独立服务端意图，不宣称断线重试必然安全。

READY/CAPTURED 必须有合法选中指针和最终计费证据；复合 FK 约束 evidence、invocation、order 归属一致。CAPTURED 与 NO_CHARGE/RELEASED 互斥，原始收费终态不可重开。迟到证据不因 order 终态而被丢弃。

## 5. fund_reservations / reservation_slices

reservation：reservation_id、order_id/payout_id、purpose(INFERENCE/PAYOUT)、billing_account_id、asset_id、total_atoms、state(OPEN/CAPTURED/RELEASED)、opened_command_id、closed_command_id、created_at、closed_at。

slice：slice_id、reservation_id、money_account_id、source_context、original_atoms、captured_atoms、returned_atoms、state、opened_command_id、closed_command_id。

规则：同一业务对象和 purpose 至多一个原始 reservation；`original>0`，终态满足 `captured+returned=original`；OPEN 时两者为零、closed_command 为空；只允许一次关闭。每个 slice 所属账户资产/付款主体与 reservation 一致。

reservation 的 total 等于全部 slices，按本命令做延迟校验；各 slice opening/closing 与 account_effects.reserved_delta 相符。零额订单没有正金额 reservation，但有订单及零额 capture 记录。

未发送可撤销、NO_CHARGE 可返还用户、未知供应商成本可保留，这些决策都不能通过删除 slice 实现。不存在自动 TTL 删除金融记录。

索引：`(reservation_id)`、`(money_account_id,state)`、`opened_command_id`、`closed_command_id`。打开/关闭命令索引用于有界提交校验，不逐笔全表 SUM。

## 6. provider_invocations：每次真实发送

| 字段 | 说明 |
| --- | --- |
| invocation_id、order_id、ordinal | 服务端 UUID、订单、物理调用序号；`UNIQUE(order,ordinal)` |
| provider_id、provider_account_scope、connector_version | 真实上游账户、模型、协议和恢复能力版本 |
| outbound_model、outbound_fingerprint、optional_encrypted_body_ref | 最终出站业务载荷，避免用客户端原 JSON 冒充真正发送内容 |
| owner_instance、owner_generation、owner_token | 一次发送接收者；不以常量字符串作为所有 owner |
| execution_state | MAY_HAVE_EXECUTED/NOT_EXECUTED/COMPLETED/UNKNOWN/REVIEW |
| permit_command_id、permitted_at、deadline_at | 耐久发送边界；不宣称这时供应商已接收 |
| provider_request_id / provider_task_id / provider_idempotency_key | 外部事实与真实端幂等身份，可为空但不能伪造 |
| cost_asset_id、cost_snapshot、max_provider_atoms | 本次进货资产与上界 |
| allowance_id、exposure_state、exposure_atoms | 所属预分配额度；HELD/ACCRUED/PROVISIONED/RELEASED |
| observed_cost_atoms、invoiced_cost_atoms、bound_excess_atoms | 计算应计与真实开票分开；单次超上界与 allowance 未融资差额不同 |
| response_started_at、finished_at、failure_class | 稀疏事实，不逐 token 更新 |
| next_recovery_at、recovery_owner/token/until、revision | 本地恢复工作认领，不授予重发权 |

一个 order 最多一个可能执行且未解决的 invocation。未知默认不透明并发重试；关闭的同一物理发送不会因为用户重试再次发放许可。外部执行 exactly-once 取决于供应商幂等，不由 partial UNIQUE 宣称。

费用释放/核销是一次性转移，按 invocation+操作种类唯一。用户 NO_CHARGE 后 invocation 可以继续 UNKNOWN/HELD；恢复扫描不能只查用户未终态订单。

索引：order/ordinal；`(provider_account_scope,provider_request_id)`；unknown due 部分索引；`(allowance_id,exposure_state)` 用于预算核对。外部 ID 唯一范围由适配器认证，不能假定各供应商全局唯一。

## 7. metering_evidence：不可变事实

字段：evidence_id、order_id、invocation_id、source_kind、source_key、finality、raw_usage、termination_reason、payload_hash、optional_raw_ref、verifier_version、observed_at、recorded_at。

`UNIQUE(invocation_id,source_kind,source_key)`；同身份同摘要幂等，不同摘要走冲突记录与复核。finality 包含 FINAL、CUMULATIVE_PARTIAL、ESTIMATED、INVOICE、NOT_EXECUTED_PROOF，不能将非空累计值默认升级为 FINAL。

最小金融用量事实及其完整摘要直接在 PG 耐久提交，不只存对象 URL。需要保存额外原件时先取得对象持久确认，再把内容哈希和引用写入 PG；对象可孤立清理，但不能让已成功封存的证据指向未上传对象。

不能原地修改 raw_usage；用户验收、价格计算、估算归属与发票差异由后续不可变 decision/capture/adjustment 记录表达。客户端不可自行提交供应商用量当可信证据。

## 8. capture_records / financial_adjustments

capture：order_id PK、selected_evidence_id、command_id UNIQUE、customer_asset_id、calculated_atoms、captured_atoms、uncollected_atoms、source_breakdown、revenue_accounts、journal_ids、captured_at。

`calculated=captured+uncollected`；captured 不超过原授权；零额 capture 允许没有 journal。source_breakdown 与已关闭 slices、原收入 accountId 对应，不在退款时重新按当前来源或条带选择。

adjustment：adjustment_id、original_order/capture、invocation/evidence、kind、asset/amount、reason、evidence_refs、requester、approver、state、command_id、journal_ids、applied_at。发起人与审批人分开；实际金融动作与 APPLIED 状态同事务。累计退款/追加授权有专门锁与业务上限，原 capture 不改写。

用户原始收费、未收标价、供应商成本差异、现金已支付金额属于不同字段和报表，不合并成一个含糊 loss。

## 9. 供应商预算 pool、grant 与 allowance

发行库：

- `provider_limit_pools`：pool_id、provider_account_scope、asset_id、period/epoch、funded_atoms、free_atoms、issued_atoms、status、revision；`funded=free+issued`。
- `allowance_grants`：grant_id、pool_id、destination_group/epoch、destination_allowance_id、amount、returned、command_id、delivery_state、proof_hash；grant/command 唯一，returned≤amount。
- `allowance_returns`：return_id、grant_id、amount、source_proof、applied_at；return_id 唯一。

资金组：

- `provider_allowances`：allowance_id、provider_account_scope、cost_asset_id、epoch/cell_no、granted/free/held/accrued/provisioned/overrun、revision。
- `received_grants`：grant_id UNIQUE、origin、destination_epoch、amount、proof、applied_at。
- `pending_allowance_returns`：return_id、grant_id、amount、deducted_at、confirmed_at。

本地恒等式：`granted+overrun=free+held+accrued+provisioned`，各余额非负。发行者 issued 包含尚未归还的已消费本金，不因下游产生费用就回到 free。

协议：

1. 发行事务扣 free、增 issued、写 grant 与 outbox。发送失败不退本金。
2. 目标校验认证来源、目的 group/epoch、金额与固定 grantId；同事务去重并增加 granted，新本金先冲未融资 overrun，剩余才增加 free。
3. 接收确认丢失时按 grantId 查询；绝不向其他目标再发同一笔“看似未收到”的本金。
4. 退还先在源组扣 available/free 和 granted，写 return 及 outbox；issued 总本金尚不增加可用。
5. 发行者按 returnId 去重核对证明后减 issued、增 free；确认丢失不再加一次。

跨库无 FK/共享本地事务。hash 只是内容指纹，不代替身份认证/授权。单库初期可用一事务划拨，未来跨组仍沿用这些身份与保守语义。

硬消费限额需要时用同类的 `quota_windows/quota_cells`：根额度预分配、请求只占本地份额、窗口/时区固定，跨日未知仍归原窗口；不能用 Redis 日统计替代财务硬限额。首发没有这项产品就不虚构已支持。

## 10. command_receipts / financial_work / outbox

### command_receipts

字段：command_id PK、billing_account_id、kind、business_key、fingerprint、stable_result、committed_at；`(kind,business_key)` 按聚合业务作用域唯一。

与金融变化同事务创建。相同键异参冲突；结果含稳定业务 ID 和金额，不保存可重发外部调用的 send capability。只有新建许可的特定执行者获得一次 `CREATED_PERMIT` 响应；金融命令重放返回 OBSERVE_ONLY，不能把首次可发送标志当普通稳定回执再次发给接管者。

### financial_work

字段：work_id、aggregate_id、kind(CAPTURE/RECOVER_COST/REFUND_QUERY/RECONCILE)、dedupe_key、state、available_at、owner/token/until、attempts、last_error、revision。

`(kind,dedupe_key)` 唯一；due 的 `(available_at,work_id)` 部分索引。收据与 CAPTURE 工作项同事务创建；最终收费与 DONE 同事务。任务状态负责调度，收费终态仍由订单/唯一 capture 决定。

SKIP LOCKED 领取有界批次，租约允许本地幂等工作重投，不允许外部推理重发。失败进 PARKED 并保留领域状态，不能因为任务重试耗尽删 hold。清理仅针对已关闭且超过保留期的调度元数据。[D2]

### integration_outbox / external_inbox

只用于确需跨系统派发的事件、支付命令/回调、grant/return、告警等。出箱与业务变化同事务；领取后在事务外网络发送；确认丢失可重复派发。入箱按外部来源/账户/业务身份去重，不只依赖随机 eventId。

不是每笔本地结算都需要再进入一个外部消息队列。未来加总线只替换派发/唤醒，不能把资金事实迁到易丢缓存。

## 11. 支付、异步任务与审计

| 表 | 字段/约束 |
| --- | --- |
| payment_orders | payment_id、account_id、merchant_scope、provider_order_id、原金额/资产、固定credit金额/资产与FX、state、credit_command_id；外部订单唯一 |
| payout_orders | payout_id、原payment_id、reservation_id、amount/asset、provider_idempotency_key、provider_refund_id、state、next_query_at；未知保持预留 |
| generation_jobs | job_id、order_id、invocation_id、provider_task_id、input/receipt模板引用、execution_state、artifact_ref、deadline；业务TTL不自动取消费用责任 |
| execution_tasks | 本地异步订单的领取/排队元数据、order_id、due、owner/token/until；领取不等于发送许可，许可后不自动重发 |
| audit_events | audit_id、actor/workload、action、resource、reason、evidence_hash、关联command、created_at；追加且独立导出 |
| reconciliation_cases | case_id、范围/水位、expected/observed资产金额、证据、处理状态、adjustment引用；不直接写余额 |

供应商账单导入还需全局 `provider_statements/statement_allocations`：以 provider_account_scope+statementId/行身份唯一，保存原币金额/摘要、目标资金组/调用分配与未匹配项；分配总额不得超过已核验账单，目标以 allocationId 幂等处理成本差额。找不到调用的费用进入平台未匹配差异，不能摊成虚构用户 usage；固定月费/税费与调用费用分开。这个低频登记属于配置/预算发行侧，实际金融调整仍在选定的权威资金组执行，不假设跨库事务。

API key、完整提示词和媒体不是普通金融日志。清理/备份和删除内容原文不能使法定/业务要求的最小财务证据失联，具体留存由适用政策确认。

## 12. 锁、隔离与权限

候选隔离为 READ COMMITTED + 显式锁/条件更新/唯一键。固定顺序：政策共享锁 → order/业务唯一记录 → 来源/预留 → money_accounts 按 ID 全序 → quota/allowance 按 ID 全序 → journal/effects。重平衡、退款与批量工作同样遵循全序，事务中不做远端调用。[D3]

冻结/撤销按同一政策名使用独占事务锁，新授权持共享锁；避免每请求改政策行。锁名称必须稳定，hash 碰撞最多增加串行化，不允许因碰撞错误识别账户权限。

资金写权限只给受控命令执行面；可使用经审查的 SECURITY DEFINER 写函数配合固定 search_path 与撤销直写，或等价严格封闭写角色方案，但必须实际落地验收。执行节点、管理查询和报表没有直接 UPDATE 钱包/INSERT 任意腿的权限。

所有跨行守恒、不可变性、effect/slice 关联与缓存链检查需要建库约束/触发器或唯一命令函数实现。不能只把本文件的公式当成数据库已经保证。表 owner、DDL/TRUNCATE 与超级用户另行分权。

## 13. 容量、分区与留存

10k/s 全天是 8.64 亿订单/日；invocation 不少于订单。必须分别测 order、effects、journal、evidence、task、索引和 WAL 体积，结合日均负载选资金组数与热保留期。

时间分区的全局唯一约束不能靠删除幂等来解决。可保留紧凑未分区的意图/命令/capture 注册表，将已关闭的大事实按水位归档；若改为时间分区须满足 PG 唯一键包含分区键的限制并维护原业务唯一性。[D4]

OPEN 预留、UNKNOWN 调用、未核对供应商责任与未完成退款不按创建时间直接清理。归档后的原始收费身份、来源/收入对手与证据仍可查；历史重放期限结束后的旧客户端键返回明确过期语义，不静默重新收费。

[D1]: https://www.postgresql.org/docs/17/ddl-constraints.html
[D2]: https://www.postgresql.org/docs/17/sql-select.html
[D3]: https://www.postgresql.org/docs/17/explicit-locking.html
[D4]: https://www.postgresql.org/docs/17/ddl-partitioning.html
