# PostgreSQL 模型与事务

当前契约为 v2 schema2026111203，2026111202 基线保持冻结。`packages/storage/src/schema/` 是普通表、字段、索引、外键和视图的唯一 Drizzle 声明；`migrations/0000_enterprise.sql` 是其生成并审阅后的基线。`sql/functions.sql` 保留精确资金与证据原子函数，`sql/invariants.sql` 声明分区、延迟约束、触发器和 ACL，`sql/enterprise-invariants.sql` 声明新增领域约束。追加 `migrations/0001_upstream_funding.sql` 增加凭证内容摘要一致性、凭证身份/摘要关联和引用索引；最新普通 schema 对应 `meta/0001_snapshot.json`。上游公开充值/凭证能力与 Core 授权及外层提交边界见 [上游资金切片](docs/migrations/enterprise-v2-upstream-funding.md)。Drizzle 不支持的月分区布局在生成 artifact 中明确加入 `PARTITION BY`，不维护另一份完整表定义。

`migrations/manifest.json` 是唯一应用清单，每个版本按文件顺序、SHA-256 和事务应用，已应用的文件不可修改。`bun run db:generate` 只生成待审阅迁移，不执行 schema push；审阅新文件、必要 custom SQL 及保留行为后追加 manifest 版本。新迁移必须复验资金、证据、ACK 和开放 hold。`createRepositories(db)` 复用同一个 Bun.SQL 池，普通 CRUD 用 Drizzle；资金函数继续显式 SQL。所有 NUMERIC(38,0) 映射为字符串，序列及未设安全上限的计数映射 BigInt。

JSONB 列通过单一 native mapper 使用 `Bun.sql.json` 参数与 identity 读取，保留对象和 JSON-looking 字符串语义，不改变 SQL schema。`null` 为 SQL NULL，显式 `jsonNull` 为 JSON null；prepared where placeholder 必须通过列 encoder。金额与大整数在 JSON 中仍使用字符串。完整调用边界、默认值限制和生命周期说明见 [JSONB 映射](docs/migrations/enterprise-v2-jsonb.md)。

迁移命令为 `bun run db:migrate -- --database <确切新库名>`，目标名称必须匹配连接 URL。工具校验实际数据库身份，在 advisory lock 与事务下拒绝非空未知库、旧/错误版本和篡改 manifest；已知 v2 前缀可继续应用后续版本，重复当前版本无副作用，中断整笔回滚。运行进程只校验当前版本，不执行 DDL。旧开发库保持原样，schema06–2026111201 升级入口与专用夹具已经退役。

新基线为新建请求、初始尝试、预扣流水和结算记录生成按毫秒排序的 UUID，减少热表持续插入时分散的索引页写入。已持久化 UUID 继续按原值关联，证据投递继续按既有 ID 顺序认领。首次最终证据在同一条集合写入语句中共享解析结果，原事务仍同时提交数字证据、可投递事件和资金效果。worker 最多将 256 个证据并入一次 SQL 提交，队列作业的单项上限仍为 128，单个作业的事实一起进入同一事务。批内保留按钱包冲突退避，数字、资金来源和流水仍逐笔校验。

请求恢复只读取 `attempts_stale`；移除没有调用方的 `requests_stale`，让请求终态更新不再因该部分索引而强制重写其他索引。usage 的 `(attempt_id,source)` 唯一索引同时支持按 attempt 查询，移除重复的单列 `usage_attempt`。请求表的新页面按 70% 填充保留更新空间；索引变化不改变唯一性、外键、历史查询和恢复契约。

本轮性能优化保持资金与证据约束：钱包变更使用 `FOR NO KEY UPDATE`，仍串行化同钱包写入，同时允许不改变钱包的外键读取；资金日志批量写入直接复用新插入身份返回的时间，不重新连接整个身份表。容量结果以当前产物的开放到达压测及精确账务核对为准。

Batch 终态与证据在同事务结算，保留每个 Batch 的数字摘要；终态 Batch 如仍有开放预扣，继续轮询并等待逐行证据。新预扣的恢复检查从创建后 30 秒开始；正常结算、人工对账和债务唤醒仍按各自触发条件处理。

批量预扣和结算在事务内以 PostgreSQL 数组累积待写入行，落表时使用类型化行或一次性 JSON 记录集，避免逐项拼接不断增长的 JSONB 数组。预扣的逐项返回结果也先使用数组按输入位置累积，完成时一次构造相同的索引对象。每笔请求的局部计算仍在全部校验后才合入批状态，资金来源顺序、冲突证据和失败项隔离规则不变。

预扣在已取得的钱包锁保护下按批建立异常边界，正常批次不为每个请求单独建立写入子事务。凭证、范围、路由、报价、冻结、资金或信用不足、精度溢出与幂等冲突是预期拒绝，作为单项结果返回且不回滚同批其他项；组织或主体停用及 Key 角色由插入触发器复核，此类拒绝与非预期错误在同一外层事务内有界二分并复用相同算法，失败子批的请求、资金分配及日志一起回滚。成功项只在最外层事务提交后才可见，幂等仲裁和授权检查保持不变。无幂等键的新请求在完成各项校验后集合插入；带幂等键的请求在全部校验通过后逐笔通过唯一索引仲裁。每一行仍执行相同授权触发器，集合写入失败沿用整批回滚与有界二分。冻结原因统一由 `wallet_freeze_reason` 计算。

预扣批次以一次查询取得输入对应的凭证、项目、路由和费率快照，逐项核对作用域、到期时间和幂等意图。请求落表时仍执行授权触发器，避免在循环内重复执行配置查询。请求、预扣和尝试直接暂存为对应表的类型化行数组，避免在事务内为这些行重复编码和解析 JSON。

首次成功的同步文本请求可统一写入数字证据：参与集合写入的请求必须互不重复且为 execution/FINAL/SUCCEEDED、尝试与请求仍运行、无既有证据或异步资源，对应钱包已锁定；忙钱包单独返回可重试错误。仍使用相同精确计价函数、数字白名单、唯一约束和外键，先写全部事实再统一结算。其他输入或集合写入失败时在同一事务内回滚该尝试并交给原逐项摄入规则；重投、冲突、未知结果和异步任务的语义保持一致。内部集合写入函数不授权给 HTTP 或 worker 登录账号。

| 数据                               | 归属与不变量                                                                                   |
| ---------------------------------- | ---------------------------------------------------------------------------------------------- |
| projects / api_keys                | 技术项目与权限；Key 仅存 HMAC verifier 和显示前缀；即时撤销                                    |
| channels / models / routes         | 上游配置与技术能力；供应商密钥 AES-256-GCM，provider_id 作为 AAD                               |
| cost_versions                      | 费率快照不可更新/删除；变更路由只引用新版本                                                    |
| gateway_requests                   | 逻辑意图、指纹、操作、别名；project/key/operation/intent 唯一                                  |
| attempts                           | 物理调用序号、供应商、定价版本、状态；每请求 ordinal 唯一                                      |
| usage_events                       | 稳定查询视图；全局 id/seq/attempt-source 在 usage_event_index 唯一；数字 payload 按 UTC 月分区 |
| event_consumers / event_deliveries | 可回放、SKIP LOCKED 租约认领、fenced ack、至少一次投递                                         |
| resources                          | 虚拟 ID、原供应商 opaque ID、项目/模型归属、状态与轮询租约；没有正文或下载 URI                 |
| audit_events                       | 配置变更动作与哈希 ID，不保存配置值、密钥或请求内容                                            |

准入与用量摄入按排序取得可用钱包锁（SKIP LOCKED），只对已锁钱包加载余额；忙钱包不阻塞同批冷钱包。Store 只重试忙项，指数退避至50ms；HTTP 准入沿用请求取消/总期限，其他调用有2秒预算。用量超过本轮重试预算仍保留队列作业，不 ACK 丢失事实。

worker 通过唯一 `persist_usage_batch` 入口，在钱包先于请求/attempt 的锁顺序下去重、合并证据、创建消费者投递并调用 `settle_requests` 完成资金事务。HTTP runtime 无此入口的执行权限。配置审计由触发器和配置更改同事务提交。

相同模型、操作、币种和供应商集合的费率加载共享在途查询，每个请求再按自己的输入和输出上限计算预扣。费率变更提交后清理缓存和在途索引，较早的加载可完成已有调用，回写缓存前需核对其加载身份。失败加载从索引移除，后续调用可以重试。

同步成功、终态用量完整、且单笔预扣资金足够的常规账单可集合结算。钱包在同批内的所有请求都满足条件时，才按钱包和资金来源汇总余额变动，并仍逐笔写入扣款、分配及平衡流水。存在到期、多来源、债务、争议、人工处置、恢复租约或其他需要顺序计算的情况时，整钱包沿用通用结算流程。集合写入遇到金额或约束异常时原子回滚，交给通用流程隔离单笔错误。

单个 worker 进程同时运行最多四个证据入库事务，每次最多 256 个事实。BullMQ 并发用于收取及合并作业，数据库事务另行有界。

注册 consumer 使用独占 advisory lock，与 usage 写入的共享锁协调，防止历史回放和实时新增的缝隙。worker 的资源认领用 `SKIP LOCKED` 和短租约；完成只接受原 token。超时尝试只追加 UNKNOWN 恢复事件，不重新执行请求。

FINAL 证据不会被后到 PARTIAL/UNKNOWN 降级。选定证据由 attempts.evidence_id 明确引用，兼容补证只能补全一致数字。缺失计数、未知成本、未完成任务不能自动清理；数据留存必须先在业务侧确认消费和责任已结束。

资金表包括 wallets/wallet_lots、holds/allocations、charges、refunds/adjustments/resolutions 和不可变平衡 money_journal。不存在 usage 结算 outbox；外部 event_deliveries 只用于业务消费。

生产使用独立迁移 owner、受限 gateway_runtime 和 gateway_worker 登录账号，执行 `infra/runtime-role.sql` 授予最小权限。保持 synchronous_commit=on，监控 WAL、连接数、锁等待和磁盘空间；不通过关闭 fsync 或异步丢证据换取压测数字。

## 分区、归档与留存

全局身份和幂等键保留在 `usage_event_index` / `money_entries`；`usage_payloads` / `journal_payloads` 按 UTC 月分区。正反复合外键保证每个身份恰有对应时间的 payload，金额、用量、全局序号及账本引用的约束不变。视图与直接底表路径都受白名单/平衡/不可变校验保护。

初始化创建当月与未来三个月，DEFAULT 防止漏维护时业务中断。定期执行 `bun run db:archive -- --prepare YYYY-MM`；已进入 DEFAULT 的月份需要停止其他客户端后加 `--quiesced`，工具在同一事务内精确搬迁并恢复约束和不可变保护。

`bun run db:archive -- --month YYYY-MM --output /absolute/archive-directory` 对已过去月份导出有界 gzip NDJSON，写入 schema、MVCC snapshot、条数、压缩/原文 SHA-256 清单并自动复验。`--verify /absolute/archive-directory/manifest.json` 可独立复验；时间比较保留 PG 微秒精度。它是某一 MVCC 快照，晚到事实会进入后续快照，不能宣称自然月永久封闭。

指定 `--tablespace <已存在名称> --quiesced` 可把已验证月份的 payload 分区及全部索引移到冷存储。证据与账本在线查询仍然有效，归档工具不删除权威事实。全局元数据与金融记录仍会增长；按目标负载实测磁盘/WAL/备份预算，不能把分区说成数据库总量有界，也不能擅自给未结责任设置删除期限。

## 授信与新增资金状态

钱包保留非负 credit_limit_atoms、credit_reserved_atoms、credit_policy_version，预扣保留 credit_atoms；默认授信为 0。wallet_credit_policies 按钱包保存唯一 external_ref、指纹、策略版本和额度，不可修改或删除。信用承诺不写现金来源流水，欠费完整记录 receivable，纯现金快速结算要求 credit_atoms=0。运行角色没有直接修改信用字段或策略记录的权限。

新增 supplier_brands、upstream_accounts、upstream_account_balances、channels、upstream_funding_operations/holds/costs/cost_heads/journals/entries、balance_observations、reconciliations、upstream_audit_events、financial_vouchers、execution_commands/permits、plugin_packages/versions、channel_plugin_bindings 及技术管理员表。账户品牌不可变，渠道通过复合外键匹配账户与品牌；币种余额独立，不隐式换汇；cost_versions 保留独立 ceilings 对象（空对象表示未配置上游预算）；attempts/resources 预留 nullable binding_id 外键，供 T04 新执行强制固定真实版本。成本 revision 保留不可变事实，cost_heads 单独选择当前版本。

原 providers 表直接改为 channels，公开 provider_id 与 `/admin/providers` 保留。当前兼容公开配置入口创建品牌、空账户和渠道，绝不生成上游充值或 opening 记录。T04/T06 负责上游预付硬闸与真实资金事务，T08 负责插件发布宿主；这些能力不由空表或 seed 代表已完成。

资源只保存稳定 opaque ID、既有定义状态与严格的 Batch 数字摘要，拒绝 URL、query、正文和未知 metadata；music 没有伪造持久化资源。music_ms 为整数毫秒，与 audio_ms 独立。包最大 32 MiB、财务凭证最大 5 MiB；财务凭证的持久化权限与模型正文禁存规则分开。

当前旧进程角色权限仍按 `infra/runtime-role.sql` 实测；新敏感包、财务凭证与管理员 credential 表对这两个旧角色明确撤权。T04/T14 进一步拆 Core API/worker 最小权限及 Gateway 无 PG 权限。完整基线、测试迁移与权限责任见 [T03 基础层记录](docs/migrations/enterprise-v2-foundation.md)。
