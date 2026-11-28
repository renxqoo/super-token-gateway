# Gateway 钱包、预扣与结算

Gateway 在进程内调用账本库进行技术钱包预扣、结算和退款。Console 通过私网管理 API 同步业务配置与发起资金操作，实际支付及原路现金退款由外部业务系统处理。当前数据流以 [ADR 0009](docs/decisions/0009-enterprise-gateway-worker-architecture.md) 和 [系统架构](architecture.md) 为准。授信规则见 [ADR 0005](docs/decisions/0005-credit-boundaries-and-capacity.md)。

## 请求链路

1. 验证 Key、计费账户、组织、模型与操作范围、来源 IP 及运行配额。Console 管理的账户复核 Gateway 数据库的组织和 Key 状态。
2. 按账户币种确定客户报价和预扣上界，分别计算上游成本并检查供应商资金。定价可使用已配置的版本化汇率。钱包资金保留原币种。
3. PostgreSQL 同事务登记幂等请求、预扣及首次供应商尝试。合资格现金、赠额和剩余信用不足时返回 402，上游尚未调用。
4. Gateway 转发模型内容并提取数字用量。正常 JSON 或 SSE 成功终态等待 `commitTerminal` 的 PostgreSQL 提交。账单、两侧分录和 Outbox 同事务写入，允许先转发可交付的 SSE 内容增量。
5. 客户只按最终选中的尝试结算。备选渠道更贵时可在切换事务中补足预扣，其他尝试的已知或未明供应商成本独立处理。非流式请求派发后若客户端提前断开，在 `UNARY_CLIENT_ABORT_GRACE_MS`（默认 600000 ms，设 0 关闭）及单次尝试超时约束内，可继续等待可信用量并结算。此路径不继续切换供应商。
6. `STREAM_ESTIMATED_SETTLEMENT` 启用时，正常结束但缺部分计量的对话流，或已有输出且无未决内容审核的断流，可以用已观测的文本估算缺失的输入或输出 token，并在两侧预扣上界内结算。已有数字用量保留，估算在访问日志与指标中标记。无输出的中断、上游显式流错误、未决审核中断或无法满足计价证据要求时保留 `UNKNOWN`。
7. `UNKNOWN` 保留预扣供 Worker 查询已配置的核账来源或处理人工收据。确认未执行可释放，可信最终用量可结算。可配置渠道级或账户级核账豁免策略，由 Worker 在等待期和逐笔、累计预算内释放客户冻结款，并留存审计。派发结果不明时，恢复依赖账本状态和供应商证据。

派发前明确失败可归还预扣。派发后只有得到足以结算、释放或豁免的证据，才推进相应资金状态。

## 金额、价格与资金来源

费率是 `{numerator,denominator}`，金额为 **10^-18 币种单位**的原子整数字符串，最大 NUMERIC(38,0)。全部维度用精确有理数汇总，仅在最终原子金额上向上取整。客户和供应商费率统一存于不可变 `price_versions`，通过 `side` 区分。路由独立引用客户价和供应商成本。已预扣的尝试记录对应版本，并可冻结生效价卡。

预扣上界由显式 ceilings、请求最大输出和收费依据决定。对纯文本 `chat`、`responses`、`messages` 和 `gemini` 对话请求（同协议与跨协议均适用，排除含多模态、服务端工具或服务端隐藏上下文的请求），网关可基于请求体 UTF-8 字节数与结构开销计算严格输入 token 上界 `estimatedInputTokens`，并在全部互斥输入计费维度（`input_tokens`、`cached_input_tokens`、`cache_write_tokens`、`cache_write_5m_tokens`、`cache_write_1h_tokens`）中按最高单价维度报价（超过对应 ceiling 时安全回退全上限），可用于收紧该路由的预扣上界。最大输出只读取入口协议的原生上界字段，多个、非正整数或非本协议字段返回 400；无对应计价维度的非默认 `service_tier` / `serviceTier` 返回 422/billing_ceiling；没有计量维度的服务端托管工具在准入前返回 422/unmetered_tool，避免上游按未计量维度另行收费。`PROVIDER_USAGE` 要求全部已定价维度的 FINAL 证据（流式估算启用时按上述明文估算边界兜底）；`INPUT_CHARACTERS` 用明确输入字符量并要求请求成功；`REQUEST_COMPLETION` 按成功请求收费。禁止把缺失维度当零。预扣上界不足造成超额时补扣可用资金；仍不足则完整登记债务；已有欠费与在途信用预扣合计超出授信时冻结新调用。现金入账可偿债，赠额不能偿还现金债务。可用余额扣减地板为0；欠费金额为实际客户应收中未被资金覆盖的部分，单独记录为 `debt_atoms`，默认授信为0时债务会冻结新调用并返回402；显式授信允许额度内继续使用。管理接口 `GET /internal/v1/wallets/:billingAccountId` 返回按币种区分的可用、预扣、已花费、欠费原子整数字符串和冻结原因，以及credit_limit_atoms、credit_reserved_atoms、available_credit_atoms、credit_excess_atoms、credit_policy_version。

字符费率按入口协议中的直接文本计算 Unicode 码点。Completions、图片和视频读取 `prompt`，Speech、Embeddings 和 Moderations 读取 `input`，对话协议读取消息、系统提示和指令的文本内容。文本数组按项相加。媒体载荷、工具元数据和仅引用的历史内容不计入字符量。无法确定字符量时，该候选费率不能用于预扣，其他可用候选仍可参与路由。

每个计费账户、每个币种有独立钱包。现金和赠额按相同条款聚合资金批次，赠额可设置模型/操作范围、到期时间和优先级。策略支持 `GIFT_FIRST`、`CASH_FIRST`、`EARLIEST_EXPIRY`；同类按 priority、到期、创建顺序消费。预扣、扣费、释放、退款均保存来源分配，退回原币种和原资金批次；过期赠额退入 expired，不能复活为可用余额。

外部充值、退款、调价和对账命令均有 external_ref 和载荷指纹；同一身份重复执行返回原结果，载荷变化返回 409。资金变动由账本实现按服务角色的数据库授权执行。租约结算先提交账单、分录和租约用量记录，余额归并可在租约关闭时完成。生产角色与授权见 [数据库说明](database.md#迁移与角色)。

## Usage 证据

| finality  | 含义                                           |
| --------- | ---------------------------------------------- |
| FINAL     | 已观察到可信最终用量；是否足以计价另看所需维度 |
| PARTIAL   | 部分或中间计量，不解释为最终计费量             |
| UNKNOWN   | 缺少可靠用量，不代表零                         |
| ESTIMATED | 仅有估算值                                     |

token、请求、图片、字符和毫秒以十进制整数字符串保存；明确的小数时长最多 18 位小数。OpenAI/DeepSeek 输入总量减缓存读取；Anthropic input_tokens 不含缓存读写；Gemini 输出合并 candidates 与 thoughts，输入合并 promptTokenCount 与 toolUsePromptTokenCount。reasoning 是输出的包含关系，不重复计价。

明确的毫秒时长可换算为秒。秒值可精确表示为合法整数毫秒时，同时记录毫秒维度。无法精确换算的值保留秒精度，毫秒费率等待可用证据。单一费率只能选择同类媒体的一种时长单位。正常结束的 SSE 即使未报告 token，仍有一次请求的最终证据，token 费率仍须等待所需维度。

媒体按响应实际用量、可确定的图片数量、输入字符或请求完成依据计价。缺少当前费率需要的数字用量时进入核账。对视频任务，创建时的结算和登记同事务提交，后续由公开查询按任务绑定的上游读取状态，符合配置的系统故障可触发退款。余额和用量投影通过业务事件更新。

## 结算恢复与 Outbox

普通租约结算及直接结算都通过 PostgreSQL 写入数字用量、客户账单、资金分配、平衡分录和 Outbox。提交结果不明时使用原请求 ID 查询或重放，避免产生第二张客户账单。Worker 清扫过期预扣和租约，跟进未明供应商成本、收据及欠款。

Outbox 至少一次投递。`POST /internal/v1/events/claim` 按已授权的消费者认领事件并返回 30 秒租约。消费者在自己的事务中按 `event_id` 去重，事务提交后通过 `POST /internal/v1/events/ack` 提交 `lease_token` 和 `through_seq`。Gateway 按消费方维护分片游标。配置了业务 Webhook 时，Worker 可用同一领批与确认机制推送事件。

账务和 Outbox 保存数字用量及必要标识。视频任务的查询结果单独保存于任务表，普通推理响应正文不用于账务恢复。赠额不抵偿现金债务，现金入账、退款或预扣释放后可推进欠款回收。

## 正常授信与扣除地板

`available_atoms` 和 `reserved_atoms` 只统计真实现金/赠额，最低为0；`credit_reserved_atoms` 是独立的信用承诺。`billing_holds.held_atoms` 仍为总预扣，其中 `credit_atoms` 是信用部分；资金来源分配合计为两者之差。正常准入在钱包锁内保证欠费加在途信用加新信用不超过授信，不用赠额偿还旧债。信用不产生资金批次或充值流水。

私网 `POST /internal/v1/wallets/:billingAccountId/credit-policy` 接受 `currency`、`expected_version`、`credit_limit_atoms` 和可选的 `policy`。版本和额度均为非负整数字符串，额度以原子为单位。新钱包的额度和版本均为 0。更新要求版本匹配，成功后版本加一，旧版本重试返回 HTTP 400。降低额度后，重新计算钱包冻结状态并保留已有欠费和在途信用。人工冻结使用 `POST /internal/v1/wallets/:billingAccountId/freeze`。信用不足返回 HTTP 402 和 `insufficient_funds`。

UNKNOWN保留信用承诺；明确释放或终态结算恰好释放一次，未被真实资金覆盖的实际费用记债务。授信约束正常承诺，异常实际应收可能突破额度，完整记录并冻结，不提供客户费用硬封顶或额外结算宽限。退款、收债、调账、人工对账和冻结统一复核债务加信用承诺；现金清偿或信用释放后重新计算可用额度。

核账可在客户请求已结束后处理未明供应商尝试。其他尝试的迟到成本不能替换已选中的客户结算。

## 精度与供应商差异计量

金额仍固定为 18 位小数的整数 atoms，计算与舍入契约见 [ADR 0007](docs/decisions/0007-money-and-model-compatibility.md)。不引入第三方十进制计算库，不以展示位数或支付渠道精度替换内部记账精度。对外支付、币种白名单与支付尾差由业务系统按渠道规则处理；已配置对应币种费率是 Gateway 的报价准入条件，不代表该币种已获支付渠道支持。

缓存写入可按 `cache_write_tokens` 合计计价，或分别使用 `cache_write_5m_tokens`、`cache_write_1h_tokens`，同一费率禁止两者混用以免重复收费。明细与合计是包含关系，总 token 统计只加合计一次。完整明细验证相加等于合计，未报告明细不补零；新合计没有明细时清除旧快照明细。已有费率保持历史行为，新建差异费率时同步配置对应 ceilings。新费率缺失所需 TTL 最终证据时保留预扣等待补证。

## 标准化商业定价与快照冻结

旧版平面费率卡保留原始配置和幂等标识。进入标准化引擎时，仅使用计费依据适用的费率、以及可识别且适用的上限。

标准化费率按生效阶梯核对计费用量。缺少独立思考、已授权托管工具或其他计费维度的证据时保留预扣供对账。有明确请求授权记录时，未授权且上游未报告用量的托管工具计为零。TTL 单独参与跳档时，账本需要对应 TTL 明细或能精确推导该值的缓存计数。

- **不可变基础费率卡扩展 (`price_versions.pricing_spec`)**：`POST /internal/v1/prices` 支持可选 `pricing_spec`，统一表达长上下文全量跳档阶梯（`cliff`）、边际累进分段（`progressive`）、媒体属性矩阵（`matrix`，如图片 `size × quality`、视频 `resolution × has_audio`）、独立思考输出单价（`reasoning_rate`）、最小起算量/计费步长量化器（`quantizers`）及单次最低/封顶收费（`min_charge_atoms` / `max_charge_atoms`）；未配置 `pricing_spec` 且无修饰器/汇率转换的路由保持零额外开销的既有租约快路径。
- **分层修饰器 (`pricing_modifiers`) 与不可变汇率版本 (`fx_rate_versions`)**：通过 `PUT /internal/v1/pricing-modifiers`、`GET /internal/v1/pricing-modifiers`、`POST /internal/v1/fx-rates`、`GET /internal/v1/fx-rates` 管理产品级修饰器（`PRODUCT`，如时区峰谷时段 `time_windows`、`service_tier`、`execution_mode`）与商业级修饰器（`COMMERCIAL`，如 `customer_group_id`、`billing_account_id`，支持 `MULTIPLY`、`BEST_FOR_CUSTOMER`、`EXCLUSIVE` 及 `EXACT_RATE_CARD`）以及版本化精确有理数汇率；每次配置变更递增 `config_epoch`，Gateway 节点在内存中零 I/O 编译为只读 `CompiledRatingPlan`。
- **预扣冻结与对账一致性 (`billing_holds.supplier_holds` / `billing_charges.pricing_snapshot`)**：使用标准化定价的请求在预扣瞬间冻结不可变 `PricingSnapshot`（含 `rated_at`、生效费率卡、命中修饰器、汇率版本和合并倍率），随 `supplier_holds` 持久化并在结算时写入 `billing_charges.pricing_snapshot` 与 `billing.settled` 事件；后续修饰器变更、峰谷跨时段或汇率切换不影响在途与 `UNKNOWN` 对账请求的结算金额。
- **零副作用试算 (`POST /internal/v1/pricing/simulate`)**：基于当前内存配置快照返回完整客户/供应商 `PricingSnapshot`、逐维度明细 `CostBreakdown`、预扣上界试算以及毛利与防倒挂评估结果。

实时推理使用 `SYNC` 定价上下文。`BATCH` 执行模式和 `region` 选择器目前只在试算请求中提供。
