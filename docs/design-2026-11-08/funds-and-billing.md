# 资金、预扣与结算

本文件定义新系统的金融规则，不依赖其他项目的钱包语义。默认是严格预付费；后付费/授信属于另行批准的产品，不隐含在余额算法中。

## 1. Money 与计价

### 1.1 统一表示

`Money = (asset_id, atoms)`，每种结算币种的内部单位为 `10^-18` 币种单位。1 USD 对应 10^18 atoms；UI 的分/小数显示不改变账本。

atoms 在数据库用 `NUMERIC(38,0)`，在 TypeScript 中用封装的 `bigint`，JSON/RPC 使用十进制字符串。Money 必须同时绑定 asset，构造时验证整数格式与 38 位范围；禁止经过 `number`、`parseFloat`、`parseInt` 或隐式数值强转。`bigint` 本身不可变，Money 对象不暴露可变字段。`JSON.stringify` 不能直接序列化 bigint：在 DTO 编码器显式调用 `toString()`，不修改 `BigInt.prototype` 或使用全局隐式转换。PG 的 numeric 读作字符串，按 Money 规则再解析；驱动边界见 [Bun 运行规范](bun-runtime.md)。[M1]

有理数以 bigint 分子/正分母表示，约分和求和全过程保持整数运算。非负费用最终取整采用 `(numerator + denominator - 1n) / denominator`，负向调整另用明确的有符号规则。输入 token/单位数若来自 JSON number，必须先证明为安全整数；大整数或小数费率直接使用保留精度的字面值/字符串解析，不能等 JSON.parse 丢精度后再转 bigint。

价格为有界有理数：`price_numerator_atoms / price_denominator`，计量数量使用整数基本单位，如 token、毫秒、字符、张。每百万 token 的分母为 10^6；按秒收费可转为每毫秒费率。配置发布限制维度数、分子/分母大小与计算复杂度，拒绝无穷、负值和超范围。

```text
raw_total = SUM(quantity[d] × rational_price[d])
charge_atoms = ceil(raw_total)   // 全请求合计后只取整一次到 atoms
```

取整规则固定在价格版本，按请求执行，不能每个 chunk、来源或维度重复取整。退款返回实际已扣 atoms；供应商发票按其原币种/最小结算规则保留并单独核对。

### 1.2 固定的产品报价

用户购买逻辑产品，价格版本固定客户资产、互斥计费维度、所有费率、最低/固定费用、计量版本、舍入和未知处置政策。供应商替换只影响平台进货价；跨产品/模型替换须经用户显式授权。

input、cached input、cache write、output、reasoning、audio 等必须声明包含关系。不能同时把 reasoning 计入 output 又收一遍；也不能因为隐藏 token 不出现在 SSE 文本里就当其不存在。

授权 R 是所有允许候选在最终出站体上的可验证最大用户费用。输出上限、n、工具/媒体/单位费用要实际传给并被供应商执行；不能依赖平均输出或字符数/4。价格和 tokenizer 未验证的维度不能获得严格上界认证。

一次 fallback 超出原 R、能力或产品范围时，默认停止；如未来允许增额，须在新增成本产生前完成独立的客户同意与原子追加预留。首版不依赖调用中途自动增额。

## 2. 资金账户与消费单元

每个付款账户/资产可以包含 CASH、PROMOTIONAL、SUBSCRIPTION_CREDIT 等资金来源。首发必须先完成 CASH；其余来源是有边界的可选产品，不能冒充可提现现金。

每个来源有 root 和多个 spend cell。`posted` 是该账户累计入账余额；`reserved` 是尚未解决的资金预留。客户可用为 `posted - reserved`，二者均非负，且 `posted >= reserved`。

```text
root.posted + SUM(spend cells.posted) = 该来源当前账内余额
当前账内余额 = 累计净入账 - 已收费用 - 已向外退还净额
每个账户 posted = SUM(其不可变 journal lines.delta)
每个账户 reserved = SUM(其有效 reservation slices.remaining)
```

root→cell 是同币种内部转账：root -X、cell +X，两条 journal 同事务。父余额减掉后才能在子 cell 使用；不能从 root 快照重复创建多个 cell 本金。归还只处理可用未占用部分。

补给由单个付款账户内的有界命令完成，按低水位批量，不逐请求锁 root。每请求优先使用一个 cell，必要时按固定顺序锁少量 cell；无法按原子上限分配时返回“资金待分配”，触发有界重平衡。真实总余额不足与资金碎片化是不同错误。

账户展示汇总所有来源，但现金可退、限制用途、来源到期分别展示。CASH 不因套餐周期变化过期。非现金来源的已预留消费可按原周期完成；释放的剩余额度若已到期，按固化政策失效，不能转为用户现金。

## 3. 预扣不等于收费

`fund_reservations` 保存一个订单的总预留，`reservation_slices` 保存具体来源/cell 分配。首次授权不写用户消费 journal，只增加 reserved 与明细；因此不会同时减少 posted 又重复增加 reserved。

授权事务：

1. 验证当前资金政策、凭证、成员归属、资产与不可变报价。
2. 业务意图唯一绑定；同键异参冲突，同键同参读取旧决策。
3. 按价格/来源政策计算 R，按已批准优先级分配 slices。
4. 原子条件更新各 cell，保证 `posted - reserved >= 本片预留`。
5. 同事务预留可选硬消费限额和本次供应商 allowance，创建 invocation 和一次发送 owner。
6. 提交订单、预留、调用许可和命令回执；只有明确的新许可可以发送。

任一步失败全部回滚，不留下孤立的用户预扣或供应商额度。运行软许可在失败后归还；金融预留不由 Redis TTL 管理。

客户授权冻结 R 不代表供应商 Kmax 已经支付。它们资产可以不同，在同一个本地事务里各自维护独立资产约束，绝不能把不同币种金额直接求和。

## 4. 用户收费状态

状态为 RESERVED → READY → CAPTURED，或者 RESERVED → RELEASED/NO_CHARGE，必要时进入 REVIEW。与订单执行阶段、供应商成本状态分别保存。

| 状态/动作 | 资金含义 |
| --- | --- |
| RESERVED | 用户已预留，尚未收费 |
| READY | 可计费的最终证据已耐久封存，等待幂等实扣 |
| CAPTURED | 唯一原始收费已完成 |
| RELEASED | 有证明的未执行/撤销，用户预留安全释放 |
| NO_CHARGE | 平台永久免收用户，可能仍存在供应商成本 |
| REVIEW | 冲突或金额异常待复核，预留不删除 |

`capture_records.order_id` 唯一。CAPTURED、RELEASED、NO_CHARGE 是原始收费终态；晚到事实不复活收费。退款/追加已获同意的费用是新业务动作，另有操作 ID、账本和审批。

## 5. 最终实扣与退差额

Worker 锁订单、认领版本和预留，读取封存的最终 evidence 与价格版本，重算 C。正常要求 0≤C≤R，按固化的来源顺序分配实际消费，不从当前账户/套餐重新规划。

对每个 slice：posted 减 actual_slice，reserved 减完整原预留；未使用差额自然回到该 cell 的可用。journal：客户资金账户 -C，平台收入账户 +C；多来源是多条客户腿，所有同资产分录合计为零。

例如 posted=100.00、reserved=30.00，最终收费 20.00：收费后 posted=80.00、reserved=0，可用 80.00。不是再向 posted 加 10.00。此处数值为币种显示单位，存储仍为 atoms。

在同事务内完成：

- 实际资金减少与 reserved 归还；
- 所有来源明细关闭；
- journal/header/lines；
- 唯一 capture record 与用户账单投影；
- order.charge_state=CAPTURED；
- 本地工作项 DONE 和需要对外派发的 outbox。

事务后崩溃、任务确认丢失或工作重投只读原回执。零元订单不造非零 journal，仍保存 capture/证据与供应商成本。

出现 C>R：默认最多捕获 R，完整保存原始用量与理论费用，多出部分记 `uncollected_customer_amount`，冻结相关价格/能力配置，进入审计。不能透支用户，也不能为了压回 R 改写供应商 usage。严格上界失效是待修复异常，不能作为常规收费手段。

## 6. 证据、截止时间与免收竞争

原始 evidence 不可变，明确 final/cumulative_partial/estimated/invoice/not_executed_proof。没有最终证据时不能把“最近累计 usage”标成精确完成。

默认 FINAL_ONLY。允许估算收费时必须采用独立的已获客户同意的产品政策，规定适用原因/上限并在账单标明 estimated；不能声称估算就是精确计量。

执行未知时在 PG 设 `resolution_deadline`，它属于当次故障处理，不是任意流持续时长。新证据与免收工作项锁同一订单：

- 截止前封存可计费最终证据：进入 READY，即使 Worker 稍晚实扣，也不因后台积压被免收。
- 到期仍无合格最终证据：按固化政策转 NO_CHARGE，释放全部客户预留。
- 截止后才到最终证据：只补外部费用/审计，不能抢先 Worker 复活原用户收费。

证据接收命令和超时命令都用 DB 时钟及同一判定规则；不是哪个异步回调先执行就收费哪个。存在事实冲突进入 REVIEW，不伪造空收据推进正常收费。

NO_CHARGE 与延迟 Capture 并发时，订单锁和状态 CAS 保证只有一个终态。NO_CHARGE 后任何 Capture 必须返回稳定的免收回执，无资金副作用。

## 7. 供应商费用单独管理

每个 invocation 预留其 Kmax，来源是本地独占的 provider allowance。状态为 HELD、ACCRUED、PROVISIONED 或 RELEASED；实际开票另记核对事实。

- 明确未执行：释放该次 Kmax。
- 有可信最终报告与价格：核销 K，释放 Kmax-K，记录应计成本。
- 执行未知：继续 HELD。用户 NO_CHARGE 不影响该预留。
- 无法长期查询、按政策提准备：可以将最坏 Kmax 转为明确标记的 PROVISIONED，不返还可用预算，不宣称真实已开票。
- 发票到达：按请求或已验证批次关联应计/准备金，追加差异分录，拒绝重复入账。无法逐请求分摊时保留批次级事实，用户账单不跟着任意分摊改变。

供应商实际成本高于 Kmax，完整记 `bound_excess` 并阻断相关新调用。核销先使用该调用原 HELD/PROVISIONED 及本 allowance 尚未占用的 free，不挪其他调用的 HELD；仍不足的部分记预算 `overrun`。bound_excess 是单次合同上界被突破，overrun 是没有本金覆盖的预算差额，两者不是同一个数。

晚到发票修正应计成本时按差额追加：增加成本消耗未占用 free、不足记 overrun；减少成本先冲减未融资 overrun，余量才返 free。新的 grant 也先覆盖 overrun，剩余才成为新 free。所有修正按外部账单项/调整操作幂等，不能把同一发票重放当成新费用。

操作预算守恒：`granted + overrun = free + held + accrued + provisioned`。本金重新可用仅来自实际未使用退还或有证据的差额冲回，不来自 TTL。

ResolveProviderCost 同事务保存唯一成本操作、预算变化和平台应计/准备 journal，不改用户余额。准备转实际要先冲回原准备再记录实际成本，不能把两者都当新费用。应计/准备的对手是平台费用/负债科目；供应商实际付款另有外部支付事实和对账，不能把 ACCRUED 标成现金已付。

成本预算是运营风险边界，不等于银行账户实际资金。应计、准备、已开票、已支付分别对账；财务总账映射由财务政策确定，本系统资金子账不冒充完整法定总账。

预算周期换代不能自动复制旧本金或清空 UNKNOWN；批准新成本预算是显式发行命令。长期未知风险按所有未关闭 epoch 汇总并限制，新日限额不是释放旧风险的理由。

## 8. 充值与退款

### 8.1 充值

先保存 payment order，固定原支付金额/资产、目标账户、入账金额和必要汇率。回调签名只证明来源，还需核对商户账户、订单、金额、币种与成功状态。

同一 `merchant_scope + provider_order_id` 入账最多一次；不同成功事件也不能重复 credit。支付 inbox 去重、订单状态 CAS、topup command receipt、root 增额与 journal 同事务。COMMIT 结果未知按旧订单查询，不重新充值。

外币支付不在回调按最新汇率换算。若需要兑换，一个业务操作关联两份各自资产平衡的 journal 与固定 FX quote；不能在一笔 journal 中直接把 USD 和 CNY 合计为零。

### 8.2 服务退款

“退服务费到用户”与“从用户余额退回银行卡”是不同业务。

服务退款锁原 capture/退款聚合，校验累计已退≤原实收，按原来源与原收入对手账户反向 journal。回到现金还是非现金权益由原资金来源与固化政策决定，不把过期套餐直接换现金。refund operation 与退款记录同事务幂等。

内部收入可以分条带，但原 capture 保存实际收入 accountId；退款不能用今天的分片数重新 hash 对手账户。

### 8.3 外部退款/提现

首版支持原支付路径退还，任意第三方提现是另行授权的产品。创建独立 payout order，从客户可用现金预留 X；INFERENCE 与 PAYOUT 预留都计入该 money account.reserved。

退款报价固定客户 atoms、支付侧原币金额/原支付可退额与汇率处理。跨币种/支付最小单位按累计金额计算舍入和尾差，禁止通过拆分部分退款多退；外部支付金额为零或不可精确表达时不得静默扣走客户现金。还要检查商户真实可付流动性/到账状态，不凭钱包可用就假定银行卡退款已经可执行。

先冻结后调用支付供应商；成功时客户 posted 与 reserved 同减 X，并记外部结算 journal；明确未执行失败只退 reserved；支付未知保持冻结，按同 payoutId 查询。重试需要供应商端幂等，不因网络超时再次支付。预留中的推理资金不可用于退款。

## 9. 冻结、授信与权限

账户准入状态与法律/风控处理分开。`NO_NEW_RESERVATIONS` 阻止新调用和新提现，但允许既有合法义务结算/退款；`LEGAL_HOLD` 的冻结资金只能经审批命令处理，不由普通 Worker 绕过。生效点是权威政策事务提交；此前已提交的外部许可不能承诺被立即撤回。

授权命令持有本账户政策的共享事务锁，冻结/撤销命令持有同名独占锁；读政策后才预留，避免每请求更新账户总行。锁顺序和粒度需在真实 PG 验证，所有写政策入口遵守同协议。

默认无客户授信或负余额。未来后付费需独立信用额度本金分配、应收/回款和风险审批，不能只将 posted>=reserved 的约束删掉。

人工资金调整要求原因、证据、金额/资产、原操作、发起人、不同审批人和操作 ID；授权不足拒绝，审批与执行状态同事务提交。管理员不能直接修改 posted 或伪造 journal line。

## 10. 正确性检查

每笔 journal 同资产且合计为零；客户 posted 与 reserved 非负；reserved=有效 slices；来源总额=订单 R；capture+释放=原预留；原始 capture 每订单一次；供应商 allowance 本金守恒；grant/return 不重复；用户 NO_CHARGE 后不再 capture。

单元测试用整数/有理数规则，真实 PG 验证唯一约束、延迟约束、锁和提交未知；外部用量与支付契约单独联调。模型推演和结构检查不能替代完整实现验收。

[M1]: https://bun.sh/docs/runtime/sql
