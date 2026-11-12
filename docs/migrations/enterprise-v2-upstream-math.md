# T04.b.math 上游累计成本与保守敞口

本单元落实已接纳事务设计第 7 节的单 attempt 纯规则。公开入口为 kernel 的 `calculateUpstreamCost`，沿用现有 `Pricing`、`Usage`、`Dimension` 和 `calculateCost` 的 BigInt 有理数计价。既有 `calculateCost` 与 `quoteReservation` 行为不变。

## 输入与调用责任

`pricing` 是本 attempt 捕获的不可变成本价；`estimated_quantities` 是产生原始 E 的捕获预估向量，不是后来重新读取的配置 ceiling。每个 priced 维度都必须有显式数量，包括零费率维度；空 rates 拒绝。数量、费率及币种继续遵守 kernel 现有精确值校验。新入口额外确认费率分子、分母确为字符串，拒绝越过 TypeScript 类型的 JS number，防止计价前发生整数舍入。

`head` 可省略，表示尚无已选成本 head；存在时提供 `usage` 与 `final_dimensions`。每个 final 维度必须是已知的 `Dimension`，且在该快照有显式合法数值。已知但不收费的维度不参与金额或 complete 判断。非 final 数值只作观察，即使存在于 `head.usage` 也不进入费用或自动扩大预留。

`trusted_partial_max` 可省略，表示没有额外可信 PARTIAL 风险观察。提供时必须是 Core 已核验、归一化并从仍需保留的历史可信 PARTIAL 事实按维度聚合的最大值；较新较小 PARTIAL 不能覆盖历史最大值。ESTIMATED 不能放入该字段。对当前尚未 final 的 priced 维度取 `max(e, trusted_partial_max)` 以扩大敞口；已 final 的维度始终取已选实际量。

`unresolved_disputes` 可省略，或提供尚未明确解决的争议快照数组，快照与 head 使用相同的数量和 final 维度形状。每条快照只贡献按自身 final 实际量和其他维度原始 e 计算的保守敞口；不选择它为 head，也不把它加进累计费用。较小争议不释放当前预留。

Core 必须在调用前完成来源可信性、完整定价归一化、累计 head 合并、revision 单调仲裁与争议识别，并保存原始事实及其关联。省略在新事实中的既有 final 维度必须在已选 head 中保留。普通推进不能减少已 post 成本；Core 必须检查新旧 C 的差额，矛盾 final 或成本下调走明确争议/审计裁决。该纯函数不比较来源、不验证版本、不推断 final，也不消解相互矛盾的快照。

## 金额与结果

对 head 的 final priced 集合 F，`K = sum(q[d] × rate[d], d ∈ F)`，未知部分 `U = sum(e[d] × rate[d], d ∉ F)`。`C = ceil(K)`，`R_base = ceil(K + U) - C`。所有维度精确相加后仅向上取整一次；不能使用 `E - C` 或逐维取整。

风险敞口把未知维度的 e 换成上述历史可信 PARTIAL 最大值与 e 的较大者；争议敞口对各未解决快照计算 `ceil(K + U)`。最终敞口是 head 基准敞口、风险敞口及所有争议敞口的最大值，`R = exposure - C`。

返回的 `estimated_atoms` 是原 E，`cost_atoms` 是当前 head 的累计 C，`remaining_reserved_atoms` 是最终 R，`exposure_atoms` 是 C + R。`complete` 只表示当前 head 的所有 priced 维度已明确 final；`has_unresolved_disputes` 独立表达未解决争议，即使 complete 为 true 也可能为 true。

`overrun_dimensions` 是当前 head 已 final 且数量超过 e 的 priced 维度；`overrun` 在该集合非空或 head 基准敞口超过 E 时为 true。`partial_risk_dimensions` 是当前未知且可信 PARTIAL 最大值超过 e 的 priced 维度，包括零费率维度。风险维度 final 后可从该集合退出并按实际值关闭其未知预留。结果不表示账户可恢复 ACTIVE；Core 已冻结状态保持 sticky，充值、低值 final 或 R 归零都不自动解冻。

任何估计、已确定费用或任一保守敞口超过 `NUMERIC(38,0)`，均抛出 `RangeError`；非法数量/费率、缺少 priced 预估、缺少 final 数值或非法 final 维度抛出 `TypeError`。不截断、不返回伪造零值。Core 捕获溢出后须按已接纳设计持久 numeric fault、保留旧 head/责任并冻结；本函数不写账户或流水。

## 独立人工验收

| 算例 | 输入及阶段                                                                        | E   | C   | R   |
| ---- | --------------------------------------------------------------------------------- | --- | --- | --- |
| A    | request 2、input/output 各 1/3，e 为 1/60/30；仅 request final，input 30 nonfinal | 32  | 2   | 30  |
| A    | request 1、input 31、output 2 全 final                                            | 32  | 13  | 0   |
| B    | input/output 单价 1，e 20/20；input 30 final、output 未知                         | 40  | 30  | 20  |
| B    | output 25 后全 final                                                              | 40  | 55  | 0   |
| C    | A 无 head、e 对应 100                                                             | 100 | 0   | 100 |
| C    | B request 20、output 单价 1/e60；仅 request final                                 | 80  | 20  | 60  |
| C    | B output 30 后全 final                                                            | 80  | 50  | 0   |
| D    | 单价 1、e 20/20；可信 input 25 PARTIAL、无 final                                  | 40  | 0   | 45  |
| D    | input 10/output 15 全 final，保留历史可信 input 25                                | 40  | 25  | 0   |

这些是单 attempt 规则。C 中 B 成功后 A 仍有 100 预留，账户聚合及客户 selected attempt 收费由 Core 负责；本函数不处理多 attempt 或资金余额。精度边界另外验证 1/3+1/3 合计 ceil 为 1、已知 ceil 抵扣联合敞口、125ms×8/1000 为 1、9007199254740993、18 位小数及明确溢出。

本交付的验证范围为纯规则及仓库静态/单测门禁。PG/Redis 资金事务、三服务、产物、耐久、HA 与真实供应商不属于本单元验收。
