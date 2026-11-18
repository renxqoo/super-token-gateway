# 10 计价与资金主链重构

2026-11-18。状态：1804资金单元、1805供应结算集合写、1806客户权威及BEGIN、1807连续准入和1808普通新证据集合片已接入并定向验证；集中门禁及300RPS失败诊断见§15。晚取预算锁造成回滚写放大已确定性复现，正在修复，无新的稳定RPS结论。未部署。用户明确要求先解决最大瓶颈，授权直接重构计价/架构，同时必须保证资金准确。本文细化06的完整资金单元，不重新开启外围微优化或逐小项容量验收。

## 1. 实测依据与范围

上一已验证构建S5在60目标/61到达连续两轮通过，服务CPU加权25.051ms/请求，其中PG16.617ms，约66.3%。PG顶层execution batch、evidence persist及配额操作仍占大头。new-api真正正额预扣对照PG约0.828ms，不能把当前差距解释成几次网络签名，也不能用去掉helper或CLOSE推算3000RPS。

当前资金串行点跨越外层和叶子：1705的core_execution_item_v2在admit/next和BEGIN均锁root；1801的core_evidence_group_v1在处理事实前已预锁全部account/root；1208的core_supplier_settle_v1再锁account/root。只改叶子、只增加批接口或只编译价格，都不能解除这些锁。批接口当前仍逐项调用item/group，须替换实际读写。

本次完整单元同时覆盖计价编译、供应预算分区、准入/BEGIN/证据结算、风险慢路、控制面、汇总消费者及历史迁移。既有S6配额变更只做必要收尾，不另开容量轮次。资金主链完成后再测完整链路；PG成本即使下降，Gateway/Core等其余约8.4ms仍须继续优化，不能提前宣称CPU≤2ms或稳定超过3000。

## 2. 不可变价格程序

原cost_versions、billing_prices及其版本ID、rates、历史quote/hold/证据摘要保持为事实。新增core_funding私有派生表，分别以真实供应/客户价格版本FK保存compiler_version、source_digest、维度、精确整数权重及公共分母；不接受请求提供的权重。新价格插入时同事务从原rates编译，历史版本先验证再回填。

对每个rate p_i/q_i先约分，零费率保留维度且分母归一为1；D为约分后各分母最小公倍数，W_i=p_i×(D/q_i)。quantity精确补零到18位，得到整数Q_i，令T=Σ(Q_i×W_i)。金额为div(T,10^18×D)+[mod(T,10^18×D)>0]。不得把rate近似到18位小数，也不得用numeric除法后ceil代替整数商余数。每个请求独立舍入，不能跨请求/attempt合并后再舍入。

合法当前域最多14个可计价维度，合法中间量可到约590位，使用无typmod numeric，不能使用BIGINT或NUMERIC(38,0)存中间量。最终atoms仍限10^38−1，超限沿原overflow/UNKNOWN责任处置。

历史PG价格输入域比TypeScript宽：cost_versions只有object约束，直接SQL价格函数允许任意维度名及整数维度的小数。迁移不得根据TS类型静默删键或截断；按实际全部key精确编译并核查旧数据。无法精确编译则全事务拒绝迁移并报告具体版本，不替换旧价格、不部分发布。零rate仍保留维度存在性及finality/risk判定，不能删掉零权重维度。

批内按价格版本复用程序，并融合cost/exposure/base/ceiling的向量求值；仍分别整体舍入各快照。partial/final按Δcost/Δreserve推进；多个dispute取完整快照exposure的最大值，不逐维度拼出不存在的更大用量。

## 3. root与16个预算单元

数学及精度依据沿06：B=root.B+Σcell.B，R=Σcell.R，S=root.S+Σcell.S，公共revision=root.rev+Σcell.rev。root只保存未分配预算及未映射历史支出；普通写只改hold所属cell。每个hold固定不可变cell指针，并保存当前净reserved_atoms，删除每次结算重扫全部旧journal求reserve的热路径。

每个ACTIVE单元必须B≥R≥0。预扣先确保本单元free≥estimated，再原子增加R和hold；账本与客户预扣同事务。普通结算只写该cell的成本及预留差额。UNKNOWN的R不得回收，内部root↔cell划拨不增加经济expense或公共revision，另记不可变划拨事实。

按真实attempt UUID选首选cell；局部不足不生成402，退出普通事务，在独占慢路选择其他足额cell或回收未reserved预算。只有独占核完真实汇总仍不足才持久拒绝。16个单元不是16份可超用余额，每份预算都有root来源。

spent/revision使用06的本地headroom上界，root加所有cap不超过MAX。本地cap不足而全局有空间时独占重分配；真实全局溢出未ACK并告警，不伪装成已结算。非ACTIVE、超预估或争议路径锁齐后检查完整聚合与每个局部范围。

## 4. 事务和批处理

账户fund advisory由PG实际account UUID派生。普通资金事务shared TRY；控制和风险在全新外层事务入口排序取得fund EX，需变更目录语义时随后catalog EX。敏感DML guard核实际预持EX，不能用GUC或TRY升级自证。保留全部客户、身份、历史证据及当前吊销权威行锁。

新准入批先验证不可变价格/manifest及当前授权，再按账户/cell归并预扣；批量插入request、attempt、permit、hold、租约与经济账本。BEGIN仍核客户冻结/授信、request关闭、permit owner/构建/一次发送、租约proof和估算绑定，但在fund shared不变量下不再写锁root。

单一PG事务最多持有128个账户fund fence。执行批原上限128项可直接满足；证据宿主按真实attempt集合分片，每片至多128个attempt且不拆开一个原子group（单group原上限64事件）。分片只改变技术批边界，逐group原子性、事实ID及重投幂等保持；后续片结果未知时不能ACK原job，之前已提交片重投为duplicate。

证据批保留组内冲突判定、全部签名RAW/receipt和组隔离；归并cell delta后批量写余额与账本。不能跨请求舍入或把部分失败当整批成功。每个请求/attempt仍有独立可追踪的金额及幂等锚点。

普通组发现风险、非ACTIVE、碎片或headroom不足，返回明确内部慢路结果并回滚该组。宿主必须等当前外层事务提交/回滚后，再为这些原事实开启新外层EX事务；同批早先成功组持有shared时绝不原地升级。未知提交不重试生成新事实、不ACK。慢路和正常入口共享原绝对期限，池等待、锁等待及SQL不分别重置预算。

## 5. 一次完整迁移面

| 当前入口                                         | 改造要求                                                                                             |
| ------------------------------------------------ | ---------------------------------------------------------------------------------------------------- |
| core_execution_item_v2（1705）                   | admit/next选定cell；BEGIN去root热锁；abort只释放当前hold的R，保留未授予限制及零额revision语义        |
| core_evidence_group_v1（1801）                   | 同时替换外层account/root预锁，保留S6关闭、客户锁、事实及账务原子性                                   |
| core_supplier_settle_v1（1208）                  | 当前R来自hold，普通写cell；保留拒绝负cost_delta和RELEASED后新结算的规则                              |
| core_abort_recovery_v1                           | 只处理未授出的RESERVED，UNKNOWN不按时间退款                                                          |
| recharge/adjustment/reconciliation/account state | 从HTTP及standalone外层runner先取fund栅栏；不用savepoint排队升级；回执和expected revision读取受锁汇总 |
| catalog与runtime审批                             | fund→catalog固定顺序，反向源行锁NOWAIT；普通余额变化不增加epoch                                      |
| API、bench、artifact、durability、backup         | B/R/S/available/revision读汇总；额外核cell归属、内部划拨与经济账本守恒                               |

控制面授权、栅栏、写入、回执与最终授权重核仍同事务。历史已保存充值/调账receipt原样返回，不用当前余额重算旧事实。管理角色的root直接DML权限随受控入口一起收紧，不给runtime开放cell写口。

只在已准入停止并排空的升级边界一次事务回填。按真实ledger净额给全部历史hold分配当前R和cell，包括PARTIAL/SETTLED/RELEASED；保留未映射历史成本在root，不能按当前route重新定价。旧guard、FK、ACL、Drizzle和迁移快照同步更新。未通过守恒核对整事务回滚，不留新旧两套余额可同时写。

## 6. 验证与出口

最小独立金额算例覆盖：同请求两个1/3只收1、两个请求各1/3合计2、已知1/3与未知1/3的cost=1/total=1/reserve=0、零rate维度、18位小数及最终38位溢出。不能只与旧实现差分而省掉独立期望。

复用原资金与故障回归，优先证明不同cell并行、不超支、碎片不假402、UNKNOWN不回收、风险跨外层事务、相同事实重放无重复收费、历史部分计量后只记差额及失败无部分写。完整检查集中在整条资金主链可验收时；不为每个辅助函数、SQL改写或小优化新增一轮全套或容量压测。

出口仍为严格60秒固定到达率、逐ID交集、P99≤1500ms、全部计划发出、120秒排空、PG三项耐久开启、证据AOF always加副本确认，同构建连续两轮。最终稳定档必须严格超过3000及new-api，并补同并发对照；任何未验证的生产HA条件单列，不把架构图当证明。

## 7. 已验证的1803基础阶段

1803先安装私有空cells、不可变划拨记录结构、精确价格程序及fund fence基础设施；该阶段原root金额语义未切换，没有请求使用cell。基础表或helper不构成解除热点的证明，后续1804实际切换见下节。

价格程序由不可变源版本同事务生成并支持历史回填，融合exposure只遍历一次priced维度同时计算四组精确金额；1803时尚未接入资金调用。独立审查未发现计价或私有权限阻断，冷编译先约分、坏版本标识两项已收紧。基础guard审查发现冻结账户ΣR可超过MAX，真实反例先0通过1失败，加入独占全局R上界后通过；ACTIVE共享快路仍由覆盖与全局B界推导R范围。

held helper只证明当前backend持有对应模式，不能单独证明session/xact生命周期、整事务账户总数或完整锁序。划拨表的FK/幂等reference/不可变性也不是已实现的资金守恒；后续真实入口及事务协议必须补齐，不以基础设施审查代替主链验收。

私有实际划拨函数要求调用前已持有account EX，root和目标cell同事务反向增减balance，限额仅在全局headroom内重新分配；普通撤回只能取cell的B−R，不能动UNKNOWN等hold的预留。reference精确绑定cell及全部delta，同载荷重放返回原划拨ID，异载荷拒绝。冷路径审计分别核对cell.B+cell.S与累计预算及历史支出归属、两类cap与划拨记录。1803的4个定向用例36断言通过，含root已扣减后全局cap溢出的整句回滚。独审发现合法旧root.B=100/R=80时审计会错误返回available=100，真实0通过1失败后改为显式拒绝未迁移root。1803仍不向runtime开放这些私有函数，不把旧root预留与cells混用。

完整cut安装root guard时，双边划拨的聚合B守恒与全局范围必须检查提交态，不能在RECLAIM先增加root、尚未减少cell的中间态因临时ΣB>MAX拒绝合法操作。行内38位范围仍立即保护；冻结表示中root本身没有接收空间时，独占归一化先补有缺口的cell再回收其他free，不把表示碎片误记为真实资金不足。

## 8. 1804切换实现边界

hold持久字段为funding_cell与current_reserved_atoms，前者同账户/币种组合FK到私有cell且不可变，后者仅由受控资金函数随cell与账本一起推进。0012先允许NULL以容纳旧行，在1804同事务回填后收紧NOT NULL。回填在已有quiesced升级边界锁住账户、root、hold、经济账本和私有预算表；仅该owner迁移在排他表锁内暂时移除cell guard，全部守恒检查与新guard安装后才提交，避免为全库所有账户积累事务advisory锁。运行事务仍受128个账户上限约束。

1804引入统一汇总view core_upstream_balances_v1；root只存未分配预算/历史未映射支出，reserved固定0。管理角色改读汇总，撤销root直接权限，实际经济资金变更经core_upstream_fund_v1；freeze/恢复入口先fund EX后catalog EX，并在同连接上排队前后及返回前重核授权。HTTP与standalone充值采用4秒绝对操作预算，含池等待、栅栏及提交；SQL savepoint保留单项失败回滚语义，未知提交仍要求检查原回执。

独审要求授权迁移逐列保留旧reader权限；只有原root完整SELECT/INSERT/UPDATE角色才自动获得强资金函数，不能把UPDATE(revision)或SELECT(account_id)扩大为完整资金能力。冻结root已到−MAX但全局金额仍可负调账的表示边界，按06补充的REBASE处理：仅非ACTIVE、预持EX、原经济delta使root局部越下界而全局合法时执行，保持全部R及责任、S和公共revision，不放宽普通预算回收。

普通execution返回内部funding_slow时，Batcher已等待原外层事务完成，宿主才在新事务进入EX慢路；沿用原1秒已知BUSY重试总预算及原envelope期限。abort recovery的两条路径共享同一个至多8秒预算。证据宿主按128个实际attempt分片且不拆group，认证取钥、各片快路和原事实慢路共享8秒预算；前片已提交但后片失败时不ACK原job，原事实重投依靠已提交receipt去重。该预算和分片行为仍须经真实回归与最终容量/故障验证，不把代码写入视为通过。

## 9. 当前接入与验证范围

1804已替换最终execution item、evidence group外层预锁、supplier settle和abort recovery：正常准入/结算写cell，BEGIN不取root或cell写锁；当前R直接来自hold，不再热扫历史journal。融合计价按真实cost_version_id运行，保留每请求及每快照舍入。应用后的PG函数目录确认旧raw-rates exposure已无调用者，随后迁移删除该函数，原独立算例改走捕获的版本程序，未保留双轨接口。

首次冷预算在EX中为各单元分配预算及headroom；之后首选实际attempt UUID对应cell，局部碎片进入新外层重分配，不能因唯一热cell尚有预算而永久把所有请求送到同一个cell。root/account guard、历史hold回填、统一余额view、充值/调账/恢复与权限脚本均已接入；bench与backup额外核hold↔cell↔经济reserve、内部预算事实及恢复摘要。冻结下界REBASE只移动表示B，UNKNOWN的11 atoms真实回归保持11。

定向资金链已取得：JSON/SSE首链2例21断言；计价/基础/碎片锁隔离/成功关闭7例79断言；风险、争议、UNKNOWN和资金管理13例160断言；恢复12例103断言；历史/权限升级13例142断言；两个独审修复及锁隔离3例34断言；独立充值19例148断言，均退出0。批次有重叠，不相加当独立用例总数。首链原0通过2失败的SQL保留字错误、升级原8通过1失败的陈旧迁移数断言，以及两个独审缺陷红证据全部保留。

旧exposure移除后的独立金额算例、Core资金权限和大额预算独立准入三例24断言退出0；后者证明首个单元尚有足额free但被锁时，另一单元可完成真实准入。check348文件、lint、全仓fmt:check及架构单测7例672断言退出0；build随后退出0，全量单测与集成、artifact、durability/backup仍未在本次完整资金版本执行。

冻结旧schema夹具使用独立的旧权限文件和历史充值事实播种，不让当前产品入口绕过fence或调用不存在的新函数；升级后继续走当前真实入口。旧V1已授出/UNKNOWN的许可摘要、资金及后续增量结算通过。现有批接口仍逐item/group执行，尚未完成按cell归并的一次批量写；全量门禁、artifact、durability、backup和新容量/并发结果均未完成，S5稳定60仍是最后有效容量结论。

## 10. 切换后诊断与下一实施重点

同原S5配置运行一次严格60秒诊断：目标60、到达61RPS，1000钱包、单供应账户、并发256、worker并发8、builtin-worker，耐久要求不变。正式3660请求全部发出并逐ID匹配，窗口完成3656，P99为201.05ms，窗口后61ms排空，退出0。含预热3755请求、7510份事实及ACK，客户spent375500、供应spent221545、双方reserve0；hold/cell/经济账本及内部预算审计全部一致。该单轮明确排除稳定两轮认定。

服务CPU25.020ms/请求，其中Gateway3.262、Core2.983、worker1.081、PG16.455、三类Redis合计1.238。PG顶层40.036调用、23.443执行ms、60639.51 WAL字节/请求；嵌套统计与顶层重叠，不能相加。证据持久化和execution batch顶层分别约10.463与7.605执行ms/请求，也包含各自子语句。与S5相比P99降低约44%–46%，总CPU几乎未变；这证明解除共享root热点不能替代削减实际SQL工作量，不能据此承诺3000RPS。

下一实施重点是真正集合式的准入及结算：共享身份/目录/价格检查按批去重；每个请求先独立求精确金额和幂等结果，再按固定cell归并差额，集合写资金、事实及账本。原子group失败不污染其他group；风险、冻结、局部预算或headroom不足仍退出普通事务后走全新EX慢路。第一片归并余额并减少同一hold的重复写，之后继续迁移事实、账本及准入；只完成余额归并不能称为整条集合式资金主链完成。

诊断还发现revision-claim简单查找偏慢。独立生产DDL探针在冷计划和10000条合成receipt下复现：ANALYZE前选origin_identity单列前缀的delivery索引，过滤10001行，嵌套执行2.540ms；ANALYZE后选attempt/source/revision索引，0行过滤，0.003ms。两者都是Index Scan，并非全表顺序扫描；这些合成数据不是有效签名资金历史，也不是容量证据。该结果支持继续检查冷计划选择，尚不能证明正式轮全部慢查均来自同一原因，不以修改共享PG参数或人为提前ANALYZE掩盖成本。

原始审计及构建绑定见[诊断轮](../docs/performance/evidence-implementation-2026-11-17/s7-diagnostic-60-a/round.json)，计划及探针源码见[计划探针](../docs/performance/evidence-implementation-2026-11-17/s7-validation/receipt-plan-probe/result.json.raw)。资金SQL成本降下后，继续完成签名context/目录版本栅栏，减少context写库及重复摘要，再按新剖析处理Gateway/Core剩余成本。容量、同并发比较与故障恢复分开验收；未验证前不声称生产HA已经成立。

## 11. 1805证据批资金写入

1805第一片不改外部事实、价格、账单或ACK协议。私有结算叶子接收显式事务内pending值：首次访问真实hold所属cell时NOWAIT锁行并保存原值，随后同cell的成功组在锁下累计各自已独立舍入的cost/reserve/revision差额。hold当前R随原状态更新一次写入，后续同attempt的partial/final仍读取真实事务内head及hold。每组仅在完整成功返回后才把新pending交给外层；组失败时原savepoint撤销其事实、账单、流水、hold及新锁，外层pending不变。

普通批最后用一次集合UPDATE写每个已锁单元，核原B/R/S/revision及cap未变，并继续触发原资金guard。供应经济journal和各独立请求的分录也随组成功进入显式pending，批末分别一次集合INSERT，保留各自journal ID、reference及逐journal平衡约束；不把多个请求合成一份经济流水。原组失败不留下待写流水，批末所有供应流水约束仍在返回前强制核验。风险、冻结、预算覆盖或headroom不足仍产生P0F01、回滚原组，在宿主结束快事务后开新的EX事务。慢路使用同一私有group/leaf的即时应用模式，不复制两套业务逻辑。若最终集合写遭遇SQL错误，包含全部组及集合写的子事务先整体回滚，再在同一外层期限内按组即时应用一次，隔离故障单元；不重放未知COMMIT、不ACK未提交组、不增加重试期限。取消与断言异常不转成这种降级。

public批入口保留OID/owner/ACL，只有内部group/leaf迁为core_funding私有函数，确认实际调用者清零后删除旧public helper。新增函数权限仅给既有受信资金函数owner，运行角色不能直接提交pending。第一片仍保留逐组证据、成本head及客户结算处理，不称为完整集合式结算或据此预测RPS。

独审指出旧slow owner可能只拥有“读hold、取EX/catalog、调用public batch”的窄权限，与batch owner不同。新私有persist明确采用原batch owner的SECURITY DEFINER委托；不依赖本次迁移执行者的身份，也不向slow owner补齐所有资金表权限。group/leaf/project/flush仍为invoker，仅原batch owner获执行权；slow owner仅获私有persist委托。设置函数owner所需schema CREATE若原先没有，只在同一迁移事务内临时授予并撤回。保留原有委托边界并用不同owner的升级前后真实调用核对。

1805余额及供应经济流水集合写已接入，尚未取得新的性能数字。定向资金、身份、冻结及权限20例177断言退出0；不同owner缺口取得真实42501红证据后修复，最终权限升级2例12断言退出0；最终版本三项新资金行为与真实worker提交后ACK/重投4例51断言退出0。上述批次有重叠，不能相加成独立总数。两个独立请求各1/3仍各收1，同attempt的partial/final两个1/3合计只收1；同cell只产生一次余额更新，两个独立journal用一次INSERT写入。批末cell失败及组内供应投影后客户账单失败，均未污染有效组的金额或重投结果。

正常换证用例曾连续触及原5秒门槛。阶段及PG等待采样定位到等待后继者BEGIN拒绝，SQL后端已idle且无锁等待；改为直接await真实RPC后断言明确CoreRpcError、owner mismatch、NOT_ATTEMPTED/NONE，原5秒不变，完整用例2.412秒通过。独审支持本用例的Bun异步matcher调度差异，不推广成所有HTTP2 matcher缺陷，也不据此改产品网络代码。临时产品诊断已按原SHA恢复。原红及装置期望/迁移事务边界错误均保留于[1805验证证据](../docs/performance/evidence-implementation-2026-11-17/s7-validation/batch-1805/validation.json.raw)。

## 12. 1806客户权威与BEGIN集合执行

实施中，未形成性能结论。原内部身份九元组去重和权威行锁不变。客户授权先从真实context/permit解析key，捕获原key/project关系，按org→principal→project→key固定顺序集合加SHARE锁并重核关系。锁下生成的私有cache只活在本外层执行scope；集合预备失败则撤销其新锁并回落原逐项授权，不能令一个busy key阻断无关项。原core_auth继续invoker、保留读锁和关系检查；静态政策、自然expiry、CIDR及scopes使用同一纯判定函数。纯函数不读库不写库，仅向原授权调用所需完整key/project读取角色授予执行，不给新的表写权。

同scope候选与runtime计算在成功item间复用，须持有原catalog/epoch栅栏；item内部拒绝回滚时不把其新cache提升到外层。runtime缓存同时保存观测时刻和相关active approval最早未来trust_until，越过任一时间边界或时钟回退都重新读取allowed_v1，默认Wasm不附造trust期限。事务内customer/catalog缓存总计至多8MiB，超预算只停止memo，不删除授权检查或扩大PG共享缓存。

单项执行直接使用同一item体即时写入，不额外做客户批预读、runtime期限查询或BEGIN flush；候选仍走原函数，runtime仅检查实际选中项。多项批再集合预读、按成功scope复用并合并写入，避免批装置增加空闲单请求的固定SQL工作量。

保留唯一item业务体，迁入私有执行内核；BEGIN仍执行原发送绑定、runtime许可、客户冻结/授信、request关闭、permit一次发送、供应预扣与估算、配额proof检查。通过后暂存permit、command及audit，连续BEGIN段末分别一次集合UPDATE/INSERT；遇非BEGIN或重复attempt/command先flush，保留原items顺序及重放语义。普通错误项仍逐项隔离。

重复键必须按PG的UUID值识别，不能按请求原始文本比较。大小写等合法UUID文本变体不得绕过prefix依赖边界；签名和body_digest仍绑定原始规范化协议数据，不重写请求字节。无效UUID不在prefix准备阶段使全批失败，而交回原逐项校验处理。

flush必须再核原permit状态、完整owner/epoch及实际更新数；重新检查key自然期限、session/certificate/permit/command期限，以及去重实际candidate的allowed_v1，不能用epoch锁代替runtime trust_until自然到期。proof在首次SQL批入口锚定绝对数值毫秒界；prefix等待、整体试批回滚及即时回落都只扣减该界，不重置proof或总期限。只有全部提交完成后宿主才可能返回发送材料。

最终写失败时，包含客户cache、全部item与暂存结果的scope先整体回滚，重新建立已失去锁保护的cache，再即时逐项执行一次；不重放未知COMMIT。新私有runner显式归原item owner的SECURITY DEFINER委托，batch owner仅可委托runner；其他新执行内核不向运行角色开放。旧public item调用清零后删除并迁移权限回归。该片尚不完成集合式admit或证据处理，不据此承诺3000RPS。

1806已接入并构建，仍无新的容量数字。新BEGIN最终6例75断言退出0，含UUID别名真实红0通过1失败3断言后的修复；早前新BEGIN/身份批11例79断言、JSON/SSE首链及私有权限8例92断言均退出0；单项路径的COMMIT未知、授权持锁及同命令竞争4例25断言退出0，后两批早于UUID边界修复，批次重叠不相加。旧执行/客户权限/升级28例为27通过1失败：旧夹具把冷预算EX的合法503 UNKNOWN/SAME_COMMAND强转receipt。保留结构化响应并严格核允许的code/重试契约后，用原ID/deadline重试，原发送及PG/Redis租约断言不变，定向1例8断言及上述4例通过。该修正不允许重试UNKNOWN BEGIN，未调产品或原用例时限。

首轮4例因可空org/principal的JSON null复合行恢复而在准入失败；改为SQL NULL后首例17断言通过，后续全新BEGIN边界通过。类型检查初次3个测试行类型错误已修。最终check350文件、lint零错误/警告、fmt:check731文件、架构单测7例678断言和build均退出0。全量unit/integration、artifact、durability/backup及容量/并发仍留待完整资金主链出口。全部红绿、源码/产物绑定及清理记录见[1806验证证据](../docs/performance/evidence-implementation-2026-11-17/s7-validation/begin-1806/validation.json.raw)。

## 13. 1807连续准入集合执行

实施中，尚无性能结论。连续admit沿用唯一执行item的原鉴权、候选、估算、quote、发送绑定及额度描述符检查。普通预算选择在真实cell锁下读取事务内pending的R/revision，逐请求独立预留后暂存差额，批末每个cell只写一次；缺预算或headroom仍返回funding_slow，由宿主结束原事务后在新EX事务处理。单项、next及慢路继续使用原即时资金逻辑，不复制预算重分配算法。

客户分配先积累原allocate_customer_batch_v1输入，连续段末一次调用，保留其钱包锁、赠送/现金/到期策略及逐请求授信判定。全部成功后集合更新请求预算和attempt binding，集合插入commands、permits、upstream holds、供应预占journal/entries、额度租约及audit。每请求仍有独立hold、journal、command和receipt。任何客户分配失败或批末写入/授权期限复核失败，先撤销整个试批及暂存结果，再在原外层期限内即时逐项处理一次；不能先释放失败请求的预占来补偿已写经济历史，不能把未知COMMIT转成试批回退。

切换命令类型或碰到重复command、request、attempt、intent时先将已完成前缀写入本事务，后项才观察前项结果；UUID依PG值规范化，intent依实际project与原文本识别。无效输入交回原逐项校验。最终flush重核客户授权、context、内部session/certificate、command期限及实际runtime approval自然到期，不能用批预读延长许可。失败item不提升它的pending或cache。原子提交、真实双方资金及重投不重扣、混合失败隔离、前缀重放和自然到期均须取得真实PG证据；本段不表示已经完成整条证据集合化或容量验收。

1807已接入并构建。集合写首例先在1806得到0通过1失败2断言，实际两次钱包更新及逐条许可/流水写；新版本同例1通过13断言。新准入7项、原BEGIN6项及真实Gateway JSON双方签名入队/结算1项，共14通过187断言；随后补足预算覆盖、失败item仍保留bulk、混合query/abort前缀、运行角色私有接口拒绝、历史升级及两项未知COMMIT回归，7通过58断言，均退出0。批次有重叠，不相加。预算覆盖用640总额初始化每cell40，两笔各30实际选择cell0/1，且upstream_holds仍一次插入两行；失败描述符位于供应pending之后，前后两个合法item仍一次插入两行，失败项无hold/journal。600客户额度两笔只能接受一笔，另一笔无供应预占，重放不重扣。批末audit故障则原试批全部撤销，逐项留下首尾合法项，移除故障后仅原失败命令新增预留。

初次装置缺少abort reason、提议UUID大写被协议拒绝、直接UPDATE immutable context均是夹具失败，未作为产品根因或资金边界红证据。UUID冲突用合法原小写值，context用真实捕获的snapshot/owner初始插入一份短期新ID记录，原immutable guard始终开启且body_digest绑定新ID；key/context在后项延迟中自然到期后均未产生前项资金。直接SQL集合用真实身份、捕获context及Redis DRAFT描述符；quote输入复用公开报价函数，但金额、余额、条数及预算覆盖期望来自独立手算。Gateway首链另经真实HTTP与worker结算验证，不能把SQL入口用例称为完整外部ACK证据。

静态独审未发现新增资金/事务缺陷，指出的预算覆盖及失败item pending证据已补足。check351、lint零错误/警告、fmt732文件、架构7项681断言及build均退出0，最新产物SQL/manifest已与源码逐字节核对，39个迁移全部文件checksum一致。原始红绿、源码/产物及清理记录见[1807证据](../docs/performance/evidence-implementation-2026-11-17/s7-validation/admit-1807/validation.json.raw)。完整unit/integration、artifact、durability/backup及新容量/并发未运行，继续留待完整资金主链出口，最后有效稳定档仍60。

## 14. 1808证据事实与结算集合执行

实施中，无新容量结论。新事实组沿原唯一group决策体执行验证、所有权、版本声明、final维度、冲突和金额判定；普通全新事实在实际attempt/permit/hold锁下，一次捕获各attempt的价格、原canonical/usage、最大快照版本和已接受source进度，事务内推进独立RAW/CANONICAL行及回执。段末集合写index、payload、delivery、receipt和attempt/request状态。版本仍按每attempt逐份快照推进，父子摘要与每份签名不合并。争议、重复、重放或隔离事实沿同一决策体的即时写模式，保留原特殊路径。

供应结算的cost/head及hold状态在组内完成全部独立计算后集合写，再调用现有客户批结算；供应cell及journal继续由1805的跨成功组pending收束。后组必须读到前组已写的canonical/cost/hold，不能把partial/final跨组提前归并后再舍入。任何组失败都撤销该组事实、客户结算和新供应pending，未提交事实不ACK。

多个原子组总事实不超过64、且无跨组attempt/event冲突时，可试合并为一次执行。取得原身份及资金锁后，必须按实际request/hold重核：不同原子组的钱包互不相交，且无到期lot时间边界；所有事实必须仍是未争议、未隔离的NEW。这样一次客户结算不改变共享钱包的资金分配顺序，原组内部顺序及原子性保留。返回时按原组拆回receipt和billing。条件不满足或合并执行异常时，先撤销整个合并试批，再用原输入及原期限逐组处理；任一组的问题不能扩大到其他组。共享钱包等路径继续逐组，不能声称全部场景已完全集合化。

需以真实签名入口证明集合写、逐请求独立金额及输出归属、重放不重复、同attempt跨组演进、失败组完整回滚、共享钱包顺序、吊销/隔离及提交后ACK。不得以更多SQL helper或改慢路时限推算3000RPS；主链实测仍须在完成验证后进行。

1808已接入并定向验证。两组各两份签名事实在1807原实现实际逐份插入index/payload，0通过1失败12断言；1808先实现组内各4行集合写，但跨组guard因billing_holds表别名h与原upstream_holds行变量h冲突而回退，首例仍失败。只在隔离夹具临时显露异常确认42703后改为bh，移除诊断块，正式路径1通过27断言：8份快照、4份receipt、2份独立cost/head/hold/customer charge均分别一次集合写。原签名及每请求舍入保持，两请求各1/3供应成本分别为1，总支出2，重放无新经济写。

新证据、原供应批、签名/吊销和额度关闭18通过202断言退出0。静态独审又指出窄列owner兼容：整行to_jsonb(t/q)引入不必要的读列，显式index.created_at写也扩大原INSERT要求。真实1807→1808升级用例证明旧路径成功、升级后42501，0通过1失败8断言；改成只捕获原必需列、index沿默认now而payload用同一transaction_timestamp，不增加授权，同例1通过16断言。公开批owner、私有persist owner和迁移owner可不同；三项受限列权限升级前后均为false。缺少合并附加条件读取能力时仍可回落原组处理，不能把frame权限失败变成无限重试。

最终版本新证据6项、运行角色私有内核拒绝1项及真实Gateway JSON签名入队/worker结算1项，8通过148断言退出0；原18项早于列权限修复，不相加。覆盖合并试批后某组内两个请求任一账单失败时整组无事实/费用、原事实重投只补该组；共享钱包与有到期lot均保留逐组顺序；同组两个revision保持final维度、客户最终3、供应整体舍入1。类型352、lint、格式733文件及架构7项684断言均退出0。完整模块单测446通过8008断言、build退出0，40个迁移全部checksum及源码/产物SQL和manifest逐字节一致。首轮全量集成562通过、10失败、13229断言、退出1；针对真实EX栅栏、副本拓扑、异步资金/配额收尾及历史版本修正夹具，原10项定向全部通过118断言、退出0，修正后全套尚未重新验收。产物5脚本41项检查、原生耐久7项及独立停写夹具的备份恢复均退出0。备份包含3个真实请求、6份receipt和外部ACK、客户支出300、供应支出45、两侧R=0，恢复资金及不可变摘要一致；自有dump校验SHA后删除。新容量与同并发尚待实测，最后有效稳定档仍60；完整失败及夹具迁移依据见性能报告§16。

## 15. 高负载下的证据回滚写放大

1808构建300目标/301到达、60秒、其余沿原1000钱包/单账户/worker8配置，退出1。计划18060、丢弃3398，正式HTTP9362成功和5300个503，P99为2225.87ms；严格窗口交集5540，即92.33RPS。含预热9458个已准入请求最终均结算，18916份事实及ACK、客户支出945800、供应支出558022、双方R=0，资金/cell/hold/流水一致性项无异常；原审计仍因失败请求产生wallet_balances及HTTP错误，不能宣称该轮全部有效或通过容量。

PG统计有效、无驱逐，顶层22.299调用、71.014执行ms、104190.38 WAL字节/已准入请求；PG CPU57.619、服务合计64.630ms/已准入请求。分母只含已准入而CPU还包含拒绝请求的工作，不能将它当正常全成功成本或直接与60档比较。61168次stage_snapshot与19702次frame捕获表明事实处理反复执行，但这些计数本身尚不能分解全部CPU损耗。

独立静态审查定位：普通组先flush_facts、更新成功请求及配额，再由supplier_settle→project首次对cell取NOWAIT锁。多worker争用时，后期55P03使已写事实回滚；合并试批回退原组，worker再重试，重复付出CPU和WAL。真实签名入口确定性回归锁住第二个实际cell后，两组共4份事实原本只应留下第一组4份快照；非事务sequence记录实际尝试16行，0通过1失败8断言，证明12行无效写入后回滚。

修正方向：普通全NEW、无争议且可用事务frame的组，在原mode/授权核实及hold锁取得后、创建frame及任何事实写入之前，按实际不可变hold指向的(account,currency,cell)排序NOWAIT预锁。只包含ESTIMATED事实的attempt不触发供应结算，不取该预算锁，仍保留原客户结算检查与成功收尾；独审指出初版扩大此竞争面，真实回归0通过1失败、55P03后收窄。竞争仍只返回原55P03，原组隔离、成功组、UNKNOWN预留及新外层EX慢路不变；duplicate/replay/争议和即时慢路沿原特殊逻辑。该改动减少已证实的晚期失败浪费，不代表全部高负载成本已消除；必须比较相同300配置的后续诊断，不能从16→4行推算3000RPS。

1809已实施，旧40项迁移不变，新文件只在原group正文增加上述预锁，保留owner及ACL。原晚锁症状绿1通过22断言；ESTIMATED初版红0通过1失败1断言后，最终全部证据8通过137断言、类型352、lint及build退出0，41项checksum及源码/产物一致。早前13项资金/单元203断言和3项版本消费者54断言通过，早于最后ESTIMATED收窄，不把重叠批次相加。

相同300档后测仍退出1：窗口5603/18000目标、P99 2190ms、2782丢弃、5229个503；含预热10143个真实请求全部结算，20286份事实及ACK，客户1014300、供应598437、双方R=0。PG统计有效、CPU采样错误0，PG CPU降至48.287、服务54.782ms/已准入，WAL70172.11B/请求；相对前轮分别约降16.2%、15.2%和32.7%，窗口容量仅升约1.1%。CPU包含失败请求工作，不当作正常成功路径成本。事实flush调用19717→9063，证明晚失败浪费降低；回执revision查找仍72366次、69.013秒，预检20286次、61.791秒，须进一步取得计划和减少实际工作量。完整数据见性能报告§17；正式稳定仍60，未将两轮失败改为通过或完成目标。

## 16. 实际查询计划与下一步数据访问改造

1809产物未变，在独立夹具库以30秒200到达、1000钱包/单账户/worker8运行采样auto_explain。它增加剖析开销且没有容量交集采样，不用其RPS或CPU作容量结论。按本库采样PG PID筛出7831条目标计划，解析错误0，归档各类首3/末3/最慢8份代表性计划。实际证据：预检取3条租约却顺扫4866条permit、1380个缓冲块；客户Key预读顺扫6042条context及4851条permit；证据permit点锁顺扫并过滤4713条，计划转主键后只命中3个缓冲块；回执revision查找走delivery索引、过滤9676条、命中9858块，而应按attempt/source/revision定位。数据支持先解决随历史增长的访问放大，不能把成本全部归为资金强保证。

随后保持同一1809产物及300/301严格诊断配置，仅对新自有库连接设置force_custom_plan，原管理员连接保留既有auto。窗口9153，即152.55RPS，PG CPU32.382、服务39.109ms/已准入，排空834ms；前一default诊断对应93.38RPS、48.287/54.782ms、14194ms。仍有3231计划丢弃、5452个503，P99为1892.11ms，退出1。含预热9460请求/18920份事实及ACK全部结算，双方R=0，PG统计有效。它是一次规划方式实验，未修改产品配置、未证明稳定档；CPU仍含拒绝工作，不能从这一次差值推断普遍比例。

全局custom实验中预检仍18920次/63.703秒，其租约连接47.382秒；因此下一改造应同时处理查询形状及缓存计划：空attempt时直接返回原空租约集合；先解析实际request，再按租约绑定的attempt做许可点查；对已证实错误计划的receipt和数组点锁采用实际参数规划，同时保留原锁序、NULL/owner/授权及输出语义。纯计算JSON/计价继续复用适当计划，避免对全部递归SQL增加规划工作。客户权威预读也须按真实context/permit键访问，不能因集合接口仍扫描全部历史。以上是后续实现面，当前未接入，也没有以实验改写41项迁移。

本次采样阈值选择有误：auto_explain.log_min_duration为整数毫秒，0.5ms实际取整0，嵌套计划输出使共享PG日志追加约2.22GB。夹具已自动关闭并删除，之后检查无该库，未终止其他连接；只读独立连接以sample_rate0核实取整后关闭。共享日志原内容及其他连接输出未截断，新增尾部暂留，磁盘约31GiB。后续只在可独立清理且有输出上限的日志夹具采样，不重复本配置。两轮数据、实际计划、局限和资源清理见性能报告§18。

## 17. 预检的参数化定向读取

先处理已证实的preflight热点，保持原core_caller及客户授权校验、owner/ctx绑定、结果NULL语义和lease_id排序。空attempt本来应返回空leases及NULL permit/closed，直接跳过无意义的许可连接。实际attempt先按主键取得permit、原context和request ID，再按request租约逐个查询其真实attempt的permit epoch；LATERAL保留每个租约至多一个许可的原主键关系，不截断租约或增加业务数量上限。已定位的易陈旧点查用常量SQL加USING绑定按实际参数规划，纯计算函数和其余连接默认不改变。

独立冷表增长回归使用真实注册身份、准入、原context和三条租约。固定连接先在空许可表预热，再准入并停止仅自有Core，随后增加4096份仅用于查询访问的合成attempt/permit元数据，保持外键但不作为有效资金历史或容量证据。两次公开preflight继续核对原NULL结果及三条租约，并通过内建PG表计数核读取量，避免绝对时间断言和额外扩展依赖。所有DDL必须在预热前完成；最初几种准备在预热后首次创建pg_temp，虽通过但未复现缺陷，准备顺序对比与search_path变化使旧计划失效相符。固定准备顺序后，旧实现0通过1失败9断言，两次查询读取4098条许可而预算≤16，取得真实红证据。

新增1810迁移只替换预检读取正文并保留owner/OID/ACL，不更改41项历史资产，不将全局force_custom_plan实验直接设为产品默认。运行角色权限、完整客户授权、三方幂等及资金SQL不变；该改造与剩余证据/批权威查询仍须分别测量和验收，不预估达到3000。

1810已接入。原症状绿1通过9断言，许可seq_tup_read增量从4098变为0，相关index扫描增量4；移除临时输出后，新回归及原核心执行/真实JSON和SSE首链共18通过175断言，三个历史版本消费者3通过54断言，均退出0。类型353、lint零错误警告、格式738文件和build退出0；42项checksum及SQL/manifest源码产物一致。独立只读审查未发现新增授权、权限或租约语义问题，未扩大角色或删除权威重核。

原default的300/301诊断仍退出1：正式11639个HTTP成功、5017个503、1404丢弃，窗口5757即95.95RPS、P99 1821.73ms、排空19351ms。含预热11730请求全部结算，23460份receipt及ACK，客户1173000、供应692070、双方R=0，资金/cell/hold及流水异常0。PG统计有效、采样错误0；PG CPU42.338、服务48.353ms/已准入，顶层21.177调用/50.970执行ms/68792.84 WAL字节每请求。preflight为23460次/9950.501ms，即0.848ms/已准入，前default1809为6.092ms；这是SQL执行成本，非单项CPU。整体窗口容量只升约2.7%，证据仍积压，不能把预检收益解释成整链路达到目标。

应用级receipt冷缓存原型在两种准备下旧实现均通过读取预算，未取得该入口的红证据；原型及输出保留为诊断，不将其作为已修复用例，也未改receipt或数组点锁。后续优先结合实际计划复现这些剩余高成本路径，保持原schema和资金语义。当前正式稳定仍60；完整全套门禁、同日new-api重测及>3000/同并发/生产HA验收未完成。数据及清理见性能报告§19。

## 18. receipt唯一索引的访问方向

1810后实测receipt revision查询仍89114次/70.621执行秒。原delivery唯一约束(origin_identity,event_id,body_digest)让低基数origin成为单独可用的索引前缀；实际计划已显示revision查询走它并过滤9676行。独立只读盘点确认三列均NOT NULL，产品delivery读取都带完整三列，未找到origin-only入口、依赖该列序的ON CONFLICT或Drizzle消费者；指向receipt的FK只引用id。候选是同名唯一约束改为(event_id,origin_identity,body_digest)，重复三元组集合完全相同，也不把event ID改为全局唯一。

新schema访问回归使用一次真实签名receipt作种子，在所有DDL完成后以原SQL形状预热，再增加10000条同origin合成索引数据；不将这些克隆行称为有效签名资金历史或容量证据。公开SQL按attempt/origin/source/revision/revision_claim查缺失版本，旧布局实际访问10001行，0通过1失败5断言，取得红证据。绿侧须同样在预热前完成新DDL，不能把重建索引导致的计划失效当优化收益；观察真实行访问预算及已知版本结果，验证完整三列重复拒绝、同event跨origin和同origin/event不同body仍允许。

重排不能保证优化器永远选择期望索引，仍可能出现全索引扫描或其他冷计划问题，必须以回归及整链路实测决定效果。迁移在既有停写边界事务内重建唯一约束，不改原42项历史资产、event/revision部分索引、字段、金额、签名或ACK。Drizzle定义、增量迁移与快照同步，角色权限不扩大；本节为当前实施依据，尚无新的容量结论。

1811已完成接入与定向验证。相同冷计划原查询回归从10001行读取降为0，已知receipt、完整三列唯一性及允许的不同origin/body语义通过；最终索引/证据批/真实JSON10项180断言和三项升级消费者54断言均退出0。43项checksum及DDL/manifest源码产物一致，旧42项未修改。含10003行重放DDL保留原签名receipt及账目，但它是当前schema含数据重建，不冒称1810前缀升级或在线无停写证明。

相同default300/301诊断仍失败：窗口6924即115.4RPS、P99 1625.91ms、793计划丢弃及5253个503。PG CPU35.908、服务41.758ms/已准入，较1810对应42.338/48.353降低，分母含预热且CPU还含拒绝工作；单次失败不作稳定提升结论。12103个已准入全部结算，24206份receipt与ACK，客户1210300、供应714077、双方R=0，资金/hold/cell异常0。revision查询不再在嵌套前25条，只能给出其执行总量<9803.109ms的上界，不称0成本。请求/许可锁、权威读取、证据事务及计算仍消耗显著，后续继续以实际访问和批处理证据削减主链成本，不猜测跨job批大小或冲突的占比。最后正式稳定仍60，>3000、CPU≤2及同并发/生产HA/最终完整门禁未完成。详见性能报告§20。

## 19. Worker跨队列任务的有界合并

1811后的代码盘点确认：UsageProducer有64组/1ms有界收集，UsageConsumer的每个job却直接调用独立store.persist；现有集合SQL只能处理单个producer job带来的组，不能利用多个就绪job。该事实来自调用边界，不把9302次persist直接折算为实际平均组数，也不声称其已解释全部PG CPU。下一片只改Worker进程内调度，不改schema、金额、签名或耐久队列。

使用现有加权Batcher，跨job收集至多128个完整group，保留每个原group及同job顺序；每次存储返回必须与输入等长，再按原job区间拆回结果。最多两个存储调用并行，忙时收集1ms，空闲时立即启动，待排队group上限4096，另有至多256个group执行中；局部重试job还保留原输入和结果，总驻留量同时受active job上界约束；队列Worker已有最多64个active job的配置上界。超出内存等待上限的job失败回队列，不丢弃或提前ACK。该并行度是待测实施选择，不以少并发必然更快为假设。

每个job仍只在自己全部group得到非ERROR持久化结果后完成；SQL已提交的错误隔离与锁冲突组沿用原局部重试。跨job共享的是一次存储调用，不能把整个2秒锁重试循环绑在一起；55P03/40P01只重投失败group，别的job可以及时ACK。存储异常包括未知COMMIT必须使本次调用所有job失败，无合成成功或补偿退款；重投仍靠原幂等键。无效签名由原存储层返回该group的REJECTED，不影响同批其他组。

关闭时先停止消费并等待原consumer关闭，再停止接收新处理并等待已经接收的processor任务结束；已有应用级shutdown grace和数据库绝对deadline继续作最终边界；存储8秒从真正进入persist开始，不包含Batcher排队，不能当作整个job的8秒期限。不能在pending batch尚可写库时仅因consumer关闭就宣布本地任务已排空。待定向证明真实队列跨job合并、每job结果/ACK独立、未知提交全批不ACK及关闭排空后，再测相同容量诊断，不能直接宣称达到3000。

Worker内部readyz新增persistence快照，由现有bench-system时间线直接采集：calls为实际host存储调用数，groups/events包含锁冲突重投，max_groups为最大调用组数，lock_conflicts仅计存储返回55P03/40P01的group，errors计抛异常及结果数不符的调用，active_jobs为尚未结束的处理任务。它们不是已提交请求数或ACK证明；SQL内部回落/慢路不可从该计数独断。用于下一轮量化实际组大小与冲突，而不增加逐请求日志或改变队列内容。

独立只读审查未发现新增金额、结果归属或提前ACK阻断。原consumer关闭会强制断开Redis，可能在PG排空前使已提交job仍未ACK；本片保证本地已接处理排空，不宣称shutdown期间全部ACK，后续依靠队列重投和SQL幂等。未知COMMIT单测只证明异常传播与等待，真实跨job COMMIT断链及强制关闭在途任务交错仍须在后续故障验收证明。

v1已接入并完成定向验证，但不接受为吞吐改进。原真实8 job回归红0/1、11断言，新版1/0、19断言；原ACK/锁重试/告警三项通过，最终相关单测9/0、43断言。相同300/301轮PG CPU35.908→23.968、服务41.758→28.803ms/已准入，PG入口9302→3002；窗口6924→5204，即115.4→86.73RPS，排空15692→41236ms，仍111丢弃/3002个503，退出1。末次周期采样平均8.738组/存储调用、最大69组、11115/26214组输出为锁冲突，含重投且非精确终态。资金15039已准入/30078receipt及ACK全部一致、双方R0，不抹去容量失败。数据反驳“只降低并行度扩大批即可提升窗口吞吐”，下一片调整job合并边界与事务并行，不据此宣称单一因果或稳定档提高。细节见性能报告§21。
