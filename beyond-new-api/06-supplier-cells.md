# 06 供应预算单元修订设计

2026-11-17。状态：下一实施单元的设计，资金产品代码尚未切换。依据是用户的完整性能与生产可靠性目标、P0/S2测量、当前SQL和独立只读审计。本文修正03的供应单元数学及锁假设；不改变外部金额、收费、UNKNOWN、幂等、精度或吊销规则。实施进度见[05](05-implementation.md)。

## 1. 为什么修改原表示

原allocated=Σquota与普通结算quota-=cost、unfolded+=cost而allocated不变互相矛盾。若保持原折叠公式，至少应为allocated=Σ(quota+unfolded)，还需处理折叠后的负allocated及负quota。另partial/risk/dispute的当前reserve并不总等于estimated。直接套用“每次减完整estimated”会重复释放。

采用更直接的表示：root保存未分配预算，16个cell保存各自预算。内部划拨不改变经济余额，不再周期性把同一成本从root扣第二次。先保留逐项子事务，不同时重写集合式客户资金算法。

## 2. 数据与不变量

| 对象                      | 新含义或字段                                                                                                                   |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| upstream_account_balances | root的balance_atoms、spent_atoms、adjustment_atoms、revision；删除无消费者后的root reserved_atoms                              |
| upstream_balance_cells    | account_id/currency/cell为主键，cell为0至15；balance_atoms、reserved_atoms、spent_atoms、revision、spent_limit、revision_limit |
| upstream_holds            | 不可变cell指针；新增当前reserved_atoms，与初始estimated_atoms分开                                                              |
| 内部划拨事实              | 不可变预算划拨、回收及迁移归属事实，绑定真实账户/币种及命令或attempt；不计入经济expense                                        |
| 汇总投影                  | 所有外部余额、对账、管理回执、审计和恢复读取汇总，不把root当总余额                                                             |

```text
B = root.B + sum(cell.B)
R = sum(cell.R)
S = root.S + sum(cell.S)
available = B - R
revision = root.rev + sum(cell.rev)
adjustment = root.adjustment
```

ACTIVE账户必须root.B>=0、每cell.B>=cell.R>=0，且cell.R等于归属该单元的hold当前reserve之和。正常预扣只在本cell.B-cell.R>=E时增加R。普通无风险结算要求DeltaCost+DeltaReserve<=0，更新cell.B-=DeltaCost、cell.S+=DeltaCost、cell.R+=DeltaReserve，并同事务更新hold当前reserve和原经济分录。release只减该hold当前reserve，重放不再减第二次。

当前成本只允许非负增量；core_supplier_settle_v1明确拒绝supplier_cost_regression，RELEASED hold也拒绝新结算。终态矛盾按现有dispute及reserve_floor处理。本次不新增负成本修订或自动退款。

账户非ACTIVE时真实证据仍必须处理，走独占慢路径；不得把停用当作丢弃已发生成本的理由。所有普通与慢路径继续保留客户预扣、账单、两侧分录、事件及receipt的原子性。

## 3. 精度与修订号预算

MAX=10^38-1。每cell单独不溢出不能证明汇总不溢出。分配本地上界，保持：

```text
root.S + sum(cell.spent_limit) <= MAX
root.rev + sum(cell.revision_limit) <= MAX
0 <= cell.S <= cell.spent_limit
0 <= cell.rev <= cell.revision_limit
```

普通写只检查本cell上界；局部预算用完但全局仍有空间时，整组回滚并在新外层事务起始独占重分配。root资金操作增加公共revision时也必须在慢路径回收足额revision headroom。内部纯预算划拨不增加公共经济revision，经济事件沿用原递增次数。

真正全局spent/revision耗尽与局部cap耗尽分开。当前单attempt COST_OVERFLOW处置不能代表累计counter溢出；后者保持fail closed、未ACK证据及报警，不伪装成已结算的稳定REJECTED。ACTIVE普通路径的B/R上界由预算覆盖保持；非ACTIVE和风险路径锁齐后核对完整范围，不能借负cell与正root抵销绕过本地精度。

## 4. 可推进的状态栅栏

独立库中16个持续共享读者的探针表明：KEY SHARE或SHARE下，普通FOR UPDATE写者直到8秒statement timeout仍未取得锁，新读者继续进入。仅设置较短lock_timeout不足以给整个操作建立截止时间。加入事务级advisory RW后，写者约39ms取得独占，排队后没有新读者进入。此为与集成并行的推进性探针，不是容量测量；原输出readers_completed_after_request实为读者新进入数。

资金状态以按真实account派生的事务级advisory RW为权威栅栏。普通admit/begin/settlement使用shared TRY，取不到立即退出当前SQL工作；控制和风险在新外层事务入口排队取得exclusive。key由数据库派生并使用独立namespace，不能来自computed、GUC或调用方声明。账户行只为实际UPDATE和FK使用必要的NO KEY UPDATE；普通结算不再显式写锁account或主余额。

直接state/mode变更、预算和headroom控制也必须经过独占栅栏。触发器在任何可能升级的TRY之前检查实际锁管理器中既有的granted ExclusiveLock，匹配当前backend、database及完整advisory key；不满足则整句回滚。不能把TRY exclusive返回true解释为预先持有exclusive。新增列和内部事实保持最小权限，runtime不能绕过受控函数直接改cell或hold归属。

2026-11-17小探针已证明原“TRY验证已持EX”措辞不充分：同backend只有ShareLock时，pg_try_advisory_xact_lock仍返回true并新增ExclusiveLock；commit后该连接advisory锁为0。实际classid/objid/objsubid与64位key的高低32位及类型1相符。pg_locks只证明当前模式，不能单独证明transaction scope或入口时序；还必须由受控入口只用xact锁、普通/慢路明确分支、敏感DML权限和commit/rollback后零遗留共同保证。当前没有把这套fence切入产品。

同一外层事务重复shared advisory可重入，即使已有exclusive排队，因此批大小和事务总期限必须有界。不能把探针中的“无新进入”解释成旧批后续item也被阻止。风险升级失败必须结束外层事务后重试；单项savepoint回滚不一定释放此前成功组的shared锁。

## 5. 目录与锁顺序

目录epoch同样需要可排队的catalog advisory RW：reader先shared TRY，再epoch SHARE；产品目录写入口在取得源行锁前排队exclusive，再变更数据和epoch。来源表I/U/D、TRUNCATE的trigger用TRY兜底，不在现AFTER失效函数里阻塞排队。

可能修改账户状态的资金writer统一：

```text
按真实account排序的fund exclusive
  -> catalog exclusive
  -> account NO KEY UPDATE NOWAIT
  -> root和按币种/cell排序的单元
  -> request/hold/attempt/permit及原账务操作
```

只在事务入口排队；反向行锁、source relation锁和普通reader均保持NOWAIT。当前plugin bind先锁channel/account、recordProvider先锁account的路径须同步调整，不能留下“持account等catalog、持catalog等account”的环。NO KEY UPDATE不阻挡journal FK的KEY SHARE，避免以FOR UPDATE给原本并行的外键检查新造热点。

普通begin在fund shared下保留原客户wallet冻结/授信、request关闭、hold/permit、配额proof与估算绑定。供应cell指针来自持久hold。覆盖不变量在shared栅栏内不会被控制或风险操作破坏，因此begin可以不取供应余额或cell写锁。

连接池排队、fund等待、catalog等待及执行共享一个owner绝对期限，等待不得各自重新获得完整预算。取消覆盖尚未取得锁和正在执行的SQL，提交结果未知继续按UNKNOWN处理；超时/取消不能触发ACK。取得栅栏后重核期限、证书时段与管理凭证自然到期。具体预算随真实调用方落地并测试，不以当前8秒statement timeout冒充整个操作期限。

## 6. 预算不足与碎片

优先使用UUID分布的首选cell；不足时可改选有足额free预算的单元。root补拨grant=min(root_free,max(need,chunk_target))，必须grant>=need且不超root实际free。root不足时不能立即报402：root=0、cell0(B=50,R=45)、cell1(B=50,R=0)、新E=10的真实可用为55，应选择cell1。

总预算足够但分散时，起始独占慢路径回收并重分配未reserved预算。发现需要慢路径的普通事务先BUSY回滚，不带已有shared/资金锁等待升级。UNKNOWN的R永远不回收。只有锁齐后的真实汇总不足，才生成持久资金拒绝。

恢复ACTIVE前检查全部币种并归一化覆盖。例如root=0、cell0(B=-30,R=10,S=90)、cell1(B=40,R=0)，总B=R=10；回收cell1的40后补给cell0即可恢复B=R=10，没有释放UNKNOWN或新增成本。充值继续不自动复活账户。

## 7. 历史迁移

冻结表示还须保留原NUMERIC38的全局调账域：令M=10^38−1，root.B=−M、cell.B=cell.R=10、adjustment=−M+10时，再调账−1的两个公共金额仍合法，不能因root单行变成−M−1而拒绝。非ACTIVE且实际预持fund EX时，允许单独REBASE表示事实，把必需的正cell.B移回仍不大于0的root，再记原经济delta；cell/hold.R、S和公共revision不因REBASE变化。普通RECLAIM仍只取B−R，不把REBASE当可重用free或UNKNOWN退款，恢复ACTIVE前照常核全币种覆盖。

存在性依据：r+d<−M且r+d+Σcell.B≥−M时，需要k=−M−r−d>0，则Σmax(cell.B,0)≥Σcell.B≥k；且r+k=−M−d≤0，因此可从正cell.B取足k而不产生正root预算或改变总B/R。REBASE独立记录负budget_delta，使内部预算审计仍成立。

只在既有quiesced升级边界执行。按真实账本净额核对每个hold的reserve及expense，所有可映射旧hold含PARTIAL/SETTLED/RELEASED均赋不可变cell；无法映射的旧成本留root，不猜测归属。

有覆盖的旧账户：cell.R为归属hold净reserve，cell.S为净expense，cell.B=cell.R；root.B=旧B-旧R、root.S=旧S-sum(cell.S)。cells.rev=0、root.rev保留旧revision。内部迁移分别记录预算R和历史spent C归属，不能把R+C从root.balance再扣一次。

非ACTIVE且underbacked的旧数据可用root.B=旧B、cells.B=0、cells.R=原净R的冻结表示，避免旧B=-MAX且R>0时旧B-R本身超出存储精度。旧状态、UNKNOWN及经济分录保持；恢复前再独占归一化。账本不一致、缺失映射或ACTIVE覆盖不足明确拒绝迁移，全事务回滚。

## 8. 完整迁移面与验收

必须同时迁移core_execution_item_v2、最终core_evidence_group_v1的外层预锁、core_supplier_settle_v1、abort/recovery、资金充值/调账/对账与setState、所有root读者、Drizzle/FK/guards/roles、bench/backup/durability/artifact和直接播种/断言。只改内部settle不会消除外层account/main balance串行。

保留原经济journal语义。validate_upstream_journal当前只验证单笔至少两行且和为零，并非counter总账一致性证明；新增汇总审计核对B/R/S、hold净reserve、expense及内部划拨守恒。

优先验收：不同cell并行且不触主余额写锁；持续读流下风险/目录writer取得进展；旧批shared重入有界结束；风险必须跨外层事务重试；plugin/FK反序快速失败；两级等待共用期限并清理锁/连接；冻结两种先后交错；碎片不假402；headroom全局上界与局部重分配；充值/调账回执同一受锁状态；跨币种恢复不释放UNKNOWN；历史partial后续只记增量；划拨/审计失败全回滚；升级前后汇总完全一致。全部仓库门禁与同构建容量连续两轮仍为阶段出口，不能由16个cell推算3000 RPS已成立。

## 9. 实施前的最终消费者核对

以下是当前最终定义的运行入口；被后续SQL覆盖的旧同名函数不是独立运行路径。此表是只读审计与实施清单，不是已迁移声明。

| 入口                                                            | 必须保持或迁移的行为                                                                                                        |
| --------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| core-execution-runtime.sql的core_execution_item_v2              | admit/next改cell及hold当前R；BEGIN保留V2构建、客户wallet冻结/授信、quota、估算及owner检查；abort仅释放当前R                 |
| core-execution-recovery.sql的core_abort_recovery_v1             | 独立迁移资金写；保留撤权/身份失效后的恢复释放规则，UNKNOWN不能按时间释放                                                    |
| core-batches.sql的最终core_evidence_group_v1                    | 外层在supplier_settle之前已经预锁account/root，须与内层同时迁移，不能只改结算函数                                           |
| core-settlement-authentication.sql的最终core_supplier_settle_v1 | partial/final/risk/dispute/overflow与真实账本净额；SETTLED仍可能在后续争议恢复reserve，不能只迁活跃hold                     |
| upstream-funds/recharge.ts、adjustment.ts                       | root仍承载充值/调账经济金额；回执、覆盖、精度和expected revision均读受fence保护的汇总，负调账先归集free预算                 |
| reconciliation.ts及accounts.ts的updateAccount                   | resume和DISABLED→ACTIVE是两条恢复入口，均须全币种覆盖与归一化；充值不自动恢复                                               |
| upstream-funds/index.ts及对账/余额API                           | B/R/S/available/revision切汇总；历史已保存回执保持原值，不重算旧事实                                                        |
| roles、FK和guard                                                | 管理角色目前仍能直接写root，须迁受控入口并收紧权限；hold cell指针不可变，当前R可变；追加迁移在quiesced事务处理旧guard及回填 |
| backup/durability/bench/artifact                                | 对账断言读汇总，备份验证cells/归属/内部划拨及恢复权限；不能只看root或单笔journal和为0                                       |
| evidence archive                                                | 原归档只有经济事实；是否增加内部划拨须明确版本与历史读取边界，不向旧经济archive补造内部预算历史                             |
| S3b runtime approval登记/撤销                                   | 新catalog writer不能遗漏；须在package SHARE及approval行写锁之前排队catalog exclusive                                        |

公共revision必须保留原递增次数：admit/abort即使金额0仍+1；supplier settle仅cost或reserve变化时+1；reconciliation不增加balance revision，resume改变独立account.revision；纯内部划拨不增加公共revision。非ACTIVE/风险慢路须检查完整聚合B/R/S/revision及全部局部范围，不能仅检查spent/revision headroom。

普通组发现碎片、风险或局部headroom不足时，整组回滚但不得记作稳定资金拒绝。persist外层可能已完成其他组并持有shared；宿主须先等待该外层事务提交或回滚，才在新事务入口为待重试组取得exclusive。现有wrapper把异常变成组ERROR，worker只识别两种锁错误，因此需要显式、可测试的内部慢路协议及统一绝对期限，不能在原savepoint内排队升级。

## 10. 身份与授权writer推进的相邻前提

同类SHARE结构也出现在service instance/certificate/session、客户organization/principal/project/key以及管理凭证。internal_identity_change_v1即使只吊销session也先对实例取FOR UPDATE；core_auth_v1按客户四层取SHARE NOWAIT，而停用/缩权为普通UPDATE。现有共享锁探针与代码结构说明有高负载writer饥饿风险，尚无这些业务链路的负载事故复现。取得写锁并提交后的原线性化仍成立；NOTIFY和读取缓存TTL不能解决写者尚未获得锁。

在取消这些权威行锁前，必须先明确每个真实(service,instance)的identity RW fence及客户四层授权fence；普通权威读、challenge/register、历史evidence/recovery都不能遗漏。定位关系的初读不是权威，取锁后继续重核当前关系和自然到期。不能为了复用current/core_auth而改变历史证据仍处理、撤权后仍可恢复释放的规则。管理凭证同类结构另列审计，未将其写成已复现缺陷。

单域writer在新外层事务入口取得该域exclusive，多对象固定排序；fund→catalog顺序保持，其他域不得引入持源行锁再阻塞等待的反向环。实施先在现有root表示下验证全部写入口、共同期限和跨事务慢路，再引入cells与移除冗余热行锁。持续读下的逐层吊销推进、排队后新读者BUSY、注册/吊销交错、历史worker/recovery、shared不能原地升级、事务结束无锁遗留都需要真实用例。

## 11. 第一完整实施片的边界

先只改catalog写者推进，保留root金额表示、身份及客户行锁，再做每账户fund fence、cells及全量汇总消费者，最后减少逐项SQL。catalog片不宣称解决身份吊销推进，也不改变canonical、receipt、V2构建绑定及账务公式。

除core_candidates外，现有evidence_group、abort/recovery及充值都持有目录源行冲突锁，须在源行锁前参与catalog shared TRY。普通目录、tariff、品牌/账户、插件lifecycle、runtime登记/撤销，以及调账/恢复ACTIVE入口均在新外层事务起始取得exclusive，不能在command/version/channel/account/package锁后等待。I/U/D/TRUNCATE覆盖12个公开目录源表和私有approvals；兜底先检查既有真实EX，不通过TRY升级伪造入口条件。

风险结算会冻结账户，因此必须一起落地外层事务慢路结果：普通组发现需要目录语义写时整组回滚，宿主等待当前批外层事务结束后，在新事务入口取得exclusive再处理。不能让trigger拒绝所有无EX写后继续用原fast重试，那会使真实risk/dispute无限失败。批内此前成功组已持shared也是不能原地升级的理由。临时让普通root资金事务参与全局catalog shared的退出条件是cells普通路径已移除account热锁；之后再缩小参与面，不保留无用双轨。

首片验收包括持续准入/BEGIN/worker下route、账户停用及runtime撤销的推进；风险后新事务重试且不重复收费/ACK；同批先成功后风险；plugin/provider/充值/审批反序快速失败；shared-only直接写拒绝；取消和等锁信任到期整体回滚；TRUNCATE无死锁；普通reserve/spent不增加epoch。连接池、栅栏和执行共用真实绝对期限，八秒或四秒statement timeout均不能单独替代该接口预算。

S3c60档审计显示每请求execution batch约5.16次、persist约3.82次、preflight3次、canonical object约118次、digest约35.5次、旧reserve账本SUM约1.76次。顶层与nested时间有包含关系，不能相加当CPU收益；catalog第一片首先是cells的正确性出口，不凭这些调用数承诺直接达到3000RPS。

真实目录管理链路也已复现推进失败：独立完整financial夹具、16个持续warm-candidate事务、真实PATCH /routes/:id。写者约8003.6ms后503，其间3404个新读事务进入、0个BUSY；该轮不是容量测量，不能与稳定档混算。原始失败结果为catalog-writer-progress-probe-result.json，后续有限路由片结果见下。

统一期限的本机能力也已独立核实：Bun1.4.4的reserve({signal})在约51.9ms按原reason取消池等待；PG17 transaction_timeout=60ms在约65.4ms以25P04终止事务，插入回滚且池可继续使用。预留连接close约34.1ms返回时仍能观察到其事务锁，约49.1ms才证实释放，不能把本地close完成等同于服务端已回滚。第一次立即检查锁的探针因此失败，随后才增加实际清理时长观测；均未改产品或共享PG持久配置。实现须在池预留、栅栏和执行共用原期限，仍保持取消/未知提交不ACK；原始结果为transaction-deadline-probe-result.json。

### 11.1 更小的首个业务垂直切片

可先只覆盖POST /models/:model/routes、PATCH /routes/:id及storage.createRoute，在新外层事务源写之前取得catalog EX，core_candidates在暖缓存返回前shared TRY。保留全部原epoch、源行、身份及资金保护，不添加要求所有writer有EX的guard，也不移除任何权限。此片只解决被覆盖路由入口相对于候选读流的推进，其他目录/账户/risk writer仍有旧限制；不声称整个catalog已公平。独立只读审计未发现该有限门控新增等待环，完整catalog片仍需前述全部参与者及跨事务慢路。

HTTP原run事务须整体由路由runner替换，授权、EX、写入、审计和复核仍同事务；recordRoute(tx)/recordRouteUpdate(tx)不得在任意既有事务内阻塞升级。现authenticate绑定原池，不能先占满reserved连接再调用它；认证应在预留前有界完成，或显式使用同一reserved读取。池预留signal在取得连接后不再负责取消，活动连接必须有独立所有者和截止收尾。

独立私有原型已在同样16路持续warm读下约41.1ms完成路由更新，0个新reader进入、182次BUSY；它复用了真实权限核验及存储函数，但没有接入产品HTTP链路，不是产品修复或容量证明。第一次原型后置冷读因现有scope填充BUSY失败，已保留，随后在结束读流后核对最终目录成功。产品验收还须覆盖真实HTTP POST/PATCH、审计一次、失败无孤立cost_version、共同期限与取消、权限等待中过期、无关模型BUSY及写后恢复。全局EX排队会让所有模型的新候选BUSY，现context重试预算1秒，不能声称可用性完全不变或把连续writer饥饿转移当作已解决。

### 11.2 路由片的具体接口与迁移

追加函数迁移2026111706，保留既有checksum及全部金额SQL。core_candidates_v1在epoch读之前尝试事务shared advisory；public.core_catalog_write_v1只派生固定stg.catalog.control.v1键并取得事务exclusive。现有有route INSERT/UPDATE权限的角色获得该新函数权限，包括仅列级写权限及继承权限；新管理角色由infra角色脚本同步授权，不授予运行角色新的数据写权限。独立审查发现首版仅has_table_privilege漏掉列级writer，真实追加迁移回归先失败后修正为has_any_column_privilege。原invalidation触发器不变，此片不加要求所有旧writer已有EX的guard。

把已验证的owner SQL生命周期提升到storage的transaction模块，不保留脚本旧路径或别名。它以本进程单调绝对deadline统一池预留、活动连接、事务及清理；单次操作上限60秒，连接abort由唯一所有者强关，PG17事务期限只取剩余值。事务callback可返回结果，COMMIT无回执为DatabaseCommitUnknown，已知成功不补抛迟到取消。owner批准/撤销调用同步迁移并将通用期限/UNKNOWN映射回原RUNTIME_APPROVAL错误，既有CLI契约不变。

路由请求完成现有有界body解析后采用4秒操作期限，覆盖池、同连接认证、EX排队、写入、审计及最终授权复核。认证store显式绑定该reserved连接，不再占着连接回原池等认证；有效权限在排队前、拿锁后及返回前重核。storage.createRoute也在新的外层事务先EX再写，recordRoute/recordRouteUpdate不自行升级已有事务。HTTP未知提交用management_outcome_unknown（503），表示需先核对资源再重试，不自动重发非幂等POST。

首个产品回归已经取得0通过2失败：读事务用交接保证永远没有共享锁空档，真实POST/PATCH均约8秒后503。该夹具比单纯依赖35ms睡眠避免了调度偶然空档；验收保持真实创建、状态、审计一次、cost_version增量和候选结果的断言，不以放宽断言通过。私有HTTP原型另已完成POST约42.2ms、PATCH约2.0ms；第一次重复route被现有唯一约束正确拒绝，修正为合法不同operations后才得到绿色，未改约束。

有限产品片已通过同两条回归；同16路探针真实PATCH200约40.696ms、0个新reader、219次BUSY。定向生命周期20例88断言通过，另真实HTTP断连1例4断言证明源行仍锁住时请求已取消且无迟到写入；单纯Hono Request.signal测试不承担这个网络结论。完整prefix1705后创建列级旧角色、应用1706的回归已红绿验证自动授权。历史fixture明确在旧版本事务内调用recordRoute，不让当前createRoute绕过新fence以适配旧schema。全部原金额、身份与目录保护仍在，完整catalog及cells未实施。
