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

直接state/mode变更、预算和headroom控制也必须经过独占栅栏。触发器使用TRY验证实际锁管理器状态，失败整句回滚；不能在持资金行后阻塞升级。新增列和内部事实保持最小权限，runtime不能绕过受控函数直接改cell或hold归属。

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

只在既有quiesced升级边界执行。按真实账本净额核对每个hold的reserve及expense，所有可映射旧hold含PARTIAL/SETTLED/RELEASED均赋不可变cell；无法映射的旧成本留root，不猜测归属。

有覆盖的旧账户：cell.R为归属hold净reserve，cell.S为净expense，cell.B=cell.R；root.B=旧B-旧R、root.S=旧S-sum(cell.S)。cells.rev=0、root.rev保留旧revision。内部迁移分别记录预算R和历史spent C归属，不能把R+C从root.balance再扣一次。

非ACTIVE且underbacked的旧数据可用root.B=旧B、cells.B=0、cells.R=原净R的冻结表示，避免旧B=-MAX且R>0时旧B-R本身超出存储精度。旧状态、UNKNOWN及经济分录保持；恢复前再独占归一化。账本不一致、缺失映射或ACTIVE覆盖不足明确拒绝迁移，全事务回滚。

## 8. 完整迁移面与验收

必须同时迁移core_execution_item_v2、最终core_evidence_group_v1的外层预锁、core_supplier_settle_v1、abort/recovery、资金充值/调账/对账与setState、所有root读者、Drizzle/FK/guards/roles、bench/backup/durability/artifact和直接播种/断言。只改内部settle不会消除外层account/main balance串行。

保留原经济journal语义。validate_upstream_journal当前只验证单笔至少两行且和为零，并非counter总账一致性证明；新增汇总审计核对B/R/S、hold净reserve、expense及内部划拨守恒。

优先验收：不同cell并行且不触主余额写锁；持续读流下风险/目录writer取得进展；旧批shared重入有界结束；风险必须跨外层事务重试；plugin/FK反序快速失败；两级等待共用期限并清理锁/连接；冻结两种先后交错；碎片不假402；headroom全局上界与局部重分配；充值/调账回执同一受锁状态；跨币种恢复不释放UNKNOWN；历史partial后续只记增量；划拨/审计失败全回滚；升级前后汇总完全一致。全部仓库门禁与同构建容量连续两轮仍为阶段出口，不能由16个cell推算3000 RPS已成立。
