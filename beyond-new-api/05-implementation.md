# 实施记录：高性能与生产可靠性

2026-11-17。状态：实施中，未达标。用户在P0之后明确要求将项目改造为稳定RPS严格超过3000、请求与并发性能超过new-api、高可用、生产可用且易于扩展的Gateway。本次已从P0测量进入实施；不提交或推送，不停止或删除既有受保护资源。P0报告和原始审计保持为历史证据。

## 目标与决定

- 本机短JSON完整链路稳定RPS必须严格超过3000且高于同日参考；同配置连续两轮60秒、完整计划请求、逐ID交集、每分钟P99≤1500ms、120秒排空，60分钟持续验收是后续最终目标，本轮仍遵守用户不跑60分钟长测的限制，另行授权前不启动。并发性能以相同并发度下的吞吐、成功率、P99、资源与可持续边界验证，不从max_inflight或RPS推算。
- 保留三进程、Gateway不直连PG、客户预扣与首次attempt原子提交、幂等、UNKNOWN资金责任、供应预付硬闸、吊销线性化、成功前WAITAOF及worker提交后ACK。CPU≤2ms仍为约束，未证明前不记作通过。
- 根据用户的整体实施授权，代理采用D1/D2/D4推荐方向作为实施选择；D3仅允许平台随发布签名且字节匹配的内置适配器进程内运行，第三方仍隔离，并先证明差分与并发状态隔离。这里不将整体授权写成用户逐条投票。D5默认/关闭信任/实际非零预扣三配置分列，请求与并发均需超越；D6不降低目标。
- P0已证明原begin“KEY SHARE读状态即可”的推演不足。迁移前必须补齐冻结、授信、供应敞口、hold与最后发送条件的线性化保护，不能直接删除余额锁。context迁移覆盖命令重放、配额、恢复、资源、batch和外键，清单复用P0报告。
- 高可用与吞吐分别验收：本地故障注入记录RPO、RTO、期间成功率及恢复容量；生产拓扑须具备PG同步复制与fenced切主、证据可晋升集合与成员变更约束、准入状态丢失关闭/恢复、备份恢复、滚动升级与可观测性。只有本机时不能声称跨故障域99.99%已经成立。真实部署、凭证和付费调用仍需对应授权。

## 单元与顺序

P0测量显示Gateway约64%的样本成本来自完整模块哈希和包复制。先完成不改变隔离或资金语义的S1，取得一个低风险真实垂直单元，再推进资金/目录与执行器改造；这不会将S1的局部改善视为3000RPS目标完成。

| 单元                  | 当前行为与证据                                                                  | 目标/动作                                                                                                                                | 消费者与验证                                                                                                                         | 状态               |
| --------------------- | ------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ | ------------------ |
| S1 模块所有权         | supervisor.load每钩子对约2.4MiB Wasm哈希；attempt创建复制完整包，P0样本约59%/5% | SDK私有拥有字节，发行不可伪造的不可变模块句柄；验证时计算身份，热路径只读身份与小元数据；cold miss才编码。公开可变视图仍不能修改执行内容 | SDK包验证、host supervisor/attempt、Gateway能力检查、fixtures；篡改/伪造、驱逐、取消、重启、预算、金丝雀、全部现有协议与资源清理测试 | 已实现，阶段验证中 |
| S2 身份/目录/上下文   | 每项重复身份重核、SQL canonical、写context/preflight                            | 去重权威校验、修订栅栏、签名上下文及完整调用方迁移；先修正begin保护设计                                                                  | 现有core-execution、identity、routing-health、quota、recovery、resources、upgrade及账务集成，逐阶段容量出口                          | 待实施             |
| S3 资金批与热点       | 逐项子事务，单供应余额行在准入/结算竞争                                         | 集合式命令与结算、额度单元、完整账本不变量和升级；拒绝组不留半写                                                                         | 手工资金算例、并发耗尽/超支/冻结/重放/提交回执丢失，1/16供应账户容量                                                                 | 待实施             |
| S4 收尾/执行器        | 5次RPC、逐帧Wasm初始化与IPC                                                     | 终态结算关闭租约、健康聚合、可信内置执行器及多帧ABI；第三方继续沙箱                                                                      | 释放恰好一次、差分fixtures、并发金丝雀、SSE取消/背压/预算及协议矩阵                                                                  | 待实施             |
| S5 请求/并发/耐久优化 | P0仅35RPS稳定，无并发极限证据                                                   | 基于新采样优化批次/IO/PG耐久参数，固定到达与同并发阶梯，长测与参考对照                                                                   | 不调低成功、耐久、延迟、资金门槛；失败轮保留、磁盘预算与资源清理                                                                     | 待实施             |
| S6 生产可靠性         | 本地7项耐久已过，跨故障域/生产SLO未验证                                         | 原生故障装置、生产部署/运维配置、故障恢复与升级备份验证，约定失败域                                                                      | PG/Redis/节点分区/进程崩溃/滚动升级/扩容故障，RPO/RTO/恢复吞吐/ACK不变量                                                             | 待实施             |

## S1 所有权与接口

模块字节以SDK私有WeakMap存储，外部只能持有冻结的句柄与不可变身份/长度。任意Uint8Array输入先取得独立副本后计算哈希；验证包的私有快照可将自有字节交给内部注册，不再复制大字节。模块原始字节不从句柄暴露，编码结果为不可变字符串且只在helper缓存未命中时生成，不永久保留额外base64缓存。

包运行描述只包含签名来源的小元数据与模块句柄，manifest/config递归冻结。公开VerifiedPackage保留其现有可变副本，但host与Gateway的执行判断读取私有认证描述。完整快照只供fixture等需要文件内容的冷路径。

supervisor每个hook仍执行load，以维持connection内LRU、驱逐、崩溃后的重新加载及原waiter取消语义。不能在attempt缓存一次成功load并永久跳过后续load。模块与状态预算、attempt释放后的moduleBytes=0、无跨请求状态、helper断开后旧attempt失败等规则不变。

## 测试与迁移矩阵

| 旧证据/测试                                       | 动作                                                | 独立目标与证据                                                                 |
| ------------------------------------------------- | --------------------------------------------------- | ------------------------------------------------------------------------------ |
| sdk package签名/路径/重复JSON/配置测试            | 保留                                                | 所有原拒绝与签名规则继续成立                                                   |
| attempts公开pkg可变性测试                         | 保留并扩展                                          | 模块、manifest、config及句柄伪造不能替换执行内容                               |
| supervisor原始Uint8Array输入                      | 内部接口迁移为owned module工厂+句柄；保留原行为断言 | 原字节改写后句柄仍执行旧内容，新句柄体现新内容；不按可变引用缓存哈希           |
| supervisor LRU/崩溃/取消/startup/预算/READY/close | 只迁移输入构造                                      | 观察到的错误、取消、重启、进程与计时器清理不变                                 |
| fixture runner与84案例/112步骤                    | 保留                                                | 所有字节输出与usage结果不变                                                    |
| P0 CPU profile                                    | 新产物复测                                          | 模块哈希/包复制的热路径消失，不能仅断言“用了缓存”；记录CPU/RSS与最终全链路成绩 |

每个单元先相关检查，阶段完整门禁依AGENTS；资金/契约/资源变化进行只读独立审查。迁移新增SQL资产以新checksum owner追加，普通schema变化同步Drizzle快照；不修改已发布迁移、不保留无必要兼容路径。数据回退采用迁移前备份恢复，先验证再操作；现有保护库/进程不用于破坏性演练。状态和实测证据随实施更新，不以文档代替完成。

## S2 审计补充（只读独立审计）

最终执行函数为core-batches.sql:309起，批包装仍逐项子事务。进入S2前修正原03的以下缺口：

- 同批advisory锁可重入，必须显式处理重复命令、意图、proposed UUID、同请求ordinal；“同批拿锁”不是批内幂等。
- 批量NOWAIT遇一行锁会整体异常，需SKIP LOCKED收集成功锁并按依赖项分类，或保留按组异常边界。不能删除savepoint后假定失败自动局部化。
- 每钱包的资金lot/授信、每供应账户累计reserve要按确定顺序推进临时工作状态，拒绝项不能消耗预算。现allocator的早插request与递归异常边界不能照搬进无半写集合规划。
- 当前授权包含organization→principal→project→key，且每项IP/CIDR、角色、operation/model、有效期与scopes仍需核对。Key revision单独不能替代全部授权。去重的是权威状态读取与持锁，每个envelope、路径、revision、deadline、owner仍分别验证。
- context令牌还需覆盖IP、scopes、resource remote refs、batch ceilings/items；permit现snapshot并非完整恢复快照。先定义有界令牌与完整恢复事实，再迁移持久上下文消费链。
- 目录epoch必须覆盖候选集合增删、tariff优先级与删除语义；仅对选中route revision或MAX(revision)周期比较不足。
- begin保留冻结/授信、balance>=reserved、HELD、估算与quota证明及最后发送保护，替代栅栏完成交错验证前不删写锁。实际Batcher等待为3ms，原03的“当前1ms”不正确。

安全垂直切分为：S2a身份读取缓存与批内权威锁去重（原资金路径不变）；S2b准入集合写；S2c begin集合处理但先保留资金锁；S2d候选集合epoch与签名context完整迁移。每片先验证幂等、授权/吊销、拒绝/BUSY隔离与恢复，再以PG语句和CPU统计判断收益。

## S1 阶段证据（未代表最终达标）

模块所有权与Gateway的签名元数据消费已落地；独立只读审查未发现有效缺陷。相关真实helper用例30/0、全部单测372/0（57文件、5258断言），check/lint/fmt/build退出码均0。全量集成与产物验收正在执行，尚未记为通过。

2026-11-17同机第一轮：offered 41、target 40、1000钱包、单供应账户、worker并发8、发生器并发256、AOF always+副本1、PG三项耐久开启，容量契约通过。HTTP P99 848.95ms，窗口完整交集2445（40.75RPS），逐ID总交集2460，窗口后排空369ms；全部2460计划请求发出且成功。Gateway 2.6504ms/请求，相比P0稳定轮加权6.797ms下降61.0%；不同负载单轮比较不是稳定容量证明。PG24.0778、Core3.6488、worker1.3312、helper9.7915、Redis三进程合计1.5116ms，合计43.0113ms；上游与PG共享后台成本另列，不计该合计。PG顶层81.376调用、33.286执行ms、64333.5 WAL字节/请求。原始审计、逐ID证据与命令已归档至[阶段证据](../docs/performance/evidence-implementation-2026-11-17/s1-40-a/round.json)。

这证明P0模块哈希/复制热点判断有实测支持；也证明S1不能单独解决资金SQL与helper成本。不能将单轮40RPS、局部61% CPU改善或现有未故障验证的配置写成3000RPS/生产HA达标。

## S2a1 身份读取缓存边界

下一单元只缓存中间件用于签名验证的peer/session读取，不缓存资金授权。正常RPC复用有界读取缓存与并发同key加载；startup challenge/registration仍直接读数据库。缓存TTL先取1秒；所有身份写入同事务NOTIFY，监听确认/重连时清空缓存，通知丢失仍受TTL约束。NOTIFY和TTL仅减少陈旧拒绝；所有执行/查询/资源/插件包处理仍必须经过数据库权威校验，吊销线性化继续由原共享锁保证。它不替代目录修订或资金栅栏。

缓存上限4096条、并行冷加载上限64；未知key和失败不缓存，不接纳超限排队；没有每条计时器。清空/关闭后在途结果不得重新填充旧代缓存；请求取消不能取消其他请求共享的读取。测试需覆盖并发合并、TTL、LRU边界、加载失败、失效中的在途结果、关闭与冷加载限额，并用真实PG/HTTP证明三层吊销在缓存命中时仍即时拒绝。

## S6 只读审计发现（尚未修复或故障验收）

生产条件必须补齐的事实：

1. 原生装置是单PG；没有同步standby、仲裁与旧主fencing，`SELECT 1`不能证明同步副本可用或主机丢失后RPO=0。需新建独立primary/standby验证，不能操作既有服务。
2. 证据Redis耐久验证当前是同时停主副本后离线晋升；在线分区、旧主仍服务、副本候选替换和水位约束未测，数量确认不等于候选身份与成员变更证明。
3. 准入Redis记录丢失后ACQUIRE按空集合重新分配，配置generation只是键前缀；旧在途仍可能执行，新请求重新获得全额配额。需持久代际和恢复屏障，先在独立Redis复现，不得对共享实例FLUSH。
4. Gateway正常业务不调用producer.ready；队列现存连接仅在首次ready检查noeviction/成员配置，发布仍做WAITAOF但不能保证及时发现配置漂移/积压。需没有健康轮询时的真实业务关闭回归和有界合并检查，保留落盘确认。
5. Core-client只有单origin和单leaf pin，部署样例单Core，不足以证明滚动换证可用。Gateway合法shutdown_grace_ms最高120秒，systemd TimeoutStopSec仅60秒，可能提前强杀，需统一并验收退出预算。
6. 备份装置限停准入、排空后的PG逻辑恢复；未消费AOF与PG协调恢复、WAL/PITR、密钥恢复、响应已成功但尚未入PG的补结算尚无验收。需独立集群故障恢复装置，不能把现有quiesced逻辑备份称为连续恢复能力。

以上来自当前实现与配置的只读审计，不是实跑故障结论。可复用native fixture资源所有权、tcpProxy、commit-ack-proxy、worker/Gateway崩溃与排空回归。生产跨故障域fencing和Linux systemd实机验收仍需要对应环境证据；本机测试不能冒称满足这些条件。

## S1 门禁失败与退出窗口核对

完整集成首轮443通过、5失败、1个用例间错误（448用例/65文件，退出1）。首个历史游标用例生成1001次顺序准入并超过原30秒；运行器杀共享夹具进程后3个用例出现传输失败。另备份等候队列排空15秒失败。原断言不改，完整billing+backup文件独立复跑52通过/0失败，历史用例29.649秒，备份4.163秒，退出0；这不能替代全量绿色。产物5脚本/41项、native、插件84案例/112步骤均退出0。

退出窗口另做确定性故障核对：当前UsageConsumer只等待process promise，然后worker.close(true)并关闭Redis连接；process提交后，BullMQ更新ACK的网络操作仍可能未完成。将用独立连接代理阻塞提交后该consumer的Redis写入（包含ACK），核对close是否提前返回，并在证明后修复。尚不把静态推演称为备份失败的已证实根因。

该ACK窗口已由真实PG/Redis/代理回归证实：资金已生成1条charge、Redis active=1、ACK写入被阻塞时，旧close在50ms观察窗口内已返回，违反等待ACK的断言（0通过/1失败、退出1）。修复采用BullMQ公开pause()等待当前任务及ACK完成，最多保留原10秒退出预算；之后才force close网络。重复close调用共享同一退出过程，避免第二次pause看到已暂停而提前强关。这个回归不把“已提交但未ACK”改为客户端成功或删除事实；超预算后仍由原stalled重投和PG幂等恢复。它证明了退出缺陷，不单独证明那轮备份超时的全部根因。

独立审查发现第一版pause()在10秒超时后仍可迟到执行reconnectBlocking，超时故障回归也出现关闭后的Redis Connection closed错误（1通过/1失败，退出1）；不能把早释放ACK的绿色用例当作超时资源清理证明。当前修正为公开pause(true)关闭拉取、公开backend.disconnectBlocking中断空闲阻塞等待，再观察公开isRunning直到含ACK的主循环结束；整个过程共享从close入口起的10秒预算。超时/异常后强关，并使迟到drain停止继续轮询，不创建会重新连接的pause continuation。测试追加超时后连接归零/无新连接和重投恰好一次，所有测试清理独立执行以保留失败证据且释放资源。

生命周期补测发现初始化黑洞时disconnectBlocking会因连接初始化失败而拒绝，首轮4例为3通过/1失败。退出应继续强制清理并通过onError记录依赖错误，不让初始化失败中断收尾；它不代表服务ready成功。另“处理尚未提交时超预算退出，随后完成并重投”已有真实资金回归，验证成功事实不丢且只收费一次。全门禁仍待后续完整复跑，不能用这些窄用例替代。

S1.1最新版生命周期回归4通过/0失败，31断言，37.15秒；check297文件与lint退出0。独立Redis和目录清理均为true。原红、首版修复暴露的新红及最终绿均保留，尚不替代下一次全量门禁。

S2a1首轮完整相关文件84通过/2失败：追加迁移后两条历史升级用例仍断言25次，实际26次；同步该数量并保留所有历史数据断言。全单元378通过/0失败（58文件、5292断言）。只读审查另发现取消方仍等待共享冷读取，可能持有stream/raw预算到SQL完成；4秒statement timeout也不覆盖64个读取在4连接池中的排队。修复前追加取消与owner关闭回归：每请求等待可独立abort而不取消其他等待者；owner读取携带独立AbortSignal，4秒总加载预算涵盖排队，close取消owner和等待者，已abort未收敛的读取仍占64个上限以防重新排队放大。数据库关闭显式最多等5秒。不得通过取消一个请求来中断别的共享读取。

取消修复已通过9个缓存单测（37断言）和4个真实PG/HTTP用例（28断言）；旧版新增两个取消单测6通过/2失败，分别证实取消者仍等待、关闭未abort owner。真实DDL锁下两个stream共享读取，取消一方后500ms内只剩幸存方的task/stream，解锁后幸存方得到401并且预算全归零。check300文件、lint退出0，只读复审未发现新增有效问题。

## S2a2 批内身份权威去重

当前batch逐项子事务反复调用core_caller。新追加迁移2026111702只重组身份检查：每项先验证完整投影/path/role/deadline，再按service、instance、certificate、三层revision、session、incarnation、key SHA的完整九字段元组分组，正常同会话只一组。代表项取有效deadline最大、原序最先者，短期限项不污染长期限项。

每组独立异常scope调用原core_caller_v1，三层共享锁不改；锁下严格join一次读取不可变certificate not_before/not_after与session expiry。成功scope离开后锁属于外层事务，才执行所有逐项子事务。不能在可能失败回滚的item中取得身份锁后把成功缓存到PL/pgSQL变量中。失败组只影响自身项，结果顺序和长度保留。

新增私有core_execution_item_v2保留现有envelope/body digest、advisory、客户授权、目录、资金与业务拒绝逻辑，仅在原core_caller位置改为投影/deadline和缓存时间界限的实时检查。时间界限仅由batch自己取得和传入，不接受computed/item里的覆盖；缺失或NULL拒绝。私有projection/item归原batch owner，撤销PUBLIC执行，runtime仍只能调用batch；迁移显式核对原item/batch同owner且有时间表读取权限，并给该owner所需projection验证执行权。未使用GUC、skip布尔或current_user伪证明。旧core_execution_v1无消费者后删除，不保留别名；其拒绝权限回归迁移到新私有item。

原current/peer函数不修改，额外一条锁下查询/authority组换取更小的权威代码改动。预核会改变投影与body同时无效时的拒绝优先级：投影错误先返回；正常RPC在中间件已验证两者，不改变其资金行为。外层提交错误仍是ExecutionOutcomeUnknown，身份失败不写命令，业务拒绝仍由原子事务处理。

独立只读设计审计已核对上述锁回滚、完整分组、自然过期、权限和错误边界；自有测试库的草案探针已验证语法、陈旧revision不污染合法项，以及第一项失败后吊销仍等待外层提交。该探针不是完整门禁，下一步新增正式资金/身份/权限/升级回归并追加checksum资产。

## S2a 阶段测量与全门禁结果

同一新构建、相同41 offered/40 target/1000钱包/单供应账户/60秒/副本1配置连续两轮通过。P99为661.76/786.97ms；按完成请求加权的总CPU约41.87ms，Gateway约2.66ms、PG约23.38ms，仍远未达到最终3000以上及≤2ms要求。顶层PG调用69.90/69.94次/请求，相比S1同档81.38下降约14%；主要资金SQL与helper仍待改造。原始审计字节、格式化视图、逐ID日志、构建hash和命令均在[阶段轮次](../docs/performance/evidence-implementation-2026-11-17/rounds.json)。当前只证明至少40稳定档，没有寻找最终上界。

完整单元381通过/0失败（58文件、5304断言，51.99秒），check301文件、lint、build、artifact、native、plugin artifact、durability、backup均退出0；artifact五脚本41项、插件84案例112步骤、耐久7项且资源全0。备份在自有DB/namespace停所有准入及producer并排空后实际运行bun run test:backup --quiesced，恢复1笔charge、两类证据各2条，恢复库/源夹具与已验证hash的2250377字节dump均清理；没有停既有受保护服务。

完整集成458通过/4失败/1个用例间错误（462用例、68文件、9640断言，1016.96秒，退出1）。仍是1001次顺序准入的历史游标用例超过原30秒，引发共享夹具被杀后的3项传输失败；备份排空及4个新增退出生命周期这轮通过。没有改超时或弱化断言，当前阶段不能记作全门禁通过。格式首轮因三份原始审计JSON视图失败，已另存原字节.raw、核对解析值一致后格式化视图并复查退出0。全部已保留验证日志见[validation](../docs/performance/evidence-implementation-2026-11-17/validation/s2-integration-isolated.log)。

下一步不改资金保证地去除重复目录扫描与SQL canonical，目标同时降低主瓶颈与历史游标用例成本。统计失败和实测40只构成中间证据，不能声称生产可用或全面超越参考。

## 无状态context的SQL权限缺口

只读审计确认，原03“Core验HMAC后把parsed key/project给PG”会扩大受限API数据库角色的主体伪造能力；原持久context只允许通过verifier定位Key并固定主体，角色不能自行写context。原生PG只读能力查询未提供pgcrypto，且当前工具链without SSL构建，不能假设标准DB HMAC扩展可用。不存在自造密码算法的计划。

两条可行路线尚需完整契约落地：明确增加标准pgcrypto并在PG独立核MAC；或admit/next重复客户凭证，Core只给PG派生verifier，PG按verifier重定位Key并从权威目录/资源事实重建授权。后一方案需把敏感凭证置于受raw-proof签名保护的独立RPC字段，避免将raw token送PG或改变语义body digest；不得让token明文暴露verifier。begin/abort/query/quota/recovery继续读持久主体，不要求过期token或客户重发凭证。当前没有实施这一接口变化。

去表迁移还必须保留每attempt实际IP：当前next不要求新ctx IP与首项一致，不能把历史同request不同IP任意折叠为一个值；无request拒绝命令也要保留自身小主体。command/permit immutable guard需要受锁同事务回填与失败回滚证明。先实施与上述选择无关的目录缓存与epoch栅栏，保留所有context消费者，逐步验证后再切换完整链路。

## S2b 目录集合epoch与私有候选缓存

实施次序调整为先完成可独立验证的目录单元，不等待无状态context认证选型。保留现有context和全部消费者、原candidate refs/metadata/digest算法；只把同一目录版本下的重复计算移到cold路径。对外函数签名与结果不变，原计算函数成为私有cache loader，不存在旧wire兼容路径。

新增私有schema core_catalog_cache，只有原候选函数owner有访问权，runtime无USAGE/表读写/loader执行权。包含单行epoch、候选scope缓存与单行用量计数。缓存key绑定epoch及(model,operation,currency)完整元组（哈希索引同时核对原值），payload最多128候选；总逻辑payload≤64MiB、行数≤4096。仅真实model/支持op/实际tariff币种scope可缓存，合法空结果可缓存，任意未知输入不增长缓存。超预算返回原计算结果但不入缓存，合法目录不因此被拒。用量锁只在cold填充NOWAIT尝试，其他scope不等待全局填充锁；预算争用时不缓存。

warm先取唯一epoch行FOR SHARE NOWAIT，持到外层事务结束，再读取本代scope。cold取得本scope+epoch的advisory try-lock，二次检查后才原样计算；争用沿现BUSY退避，不依赖唯一索引等待。源表原有NOWAIT保留。epoch缺失严格失败，禁止无栅栏compute或返回旧代。所有缓存写与计数同事务，语义变更同事务递增epoch、DELETE旧代缓存并清零用量，不用TRUNCATE或后台清理。

触发器覆盖models/routes/channels/bindings/plugin_versions整行（现digest确实包含name/created_at/actor/validation_result等）；tariff的id/model/op/provider/currency/price/enabled；brand的id/enabled；account的id/mode/state；cost的id/currency/rates/ceilings；price的id/currency/rates/ceilings/basis；package的digest及capabilities.operations；publication的version存在性。INSERT/DELETE均保守失效，允许尚未引用的版本/价格/包产生额外失效；UPDATE实际语义值未变不递增。普通余额reserve/spent/revision、account name/revision、brand name不失效。不得擅自把channel name当非语义而改变原digest规则。

warm不再取各源行共享锁，而以epoch锁替代；元数据写者在提交前必须取得epoch写锁，reader始终NOWAIT。账户控制/风险结算先取资金锁后碰epoch，不能把reader改为阻塞等锁造成反向等待。无关目录变动后当前选中ref仍相同的begin继续合法，停用/改价/凭证变化仍拒绝。原compute函数中两处core_candidates_v1.model限定名必须同步改为私有loader标签，其余计算字节保持不变。

本单元追加2026111703：Drizzle私有表声明/新快照和DDL，再追加函数/触发器checksum资产。验证包括迁移前后cold/warm输出相同、全部可变语义源、明确非语义源、route增删/tariff覆盖与LIMIT128、两种锁交错、同/不同scope cold、回滚/旧代清理、任意输入与预算、私有权限，以及现有资源/batch/资金/恢复与全部门禁。首要性能回归仍是原30秒历史用例，不修改其门槛。

首轮24项候选缓存集成全部通过（133断言，49.43秒）。相关资金/升级文件73通过、4失败、1错误：原1001次顺序准入仍超过30秒，其后3项为共享进程退出的传输失败，不能宣称本阶段全绿。相同强耐久配置40目标单轮通过，P99 433.77ms，PG约20.97ms/请求，顶层66.30调用、32.02执行ms、59103 WAL字节；该单轮尚非新的稳定档证明。

只读审查提出TRUNCATE关系锁反向边界，真实交错已复现：reader先持epoch，TRUNCATE持源表ACCESS EXCLUSIVE后等epoch，cold reader的SELECT即使FOR SHARE NOWAIT仍等待关系锁，最终300ms statement timeout而非BUSY（1通过/1失败，退出1）。修正为cold二次检查后、调用原compute前，对全部12张来源表显式取得ACCESS SHARE NOWAIT关系锁；不改变warm读取、源行锁和原候选算法。关系锁争用立即沿原55P03/BUSY返回，避免epoch→关系与关系→epoch的阻塞环。另保留TRUNCATE提交后缓存及用量失效回归。该缺陷与修复前后日志均保留。

## S2c 请求租约查询的增长成本

当前execution_quota_leases只有lease主键、attempt前缀唯一索引和排除channel的request部分唯一索引；preflight、begin检查、CLOSE按完整request读取/更新，无法由这个部分索引覆盖。P0/S2a语句统计中，preflight租约读取和RELEASE_PENDING更新均已进入热点。这是需实证的历史量增长问题，不能用缓存掩盖。

下一步先以独立库中3条目标租约和30000条无关已释放租约验证完整request查询的实际buffer访问预算（至多128个8KiB块，不依赖索引名或固定执行计划），保留修复前失败。若证实，追加完整request前缀索引，保持全部函数、约束与权限；已有部分唯一索引继续承担只对公共scope去重的不同约束，不以非唯一索引替换它。同步Drizzle和追加迁移，复跑原1001次顺序准入的30秒回归与资金/升级路径，再测完整链路。部署迁移会取建索引锁，采用已有停准入/排空迁移边界，不能未经授权对共享运行库直接建索引。

增长回归已实证：旧查询返回3条目标但访问3336个8KiB块，超过128块预算；追加2026111704及完整request索引后同回归通过（1例、5断言）。相关10文件85通过、4失败、1错误，原历史用例仍超过30秒，说明索引不是该超时的全部根因，不能将这项修复当作全链路达标。

## S2d 空闲批次等待

现Batcher在无在途事务时也固定等收集窗口，准入和begin各3ms，1001次顺序流程累计至少约6秒人为等待。调整为无在途时在本轮microtask合并同步到达项并立即发起；有在途时保留原delay、capacity、maxActive和加权maxQueued。每项仍等待所属事务完整输出，提交失败整批拒绝。空闲后已排队的项同样不再补等一个窗口。目标是减少不必要的低负载延迟，同时在繁忙时保留聚合；不改变容量门槛、数据库事务或资金保证。先以250ms收集窗口独立用例验证同步两项能在100ms内发起且提交前不ACK，旧行为须失败，再核对高负载批次成本。

独立只读审查未发现新调度的有效问题。单元5通过、13断言；相关10文件89通过、0失败，6903断言，139.30秒。原1001次历史回归17.155秒通过，超时和有效断言未改；完整单元383通过、0失败，58文件、5320断言，52.05秒。check304文件、lint、build退出0；fmt首轮因Drizzle新生成的0008快照和journal失败，只格式化这两份本次资产后复查退出0。

完整集成首轮486通过、4失败，490例/70文件、12335断言，1076.84秒，退出1。其一是9个合法context并发冷读触发新增scope BUSY，store.context此前没有如execute一样的有界重试；其余三项在独立夹具bootstrap插件验证失败，具体error_code另行复现，不能记成网络波动后忽略。新增确定性冷填充回归先持填充事务，合法RPC在50ms内已提前失败（0通过/1失败），证实该用户可见回归。修正仅对已明确回滚的55P03/P0503重试，复用同一context ID，上限1秒并受原请求deadline约束；权限、验证与未知提交错误不重试，不重复执行路由排序。

## S2e context预算与最新容量复测

S2e修正context取消、定时器延迟和单调时钟预算后，完整门禁退出0：单元386例、集成493例，原生33项顶层/45项含子用例，插件产物84例112步，五个产物脚本41项、耐久7项；独立排空夹具的备份及恢复也通过。三次历史插件bootstrap失败仍未定位，120次独立bootstrap未复现，不将其记为已解决。

最新S2e构建40目标/41到达的60秒容量复测失败：2460个正式请求中2065成功、4个502 upstream_invalid_response、391个503 no_route，P99 915.81ms，120秒未排空。4个未知结果仍保留客户400 atoms和供应66432 atoms预留。失败原始数据已归档为evidence-implementation-2026-11-17/s2e-40-a；上一构建连续两轮40通过不能用作最新构建的稳定性结论。

## S2f 本机插件故障与共享通道健康

定位发现relayJSON把PluginHostError丢成upstream_read_failed，并将其记作供应通道FAILURE；流路径同样丢掉本机执行器错误。因此本机故障可以污染其他Gateway共享的通道健康。真实故障注入已证明：在上游已收到请求且等待响应时，只杀本测试Gateway的helper，未知发送与资金责任正确保留，但共享健康fails错误地增为1，回归0通过/1失败。首版回归使用了不存在的hold余额字段，仅为夹具错误；改为读取权威余额表后才取得此有效失败证据。

修正目标是：PluginHostError在签名结果中保留有界规范错误码plugin_host_<code>，不暴露消息、输入、凭证或任意原始错误字符串；它沿既有NOT_SENT健康观测分支仅释放本次探针，不累计供应故障，也不宣称供应成功。该名称仅是现有健康协议枚举，不改变实际已发送事实、UNKNOWN状态或预留责任。JSON和流均覆盖。bench-system增加最多25组attempt错误状态统计供失败定位；HTTP外部错误和资金结算规则不变。此回归证明故障放大问题，不足以证明前述4个原始错误由helper导致；下一轮保留具体内部错误码继续定位。

## 下一资金单元的数学反例（尚未实施）

只读审计发现原03把allocated_atoms定义为Σquota，但普通结算只修改cell.quota-=cost、unfolded+=cost且不写主行allocated，结算一次就不再满足定义。若坚持原表示，至少须改为A=Σ(Q+U)，并定义折叠后负A/负Q、超预估冻结及恢复归一化，不能直接加A>=0检查。独立反例：初始主余额100、已分配80，单元Q=80/U=0；实际成本120后Q=-40/U=120而A仍80，原A=ΣQ已错。有效余额为-20，不能因折叠或新准入把这笔真实成本丢掉。partial和修订结算还必须按成本及预留额的差额，不能每次都减完整estimated。

正在审计更小的替代表示：主行保存未分配balance/reserved/spent，单元保存其balance/reserved/spent；对外及全部账务判定读取主行与单元之和。root到cell仅同事务转移有限预算，普通结算按差额更新单元，不要求每秒再次扣主余额。内部划拨有不可变事实，汇总经济账不重复计费；超支允许负单元且立即冻结，恢复前归一化，UNKNOWN reserve不被回收。字段、锁栅栏、全部旧消费者与历史迁移仍在核对中，此段不是已完成实现或资金正确性证明。
