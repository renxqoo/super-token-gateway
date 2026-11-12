# Core 金融准入实现单元

本记录对应 T04.b.2a、追加版本 `2026111206`。目标是准入、单次 begin、双方预留及共享 quota；RAW/CANONICAL/DISPUTED、实际供应结算和数字证据 producer 验证由 T04.b.2b 继续，Gateway 本地发送 registry、真实网络发送、三业务进程及 WAITAOF 由 T04.c 继续。金融设计、内部身份设计及协调者 quota ownership revision2 为本单元依据；此文件不是整个企业迁移的完成声明。

## 公开入口与真实边界

`startCoreInternalListener` 继续负责真实 mTLS、证书目录、Ed25519 启动 session 和逐命令 proof。执行入口复用该 listener 内的私有 WeakMap context；JSON 不能创建 trusted caller，现有 identity SQL 最终当前状态屏障仍在每次事务内执行。执行模块仅接受它得到的真实 caller 投影。Gateway 不得到 SQL、pepper、加密主密钥或 secret 查询能力。

Core client 新增 `execution(kind,data,options)`，所有有限 RPC 均为 POST，路径如下。

| kind                        | 路径                              | 行为                                                                                                                                                                             |
| --------------------------- | --------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| execution_context_v1        | /_internal/core/v1/context        | 短时客户 token 仅用于验证；持久 context 固定项目、key、IP、operation、model、fingerprint、候选与30秒内期限                                                                       |
| execution_admit_v1          | /_internal/core/v1/admissions     | command/项目 intent 仲裁、采纳 proposed UUID、客户来源预扣、供应预留、journal、RESERVED 与安全 receipt 原子提交                                                                  |
| execution_next_v1           | /_internal/core/v1/attempts       | 原 request/session、ordinal、deadline、捕获价和累计 token 预算校验；仅新 attempt/供应 hold/channel quota                                                                         |
| execution_begin_v1          | /_internal/core/v1/begin          | 当前授权、目录、密文版本、e向量、quota proof 和 RESERVED CAS；只有已确认外层 commit 的首次胜者返回 transient_send                                                                |
| execution_abort_v1          | /_internal/core/v1/abort          | 仅 RESERVED；默认只结束该 attempt。close_request=true 还须全 request 从未 grant且其他 permit 已 ABORTED，才能关闭逻辑请求并零额释放客户                                          |
| execution_query_v1          | /_internal/core/v1/commands       | 同稳定 caller 查询安全 receipt/current permit；新启动 session 不能取旧 grant                                                                                                     |
| execution_quota_v1          | /_internal/core/v1/quota          | ADOPT、RENEW、CLOSE、RELEASE；CLOSE 由当前原 owner/session 声明逻辑发送已结束                                                                                                    |
| execution_abort_recovery_v1 | /_internal/core/v1/abort-recovery | 当前Core在全序锁内核实原deadline/真实撤销，仅终止never-granted RESERVED；paired供应释放、receipt、audit和channel barrier共同提交；全never-grant ABORTED后才closure及客户零额释放 |
| execution_quota_recovery_v1 | /_internal/core/v1/quota-recovery | 当前 core-api/core-worker 仅恢复已提交 RELEASE_PENDING，不能制造 closure、转移 owner或释放资金                                                                                   |

未列业务 kind 一律拒绝。写命令显式提供command_id时client要求同时提供原deadline_at，重试不改语义、不自动缓存或重发grant；安全查询可以用新proof。客户 token 不落 PG；完整 target、headers、body 不送 Core。`DispatchProjectionV1` 沿已接纳14字段；manifest保留SDK prepared.manifest的operation/model/upstream_model/stream/input_bytes/ceilings，明确添加version、quantities、resource_ids及可选input_characters/images。ceilings是硬上界；quantities只接收可信宿主已经准确测得的数字，收窄e时须不超过该维硬上界和成本配置。quantities仅表示prepare之前或最终准备结果中可信宿主已能准确测量的量，不把插件临时估计或发送后未知量填入其中；未知维度保持缺失。input_characters来自客户原始请求的既有字符计量，images来自可信宿主准确图像数量，二者与转换后的供应manifest数量可以不同，不互相推断。resource_ids仅接受有界UUID列表。客户准确字符数独立读取input_characters，缺失即拒绝INPUT_CHARACTERS报价，不把characters上界变成准确收费量。model当前max_output_tokens在context返回且准入持锁再核；token输出操作缺少输出上界或超过model上限持久拒绝。prep绑定明确映射SDK public session的id/tenant/attempt/incarnation/plugin/config；Core独立重建tenant=project、attempt=proposed_attempt、incarnation=current owner、plugin=package digest、config=canonical配置SHA，并绑定原owner session/request/operation/binding/revision。b1捕获config_digest沿其原SQL jsonb编码合同，SDK session config SHA为独立技术摘要，不混同二者。Core不能只凭摘要证明私有SDK session仍未消费，T04.c宿主负责该本地状态。Core 持久的 authorization plan 从严格投影、原 owner/epoch 构造，epoch固定1。

context 候选来自当前真实 catalog/binding/publication/price/cost行。路由优先级、加权、轮转、最低延迟与已有网关共用 runtime `orderRouteCandidates`；准入和 begin 在 SHARE 锁下重新检查候选语义摘要。provider-specific 客户 tariff 优先于通用 tariff，报价复用 `quoteReservation`，金额复用 `calculateCost`/SQL `exact_billing_price`。prepare原始文本不作为可信计量来源。

## 原子资金与一次性响应

旧 `admit_gateway_batch` 仅为有明确退出条件的旧 caller adapter；客户现金/赠款/授信/过期来源分配已提取为唯一 `allocate_customer_batch_v1`，显式接收预生成 attempt UUID。Core role无该私有内核及旧 adapter 权限。T04.c切换、T16删除遗留入口；本单元不构造第二金融算法。

供应估量 e 对每个费率维度必须有显式 ceiling；已验证最终 manifest 的上界可收窄，超 ceiling拒绝。e、其digest、manifest digest、cost版本与 E一起捕获，不以更大配置 ceiling替换。人工向量 input60/output30、rate均1/3，E=30；初始未知供应占用也是30。每个物理 attempt有独立供应 hold。next保留最初客户 quote；新价不在捕获 map、客户上界提高、累计 token上界超原预算均为持久 REJECTED，不新建 attempt，不释放旧 UNKNOWN。

`core_execution_batch_v1` 每组子事务，批量128、有界队列2048；成功组锁保留到外层 commit，后续组只取 advisory try/NOWAIT。授权按身份→organization/principal/project/key→model/routes/tariffs/brand/channel/plugin/account；客户 wallet与供应 balance取 NO KEY UPDATE，之后 request/hold/attempt/permit。proposed UUID、命令、intent有独立 advisory namespace与真实唯一约束。旧 allocator或settler返回 ERROR、审计失败及journal延迟约束异常均使整组回滚，不能被写成成功或稳定业务拒绝。

安全 receipt 不含 fresh 标记、grant或凭证。SQL 返回的首次胜者信息只在当前 repository调用内存中传递；Bun外层 begin确认 commit后handler才短时解密该通道捕获密文并构造 transient_send。驱动提交未知、重复 POST、安全查询、新session、过期响应都不能重造材料。ciphertext版本在channel SHARE下固定；临时Buffer finally清零，字符串仅短时引用，不承诺JS物理内存零残留。

## Quota 归属与恢复

organization/principal/project/key lease属于逻辑 request，仅首次 admission固定 creator command/body digest、原bucket、owner。channel lease属于每物理attempt。表有 request/common partial UNIQUE及attempt/scope UNIQUE；common的首次attempt仅provenance，第一attempt ABORTED/UNKNOWN不改变后续 common adopt/renew。next只申请channel lease，不能借自己的REJECTED删除原common。

`execution_budget`捕获Core实际 max_attempts（1..5，默认3）、原总deadline、有限 token预算=首次验证token上界×max_attempts，之后不可修改。`execution_closed_at`为独立单向生命周期字段，后续 next/begin必拒绝。CLOSE只需当前原workload/request绑定；客户key撤销不阻止纯关流和已拥有并发清理。workload本身撤销后只能由技术Core恢复已提交屏障或执行经证明的never-granted恢复。CLOSE不动客户或供应资金；SEND_GRANTED/UNKNOWN责任继续保留。普通金融终态/FINAL/settle成功都不是transport关闭证明。T04.c宿主必须在CLOSE前同步失效未消费grant并停止/等待所拥有transport；本单元的RPC测试不冒称该宿主已交付。

Redis stable acquire只返回共享引用，无delete闭包；同lease不重扣RPM/TPM、不延长DRAFT。DRAFT30秒，live30秒、宿主10秒续租，tombstone至原deadline后30秒。adopt/renew使用稳定operation ID与revision；旧operation重放不延长。每record最多4096个有限operation回执，达到限额fail closed。所有key有限TTL，generation/shard/key由部署配置派生。

只有已确认REJECTED的匹配creator且未adopted，或PG已提交RELEASE_PENDING，才可请求清理。BUSY、rollback、commit未知、NOT_FOUND、迟到finally无清理权。PG ADOPTION_PENDING→Redis adopt→PG ACTIVE；Redis网络不发生在金融锁内。proof使用monotonic clock，减完整RPC耗时和1秒余量，在SQL锁内减去事务耗时并在回复前再次检查。release barrier不回ACTIVE，迟到renew不能复活。

RPM不退款。runtime提供原bucket幂等target delta端口，但本单元不开放任填target的对账RPC；T04.b.2b从唯一证据链推导全attempt实际累计TPM，UNKNOWN保原估量。已关闭并发的tombstone仍可按原bucket作对账，原bucket缺失不触碰新bucket。

## 最小平台管理

`createCoreManagementApplication`是独立可挂载的Core HTTP factory，沿现 `/admin/providers`集合，不建channels别名。真实创建provider（新账号零资金或引用明确account/brand）、model、project/key、customer credit/tariff、显式cost ceilings route及PATCH、voucher/RECHARGE/receipt/balances。插件安装、真实helper验证、发布与binding复用已接纳b1 factory，不以SQL改PUBLISHED或余额作为产品前置。provider只接受HTTPS及有界none/bearer/api-key范围，其他云签名能力留后续。

新增平台权限是 `catalog:manage`、`customer:fund`、`funding:read`、`funding:recharge`。私有PlatformActor WeakMap派生的credential ID/admin ID/verifier送最终SQL guard；credential/admin当前enabled、expiry、逐动作permission与绑定在同资金外层事务SHARE锁下检查直到receipt/journal/audit commit。actor字符串仅审计归属。项目ADMIN不获平台权限，NULL权限不放行，充值不清冻结。付款币种与入账币种分别记录，不执行支付或换汇。

## 迁移与验证

只追加2026111206及新Drizzle snapshot/journal entry；已接纳SQL、旧snapshot和manifest前缀不变。历史重复project intent由新UNIQUE拒绝；活跃旧供应hold缺少可证明e向量时拒绝升级，不伪造历史事实。旧已结清行保留可空e。证据旧source唯一键与RAW/CANONICAL/DISPUTED DDL在本单元不改。

实际门禁、回归计数、错误到通过记录及资源前后manifest随本单元冻结证据交付。独立审查和主树真正单次verify由协调者执行。Docker、真实模型、Go/Cargo/guestABI变更、容量/HA、b2b/c后继验收不在本单元实测声明内。

冻结前最终分阶段检查均实际执行，未使用整仓verify摘要代替这些进程：

| 检查                     | 实际结果                                           |
| ------------------------ | -------------------------------------------------- |
| check / lint / fmt:check | 217个文件无类型错误；0错误/警告；317个文件已格式化 |
| test                     | 281通过、4025断言、40个文件                        |
| test:integration         | 438通过、10137断言、25个文件；真实PG/Redis/HTTP    |
| build                    | 成功，沿项目脚本使用已供给的Go编译缓存             |
| test:plugin-artifact     | 15场景、24步骤、pending=0                          |
| test:artifact            | Gateway9、catalog7、identity7、financial9项通过    |

最后一轮实际数值回归包含：model输出上限10拒绝最终上界30；客户原始3字符、单价2，客户预扣6；供应准确input31和output上界30、各单价1/3，供应E21；缺准确客户字符数拒绝；准确量61大于硬上界60拒绝；next仍保留首次客户字符数3与预扣6。技术恢复与begin两锁序、恢复audit失败的双方资金/closure/receipt全回滚、实际PG COMMIT回复丢失均已覆盖。

开发期失败原始日志全部保留。已确认并修复的原因包括管理事务内再次begin、SQL与SDK配置键UTF16规范化不一致、缺model输出cap以及字符上界误作准确量。quota回收旧revision谓词和字符误用旧逻辑另外以负控制证明确实失败，负控制结束后恢复原字节。首轮全PG为436通过/2失败，原因是既有升级测试尚未列入新版本/nullable默认字段；补齐新版本与默认断言，保留全部历史行和不可变校验后，全量重跑为上述438通过。某重复CLOSE测试的Promise matcher与普通await观察曾不同；最终保留普通await后的精确Error/code断言，不宣称已定位Bun库根因，不修改已接纳transport或延长期限。

最终资源回到原4个数据库、18个role、两Redis的0/97个原key/type集合，TLS和自有临时目录无遗留。自建core_identity_owner只在所有自有DB关闭且共享依赖为0后删除。原始资源快照、所有资源登记与回收、51路径before/after字节SHA、实际apply验证过的补丁、源码tuple及全部日志exitcode随冻结证据包交付。独立审查与协调者主树单次verify尚未在本单元执行；Go测试/race、Cargo/WASM重建、Docker容器、独立durability/backup、真实模型及b2b/c后继验收均未在本单元运行。

## 独立审查修正（fix1）

同步 quota 前先读取 PG 当前原 owner/session/incarnation、permit epoch、permit/closure 与全部相关 lease 屏障。客户端 epoch 只作比较；Redis adopt/renew/release 的 attempt/epoch 来自各 lease 的 PG permit，不能从请求复制，也不能因首 attempt ABORTED 而撤销后续 common lease 权利。Redis 操作仍在资金事务外，最终 PG 屏障保留；非法输入在首个 Redis 写入前拒绝。

持久业务拒绝使用单一内部错误 DTO `{code,category,retryability,command_state,request_id?,attempt_id?,message}`，其中 command_state=REJECTED、retryability=NONE。客户不足402、供应不足503、意图/请求/重试冲突409、有限输入/计量拒绝422；安全 query 仍以200返回持久 receipt。CoreRpcError 保留该 DTO 和实际 HTTP status。重放同命令恢复相同拒绝，配置或余额变化后须新命令。错误响应不再同时嵌套另一份 receipt。

begin 的外层事务结果未知固定 STATUS_ONLY；可确认的事务前 BUSY 仍可 SAME_COMMAND。客户端在 begin 的断连/超时等无确定结果时也只报告 UNKNOWN/STATUS_ONLY，不自动重发。安全 query/replay 永不返回 transient_send。next 在原 request 锁下比较原 fingerprint、operation/model、key/project、owner incarnation 及首次 manifest 的 resource_ids，拒绝不改变原 quote、客户 hold、先前 UNKNOWN 或 common quota。

b2a 不添加 remaining_reserved_atoms 缓存列。未发送的 HELD 预留由 immutable estimated_atoms 导出，never-granted abort/recovery 在同事务释放该 E 并转 RELEASED；UNKNOWN 继续保留 E。e 向量重建和成对 journal 校验保留；b2b 才引入基于证据 head 的派生 R。

## begin 响应阶段修正（fix2）

已认证 begin 的 handler 返回已提交 receipt 后，listener 将该命令标记为已进入提交后响应阶段；随后响应构造、JSON 序列化、有限 body 预算或同步写回异常与提交结果未知使用相同的 UNKNOWN/STATUS_ONLY 错误。该标记只来自当前私有 caller context 下实际 handler 返回的 COMMITTED receipt，不从请求 header 或 body 推断。事务前失败、已确认 SQL 回滚和持久 REJECTED 不进入此阶段，保留 SAME_COMMAND 或 REJECTED/NONE。

合法 responseBytes=4096 与管理 PATCH 接纳的4096字节凭证用于真实 mTLS/h2、PG 和官方 client 回归。首次 begin 提交 SEND_GRANTED 后，过大首次响应只能返回有限无材料错误；安全 query 和原命令重放都无 transient_send，新命令收到 permit_not_reserved/REJECTED/NONE。客户100和供应30预留继续保留，只有一次供应 RESERVE，RELEASE 为0，不扩大响应或凭证预算，也不补发 grant。

该回归先以独立原件取得0通过/1失败/14断言，唯一失败是 STATUS_ONLY；公开回归另保留修复前失败记录。fix2 的分项门禁、资源对账、源码冻结和可应用补丁由本轮独立交付记录承载，后续全新只读复查与 main 单次 verify 仍由协调者执行，不据此宣称 v6 或整个迁移接纳。
