# PostgreSQL 模型与事务

当前契约为 schema2026111008，定义由 `packages/storage/src/schema.sql` 与 `lifecycle.sql` 组成。运行进程只校验当前版本，不执行 DDL。初始化在事务与 advisory lock 下完成；非空未知库拒绝修改。

请求恢复只读取 `attempts_stale`；移除没有调用方的 `requests_stale`，让请求终态更新不再因该部分索引而强制重写其他索引。usage 的 `(attempt_id,source)` 唯一索引同时支持按 attempt 查询，移除重复的单列 `usage_attempt`。请求表的新页面按 70% 填充保留更新空间；索引变化不改变唯一性、外键、历史查询和恢复契约，离线升级统一应用。

本轮性能优化保持资金与证据约束：钱包变更使用 `FOR NO KEY UPDATE`，仍串行化同钱包写入，同时允许不改变钱包的外键读取；资金日志批量写入直接复用新插入身份返回的时间，不重新连接整个身份表。schema07 可在停止所有客户端并备份后，以 `db:migrate -- --upgrade --quiesced` 原子刷新函数、删除冗余索引并调整请求表填充率至 schema08，不重建表或改写余额；schema06 先完成既有分区转换，再刷新全部函数。容量结果以当前产物的开放到达压测及精确账务核对为准。

批量预扣和结算在事务内以 PostgreSQL 数组累积待写入行，落表时统一转换为 JSON 记录集，避免逐项拼接不断增长的 JSONB 数组。预扣的逐项返回结果也先使用数组按输入位置累积，完成时一次构造相同的索引对象。每笔请求的局部计算仍在全部校验后才合入批状态，资金来源顺序、冲突证据和失败项隔离规则不变。

预扣在已取得的钱包锁保护下按批建立异常边界，正常批次不为每个请求单独建立写入子事务；失败批次在同一外层事务内有界二分并复用相同算法。失败子批的请求、资金分配及日志一起回滚，成功项只在最外层事务提交后才可见，幂等仲裁和授权检查保持不变。 无幂等键的新请求在完成各项校验后集合插入；带幂等键的请求仍逐笔通过唯一索引仲裁。每一行仍执行相同授权触发器，集合写入失败沿用整批回滚与有界二分。

首次成功的同步文本请求可统一写入数字证据：参与集合写入的请求必须互不重复且为 execution/FINAL/SUCCEEDED、尝试与请求仍运行、无既有证据或异步资源，对应钱包已锁定；忙钱包单独返回可重试错误。仍使用相同精确计价函数、数字白名单、唯一约束和外键，先写全部事实再统一结算。其他输入或集合写入失败时在同一事务内回滚该尝试并交给原逐项摄入规则；重投、冲突、未知结果和异步任务的语义保持一致。内部集合写入函数不授权给 HTTP 或 worker 登录账号。

schema2026111006 可经 `bun run db:migrate -- --upgrade --quiesced` 显式离线升级。先停止所有 Gateway/worker/其他数据库客户端，保全队列与备份，再执行 owner 工具。工具独占锁版本表、检查其他连接、在单事务内精确比较每个 payload、保留全局唯一约束及外键 OID、重写瘦身后的元数据表、校验资金快照后更新版本。长升级语句不受 runtime 的 8 秒超时限制。失败整笔回滚；更旧或未知版本不猜测转换。新旧运行进程不共存，不保留旧业务执行路径。

| 数据                               | 归属与不变量                                                                                   |
| ---------------------------------- | ---------------------------------------------------------------------------------------------- |
| projects / api_keys                | 技术项目与权限；Key 仅存 HMAC verifier 和显示前缀；即时撤销                                    |
| providers / models / routes        | 上游配置与技术能力；供应商密钥 AES-256-GCM，provider_id 作为 AAD                               |
| cost_versions                      | 费率快照不可更新/删除；变更路由只引用新版本                                                    |
| gateway_requests                   | 逻辑意图、指纹、操作、别名；project/key/operation/intent 唯一                                  |
| attempts                           | 物理调用序号、供应商、定价版本、状态；每请求 ordinal 唯一                                      |
| usage_events                       | 稳定查询视图；全局 id/seq/attempt-source 在 usage_event_index 唯一；数字 payload 按 UTC 月分区 |
| event_consumers / event_deliveries | 可回放、SKIP LOCKED 租约认领、fenced ack、至少一次投递                                         |
| resources                          | 虚拟 ID、原供应商 opaque ID、项目/模型归属、状态与轮询租约；没有正文或下载 URI                 |
| audit_events                       | 配置变更动作与哈希 ID，不保存配置值、密钥或请求内容                                            |

准入与用量摄入按排序取得可用钱包锁（SKIP LOCKED），只对已锁钱包加载余额；忙钱包不阻塞同批冷钱包。Store 只重试忙项，指数退避至50ms；HTTP 准入沿用请求取消/总期限，其他调用有2秒预算。用量超过本轮重试预算仍保留队列作业，不 ACK 丢失事实。

worker 通过唯一 `persist_usage_batch` 入口，在钱包先于请求/attempt 的锁顺序下去重、合并证据、创建消费者投递并调用 `settle_requests` 完成资金事务。HTTP runtime 无此入口的执行权限。配置审计由触发器和配置更改同事务提交。

注册 consumer 使用独占 advisory lock，与 usage 写入的共享锁协调，防止历史回放和实时新增的缝隙。worker 的资源认领用 `SKIP LOCKED` 和短租约；完成只接受原 token。超时尝试只追加 UNKNOWN 恢复事件，不重新执行请求。

FINAL 证据不会被后到 PARTIAL/UNKNOWN 降级。选定证据由 attempts.evidence_id 明确引用，兼容补证只能补全一致数字。缺失计数、未知成本、未完成任务不能自动清理；数据留存必须先在业务侧确认消费和责任已结束。

资金表包括 wallets/wallet_lots、holds/allocations、charges、refunds/adjustments/resolutions 和不可变平衡 money_journal。不存在 usage 结算 outbox；外部 event_deliveries 只用于业务消费。

生产使用独立迁移 owner、受限 gateway_runtime 和 gateway_worker 登录账号，执行 `infra/runtime-role.sql` 授予最小权限。保持 synchronous_commit=on，监控 WAL、连接数、锁等待和磁盘空间；不通过关闭 fsync 或异步丢证据换取压测数字。

## 分区、归档与留存

全局身份和幂等键保留在 `usage_event_index` / `money_entries`；`usage_payloads` / `journal_payloads` 按 UTC 月分区。正反复合外键保证每个身份恰有对应时间的 payload，金额、用量、全局序号及账本引用的约束不变。视图与直接底表路径都受白名单/平衡/不可变校验保护。

初始化及升级创建存量月份与未来三个月，DEFAULT 防止漏维护时业务中断。定期执行 `bun run db:archive -- --prepare YYYY-MM`；已进入 DEFAULT 的月份需要停止其他客户端后加 `--quiesced`，工具在同一事务内精确搬迁并恢复约束和不可变保护。

`bun run db:archive -- --month YYYY-MM --output /absolute/archive-directory` 对已过去月份导出有界 gzip NDJSON，写入 schema、MVCC snapshot、条数、压缩/原文 SHA-256 清单并自动复验。`--verify /absolute/archive-directory/manifest.json` 可独立复验；时间比较保留 PG 微秒精度。它是某一 MVCC 快照，晚到事实会进入后续快照，不能宣称自然月永久封闭。

指定 `--tablespace <已存在名称> --quiesced` 可把已验证月份的 payload 分区及全部索引移到冷存储。证据与账本在线查询仍然有效，归档工具不删除权威事实。全局元数据与金融记录仍会增长；按目标负载实测磁盘/WAL/备份预算，不能把分区说成数据库总量有界，也不能擅自给未结责任设置删除期限。
