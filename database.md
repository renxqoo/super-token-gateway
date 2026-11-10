# PostgreSQL 模型与事务

唯一当前定义为 `packages/storage/src/schema.sql`，初始化在事务和 advisory lock 下进行。只接受空库或精确的当前版本；非空旧库明确拒绝且不修改数据。运行进程只校验版本，不执行 DDL。不提供旧 schema、双写或向后兼容路径。

| 数据                               | 归属与不变量                                                                   |
| ---------------------------------- | ------------------------------------------------------------------------------ |
| projects / api_keys                | 技术项目与权限；Key 仅存 HMAC verifier 和显示前缀；即时撤销                    |
| providers / models / routes        | 上游配置与技术能力；供应商密钥 AES-256-GCM，provider_id 作为 AAD               |
| cost_versions                      | 费率快照不可更新/删除；变更路由只引用新版本                                    |
| gateway_requests                   | 逻辑意图、指纹、操作、别名；project/key/operation/intent 唯一                  |
| attempts                           | 物理调用序号、供应商、定价版本、状态；每请求 ordinal 唯一                      |
| usage_events                       | 不可变白名单数字证据；attempt/source 唯一；finality 与 cost_complete 分离      |
| event_consumers / event_deliveries | 可回放、SKIP LOCKED 租约认领、fenced ack、至少一次投递                         |
| resources                          | 虚拟 ID、原供应商 opaque ID、项目/模型归属、状态与轮询租约；没有正文或下载 URI |
| audit_events                       | 配置变更动作与哈希 ID，不保存配置值、密钥或请求内容                            |

worker 通过唯一 `persist_usage_batch` 入口，在钱包先于请求/attempt 的锁顺序下去重、合并证据、创建消费者投递并调用 `settle_requests` 完成资金事务。HTTP runtime 无此入口的执行权限。配置审计由触发器和配置更改同事务提交。

注册 consumer 使用独占 advisory lock，与 usage 写入的共享锁协调，防止历史回放和实时新增的缝隙。worker 的资源认领用 `SKIP LOCKED` 和短租约；完成只接受原 token。超时尝试只追加 UNKNOWN 恢复事件，不重新执行请求。

FINAL 证据不会被后到 PARTIAL/UNKNOWN 降级。选定证据由 attempts.evidence_id 明确引用，兼容补证只能补全一致数字。缺失计数、未知成本、未完成任务不能自动清理；数据留存必须先在业务侧确认消费和责任已结束。

资金表包括 wallets/wallet_lots、holds/allocations、charges、refunds/adjustments/resolutions 和不可变平衡 money_journal。不存在 usage 结算 outbox；外部 event_deliveries 只用于业务消费。

生产使用独立迁移 owner、受限 gateway_runtime 和 gateway_worker 登录账号，执行 `infra/runtime-role.sql` 授予最小权限。保持 synchronous_commit=on，监控 WAL、连接数、锁等待和磁盘空间；不通过关闭 fsync 或异步丢证据换取压测数字。
