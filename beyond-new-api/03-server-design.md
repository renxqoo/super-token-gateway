# 03 服务端设计

细化 [02-architecture.md](02-architecture.md) 的 A1–A7。所有新函数、表、字段、错误与配置在本文中是**拟定 v2.1 契约**，实现时在 `packages/contracts`、迁移与 OpenAPI 中落实；分阶段实施状态见[05-implementation.md](05-implementation.md)，未在实施记录中列为已完成的内容仍为拟定设计。标“待核对”的条目须在实现前查明现有引用。

## 1. 不变量

| ID  | 规则                                                                                          | 现有依据                                  |
| --- | --------------------------------------------------------------------------------------------- | ----------------------------------------- |
| I1  | 同一 project 同一意图摘要只产生一个请求；相同命令 ID 重放返回已提交结果，不同载荷返回冲突     | R06，`command_conflict`                   |
| I2  | 客户预扣、意图幂等与首次 attempt 在同一事务提交                                               | R06                                       |
| I3  | 供应侧：任一时刻 Σ(在途预扣) + Σ(已结算未折叠成本) ≤ 账本余额，超预估成本完整入账并关闭新许可 | R08、R09                                  |
| I4  | UNKNOWN 敞口不因时间、重启、停用或缓存失效释放                                                | R06                                       |
| I5  | 吊销线性化：吊销提交早于资金事务取得身份共享锁，则该事务按吊销处置；反之吊销等待该事务提交    | `enterprise-v2-internal-identity.md:211`  |
| I6  | 准入使用的候选语义与资金事务提交时的目录状态一致                                              | `enterprise-v2-financial-admission.md:25` |
| I7  | 成功终态之前证据已 WAITAOF 确认；worker 在证据、账单、流水、事件同事务提交后才 ACK            | R07                                       |
| I8  | 每个 attempt 绑定不可变插件版本与配置快照；内置与沙箱两种执行路径对同一输入产生相同字节输出   | R11；本方案新增后半句                     |
| I9  | 客户 Key 吊销后，提交时间晚于吊销的准入一律拒绝                                               | 现有 Key 授权语义                         |
| I10 | 配额租约最终释放恰好一次；租约释放不早于其 attempt 进入终态或明确未执行                       | 现有 quota lease 语义                     |

## 2. 契约变化

### 2.1 `execution_context_v1`：签名上下文

本节尚未实施，原Core单独验MAC后向PG提交主体的权限假设已被审计推翻。切换前须落实PG独立MAC验证或通过客户verifier重新定位主体，补齐IP、完整scopes、资源与batch事实、历史命令及恢复迁移；当前保留持久context。不能按以下拟定格式直接绕过SQL主体绑定，详见[实施记录](05-implementation.md)与[ADR 0008](../docs/decisions/0008-performance-and-production-reliability.md)。

响应删除 `context_id`，新增 `context_token`；其余字段不变（`packages/contracts/src/core-execution.ts:316`）。

```json
{
  "owner": { "service": "gateway", "instance_id": "…", "session_id": "…", "incarnation_id": "…" },
  "context_token": "v1.k3.eyJ…payload….3q2-…mac…",
  "project_id": "…",
  "key_id": "…",
  "customer_currency": "USD",
  "expires_at": "2026-11-17T08:00:30.000Z",
  "operation": "chat",
  "model": "gpt-4o-mini",
  "candidates": [
    {
      "ref": { "route_id": "…", "channel_id": "…", "binding_id": "…" },
      "catalog_revision": "418",
      "config": {}
    }
  ]
}
```

- `context_token = "v1." + key_id + "." + B64(J(payload)) + "." + B64(HMAC-SHA256(key, J(payload)))`。payload 为 `{owner, project_id, key_id, key_revision, customer_currency, operation, model, fingerprint, candidates:[{ref, catalog_revision}], max_output_tokens, execution_budget, issued_at, expires_at}`。`J` 与 `B64` 沿用 `enterprise-v2-internal-identity.md:172` 的定义。
- 密钥为 Core API 部署密钥，按 `key_id` 轮换；新增配置项名称在实现时写入 `apps/core-api/src/config.ts` 与部署文档，Gateway 不持有。
- 有效期沿用现有 `expires_at` 规则。

### 2.2 `execution_admit_v1` / `execution_next_v1`

`dispatch.context_id` 改为 `dispatch.context_token`（字符串，≤ 8 KiB）。Core 在事务外验 MAC、owner 等于调用方、未过期、`fingerprint` 一致，失败返回现有 `context_invalid`；事务内修订号或 Key 修订不一致返回现有 `candidate_stale`，Gateway 现有处理不变（重新取 context）。

### 2.3 健康观测批

新增 `execution_health_v1`，替代成功路径上的 `execution_quota_v1 action=OBSERVE`：

```json
{
  "observations": [{ "channel_id": "…", "attempt_id": "…", "outcome": "SUCCESS", "latency_ms": 41 }]
}
```

每批 1–256 项；Gateway 按 100 ms 或 256 项刷新。Core 逐项调用现有 `routingLimits.settleHealth`。失败只影响路由健康统计，不影响资金；丢失时与现有 `.catch(() => undefined)` 行为一致。

### 2.4 成功路径不再发送 `execution_quota_v1 action=CLOSE`

attempt 的成功终态由证据驱动，结算事务关闭其配额租约（§4.3）。取消、未发送、begin 结果未知等现有路径保持 `execution_abort_v1` 与 CLOSE。

### 2.5 插件执行接口

`packages/plugin-host` 对外暴露同一执行接口：`call({ packageHash, function, input, state, deadline }) → { output, state }`。两个实现：

| 实现        | 适用                                               | 隔离                                             |
| ----------- | -------------------------------------------------- | ------------------------------------------------ |
| `inProcess` | 包的发布者信任级别为平台内置签名（T08 发布者信任） | 同进程；每次调用新建适配器状态对象，状态显式传入 |
| `wasm`      | 其他所有包                                         | 现有 helper，每次调用新建 guest                  |

选择发生在绑定插件版本时并写入 attempt 快照，attempt 生命周期内不切换。新增多帧批 ABI：`stream_feed` 输入可携带多帧，输出按帧顺序返回，两种实现共用。

## 3. 数据

### 3.1 `catalog_revisions`

先行目录切片采用私有core_catalog_cache schema的单行集合epoch与按scope派生候选缓存，保留现有context与候选ref比较；warm在资金事务内先持epoch SHARE，再读本代缓存，不再逐项重算原候选。全局集合epoch覆盖新增/删除与LIMIT边界，不能用每route revision的MAX代替。缓存总逻辑payload64MiB/4096行，cold预算竞争或超额时计算但不缓存，不拒绝合法请求；候选本身的金额、ref和digest算法不变。原route级细分表是后续可选精细化，未实施，也不能将本切片称为签名context迁移完成。

| 列         | 类型          | 说明          |
| ---------- | ------------- | ------------- |
| `route_id` | uuid PK       | 候选所属路由  |
| `revision` | numeric(38,0) | 单调递增，≥ 1 |

触发器覆盖 `core_candidates_v1` 读取的全部表与列：任一影响该路由候选语义的写入，在同一事务内 `UPDATE catalog_revisions SET revision=revision+1`。触发表清单由实现从 `core_candidates_v1` 的 FROM/JOIN 生成，`tests/integration` 中逐表逐列断言“改语义字段 → 修订递增；改非语义字段 → 不递增”。共享对象（如插件发布、渠道）变化时递增所有引用它的路由。

### 3.2 供应额度单元

此节原表示尚未实施且存在已证实的数学缺口：结算同时quota-=cost、unfolded+=cost而主行allocated不变时，allocated不能定义为Σquota；保持后续公式至少要求allocated=Σ(quota+unfolded)，还需闭合超预估后的负单元及恢复语义。partial和修订结算必须按差额推进。实施前正在核对更小的root未分配余额与cells汇总表示，详见[实施记录的独立反例](05-implementation.md)。以下原字段表不构成可直接落库的最终设计。

原提案 `upstream_balance_cells`：

| 列                     | 类型          | 说明                            |
| ---------------------- | ------------- | ------------------------------- |
| `account_id`           | uuid          | PK 之一，FK `upstream_accounts` |
| `currency`             | text          | PK 之一                         |
| `cell`                 | smallint      | PK 之一，0..N−1                 |
| `quota_atoms`          | numeric(38,0) | 从主行划入、尚未消耗的额度      |
| `reserved_atoms`       | numeric(38,0) | 在途预扣                        |
| `unfolded_spent_atoms` | numeric(38,0) | 已结算、尚未折叠回主行的成本    |
| `revision`             | numeric(38,0) |                                 |

约束：`reserved_atoms ≥ 0`、`unfolded_spent_atoms ≥ 0`。`upstream_holds` 增加 `cell smallint`。若保留本表示，主行 `allocated_atoms` 必须为Σ单元(quota_atoms+unfolded_spent_atoms)；原Σquota定义撤回，不能在未定义超预估归一化前增加allocated非负约束。

账本关系：

```text
账户可用 = balance_atoms − reserved_atoms(主行) − allocated_atoms + Σ(cell.quota_atoms − cell.reserved_atoms)
账户余额 = balance_atoms − Σ cell.unfolded_spent_atoms
```

报表、对账、充值与调账读取汇总视图，不直接读主行字段；`validate_upstream_journal` 等不变量函数同步改写（待核对全部引用）。

### 3.3 `execution_contexts`

热路径不再写入。permit 以 `context_digest = H(J(payload))` 替代对 context 行的引用；现有恢复与升级路径若读取 `execution_contexts`，改为读取 permit 快照。**待核对**：所有引用 `execution_contexts` 的外键、函数与测试。

### 3.4 配额租约

`execution_quota_leases` 不变；结算事务写 `RELEASE_PENDING`，Core API 恢复循环（`core_quota_recovery_v1`）按现有流程释放 Redis 并写 `RELEASED`。

## 4. 事务

### 4.1 集合式准入批 `core_admit_batch_v2(items jsonb)`

先行切片S2a2仅对身份去重，尚不进行集合资金写：按完整九字段authority元组（含所有三层revision/证书/key/incarnation）分组，每组在独立预阶段scope调用原权威屏障并把锁提升到外层；item失败不会释放已缓存身份锁。每item仍实时检查自身投影与期限、证书时段与session expiry，其余客户授权/目录/资金路径保持原样。新私有item替换无调用旧kernel并撤销runtime执行权限，完整边界见[实施记录](05-implementation.md)。以下集合资金路径仍为后续设计，不能将“Key revision相等”替代全部organization/principal/project/key授权。

替代 `core_execution_batch_v1` 中 admit/next 的逐项路径。每批 1–128 项（沿用 Batcher）。

1. 事务外（Core API）：验 context MAC、owner、过期、fingerprint；验 manifest 与 dispatch 摘要；按 `(account, currency)` 和 attempt ID 哈希计算目标单元。
2. 第 0 级：对批内全部命令 ID、意图摘要取 advisory try-lock（现有规则）。
3. 身份：按批内去重后的 session，按 `instance → certificate → session` 全序 `FOR SHARE NOWAIT`，用 `clock_timestamp()` 重核有效性与 revision；某会话失败，只拒绝该会话的项（`internal_identity_revoked` 等现有码）。锁持到提交。
4. Key：去重 `key_id`，`FOR SHARE`，比对 `key_revision`；不一致的项 `candidate_stale`。
5. 目录：去重 `route_id`，`catalog_revisions FOR SHARE`，比对修订号；不一致的项 `candidate_stale`。
6. 资金锁：钱包按 id 排序、额度单元按 `(account, currency, cell)` 排序，`FOR NO KEY UPDATE NOWAIT`；取不到锁的组整组返回 `identity_busy`（现有 BUSY 码，Core 1 秒内重新入批，R19）。
7. 判定：逐组按现有规则计算预扣、授信、单元可用额；单元不足时对主行 `NOWAIT` 补拨（§4.4），仍不足则拒绝该项。
8. 集合写：`INSERT … SELECT FROM jsonb_to_recordset` 一次写入 requests、billing_holds、attempts、permits、upstream_holds、命令回执；钱包与单元各一次 `UPDATE … FROM (VALUES …)`。
9. 提交，返回逐项结果。

成本：身份、Key、目录各按去重集合执行一次；无逐项子事务。批内某组失败通过“先判定后集合写”排除，不依赖 savepoint 回滚。

### 4.2 集合式 begin 批

P0和实施审计已推翻原“KEY SHARE读状态足够，因此不锁钱包/余额”的估计。当前保留原钱包及供应余额写锁；它们保护冻结、授信、balance>=reserved、HELD和最后发送条件。普通状态UPDATE不受KEY SHARE阻挡。后续只有在设计并验证替代栅栏、重放及冻结交错后才能减少锁，以下原步骤不得作为直接删锁的授权。

1. 身份同 4.1 第 3 步。
2. permit 行 `FOR NO KEY UPDATE NOWAIT`，校验 `RESERVED`、epoch、owner、dispatch 摘要。
3. 账户状态：`upstream_accounts FOR KEY SHARE`，拒绝 `DISABLED/QUARANTINED/RECONCILE_REQUIRED`。不锁余额与单元。
4. 集合 UPDATE permit → `SEND_GRANTED`。

最终函数位于core-batches.sql，S2a2以等价资金正文迁至私有core_execution_item_v2；旧core-execution.sql行号仅供历史追溯。完整原因及保护要求已核对，不再标为未知；KEY SHARE替代方案不成立。

### 4.3 集合式结算批

替代 `persist_core_evidence_groups_v1` 中逐组子事务与锁冲突重试。

1. 验签与 permit 绑定预读（现有，事务外）。
2. 第 0 级 advisory，身份共享锁（每 producer 会话一次）。
3. 锁：请求、attempt、钱包、额度单元，各按主键排序一次取锁；`upstream_accounts` 改为 `FOR KEY SHARE`（现 `core-settlement.sql:105,207` 为 NO KEY UPDATE，仅读状态时降级；状态迁移另走主行路径）。
4. 集合写 RAW/receipt/CANONICAL、账单、两侧流水、事件投递；单元 `reserved −= est`、`quota −= cost`、`unfolded_spent += cost`；超预估时单元 `quota` 可为负，同事务把账户状态置为现有超支冻结态（走主行路径，罕见）。
5. 终态 attempt 的配额租约置 `RELEASE_PENDING`。
6. 提交后 ACK（I7）。

### 4.4 补拨与折叠

- 补拨（准入内）：主行 `FOR NO KEY UPDATE NOWAIT`，划入 `chunk = clamp(est × 64, floor, 账户可用 / N)`；失败按单元不足处理。
- 折叠（Core worker 定时，默认 1 s 或单元未折叠成本超过阈值）：主行与该账户全部单元按序加锁，`balance −= Σ unfolded`、`spent += Σ unfolded`、`allocated −= Σ unfolded`，单元 `unfolded_spent = 0`；与准入/结算冲突时 NOWAIT 跳过本轮。
- 回收：账户停用、调账或单元长期空闲时，把单元剩余 `quota − reserved` 归还主行。

### 4.5 吊销与目录写入

- 身份吊销：保持现有全序 UPDATE 与审计，提交后 `NOTIFY stg_identity`，Core API 清除对应会话缓存。线性化由 4.1 第 3 步的共享锁保证，缓存不参与判断。
- Key 吊销：现有写入加 `key_revision` 递增与 `NOTIFY stg_key`。
- 目录写入：触发器递增修订号，提交后 `NOTIFY stg_catalog`，Core API 重建该路由快照。

## 5. 缓存

S2a1先实现中间件peer/session的有界预读缓存：TTL 1秒、4096条、64个冷加载；身份写入同事务NOTIFY，监听确认/重连清空缓存。缓存只服务密码证明与提前拒绝，每个业务处理仍进入原数据库权威身份屏障；因此通知丢失不产生授权，只会暂时增加陈旧拒绝。这里采用TTL而不是另加全局身份修订轮询。下表客户Key和目录快照仍属后续设计，不能将其视为已实现。

| 缓存（Core API 内存） | 键                       | 失效                                           | 允许陈旧       | 权威校验                          |
| --------------------- | ------------------------ | ---------------------------------------------- | -------------- | --------------------------------- |
| startup 会话          | session_id               | `NOTIFY stg_identity`；每 5 s 比对全局身份修订 | 只影响提前拒绝 | 资金事务身份共享锁（4.1/4.2/4.3） |
| 客户 Key              | `H(pepper, token)`       | `NOTIFY stg_key`；条目 TTL 30 s                | context 可陈旧 | 准入 Key 共享锁 + 修订比对        |
| 目录快照              | route_id → 候选 + 修订号 | `NOTIFY stg_catalog`；每 5 s 比对最大修订      | context 可陈旧 | 准入修订号栅栏                    |

`NOTIFY` 丢失（连接重建）时，重连后全量比对修订号再恢复服务；比对完成前 context 直接读 PG（现有路径）。

## 6. 场景核对

以下推演未实跑；“验证入口”为 [04-spec.md](04-spec.md) 对应条目。

| 规则 | 前置与交错                              | 期望                                               | 机制                               | 验证                         |
| ---- | --------------------------------------- | -------------------------------------------------- | ---------------------------------- | ---------------------------- |
| I5   | 准入批已取会话共享锁，吊销 UPDATE 到达  | 吊销等待批提交；批内结果有效                       | 4.1 第 3 步锁持到提交              | SR-11                        |
| I5   | 吊销已提交，缓存尚未失效，admit 到达    | 该会话所有项被拒绝                                 | 事务内重核不读缓存                 | SR-11                        |
| I6   | context 签发后目录改价，admit 到达      | `candidate_stale`，不按旧价预扣                    | 修订号栅栏                         | SR-12                        |
| I6   | 准入批持修订共享锁时目录写入            | 目录写入等待；准入按旧修订提交合法                 | 触发器 UPDATE 与 SHARE 冲突        | SR-12                        |
| I9   | Key 吊销提交后，持旧 context 的 admit   | 拒绝                                               | Key 共享锁 + 修订比对              | SR-13                        |
| I3   | 1 账户 16 单元并发准入至额度耗尽        | 总预扣 ≤ 余额；超出者拒绝；无负可用                | 单元行锁 + 补拨锁主行              | SR-14                        |
| I3   | 结算成本超预估且单元额度不足            | 成本完整入账，账户冻结新许可                       | 4.3 第 4 步                        | SR-14                        |
| I3   | 折叠与准入、结算并发                    | 余额恒等式始终成立                                 | 全序加锁 + NOWAIT 跳过             | SR-14                        |
| I2   | 批内一组钱包锁失败                      | 仅该组 BUSY，其余提交；无半写                      | 先判定后集合写                     | SR-10                        |
| I1   | 同一命令 ID 在两个批中重复              | 只一次生效，另一次返回原回执                       | advisory + 回执唯一约束            | SR-10                        |
| I10  | 成功证据结算后 Core 在释放 Redis 前崩溃 | 重启后恢复循环释放，恰好一次                       | `RELEASE_PENDING` + 恢复循环       | SR-15                        |
| I10  | 结算滞后时同渠道并发配额                | 占用延长至结算完成，不超发                         | 租约仍计数                         | SR-15                        |
| I4   | admit 后、begin 前 Gateway 崩溃         | permit 停在 RESERVED，由现有恢复判定为未发送并释放 | 未合并 admit 与 begin              | 现有 core-execution-recovery |
| I8   | 同一内置包分别走 inProcess 与 wasm      | 全部合成 fixtures 输出字节一致                     | 差分测试                           | SR-16                        |
| I8   | 内置包两个并发 attempt                  | 无跨请求状态（金丝雀正文不串）                     | 每次调用新建状态对象、状态显式传入 | SR-16                        |
| I7   | WAITAOF 未确认时 Gateway 崩溃           | 客户端未收到成功；恢复路径按现有规则               | 未改                               | test:durability              |

## 7. 运行

- 批参数：admit/begin 批上限 128、当前 Batcher 等待 3 ms（目标等待值须实测确定）；结算批 ≤ 128 组（现有）。新增指标：每批项数、批事务时长、BUSY 率、`candidate_stale` 率、补拨次数、折叠延迟。
- 背压：BUSY 沿用 R19 的 1 秒内重新入批；准入队列满时返回现有 `internal_capacity`，Gateway 现有 `whileBusy` 退避。
- 审计：`bench-system` 的 `audit.json` 增加 `pgStatements`（每请求调用数、执行时间、WAL 字节）与按 PID 的进程 CPU（覆盖退出的 PG 后端）。

## 8. 演进

开发环境、单一实现（R13 与 `enterprise-v2.md` §1.2）：新函数与新表通过新增迁移引入，同一阶段内切换调用方并删除被取代的函数（`core_execution_batch_v1` 的 admit/next 分支、`core_preflight_v1` 在 admit 中的调用、`persist_core_evidence_groups_v1` 旧实现），不保留双轨。额度单元迁移为存量账户创建 N 个空单元，`allocated_atoms = 0`，账本恒等式在迁移后由 `validate_upstream_journal` 校验。回退方式为回退迁移前备份（`test:backup` 覆盖），不提供在线降级。
