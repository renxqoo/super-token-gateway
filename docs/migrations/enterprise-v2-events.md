# T12 数字事件、审计、月归档与恢复

覆盖主规格 C21–C24 的 Core 侧。本文件记录现状核对、决策与里程碑。

## 1. 当前行为（代码核对，2026-11-15）

- 事件生产已在 v2：worker 结算经 `core_evidence_group_v1` → `core_evidence_snapshot_v1` 在证据、账单、流水同一事务写 `usage_event_index`/`usage_payloads`（视图 `usage_events`）并为已登记消费者插入 `event_deliveries`；事件只含数字快照，不含正文。
- 事件消费只在 legacy：`/admin/usage`、`/admin/consumers`、`/{id}/claim`、`/{id}/ack` 在 `apps/legacy-gateway/src/admin.ts`，实现位于 legacy `Store` 类；Core 管理面没有对应路由，`core_management_api` 角色也无事件表权限。Core 公共面 `/v1/usage` 复用同一查询。
- 运维面：v2 执行恢复由 core-worker 周期执行（`recoverExecutions`），worker 计数器只在 `/readyz` 暴露；legacy 的 `/admin/recover`、`/admin/queue`、`/admin/queue/jobs/{id}/retry`、`/admin/metrics` 只操作 legacy 队列。
- 归档：`db:archive`（`scripts/archive.ts`）按月准备、导出、校验 `usage_payloads`/`journal_payloads` 分区并可迁移表空间；v2 新增的证据回执、资源、插件目录未纳入核对。

## 2. 决策

- **E1 事件实现单一来源**：查询与 claim/ACK SQL 从 legacy `Store` 移到 `packages/storage/src/events.ts`（`createEventStore`、`recordEventConsumer`）；legacy `Store` 的同名方法改为委托，T18 随 legacy 一并删除。
- **E2 权限**：新增平台权限 `events:read`（`GET /admin/usage`）与 `events:consume`（消费者登记、claim、ACK）；迁移 `2026111223`（`sql/core-events.sql`）只替换 `core_platform_authorize_v1` 权限枚举。每次调用在同一事务内前后两次核对平台凭证。
- **E3 审计同事务**：消费者登记与 `audit_events`（`event_consumer.create`）同事务；claim/ACK 属数据面，沿用 legacy 不审计。
- **E4 最小权限**：`core_management_api` 只获 `usage_events` 数字列与 `upstream_costs(evidence_id,revision)` 列级读权、`event_consumers` 读写与 `replay_cursor` 更新、`event_deliveries` 读写与租约列更新。
- **E5 未知消费者**：claim/ACK 对不存在的消费者返回 404 `not_found`（legacy 返回空页）。
- **E6 运维权限**：新增 `operations:read`（`GET /admin/queue`、`GET /admin/metrics`）与 `operations:manage`（`POST /admin/queue/jobs/{id}/retry`）；迁移 `2026111224`（`sql/core-operations.sql`）只替换权限枚举。
- **E7 settlement 队列运维**：Core admin 配置新增可选 `admin.queue`（`url`、`namespace`、`required_replicas`、`ack_timeout_ms`，与 Gateway `queue` 段同形，须与 core-worker 的 `QUEUE_REDIS_URL`/`REDIS_NAMESPACE` 一致）；配置时 Core 以 `core-evidence-v1` 通道的生产者读取 waiting/active/delayed/failed 计数，并只允许重试 failed 任务（非 failed 409 `job_not_failed`、不存在 404、非法 ID 400），重试后等待 WAITAOF。重试只重放 PG 证据持久化，结算对同一证据组幂等，不触达上游，因此不会重发生成。未配置时不注册 queue 路由，`/metrics` 的 `settlement_queue` 为 null。
- **E8 重试审计**：鉴权 → Redis 重试 → `audit_events`（`settlement_job.retry`）在同一 PG 事务中提交；Redis 与 PG 不能原子提交，若重试后审计事务失败，任务已重新排队但无审计行，接口返回错误，运维以队列状态核对。
- **E9 指标**：`GET /admin/metrics` 返回 PG 数字量：未终结请求（ADMITTED/RUNNING/UNKNOWN）、HELD 预留、已过期未授予的 permit（恢复积压）、等待终态用量的 v2 资源、用量冲突数、各消费者未 ACK 数，以及队列计数。进程内计数仍由 core-worker `/readyz` 提供。
- **E10 不迁移手动恢复触发**：v2 恢复由 core-worker 按 `RECOVERY_INTERVAL_MS` 连续执行，批满时立即续批，过期 permit 积压在指标中可见；legacy `POST /admin/recover` 不在 Core 提供，T18 随 legacy 删除。

## 3. 里程碑

| 里程碑 | 内容                                                                                       | 状态   |
| ------ | ------------------------------------------------------------------------------------------ | ------ |
| e1     | Core 事件 API：usage、消费者登记、claim、ACK，权限、审计与角色                             | 完成   |
| e2     | Core 运维：settlement 队列统计与失败任务重试（审计、不重发上游）、指标；手动恢复触发不迁移 | 完成   |
| e3     | 归档与恢复：v2 分区/归档失败回滚、备份恢复后账本/ACK/插件/资源一致                         | 待执行 |

## 4. e1 记录

- 路由（Core 管理面 `/admin`）：`GET /usage?project_id&before`、`POST /consumers`、`POST /consumers/{id}/claim`、`POST /consumers/{id}/ack`，请求体与 legacy 相同（严格 schema）。
- 测试 `tests/integration/core-events.test.ts` 2 例：真实 Gateway 请求结算后，`events:consume` 凭证登记回放消费者并审计（actor 为技术管理员）；claim 只含 14 个数字字段且不含提示/回答正文；错误租约 ACK 为 0；租约过期后重新 claim 得到同一批事件，旧租约 ACK 为 0、新租约全部确认，之后为空；非回放消费者只收到登记后的新事件。`events:read` 可按项目列出、非法游标 400、无关权限 403、只读凭证登记消费者 403 且无行写入、未知消费者 404。
- 红绿证据：未加角色授权时两例均 503（`management_unavailable`）；补授权后通过。legacy 事件回归（`lifecycle`、`gateway` 消费者竞争、`core-settlement-schema`）在委托后通过。

## 5. e2 记录

- 测试 `tests/integration/core-operations.test.ts` 2 例：真实 Redis 队列中首次处理失败的任务，`operations:read` 可见 failed 计数与指标、无关权限 403；只读凭证重试 403 且任务仍 failed、处理次数不变；`operations:manage` 重试返回 `scheduled`，任务恰好再处理一次后从 failed 清空，审计行记录技术管理员；再次重试 404、非法 ID 400。未配置队列时 `settlement_queue` 为 null、`/queue` 404，回放消费者在指标中列出且未 ACK 为 0。
- 红绿证据：去掉 `core-management-role.sql` 新增的 3 行列级授权，两例均 503；恢复后通过。
