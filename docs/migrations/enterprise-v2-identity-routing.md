# enterprise-v2 T07 技术身份、目录、路由与分布式准入

## 1. 当前行为（代码核对）

- Core 管理面已有：`POST /admin/projects`、`POST /admin/projects/{id}/keys`、`POST /admin/models`、`POST /admin/models/{model}/routes`、`PATCH /admin/routes/{id}`、`/admin/providers` GET/POST/PATCH。C14 组织/成员、项目列表与修改、Key 列表/修改/撤销、模型列表与修改、路由列表只存在于 legacy `/admin`（仅 `ADMIN_KEY`，无平台权限与审计）。
- C11（`/v1/models`、`/v1/usage`、`/v1/requests/{id}`）与 C12（`/v1/project/keys` 委派与撤销）只在 legacy 公开面；Caddy 仍把它们路由到 legacy。
- v2 准入：Core context 按 `orderRouteCandidates` 排序（WEIGHTED/ROUND_ROBIN/LEAST_LATENCY、priority），渠道/项目/Key/组织/成员配额由 Core 配额租约实施，跨渠道共享 `max_attempts`。但 v2 路径**从不调用 `Limits.observe`/`probe`**：熔断永远不会打开，half-open 代次无效，LEAST_LATENCY 只看到默认延迟 100。legacy 在每次发送后上报失败/延迟并持有 half-open 探测许可。
- Retry-After：legacy 只在同一请求重试**同一渠道**前等待；v2 每次重试都选择未尝试过的渠道，单请求内不存在同渠道重试，因而 Retry-After 没有作用对象。

## 2. 决策

- **D1 管理面迁入 Core**：C14、C15、C17 缺失的路径在 Core `/admin` 以原路径与原请求体提供，复用同一 storage 函数；写操作要求 `catalog:manage`，读取新增平台权限 `catalog:read`（追加迁移替换授权枚举）；每个写与 `audit_events` 同事务。legacy 同名管理路由仍被 legacy 非 Chat 执行路径的测试装置使用，随 T16 清理旧 Store/execute 路径一并删除。
- **D2 公开元数据与委派迁入 Core 公开监听**：C11、C12 在 e2 的公开监听实现，鉴权复用 `core_account_auth_v1`；角色规则与 legacy 相同：非 GET 需 ADMIN/INVOKER，BILLING 只访问 wallets/usage/requests/models，Key 管理仅 ADMIN。委派不得扩大 principal/model/operation/CIDR/配额/到期。Caddy 把这些路径改路由到 Core；公开角色仅获所需列权限。
- **D3 熔断与延迟归 Core**：Core 在 `execution_admit_v1`/`execution_next_v1` 授予前为所选渠道取得健康许可（closed 或 half-open 探测，Redis 中按 attempt 记录模式与代次）；无许可时拒绝该候选。Gateway 在 `execution_next_v1`（上一 attempt）与 `execution_quota_v1 CLOSE`（最后 attempt）中附带已发送 attempt 的观测 `{failed, latency_ms}`，Core 以许可的模式/代次调用 `observe`；未发送的 attempt 不观测。失败分类沿用 legacy：可重试状态、401/403 与传输中断为失败。
- **D4 Retry-After**：v2 不对同一渠道在单请求内重试，Retry-After 不延长期限也不阻塞其他渠道；作为有意差异记录，不实现等待。
- **D5 Redis 丢失**：配额或健康 Redis 不可用时 context/准入 fail-closed（不授予发送许可），恢复后无需重启即可继续；以真实 Redis 停止/恢复之外的可控手段（指向不可达端口的独立 Core）验证。

## 3. 里程碑

| 里程碑      | 交付                                                                    | 完成证据                                                                                      |
| ----------- | ----------------------------------------------------------------------- | --------------------------------------------------------------------------------------------- |
| g1 管理面   | C14/C15/C17 缺失路径、`catalog:read` 迁移、管理角色授权、审计           | 真实 Core 管理面 HTTP：组织/成员/项目/Key/模型/路由全生命周期、403、审计同事务、撤销即时生效  |
| g2 公开面   | C11/C12 在 Core 公开监听、公开角色授权、Caddy、artifact 检查            | 委派不能扩大权限、跨项目撤销 404、BILLING/VIEWER 限制、usage 游标、请求元数据不含正文         |
| g3 健康准入 | Core 健康许可、Gateway 观测上报、half-open 代次、Redis 丢失 fail-closed | 三次失败打开熔断并被 context 跳过、half-open 仅一个探测、旧代次观测被丢弃、LEAST_LATENCY 生效 |
| g4 审查验收 | 自审、文档、全量门禁                                                    | 同 T05 e4                                                                                     |

## 4. g1 实施记录

| 项       | 结果                                                                                                                                                                                                                                                                                                                              |
| -------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 路由     | Core `/admin` 新增 `POST /organizations`、`PATCH /organizations/{id}`、`POST /organizations/{id}/principals`、`PATCH /principals/{id}`、`GET /projects`、`PATCH /projects/{id}`、`GET /projects/{id}/keys`、`PATCH`/`DELETE /keys/{id}`、`GET /models`、`PATCH /models/{model}`、`GET /models/{model}/routes`，响应与 legacy 相同 |
| 契约     | 组织、成员、项目、模型、Key 修改请求体移入 `@gateway/contracts`（`organizationSchema` 等），legacy 与 Core 共用，不复制校验                                                                                                                                                                                                       |
| 权限     | 迁移 `2026111215`（`sql/core-catalog-read.sql`）加入 `catalog:read`；读取用 `catalog:read`，写入用 `catalog:manage` 并与 `audit_events` 同事务                                                                                                                                                                                    |
| 角色     | `core_management_api` 追加组织/成员读写、项目/Key/模型的可修改列更新权限                                                                                                                                                                                                                                                          |
| 测试     | `core-identity-catalog.test.ts` 2 例经真实 Core 进程 context：成员降为 VIEWER、组织停用、Key 模型/CIDR 收窄、Key 撤销、项目停用都在下一次 context 立即返回 `customer_forbidden`，恢复后立即可用；模型停用返回 `no_route`；Key 列表不含 verifier/明文；审计动作与资源逐条核对                                                      |
| 红绿证据 | 去掉 `registerIdentityCatalog` 后 2 例失败，恢复后通过                                                                                                                                                                                                                                                                            |

## 5. g2 实施记录

| 项       | 结果                                                                                                                                                                                                                                                                             |
| -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 路由     | Core 公开监听新增 `GET /v1/models`、`GET /v1/usage`、`GET /v1/requests/{id}`（全部 Key 角色，按 Key 模型/operation 范围过滤）与 `GET`/`POST /v1/project/keys`、`DELETE /v1/project/keys/{id}`（仅 ADMIN）；`/v1/wallets` 与 billing 回执维持 ADMIN/BILLING；响应体与 legacy 相同 |
| 委派     | 复用 `Store.delegateCredential`：principal、模型、operation、CIDR、RPM/TPM/并发与到期均不能超出签发 Key；撤销限本项目，跨项目 404                                                                                                                                                |
| 角色     | `core_public_api` 追加只读列（模型、请求状态、attempt、usage 视图、成本 revision、Key 非 verifier 列、项目/组织/成员状态）、`api_keys` 列级 INSERT 与 `UPDATE(enabled)`、配置审计触发器所需 `audit_events` 列级 INSERT；仍不能读 verifier/审计、不能改 Key 权限或项目            |
| Caddy    | `@account`（wallets/models/usage/project keys）与 `@request`（`/v1/requests/{id}` 及其 billing）在 legacy 之前路由到 Core 公开监听；架构测试约束顺序                                                                                                                             |
| 测试     | `core-public-metadata.test.ts` 3 例：四种角色读取元数据、请求详情字段集合固定无正文、游标与非法游标、范围外/跨项目隐藏、非 ADMIN 不能管理 Key；委派五种越权均 `delegation_denied`、继承上限、列表不含 verifier/明文、跨项目撤销 404、撤销后 401；公开角色权限边界（42501）       |
| 缺陷     | 首次实现委派返回 500：`audit_configuration` 触发器以调用者权限写 `audit_events`，公开角色无 INSERT；补列级授权后通过                                                                                                                                                             |
| 红绿证据 | 以 HEAD 版本公开监听运行，前 2 例失败（权限边界例在 HEAD 已成立）；恢复后 3 例通过                                                                                                                                                                                               |
| 产物     | `verify-catalog-artifact.ts` 对编译后 Core 公开面的 models/usage/project keys 校验未知 Key 返回 401                                                                                                                                                                              |
