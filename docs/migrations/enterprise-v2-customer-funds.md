# Enterprise v2 客户资金迁移（T05）实施方案

状态：用户已确认 D1–D3 按推荐执行。日期：2026-11-14。起点：`main` `cdf19a1`（T04.c 完成，未推送）。

本记录对应 [enterprise-v2.md](enterprise-v2.md) 任务图 T05：客户钱包、授信、价格、退款完整迁移到 Core，覆盖能力 C13、C18–C20 及 §5.1 客户资金不变量。客户账本的 SQL 规则（`credit_wallet`、`configure_wallet_credit`、`refund_billing_charge`、`adjust_billing_charge`、`settle_requests`、`expire_wallet`、`collect_wallet_debt`）由 legacy 与 v2 结算共用，本单元不改其语义，只迁移调用方、权限与驱动进程，并以 v2 三进程真实用例验证。

## 1. 当前行为（实测与代码核对）

| 范围           | 现状                                                                                                                                                                                                                  | 依据                                          |
| -------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------- |
| C18–C20 管理   | 仅 legacy：`/admin/projects/{id}/credits`、`/wallets`、`/wallets/{currency}/lots`、`/journal`、PATCH 钱包、`/credit-policy`、`/admin/tariffs`、退款、调账、resolution；`FundsStore` 直连 PG，ADMIN_KEY 鉴权，不写审计 | `apps/legacy-gateway/src/admin.ts:269–335`    |
| C13 公开查询   | 仅 legacy：`GET /v1/wallets`、`GET /v1/requests/{id}/billing`，project key 角色须为 ADMIN/BILLING                                                                                                                     | `apps/legacy-gateway/src/app.ts:209–224`      |
| Core 管理面    | 已有 `POST /admin/projects/{id}/credits` 与 `POST /admin/billing/tariffs`（T04.b 测试装置引入；后者不是原路径）                                                                                                       | `apps/core-api/src/platform/management.ts`    |
| 平台权限       | `catalog:manage`、`funding:read`、`funding:recharge`、`customer:fund`，由 `core_platform_authorize_v1` 枚举                                                                                                           | `packages/storage/src/sql/core-execution.sql` |
| 争议后重新结算 | `reconcileKnownHolds` 只由 legacy worker 驱动，v2 请求的争议裁决依赖 T16 要删除的进程                                                                                                                                 | `apps/legacy-gateway/src/worker-main.ts:54`   |
| 测试           | `billing.test.ts` 直接调用 `FundsStore`/`Store`/SQL；legacy HTTP 用例约 8 处（`gateway.test.ts`）、`routing.test.ts` 2 处、`scripts/verify-artifact.ts` 2 处                                                          | `tests/integration`                           |

## 2. 关键决策（用户已确认）

- **D1 C13 由 Core API 公开监听提供**：Core API 新增独立 `public` 监听，只承载两个只读账务端点；project key 校验复用执行路径的 pepper verifier 与 `core_auth_v1`（撤销、IP、组织/成员状态），并要求 ADMIN/BILLING 角色。Caddy 把 `/v1/wallets`、`/v1/requests/*/billing` 路由到该监听；管理监听仍不公开。T07 扩展 C11–C12 时复用同一实现。
- **D2 新增平台权限**：追加迁移新增 `customer:read`（钱包、lots、流水、费率读取）与 `customer:adjust`（退款、调账、人工 resolution）；充值、授信、钱包冻结/策略用 `customer:fund`；写费率用 `catalog:manage`。所有写操作与审计同事务。
- **D3 争议后重新结算移到 core-worker**：core-worker 定时调用同一结算 SQL（SKIP LOCKED + 租约 token，封装为 `core_reconcile_holds_v1`，实施见 §8），从 legacy worker 删除，core-worker 为唯一执行者；结算角色追加最小授权并实测。

其余工程选择：

1. 管理与查询能力移到 `packages/storage/src/customer-funds/`，按函数暴露；legacy `FundsStore` 只保留 legacy 准入的 `quote`，T16 随 legacy 删除。
2. legacy 删除 C13、C18–C20 路由；Core `/admin/billing/tariffs` 改为原路径 `/admin/tariffs`（GET/POST），调用方同步迁移，不留别名。路径与请求体不变，`openapi.ts` 按 T04.c 惯例继续描述整体契约，不删除这些条目，T14 统一 OpenAPI 时迁走。
3. `core_management_api` 补齐所需函数与列级授权；公开监听使用独立只读角色；每个角色以“允许的成功、其余被拒”实测。
4. 数据库异常只在 storage 边界转换。资金业务错误码保持 legacy 原值（`insufficient_funds`、`billing_conflict`、`not_found`、`invalid_resolution`、`currency_mismatch`、`incomplete_evidence`）；输入校验与鉴权使用 Core 管理面统一信封（`invalid_management_input` 400、`unauthenticated` 401、`platform_scope_denied` 403），这是有意差异。
5. ADR 0005 中授信策略“独立 ADMIN_KEY 授权”由 `customer:fund` 平台权限取代；ADMIN_KEY 仍作为 bootstrap 凭证拥有全部权限。ADR 作为历史决定不改写。

## 3. 里程碑

| 里程碑           | 交付                                                                                                                     | 完成证据                                                                                                                               |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------- |
| e1 管理 API      | customer-funds 存储模块；Core 管理面 C18–C20 全部原路径；权限迁移与审计；管理角色授权；legacy 路由删除与测试迁移         | 真实 Core API HTTP 覆盖每个端点：幂等回执、同 key 异 payload 冲突、401/403、审计同事务；`billing.test.ts` 改用新模块全部通过；全量门禁 |
| e2 公开查询      | Core 公开监听与 project key 鉴权；C13 两端点；配置、systemd、Caddy、OpenAPI                                              | ADMIN/BILLING 允许、INVOKER 拒绝、撤销即时生效、IP 限制、跨项目 404、币种隔离、精确字符串金额                                          |
| e3 v2 资金端到端 | v2 charge 退款/调账；争议→冻结→resolution→core-worker 重新结算；超额应收完整入账并冻结；授信并发；热点钱包不阻塞无关钱包 | 三进程真实用例，期望值手算；reconcile 迁移先红后绿；结算角色实测                                                                       |
| e4 审查与验收    | 只读独立审查、逐条处置、文档、全量门禁                                                                                   | 同 T04.c c4                                                                                                                            |

每个里程碑结束运行全部门禁并本地提交，推送需另行指示。

## 4. 测试迁移矩阵

| 旧测试                                                                 | 处理                                                                                                                |
| ---------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| `billing.test.ts` 账本规则（来源、过期、债务、版本、并发、争议、批次） | 断言保留，改为调用 customer-funds 模块；依赖 legacy 准入（`admit_gateway_batch`）的用例保留为 legacy 回归，T16 删除 |
| legacy HTTP 中这些路由的用例                                           | 改为调用 Core 管理面或公开监听的同一路径，断言不弱化                                                                |
| Core 测试装置的 `/admin/billing/tariffs`                               | 改为 `/admin/tariffs`                                                                                               |
| 新增                                                                   | e1–e3 列出的真实 HTTP 与三进程用例                                                                                  |

## 5. 风险

- C13 的 key 鉴权先于 T07 在 Core 落地，T07 必须复用，不写第二套。
- 争议与超额应收在 v2 路径上目前只有 SQL 级证据，e3 首次端到端验证，可能暴露 T04.b 结算缺陷；如暴露，按先红后绿在本单元修复并记录。

## 6. e1 实施记录

| 项           | 结果                                                                                                                                                                                                                                                                               |
| ------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 存储         | `packages/storage/src/customer-funds/` 提供 `createCustomerFundsStore(db \| tx)`（入参为事务时不再开新事务）、`recordBillingTariff`、`fundingError`；`FundsStore` 只保留 legacy 准入的 `quote`                                                                                     |
| Core 管理面  | `apps/core-api/src/platform/customer-funds.ts` 在 `/admin` 下提供 C18–C20 全部原路径；每个写操作与 `audit_events` 同事务，授权在动作前后各校验一次                                                                                                                                 |
| 权限         | 迁移 `2026111211`（`sql/core-customer-funds.sql`）替换 `core_platform_authorize_v1` 的权限枚举，加入 `customer:read`、`customer:adjust`                                                                                                                                            |
| 角色         | `core_management_api` 追加资金函数与列级授权；`gateway_runtime` 收回 `credit_wallet`、`configure_wallet_credit`、`refund_billing_charge`、`adjust_billing_charge` 执行权及费率、resolution、钱包策略列写权（legacy 不再承载这些路由）                                              |
| legacy       | 删除 C13、C18–C20 路由；争议后重新结算暂以 `reconcileHolds` 依赖保留在 legacy worker，e3 迁到 core-worker                                                                                                                                                                          |
| 测试迁移     | `billing.test.ts`、`lifecycle.test.ts`、`routing.test.ts`、`gateway.test.ts` 改调存储模块；legacy HTTP 授信策略用例的 404/400/409 断言迁到 Core HTTP 用例；legacy runtime 角色用例改为断言上述资金函数与表写入被拒（42501）；Core 装置与数值用例改用 `/admin/tariffs`              |
| 新增         | `tests/integration/core-customer-funds.test.ts`：v2 charge 退款/调账幂等与冲突、钱包/lots/流水分页、授信与授信策略、冻结、费率读取、resolution 重放/冲突/未捕获价格/调度重新结算、401/403 拒绝时资金与审计不变、失败写操作不留审计                                                 |
| 红绿证据     | 去掉 `core-management-role.sql` 新增授权或去掉 `2026111211` 迁移，新增 4 个用例全部失败；恢复后全部通过                                                                                                                                                                            |
| 迁移版本断言 | `core-execution-upgrade`、`plugin-catalog-upgrade`、`lifecycle` 中“升级到当前清单”的版本数、版本列表与最高版本随追加迁移更新；`lifecycle` 升级用例原断言升级后 `gateway_runtime` 可设置授信策略，现改为断言该角色被拒（42501），并经 customer-funds 模块验证升级后授信策略仍可设置 |

## 7. e2 实施记录

| 项         | 结果                                                                                                                                                                                                                                                                                                                                                       |
| ---------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 配置与进程 | Core API 配置新增可选 `public` 段（`database_url`、`hostname`、`port`、`trusted_proxy_cidrs`、`shutdown_grace_ms`），须与 `execution` 同时配置以复用 pepper；同一 Core API 进程启动独立监听，ready 日志输出 `public_port`，SIGTERM 与管理监听一起有界停止；监听启动失败时关闭已建连接池                                                                    |
| 端点       | `apps/core-api/src/public/account.ts` 提供 `GET /v1/wallets`、`GET /v1/requests/{id}/billing`、`/livez`、`/readyz`；token 格式、401/403 文案与错误信封沿用 legacy（`unauthenticated`、`ip_denied`、`role_denied`、`not_found`、`invalid_request`，带 `request_id`）；`no-store`、`nosniff`                                                                 |
| 鉴权       | 迁移 `2026111212`（`sql/core-account-access.sql`）新增 `core_account_auth_v1(verifier, ip)`：每次请求读库，校验 key/项目/组织/成员启用与过期、成员所属组织与按 key 角色允许的成员角色、`allowed_cidrs`；返回项目、角色与模型/操作范围。角色仅允许 ADMIN/BILLING。`core_auth_v1` 只服务执行路径（角色固定 ADMIN/INVOKER 并按操作/模型授权），因此不直接复用 |
| 可见性     | 请求账单按项目隔离，并受 key 的 `models`/`operations` 范围约束（与 legacy 相同），范围外与跨项目均 404                                                                                                                                                                                                                                                     |
| 角色       | `infra/core-public-role.sql`：`core_public_api` 只有鉴权函数执行权、`wallets` 读权及账单投影所需列的列级读权；不能读凭证、渠道密钥、请求指纹、流水，不能写钱包或调用资金函数（实测 42501）                                                                                                                                                                 |
| 部署       | Caddy 把 `/v1/wallets` 与 `/v1/requests/*/billing` 路由到 Core 公开监听（示例 `127.0.0.1:8095`）；systemd 单元不变（同一进程）；OpenAPI 仍由 legacy `openapi.ts` 描述整体契约，T14 统一                                                                                                                                                                    |
| 测试       | `tests/integration/core-account.test.ts` 3 例经真实 Core 进程 HTTP：ADMIN/BILLING 允许且金额为精确字符串、INVOKER/VIEWER 403、跨项目与范围外 404、币种分行、撤销/过期/项目停用下一次即 401、IP 经可信代理判定、角色最小权限；`verify-catalog-artifact` 对编译产物检查公开监听 readyz 与 401                                                                |
| 红绿证据   | 去掉 `2026111212` 迁移后新增 3 例全部失败，恢复后全部通过                                                                                                                                                                                                                                                                                                  |

## 8. e3 实施记录

| 项           | 结果                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| 重新结算     | 迁移 `2026111213`（`sql/core-hold-reconciliation.sql`）把原 `reconcileKnownHolds` 的候选选择、SKIP LOCKED 租约与 `settle_requests` 调用封装为 SECURITY DEFINER 函数 `core_reconcile_holds_v1(batch)`（1–256），租约与结算同一事务；storage `reconcileKnownHolds` 改为调用它，没有第二份 SQL                                                                                                                                                                                                                                                                              |
| 进程归属     | legacy worker 与 `Dependencies.reconcileHolds` 删除重新结算；core-worker 成为唯一执行者，`RECONCILE_INTERVAL_MS`（默认 1000）、`RECONCILE_BATCH`（默认 32）；整批结算时立即继续，否则按间隔等待；失败计入 `maintenance_failures`，readyz/停止日志输出 `reconciled`                                                                                                                                                                                                                                                                                                       |
| 遗留请求回收 | core-worker 新增可选 Core 内部 RPC 客户端（`CORE_ORIGIN`、`CORE_SERVERNAME`、`CORE_AUDIENCE`、`CORE_PEER_SHA256`、`CORE_SUBJECT_ALTNAME`、`CORE_KEY_FILE`、`CORE_CERT_FILE`、`CORE_CA_FILE`，须同时配置；`NODE_ENV=production` 时必填），以 core-worker 身份按 `RECOVERY_INTERVAL_MS`/`RECOVERY_BATCH` 周期执行：① `execution_quota_recovery_v1` 完成 RELEASE_PENDING 配额释放；② 对 `RESERVED`、未授予且已过期的 permit 调用 `execution_abort_recovery_v1`，由 Core 在原锁序内复核资格后释放供应 hold、在全部 attempt 未授予时关闭请求并退回客户预留；不合格（409）跳过 |
| 有意保留     | `SEND_GRANTED`/`UNKNOWN` permit 按 R06 永不因时间或进程死亡释放；Gateway 在 begin 后被强杀的请求保留双方占用，等待证据或人工 resolution（T11 异步补证再处理 `recovery_interruption`）。候选只按 permit 到期筛选，安全撤销导致的提前资格在到期后同样被回收                                                                                                                                                                                                                                                                                                                |
| 角色         | `core_settlement_worker` 追加 `core_reconcile_holds_v1` 执行权与 `execution_permits(attempt_id,state,granted_at,expires_at)` 列读权；实测不能直接调用 `settle_requests`、读钱包或改 hold（42501），超界批量 P0422                                                                                                                                                                                                                                                                                                                                                        |
| 测试         | `tests/integration/core-worker-maintenance.test.ts` 4 例经真实 core-worker 进程：矛盾 FINAL 争议 → 管理 RELEASE → worker 重新结算退回预留（零额 charge）；过期未授予预留经 Core 中止，客户/供应/3 个配额租约各释放一次；v2 超额应收完整入账（1200 = 1000 funded + 200 debt）并 DEBT 冻结，现金入账后由 worker 回收；角色最小权限。`apps/core-worker/src/__test__` 补配置与周期任务单测                                                                                                                                                                                   |
| 红绿证据     | 换回 HEAD 的 core-worker 入口后前三例超时失败，恢复后 4 例通过                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| 发现         | 准入候选为空时 `orderRouteCandidates` 抛 `no_route`，内部监听把它映射为 `identity_store_unavailable`（UNKNOWN、SAME_COMMAND），应为确定性拒绝；属准入路由，在 T07 修复并补回归                                                                                                                                                                                                                                                                                                                                                                                           |
