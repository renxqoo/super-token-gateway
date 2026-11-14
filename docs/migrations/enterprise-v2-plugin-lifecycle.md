# enterprise-v2 T08 动态插件发布全生命周期

## 1. 当前行为（代码核对）

- Core 目录只接受受控 builtin：`releaseDescriptorSchema` 与部署 policy 的 `release_id` 固定为 `gateway.openai-chat-1.0.0`，`verifyRelease` 还要求包内容等于仓库内置文件；新增任何插件都要改代码并重新部署。
- 生命周期 SQL（`plugin_catalog_command_v1`）已有 UPLOADED→VALIDATING→VALIDATED→PUBLISHED/REJECTED、租约验证、恢复、发布回执与绑定历史（`channel_plugin_bindings` 按 `config_revision` 追加）；`plugin_versions` 已有 `DISABLED` 状态与 `quarantined` 标志，但没有命令能进入它们。
- 候选查询要求 `v.state='PUBLISHED' AND NOT v.quarantined`；Gateway 经 Core `plugin-package` 按 digest 拉取包并按部署 policy 验签，因此新发布的包无需重建 Gateway 即可加载，前提是 policy 信任其签名者。
- 验证在 Core API 进程内经持久租约运行原生 helper（墙钟、slot、调用与编译期限由 `validationPolicy` 固定），有恢复命令；不在 core-worker。

## 2. 决策

- **D1 受信发布者**：部署 policy 增加可选 `publishers`（`id`、`public_key`、`not_before`、`expires_at`、`revoked`、`namespace`）。发布者可在不改 policy、不重启 Core/Gateway 的情况下发布 `<namespace>.` 前缀下任意新插件/版本；`gateway` 命名空间保留给 builtin。上传 release 的 `source` 为 `upload:<release_id>`，`release_id` 为 `<manifest.id>-<manifest.version>`；包签名与 release 签名必须来自同一发布者 key，验证策略与 runtime digest 必须等于部署值。新增发布者或撤销发布者仍按 T04.b.1 的 snapshot 规则重启生效。
- **D2 上传 API**：`POST /admin/plugins`（`command_id`、`package_base64`、`release`、`reason`），新权限 `plugins:upload`；进入同一 `plugin_register_v1` 事务（SQL 按 release source 要求 `plugins:install-bundled` 或 `plugins:upload`），后续沿用现有 validate/publish/bind。
- **D3 Gateway 信任**：`packageTrust` 对未列入 releases 的 digest 接受发布者 key 签名，验签后要求包 `manifest.id` 位于签名发布者的命名空间。
- **D4 停用与隔离**：`POST /admin/plugins/{id}/disable`（PUBLISHED→DISABLED）与 `/quarantine`（任意状态置 `quarantined`），`command_id`、`expected_revision`、`reason`，权限 `plugins:publish`，审计同事务，重放返回原回执。二者立即使新 context 排除该版本、拒绝新绑定；已 begin 的 attempt 使用捕获配置继续，已准入未 begin 的 attempt 在 begin 复核时 `candidate_stale`。包与历史不删除。
- **D5 列表与回滚**：`GET /admin/plugins` 返回版本列表；`GET /admin/providers/{id}/plugin-bindings` 返回渠道绑定历史。回滚即用 `PUT /admin/providers/{id}/plugin-binding` 绑定历史已发布版本，生成新 revision，不改旧 binding。
- **D7 进程内预算**：每个 Core API 进程同时最多 1 个上传（含请求体读取，上限 64 MiB、120 s）与 2 个新启动的验证；超出立即返回 `429 catalog_busy`，不排队、不落库。相同命令的进行中验证重放不占新槽。
- **D6 验证执行位置**：保持在 Core API 内以持久租约运行，不迁 core-worker；规格要求的 worker 拥有 VALIDATING 任务与 CPU/RSS/队列预算记为 T08 未完成项（见 §4），不以本次改动冒称已实现。

## 3. 里程碑

| 里程碑    | 交付                                                                      | 完成证据                                                                                 |
| --------- | ------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| h1 上传   | policy publishers、release 泛化、上传 API、SQL 权限与新命令、Gateway 信任 | 真实 Core：发布者上传→验证→发布→绑定；越权命名空间、错误签名者、过期发布者、无权限均拒绝 |
| h2 停用   | disable/quarantine 命令、版本列表、绑定历史                               | 停用/隔离后 context 排除、绑定被拒、审计；回滚绑定生成新 revision                        |
| h3 双网关 | 两个 Gateway 进程调用新上传插件，不重建                                   | 两 Gateway 同时成功调用新版本；停用后 `no_route`；回滚绑定后恢复                         |
| h4 验收   | 自审、文档、全量门禁                                                      | 同 T07 g4                                                                                |

## 4. 实施记录

- **h1 上传**：`releasePolicySchema.publishers`（可选，默认空）与 `packageTrust(...).admits`；`describeRelease` 按包 manifest 生成 `release_id`/`name`/`version`，`origin` 区分 `bundled`/`upload`；`verifyRelease` 对 `upload:` 走发布者路径（同一发布者 key 验包与 release、命名空间、有效期、runtime/验证策略 digest、内容重算一致），bundled 路径仍固定 `gateway.openai-chat-1.0.0`。迁移 v2026111216（`sql/core-plugin-lifecycle.sql`）以 `CREATE OR REPLACE` 更新 `plugin_catalog_command_v1`、`plugin_catalog_receipt_v1` 与 `internal_identity_command_owner_v1`：register 权限按 release source 区分 `plugins:install-bundled`/`plugins:upload`，新增 `plugin_disable_v1`/`plugin_quarantine_v1`。Gateway 包缓存在验签后要求 `trust.admits(pkg)`，发布者包只在其命名空间内被接受。Core `admin.max_body_bytes` 上限放宽到 64 MiB 以承载上传，默认仍为 8 MiB。
- **h2 停用与历史**：`POST /admin/plugins/{id}/disable|quarantine`、`GET /admin/plugins`、`GET /admin/providers/{id}/plugin-bindings`；审计动作 `plugin_disabled`/`plugin_quarantined`。回滚即再次 `PUT plugin-binding` 绑定历史已发布版本。
- **D7 预算**：上传槽 1、验证槽 2，超出 `429 catalog_busy`。
- **h3 双网关**：`tests/integration/gateway-plugin-lifecycle.test.ts` 在运行中的两个 Gateway 进程前完成上传→验证→发布→改绑，两进程各自经 Core 拉包、验签后服务新 digest（`execution_permits` 记录的 `package_digest` 均为新包）；停用后两进程均 `503 no_route` 且上游零调用；改绑回 builtin 后两进程恢复且 digest 回到原包，全程无重启。
- **测试**：`tests/integration/plugin-lifecycle.test.ts`（5）、`tests/integration/gateway-plugin-lifecycle.test.ts`（1）、`packages/plugin-sdk` 发布者信任单测（2）、Gateway 包缓存 admits 单测（1）；升级测试改为 15 个迁移 / v2026111216。
- **失败证据**：把 `application.ts`、`http.ts`、`authentication.ts`、`storage/plugins/catalog.ts` 换回 HEAD 后 lifecycle 4 例全部失败；移除两个 `throw busy()` 后 catalog_busy 用例以 `Expected 429 / Received 201` 失败；恢复后全部通过。

## 5. 未完成与偏差

- D6：验证仍由 Core API 进程以持久租约执行，不是 core-worker 拥有 `VALIDATING`；无每任务 CPU/RSS 硬限制（已有墙钟 60 s、单槽、调用 1 s、编译 5 s、guest 内存 1024 页、进程组回收）。该项需要 Linux cgroup/systemd 条件，归入 T14 资源限制验收，T08 不宣称完成。
- 隐藏 global 金丝雀、取消、trap、网络/权限拒绝沿用 T04 的 `plugin-catalog-validator`/`plugin-host` 真实 helper 证据，本次未新增。
- 自审为同一执行者的自我审查，不是独立审查。
