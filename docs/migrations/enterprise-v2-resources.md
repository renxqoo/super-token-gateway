# T11 资源、Batch 与异步补证迁移

覆盖主规格 C06、C08–C10、C22（恢复部分）、C26 的资源侧。本文件记录现状核对、决策与里程碑；与主规格 §4.1 资源接口表不一致之处在“对设计的修订”中说明。

## 1. 当前行为（代码核对，2026-11-15）

- legacy（`apps/legacy-gateway/src/resources.ts`、`recovery.ts`、`batch-input.ts`、`batch-usage.ts`）：创建响应中的远端 `id`/`name` 写入 `resources` 表，公开 ID 为 `{kind}_{32 hex}`，表唯一键 `(project_id,provider_id,kind,remote_id)`；GET/DELETE/cancel/content/input_items 按 `provider_id` 取渠道当前配置直连上游；后台 `recoveryTick` 认领到期资源轮询，终态时按 `extractUsage` 或 Batch 输出文件聚合发布 async/batch 用量；请求体中的 `file_id`/`file_ids`/`input_file_id`/`previous_response_id` 解析为本项目资源的远端 ID 并要求同一渠道，`conversation`、`vector_store_ids`、`thread_id`、`assistant_id`、`cached_content`、`container`（字符串）、`prompt.id`、`fileData`、对象型 `file_data` 等 422 `unmanaged_resource`。
- v2：Gateway 无资源路由；Core 无资源命令；插件 ABI 有 `resource_prepare`/`resource_response` 钩子名但无输出 schema；openai-chat 插件对 Responses 的 `previous_response_id` 与 `background: true` 422，其余字段原样转发。`resources` 表已有 `binding_id` 列，v2 未写入。
- **已证实的安全缺口**：v2 chat（T09 起已切流）把客户提交的上游 `file_id` 原样转发给共享上游账号（回归用例修复前 200 且上游收到 `file-other`）。由于多个租户共用上游账号，任一租户可引用其他租户的上游文件。legacy 无此问题。
- 签名证据只接受 attempt permit 所有者 session 签名（`core-evidence-replay.sql`），Gateway 重启后的新 session 不能为旧 attempt 补证；无用户轮询的异步补证需要新的签名主体。

## 2. 决策

- **R1 引用隔离先行**：Gateway 在 Core context 之前对 JSON（含 multipart 视图）做显式栈遍历，规则与 legacy 相同；工具/函数的 JSON schema、`tool_use.input`、`functionCall.args`、`functionResponse.response` 为客户自定义载荷不检查；值为 null 的字段不视为引用。在 R3 的所有权解析落地前，`file_id`/`file_ids`/`input_file_id`/`previous_response_id` 也一律 422 `unmanaged_resource`，不再出现插件 422 `unsupported_semantics` 与宿主 422 的差异。
- **R2 存储**：沿用 `resources` 表与 `{kind}_{32 hex}` 公开 ID，使 legacy 已发放的 ID 在切流后仍有效。v2 写入的行 `attempt_id`、`binding_id` 必填；`binding_id` 指向不可变绑定行（插件版本、config、渠道），资源操作始终按该行执行，不随渠道重绑漂移。legacy 行由迁移按渠道当时的 `binding_id` 回填；渠道无绑定的行在 v2 返回 404。
- **R3 Core 资源命令**：新增执行命令 `execution_resource_commit_v1`（attempt 所有者 session 提交远端 ID、种类、状态，Core 生成或返回既有公开 ID，同一远端 ID 幂等）与 `execution_resource_v1`（客户凭证 + 公开 ID + 动作：Core 核对 Key、项目归属、模型/操作授权，返回固定的候选投影与一次性凭证材料；`delete`/`cancel` 以 command_id 记录，只在首次提交的回执上发放发送材料，重放只返回状态）。列表由 Core 直接返回公开字段，不访问上游。context 增加公开引用：Core 解析为远端 ID 并把候选限制为资源所在绑定。
- **R4 插件资源 ABI**：`resource_prepare` 输入种类、动作、远端 ID，输出方法与路径（路径由插件按协议拼接，宿主校验只含安全字符）；`resource_response` 输出状态枚举、可选用量与 finality、把远端 ID 改写为公开 ID 的编辑计划。content 动作只做宿主二进制转发（content-type 白名单），不交插件解析正文。
- **R5 异步补证由 Gateway 拉取**：Gateway 周期性向 Core 认领到期资源（`execution_resource_claim_v1`，带租约 epoch 与期限），按 R4 查询并以自身 session 签名发布用量。Core 证据入口在 attempt 所有者之外，接受持有该 attempt 资源有效租约的 session 签名，并核对租约 epoch。取代主规格“worker 调 Gateway `resource-operations`”的推送方式：Gateway 不新增入站内部端口，租约 fencing 与签名主体在同一 PG 事实上核对。
- **R6 Batch**：JSONL 校验、单模型/唯一 custom_id、端点白名单与聚合 ceiling 沿用 legacy 规则，在 Gateway 解析并以数字 manifest 交 Core；正文不进 Core。终态用量由 R5 读取输出文件聚合。
- **R7 视频**：创建按 M7 在此实现，用量（秒）只来自终态查询，经 R5 补证。

## 3. 里程碑

| 里程碑 | 内容                                                                                                                           | 验收                                                                          |
| ------ | ------------------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------- |
| l1     | R1 引用隔离                                                                                                                    | 回归用例（修复前 chat 200 且上游收到 `file-other`）；宿主遍历单测             |
| l2     | R2/R3 Core 资源登记、动作许可、列表与 context 引用解析；legacy 行回填迁移                                                      | 集成：归属/跨项目 404、受限 Key、cursor 越权、变更许可只发一次                |
| l3     | R4 + Responses：创建登记（JSON 与 SSE）、GET/DELETE/cancel/input_items/列表、`previous_response_id`；Caddy 切 `/v1/responses*` | 真实 Gateway→插件→上游；公开 ID 稳定且不暴露远端 ID                           |
| l4     | R5 异步补证与 `background: true`                                                                                               | 无用户轮询时终态用量结算；Gateway 重启后仍可补证；租约过期旧 session 证据拒绝 |
| l5     | files 与 R6 Batch                                                                                                              | Batch 现有回归迁移到 v2；完成/取消/失败用量                                   |
| l6     | R7 视频                                                                                                                        | 创建、轮询补证、content                                                       |
| l7     | 自审、全量门禁、T11 状态                                                                                                       | —                                                                             |

## 4. 实施记录

- **l1 引用隔离**：`apps/gateway/src/references.ts` 在 `requestFacts` 解析 JSON/multipart 视图后、Core context 之前执行。测试：`tests/integration/gateway-references.test.ts` 1 例覆盖 chat `file.file_id`、Responses `input_file`、`conversation`、`prompt.id`、`file_search.vector_store_ids` 均 422 `unmanaged_resource` 且上游 0 次调用（修复前首个子用例 200 且上游收到 `file-other`）；`apps/gateway/src/__test__/references.test.ts` 覆盖 schema/工具载荷豁免、内联 `file_data` 与 null、嵌套引用、10 万层嵌套不爆栈（l3 起该模块为 `collectReferences`，另含公开 ID 引用收集用例）。
- **l2 Core 资源命令**（迁移 2026111218 `core-resources.sql`）：
  - legacy 行按渠道当前 `binding_id` 回填，渠道无绑定的行保持为空（v2 对其返回 404）。
  - `execution_resource_commit_v1`（`/_internal/core/v1/resources/commit`）只接受 permit 所有者 session/incarnation/epoch 且 permit 已发放发送权，种类须与请求操作一致；公开 ID 为 `{kind}_{请求 ID 去连字符}`，与 legacy 流式 Responses 的派生方式一致，同一远端 ID 幂等返回已有 ID。
  - `execution_resource_v1`（`/_internal/core/v1/resource-permits`）分三段：无 `auth` 时核对 Key、项目归属、Key 模型可见性与动作白名单，按 Key 与渠道 RPM 限流后返回固定的解析结果（插件包摘要、绑定 config、源站、远端 ID）；带 `auth` 时下发一次凭证材料，`delete`/`cancel` 以 command_id 写入 `core_resource_actions`，同一命令重放只返回 `REPLAYED`；带 `granted_command_id` 时核对该删除授权后把资源置为 `DELETED`。
  - 插件版本被隔离、渠道停用或插件未声明 `resources` 时返回 `no_route`。
  - `execution_resource_list_v1`（`/_internal/core/v1/resources/list`）按 `(created_at,id)` keyset 分页，游标必须对当前 Key 可见，否则 404。
  - context 增加可选 `resource_ids`（≤16，仅 `response_`/`file_`）：逐个核对归属与 Key 模型可见性，要求同一渠道（否则 422 `resource_provider_mismatch`），并把候选限制为该渠道；快照与响应携带公开 ID 到远端 ID 的映射。
- **l3 Responses**：
  - ABI：输出可带 `resource {kind, remote_id}`（插件须声明 `resources`）；新增结果 `resource_request`（方法、路径、头、认证）；`resource_response` 为 JSON 文档钩子，元数据带 `action` 与 `resource_id`，此时 `model` 可省略；SSE 宿主在一次批处理内传递帧级声明，同一流重复声明或丢弃帧声明为 `SSE_FRAME`。
  - openai-chat 1.5.0：JSON 与 SSE（每帧 `/response/id`）改写为公开 ID 并声明远端 ID，流中远端 ID 变化为 `invalid_stream`；`previous_response_id` 不再由插件拒绝（宿主保证已解析）；资源动作映射到 `/v1/responses/{id}`、`/cancel`、`/input_items` 与 DELETE。
  - Gateway：请求创建的 Responses 在交付 JSON 或放出首个携带公开 ID 的 SSE 批次之前向 Core 登记，登记失败则请求失败且不交付；上游对另一请求返回已登记的远端 ID 时登记返回的是原公开 ID，Gateway 以 502 `upstream_invalid_response` 拒绝交付，不为同一远端 ID 发放第二个公开 ID；`previous_response_id` 仅顶层、仅 `response_` 公开 ID，经 context 解析后由宿主按记录的 JSON 指针逐字节改写（`rewriteJSON`，与插件编辑同一引擎），跨协议转换路径遇引用 422；资源动作路由 GET/DELETE `/v1/responses/{id}`、POST `/cancel`、GET `/input_items`、GET `/v1/responses` 列表，上游 404 映射为 404，其他失败 502 `resource_upstream_failed`。
  - Caddy：`/v1/responses` POST 进入 `@chat`，`@resources` 把 `/v1/responses` 与 `/v1/responses/*` 全部方法转到 v2，均不重放。
- **l2/l3 测试**：
  - `tests/integration/gateway-resources.test.ts` 3 例（第 3 例覆盖远端 ID 复用被拒绝且资源仍只映射到首个公开 ID）：JSON/SSE 创建得到稳定公开 ID 且全文不含远端 ID；取回改写 ID 与逻辑模型并以原渠道凭证访问远端路径；input_items、列表分页（limit=1 与 after）不触达上游；以公开 ID 续写时上游收到远端 ID 并正常结算；取消、删除后取回与续写均 404 且不触达上游；其他项目 Key、同项目受限模型 Key 取回 404 且列表为空，未知 ID 404，均不触达上游；同一 command 的取消授权第二次只得 `REPLAYED` 且无凭证；非所有者 session 登记 403 `internal_owner_mismatch` 且未写入。
  - `tests/integration/core-resources-upgrade.test.ts` 1 例：2026111217 上建立已绑定与未绑定渠道的 legacy 资源，升级后前者 `binding_id` 为渠道绑定、后者为空。
  - openai-chat fixture 60 cases/83 steps（新增身份改写、身份变化、资源请求映射、资源回复改写/类型校验、续写透传）；`apps/gateway/src/__test__/references.test.ts` 5 例。
- **l2/l3 已知限制**：资源动作只按 RPM 限流，不占并发配额；`background: true` 仍 422（l4）；资源动作不做异步补证（l4）；渠道重绑后资源仍按创建时绑定执行，未单独做可观察差异的测试。
- **l4 设计（R5 落地）**：
  - ABI 终止类型新增 `pending`：输出已交付但用量待异步终态，必须同时声明 `resource`；宿主拒绝无资源的 `pending`。openai-chat 1.6.0 对 `background: true` 放行（与 `stream: true` 同时出现仍 422），`queued`/`in_progress` 状态返回 `pending` 且不报用量。
  - Gateway 收到 `pending`：以 `ACTIVE` 登记资源，执行证据以 `attempt_state=PARTIAL` 结束且不带 `request_state`，请求保持 `PENDING`，结算对 PARTIAL attempt 返回 WAITING，预授权保留至终态；配额租约按 COMPLETED 关闭。
  - 迁移 2026111219 `core-resource-polling.sql`：`resources` 增加 `lease_session_id`、`lease_epoch`。三个 Gateway 命令：
    - `execution_resource_claim_v1`（`/resources/claim`）：认领 v2 行（attempt 有 execution permit 且仍为 `PARTIAL`、`next_poll_at` 到期且无有效租约；已被客户删除的行同样认领），`lease_epoch+1`、租约 60 秒，返回解析结果与原 attempt 的 permit epoch。认领条件以 attempt 是否已终结为准而非资源状态，终态证据丢失或被拒时会再次认领补证。
    - `execution_resource_poll_v1`（`/resources/poll-permits`）：仅当前租约持有 session 且 epoch 一致、租约未过期时下发凭证，按渠道 RPM 限流；插件隔离、渠道停用为 `no_route`。
    - `execution_resource_release_v1`（`/resources/release`）：持有者写回资源状态与退避（未终态 2^n 秒，最少 1 秒、最多 3600 秒；已终态 60 秒后仅在 attempt 仍未终结时再认领）。
  - 证据入口：`core_evidence_group_v1` 的主体核对改为“permit epoch 一致，且（permit 所有者 session，或 `async_partial`/`async_final` 事件由该 attempt 资源的当前租约 session 签名、`source_revision` 等于当前 `lease_epoch`）”。再次认领会递增 epoch，旧持有者此后入库的证据按 `evidence_subject_invalid` 拒绝并记入拒绝表。
  - legacy 轮询 `claimResources` 排除有 execution permit 的 attempt 行，v2 行只由 v2 Gateway 认领。
  - Gateway 轮询器按 `resource_poll_ms`（默认 5000）周期认领，经插件 `resource_prepare` 取回远端对象、`response` 钩子取用量：仍为 `pending` 则释放为 `ACTIVE`；完成则以自身 session 签发 `async_final`（`attempt_state=SUCCEEDED`、`request_state=SUCCEEDED`）后释放为 `COMPLETED`；上游 `failed`/`cancelled` 以 `FAILED` 结束请求；客户已删除且上游 404 时以 `FAILED`、`error_code=resource_deleted`、用量 UNKNOWN 结束请求以释放预授权；其他上游 404 或插件失败按失败退避重试。
- **l4 测试**：
  - `tests/integration/gateway-background.test.ts` 3 例（创建方 Gateway 轮询间隔 600 秒，排除其自身补证）：
    - 后台请求得到 `queued` 与公开 ID、attempt 为 PARTIAL、无扣费、资源 ACTIVE；另起 `gatewayB`（不同 session，代表重启或其他实例）认领后先得 `in_progress` 再得 `completed`，`async_final` 由 `gateway:gatewayB` 签发并 ACCEPTED，请求 SUCCEEDED 结算，资源 COMPLETED，之后不再轮询；客户全程未查询。
    - 租约：未持租约的 session 签发的 `async_final` 被拒；同一行在租约有效期内不可再认领；租约过期再认领 epoch 递增，旧 epoch 取凭证 403 `internal_owner_mismatch`、旧 epoch 证据被拒且无扣费；当前 epoch 证据 ACCEPTED 并结算；释放为 COMPLETED 后即使到期也不再认领。
    - 客户删除未完成的后台响应后，补证以 FAILED/`resource_deleted` 结束请求并结算，资源保持 DELETED。
  - 宿主：`json-guest-boundary.test.ts` 新增 `pending` 无资源被 ABI_OUTPUT 拒绝、带资源被接受；openai-chat 1.6.0 fixture 63 cases/86 steps（后台透传、流式后台 422、`queued` 延迟用量并声明身份、无公开 ID 的 `in_progress` 无效）。
- **l4 已知限制**：上游 `failed`/`cancelled` 与客户删除按 UNKNOWN 用量结束，供应商侧按预估上限处理而非真实部分用量；资源被删除以外的上游 404（远端过期）按退避无限重试，最长间隔 3600 秒；轮询只做 `response` 类，batch/video 在 l5/l6 接入；后台 + 流式不支持。
