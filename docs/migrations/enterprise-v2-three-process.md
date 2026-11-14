# Enterprise v2 三进程与 Chat 产品路径（T04.c）实施方案

状态：用户已确认方案 A；c1–c4 已实现，门禁与审查处置见第 7–10 节。日期：2026-11-14。起点：`main` `27a154a`（T04.b.2b ⑦ 已推送）。

本记录对应 [enterprise-v2.md](enterprise-v2.md) 任务图 T04.c：三进程装配、Gateway Core client、宿主受控传输及 OpenAI Chat 产品路径，并完成 T04 表列出的独立进程、双 Gateway、耐久终态、双方账本/事件/ACK、取消与排空验收。资金、证据、身份 SQL 与 RPC 已由 T04.b 交付，本单元只消费，不改其语义；需要新增的内部契约在本文明确定义。

## 1. 当前行为（实测与代码核对）

| 范围         | 现状                                                                                                                                                                              | 依据                                                    |
| ------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------- |
| Gateway 进程 | 单体：`apps/gateway/src/main.ts` 装配 `Store`/`FundsStore`/PG/Redis 限流/耐久队列，承载全部 `/v1`、`/v1beta`、`/admin` 与恢复循环；Chat 走旧 `execute()` 与 `admit_gateway_batch` | `assembly.ts:1–80`、`app.ts:112–572`、`execution.ts:70` |
| 旧 worker    | `apps/gateway/src/worker-main.ts` 消费旧 usage 通道直写 PG                                                                                                                        | `scripts/build.ts:5`                                    |
| Core 进程    | 仅 `apps/core-api/src/identity-main.ts`：h2/mTLS 内部 listener，含 identity 与 execution RPC；没有公开管理监听，catalog 路由仍挂在 Gateway                                        | `identity-main.ts:15–44`、`app.ts:123`                  |
| 结算 worker  | `apps/core-api/src/settlement/worker.ts` 只在测试中装配，无独立进程入口                                                                                                           | 签名结算记录第 153、187 行                              |
| Core RPC     | context/admit/next/begin/abort/query/quota(ADOPT/RENEW/CLOSE/RELEASE)/恢复已实现；**没有**插件包按 digest 分发的 RPC                                                              | `contracts/core-execution.ts:13–122`                    |
| 首链         | 仅测试装配：测试代码手工注入 begin 下发的 bearer、手工 fetch、手工观察桥                                                                                                          | `tests/integration/core-first-chain.test.ts:53–145`     |
| 部署         | `infra` 仍为单体 `gateway@.service`/`gateway-worker.service` 与容器文件                                                                                                           | `infra/`                                                |

## 2. 关键决策

### D1 未迁协议的过渡（需用户确认）

T04 验收要求 Gateway 无 PG，而其余 24 项公开能力分别在 T05–T11 迁移。三种做法：

| 方案                    | 内容                                                                                                                                                     | 后果                                                                                                                                                                  |
| ----------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **A（推荐）绞杀式并行** | 新建无 PG 的 v2 Gateway 产品进程，本单元只承载 Chat、`/livez`、`/readyz`；旧单体机械移到过渡入口 `apps/legacy-gateway`，行为与测试不变，继续承载未迁能力 | Gateway 无 PG 可由产物与架构测试实证；旧单体是有明确退出条件的过渡实现：T05–T11 每迁一项即从 legacy 删除对应路由，T16 删除整个 legacy 应用、旧 worker 与旧 SQL writer |
| B 本单元全量切换        | 所有路由立即改走 Core                                                                                                                                    | 实际是提前做 T05–T11，范围失控                                                                                                                                        |
| C 同进程混合            | Chat 走 Core，其余仍直连 PG                                                                                                                              | 违反“Gateway 无 PG”验收，不可接受                                                                                                                                     |

A 下 Chat 在过渡期存在两条实现：legacy Chat 继续作为 T05/T09 迁移前旧资金回归的载体，但部署配置只把 `/v1/chat/completions` 路由到 v2 Gateway；legacy Chat 在 T09 迁完 C01 后删除。“T04.c 撤权切换”按 A 解释为：v2 三进程的部署角色不授予 Gateway 任何 PG 权限，worker 只得 Core settlement 角色；legacy 角色只服务 legacy 进程，T16 删除。

### D2 其余已定的工程选择（按推荐执行）

1. **进程入口**：`apps/core-api/src/main.ts`（内部 h2 listener + 公开管理监听，catalog/management 路由从 Gateway 迁到这里）、`apps/core-worker/src/main.ts`（结算 worker，从 `apps/core-api/src/settlement` 移入）、`apps/gateway/src/main.ts`（v2）。`identity-main.ts` 合并入 Core API 入口后删除，调用方与产物脚本同步。
2. **插件包分发**：新增内部 RPC `plugin_package_v1`：输入 digest，返回 Core 已 PUBLISHED 的签名包字节；Gateway 按 digest 缓存、验签后再加载，缺包或校验失败拒绝执行，不回退本地默认（规格 §8 末段）。
3. **宿主受控传输**：复用 `runtime` 的 `network`/`http-pool`/`ip` 能力，宿主按 begin 返回的 `transient_send` 注入认证，DNS/IP 拒私网、禁止跳转、header/总/idle/下游停滞期限；插件只拿 prepare 的方法/路径/头/体，拿不到凭证。
4. **本地发送 registry**：每个 attempt 只有一次消费；begin 回执迟到或本地已关闭时不发送。取消/排空先同步作废未消费 grant、停止并等待自有传输，再 quota CLOSE（core-execution §“trusted Gateway host” 段）。
5. **耐久终态**：观察桥事件经 `evidenceChannel` 同连接 WAITAOF；JSON 在响应前、SSE 的正常 terminal 在 publish 成功后才放行，失败表现为错误/截断。
6. **开发入口**：不在 v2 Gateway 内转发 legacy 路由；开发与测试分别连接各进程端口，`Caddyfile` 示例同步按路径分流。

## 3. 目标结构与依赖

```text
apps/gateway/src/        v2：main（装配与排空）、config、app（Chat 与健康路由）、chat（许可执行、重试、JSON/SSE 中继）、packages（插件包信任与缓存）
apps/core-api/src/       main/assembly/config；既有 identity/execution/plugins/platform
apps/core-worker/src/    main/assembly/config、settlement
apps/legacy-gateway/src/ 现单体原样移入，过渡期唯一 PG 直连 Gateway
packages/core-client     增 plugin_package 调用，不做全量 Store 代理
```

架构测试新增：`apps/gateway` 不得依赖 `@gateway/storage`、`@gateway/core-api`、`DATABASE_URL`/`KEY_PEPPER` 配置；`apps/core-worker` 不得依赖 Gateway 私有模块；legacy 不得被其他应用导入。

## 4. 里程碑

| 里程碑              | 交付                                                                                                                                                                             | 完成证据                                                                                                           |
| ------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| c1 进程拆分         | legacy 机械迁移（路径与产物名变化，行为零变化）；Core API 主入口含公开管理监听；core-worker 独立入口；三者 config/readiness/drain；build 产物与 `test:artifact` 入口更新         | 全量门禁；legacy 测试数与迁移前一致；三产物空 cwd 启动/就绪/排空检查                                               |
| c2 Gateway 执行核心 | v2 assembly（无 PG）、core-client 启动 session、`plugin_package_v1`、宿主受控传输、发送 registry、context→prepare→admit→begin→send→observe→publish→CLOSE；next 重试；JSON 与 SSE | 多进程集成：客户与上游已充值→一次发送→同连接 WAITAOF→PG 证据/双方账本/事件→ACK；上游收到的凭证、模型映射、输出上限 |
| c3 故障与生命周期   | 取消、排空、grant 丢包 UNKNOWN、worker 暂停/恢复、helper 强杀/重启不重发、两 Gateway 重复意图、截断流与空终态、私网地址拒绝                                                      | 每项真实进程用例；修复缺陷先红后绿                                                                                 |
| c4 部署与验收       | 原生 systemd 三进程单元与最小权限角色、Caddyfile 分流、OpenAPI/文档同步、独立审查及处置                                                                                          | 全量门禁含 `test:durability`；审查发现逐条处置                                                                     |

每个里程碑结束运行全部门禁并本地提交，推送需另行指示。

## 5. 测试迁移矩阵

legacy 测试原样保留，作为未迁能力的回归，删除随各自迁移任务。下列旧 Chat 断言的语义在 v2 路径以新用例重新验证（期望来自规格与手算，不复用 legacy 实现）：

| 旧用例（`tests/integration/gateway.test.ts`）                  | v2 对应验收                                                                                          |
| -------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| 337 availability-first retry preserves unknown attempt         | next 重试：A 保持 UNKNOWN 与供应 hold，B 交付并单独结算                                              |
| 353 truncated converted stream never emits success             | SSE 中断：无正常终态，PARTIAL 证据，客户不按成功收费                                                 |
| 798 durable queue acceptance failure withholds stream terminal | WAITAOF 失败扣住 terminal，且不重发                                                                  |
| 888 client cancellation releases concurrency                   | 取消：本地 grant 作废、传输等待结束、CLOSE 释放并发，不释放 SEND_GRANTED 资金                        |
| 1258 independent gateway instances share idempotency           | 两个 v2 Gateway 进程同意图：只有一次 begin 胜出                                                      |
| 1339 crash recovery appends UNKNOWN without re-execution       | Gateway 在 begin 后被强杀：attempt UNKNOWN，重启不重发                                               |
| 1867 streamed content absent from PG/Redis/queue               | 双 canary：正文不进 Core、PG、Redis、队列与日志                                                      |
| 2805 retry rechecks route/credential                           | next 在 Core 重新复核目录与授权                                                                      |
| 2899 empty chat JSON / terminal-only stream                    | 不产生成功客户收费                                                                                   |
| 3183 upstream address blocked before dispatch                  | 私网字面量地址在准入前拒绝、不预留资金；解析到私网的主机名在 begin 后由受控传输拒绝并记 NOT_EXECUTED |

## 6. 风险与未验证项

- 两套 Chat 实现并存至 T09；若部署未按路径分流，Chat 可能落到 legacy。c4 用部署配置与产物检查覆盖。
- 跨故障域 HA、真实供应商与容量不在本单元范围（T14、T17、T19）。
- `test:backup` 仍需 T12/T14 适配；`test:container` 按用户决定退役。
- 遗留请求回收已在 T05 e3 交付（见[客户资金记录](enterprise-v2-customer-funds.md) §8）：core-worker 中止已过期、从未授予的 RESERVED 预留并完成 RELEASE_PENDING 配额释放。Gateway 在 begin 后被强杀的 `SEND_GRANTED` 请求按 R06 保留双方占用，不自动释放。
- 证据写入队列后 WAITAOF 未确认（副本落后或超时）时，Gateway 无法判断证据是否会被结算：JSON 返回 503 `usage_queue_unavailable`（提示先查询原请求再重试），SSE 以 `gateway_stream_error` 结束且无 `[DONE]`，但 worker 仍可能结算已入队的证据，客户可能已被收费。真实进程用例只覆盖了队列整体断开（未写入）的情形。
- 续租失败一次即停止；attempt 之间（换渠道时的 context 与插件包下载）不续租，耗时超过 30 秒时下一次 next 可能因租约过期而失败。

## 7. c1 进程拆分记录

| 项                  | 实施                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| ------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| legacy 迁移         | `apps/gateway` 原样移到 `apps/legacy-gateway`，包名 `@gateway/legacy-gateway`，产物 `dist/legacy-gateway`；调用方（测试、脚本、`infra` 单元、文档）同步改路径。`apps/gateway` 留给 c2 的 v2 Gateway                                                                                                                                                                                                                                                                                                                                                                   |
| catalog 迁出 legacy | legacy 删除 catalog 装配、路由及 `PLUGIN_RELEASE_*` 环境变量；`/admin/plugins*` 与 `/admin/providers/{id}/plugin-binding` 改由 Core API 管理监听提供。legacy `openapi.ts` 仍从 `@gateway/core-api/catalog` 取 schema 描述整体公开契约，T14 统一 OpenAPI 时迁走                                                                                                                                                                                                                                                                                                        |
| Core API 入口       | `identity-main.ts` 并入 `apps/core-api/src/main.ts`，配置从 `config.ts` 读取，环境变量改名 `CORE_API_CONFIG_FILE`。新增可选 `admin` 段：独立 `database_url`（catalog/management 角色）、监听地址、`admin_key`、`max_body_bytes`、`shutdown_grace_ms`、可选 `plugins`（发行策略/目录/helper）。`admin` 必须与 `execution` 同时配置，复用其 pepper 与加密主密钥，不在配置中重复密钥。管理监听提供 `/livez`、`/readyz`（停机中或 PG 不可达为 503）、Core management 与 catalog 路由；SIGTERM 先停管理监听（有界等待在途请求）、关闭 catalog 与 helper，再停内部 listener |
| Core worker 入口    | 结算 worker 从 `apps/core-api/src/settlement` 移到 `apps/core-worker/src/settlement`；新增 `config.ts`（环境变量 `DATABASE_URL`、`DB_POOL_SIZE`、`QUEUE_REDIS_URL`、`QUEUE_ACK_TIMEOUT_MS`、`REDIS_NAMESPACE`、`SETTLEMENT_CONCURRENCY`、`HOST`、`WORKER_PORT`、`SHUTDOWN_GRACE_MS`）与 `main.ts`：启动核对 schema 版本，健康端点 `/livez`、`/readyz`（含 committed/rejected/quarantined/failures 计数），SIGTERM 暂停消费、有界等待在途组提交后退出 0。指标鉴权端点不在 c1 引入，随 T14 可观测性                                                                     |
| 产物                | `scripts/build.ts` 产出 `dist/core-api/main.js`、`dist/core-worker/main.js`；`test:artifact` 的 catalog 检查改为启动两个编译后的 Core API（真实 mTLS 身份）并经其管理监听完成供应商创建、15/24 验证、发布、绑定与 SIGTERM 零资源退出；execution 检查追加在空 cwd 启动编译后的 worker，结算签名事实并核对手算金额                                                                                                                                                                                                                                                      |

有意差异：Core management 的 `/admin/providers` 只接受 HTTPS base_url（T04.b 已定），catalog 产物检查的合成供应商由 `http://127.0.0.1:1/v1` 改为 `https://127.0.0.1:1/v1`，检查不向该地址发请求。

新增验证：

- `apps/core-api/src/__test__/config.test.ts`、`apps/core-worker/src/__test__/config.test.ts`：缺执行配置的管理监听、未知字段、短 admin key、越界命名空间与并发被拒；错误只列字段名，不回显密钥。
- `tests/unit/architecture.test.ts`：Core API/worker/mock 不得引用 legacy、彼此私有模块或 Gateway 专属包；植入违规 import 时该用例失败，移除后通过。
- 产物：execution 检查 14 项（编译后 worker 结算 1 组：客户 100，上游余额 985/预留 0/已花 15，SIGTERM `committed:1` 退出 0、无 stderr）；catalog 检查通过 7 项。

c1 全量门禁：check 238 文件无类型错误；lint 0；fmt 344 文件；test:native 通过；test 297 通过 / 4177 断言 / 43 文件（较 ⑦ 的 291 增 6：两份配置用例 5 条、架构 1 条）；test:integration 501 通过 / 0 失败 / 10755 断言 / 34 文件（与迁移前用例数一致，legacy 行为未变）；build 成功；test:plugin-artifact 15/24；test:artifact 四项全部通过（legacy、catalog、identity 7、admission+worker 14）；test:durability 通过。test:backup 未通过的原因同 T04.b.2b；test:container 已退役，未运行。

## 8. c2 Gateway 执行核心记录

| 项           | 实施                                                                                                                                                                                                                                                                                                                                                                                                            |
| ------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 插件包分发   | 追加迁移 v2026111210 `core_plugin_package_v1`（SECURITY DEFINER，仅 `gateway` 服务的 `plugin_package_v1` 命令、复核当前 session，只返回 PUBLISHED 且未隔离版本引用的包），`core_execution_api` 获 EXECUTE。内部 RPC 新增 `/_internal/core/v1/plugin-package`，以 `application/octet-stream` 回包（上限 44 MiB），错误仍为 JSON；`plugin_package_unavailable` 映射 404。core-client 新增 `pluginPackage(digest)` |
| 包信任       | 发行策略 schema 与 `bundledReleaseId` 从 Core 私有模块移到 `@gateway/plugin-sdk`（Core、脚本、测试改为从此导入，不留转发）。新增 `packageTrust(policy)`：按摘要只给出未撤销发行所绑定、在有效期内且用途含 `package` 的 Ed25519 key；Gateway 用自己的部署策略校验 Core 下发的包，摘要不在策略内时不下载，下载结果摘要不符即拒绝                                                                                  |
| Gateway 进程 | `apps/gateway`：`GATEWAY_CONFIG_FILE` JSON 配置（Core mTLS pin、证据队列、helper、信任策略、私网放行仅限非 production），production 禁止私网放行且要求证据至少一个已确认副本。无 PG、无 `DATABASE_URL`/`KEY_PEPPER`。启动建立 Core session 与证据 producer；`/readyz` 复核 session 与队列；SIGTERM 停止接收、有界排空在途请求、撤销未消费发送许可后退出 0                                                       |
| Chat 路径    | context→插件 prepare→admit→begin→受控发送（注入 Core 下发的上游凭证）→observe→签名证据 publish→关闭请求。JSON 在证据持久化后才返回；SSE 在终止帧前持有，证据持久化后才放出终止帧。失败前的中间 attempt 只发不带 `request_state` 的证据，不关闭请求                                                                                                                                                              |
| 重试         | 沿用 legacy 的可重试状态（408/409/429/500/502/503/504/529 及 401/403/404 换渠道）与传输失败换渠道；每次重试取新 context 并选未尝试过的渠道，经 `execution_next_v1` 续接同一请求，序号与次数由 Core 约束。客户端取消、请求截止或不可重试状态立即结束。整个请求只由最后一个 attempt 发一次 CLOSE（或 begin 前的 abort 带 `close_request`）                                                                        |
| 运行时缺陷   | `createTransport` 对 IP 字面量 HTTPS 上游把地址作为 TLS ServerName 传给 node，被拒为 `ERR_INVALID_ARG_VALUE`，配置为 IP 地址的 HTTPS 供应商无法连接（legacy 同受影响）。修复为 IP 字面量不发 SNI、证书按 IP 校验；回归用例先红后绿                                                                                                                                                                              |
| 产物         | `scripts/build.ts` 产出 `dist/gateway/main.js`；新增 `scripts/verify-gateway-artifact.ts` 并入 `test:artifact`：编译后的 Core API、worker、Gateway（空 cwd）真实三进程完成 JSON 与 SSE 各一次并核对手算金额                                                                                                                                                                                                     |

有意差异与说明：

- 证据桥新增 `notExecuted`：只能在尚未观察到任何用量时以 `NOT_EXECUTED` 终结，用于发送前即失败（未拿到发送许可、连接前失败）；`AttemptOutcome` 不再接受 `NOT_EXECUTED`，避免带用量的 attempt 声称未执行。
- 失败请求按 REQUEST_COMPLETION 计费基础结算为 0 原子的客户 charge 行、`billing_outcome` 为 FAILED，而非没有 charge 行。
- 上游非 2xx 的证据：400/401/403/404/422 记 `FAILED`，其余记 `UNKNOWN`（保守保留供应商敞口），错误码 `upstream_http_<status>`；公开错误码沿用 legacy 的 `upstream_rejected`。
- 公开错误响应的 `x-request-id` 与 Core 记录的请求 ID 一致。

新增验证：

- `tests/integration/gateway-chat.test.ts`（真实 Core API、worker、Gateway 三进程与 TLS 上游）：JSON 与 SSE 各一次发送，客户 100、上游余额 985/预留 0/已花 15、最新 CANONICAL 快照为 FINAL usage 30/15 成本 15（SSE 每个证据修订追加一个快照，结算只取最新），上游收到 Core 下发凭证、模型映射与输出上限；503 后换到第二渠道同一请求成功、仅收费一次；400 不重试、请求 FAILED 关闭且客户 charge 为 0。
- `tests/integration/core-plugin-package.test.ts`：Gateway session 下载的包经部署策略 `packageTrust` 校验等于候选摘要；未知摘要 404、worker 身份 403、隔离后 404。
- `packages/plugin-sdk/src/__test__/release-policy.test.ts`、`apps/gateway/src/__test__/packages.test.ts`、`apps/gateway/src/__test__/config.test.ts`、`packages/plugin-host/src/__test__/evidence.test.ts`（`notExecuted` 两条）、`packages/runtime/src/__test__/network.test.ts`（IP 字面量 SNI 回归）。
- `tests/unit/architecture.test.ts`：Gateway 的工作区依赖闭包不含 storage/Core/legacy/数据库驱动，源码只读取 `GATEWAY_CONFIG_FILE` 环境变量；植入违规 import 或 `DATABASE_URL` 读取时用例失败，移除后通过。

未在 c2 验证（已由 c3 补齐，见 §9）：取消与排空中的请求、grant 丢失、worker 暂停、helper 强杀、双 Gateway、截断流、私网拒绝的真实进程用例；集成测试的证据队列为 0 副本，production 的副本确认要求只由配置校验覆盖。

c2 全量门禁：check 251 文件无类型错误；lint 0；fmt 358 文件；test:native 通过；test 312 通过 / 4261 断言 / 46 文件（较 c1 增 15：发行策略 3、Gateway 包缓存 5 与配置 3、证据桥 2、传输 1、架构 1）；test:integration 507 通过 / 0 失败 / 10805 断言 / 36 文件（增 6：Gateway Chat 4、插件包分发 2）；build 成功；test:plugin-artifact 15/24；test:artifact 五项全部通过（legacy、catalog、identity 7、admission+worker 14、Gateway 三进程 10）；test:durability 通过。test:backup 未运行，原因同 c1；test:container 已退役。首轮全量门禁中 SSE 用例断言“每请求仅一行 usage_events”失败，核实为结算按证据修订追加 CANONICAL 快照的既定设计，断言改为核对最新快照后该文件与整套集成重跑通过。

## 9. c3 故障与生命周期记录

| 项             | 实施                                                                                                                                                                                                                                                                                                                            |
| -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 取消与排空     | 请求信号合并客户端断开、Gateway 排空、请求截止与本地放弃。begin 前取消以 `execution_abort_v1`（`close_request`）结束；发送后取消或排空记 attempt `PARTIAL`（已向客户端交付字节）或 `UNKNOWN`，保留供应商敞口。超出宽限期仍在途的请求回 503 `draining`，CLOSE 原因 `DRAINED`、请求 FAILED；客户端取消的 CLOSE 原因为 `CANCELLED` |
| CLOSE 与 abort | 两者使用稳定 command_id，在 5 秒截止内对可重试错误（含 `identity_busy` 与传输失败）以同一命令重试，`retryability` 为 `NONE` 时停止。Core 对重复 CLOSE 返回原回执并再次完成租约释放                                                                                                                                              |
| 配额续租       | 发送许可消费后按 `quota_renew_ms`（默认 10000，范围 100–20000）对本 attempt 发 RENEW，失败即停、请求结束时停止，避免 30 秒租约在长请求中过期                                                                                                                                                                                    |
| begin 结果未知 | begin 回执丢失时按同一 command_id 以 `execution_query_v1` 查询（最多 5 次）：命令不存在则 abort `PREPARE_DISPOSED`；已授予发送则不使用该许可，发布 `NOT_EXECUTED` 证据（`begin_outcome_unknown`）并 CLOSE FAILED；仍未知则保持原状。三种情况均回 503 `core_unavailable`，不向上游发送                                           |
| 流式失败       | SSE 在上游截断、证据无法持久化或 helper 失效时不放出 `[DONE]`，改为发送与 legacy 相同的 `gateway_stream_error` 事件后结束；客户端自行断开时不再写入。最终证据为 `PARTIAL`（已交付）或 `UNKNOWN`，错误码 `stream_interrupted`、`client_cancelled` 或 `gateway_draining`                                                          |
| 私网地址       | 渠道 base_url 为私网字面量时在准入前由 `validateBaseUrl` 拒绝，回 502 `unsafe_upstream`，不创建请求、不预留资金；主机名在发送时解析到私网时由受控传输拒绝，attempt 记 `NOT_EXECUTED`、错误码 `unsafe_upstream`，不重试                                                                                                          |

有意差异与说明：

- 被重试替代的 attempt 以及发送后被取消的 attempt，供应商 hold 为 `PARTIAL` 而非 `UNKNOWN`，预留按 ceiling 保留（手算 ceil(1000/3 + 30/3) = 344）；客户只按成功 attempt 收费一次。
- 矩阵 3183 原写“RESERVED abort”；实现对私网字面量地址在准入前拒绝，比 RESERVED 后 abort 更早且无资金变动，只有主机名解析路径在 begin 后拒绝。
- Gateway 在 begin 后被强杀时，permit 保持 `SEND_GRANTED`、供应商 hold 为 `HELD`、请求未关闭，重启的 Gateway 不重发。此类请求按 R06 保留占用，不由回收流程释放；未授予的过期预留由 core-worker 回收（T05 e3）。
- 启动时证据队列无法确认所需副本，Gateway 拒绝启动（`usage_queue_unavailable`）；运行中队列断开时 `/readyz` 转为 503。

缺陷修复（先红后绿）：

- CLOSE 遇到 Core `identity_busy`（与结算 worker 争用行锁时的 `NOWAIT` 冲突）时，c2 吞掉错误且不重试：Core 已提交关闭，但租约停在 `RELEASE_PENDING`。客户端断开用例在单次 CLOSE 下稳定失败（等待 “released quota leases” 超时），改为同命令重试后通过。
- c2 缺少配额续租、把排空记为 CANCELLED、SSE 失败时直接断连、begin 结果未知时资金滞留，均按上表实施，并由下列用例覆盖。

新增验证 `tests/integration/gateway-lifecycle.test.ts`（真实 Core API、worker、Gateway 与 TLS 上游，共 14 条）：

- 截断流：发出错误事件、无 `[DONE]`、attempt 为 PARTIAL、客户收费 0。
- 空 choices、缺 message、空 content、未结束的 choice、仅含终止的流，共 5 种均不按成功收费。
- 无副本时拒绝启动；运行中 Redis 断开时扣住终态，且不重发。
- 私网字面量地址在准入前被拒绝。
- worker 停止期间证据留在队列，重启后结算一次。
- 客户端断开：上游被取消，租约为 RELEASED，hold 为 PARTIAL，预留 344。
- SIGTERM 宽限期内完成在途请求，并拒绝新请求。
- 超出宽限期的请求回 503 `draining`，租约被释放。
- 两个 Gateway 收到相同 Idempotency-Key 时只发送一次、只收费一次。
- Gateway 被强杀后重启，不重发。
- begin 回执丢失（COMMIT 应答被代理丢弃）：查询后不发送，记 NOT_EXECUTED，供应商预留归零。
- helper 被强杀：流失败且不重发，下一个请求由新 helper 完成。
- prompt 与输出 canary 不进入 PG、Redis、队列和进程日志。
- 长请求续租：租约修订递增，请求关闭后释放。

其他测试变更：

- `gateway-chat.test.ts` 的重试用例补充 permit/hold 状态，以及供应商余额 985、预留 344、已花 15。
- 测试辅助：`tests/support/commit-ack-proxy.ts` 支持按 SQL 文本触发；`tests/support/gateway-process.ts` 新增三进程栈 `withGatewayStack`、TCP 断开代理和进程日志读取。

c3 全量门禁：check 252 文件无类型错误；lint 0；fmt 359 文件；test:native 通过；test 312 通过 / 4261 断言 / 46 文件（c3 无新增单测，生命周期行为全部以真实进程验证）；test:integration 521 通过 / 0 失败 / 11043 断言 / 37 文件（较 c2 增 14，均为 `gateway-lifecycle.test.ts`）；build 成功；test:plugin-artifact 15/24；test:artifact 五项全部通过（Gateway 三进程 10）；test:durability 通过。test:backup 未运行，原因同 c1；test:container 已退役。

## 10. c4 部署与验收记录

| 项         | 实施                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| ---------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| systemd    | 新增 `infra/core-api.service`、`infra/core-worker.service`、`infra/gateway@.service`（v2，`GATEWAY_CONFIG_FILE=/etc/llm-gateway/gateway-%i.json`，不读任何 env 文件）；原单体单元改名为 `legacy-gateway@.service`、`legacy-gateway-worker.service`，env 文件改为 `legacy-%i.env`、`legacy-worker.env`。五个单元使用五个不同的低权限用户。v2 三个单元在原有加固基础上增加 `PrivateDevices`、`ProtectKernelLogs`、`ProtectClock`、`ProtectHostname`、`RestrictNamespaces`、`RestrictRealtime`、`LockPersonality`、空 `CapabilityBoundingSet` 与地址族限制；不设 `MemoryDenyWriteExecute`（Bun JIT 需要可写可执行内存），Go helper 不使用命名空间或 seccomp。三个进程运行时均不写文件，`ProtectSystem=strict` 成立 |
| 数据库角色 | 新增 `infra/core-settlement-role.sql`（`core_settlement_worker`）：只读 `schema_migrations.version` 与 `internal_startup_sessions` 的 session_id/公钥/公钥摘要三列，只执行 `persist_core_evidence_groups_v1`。v2 Gateway 没有数据库账号                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| Caddy      | POST `/v1/chat/completions`、`/v1/messages`、`/v1beta/models/*`（T09 j7a 起）与 `/v1/embeddings`、`/v1/moderations`、`/v1/completions`（j7b 起）、图片与音频五个路径（T10 k1/k2 起）只代理到 v2 Gateway（127.0.0.1:8090/8091），`/v1beta/models` 与 `/v1/models` 同到 Core 公开监听，其余 `/v1`（含 T11 前的 `/v1/responses`）与 `/v1beta` 代理到 legacy（8088/8089）；两段都禁止透明重放 POST；管理与内部 RPC 不公开                                                                                                                                                                                                                                                                                           |
| 示例配置   | `infra/gateway.example.json`：production、禁止私网、证据队列 2 个确认副本、TLS Redis、与 worker 默认一致的 namespace `stg`。Gateway 配置加载时按 Core client 的同一规则校验 `core.audience`（`coreAudiencePattern` 由 core-client 导出），不再到启动时才失败                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| OpenAPI    | 200 响应说明补充：流在响应头之后失败时以 `gateway_stream_error` 事件结束、没有正常终止帧（legacy 与 v2 行为一致）。v2 的错误状态码均在既有列表内，未新增                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| 文档       | `reliability-and-operations.md` 生产发布步骤改为三进程加过渡 legacy 的部署：每个进程的数据库角色、配置文件与权限、五个用户、启动依赖、证据队列 namespace 与 audience 配对                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |

独立审查（只读子代理，范围 c1–c4，结论逐条处置）：

| 发现                                                           | 级别 | 处置                                                                                                                                                                                                                                                                                                                     |
| -------------------------------------------------------------- | ---- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| WAITAOF 未确认时客户可能被收费而客户端看到错误                 | 高   | 部分修复：JSON 原先把队列失败报成 502 `upstream_invalid_response`（插件观察阶段发布证据失败被当作上游读取失败），改为原样返回队列的 503 `usage_queue_unavailable`（提示先查询原请求），回归用例先红后绿。“已入队但未确认”的结果本质上不可判定，不改为 CLOSE FAILED（Core 只记录 CLOSE 原因，结算以证据为准），在 §6 记录 |
| admit/next 结果未知时预留资金无人释放                          | 中   | 修复：按同一 command_id 用 `execution_query_v1` 查询，已提交且仍为 RESERVED 时以 `close_request` abort；回归用例（丢弃 admit 的 COMMIT 应答）在无修复时请求始终不关闭，修复后请求 FAILED、双方预留归零、permit ABORTED                                                                                                   |
| begin 被明确拒绝时不 abort                                     | 中   | 修复：查询结果为 REJECTED 或 permit 仍为 RESERVED 时与 NOT_FOUND 一样 abort。该路径需要“begin 被拒绝且应答丢失”同时发生，没有真实进程用例，只经代码核对                                                                                                                                                                  |
| v2 Gateway 与 legacy 共用 Unix 用户                            | 中   | 修复：legacy 改用 `llm-legacy-gateway`、`llm-legacy-worker`；架构用例改为要求所有单元用户互不相同                                                                                                                                                                                                                        |
| 示例配置的 audience 会在启动时被拒，namespace 与 worker 不一致 | 中   | 修复：配置 schema 校验 audience，示例改为 `core.example/core-api` 与 `stg`，文档说明配对；新用例在旧 schema 下失败                                                                                                                                                                                                       |
| `stream_finish` 结果被忽略                                     | 低   | 不改：读取循环已 drain 到插件要求更多输入，结束时 framer 为空，未终止的末帧会抛错而非丢弃                                                                                                                                                                                                                                |
| 一次续租失败即停止、attempt 之间不续租                         | 低   | 不改，记入 §6                                                                                                                                                                                                                                                                                                            |
| OpenAPI 说明适用于全部端点、未列 v2 错误码                     | 低   | 不改：说明以“流”为条件；OpenAPI 只列状态码，v2 状态码均在列表内，499 不会被客户端收到                                                                                                                                                                                                                                    |
| SIGTERM 宽限期后 Core client 关闭早于慢客户端流的 CLOSE        | 低   | 不改，与无回收进程的风险合并记入 §6                                                                                                                                                                                                                                                                                      |

新增与修改的验证：

- `tests/integration/gateway-chat.test.ts`：所有 `withGatewayStack` 用例（Chat 5、生命周期 15）的 worker 都以 `core_settlement_worker` 派生的登录角色运行；新增用例确认该角色能完成结算，但读 wallets、gateway_requests、api_keys、channels、启动 session 的 boot_nonce，改上游余额，执行 `core_execution_batch_v1`、`core_quota_close_v1` 时均被拒绝。`test:artifact` 的编译产物三进程检查同样以该角色运行 worker。
- `tests/integration/gateway-lifecycle.test.ts`：新增 admit 应答丢失用例；队列断开用例补充 JSON 请求返回 503 `usage_queue_unavailable`。
- `tests/unit/architecture.test.ts`：单元的 `ExecStart` 必须是构建脚本产出的入口、非 root、`NoNewPrivileges`、`ProtectSystem=strict`、用户互不相同；v2 Gateway 单元不读 env 文件；Caddy 的 Chat 段先于 legacy 段且只指向示例配置的 v2 端口，不公开 `/admin` 与 `/_internal`。植入错误入口或把 Chat 指向 legacy 端口时用例失败。
- `apps/gateway/src/__test__/config.test.ts`：发布的示例配置能加载且保持 production 约束；Core client 会拒绝的 audience 在加载时被拒。

c4 全量门禁（审查处置后重跑）：check 252 文件无类型错误；lint 0；fmt 360 文件；test:native 通过；test 315 通过 / 4314 断言 / 46 文件（较 c3 增 3：部署一致性 1、示例配置 1、audience 1）；test:integration 523 通过 / 0 失败 / 11055 断言 / 37 文件（增 2：结算角色边界、admit 应答丢失）；build 成功；test:plugin-artifact 15/24；test:artifact 五项全部通过（Gateway 三进程 10，worker 以结算角色运行）；test:durability 通过。test:backup 未运行，原因同 c1；test:container 已退役。本机无 caddy 与 systemd，`caddy validate`、`systemd-analyze verify` 未执行，单元与 Caddyfile 只经架构用例做静态一致性核对。
