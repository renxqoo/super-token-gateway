# Enterprise v2 三进程与 Chat 产品路径（T04.c）实施方案

状态：用户已确认方案 A；c1 已实现，门禁结果见第 7 节。日期：2026-11-13。起点：`main` `27a154a`（T04.b.2b ⑦ 已推送）。

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
apps/gateway/src/        v2：main/assembly/config、http（Chat、健康）、execution（许可执行、重试、流、取消）
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

| 旧用例（`tests/integration/gateway.test.ts`）                  | v2 对应验收                                                                   |
| -------------------------------------------------------------- | ----------------------------------------------------------------------------- |
| 337 availability-first retry preserves unknown attempt         | next 重试：A 保持 UNKNOWN 与供应 hold，B 交付并单独结算                       |
| 353 truncated converted stream never emits success             | SSE 中断：无正常终态，PARTIAL 证据，客户不按成功收费                          |
| 798 durable queue acceptance failure withholds stream terminal | WAITAOF 失败扣住 terminal，且不重发                                           |
| 888 client cancellation releases concurrency                   | 取消：本地 grant 作废、传输等待结束、CLOSE 释放并发，不释放 SEND_GRANTED 资金 |
| 1258 independent gateway instances share idempotency           | 两个 v2 Gateway 进程同意图：只有一次 begin 胜出                               |
| 1339 crash recovery appends UNKNOWN without re-execution       | Gateway 在 begin 后被强杀：attempt UNKNOWN，重启不重发                        |
| 1867 streamed content absent from PG/Redis/queue               | 双 canary：正文不进 Core、PG、Redis、队列与日志                               |
| 2805 retry rechecks route/credential                           | next 在 Core 重新复核目录与授权                                               |
| 2899 empty chat JSON / terminal-only stream                    | 不产生成功客户收费                                                            |
| 3183 upstream address blocked before dispatch                  | 受控传输拒绝私网地址，RESERVED abort，资金释放                                |

## 6. 风险与未验证项

- 两套 Chat 实现并存至 T09；若部署未按路径分流，Chat 可能落到 legacy。c4 用部署配置与产物检查覆盖。
- 跨故障域 HA、真实供应商与容量不在本单元范围（T14、T17、T19）。
- `test:backup` 仍需 T12/T14 适配；`test:container` 按用户决定退役。

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
