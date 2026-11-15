# T14 三进程原生部署、产物、故障与可观测性

覆盖主规格 T14 行与 C23、C24。T14.a（原生耐久设施）已在先前交付，见[原生耐久记录](enterprise-v2-native-durability.md)。UI 资产随 T13 暂缓不执行。

## 1. 当前行为（代码核对，2026-11-15）

- 开发/测试依赖：`infra:up` 调用 `docker compose`（`infra/compose.yaml`、`compose.ha.yaml`）启动 PG/Redis；`test:container` 以 `infra/Dockerfile` 镜像运行 legacy Gateway/worker；CI 以 Docker service 提供 PG/Redis。本机 55442 端口的 PG 由 OrbStack 提供，`.env.local` 指向的 `gateway_current6` 停在 schema 2026111006。
- 产物检查：`test:artifact` 已在独立空 cwd 运行编译后的 Gateway、Core API、core-worker，覆盖 JSON/SSE、双方精确记账、未授权拒绝、SIGTERM 与 stderr 为空（10 项）。
- 耐久（n2 前）：`test:durability` 原生自有集群，但结算腿仍是 legacy 单账本（`usage-v1` + `Store.persistUsage`）；v2 的进程故障窗口由 `gateway-lifecycle.test.ts` 14 例覆盖（排空、begin 未知、Gateway/helper 强杀、worker 停止重启等），缺 core-worker 在提交前与提交后 ACK 前被强杀。
- 备份：T12 e3 已适配 v2，但校验仍有 Docker 分支并限定 55442 端口。

## 2. 决策

- **N1 原生依赖服务**：新增 `scripts/native-services.ts start|stop|status`（`infra:up`/`infra:down`/`infra:status`），用 `PG_TOOLS_DIRECTORY` 与 `DURABILITY_REDIS_SERVER` 指定的本机工具（与 `test:durability` 同一工具选择）在 `.local/native-services/` 启动 PG（initdb SCRAM，密码取自 `DATABASE_URL`，fsync/synchronous_commit/full_page_writes 开启，200 连接）、准入 Redis（AOF、noeviction）与队列 Redis（`appendfsync always`、`aof-load-truncated no`、noeviction），只监听 127.0.0.1，端口取自三个 URL。端口被非本脚本进程占用时拒绝启动；停止只作用于本脚本登记的 pid，且核对进程标识（PG 数据目录、Redis 的 `127.0.0.1:端口` 标题与 `CONFIG GET dir`）。不接管、不停止既有容器或服务。
- **N2 Docker 退役**：删除 `infra/compose.yaml`、`compose.ha.yaml`、`infra/Dockerfile`、`scripts/verify-container.ts` 与 `test:container`；`setup:local`/`.env.example` 去掉仅供 compose 的 `POSTGRES_PASSWORD`，加入工具路径示例。容器检查的有效断言迁移：独立进程、JSON/SSE、精确记账与 SIGTERM 已由 `test:artifact` 覆盖；新增“提示与回答正文不出现在 Gateway/worker 输出”（第 11 项）。只读根、非 root、cap-drop 属容器/systemd 加固，本机 macOS 无法验证，留待 Linux 部署验证。同机多副本演练由 `test:durability` 自有集群承担。
- **N3 备份校验原生化**：去掉 Docker 分支，要求绝对 `PG_TOOLS_DIRECTORY`，目标只限 loopback（不再固定 55442），子进程只得到 PATH 与 PG 连接变量。
- **N4 CI 原生化**：ubuntu-24.04 上从带 sha256 校验的源码构建 PostgreSQL 17.11（含 contrib，`schema.test.ts` 需要 `btree_gin`）与 Redis 7.4.12 并缓存，`actions/setup-go` 提供 Go 1.27.4，`infra:up` 后运行 `verify` 与 `test:durability`，最后 `infra:down`。本仓库无推送授权，CI 改动未在 GitHub 上运行。

## 3. 里程碑

| 里程碑 | 内容                                                                       | 状态   |
| ------ | -------------------------------------------------------------------------- | ------ |
| n1     | 原生依赖服务、Docker 退役、备份与 CI 原生化、容器有效断言迁移              | 完成   |
| n2     | v2 结算故障窗口：core-worker 提交前/提交后 ACK 前强杀；耐久脚本结算腿迁 v2 | 完成   |
| n3     | 内部身份轮换/伪造/过期/重放、helper 泄漏与 shutdown 覆盖核对与补齐         | 完成   |
| n4     | 部署：三服务 systemd 单元与配置样例、legacy 单元过渡标注、OpenAPI          | 完成   |
| n5     | 容量发生器纳入 Core 与上游账本                                             | 待执行 |

## 4. n1 记录

- 原生门禁：以 `.local/native-gate.env` 覆盖 `DATABASE_URL`/`REDIS_URL`/`QUEUE_REDIS_URL` 指向 n1 启动的原生服务（55452/56392/56393，`.env.local` 未改），完整 `bun run verify` 通过：单测 365、集成 605、产物 7/14/11。首次原生运行 1 例失败：`schema.test.ts` 需要 contrib 扩展 `btree_gin`，本机工具链此前未装 contrib；补装 contrib 后该文件 14 例通过、完整门禁通过。CI 构建同步加入 contrib。
- `test:durability` 原生通过 8 项（scope 仍为 legacy-single-ledger，见 n2）；原生开发库迁移到 2026111224 后 `test:backup --quiesced` 通过（钱包/上游一致性 0、队列已排空、角色重新应用）。
- 服务脚本实测：占用中的 55442（OrbStack）被拒绝且未受影响；start/status/stop/start 循环后数据保留，重复 start 幂等。

## 5. n2 记录

- **N5 强杀窗口在真实进程上验证**：新增 `tests/integration/core-worker-crash.test.ts`（真实 Core API、Gateway、core-worker 与 TLS 上游，2 例）。每个 chat 请求发布两条证据（revision 1 用量、revision 2 结局），客户收费随 revision 2 落账；被杀 worker 以 `SETTLEMENT_CONCURRENCY=1` 运行、经 `tests/support/commit-ack-proxy.ts` 连接 PG，代理以 `"attempt_state":"SUCCEEDED"` 识别 revision 2 的落账事务。新增两种保持模式：`commit` 扣住客户端 COMMIT（PG 中该会话为 idle in transaction、写入未提交），`acknowledgement` 放行 COMMIT 并吞掉应答；两者都保持连接不断，直到 SIGKILL 后关闭代理。
  - 提交前：保持期间队列为 active 1，客户收费为空；SIGKILL 并关闭代理后 PG 回滚，账本与保持期间一致；重启 worker 在约 30 秒锁过期后经 BullMQ 停滞恢复重投，收费 100 一次、回执 +1、供应商 985/0/15、客户已花 +100，队列计数全为 0。
  - 提交后 ACK 前：保持期间收费 100 已提交、供应商 985/0/15、队列仍为 active 1；重启后重投完成，收费、回执、供应商与客户钱包与提交时完全一致。
- **N6 耐久脚本迁 v2**：`scripts/verify-durability.ts` 在本轮 PG 与准入 Redis 上以 `withFinancial` 建立 v2 资金夹具，经 Core 执行 admit/begin 后签名证据组，发布到 `core-evidence-v1`，由 `createSettlementWorker` 落账；scope 由 `legacy-single-ledger` 改为 `v2-core-settlement`。保留六项：生产拓扑与 live drift、主 AOF 硬重启保留已确认证据、副本晋升后精确结算一次（客户 100、供应商 985/0/15）、缺副本拒绝准入、appendfsync everysec 拒绝准入、正文拒收与资金唯一性（1 收费、1 回执、上游日记账逐笔平衡、非法证据对应 hold 保持 HELD、供应商 985/30/15）。原脚本内的合成 worker 两窗由 N5 的真实进程回归取代，`--durability-worker` 入口删除。
- `native-durability.test.ts` 的受控 loader 原以只导出两个空类的桩替换 `@gateway/runtime`；v2 业务依赖静态导入 `evidenceChannel` 等真实导出，桩会在链接期失败而非模拟慢加载。改为在真实模块源码前插入模拟求值（慢加载或抛错），各用例意图不变。

缺陷修复（先红后绿）：

- 耐久脚本在 ready 后收到 SIGTERM/SIGINT 时留下 2 个孤儿 Core API 进程与 `/tmp/gateway-identity-*` 目录（含合成 CA 与证书）。根因：fixture 关闭本轮 Redis 后，`financial-execution.ts` 清理用的 `RedisClient` 默认自动重连，对已停端口约 31 秒才拒绝，脚本退出前身份夹具来不及停止 Core API；即便停止成功，身份夹具 `close` 在 PG 已停时 DROP ROLE 抛错，跳过了临时目录删除。修复：清理客户端 `autoReconnect: false`、`connectionTimeout: 1000`；身份夹具在 `finally` 中删除临时目录；脚本在收尾中对资金夹具自身清理做有界等待。回归：两个信号用例新增“无本轮 Core API 进程残留”“无本轮身份目录残留”断言，修复前分别得到 2 个孤儿进程与 1 个残留目录的红灯，修复后通过。

n2 全量门禁（原生服务 55452/56392/56393，`.env.local` 未改）：`verify` 退出 0，单测 365、集成 607（较 n1 增 2，均为 `core-worker-crash.test.ts`）、产物 7/14/11；`test:durability` 通过（scope `v2-core-settlement`，6 项）；`native-durability.test.ts` 17 例通过。

## 6. n3 记录

按[内部身份设计](enterprise-v2-internal-identity.md) T14 验收行逐项核对，已有覆盖记来源，缺口补测：

| 验收项                                    | 证据                                                                                                                                                                                                                                                                                                                        |
| ----------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 伪造与篡改                                | 已有：`internal-identity.test.ts`“signed command tampering and forged context…”、`core-evidence-authentication.test.ts`“a tampered signature is rejected…”                                                                                                                                                                  |
| 重放                                      | 已有：注册响应丢失跨 Core 回放同一 session、过期 session 只回放原 receipt、证据重投为 duplicate                                                                                                                                                                                                                             |
| 已连接 session 下一 stream 撤销           | 已有：“shared PG security revoke … rejects next stream on already connected Core A”                                                                                                                                                                                                                                         |
| 过期与未生效证书（时钟漂移）              | **新增**：可信 CA 签发且已预配置的过期证书、未生效证书在 TLS 握手即被拒，同法签发的有效证书得到 200；Core RPC 客户端对 pin 与 SAN 均匹配但已过期/未生效的 Core 服务端证书拒绝建流（服务端收到 0 个 stream，有效对照 >0）                                                                                                    |
| TLS 会话票据与 early data                 | **新增**：内部 TLS 服务端不发会话票据，重连只能完整握手（`isSessionReused` 均为 false）；0-RTT 需要票据派生的 PSK，因此同时排除 early data。负对照：临时去掉 `SSL_OP_NO_TICKET` 后收到 2 张票据且重连复用，用例失败；已恢复原文件                                                                                           |
| 证书/pin/CA 轮换                          | 已有：pin 或完整 SAN 不符即拒（Core 证书轮换须同步 Gateway pin）。**新增**：只信任原 CA 时拒绝新 CA 签发的对端，CA 重叠信任包同时接受新旧 CA 对端（传输层与 Core 同一 `listenBoundedH2`）；正常 RETIRE/DISABLE 链见下行                                                                                                     |
| 正常轮换后旧链证据                        | **新增**：旧 session 已签名的事实在 session DISABLE、证书 RETIRE、旧 Gateway 关闭后仍按捕获公钥验证（`VERIFIED`）并只入账一次（客户 100、供应商 985/0/15，重投为 duplicate）；经 Core B 启动的新 owner 不能 begin 旧 attempt，其为旧 attempt 签发的 NOT_EXECUTED 以 `evidence_subject_invalid` 整组拒绝、不留回执、不动资金 |
| 反向 CoreWorker→Gateway 身份              | 已有：Gateway 不能调用 worker recovery、worker 不能取插件包。**新增**：core-worker 身份不能取执行上下文、admit 或 begin（均 `internal_peer_forbidden`），客户端拒绝以 worker 身份签发证据，permit 仍归 gateway session、资金不动                                                                                            |
| 真实进程/连接强杀、跨 Core、老 owner 死亡 | 已有：`gateway-lifecycle` 14 例、`core-worker-crash`（n2）、跨 Core 注册/重连用例                                                                                                                                                                                                                                           |
| helper 泄漏                               | 已有：plugin-host 对 helper 崩溃、超时、取消、SIGSTOP 后进程组回收及 close 结清全部排队承诺的真实进程用例                                                                                                                                                                                                                   |
| shutdown                                  | 已有：Gateway SIGTERM 宽限内完成/超时 503 `draining`、worker 停止重启；`test:artifact` 对三个编译产物检查 SIGTERM 退出事件                                                                                                                                                                                                  |
| 边界/缓冲                                 | 已有：连接、stream、header、接收预算与 watchdog 用例；任务 RSS 属容量（T17），本里程碑不声称                                                                                                                                                                                                                                |

未覆盖与说明：session `expires_at` 由守卫触发器保护不可改，自然到期以设计表中等价的正常 DISABLE 路径验证，未等待真实 TTL；Core API 进程重启不参与证据结算路径，未在轮换链中重启 Core。

测试装置观察：在 `expect(...).rejects` 内直接调用 `prepareFinancialAdmission(...)` 时，Core 的拒绝应答在 3 秒客户端计时内未被处理，报 `Internal response timeout`；同一调用先发起再断言或直接 await 时 4–12 ms 收到 `internal_peer_forbidden`（独立探针复现，Core 行为正确）。用例改为 await 结果后断言。

n3 全量门禁（原生服务）：`verify` 退出 0，单测 365、集成 613（较 n2 增 6：`internal-identity.test.ts` 4、`core-evidence-authentication.test.ts` 1、`core-execution-recovery.test.ts` 1）、产物 7/14/11。n3 未改耐久路径，`test:durability` 沿用 n2 结果。

## 7. n4 部署与 OpenAPI

当前行为（代码核对，2026-11-15）：

- systemd：三服务单元与 legacy 两单元已在 T04.c c4 交付，由架构用例约束入口、非 root、加固项与用户互异。`core-api.service` 读取的 `/etc/llm-gateway/core-api.json`、`core-worker.service` 读取的 `/etc/llm-gateway/core-worker.env` 没有样例，只有 `infra/gateway.example.json`。
- OpenAPI：生成器仍在 `apps/legacy-gateway/src/openapi.ts`，`/openapi.json` 只由 legacy 网关提供，Caddy 不转发该路径（公网 404）。按实际 Hono 路由表比对：v2 Gateway 的 `POST /v1/music/generations`（T10 k3）与 Core 管理监听的 18 个上游资金路由（T12：supplier-brands、upstream-accounts 及其 recharges/balances/holds/journal/reconciliations/adjustments、financial-vouchers）不在文档中；文档中的 `POST /admin/recover` 只由 legacy 管理路由提供（[E10](enterprise-v2-events.md)，随 legacy 删除）。
- 发布文档：`reliability-and-operations.md` 仍写 Docker 镜像与 `test:container`（n1 已退役）、运行 schema `2026111202`（实际为 manifest 末项，当前 2026111224）、“其余 `/v1` 与 `/v1beta` 代理到 legacy”（T09–T12 后公开推理、资源、账户与请求证据路径已分别到 v2 Gateway 与 Core 公开监听），没有内部证书、Core pin 与 CA 的轮换步骤。

决策：

- **N7 OpenAPI 归 Core**：生成器移到 `apps/core-api/src/openapi.ts`，Core 公开监听提供 `GET /openapi.json`，Caddy 把该路径转到 Core 公开监听；legacy 删除自身的 `/openapi.json` 路由，推理与资源路由表改为 legacy 私有常量，不再从文档生成器取。不放进 `apps/gateway`：文档需要 Core 管理与插件目录的 schema，而架构约束禁止 v2 Gateway 依赖 `@gateway/core-api` 与 storage；主规格测试矩阵相应改为 `apps/core-api/src/__test__/openapi.test.ts`。上游资金请求体直接取 `@gateway/contracts` 的 schema（按输入形态导出 JSON Schema），凭证上传体取 Core 管理路由实际使用的 schema。
- **N8 路由与文档双向一致**：`tests/unit/openapi.test.ts` 取 v2 Gateway、Core 公开、Core 管理（插件目录与管理路由，配置队列时的路由一并计入）以及 legacy 管理路由的实际 Hono 路由表：每个 v2 Gateway 与 Core 路由在发布文档中有同方法条目，每个文档条目由某个运行进程提供（路径参数按段匹配，`/v1beta/models/{model}:generateContent` 对应 `/v1beta/models/:action`）。legacy 删除时文档中的 legacy 专有条目随之失败，须同步删除。
- **N9 配置样例**：新增 `infra/core-api.example.json` 与 `infra/core-worker.example.env`。Core API 样例的 pepper、加密主密钥与管理 key 留空，未填写时加载失败且只报这三个字段，样例密钥不会被原样部署；数据库 URL 中的口令为占位文本，填错只会认证失败。三个样例与 Caddyfile 的配对由架构用例核对：Core audience、内部 RPC 主机名与端口、Core 证书 SAN 与 Gateway 配置一致，worker 的 Core 字段与 Gateway 一致，worker `REDIS_NAMESPACE` 与 Gateway 队列 namespace 一致，Caddy 指向的 Core 公开端口与 Core 样例一致。
- **N10 legacy 单元标注**：`legacy-gateway@.service`、`legacy-gateway-worker.service` 的 Description 标明过渡期与随 legacy 删除；文件名与用户不变。
- 发布文档同步修正上述过期内容，新增按 n3 已验证行为编写的轮换步骤。

实施与验证：

- OpenAPI：生成器与同步用例随 `git mv` 移到 Core（`apps/core-api/src/openapi.ts`、`__test__/openapi.test.ts`），新增“Core 公开监听无需凭证返回发布文档且 `cache-control: no-store`”。文档新增 `/v1/music/generations` 与 18 个上游资金管理路由（holds/journal 的 currency、channel_id、before 查询参数，对账重放 200）；凭证上传体抽为 `@gateway/contracts` 的 `financialVoucherUploadSchema`，Core 管理路由与文档共用。legacy 去掉 `/openapi.json` 与对 `@gateway/core-api` 的依赖（锁文件同步）。Caddy `@account` 加入 `/openapi.json`。
- `tests/unit/openapi.test.ts` 2 例：负对照从发布文档删去 music 条目、再加一个无人提供的 `/v1/stale` 时两例分别只报该路径失败，重新生成后通过。legacy 专有条目当前只有 `POST /admin/recover`。
- 配置样例：Core API 样例未填密钥时只报 `admin.admin_key`、`execution.encryptionKey`、`execution.pepper`，填入后加载且两个 HTTP 监听只绑定 loopback、PG 为 `sslmode=verify-full`、Redis 为 `rediss`；worker 样例以 production 加载且 Core 恢复客户端完整。架构用例核对三个单元引用的配置名均有样例、样例间 Core 地址/证书/audience/队列配对与 Caddy 公开端口一致。负对照：样例预填 pepper、公开端口改为 8097 时对应用例失败，恢复后通过。
- 发布文档：补 Core 公开连接角色 `core_public_api`、Core 三个监听的示例端口；去掉 Docker 段落，改为原生产物与 `test:artifact`；schema 改为 manifest 末项；Caddy 路由按实际分组描述；新增“内部证书与 pin 轮换”（客户端证书、Core 服务端证书、CA 重叠信任包、私钥泄露四种步骤）。
- 未验证：本机无 systemd 与 caddy，`systemd-analyze verify`、`caddy validate` 未执行；PG `sslmode=verify-full` 只经配置加载核对，未连接真实 TLS PG。

n4 全量门禁（原生服务）：`verify` 退出 0，类型检查 316 文件、lint 0、格式 460 文件，单测 371（较 n3 增 6：Core OpenAPI 公开监听 1、Core/worker 样例各 1、路由双向一致 2、部署样例配对 1）、集成 613、产物 7/14/11。n4 未改耐久路径，`test:durability` 沿用 n2 结果。
