# T14 三进程原生部署、产物、故障与可观测性

覆盖主规格 T14 行与 C23、C24。T14.a（原生耐久设施）已在先前交付，见[原生耐久记录](enterprise-v2-native-durability.md)。UI 资产随 T13 暂缓不执行。

## 1. 当前行为（代码核对，2026-11-15）

- 开发/测试依赖：`infra:up` 调用 `docker compose`（`infra/compose.yaml`、`compose.ha.yaml`）启动 PG/Redis；`test:container` 以 `infra/Dockerfile` 镜像运行 legacy Gateway/worker；CI 以 Docker service 提供 PG/Redis。本机 55442 端口的 PG 由 OrbStack 提供，`.env.local` 指向的 `gateway_current6` 停在 schema 2026111006。
- 产物检查：`test:artifact` 已在独立空 cwd 运行编译后的 Gateway、Core API、core-worker，覆盖 JSON/SSE、双方精确记账、未授权拒绝、SIGTERM 与 stderr 为空（10 项）。
- 耐久：`test:durability` 原生自有集群，但结算腿仍是 legacy 单账本（`usage-v1` + `Store.persistUsage`）；v2 的进程故障窗口由 `gateway-lifecycle.test.ts` 14 例覆盖（排空、begin 未知、Gateway/helper 强杀、worker 停止重启等），缺 core-worker 在提交前与提交后 ACK 前被强杀。
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
| n2     | v2 结算故障窗口：core-worker 提交前/提交后 ACK 前强杀；耐久脚本结算腿迁 v2 | 待执行 |
| n3     | 内部身份轮换/伪造/过期/重放、helper 泄漏与 shutdown 覆盖核对与补齐         | 待执行 |
| n4     | 部署：三服务 systemd 单元与配置样例、legacy 单元过渡标注、OpenAPI          | 待执行 |
| n5     | 容量发生器纳入 Core 与上游账本                                             | 待执行 |

## 4. n1 记录

- 原生门禁：以 `.local/native-gate.env` 覆盖 `DATABASE_URL`/`REDIS_URL`/`QUEUE_REDIS_URL` 指向 n1 启动的原生服务（55452/56392/56393，`.env.local` 未改），完整 `bun run verify` 通过：单测 365、集成 605、产物 7/14/11。首次原生运行 1 例失败：`schema.test.ts` 需要 contrib 扩展 `btree_gin`，本机工具链此前未装 contrib；补装 contrib 后该文件 14 例通过、完整门禁通过。CI 构建同步加入 contrib。
- `test:durability` 原生通过 8 项（scope 仍为 legacy-single-ledger，见 n2）；原生开发库迁移到 2026111224 后 `test:backup --quiesced` 通过（钱包/上游一致性 0、队列已排空、角色重新应用）。
- 服务脚本实测：占用中的 55442（OrbStack）被拒绝且未受影响；start/status/stop/start 循环后数据保留，重复 start 幂等。
