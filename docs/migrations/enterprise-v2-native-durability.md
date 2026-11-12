# T14.a 原生耐久设施

本单元把 `scripts/verify-durability.ts` 迁为原生单轨，保留接受基线的八项有效断言。现有 `Store`、`FundsStore`、`persist_usage_batch` 与 usage queue 仍是旧单账本业务装配；本单元没有迁移 SQL、资金行为、Core 三服务或双方账本，不代表 T14/T18、容量或 HA 验收完成。

## 入口与配置

`bun run test:durability` 不接受参数。设置以下两个环境变量；它们只选择本机已有工具，脚本不安装软件、不修改全局 PATH。

| 配置                      | 定义与验证                                                                                                                     |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `PG_TOOLS_DIRECTORY`      | PostgreSQL 工具的绝对目录，复用备份脚本已有变量名。实际使用其中 `initdb`、`postgres`，解析 realpath 并分别验证 PostgreSQL 17。 |
| `DURABILITY_REDIS_SERVER` | 本单元新增的 Redis server 绝对路径，解析 realpath 并验证 Redis 7.4。不会从 PATH 猜选 Redis。                                   |

本机验证使用 PostgreSQL 17.9 的 `/opt/homebrew/bin` 与 `/Users/wrr/work/super-token-gateway/.local/credit-capacity-20261111/redis-7.4.12/src/redis-server`（7.4.12）。其他机器须配置自己实际安装并验证过的路径。

```sh
MINIMAX_API_KEY= PG_TOOLS_DIRECTORY=/opt/homebrew/bin \
DURABILITY_REDIS_SERVER=/absolute/path/to/redis-server \
bun --no-env-file scripts/verify-durability.ts
```

`DATABASE_URL`、`REDIS_URL`、`QUEUE_REDIS_URL`、`REDIS_NAMESPACE` 不参与父脚本的资源选择。父脚本为本轮生成 PG owner、SCRAM 密码、Redis 密码、loopback 端口及 namespace。内部 `--durability-worker` 仅用于同一脚本的受监督 worker，要求完整的本轮 `DURABILITY_PHASE`、`DURABILITY_MARKER`、`DATABASE_URL`、`QUEUE_REDIS_URL`、`REDIS_NAMESPACE`；父脚本只传合成配置，不继承用户环境。环境变量不能单独激活内部 worker。

## 所有权与生命周期

公开测试设施位于 `tests/support/native-fixture.ts`，目前仅被本脚本和真实集成回归消费。`createNativeFixture` 在创建目录前做绝对路径、工具版本与可用空间预检，然后只拥有自己以 `mkdtemp` 创建的 `/tmp/gateway-native-*` 目录。没有任意目录清理、按端口杀进程、接管既有服务或 Docker 分支。

- 顶层与服务目录 0700；新文件与子进程继承的 umask 为 0077。密码只进入本轮 0600 文件/最小子进程环境，不放在 argv 或证据输出，initdb 密码文件用后删除。
- PG 只监听 loopback，使用本轮独立 Unix socket 目录与 SCRAM host/local 认证；`fsync`、`synchronous_commit`、`full_page_writes` 均保留 on。PG 日志保留 warning/error/fatal 技术信息，显式设置 `log_min_error_statement=panic`、`log_error_verbosity=terse`，避免预期负例把 SQL 语句正文和参数写入进程输出；客户端收到的 SQL 错误与事务行为不变。ready 核对 `data_directory`、`cluster_name`、owner、`postmaster.pid` 与子进程 PID，拒绝仅靠端口通畅认定身份。
- Redis 使用独立目录、完整 multipart AOF 与 manifest，保持 appendonly yes、appendfsync always、aof-load-truncated no、no-appendfsync-on-rewrite no、noeviction。命令执行前核对 PID、run_id 和 dir；restart 只重启本轮已停止的进程及原目录，不拼接部分 AOF。
- 子进程通过无 shell 的 `node:child_process.spawn` 和 detached 进程组启动；登记本轮 spawn incarnation 与 PID/PGID，保留进程句柄。Bun 一律使用 `--no-env-file`，最小环境只包含固定 PATH/locale/timezone 与显式合成配置，`MINIMAX_API_KEY` 强制为空。
- 就绪默认上限 15 秒，每个 PG/Redis 探测另有连接/语句/命令期限。正常关闭先 TERM，默认 750ms 后升级整组 KILL，随后对退出、后代与 reader 分别作有界等待。硬故障只对本轮已登记组执行 KILL。并发启动中的进程也纳入清理，单项失败不能阻止其他进程的关闭。公开 deadline 参数拒绝无限或越界输入。
- stdout/stderr 合计每进程最多保留 256KiB，超限立即使 fixture 失败并清理。每 500ms 检查本轮文件总量，384MiB 超限失败并清理；PG shared_buffers 32MiB、最多 64 连接、WAL 预算 64MiB；每 Redis maxmemory 64MiB、maxclients 128。创建前至少保留 1GiB 可用空间。遍历期间本轮文件/子目录被正常删除时只容忍该 entry 的 ENOENT；根目录消失、symlink 或其他文件系统错误仍使 fixture 失败。这是短验收预算，不是压测容量模型。
- 版本探测各有 2 秒期限，预检阶段也接收 INT/TERM，先回收本轮探测进程再失败。服务验收阶段父脚本有 120 秒总期限，SIGINT/SIGTERM 也走失败清理。queue/PG close、进程组与目录分别收尾；失败不打印 pass，输出真实残留资源。确认组已无进程才移除本轮目录；无法回收会保留精确资源并失败。

fix2 将原生工具预检与资金/队列依赖加载分开：父脚本先安装预检信号处理并取得本轮 fixture，再在服务验收的同一失败清理作用域加载 runtime/storage。加载失败或加载期间的 INT/TERM 必须关闭本轮 fixture，并阻止后续服务启动。受监督 worker 仍在验证内部参数后加载其业务依赖。受控 loader 用真实 CLI 延迟 runtime 求值，以独立证明旧装配会让无关业务依赖阻挡 preflight；该控制不说明此前自然启动异常的底层原因。自然首次 3 秒无 probe、零输出、driver 存活的原因仍为 UNKNOWN，不能归因于 Bun、缓存、CPU、shell 或依赖解析，也不能用随后绿灯消除原红。

父驱动自身被 SIGKILL 后不能运行 finally；本单元没有增加 watchdog/跨进程 registry 恢复协议，也没有验证该情形自动归零。fixture API 的调用方须在 finally 中调用 close，信号编排由消费脚本负责。备份、artifact、bench、长期原生依赖与 Linux systemd 尚未迁入本设施。

## 保留与修正矩阵

| 行为                       | 本单元处理与可观察证据                                                                                                                           |
| -------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| 默认 Docker、外部 PG owner | 修正为显式工具预检和完全自有 cluster。缺工具时在建资源前失败；Docker sentinel 不被调用，提供的邻居端点无连接。                                   |
| 生产拓扑与 live drift      | 保留所有 connected 可晋升候选均需确认；改 noeviction 后 ready 失败，声明零确认但存在候选时 ready 失败。                                          |
| 提交前强杀                 | 保留未提交的 evidence/ledger 事务回滚，队列重投后仅结算一次。                                                                                    |
| 提交后 ACK 前强杀          | 保留 PG 先提交；重投后只有单次 charge/journal，消费后队列计数归零。                                                                              |
| 主 AOF 硬重启              | 保留两节点同时 KILL 后，主节点独立重启仍有已确认待消费证据。                                                                                     |
| 副本 AOF 晋升              | 保留主节点停机时独立重启副本并晋升，恢复同笔精确结算。                                                                                           |
| 缺少 durable 副本          | 保留晋升节点要求一个副本时拒绝准入，不自动降副本数。                                                                                             |
| 错误持久化配置             | 保留 appendfsync everysec 拒绝准入，再恢复 always。                                                                                              |
| 非法正文与资金唯一性       | 保留正文拒收；三笔人工算例各 7+5=12 atoms，spent=36、debt=0，非法证据对应 UNKNOWN hold 100 atoms 保留，三个 usage_events 与三个 SETTLE journal。 |
| 生命周期与隔离缺口         | 新增真实进程、HTTP、PG/Redis 回归，验证环境隔离、SCRAM、期限、后代回收、部分启动、输出预算、邻居保护与 INT/TERM 后资源零。                       |

原始默认路径通过 harmless Docker sentinel 取得红灯：调用 network/rm/volume，未接触真实 Docker 或任何外部数据库。修复后的同一缺工具场景不会调用 sentinel。预检 TERM 的首次假工具用例未生成 marker，属于装置失败；修正 echo 版本后，旧 handler 实测在驱动退出后留下存活 probe，修复后 probe 回收。新增能力测试不伪造历史红灯。

独立审查 T14a-R1 又观察到一次 TERM 注入前 driver 以 1 退出、stderr 为 `Native tool version timed out`，marker 在 3 秒内未被观察到；fix1 的原断言观测还记录一次 3 秒内 driver 仍存活、磁盘与 Bun 均无 marker、外部快照未见 probe 的样本。这两次启动异常的底层根因尚未知，不能归为 Bun 缓存、性能或 shell，也不能用随后绿灯抵消。fix1 仅修正可证明的触发与诊断缺口：TERM 用例不再用 fake 写文件作为就绪条件，而由操作系统确认本轮 probe 的 PID、父 PID、独立进程组和精确 fake argv；再次核对存活后向真实 `verify-durability.ts` driver 注入 TERM。就绪与 driver 退出竞争，启动期限仍为 3 秒；提前退出和超时均失败，保留最多各 16KiB 的 stdout/stderr、总输出字节数、退出状态、fake 内容及 hash/mode/realpath、文件系统和本轮进程状态。fake shell 保持其唯一 argv 并等待一个 sleep 后代，断言 driver 退出时 probe 及整组都已消失；失败收尾仅回收本轮已识别组。

fix2 在依赖已安装、源码未改时的第一次原公开 TERM 用例再次得到 3 秒无 probe、零输出、driver 存活的红灯。随后加入最小阶段写入的真实 CLI 观察到 main、预检 handler、fixture 和 tool spawn，且独立私有冷转译缓存控制通过；这些后续样本不能解释首个自然失败。独立受控 loader 则明确观察到旧 driver 在 runtime 求值时 INT/TERM listener 均为零，3 秒内原生 probe 未启动。fix2 只修复这一已证的装配边界：类型导入保留静态检查，业务值导入在 fixture 与服务阶段信号处理建立后加载；加载承诺与既有中断竞争，中断和加载错误都进入相同 finally。新增三个真实 CLI 回归覆盖延迟依赖不阻挡 preflight、加载失败关闭 fixture、加载期间 TERM 关闭 fixture 并阻止后续服务。原 13 个用例及原 3 秒启动/6 秒退出上限均保留，新增后文件共 16 个用例；直接耐久入口的八项业务断言未改。

该用例的因果负控制在独立副本中移除真实 driver 的预检信号 handler：确认活 probe 后发送 TERM，driver 退出 143 而 probe 存活，回归确实失败并由测试回收；恢复原 handler 后同一用例通过。另一个独立负控制让 driver 以 23 提前退出，测试立即保留合成 stderr 与退出码，未等待 missing-marker 超时。它们证明真实信号生命周期的可观测性和失败诊断，不证明前述未知启动异常已消除。原失败及后续精确候选验证保留在 `.local/T14a-fix1-report.md/json` 与 evidence manifest。

首次完整 PG 运行被监控关闭时，最初版本未输出内部监控异常，不能认定那一次的具体错误。随后真实文件 churn 回归独立复现 ENOENT lstat 误停并取得红灯，修复后通过；第二次完整 PG 运行明确因 postgres 累计输出超过 256KiB 中止。真实查询负例又复现默认日志回显 SQL 正文，修复仅收紧 PG 日志内容，不增加输出预算；两条 identity 定点诊断也通过。第三次完整 PG 运行明确出现连接上限 32 的 FATAL 53300，并级联阻断部分门禁；按现有并发连接池的实际需要将本轮 PG 上限调整为 64，shared_buffers 与所有耐久/日志预算不变。`assertHealthy` 让监控失败可在收尾后显式报告，禁止资源清理成功掩盖验收失败。

## 验证与范围限制

路径回归以 `realpath("/tmp")` 确定本机物理根，校验和失败兜底共用同一规则：只接受该根或经 realpath 核实属于该根的 `/tmp` 拼写，以及 `gateway-native-` 前缀和既有字符集构成的直接子目录名；拒绝相对路径、URL、目录穿越、外部根和现存 symlink。正常失败时 CLI 已删除本轮目录，因此归属校验不要求已删除的子目录仍可 realpath。新增真实 CLI 回归让 fixture 通过合法 `/tmp` 拼写创建和删除目录，再验证加载失败与资源归零；本机 macOS 同 inode 别名控制不等于已执行 Linux。 本次路径修正后文件共 17 个用例，保留 fix2 的 16 个用例。该测试侧可移植性修正不改 driver、公开 fixture、3 秒启动、6 秒退出或 120 秒服务期限，也不改变此前自然启动 UNKNOWN 的结论。

相关回归入口为 `bun --no-env-file test tests/integration/native-durability.test.ts --timeout 30000`，也由现有 integration 目录发现，无需修改 package 扫描。集成门禁须显式提供上述两个工具变量；普通业务测试的 PG/Redis 环境由门禁自己的隔离设施配置，不能从用户正在使用的集群借用。

交付时的实际命令、退出码、用例数、原始失败、最终源 tuple 与资源记录见本单元 `.local/T14a-report.md/json` 和 evidence manifest。根任务完成独立审查及合并版本的完整验证；本机结果不代表真实掉电、跨故障域复制/仲裁/分区 fencing、99.99% SLO、Linux/systemd 或未来三服务双方结算。真实模型未调用，Docker、备份、压测与未改动的 Go/race/WASM 测试未在本单元执行。
