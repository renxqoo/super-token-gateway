# T04.native-startup

本次独立生命周期加固不宣称定位或根治历史原生产物 WRITE_TIMEOUT。历史五场真实复现通过与 dyld/读取前延迟只属于观测证据。

## 合同与迁移

可信 parent 的 JSONL IPC 升为 version 2；WASM ABI、SDK guest schema 与 worker 编译/取消流程不变。parent 在配置有效、命令 scanner、取消与监控设施安装后，以已有有界 stdout writer 输出且仅输出一次 `{"version":2,"id":"","event":"ready"}`，随后接收命令。worker 不输出 READY，双方内部消息只机械升级版本。host、Go 测试 client、Bun acceptance caller 与 fakeparent 同时切换，不保留旧协议分支。

连接状态为 starting → ready → retired。starting 期间禁止发送任何命令帧，包括 stats/cancel/close；仅接受完全匹配的 READY。heartbeat、普通 reply、错误版本、额外字段、重复 READY 都不能替代 READY。退休连接的输出和 late write callback 无作用。READY 以后保留 heartbeat、slot、队列预算和单帧 write stall 约束。

host `native.startupMs` 默认 5000，整数范围 1–30000；parent 接受相同配置但启动接收能力由 host 限时。单帧 `writeMs` 默认 2000 不变。每个操作在入队时确定绝对 deadline（load 为 compileMs+250；call 为原 timeoutMs+200；stats 为 1000），启动与排队都消耗该预算，READY 或 dispatch 不重置。starting 队列受 pending count/bytes 限制且不占执行 slot，不套用普通忙队列 queueMs；ready 后等待 slot 最多 queueMs，同时受原绝对期限约束。缺失 READY 在 startupMs 或最早操作总期限到达时以 STARTUP_TIMEOUT 失败。任何已发送帧均不重试，不重放网络操作。

starting 中取消一个共享 load waiter 不影响其他 waiter；最后 waiter 取消会移除命令，无其余命令的未就绪进程立即 kill/reap。close/crash/超时立即退休连接并回收进程组。fail/close 主动清除 startup、heartbeat、operation、inflight write timer；late callback 不泵队列。loaded 数量只统计真实 load reply 成功的模块。

独立模块 A、B 在同一连接启动时，取消 A 必须立即从启动/待写队列物理移除 A 并释放其帧引用，保留 B。READY 仅为仍 pending 的命令建立排队 timer，不能为已取消命令重建 timer。真正 inflight 的写入仍保留帧及原 writeMs 约束，直到写回调或连接退休。永久回归通过公开 API 子进程验证 B 成功、close/reap 和 fixture 清理完成后自然退出，避免只凭 snapshot 的计数推断实际资源已归零。

诊断只含有限 phase、操作枚举、当前帧字节数、启动/写入毫秒和连接标识；不含 frame、正文、base64、guest 文本、认证、环境、stdout 内容。错误与公开 snapshot 都可提供同样安全的字段；snapshot 提供 timer、inflight、waiter 与队列计数供收尾核对。

## 行为映射与验收

保留所有原 IPC16、SDK、Go 取消/资源/worker 流程与真实15 fixture/24 step 断言。合法 fakeparent 延迟 READY/开始读2200ms且5250ms原总load期限内成功，是新合同差异；旧实现应真实报 WRITE_TIMEOUT。新增 never-ready、错误/重复/额外 READY、READY 前 reply/heartbeat、startup 共享 waiter abort/最后 abort/reuse、close/crash、READY 后停读 WRITE_TIMEOUT、绝对总期限、延迟事件循环后的 timer 清理、旧连接隔离等公开行为回归。

执行顺序为新公开回归旧红证据、实现及 focused 绿、Go vet/标准/race、全 unit、check/lint/fmt、完整 integration、build、独立 plugin artifact、完整 artifact。真实 native 与 PG 窗口由 root 协调。仅交付本任务明确路径差异；不改 SDK/WASM、SQL/snapshot/migration、package/lock 或资金逻辑。回退须作为同一 IPC 版本合同整体回退 host、parent、caller 与测试，不回退已持久化 schema。

验证结果与真实失败、资源账本、逐文件 before/after hash 在 `.local/T04-native-startup-report.md/json` 中记录；root 后续独立审查与主树完整 verify 是接纳步骤，作者测试不替代该证据。

## 本轮作者验证（2026-11-12）

原始 supervisor SHA `7c881915ef07ffb56fe7417dfffb69aa353ccd48642def2ee31a68a4e4f8e0ed` 的隔离副本运行公开延迟 READY 回归，约 2000ms 后实际 WRITE_TIMEOUT，原队列/字节回收 snapshot 已保全；新实现该同一回归成功。新增同 chunk load reply 后重复 READY 的候选实现边界也取得隔离旧候选失败证据，修正后拒绝退休连接结果。

作者检查：check 180 文件、lint 零警告、fmt 274 文件；focused 53 例/320 断言、全 unit 262 例/3907 断言；Go vet/标准与 race 均 33 顶层/含子例45；完整 integration 353 例/8047 断言；build、独立插件产物15 fixture/24步、完整产物原9项及签名/catalog7项通过。原 unsupported-version 测试向量由2调整为3，保留原 VERSION_UNSUPPORTED 断言；未改 worker 编译/取消语义及既有资源时限。

独立产物原输出的安全诊断实测 startupMs=2795，说明这一成功调用真实经历了显式启动阶段；它不解释三次历史失败。实际新 helper SHA `c2767f5be29a843e0586ca31b4e8baa9d2fbe81527599cdcfaad23a7a7962663` 与 dist 副本一致，编译后 signer/installer/catalog 使用该实际 helper 通过完整链。历史原因仍未确认。

作者未运行单一 aggregate `verify`，其各组件已分别执行；root 的全新独立审查及主树组合完整 verify 仍是接纳条件。Docker/container、durability、backup、真实模型与 Cargo/WASM 重建未执行，分别受本轮禁止 Docker/停机及明确范围约束。未执行覆盖率统计。

## 独立审查后修复（T04.native-startup.fix1）

独立审查确认取消 A 后旧队列条目仍在 READY 重建 refed timer，导致 close 的 snapshot timers=0 但 owner 进程滞留。修前重放原 timer 观察向量实际失败，新增公开 API 子进程回归也因清理后仍未自然退出失败。修复在 settle 中移除队列/待写引用并同步待写字节，READY 防御检查 pending；不修改既有预算、FIFO、slot、共享 waiter、真正 inflight 写入和合法取消语义。本轮独立验证、资源账本、失败与逐文件 hash 以 `.local/T04-native-startup-fix1-report.md/json` 为准；历史 WRITE_TIMEOUT 根因仍未确认，主树整体 verify 接纳结论仍由 root 给出。
