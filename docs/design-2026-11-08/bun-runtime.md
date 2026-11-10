# Bun + TypeScript 运行与工程规范

版本：1.1 · 2026-11-08。后端技术栈由用户确定为 Bun + TypeScript。本文件是实施约束，尚未建成服务；配合[总体架构](architecture.md)、[资金规则](funds-and-billing.md)与[验收计划](delivery-and-validation.md)使用。

## 1. 运行面与依赖

| 组件 | 首版选择 | 边界 |
| --- | --- | --- |
| HTTP 服务 | Bun.serve，标准 Request/Response | 执行节点直接控制流和终态屏障；中间件不得提前消费正文 |
| 上游与内部 RPC | fetch，版本化 HTTPS JSON | 上游重定向、重试和发送次数由连接器审计；内部重放沿用 commandId |
| 流 | Web Streams，增量解析 | 单条有界管线，按字节背压，终态事件受证据屏障控制 |
| 精确金额 | 封装 bigint + bigint 有理数 | DB numeric / DTO 字符串；不经过 number |
| PG | Bun.SQL，独立 storage-pg 适配器 | 实施时锁定版本并通过真 PG 验收；领域模块不依赖驱动类型 |
| Redis | 经 Bun 兼容验收的 ioredis 客户端 | 首版只持有软状态；限定重连、离线排队和命令期限，不隐式无限重放 |
| 后台任务 | 独立 Bun Financial Worker 进程 + PG 工作表 | 金融事实耐久，不依赖内存队列、未等待 Promise 或 Worker 线程消息 |
| 工程 | Bun workspace、bun.lock、TypeScript strict | CI 独立类型检查、依赖边界、测试和可复现安装 |

不要求先引入通用 Web 框架。若控制 API 后续采用路由框架，仍须通过相同 Request/Response、认证、输入上限、取消与错误契约；执行热路径不能因框架默认行为丢失背压或终态屏障。

## 2. TypeScript 与领域边界

开启 `strict`、`noUncheckedIndexedAccess`、`exactOptionalPropertyTypes`、`useUnknownInCatchVariables` 和 `noImplicitOverride`，目标至少支持 bigint；使用 `moduleResolution: bundler`、`noEmit` 与匹配的 Bun 类型。具体 tsconfig 按锁定版本验证。Bun 转译/执行 TS 不等于完成类型检查，CI 必须独立执行 `tsc --noEmit`。[B1]

contracts 包保存版本化 DTO、运行时 schema 与编解码器。HTTP、RPC、PG 行、配置和供应商响应都来自不可信边界，校验后才进入领域；`as Money` 或 TypeScript 泛型不是运行时校验。错误值按 unknown 收窄，不通过 `any` 跳过金额和状态检查。

finance 包集中 Money、价格、预扣、Capture、免收、预算、退款规则；执行与后台不能各写一份扣款算法。用判别联合表达命令结果，但非法状态跃迁仍由事务与约束拒绝。包导出和依赖规则在 CI 检查；数据库角色和工作负载身份提供实际权限隔离。

## 3. 金额、序号与序列化

Money 的内部值为只读资产身份与 bigint atoms。构造函数限制格式、符号和绝对值至 NUMERIC(38,0) 的范围；平台有符号分录与客户非负金额采用不同构造入口。所有操作先验证资产相同，结果重新检查范围。

费率使用 bigint 分子/正分母，明确约分与复杂度上限。非负费用按全请求精确求和后取整一次；不能用 Number、Math.ceil、toFixed 或二进制浮点中间结果。输入数量即使最后转 bigint，若先经不安全 JSON number，也已无法恢复原值。供应商大整数/精确小数需使用按 schema 保留字面精度的解析方式，或要求字符串；缺少可靠解析时拒绝该计量能力。

DTO 只输出显式的十进制字符串，禁止直接序列化 bigint，也不修改全局 BigInt.prototype。精确账户序号、计量和大版本号沿用字符串；明确有安全范围的 HTTP 状态、计数与展示值才用 number。原始证据先按受限字节保存摘要/来源，不用重新 JSON.stringify 的结果冒充原件摘要。

## 4. PG 事务与提交未知

Bun.SQL 支持 PG 事务和连接池，numeric 默认读取为字符串。[B2] 首版适配器还必须锁定并验证以下行为：

- 金融事务始终使用 `sql.begin` 回调的 tx。所有语句在回调中被等待；禁止用全局池对象代替 tx，禁止事务外再等待惰性语句。
- 所有金融写入参数化；金额以已校验字符串绑定并显式转换为 numeric。查询序号保持字符串或经过精确配置的 bigint，不注册转 number 的 numeric 解析器。
- 在 tx 内完成锁、journal、effects、slices、命令回执与工作项；COMMIT 确认后才向调用者答复。事务内不等待供应商、支付或对象存储。
- Promise 超时、fetch abort 或驱动报连接错误不证明服务器已回滚。提交结果不确定时查询原 commandId，不能再次返回发送能力。
- 使用数据库 statement_timeout、lock_timeout 与连接获取上限，并验证取消后的连接可安全复用。仅用 Promise.race 让调用方退出不会停止数据库工作，也不能释放仍在执行的连接。
- 数据库明确返回死锁/序列化回滚时，可在原 commandId 和总期限内有界重试；不把所有异常都判为可重试，不对提交未知重新发上游。

资金 API、SealEvidence、金融后台、对账/管理按优先级配置独立资源预算；可用独立池落实隔离，并计算所有进程与滚动发布副本的总连接数。NOTIFY 只作提示，可用专用会话，任务真相仍在 PG。连接重连、TLS、安全切主与池中旧连接失效必须实测。

## 5. 流、背压与取消

Bun 的 HTTP 和 Streams API 是实现基础，[B3] 但应用仍负责证明有界缓冲、事件解析和证据顺序。供应商 body → 增量协议解析/计量 → 下游输出使用单条管线，不 tee 出一个不受客户速度限制的完整副本。

使用 pull 驱动读取，或等待 writer.ready/write；highWaterMark 采用字节计数。一次 read 可能返回大块，highWaterMark 不是完整的硬内存上限：对单块、完整事件、解压后大小、待发送窗口和终态窗口分别检查，检查前也需限制源读取与累计量。拒绝超限不能静默丢计费证据。

不能在 start 中无条件循环 enqueue，也不能把 response.text/json/arrayBuffer 用于无限流。非流式 JSON 先限制体积，再收集与解析。SSE 解析处理增量 UTF-8、CR/LF、跨块事件和同块正文/usage/DONE；遇终态先暂存、SealEvidence 确认、再转发正常完成标记。

维护三个不同生命周期：客户请求 signal、上游 controller、服务管理的证据收尾 controller。客户断开触发上游取消或政策允许的有界 drain；若已取得最终证据，使用独立且有期限的收尾 signal 保存。收尾不能是返回 Response 后无人管理的异步调用。取消只终止本地等待/传输，不能证明供应商未计费。

Bun.serve 空闲计时与入口/LB 超时要显式配置。[B4] 需要长静默流时，可按请求覆盖默认计时，但必须保留自己的首字节、流空闲、慢客户与总产品期限。额外 SSE 心跳只在协议允许时发送，不能伪造业务内容或最终用量。

## 6. 多核、CPU 与停机

首版每个服务实例使用独立 Bun 进程/容器，由负载均衡分发连接与调度器使用多核。每个 JS 实例中的同步 tokenizer、JSON 解析、计费计算、压缩和日志编码都有 CPU/输入上限。await 不会把同步工作变成并行工作；单进程吞吐须测量，再决定副本数。

CPU 密集任务在确有证据时移入有界独立计算池，队列满则拒绝/降载。计算结果绑定输入摘要、算法/配置版本与期限；超时不能把迟到结果用于另一请求。计算池不持有资金写权限或直接供应商发送权。Bun Worker API 在核对时仍标注实验性，首版不依赖线程终止语义完成金融收尾。[B5]

SIGTERM 后先撤 readiness、停止新准入，再排水已有连接与收尾注册表；停止接收新的后台工作。Bun.serve 的停服等待与应用在途 Promise 都纳入宽限。宽限结束时不统一释放预扣，未完成义务按 PG 事实进入恢复。已暂停旧实例重新运行也不能因任务租约过期自动取得新调用许可。

## 7. 构建、观测与发布

运行时固定到具体 Bun 版本和镜像摘要；TypeScript、@types/bun、客户端和其他依赖入锁文件。使用 frozen-lockfile 安装与生产模式启动，禁止部署时自动下载缺失依赖或使用 watch/hot。是否打包为单文件只是交付形式，不改变进程边界和资源限制。[B6]

CI 最少包含严格类型检查、依赖边界/lint、`bun test`、真实 PG 集成、连接器假体与故障场景。明确金融 Promise 必须等待或转为耐久工作；仅在 finally 中做释放不能代替检查领域状态。schema/config/RPC 的兼容发布规则沿用架构文档。

观测包括事件循环调度延迟、CPU、RSS/堆/外部缓冲、GC 可用指标、socket/在途、流窗口、PG 排队/锁/WAL、SealEvidence 延迟和 backlog。OpenTelemetry、AsyncLocalStorage/上下文传播与 Node 兼容探针按锁定版本实际验证，不能假设调用成功就有完整链路。日志有界、脱敏且可采样；金融证据独立耐久。

压测同时给出单进程与集群的 CPU/内存/吞吐曲线；长时间等待和客户端慢读必须覆盖。10,000+ RPS 仍是集群业务验收目标，不是 Bun 或单进程的现成容量声明。

## 8. 实施前版本验收

| 项目 | 必须得到的证据 |
| --- | --- |
| Bun/OS/CPU 架构 | 精确版本和镜像可复现；同环境复跑类型检查与协议测试 |
| Bun.SQL + PG | 38 位整数往返、同连接事务、COMMIT 断线、取消后连接状态、切主重连 |
| HTTP/fetch/Streams | 真 socket 慢读/断连/静默、内存有界、上游发送次数、终态先落证据 |
| 金融收尾 | 客户 signal 已取消仍可有界封存；停机后无孤立内存金融义务 |
| CPU 与多进程 | 大输入/日志压力下事件循环延迟与租户公平；副本/池总预算 |
| Redis/观测依赖 | 重连与队列上限、脚本兼容、追踪传播、指标实际可用性 |

本次仅完成文档更新和[文档验证](validation.md)。尚未建立上述 Bun 服务、数据库或集群验收环境。

[B1]: https://bun.sh/docs/runtime/typescript
[B2]: https://bun.sh/docs/runtime/sql
[B3]: https://bun.sh/docs/runtime/streams
[B4]: https://bun.sh/docs/runtime/http/server
[B5]: https://bun.sh/docs/runtime/workers
[B6]: https://bun.sh/docs/pm/cli/install
