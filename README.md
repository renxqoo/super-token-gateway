# Super Token Gateway

企业级高性能 LLM Gateway 与微批核心账本系统，基于 **Bun 1.4.5 / TypeScript + Hono + PostgreSQL 17 + 极薄 Rust `wasmtime` 插件沙箱**。控制台业务面使用独立 PostgreSQL 数据库，通过内部 HTTP API 与异步 Outbox 与网关协作。控制台能力、数据流和运行边界见 [Console API](docs/console-api.md)。

架构与核心账本分片租约见 [ADR 0009](docs/decisions/0009-enterprise-gateway-worker-architecture.md)。插件定义、显式绑定、发布和迁移见 [ADR 0012](docs/decisions/0012-plugin-revisions-and-provider-verification.md)。

[OpenAPI](docs/openapi.json) 从当前公开和内部监听路由以及内部请求校验生成。推理响应格式由协议和所选渠道决定。接口变更后运行 `bun run openapi` 更新文件。

## 目录结构

| 路径                           | 职责                                                                                                                                                                                                                                                           |
| ------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `apps/gateway`                 | 高性能无状态公开网关（Chat/Responses/Messages/Gemini/Embeddings/Moderations/Images/Audio、`/livez`、`/readyz`、Prometheus 直方图 `/metrics`、`traceparent` 链路追踪、零明文 JSON 访问日志）+ Zod 严格校验 `/internal/v1/*` 管理与分页对账 API                  |
| `apps/worker`                  | 后台账务与调度 Worker（PG Advisory Lease 选主、超时预扣与过期节点租约扫尾 `sweepExpiredLeases`、批次过期、自动收债、供应商分片再平衡、Outbox 推送）                                                                                                            |
| `apps/console-api`             | 客户与平台管理 API、组织和账户权限、模型市场、账务查询、用量投影和告警，独立 PostgreSQL 持久化                                                                                                                                                                 |
| `apps/mock-providers`          | 独立测试用上游替身（OpenAI / Anthropic / Gemini / Azure / Bedrock / Vertex / DeepSeek / Ollama）                                                                                                                                                               |
| `packages/ledger`              | 精简 PG 核心账本：`drizzle-orm/bun-sql` 强类型 Schema（23 张表 1:1 零漂移）/目录 CRUD/Outbox/归档/分页对账查询 + 原生 `Bun.SQL` 自适应 PG 分片资金租约（`wallet_leases`/`supplier_leases`）、2 ms 微批与 9 表单 CTE 原子结算热路径                             |
| `packages/protocol`            | 多协议无损透传、跨协议转换（含 `reasoning`/`thinking` 互通、strict/compat 策略表、`x-gateway-dropped-fields` 提示与协议原生错误封包）、有界 SSE 流解析、精确 JSON 快速路径与精确计量                                                                           |
| `packages/plugin-engine`       | 统一插件引擎（`CONTENT` 内容风控 + `PROVIDER` 渠道插件）：内置 15 个厂商声明式预设、预编译转换器（参数改名/裁剪/钳位/伪 200 错误匹配/Usage 映射）、原生 Rust `wasmtime` FFI 池化沙箱（内容/渠道双隔离池、`egress > ingress` 公平队列）、外部 HTTP Webhook 风控 |
| `packages/runtime`             | SmartRouter 智能路由熔断器（含 `probeTimeoutMs` 防死锁自愈）、8 种宿主鉴权/签名器（含 `aws_sigv4`/`gcp_vertex_oauth`/`hmac_sha256`）、Redis 集群协同与 SSRF/DNS-Pinning 高复用连接池                                                                           |
| `packages/kernel`              | 纯函数精确 `BigInt` 有理数计价与预扣上界计算                                                                                                                                                                                                                   |
| `packages/contracts`           | 公共 Schema 校验与错误类型                                                                                                                                                                                                                                     |
| `native/plugin-wasmtime`       | 极薄 Rust `wasmtime` cdylib 沙箱（~200 行，零 WASI/网络/FS，按插件硬内存与 Fuel 上限）                                                                                                                                                                         |
| `native/builtin-pii-guard`     | 内置开箱即用 Rust → `wasm32-unknown-unknown` DLP/PII 脱敏与防注入插件（~26 KB 零导入 WASM，支持草稿安装、预检发布与沙箱试跑）                                                                                                                                  |
| `native/example-provider-wasm` | 示例 Rust → `wasm32-unknown-unknown` 私有协议 `PROVIDER` WASM 插件（覆盖 `adapt_request` / `adapt_response` / `adapt_stream_event` + 有界跨帧 `state` + `usage` 提取）                                                                                         |

## 核心特性

1. **资金扣款零差错底线与万级单机微批账本（`packages/ledger`）**：
   - 23 张严格受控表（含 `wallet_leases` / `supplier_leases` PG 持久化分片租约），PostgreSQL 强约束（批次守恒 `CHECK`、不可变凭证触发器、`DEFERRABLE` 复式借贷平衡触发器）。
   - 多币种/积分（`PTS`/`USD`/`CNY`）`NUMERIC(38,0)` 原子计量，精确有理数 `BigInt` 计价与单次向上取整。
   - 多批次（`CASH`/`GRANT`）优先级与严格过期，过期预扣余量及租约内过期批次永不复活；退款按逆序归还批次；有界授信与自动收债冻结；16 分片供应商资金池与 `UNKNOWN` 预扣保守保留。
   - **自适应 PG 持久化分片租约 + 9 表单 CTE 原子提交**：热点钱包与供应商分片在 PG 中按需预留租约，热路径 `acquireHold` 仅执行单条 `billing_holds` 持久化写入（先落盘再调用上游，`SIGKILL` 零丢账），`commitTerminal` 以全局升序 `FOR NO KEY UPDATE` 锁序在单条 9 表 Data-Modifying CTE 内原子完成结算、复式凭证（每笔 `billing_charges` 绑定 `customer_journal_id`/`supplier_journal_id` 可追溯）与 Outbox 写入；空闲 250 ms 自动退租，节点异常宕机由 Worker `sweepExpiredLeases` 全量回收归零。
   - 并发 `Idempotency-Key` 在 `HELD` 在途期返回 `409 idempotency_in_progress`，杜绝重复派发上游与双扣。
   - **零明文落库**：Prompt、回复文本与工具内容永不写入数据库、日志或 Outbox 事件。
2. **智能路由与动态故障切换（`SmartRouter`）**：
   - 优先级分层 + `WEIGHTED` / `ROUND_ROBIN` / `LEAST_LATENCY` 策略。
   - 通道级单调时钟熔断器（`CLOSED` / `OPEN` / `HALF_OPEN` 单探针 + `probeTimeoutMs` 防死锁自动释放 + 指数退避）、429 `Retry-After` 冷却与 401/403 故障隔离。
   - 首字节前自动无感故障切换：原子释放失败通道供应商预扣、为更贵备选路由补足客户预扣、`retry_on_ambiguous` 超时切换保留旧渠道 `UNKNOWN` 风险占位；绝不重复向客户扣款；首字节后流式中断永不拼接其他模型输出，客户端主动断流不误伤上游健康度。
3. **企业级内容处理与风控插件（`packages/plugin-engine` + 内置 `pii-guard`）**：
   - WASM 插件唯一运行于原生 Rust `wasmtime` FFI 池化沙箱（零 Host Import、`catch_unwind` FFI 边界、紧致 `PoolingAllocationConfig`、精确指令级 Fuel 与 `ResourceLimiter` 内存硬上限、`sha256` 完整性 + Ed25519 签名缓存校验；生产模式强制要求 `PLUGIN_SIGNING_PUBLIC_KEY_PEM` 并启动自检）。内容与渠道插件使用双隔离执行池、租户公平 `egress > ingress` 队列与长文本动态 Fuel/分段滑窗扫描。
   - **内置完整可用插件 `pii-guard` v1.0.1**（`native/builtin-pii-guard` 以 `opt-level = 3` 编译为零导入 `.wasm`）：覆盖 `on_request` / `on_response` / `on_stream_chunk` / `on_stream_end` 全生命周期，单趟扫描阻断 PEM 私钥泄露与 Prompt 注入，原位脱敏 API Key（`sk_*`、`sk-*`）、AWS Key（`AKIA*`）、GitHub Token（`ghp_`/`gho_`/`ghu_`/`ghs_`）、邮箱、中国 18 位身份证（GB 11643-1999 校验码）、中国 11 位手机号、美国 SSN 与 Luhn 有效银行卡号；支持 `/internal/v1/plugins/builtins` 发现、`POST /internal/v1/plugins/builtins/pii-guard/install` 创建草稿、`GET /internal/v1/plugins` 列表及 `POST /internal/v1/plugins/:id/test` 沙箱试跑。
   - 外部 HTTP Webhook 风控支持 `HOLD_UNTIL_APPROVED`（零违规字节泄漏）与 `STREAM_AND_CUTOFF`（边推边检异步截断上游并精确按策略结算），支持 `manifest.hooks` 精准过滤、单插件并发上限、有界响应体上限、`HALF_OPEN` 单探针熔断恢复、注册与调用全链路 SSRF/DNS-Pinning 校验及 `FAIL_CLOSED` / `FAIL_OPEN` 降级策略。
   - 显式三分故障分类（`content_decision` / `plugin_fault` / `host_capacity`）：内容拦截按路由/插件配置执行 `CHARGE_CUSTOMER`（默认）或 `PLATFORM_ABSORB`；插件故障或容量耗尽在 `FAIL_CLOSED` 下绝不向客户扣费（上游后置场景自动 `PLATFORM_ABSORB` 且受每租户预算保护）且绝不触发上游渠道熔断；`FAIL_OPEN` 放行正文时正常计费并写入 `fail_open: true` 审计与指标。流式和非流式审计都写入 `billing_charges.plugin_audit`。

4. **上游渠道插件与不可变修订（`ADR 0012`）**
   - 渠道保存时固化 `provider_plugin_id`，请求按修订从内存快照解析。已有渠道和路由使用 `plugin-releases` 预检、发布和回滚。配置纪元经 Redis 通知与 PostgreSQL 轮询传播。上传和内置安装拒绝 `enabled` 字段。首次启用可以发布 `targets: [{ "plugin_id": "<修订 UUID>" }]`，也可以在发布时将草稿绑定到已存在的渠道或路由。发布记录返回提交时在册节点及实际应用确认。紧急撤销使用 `POST /internal/v1/plugins/:pluginId/revoke`，传入 `reason`，以新修订恢复。
   - **15 个内置预设各有独立定义**，包括 `openai`、`openai_compatible`、`anthropic`、`gemini`、`azure`、`bedrock`、`vertex`、`deepseek`、`ollama`、`qwen`、`volcengine`、`zhipu`、`moonshot`、`minimax`、`openrouter`。可用 `GET /internal/v1/plugins/builtins?capability=PROVIDER` 查看；[上游模拟协议矩阵](apps/mock-providers/README.md#厂商对话协议矩阵) 记录完整测试范围与官方依据。
   - **双形态扩展**：`DECLARATIVE` 声明式插件原生预编译执行（零 WASM 开销，支持路径模板、参数重命名/裁剪/默认/钳位、伪 HTTP 200 错误识别转 SmartRouter 切换、`usage_aliases` 映射与渠道/路由级 `adapter_overrides` 隔离叠加）；`WASM` 可编程渠道插件复用隔离沙箱，覆盖 `adapt_request` / `adapt_response` / 无状态逐帧 `adapt_stream_event`（有界 4 KB `state`）。
   - **安全与试跑边界**：渠道秘钥永不进入 WASM 沙箱，鉴权与动态签名由宿主统一注入（支持 `none`/`bearer`/`header`/`query`/`azure_key`/`aws_sigv4`/`gcp_vertex_oauth`/`hmac_sha256`）；`usage` 产出受 `grants: ["extract_usage"]` 与宿主 `BigInt` 强校验约束；提供 `POST /internal/v1/providers/preview` 在线干跑预览（鉴权头自动脱敏 `<REDACTED>`，零外网调用）。

## 本机性能复测

Bun 1.4.5、PostgreSQL 17.11 和 Mock 上游。数据库开启 `fsync`、`synchronous_commit` 与 `full_page_writes`。

- 单 Gateway 短 JSON 在 1,024 并发达到 **13,666 RPS**，P99 **96 ms**，比第二轮基线提升 **32.0%**。在 1,536 并发进入约 **14,300 RPS** 的平台。
- 双 Gateway 配合 Worker 与 Redis，在 2,304 并发两次达到 **17,361–17,896 RPS**，零错误。2,560 并发开始出现连接重置。成功响应均唯一结算，排空后余额守恒。

并发档、尾延迟、基线和故障演练见 [插件修订改造本机验证](docs/performance/plugin-revisions-2026-11-22.md#第二轮发布确认紧急撤销与租约锁优化)。首次 30 分钟混合插件稳态及连接重置复测见 [混合稳态与并发验收](docs/performance/gateway-mixed-soak-2026-11-22.md)。共享供应商租约的锁诊断、同口径吞吐对照及零漏发复跑见 [供应商租约结算与混合发压复测](docs/performance/supplier-lease-settlement-2026-11-22.md)。

## 快速开始与部署

需要 Bun **1.4.5**、PostgreSQL 17 与 Redis 7.4 的本地工具。没有使用仓库的本地工具目录时，先设置 `PG_TOOLS_DIRECTORY`、`DURABILITY_REDIS_SERVER`，再生成配置。

```sh
bun run setup:local
bun run infra:up
bun run db:migrate
bun run dev
```

单机和多副本运行方式：

```sh
# 构建必需的 Rust wasmtime 沙箱与内置 WASM 插件（需要本地安装 rustc/cargo 与 wasm32-unknown-unknown）
bun run build:wasmtime
bun run build:plugins

# 构建无需 node_modules 的发布产物
bun run build

# 单机模式（Gateway + 内置 Worker）
bun dist/gateway/main.js --mode=all-in-one

# 多副本模式（无状态 Gateway 与 Worker Advisory Lock 选主）
bun dist/gateway/main.js --mode=gateway
bun dist/worker/main.js
```

Docker Compose 提供单主机验证环境，要求显式设置数据库密码及 Gateway 密钥。Systemd 和 Caddy 是主机内运行模板。生产拓扑需要独立故障域的入口与 Gateway、可写 PostgreSQL 主备切换地址、独立数据库角色及容灾演练。配置共享 Redis 时，Redis 不可用会影响准入和就绪检查。

Console 的独立部署方式、数据库角色、Gateway 联动和发布验收见 [Console API 部署与验收](docs/console-api.md#部署与验收)。

Gateway 计费账户的 API Key 采用 `sk_` 加 43 位 Base64URL 字符。从 Console 签发 Key 后配置客户端。部署中若路由依赖 `pii-guard` 脱敏，请在分发新 Key 前安装、发布并确认路由已应用 v1.0.1。发布沿用 [插件修订流程](docs/decisions/0012-plugin-revisions-and-provider-verification.md#已交付的管理操作)。

Gateway 和 Console 从各自的空 PostgreSQL 数据库初始化。运行 `bun run db:migrate`、`bun run console:migrate` 或对应的独立产物。应用启动核对现行基线校验和，运行时就绪探测要求可写连接。

生产身份与角色配置、监控和主备恢复流程见 [ADR 0011](docs/decisions/0011-production-operations.md)。本机生产演练覆盖实际 PostgreSQL 主备提升、Redis Sentinel 选举与 WAL 指定时间点恢复。

## 验证与基准

```sh
bun run check            # Bun TypeScript 类型检查
bun run lint             # oxlint（零告警零错误）
bun run fmt:check        # 代码格式检查
bun run openapi:check    # 确认 OpenAPI 与当前路由和内部请求 schema 一致
bun run test             # 单元测试与跨模块架构约束
bun run test:integration # 真实 PG 17 / HTTP 集成测试（账本 + 智能路由 + 插件风控 + Worker）
bun run build            # 打包 dist/ 独立产物
bun run test:artifact    # 隔离目录端到端产物验收与归档校验
bun run test:durability  # SIGKILL 强杀预扣回收 + pg_dump/psql 备份恢复耐久校验
bun run test:production  # 本机隔离的角色、PostgreSQL、Redis Sentinel 和 PITR 演练
bun run test:alerts      # 本机验证 Prometheus 到 Alertmanager 和 HTTP 接收器
bun run bench            # 单进程真实 PG 17 (fsync=on) 基准
bun run bench:single     # 单独立 Gateway 进程极限并发基准（128c-1024c JSON / 512c SSE / 512c 单热点钱包）
bun run bench:prod       # 双 Gateway + Worker + Redis 7.4 拓扑基准（--stress 长流和故障切换，--limit 512/1024 并发）
bun run verify           # 一键执行全部门禁
```
