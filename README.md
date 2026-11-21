# Super Token Gateway

企业级高性能 LLM Gateway 与微批核心账本系统，基于 **Bun 1.4.4 / TypeScript + Hono + PostgreSQL 17 + 极薄 Rust `wasmtime` 插件沙箱**，单一架构无双轨遗留。业务系统（未来 `apps/business`）与网关/账本物理分离，仅通过幂等内部 HTTP API 与异步 Outbox 协作。

完整架构决策、核心账本分片租约设计与验收标准见 [ADR 0009](docs/decisions/0009-enterprise-gateway-worker-architecture.md)。

## 目录结构

| 路径                     | 职责                                                                                                                                                                                                                                          |
| ------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `apps/gateway`           | 高性能无状态公开网关（Chat/Responses/Messages/Gemini/Embeddings/Moderations/Images/Audio、`/livez`、`/readyz`、Prometheus 直方图 `/metrics`、`traceparent` 链路追踪、零明文 JSON 访问日志）+ Zod 严格校验 `/internal/v1/*` 管理与分页对账 API |
| `apps/worker`            | 后台账务与调度 Worker（PG Advisory Lease 选主、超时预扣与过期节点租约扫尾 `sweepExpiredLeases`、批次过期、自动收债、供应商分片再平衡、Outbox 推送）                                                                                           |
| `apps/mock-providers`    | 独立测试用上游替身（OpenAI / Anthropic / Gemini / Azure / Bedrock / Vertex / DeepSeek / Ollama）                                                                                                                                              |
| `packages/ledger`        | 精简 PG 核心账本：`drizzle-orm/bun-sql` 强类型 Schema（23 张表 1:1 零漂移）/目录 CRUD/Outbox/归档/分页对账查询 + 原生 `Bun.SQL` 自适应 PG 分片资金租约（`wallet_leases`/`supplier_leases`）、2 ms 微批与 9 表单 CTE 原子结算热路径            |
| `packages/protocol`      | 多协议无损透传、跨协议转换、有界 SSE 流解析、精确 JSON 快速路径与精确计量                                                                                                                                                                     |
| `packages/plugin-engine` | 内容处理插件管道（内置 Bun 隔离 Worker `WebAssembly` 沙箱开箱即用 + 可选 Rust `wasmtime` FFI 加速、复用 SSRF/DNS-Pinning 传输层的外部 HTTP Webhook 风控、流式滑窗、请求/响应改写与熔断）                                                      |
| `packages/runtime`       | SmartRouter 智能路由熔断器（含 `probeTimeoutMs` 防死锁自愈）、Redis 集群协同（跨节点限流/熔断与配置纪元 Pub/Sub 毫秒同步 + 单机降级）、SSRF/DNS-Pinning 高复用 keep-alive 连接池                                                              |
| `packages/kernel`        | 纯函数精确 `BigInt` 有理数计价与预扣上界计算                                                                                                                                                                                                  |
| `packages/contracts`     | 公共 Schema 校验与错误类型                                                                                                                                                                                                                    |
| `native/plugin-wasmtime` | 极薄 Rust `wasmtime` cdylib 沙箱（~190 行，零 WASI/网络/FS，硬内存与 Fuel 上限）                                                                                                                                                              |

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
3. **企业级内容处理与风控插件（`packages/plugin-engine`）**：
   - WASM 插件内置 Bun 隔离 Worker `WebAssembly` 沙箱开箱即用（零外部工具链依赖，同样 ABI 无缝兼容可选 Rust `wasmtime` FFI 加速）：强制零 Host Import、单次调用独立零化线性内存防跨租户泄漏、内存硬上限校验、死循环超时硬终止、Ed25519 签名校验与流式滑动窗口跨 chunk 捕获。
   - 外部 HTTP Webhook 风控支持 `HOLD_UNTIL_APPROVED`（零违规字节泄漏）与 `STREAM_AND_CUTOFF`（边推边检异步截断上游并精确按策略结算），支持 `manifest.hooks` 精准过滤、独立熔断与冷却恢复、注册与调用全链路 SSRF/DNS-Pinning 校验及 `FAIL_CLOSED` / `FAIL_OPEN` 降级策略。
   - 拦截计费策略支持按路由/插件配置 `CHARGE_CUSTOMER`（默认客户承担）或 `PLATFORM_ABSORB`（平台兜底并记入 `system:moderation_loss` 复式凭证），流式与非流式审计明细完整持久化至 `billing_charges.plugin_audit`。

## 实测性能（Bun 1.4.4 + 真实 PG 17 `fsync=on / synchronous_commit=on / full_page_writes=on`）

- **单独立 Gateway 进程（`BENCH_SECONDS=8 bun run bench:single`，593,882 笔实结，0 错误，守恒 `ok=true`，空闲退租 `active_leases=0`）**：
  - 短 Chat JSON（512c / 768c / 1024c）：**11,334 RPS**（P99 55.8 ms） / **12,457 RPS**（P99 99.7 ms） / **12,930 RPS**（P99 100.5 ms，GW CPU 81.7%，RSS 308.7 MB）
  - 单热点钱包 512 并发打同一钱包：**11,485 RPS**（P99 67.9 ms，0 错误，WAL 3,458 B/笔）
  - 短流式 SSE（512c，`[DONE]` 前原子落库）：**9,165 RPS**（P99 103.0 ms，0 错误）
- **双独立 Gateway + Worker + Redis 7.4 生产拓扑（`bun run bench:prod --stress`，385,752 笔实结，0 错误，守恒 `ok=true`）**：
  - 15s 持续 JSON（256c）：**9,034 RPS**（P99 50.0 ms）；1,000 并发 5s 长 SSE 活流：**1,000/1,000 成功**（RSS 仅增 1.7 MB）；实时 503 故障切换：**1,000/1,000 成功**（64 次跨节点切换）。

## 快速开始与部署

需要 Bun **1.4.4** 与 PostgreSQL 17（本地验证支持 `scripts/native-services.ts` 或 `infra/docker-compose.yml`）。

```sh
bun run setup:local
bun run infra:up
bun run db:migrate
bun run dev
```

单机一体化启动或集群高可用部署：

```sh
# 构建无需 node_modules 的生产产物
bun run build

# 可选：构建 Rust wasmtime 插件动态库（需要本地安装 rustc/cargo）
bun run build:wasmtime

# 单机模式（Gateway + 内置 Worker）
bun dist/gateway/main.js --mode=all-in-one

# 集群高可用模式（多无状态 Gateway + 主备 Worker Advisory Lease 选主）
bun dist/gateway/main.js --mode=gateway
bun dist/worker/main.js
```

Docker Compose / Systemd / Caddy 部署模板见 [`infra/`](infra/)。

## 验证与基准

```sh
bun run check            # Bun TypeScript 类型检查
bun run lint             # oxlint（零告警零错误）
bun run fmt:check        # 代码格式检查
bun run test             # 单元测试与跨模块架构约束
bun run test:integration # 真实 PG 17 / HTTP 集成测试（账本 + 智能路由 + 插件风控 + Worker）
bun run build            # 打包 dist/ 独立产物
bun run test:artifact    # 隔离目录端到端产物验收与归档校验
bun run test:durability  # SIGKILL 强杀预扣回收 + pg_dump/psql 备份恢复耐久校验
bun run bench            # 单进程真实 PG 17 (fsync=on) 基准
bun run bench:single     # 单独立 Gateway 进程极限并发基准（128c-1024c JSON / 512c SSE / 512c 单热点钱包）
bun run bench:prod       # 双独立 Gateway 进程 + Worker + Redis 7.4 生产拓扑基准（支持 --stress）
bun run verify           # 一键执行全部门禁
```
