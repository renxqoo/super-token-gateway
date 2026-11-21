# Super Token Gateway

企业级高性能 LLM Gateway 与微批核心账本系统，基于 **Bun 1.4.4 / TypeScript + Hono + PostgreSQL 17 + 极薄 Rust `wasmtime` 插件沙箱**，单一架构无双轨遗留。业务系统（未来 `apps/business`）与网关/账本物理分离，仅通过幂等内部 HTTP API 与异步 Outbox 协作。

完整架构决策、14 表核心账本设计与验收标准见 [ADR 0009](docs/decisions/0009-enterprise-gateway-worker-architecture.md)。

## 目录结构

| 路径                     | 职责                                                                                                                                                                                                                                          |
| ------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `apps/gateway`           | 高性能无状态公开网关（Chat/Responses/Messages/Gemini/Embeddings/Moderations/Images/Audio、`/livez`、`/readyz`、Prometheus 直方图 `/metrics`、`traceparent` 链路追踪、零明文 JSON 访问日志）+ Zod 严格校验 `/internal/v1/*` 管理与分页对账 API |
| `apps/worker`            | 后台账务与调度 Worker（PG Advisory Lease 选主、超时预扣扫尾、批次过期、自动收债、供应商分片再平衡、Outbox 推送）                                                                                                                              |
| `apps/mock-providers`    | 独立测试用上游替身（OpenAI / Anthropic / Gemini / Azure / Bedrock / Vertex / DeepSeek / Ollama）                                                                                                                                              |
| `packages/ledger`        | 精简 PG 核心账本：`drizzle-orm/bun-sql` 强类型 Schema/目录 CRUD/Outbox/归档/分页对账查询 + 原生 `Bun.SQL` 2–5 ms 同钱包微批聚合热路径                                                                                                         |
| `packages/protocol`      | 多协议无损透传、跨协议转换、有界 SSE 流解析与精确计量                                                                                                                                                                                         |
| `packages/plugin-engine` | 内容处理插件管道（极薄 Rust `wasmtime` FFI 沙箱 + 外部 HTTP Webhook 风控、流式滑窗、请求/响应改写与熔断）                                                                                                                                     |
| `packages/runtime`       | SmartRouter 智能路由熔断器、Redis 集群协同（跨节点限流/熔断与配置纪元 Pub/Sub 毫秒同步 + 单机降级）、SSRF/DNS-Pinning 连接池                                                                                                                  |
| `packages/kernel`        | 纯函数精确 `BigInt` 有理数计价与预扣上界计算                                                                                                                                                                                                  |
| `packages/contracts`     | 公共 Schema 校验与错误类型                                                                                                                                                                                                                    |
| `native/plugin-wasmtime` | 极薄 Rust `wasmtime` cdylib 沙箱（~190 行，零 WASI/网络/FS，硬内存与 Fuel 上限）                                                                                                                                                              |

## 核心特性

1. **资金扣款零差错底线（`packages/ledger`）**：
   - 14 张核心表（热路径 5 张），PostgreSQL 强约束（批次守恒 `CHECK`、不可变凭证触发器、`DEFERRABLE` 复式借贷平衡触发器）。
   - 多币种/积分（`PTS`/`USD`/`CNY`）`NUMERIC(38,0)` 原子计量，精确有理数 `BigInt` 计价与单次向上取整。
   - 多批次（`CASH`/`GRANT`）优先级与严格过期，过期预扣余量永不复活；退款按逆序归还批次；有界授信与自动收债冻结；16 分片供应商资金池与 `UNKNOWN` 预扣保守保留。
   - 2–5 ms 同钱包微批聚合 + 稳定锁序，消除高并发单钱包 `55P03` 锁抖动。
   - **零明文落库**：Prompt、回复文本与工具内容永不写入数据库、日志或 Outbox 事件。
2. **智能路由与动态故障切换（`SmartRouter`）**：
   - 优先级分层 + `WEIGHTED` / `ROUND_ROBIN` / `LEAST_LATENCY` 策略。
   - 通道级单调时钟熔断器（`CLOSED` / `OPEN` / `HALF_OPEN` 单探针 + 指数退避）、429 `Retry-After` 冷却与 401/403 故障隔离。
   - 首字节前自动无感故障切换：原子释放失败通道供应商预扣、为更贵备选路由补足客户预扣、绝不重复扣款；首字节后流式中断永不拼接其他模型输出。
3. **企业级内容处理与风控插件（`packages/plugin-engine`）**：
   - 极薄 Rust `wasmtime` 沙箱执行微秒级 PII 脱敏/规则重写（Ed25519 签名校验 + 流式滑动窗口跨 chunk 捕获）。
   - 外部 HTTP Webhook 风控支持 `HOLD_UNTIL_APPROVED`（零违规字节泄漏）与 `STREAM_AND_CUTOFF`（边推边检即时截断上游），独立熔断器与 `FAIL_CLOSED` / `FAIL_OPEN` 降级策略。
   - 拦截计费策略支持按路由/插件配置 `CHARGE_CUSTOMER`（默认客户承担）或 `PLATFORM_ABSORB`（平台兜底并记入 `system:moderation_loss` 复式凭证）。

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
bun run bench:prod       # 双独立 Gateway 进程 + 500 并发长流 SSE 生产拓扑基准
bun run verify           # 一键执行全部门禁
```
