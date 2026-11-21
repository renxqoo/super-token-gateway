# ADR 0009: 企业级 Bun 主力架构（Gateway + Worker、精简微批 PG 核心账本、智能路由与插件管道）

- **状态**：待确认实施
- **日期**：2026-11-21
- **适用范围**：`apps/gateway`、`apps/worker`、`native/plugin-wasmtime`、`packages/{kernel,contracts,protocol,plugin-engine,ledger,runtime}`（后续同仓扩展 `apps/business` 业务系统）

---

## 1. 背景、目标与非目标

### 1.1 问题根因

1. **旧 v2 三进程栈性能与维护双瓶颈**：单笔请求跨 20+ 张表写约 45 KB PG WAL，4,600+ 行巨型 PL/pgSQL 消耗约 20 ms PG CPU/笔，加上 Gateway → Core API 4 次同步 mTLS RPC 与每条 SSE 流死占 1 个插件槽位，实测仅稳定通过 175 RPS。
2. **临时 Go 原型造成双轨并存**：`apps/v3-engine` 验证了单机物理极限，但不具备多协议、多批次 lot 账本、插件处理 LLM 内容与易部署控制面，与 v2 形成新旧两套代码并存。

### 1.2 核心目标

1. **高性能与高可用**：以 **Bun 1.4.4 / TypeScript 为唯一主力栈**，依托标准 PostgreSQL + 可选 Redis，通过精简核心账本表、同钱包微批聚合提交、本地配置快照与无状态水平扩展，实现单机万级 RPS 能力与万级并发长 SSE 稳定运行。
2. **扣款准确无差错（绝对底线）**：100% 保留精确 `BigInt` 有理数单次向上取整、多币种/积分 `atoms`、多批次 `CASH`/`GRANT` lot（优先级、有效期、模型/操作限定、过期不复活）、原路退款、有界授信/欠款补扣/争议冻结、客户+供应商双侧双式平衡分录、`UNKNOWN` 挂账闭环与零模型正文持久化。
3. **可扩展的 LLM 内容处理插件体系**：内置主流官方协议原生高速实现；通过 **极薄 Rust `wasmtime`（`bun:ffi` 同进程池化）** 与 **外部 HTTP Webhook 风控插件**，安全隔离地对 LLM 请求与返回（含流式 SSE）进行脱敏、合规审查、改写与拦截止损。
4. **智能路由与故障动态切换**：基于优先级分层、策略选路（`WEIGHTED`/`ROUND_ROBIN`/`LEAST_LATENCY`）、实时 EWMA 延迟、单调时钟熔断器与 429 冷却期，在渠道故障时自动无缝切换至健康备用渠道，且严格保证绝不向客户重复扣款。
5. **双系统解耦与极简部署**：业务系统（用户/积分/Model CRUD，后续同仓 `apps/business` 开发）与本阶段交付的 **Gateway + Worker** 物理分库、通过幂等内网 API 与异步 Outbox 契约解耦；单机 `all-in-one` 模式仅需 `Bun + PostgreSQL` 即可一键运行，集群按需水平扩容。
6. **零双轨并存**：重构收口后彻底移除 `apps/v3-engine`、`apps/core-api`、`apps/core-worker`、`packages/core-client`、旧 Go stdio `packages/plugin-host` 及旧 60+ 表巨型 PL/pgSQL，全仓只保留一套最新实现。

### 1.3 非目标（本阶段排除）

- 不在本阶段开发 `apps/business` 商业用户中心、登录鉴权、支付渠道对接或运营前端（本阶段只冻结其与 Gateway + Worker 之间的内网管理 API 与 Outbox 契约）。
- 不自研多节点分布式共识/Quorum WAL 存储集群；持久化与高可用完全依托标准 PostgreSQL（主备/RDS）与标准 Redis。
- 不持久化任何模型输入/输出文本、工具参数或媒体正文。

---

## 2. 系统架构与模块职责

```mermaid
flowchart LR
    BIZ["系统 A: apps/business (后续同仓)<br/>用户/积分/Model CRUD 与商业前台"] -->|"内网幂等管理 API<br/>(配置同步 + 充值/退款/调账)"| INT["Gateway 内网管理端口<br/>/internal/*"]
    Client["Client (HTTP/1.1 / H2 / SSE)"] --> GW["apps/gateway (无状态 Bun 多进程)<br/>本地配置快照 + 批量配额<br/>智能路由/熔断/动态切换<br/>原生多协议 + 插件管道<br/>2-5ms 微批同钱包聚合账本"]
    GW <-->|"bun:ffi 同进程池化<br/>Fuel 抢占 + 内存页重置"| WASM["native/plugin-wasmtime<br/>极薄 Rust WASM 沙箱"]
    GW <-->|"微批滑窗送审<br/>独立超时/熔断/降级"| HOOK["外部 HTTP Webhook 风控"]
    GW -->|"SSRF/DNS Pinning<br/>故障自动切换"| LLM["多上游 LLM 供应商"]
    GW <-->|"精简 5 表微批原子事务<br/>PG 单一事实源"| PG[("标准 PostgreSQL 17<br/>唯一权威账本 + 配置表")]
    INT --> PG
    INT -.->|"版本号快照增量推送<br/>(内存/Redis PubSub)"| GW
    WK["apps/worker (PG Lease 高可用守护)<br/>UNKNOWN 恢复 / 欠款补扣<br/>过期Lot清算 / Cell重平衡<br/>Outbox 投递 / 月度归档"] <--> PG
    WK -->|"异步 Outbox 账单明细回流"| BIZ
```

### 2.1 目标目录边界（单栈唯一实现）

| 路径                     | 职责                                                                                                                                         | 权威状态归属                                                  |
| ------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------- |
| `apps/gateway`           | 公网推理网关（`/v1/*`）+ 独立端口内网管理/资金 API（`/internal/*`），支持 `--mode=all-in-one\|gateway`                                       | 无持久化权威状态；内存持有配置快照、路由健康/熔断态与微批队列 |
| `apps/worker`            | 后台高可用守护（PG Advisory Lease 选主）：`UNKNOWN` 恢复、欠款自动补扣、过期 lot 清算、供应商 Cell 重平衡、Outbox 事件投递、月度分区与冷归档 | 后台任务租约与投递游标                                        |
| `apps/mock-providers`    | 独立 Hono 测试上游替身（零内部依赖）                                                                                                         | 仅测试态                                                      |
| `native/plugin-wasmtime` | 极薄 Rust `wasmtime` 沙箱共享库（~600 行）：AOT 池化、内存硬上限、Fuel 抢占、实例内存页清零、同步/异步双通道 `bun:ffi` ABI                   | 编译后模块缓存与实例池                                        |
| `packages/kernel`        | 零外部依赖纯函数：`BigInt` 有理数精确计费（`NUMERIC(38,0)`）、规范 JSON、维度与错误契约                                                      | 无状态                                                        |
| `packages/contracts`     | 公开推理、内网配置/资金同步、插件 Manifest/Hook、Outbox 事件的 Zod Schema 与类型契约                                                         | 无状态                                                        |
| `packages/protocol`      | 原生多协议转换（OpenAI / Anthropic / Gemini / Azure / Bedrock / Vertex / Embeddings / Audio / Images / Files / Batch）与零缓冲流式 SSE 解析  | 无状态                                                        |
| `packages/plugin-engine` | 统一插件管道编排（Rust `wasmtime` FFI 绑定 + 外部 HTTP Webhook 执行器 + 有界滑窗缓冲 + Ed25519 包验签）                                      | 插件实例与熔断态                                              |
| `packages/ledger`        | 精简 PG 核心账本 Schema、单一基线迁移、`BigInt` 领域状态机、2–5 ms 同钱包微批提交器、账务守恒校验与归档工具                                  | **PG 核心财务表唯一写入层**                                   |
| `packages/runtime`       | SSRF/DNS Pinning 传输层、加密/签名（AES-GCM/HMAC/Ed25519/SigV4）、本地+Redis 批量限流、智能路由健康与熔断器                                  | 本地/Redis 限流与健康态                                       |

### 2.2 彻底清理的旧双轨代码（实施完毕后零残留）

- 删除：`apps/v3-engine`、`scripts/v3-engine.ts`、`apps/core-api`、`apps/core-worker`、`packages/core-client`、`packages/plugin-host`（含旧 Go stdio `native/`）、`packages/plugin-sdk`、`packages/adapters`（协议层迁入 `packages/protocol`）、`packages/storage`（财务与目录层重构入 `packages/ledger`）、空目录 `apps/admin` 与 `apps/client`。

---

## 3. 智能路由与渠道故障动态切换

### 3.1 候选过滤与排序（纯内存，< 10 µs）

1. **快照查表**：根据 `(model, operation)` 取出全部已启用 `routes`（关联 `channel`、`upstream_account`、客户价目与供应商成本价目）。
2. **硬过滤**：剔除禁用路由/渠道、协议不支持项、熔断状态 `OPEN`、处于 429 冷却期（`cooldown_until > now`）、或本地已知供应商账户冻结/耗尽的渠道；若模型不支持所请求托管工具且无计量维度，直接返回 `422 unmetered_tool`。
3. **分层选路**：按 `priority DESC` 分组，同优先级内按模型策略无放回排序：
   - `WEIGHTED`：按 `weight * health_factor` 加权随机；
   - `ROUND_ROBIN`：进程内原子计数轮询；
   - `LEAST_LATENCY`：按渠道实时 EWMA 延迟（`0.8 * prev + 0.2 * sample`）升序。

### 3.2 单调时钟熔断状态机与 429 冷却

- 全部时间基于单调时钟（`performance.now()`），免疫系统挂钟回拨：
  - `CLOSED → OPEN`：连续可重试上游失败 `>= failureThreshold`（默认 3）或滑动窗错误率 `> 50%`（样本 `>= 10`）；熔断时长 `openDurationMs`（默认 15s，指数退避上限 120s）。
  - `OPEN → HALF_OPEN`：冷却到期后仅放行 1 路并发探针；探针成功立即重置为 `CLOSED`，失败立即回到 `OPEN`。
  - **429 精准避让**：解析上游 `Retry-After`（默认 5s，上限 60s）写入 `cooldown_until`，立即切走且不将临时限流误判为永久宕机。
  - **渠道鉴权故障快速隔离**：上游返回渠道级 `401`/`403`（上游 Key 失效或欠费）立即进入长冷却（60s）并上报渠道故障告警，自动切换备用渠道。
  - **集群同步**：多实例部署时通过 Redis Pub/Sub 异步广播熔断/冷却事件（零同步阻塞）。

### 3.3 动态故障切换与资金安全边界（核心铁律）

| 故障阶段与类型                                                                                                            | 是否允许动态切换                                                | 旧 Attempt 供应商侧资金处理                                                 | 客户侧资金处理                                                                                                     |
| ------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------- | --------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| **Pre-First-Byte 确定未消费失败**：TCP/DNS/TLS 失败、连接超时、`429`、`500`/`502`/`503`/`504`/`529`、渠道 Key `401`/`403` | **允许**（上限 `max_attempts` 默认 3，受总请求 deadline 约束）  | 标记 `FAILED`，原子释放旧 attempt 的供应商 Cell 预扣                        | **绝不重复预扣/扣款**；仅保持单一请求 `billing_hold`，若备选渠道更贵则原子补足差额预扣，最终按成功渠道价目精确结算 |
| **Pre-First-Byte 结果未明**：请求已发出后遭遇断连或读头超时                                                               | **按路由策略可选**（仅幂等请求或路由允许 `retry_on_ambiguous`） | 标记 `UNKNOWN`，**保留旧 attempt 供应商风险预扣**交 Worker 对账，不静默释放 | 客户侧始终只按最终成功的唯一 `selected_attempt` 结算一次；若全部尝试均未明则保留客户 `UNKNOWN` 占位                |
| **不可重试客户端错误**：`400`、`404`、`413`、`422`、上游内容政策拒绝                                                      | **禁止切换**，直接返回                                          | 标记 `FAILED`，原子释放供应商预扣                                           | 原子释放客户 `billing_hold`（0 扣费）                                                                              |
| **Post-First-Byte（已向客户端输出首字节 SSE）**                                                                           | **禁止切换**（防止半截拼接另一模型正文）                        | 按已观测 usage 结算或保留 `UNKNOWN` 风险，同时触发故障渠道降权/熔断         | 按已确认证据结算或挂 `UNKNOWN` 待对账，明确返回错误终止帧                                                          |

---

## 4. 精简微批 PG 核心账本与数据结构设计

### 4.1 财务不变量底线（数据库硬约束 + 领域层双重守卫）

1. **金额类型**：全部金额为整数原子单位 `NUMERIC(38,0)`（TS 侧 `BigInt`），支持任意 3–8 位币种/积分代码（如 `USD`、`CNY`、`PTS`）。
2. **精确计费**：所有计量维度按有理数 `numerator / denominator` 通分求和后**单次向上取整**，无任何浮点运算。
3. **Lot 守恒铁律**：`wallet_lots` 每行强制数据库约束 `issued_atoms = available_atoms + reserved_atoms + spent_atoms + expired_atoms`，且各项 `>= 0`。
4. **过期不可复活铁律**：结算返还多余预扣或退款回流时，若原 lot 已过期（`expires_at <= now()`），必须转入 `expired_atoms`，严禁重新变为 `available_atoms`。
5. **双式平衡与不可变铁律**：`ledger_entries` 禁止 `UPDATE` 与 `DELETE`；同一 `journal_id` 至少 2 条分录且 `SUM(atoms) = 0`。
6. **唯一计费幂等铁律**：`billing_charges.request_id` 全局唯一；充值/退款/调账按 `(scope, external_ref)` 唯一且校验载荷 SHA-256 指纹冲突（`409`）。
7. **隐私红线**：任何表、日志、Outbox 事件均不存储 LLM 输入/输出正文或工具参数。

### 4.2 精简核心表结构（热路径仅触碰 5 张核心表，目标 PG WAL ≤ 2–3 KB/笔）

#### A. 配置与路由表（由 `/internal/*` 写入，Gateway 启动加载并内存热更新，热路径 0 DB 读）

- `projects (id PK, external_ref UNIQUE, name, currency, rpm, tpm, concurrency, enabled, config_version, created_at, updated_at)`
- `api_keys (id PK, project_id FK, verifier UNIQUE, prefix, role, allowed_cidrs text[], models text[], operations text[], enabled, expires_at, created_at)`
- `upstream_accounts (id PK, name, state CHECK('ACTIVE','DISABLED','RECONCILE_REQUIRED'), revision, created_at)`
- `channels (id PK, account_id FK, name, kind, base_url, secret_ciphertext, rpm, concurrency, enabled, routing_policy jsonb, config_revision, updated_at)`
- `models (name PK, operations text[], routing_strategy CHECK('WEIGHTED','ROUND_ROBIN','LEAST_LATENCY'), max_output_tokens, enabled)`
- `routes (id PK, model_name FK, channel_id FK, upstream_model, operations text[], priority, weight, billing_price_id FK, cost_version_id FK, request_policy jsonb, plugin_pipeline jsonb, enabled)`
- `price_versions (id PK, side CHECK('CUSTOMER','SUPPLIER'), currency, basis CHECK('PROVIDER_USAGE','REQUEST_COMPLETION','INPUT_CHARACTERS'), rates jsonb, ceilings jsonb, source_digest, created_at)` — 不可变触发器保护
- `plugins (id PK, name, version, kind CHECK('WASM','WEBHOOK'), digest, wasm_bytes bytea CHECK(octet_length<=16777216), webhook_config jsonb, signature, signing_key_id, manifest jsonb, enabled, created_at)`

#### B. 核心资金账本表（热路径 5 表）

1. **`wallets`**（客户钱包主状态，每 `(project_id, currency)` 一行）：
   - `id PK`, `project_id FK`, `currency`, `policy CHECK('GIFT_FIRST','CASH_FIRST','EARLIEST_EXPIRY')`
   - `available_atoms`, `reserved_atoms`, `spent_atoms`, `expired_atoms`, `debt_atoms`, `credit_limit_atoms`, `credit_reserved_atoms`（均 `NUMERIC(38,0) CHECK >= 0`）
   - `credit_policy_version NUMERIC(38,0)`, `manual_frozen bool`, `evidence_disputed bool`, `freeze_reason text`
   - 唯一索引：`UNIQUE(project_id, currency)`
2. **`wallet_lots`**（客户批次资金源）：
   - `id PK`, `wallet_id FK`, `kind CHECK('CASH','GRANT')`, `priority int`, `models text[]`, `operations text[]`, `expires_at timestamptz`
   - `issued_atoms`, `available_atoms`, `reserved_atoms`, `spent_atoms`, `expired_atoms`（强制行级守恒 `CHECK`）
   - 部分索引：`INDEX ON (wallet_id, priority, expires_at) WHERE available_atoms > 0`
3. **`supplier_cells`**（供应商 16 分片资金单元，消除大渠道单行锁）+ `supplier_root_balances`：
   - `supplier_cells (account_id, currency, cell SMALLINT CHECK(0..15), PRIMARY KEY(account_id, currency, cell), balance_atoms, reserved_atoms, spent_atoms, revision)`
4. **`billing_holds`**（轻量过渡占位，正常请求终态即闭合；异常 `UNKNOWN` 永久留存待对账）：
   - `request_id PK`, `project_id`, `wallet_id`, `operation`, `model`, `idempotency_key`, `fingerprint`
   - `held_atoms`, `credit_atoms`, `lot_allocations jsonb`（`[{lot_id, amount_atoms}]`）, `supplier_holds jsonb`（`[{attempt_id, account_id, cell, held_atoms, state}]`）
   - `state CHECK('HELD','SETTLED','RELEASED','UNKNOWN','CONFLICT')`, `expires_at`, `created_at`, `closed_at`
   - 唯一部分索引：`UNIQUE(project_id, operation, idempotency_key) WHERE idempotency_key IS NOT NULL`；回收部分索引：`INDEX ON (expires_at) WHERE state IN ('HELD','UNKNOWN')`
5. **`billing_charges`** + **`ledger_entries`**（按月分区不可变终态凭证与统一双式分录）：
   - `billing_charges (id PK, request_id UNIQUE, project_id, wallet_id, selected_attempt_id, channel_id, model, operation, currency, billing_price_id, cost_version_id, usage jsonb, usage_digest, customer_atoms, funded_atoms, debt_atoms, refunded_atoms, supplier_cost_atoms, supplier_currency, lot_allocations jsonb, plugin_audit jsonb, charge_policy, outcome, created_at)`
   - `ledger_entries (journal_id, ordinal, created_at, side CHECK('CUSTOMER','SUPPLIER'), entity_id, request_id, reference, kind, currency, account, atoms)` — **按 `created_at` 月分区**，不可变触发器 + 事务级 `SUM(atoms)=0` 约束；`account` 枚举覆盖 `lot:<id>:(available|spent|expired)`、`system:(funding|revenue|receivable|moderation_loss|adjustment)`、`upstream:(available|expense|funding)`。

#### C. 配套幂等运维与事件回流表

- `funding_operations (id PK, side, entity_id, kind CHECK('CREDIT','REFUND','ADJUST','SUPPLIER_RECHARGE','SUPPLIER_ADJUST'), external_ref, fingerprint, amount_atoms, currency, payload jsonb, receipt jsonb, created_at)` — `UNIQUE(side, entity_id, kind, external_ref)`
- `billing_resolutions (id PK, request_id UNIQUE, action CHECK('SETTLE','RELEASE'), billing_price_id, usage jsonb, reason, actor, created_at)`
- `outbox_events (seq BIGSERIAL PK, event_id UNIQUE, kind, request_id, project_id, payload jsonb, delivered_at, created_at)` + `outbox_consumers (name PK, acked_seq, updated_at)`

### 4.3 2–5 ms 同钱包微批聚合事务协议（强类型 Bun/TS）

1. **收集窗口**：微批提交器按 `maxWaitMs = 2..5` 或 `maxBatch = 64` 聚集一批 `AcquireHold` / `AdjustHold` / `CommitTerminal` 请求。
2. **全局升序锁序**：提取本批所有 `wallet_id` 升序排列 + `supplier_cells` 键升序排列，彻底避免 PG 死锁。
3. **一次性批量拉取**：单次 `SELECT ... FROM wallets WHERE id = ANY($1) ORDER BY id FOR NO KEY UPDATE` + `wallet_lots` + `supplier_cells`。
4. **Bun 强类型内存精确计算与聚合**：
   - 同一钱包本批多笔请求（例如 50 笔并发打同一大客户钱包）在内存中按序完成 lot 扣减/实结、过期判断、授信/欠款校验，**直接消除旧版 `SKIP LOCKED → 55P03` 假失败**，且原生支持带 `expires_at` 的赠送 lot 快路径；
   - 单笔业务校验失败（余额不足、冻结、幂等指纹冲突）在内存中直接标错返回，不污染同批其他正常请求。
5. **集合式批量写回**：每批仅执行固定数量 `UNNEST` 批量 SQL（更新去重后的 `wallets`/`wallet_lots`/`supplier_cells` 净值，批量 `INSERT` `billing_holds`/`billing_charges`/`ledger_entries`/`outbox_events`）并 `COMMIT`。
6. **异常与提交未明恢复**：若 `COMMIT` 网络超时结果未知，Gateway 保留客户端 `UNKNOWN` 或按幂等键重查终态；未闭合 `billing_holds` 超过 `expires_at` 由 `apps/worker` 自动扫描对账或置 `UNKNOWN` 锁定待决议。

---

## 5. 插件管道设计（极薄 Rust `wasmtime` + 外部 HTTP Webhook）

### 5.1 统一管道语义

路由通过 `plugin_pipeline` 声明有序插件链（按 `priority` 顺序执行）：

- **Hook 点**：`on_request` →（上游调用）→ `on_response`（非流式）或 `on_stream_chunk` + `on_stream_end`（流式）。
- **动作返回值**：
  - `PASS`：放行原文；
  - `TRANSFORM`：替换为改写/脱敏后的文本，流向下游插件或客户端；
  - `BLOCK { rule_code, http_status, safe_message }`：立即短路后续插件并向 LLM 上游发出 `Abort` 止损。
- **权限铁律**：内容处理插件**无权写入或篡改 `usage` 计量维度**；仅带显式授权的协议适配器/计量插件可产出 usage。

### 5.2 极薄 Rust `wasmtime` 沙箱（`native/plugin-wasmtime`）

- **核心能力（~600 行 Rust，编译为 `libstg_plugin_wasmtime.{dylib,so}` 经 `bun:ffi` 调用）**：
  - AOT 预编译 `CompiledModule` 跨请求共享；
  - 每次执行借用池化实例，归还前强制执行线性内存页清零/重置，杜绝跨租户对话残留；
  - 硬约束：内存上限（默认 16–32 MiB）、**Fuel 指令步数抢占熔断**、零文件系统/零网络/零环境变量、加载前校验 Ed25519 签名与 SHA-256 摘要；
  - **双调度通道**：轻插件（Fuel 硬限 ≤ 200 µs）同步 FFI 直调；重插件（1–10 ms）提交 Rust 内部线程池异步执行，绝不卡死 Bun 事件循环；
  - **零长流占槽**：流式插件状态以受限字节（≤ 8 KB）寄存在 Gateway 连接上下文中，SSE 等待吐字期间占用 **0 个插件实例**。

### 5.3 外部 HTTP Webhook 风控插件

- **流式微批聚合**：按字符阈值（默认 256 chars）、时间窗（默认 300 ms）或句界聚合送审，并在 `[DONE]` 释放前必做一次终审，避免逐 token 轰炸外部风控。
- **两种可配置流式模式**：
  - `HOLD_UNTIL_APPROVED`（缓冲审核）：暂存当前有界小窗（上限 4 KB），Webhook 放行后再吐给客户端，违禁词零漏出；
  - `STREAM_AND_CUTOFF`（旁路审核）：首字直出保 TTFT，异步送审，命中 `BLOCK` 立刻截断后续流并中止上游。
- **弹性保护**：独立超时（默认 200 ms）、专用熔断器与可配降级策略（`FAIL_CLOSED` 严格阻断 / `FAIL_OPEN` 放行并记审计告警）。

### 5.4 拦截后的账务结算策略

- 供应商侧：始终按上游实际已消耗 usage 如实结算供应商成本；
- 客户侧：按路由/插件配置执行：
  - **`CHARGE_CUSTOMER`（默认）**：按实际已生成 usage 正常向客户扣费；
  - **`PLATFORM_ABSORB`**：全额释放/退还客户预扣，对应科目计入 `system:moderation_loss`，双式分录严格归零；
- 账单 `billing_charges.plugin_audit` 记录 `{plugin_id, version, action, rule_code, latency_us}`，不留存任何文本正文。

---

## 6. 业务系统对接契约（`apps/business` ↔ `Gateway + Worker`）

1. **内网管理与同步 API（独立监听端口 `/internal/v1/*`，Bearer/mTLS 鉴权）**：
   - 配置 CRUD 同步（幂等 upsert + `config_version` 递增并触发 Gateway 本地快照热更新）：`projects`、`api_keys`、`upstream_accounts`、`channels`、`models`、`routes`、`price_versions`、`plugins`。
   - 资金操作（单一权威写入 Gateway PG，强幂等 `external_ref`）：
     - `POST /internal/v1/wallets/:projectId/credits`（现金/赠送积分 Lot 入账）
     - `POST /internal/v1/charges/:chargeId/refunds`（精确原路退款）
     - `POST /internal/v1/charges/:chargeId/adjustments`（账单调账）
     - `POST /internal/v1/wallets/:projectId/credit-policy`（有界授信配置）
     - `POST /internal/v1/requests/:requestId/resolve`（`UNKNOWN` / 争议决议）
     - `GET /internal/v1/wallets/:projectId`、`GET /internal/v1/charges`（实时余额与账单明细查询）
2. **异步消费明细回流（Outbox）**：
   - `POST /internal/v1/events/claim` + `POST /internal/v1/events/ack`（以及可选 Worker 主动 Webhook 推送至业务系统），至少一次投递、按 `event_id` 幂等去重，回流 `request_id`、`usage`、精确扣款、`lot_allocations`、插件审计与最新余额快照。

---

## 7. 验收规则（客观门禁，全部必须通过）

### G1：财务准确与守恒底线（零容忍）

- **G1.1 精确计费**：覆盖 `1/3` 原子多维求和单次向上取整、18 位小数秒、超过 `Number.MAX_SAFE_INTEGER`（`9007199254740993`）直至 `NUMERIC(38,0)` 边界、缓存写入分区一致性检验。
- **G1.2 Lot 生命周期与原路退款**：跨带过期时间的 `GRANT` + `CASH` 多 lot 预扣→实结→多退（过期转 `expired_atoms` 不复活）→部分/全额原路退款→欠款自动补扣；每次操作后断言全库 `SUM(atoms)=0` 及每行 `issued = available + reserved + spent + expired`。
- **G1.3 热点并发与防双扣**：100 并发争抢同一钱包最后一笔余额，零负余额、零 `55P03` 假失败；相同 `idempotency_key` 并发重放只扣一次；进程异常后遗留 `UNKNOWN` 占位经 Worker 恢复与决议后全库守恒。
- **G1.4 插件拦截记账与隐私**：`CHARGE_CUSTOMER`（默认）与 `PLATFORM_ABSORB` 在非流式/流式拦截下分录严格平衡；全库与日志中零模型正文泄露。

### G2：智能路由与动态故障切换

- **G2.1 Pre-First-Byte 自动切换**：注入连接拒绝、超时、`500`/`502`/`503`、`429`（含 `Retry-After`）、渠道 Key `401` 故障，断言自动无缝切换至备用健康渠道并返回 200；旧渠道供应商预扣精确释放（确定失败）或留存 `UNKNOWN`（发后超时），客户侧仅扣 1 次且按实际成功渠道价目计费。
- **G2.2 熔断与恢复**：连续失败达阈值进入 `OPEN` 后，后续请求 0 ms 跳过故障渠道；冷却到期进入 `HALF_OPEN` 单探针成功后自动恢复 `CLOSED`。
- **G2.3 策略与流式边界**：验证 `WEIGHTED`/`ROUND_ROBIN`/`LEAST_LATENCY` 选路分布；验证 Post-First-Byte SSE 断流时不拼接另一模型文本、正确结算并触发渠道降权熔断。

### G3：插件内容处理与安全隔离

- **G3.1 Rust `wasmtime` 能力与隔离**：验证非流式与流式（含跨 chunk 拆分敏感词有界滑窗）脱敏/改写/拦截；恶意插件（死循环、内存爆分配、文件/网络越权、跨租户内存探测）100% 被 Fuel/上限/页重置拦截，宿主存活且其他并发请求 0 报错。
- **G3.2 外部 Webhook 风控**：验证 `HOLD_UNTIL_APPROVED`（违禁词 0 字节漏出）、`STREAM_AND_CUTOFF`（命中即断并中止上游）、超时/5xx 触发 `FAIL_CLOSED`/`FAIL_OPEN` 及独立熔断。

### G4：性能与并发目标（本机 Apple M6 12 核 + 标准 PG 17 `fsync=on`）

- 短 Chat JSON（含完整多 lot 微批 PG 账本 + 外部 ACK）：稳定 **≥ 5,000 RPS（冲刺 10,000+ RPS）**，P99 ≤ 200 ms，错误率 0%，PG WAL ≤ 3 KB/笔。
- 短 SSE（`[DONE]` 后置落库）：稳定 **≥ 4,000 RPS（冲刺 8,000+ RPS）**，错误率 0%。
- 长 SSE 并发（2–5 秒活流 + 启用流式内容插件）：单机稳定 **≥ 10,000 路并发活流**，0 槽位错误，排空后预扣归零。
- 单热点钱包压测（100 并发打同一钱包）：0 `55P03` 拒绝，余额守恒 100% 精确。

### G5 & G6：单栈无双轨、易部署与工程门禁

- 全仓零残留旧双轨代码（`apps/v3-engine`、`apps/core-api`、`apps/core-worker`、`packages/core-client`、旧 Go stdio `plugin-host` 等彻底删除）。
- `all-in-one` 模式仅需 `Bun + PostgreSQL` 单条命令启动并完成全链路闭环；独立 `dist/` 产物无 `node_modules` 启动验收通过。
- `bun run check`、`bun run lint`（0 warning/0 error）、`bun run fmt:check`、`bun run test`、`bun run test:integration`、`bun run build`、`bun run test:artifact` 全绿，且严格遵守 `AGENTS.md`（零代码注释）。

---

## 8. 分阶段高效实施路线（垂直切片，每阶段独立可验收）

1. **阶段 1 — 精简微批 PG 核心账本（`packages/ledger`）**：落地精简 14 表单一基线迁移、强类型 Bun/TS 双式财务引擎、2–5 ms 同钱包微批聚合器，跑通全部 **G1** 财务守恒与热点并发测试。
2. **阶段 2 — 全新 `apps/gateway` + 智能路由 + 原生多协议 + 外部 Webhook 插件**：收口 `packages/protocol`、构建本地配置快照、批量限流、智能路由/熔断/Pre-First-Byte 故障切换、内网同步 API 与外部 HTTP Webhook 审核管道，跑通 **G2** 与 **G3.2**。
3. **阶段 3 — 全新 `apps/worker`**：落地 PG Lease 选主、`UNKNOWN` 挂账恢复、欠款自动补扣、过期 lot 清算、供应商 Cell 重平衡、Outbox 投递与月度冷归档，打通 `all-in-one` 与拆分模式端到端验收。
4. **阶段 4 — 极薄 Rust `wasmtime` 插件引擎（`native/plugin-wasmtime` + `packages/plugin-engine`）**：实现 AOT 池化、Fuel 抢占、页重置与同步/异步 `bun:ffi` ABI，接入流式滑窗管道并跑通 **G3.1** 安全隔离与内容处理验收。
5. **阶段 5 — 彻底清理旧双轨 + 性能压测 + 全门禁收口**：删除全部旧双轨应用与包，同步更新 OpenAPI、部署模板、`README.md` 与架构门禁测试，完成 **G4/G5/G6** 全量验收。

---

## 9. 实施前唯一环境前置项

- **本机 Rust 工具链**：已核查当前环境 `PATH` 与 `.local/toolchain/` 中尚未安装 `rustc` / `cargo`。**阶段 1–3（纯 Bun + PG）可立即启动不受任何阻塞**；阶段 4 编译 `native/plugin-wasmtime` 前需安装固定版本 Rust 工具链（如 `rustup`）或指定本机已有 Rust 路径。
