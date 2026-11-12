# Super Token Gateway

独立的 LLM Gateway 后端，使用 **Hono + Bun 1.4.4**，原生 `bun check`、`bun fmt` 和 oxlint。业务系统不在此仓库的运行依赖中。

当前验收目标为同一台 Mac 上网关转发 6000 RPS。正式验收要求窗口内有效响应、精确结算与外部ACK共同完成，见[6000 RPS契约](docs/performance/capacity-contract.md)；长期高可用仍需独立基础设施验证。功能、测试和容量证据持续更新于[验收记录](validation.md)。

Gateway 管理技术组织/主体、项目、API Key/IP、模型别名、供应商连接、路由与限流，以及多币种钱包、赠送额度、预扣、有界正常授信、精确计费、退款冲正和 BullMQ 结算 worker。独立业务系统负责登录账号、商业组织关系、支付渠道、套餐和营销；通过幂等内部 API 入账，不直接访问 Gateway 数据库。见 [系统边界](architecture.md) 和 [计量契约](funds-and-billing.md)。

**不持久化模型输出**：文本、工具参数、向量、图片、音频、视频、完整响应和 Batch 结果不进入 Gateway 数据库、文件、日志、Redis 或 usage 事件。文件、状态查询与下载依赖原供应商；没有本地正文重放缓存。

## 本地运行

需要 Bun **1.4.4**、已有 Docker，或者已有 PostgreSQL 17 / Redis 7。脚本不安装 Docker。独立 mock 是另一个应用，仅使用 Hono，不依赖 Gateway 解析器或业务模块。

`DATABASE_URL` 必须显式指向已创建的空开发库 `gateway_v2_dev`；已有旧 schema 的数据库会被拒绝并保留。

```sh
bun install --frozen-lockfile
bun run setup:local
bun run infra:up
bun run db:migrate -- --database gateway_v2_dev
bun run seed
```

然后分别在三个终端运行：

```sh
bun run mock
bun run dev
bun run worker
```

`setup:local` 生成权限为 `0600` 的 `.env.local`，保留已有密钥；`seed` 将开发调用凭证写入 `.local/client.env`。开发 PG、准入 Redis、耐久队列 Redis 分别使用 `127.0.0.1:55442`、`:56382`、`:56383`，不复用其他项目的数据。已有本机 PG/Redis 可以直接修改 `DATABASE_URL`、`REDIS_URL`、`QUEUE_REDIS_URL` 并跳过 `infra:up`。

```sh
set -a
. .local/client.env
set +a
curl "$GATEWAY_URL/v1/chat/completions" \
  -H "Authorization: Bearer $GATEWAY_API_KEY" -H 'Content-Type: application/json' \
  -d '{"model":"demo","messages":[{"role":"user","content":"hello"}],"max_completion_tokens":32}'
```

管理凭证在 `.env.local`，不要发到聊天或提交仓库。管理 API 示例见 [API 契约](api-contracts.md)。

## 能力

| 接口                                                                       | 上游与处理方式                                                                        |
| -------------------------------------------------------------------------- | ------------------------------------------------------------------------------------- |
| Chat Completions / Responses / Anthropic Messages / Gemini GenerateContent | 原生 JSON/SSE；文本、工具、图片输入和结构化输出的能力受限转换                         |
| Embeddings / Moderations / 旧 Completions                                  | OpenAI 兼容原生接口；Embeddings 直接转发向量                                          |
| 图片生成、编辑                                                             | OpenAI 兼容 JSON / multipart；原生图片 SSE                                            |
| 语音合成、转录、翻译                                                       | OpenAI 兼容二进制、JSON、text/SRT/VTT；合成与转录原生 SSE                             |
| Files / Batch                                                              | 上游文件、归属隔离、JSONL 模型映射、提交、查询、取消、流式结果下载与纯内存 usage 提取 |
| Video / 异步生成                                                           | Gemini Veo 长任务及 OpenAI 兼容供应商的视频任务；状态、取消能力按上游、下载与后台补证 |
| Responses 状态                                                             | 所有权校验、previous_response_id、后台任务、查询、取消、删除、input_items             |

供应商不能表达的跨协议语义返回 `422`，例如托管工具、签名思维链、多候选或无法保真的工具结果；原生接口保留该供应商能接受的字段，但输出上界只接受本协议原生字段，没有计量维度的服务端托管工具返回 `422 unmetered_tool`（[ADR 0006](docs/decisions/0006-review-remediation.md)）。不同供应商的模型不被假设为支持所有端点。OpenAI 官方 Videos API 已于 2026-09-24 停用，视频适配器用于 Gemini 和仍提供相应契约的兼容供应商，见 [官方参考](references.md)。

Key 支持 IPv4/IPv6 CIDR、模型/操作权限及角色。转发头只接受 `TRUSTED_PROXY_CIDRS` 内的实际对端；生产内网 Ollama 通过明确的 `PRIVATE_UPSTREAM_CIDRS` 授权，禁止全局私网绕过。DeepSeek 与 Ollama 使用其官方 OpenAI 兼容接口；Ollama 基地址填写 `/v1`。

## 工程与验收

```sh
bun run verify
bun run openapi
bun run bench --model gpt-4o-mini --seconds 30 --concurrency 32
bun run bench --model gpt-4o-mini --seconds 30 --concurrency 32 --stream
```

测试使用真实 PostgreSQL、Redis 和真实 HTTP mock，自动创建并删除独立测试数据库。测试数据库账号需要 `CREATEDB`，生产运行账号不需要。验证内容和实测数据见 [验收报告](validation.md)；不会把短时本机测试描述为 10,000 RPS 持续一小时或完整机房高可用验证。

`bun run build` 输出无需 `node_modules` 的 Gateway、计费与恢复 worker、迁移工具与独立 mock。生产入口为 `bun dist/gateway/main.js`，独立 worker 为 `bun dist/gateway/worker-main.js`。Linux、双实例、最小权限、发布/回滚与恢复步骤见 [运维手册](reliability-and-operations.md)。

T04.a 增加独立插件组件：`@gateway/plugin-host` 提供有界 Bun 监督、已验证包与 attempt API，`@gateway/plugin-sdk` 定义 ABI v1，`@gateway/adapters` 提供 OpenAI Chat WASM 包及锁定来源的构建入口。当前 Gateway 装配和资金路径仍由 T04.b/c 迁移。原生构建需要 Go 1.27.4；`PLUGIN_GO` 可指定该 Go 的绝对路径，Go 缓存保存在 `.local`。`bun run verify` 包含显式 native gofmt/vet/`go test ./__test__`；完整 race 入口为 `bun run test:native:race`。CI 和部署环境须安装同版本 Go。

```sh
bun run test:plugins
bun run build
bun run test:plugin-artifact
bun run setup:plugin-tools --fetch-tools --fetch-dependencies --rust-bin /absolute/rust/1.95.0/bin
bun run build:plugins
```

前三项使用已提交 WASM，不下载插件工具、不访问数据库或供应商。`setup:plugin-tools` 在 macOS arm64 上显式获取并校验官方源码、冻结锁、最小 Wizer/Linker 补丁和未改 engine，使用指定 Rust 1.95.0 在 `.local` 以两 jobs 有界构建确定性 CLI；也可用 `--import-tool /absolute/extism-js` 导入 hash 已批准的本地 CLI。后续 `build:plugins` 从 `.local/plugin-tools` 离线重建，并强制执行同目录两次、异目录一次的真实 Chat/isolation WASM hash 一致门禁。内置包没有默认可信签名；生产加载必须提供发布信任锚，上传/发布生命周期留 T08。构建增加 `dist/native/plugin-host`、`dist/plugin-host/index.js`、`dist/adapters/index.js` 和 `dist/openai-chat/package/`；独立产物检查使用临时合成签名，在隔离目录执行真实包 fixtures 并回收 helper。公开 API、JSON 16MiB 边界、预算与终态责任见 [插件 SDK 迁移记录](docs/migrations/enterprise-v2-plugin-sdk.md)。

官方 CLI 原始快照的非确定性失败样本已保留；正式构建使用已批准的确定性预初始化路径，运行时和产物字节没有重写。未重建官方 engine 源码、未测 CLI 二进制自身复现及跨平台矩阵；T04.a 仍待协调集成和独立审查。生产签名和内容 digest 始终严格不可替换。

## 文档

[架构](architecture.md) · [数据模型](database.md) · [路由执行](routing-and-execution.md) · [API / OpenAPI](api-contracts.md) · [计量](funds-and-billing.md) · [运行时](bun-runtime.md) · [图示](diagrams.md) · [验证模型](validation-model.md) · [测试说明](delivery-and-validation.md) · [参考对照](references.md)

原有 13 份设计已全部审阅，原文及 SHA-256 清单保存在 `docs/design-2026-11-08/`。当前目录文档已按最终澄清更新；资金归属以 [ADR 0002](docs/decisions/0002-gateway-wallet-worker.md) 为准，计量入口以最新 [ADR 0003](docs/decisions/0003-durable-usage-queue.md) 为准。运行代码只有当前实现，旧数据库明确拒绝启动。

## 云供应商与数据维护

新增三类受明确能力约束的适配：

| kind    | base_url                                                                                          | api_key 内容与能力                                                                                                                                             |
| ------- | ------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| azure   | `https://RESOURCE.openai.azure.com`                                                               | 普通 Key 或 JSON `{"apiKey":"...","apiVersion":"2024-10-21"}`；部署名放 upstream_model；Chat/Embeddings，其他文本协议转换到 Chat                               |
| bedrock | `https://bedrock-runtime.REGION.amazonaws.com`                                                    | JSON `{"accessKeyId":"...","secretAccessKey":"...","region":"us-east-1","sessionToken":"..."}`，sessionToken 可省略；Claude Messages InvokeModel/事件流，SigV4 |
| vertex  | `https://REGION-aiplatform.googleapis.com/v1/projects/PROJECT/locations/REGION/publishers/google` | Google service_account JSON；签署 JWT、合并并发刷新、到期前刷新；Gemini GenerateContent/SSE                                                                    |

Vertex 也接受有明确 `expires_at` 的 `{"access_token":"...","expires_at":"...Z"}`，用于已受控轮换的短时令牌；过期不发送推理、不保留预扣。服务账号仅向官方 OAuth 端点换取令牌。JSON 凭证允许多行，继续用 AES-GCM 保存；Azure/Bedrock/Vertex 的其他模型族与端点不被默认宣称支持。真实云账号联调需要对应账号与预算，本地合同测试不替代它。

schema06 / schema07 到当前版本的停机升级、月分区准备、DEFAULT 修复、归档校验和冷存储移动见 [数据库与生命周期](database.md)。运行时仍只接受当前契约，财务幂等身份与未结责任持续保留。
