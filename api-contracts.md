# API 契约

Gateway 路由的完整规范由 `bun run openapi` 生成，见 [OpenAPI](docs/openapi.json)。公开推理使用 `/v1` 和 `/v1beta`，Gateway 管理接口 `/internal/v1` 位于独立私网监听。Console 的客户与管理域见 [Console API](docs/console-api.md)。

## 认证与响应

推理使用 `Authorization: Bearer sk_...`；Messages 可用 `x-api-key`，Gemini 可用 `x-goog-api-key`。不把 URL 查询参数中的凭证写入访问日志；当前客户端应使用请求头。管理接口用独立的 Bearer 凭证按角色授权。

推理响应通过 `x-request-id` 标识请求。已有预扣和账单可用该标识查询。响应还携带 `x-trace-id` 和 `traceparent`。相同 `Idempotency-Key` 的重放返回 409 及已有请求的 ID 和状态。不同请求复用幂等键返回 `idempotency_conflict`。响应体不作为幂等回放缓存。

400 输入无效；401 未认证；402 合资格资金或授信不足、钱包冻结（insufficient_funds）；403 操作/模型越权；404 自有资源不存在；409 意图/配置冲突；413 字节超限；422 能力不可表达；429 准入超限；502 上游或转换失败；503 依赖、容量或提交不可用；504 超时。

非流式会话推理收到上游 SSE 时返回 HTTP 502 和 `invalid_upstream`，并关闭上游响应体。网关读取缓冲的上游推理 JSON 时，非法 UTF-8 与畸形 JSON 均返回 HTTP 502 和 `invalid_upstream_json`。两种情况都保留已派发请求的 `UNKNOWN` 预扣待核账，停止自动切换并且不产生客户扣费。

输出上界只接受入口协议的原生字段（Chat `max_completion_tokens`/`max_tokens` 二选一，Responses `max_output_tokens`，Messages/Completions `max_tokens`，Gemini `generationConfig.maxOutputTokens`），显式 `null` 视为未提供；多个或非本协议的上界字段返回 400/invalid_request。没有计量维度的服务端托管工具（如 web search、code interpreter、file search、Gemini Google Search/URL Context/Code Execution）在准入前返回 422/unmetered_tool；按白名单放行函数工具及只按 token 计费的客户端执行工具（列表见 ADR 0006），`tools: null` 视为未提供，未列入白名单的新工具类型默认拒绝。见 [ADR 0006](docs/decisions/0006-review-remediation.md)。

## 管理面

| 方法和路径                                                         | 用途                                                            |
| ------------------------------------------------------------------ | --------------------------------------------------------------- |
| `PUT /internal/v1/billing-accounts`                                | 创建或更新计费账户与额度配置                                    |
| `PUT /internal/v1/api-keys`                                        | 签发或更新账户的 Key。明文只在签发时返回                        |
| `GET /internal/v1/channels`、`PUT /internal/v1/channels`           | 查询或配置渠道                                                  |
| `PUT /internal/v1/models`、`PUT /internal/v1/routes`               | 配置模型能力和渠道路由                                          |
| `POST /internal/v1/prices`、`GET /internal/v1/prices/:priceId`     | 创建或读取不可变价卡                                            |
| `GET /internal/v1/charges`、`GET /internal/v1/requests/:requestId` | 查询账单和请求预扣、尝试                                        |
| `POST /internal/v1/events/claim`                                   | 以 `consumer`、`limit` 认领事件，返回 `events` 和 `lease_token` |
| `POST /internal/v1/events/ack`                                     | 以 `consumer`、`lease_token`、`through_seq` 确认消费            |
| `GET /internal/v1/health`                                          | 查询 Gateway 配置纪元、Redis 连接及渠道健康                     |

目录、财务、审计和事件使用各自的管理角色。变更渠道上游来源时提供新目标凭据，已有上游任务依其记录的渠道处理。客户通过 Console 的 `/api/v1/billing-accounts/:billingAccountId/api-keys` 管理账户 Key。

## 数据面与资源

个人注册和团队创建自动开通计费账户。登录用户通过 Console 的账户路径管理 Key。账户选择在签发时固定，推理请求继续使用 Key 作为调用凭证。账户停用返回 HTTP 403 和 `billing_account_disabled`。组织角色与签发权限见 [Console API](docs/console-api.md)。

`GET /v1/models` 按 Key 的模型范围过滤。视频使用 `POST /v1/videos` 提交，`GET /v1/videos/:taskId` 查询，`POST /v1/videos/:taskId/cancel` 取消。同账户内，普通 Key 只可操作其轮换链签发的任务。账户 ADMIN 可查询或取消该账户内任意 Key 签发的任务。查询与取消均按数据库的最新限制复核调用方 Key 的模型、操作和客户端 IP 权限。跨账户或同账户其他调用方的任务返回 404。

`POST /v1/videos` 使用非流式响应。转写、翻译和图片编辑支持 multipart，具体允许字段由协议、路由与上游适配共同决定。

## 授信策略

`POST /internal/v1/wallets/:billingAccountId/credit-policy` 需要 `finance` 权限，钱包须已经存在。请求体接受以下字段和可选的 `policy`，其值可为 `GIFT_FIRST`、`CASH_FIRST` 或 `EARLIEST_EXPIRY`。`currency` 默认 USD。

```json
{
  "currency": "USD",
  "expected_version": "0",
  "credit_limit_atoms": "100000000000000000000"
}
```

成功时返回 HTTP 200 和更新后的钱包，包含 `id`、`credit_policy_version` 和 `credit_limit_atoms`。每次成功更新使版本加一。再次提交旧版本或钱包不存在时，返回 HTTP 400 和 `internal_api_error`。请求体格式错误返回 HTTP 400。降低额度后，系统按新额度和现有敞口重新计算钱包冻结状态。

钱包查询新增credit_limit_atoms、credit_reserved_atoms、available_credit_atoms、credit_excess_atoms、credit_policy_version，均为整数串；available_credit_atoms不包含模型受限赠额。`GET /internal/v1/requests/:requestId` 新增该请求credit_reserved_atoms，描述原预扣中的信用部分，终态后的实际欠费看debt_atoms。现金/赠额available最低0；异常应收超过授信时保留全额债务并冻结。

`PUT /internal/v1/routes` 支持可选 `request_policy`，策略整体替换，`{}` 清空。校验阶段和扩展约束见 [路由设计](routing-and-execution.md) 与 [ADR 0007](docs/decisions/0007-money-and-model-compatibility.md)。新增缓存写入维度 `cache_write_5m_tokens`、`cache_write_1h_tokens` 均为精确非负整数字符串；原合计维度继续用于已捕获的历史费率，同一费率禁止同时定价合计与明细。金额和用量通过 PostgreSQL 账单与 Outbox 交付，见 [资金与定价](funds-and-billing.md)。
