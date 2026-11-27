# API 契约

完整可生成规范为 `GET /openapi.json` 与 `docs/openapi.json`；生成命令 `bun run openapi`。版本为 `/v1`、`/v1beta`；管理接口应仅在私网暴露。

## 认证与响应

推理使用 `Authorization: Bearer sk_...`；Messages 可用 `x-api-key`，Gemini 可用 `x-goog-api-key`。不把 URL 查询参数中的凭证写入访问日志；当前客户端应使用请求头。管理接口用独立的 Bearer 凭证按角色授权。

每次 HTTP 请求生成 `x-request-id`，推理另返回持久化的 `x-gateway-request-id`。`Idempotency-Key` 最长 200 个可打印 ASCII 字符；重复请求返回 409 和已有请求状态，不保存/重放响应体。所有错误正文均为清洗过的 error.code/message，不回显原供应商错误正文或 SQL。

400 输入无效；401 未认证；402 合资格资金或授信不足、钱包冻结（insufficient_funds）；403 操作/模型越权；404 自有资源不存在；409 意图/配置冲突；413 字节超限；422 能力不可表达；429 准入超限；502 上游/转换/完成提交失败；503 依赖/容量不可用；504 超时。

非流式会话推理收到上游 SSE 时返回 HTTP 502 和 `invalid_upstream`，并关闭上游响应体。网关读取缓冲的上游推理 JSON 时，非法 UTF-8 与畸形 JSON 均返回 HTTP 502 和 `invalid_upstream_json`。两种情况都保留已派发请求的 `UNKNOWN` 预扣待核账，停止自动切换并且不产生客户扣费。

输出上界只接受入口协议的原生字段（Chat `max_completion_tokens`/`max_tokens` 二选一，Responses `max_output_tokens`，Messages/Completions `max_tokens`，Gemini `generationConfig.maxOutputTokens`），显式 `null` 视为未提供；多个或非本协议的上界字段返回 400/invalid_request。没有计量维度的服务端托管工具（如 web search、code interpreter、file search、Gemini Google Search/URL Context/Code Execution）在准入前返回 422/unmetered_tool；按白名单放行函数工具及只按 token 计费的客户端执行工具（列表见 ADR 0006），`tools: null` 视为未提供，未列入白名单的新工具类型默认拒绝。见 [ADR 0006](docs/decisions/0006-review-remediation.md)。

## 管理面

| 路径                                                | 方法 / 用途                                                   |
| --------------------------------------------------- | ------------------------------------------------------------- |
| /internal/v1/billing-accounts                       | PUT 创建或更新计费账户的币种、状态、RPM、TPM 和并发额度       |
| /internal/v1/api-keys                               | PUT 签发或更新账户的 Key，明文只在签发时返回                  |
| /api/v1/billing-accounts/:billingAccountId/api-keys | Console 客户端在账户内列出和签发 Key                          |
| /admin/providers                                    | GET / POST；列表不返回密钥                                    |
| /admin/providers/:id                                | PATCH 开关、rpm、并发与加密密钥轮换                           |
| /admin/models                                       | GET / POST；含禁用模型                                        |
| /admin/models/:model                                | PATCH 能力、输出上限与开关                                    |
| /admin/models/:model/routes                         | GET / POST 配置渠道及费率                                     |
| /admin/routes/:id                                   | PATCH 开关、优先级、权重和新定价版本                          |
| /internal/v1/charges                                | GET 过滤账单，可指定 billing_account_id、api_key_id、时间范围 |
| /admin/consumers                                    | POST name、replay                                             |
| /admin/consumers/:id/claim                          | POST limit=1..500，返回 events 与 lease_token                 |
| /admin/consumers/:id/ack                            | POST lease_token、event_ids                                   |
| /admin/metrics                                      | GET 未结请求、预扣、积压等计数的 JSON                         |

供应商地址和种类建立后不原地改变；需要换账号/地址时建新供应商和路由，旧资源继续绑定原账号。模型名用作路径参数时应 URL 编码。按计费账户划分总限额，按 Key 限制模型、操作和日常使用。

## 数据面与资源

个人注册和团队创建自动开通计费账户。登录用户通过 Console 的账户路径管理 Key。账户选择在签发时固定，推理请求继续使用 Key 作为调用凭证。账户停用返回 HTTP 403 和 `billing_account_disabled`。组织角色与签发权限见 [Console API](docs/console-api.md)。

`GET /v1/models` 按 Key 的模型范围过滤。视频使用 `POST /v1/videos` 提交，`GET /v1/videos/:taskId` 查询，`POST /v1/videos/:taskId/cancel` 取消。同账户内，普通 Key 只可操作其轮换链签发的任务。账户 ADMIN 可查询或取消该账户内任意 Key 签发的任务。查询与取消均按数据库的最新限制复核调用方 Key 的模型、操作和客户端 IP 权限。跨账户或同账户其他调用方的任务返回 404。

Gemini 视频将 JSON 或 multipart 的 prompt、seconds、size/aspect_ratio 转为任务 JSON；参考文件及无法表达的字段返回 422。OpenAI 兼容视频入口按上游能力透传。

Multipart 推理请求的 `prompt` 和 `input` 各只接受一个字段。重复字段返回 400，避免报价与上游读取不同的文本。

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

路由创建与 PATCH 支持可选 `request_policy`，策略整体替换，`{}` 清空。格式、目标原生字段及扩展约束见 [路由设计](routing-and-execution.md) 与 [ADR 0007](docs/decisions/0007-money-and-model-compatibility.md)。新增缓存写入维度 `cache_write_5m_tokens`、`cache_write_1h_tokens` 均为精确非负整数字符串；原合计维度继续用于已捕获的历史费率，同一费率禁止同时定价合计与明细。金额单位、舍入、旧账单和外部队列版本不变；启用新维度前须先升级所有消费者。
