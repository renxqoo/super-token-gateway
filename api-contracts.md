# API 契约

完整可生成规范为 `GET /openapi.json` 与 `docs/openapi.json`；生成命令 `bun run openapi`。版本为 `/v1`、`/v1beta`；管理接口应仅在私网暴露。

## 认证与响应

推理使用 `Authorization: Bearer gw_...`；Messages 可用 `x-api-key`，Gemini 可用 `x-goog-api-key`。不把 URL 查询参数中的凭证写入访问日志；当前客户端应使用请求头。管理接口使用独立 `ADMIN_KEY` 的 Bearer。

每次 HTTP 请求生成 `x-request-id`，推理另返回持久化的 `x-gateway-request-id`。`Idempotency-Key` 最长 200 个可打印 ASCII 字符；重复请求返回 409 和已有请求状态，不保存/重放响应体。所有错误正文均为清洗过的 error.code/message，不回显原供应商错误正文或 SQL。

400 输入无效；401 未认证；402 合资格资金或授信不足、钱包冻结（insufficient_funds）；403 操作/模型越权；404 自有资源不存在；409 意图/配置冲突；413 字节超限；422 能力不可表达；429 准入超限；502 上游/转换/完成提交失败；503 依赖/容量不可用；504 超时。

输出上界只接受入口协议的原生字段（Chat `max_completion_tokens`/`max_tokens` 二选一，Responses `max_output_tokens`，Messages/Completions `max_tokens`，Gemini `generationConfig.maxOutputTokens`），显式 `null` 视为未提供；多个或非本协议的上界字段返回 400/invalid_request。没有计量维度的服务端托管工具（如 web search、code interpreter、file search、Gemini Google Search/URL Context/Code Execution）在准入前返回 422/unmetered_tool；按白名单放行函数工具及只按 token 计费的客户端执行工具（列表见 ADR 0006），`tools: null` 视为未提供，未列入白名单的新工具类型默认拒绝。见 [ADR 0006](docs/decisions/0006-review-remediation.md)。

## 管理面

| 路径                        | 方法 / 用途                                         |
| --------------------------- | --------------------------------------------------- |
| /admin/projects             | GET / POST 项目                                     |
| /admin/projects/:id         | PATCH enabled、name、rpm、tpm、concurrency          |
| /admin/projects/:id/keys    | GET / POST；明文 Key 只在创建时返回一次             |
| /admin/keys/:id             | PATCH 模型/操作/到期；DELETE 撤销，轮换用新建后撤销 |
| /admin/providers            | GET / POST；列表不返回密钥                          |
| /admin/providers/:id        | PATCH 开关、rpm、并发与加密密钥轮换                 |
| /admin/models               | GET / POST；含禁用模型                              |
| /admin/models/:model        | PATCH 能力、输出上限与开关                          |
| /admin/models/:model/routes | GET / POST 配置渠道及费率                           |
| /admin/routes/:id           | PATCH 开关、优先级、权重和新定价版本                |
| /admin/usage                | GET 诊断浏览；project_id / before=seq               |
| /admin/consumers            | POST name、replay                                   |
| /admin/consumers/:id/claim  | POST limit=1..500，返回 events 与 lease_token       |
| /admin/consumers/:id/ack    | POST lease_token、event_ids                         |
| /admin/recover              | POST 过期尝试恢复；不能触发重新执行                 |
| /admin/metrics              | GET Prometheus 文本                                 |

供应商地址和种类建立后不原地改变；需要换账号/地址时建新供应商和路由，旧资源继续绑定原账号。模型名用作路径参数时应 URL 编码。列表有服务端硬上限（项目/Key 200、供应商 500、模型/路由 2000）；应按技术项目隔离容量。

## 数据面与资源

项目侧 `POST /v1/project/keys` 只允许 ADMIN Key 签发。派生 Key 在事务内重新验证签发者，继承其主体、模型/操作、CIDR、有效期及 Key 级限额；显式请求只能进一步收窄。省略操作权限时继承，受限模型或 CIDR 的空数组也继承。独立管理面可以签发无主体或更宽范围的服务 Key。各 Key 独立撤销，主体禁用对其所有 Key 生效。

推理端点见 README。`GET /v1/models` 按 Key 的模型范围过滤；`GET /v1/usage`、`GET /v1/requests/:id` 只能查看本项目且符合操作/模型范围的证据。

Files 支持上传/列表/查询/删除/content；Batch 支持创建/列表/查询/cancel；Video 支持创建/列表/查询/删除/content；Responses 支持创建、后台、列表、查询、删除、cancel、input_items 和 previous_response_id。资源列表支持 limit=1..100、after=<last_id>，返回 has_more/first_id/last_id；授权过滤先于分页，游标必须属于当前项目和模型范围。列表只返回本地必要元数据，不缓存供应商正文。原供应商不支持的资源动作返回明确上游错误。

普通 Files 可省略 model，由有权限且支持 files 的配置选择；Batch 文件必须包含合法 POST JSONL、唯一 custom_id、统一逻辑模型和端点，支持不超过 50,000 项（仍受上传字节限制）。输出按每行检查捕获的供应商价格与客户价格所需维度；任一行缺证据则保留 PARTIAL 与待对账预扣，聚合后字段齐全也不能提前结算。Gateway 重写为该供应商实际模型；创建 Batch 不向上游发送无效的顶层 model 字段。

图片 Batch 的张数来自每行图片结果。图片编辑的 multipart `n` 接受单个正整数文本，并按其数值预扣。完整读取非 JSON 音频响应后记录精确请求次数，音频时长仍需要上游证据。

Gemini 视频将 JSON 或 multipart 的 prompt、seconds、size/aspect_ratio 转为任务 JSON；参考文件及无法表达的字段返回 422。OpenAI 兼容视频入口按上游能力透传。

Multipart 推理请求的 `prompt` 和 `input` 各只接受一个字段。重复字段返回 400，避免报价与上游读取不同的文本。

文件引用、file_ids 数组、previous_response_id 都经过项目/模型授权并改写为自有 upstream ID。未经管理的 Gemini fileData/cachedContent、容器 ID、向量库/会话/assistant 资源引用返回 422，不能利用共享上游密钥越过项目边界。

没有“跨供应商查询同一个资源”和“本地下载归档”功能。原供应商删除或停止保存内容后，Gateway 只能保留计量与元数据。

## 授信策略

`POST /admin/projects/:id/wallets/:currency/credit-policy` 仅允许管理凭证，钱包须已经存在。严格输入示例：

```json
{
  "external_ref": "credit-policy-001",
  "expected_version": "0",
  "credit_limit_atoms": "100000000000000000000"
}
```

成功200返回id、credit_policy_version、credit_limit_atoms及replayed。相同命令重试返回原id与版本，不重新应用；不同载荷、旧版本、新额度低于欠费加在途信用返回409/billing_conflict，钱包不存在404/not_found，HTTP输入格式错误400。数据库入口的非法金额映射422/billing_rejected。

钱包查询新增credit_limit_atoms、credit_reserved_atoms、available_credit_atoms、credit_excess_atoms、credit_policy_version，均为整数串；available_credit_atoms不包含模型受限赠额。`GET /v1/requests/:id/billing` 新增该请求credit_reserved_atoms，描述原预扣中的信用部分，终态后的实际欠费看debt_atoms。现金/赠额available最低0；异常应收超过授信时保留全额债务并冻结。

路由创建与 PATCH 支持可选 `request_policy`，策略整体替换，`{}` 清空。格式、目标原生字段及扩展约束见 [路由设计](routing-and-execution.md) 与 [ADR 0007](docs/decisions/0007-money-and-model-compatibility.md)。新增缓存写入维度 `cache_write_5m_tokens`、`cache_write_1h_tokens` 均为精确非负整数字符串；原合计维度继续用于已捕获的历史费率，同一费率禁止同时定价合计与明细。金额单位、舍入、旧账单和外部队列版本不变；启用新维度前须先升级所有消费者。
