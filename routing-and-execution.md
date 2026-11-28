# 路由、重试与流式完成

公开推理在 Gateway 进程内完成编排，账务由 `@gateway/ledger` 提交 PostgreSQL。[系统架构](architecture.md) · [ADR 0009 推理请求数据流](docs/decisions/0009-enterprise-gateway-worker-architecture.md#34-网关推理请求的数据流)

## 从准入到发出

1. 入口检查节点容量、可选 IP 限额和请求体预算。验证 Key、计费账户和模型权限后获取账户 RPM、TPM 及并发许可。使用共享 Redis 时由其原子处理配额，未配置时可用节点内限流。
2. 使用同一个目录快照加载候选。按渠道状态、熔断、冷却、优先级和模型策略排序。支持 `WEIGHTED`、`ROUND_ROBIN` 与 `LEAST_LATENCY`。默认自适应规则和兼容判定见 [自适应路由](docs/adaptive-provider-routing.md#3-选路策略与配置)。
3. 按候选执行内容策略并准备上游请求，核对原生字段、允许的计量维度、报价和输出上界。供应商插件决定协议适配，Gateway 负责真实传输、鉴权与安全出站。
4. Ledger 创建客户预扣与首次物理尝试。带幂等键的重放根据账户、操作及请求指纹判定，返回已有请求状态或冲突。支持预建连接的路径可在预扣前取得连接，实际请求在预扣后发送。派发前核对渠道许可及插件状态。

## 切换与完成

重试沿已排序的候选向后选择，按第一条实际预留的路由预算收口。路由 `retry_policy.max_attempts` 默认 3，可设为 1 至 10。实际次数受候选、期限、限流、策略及已交付内容约束。显式自适应策略可进一步收紧切换边界。

确定未执行的尝试可释放其供应商预扣。结果不明时保留旧尝试，只有路由允许且满足额外限制时才切换。备选重新执行自己的输入策略和报价，账本以原请求 ID 转换供应商尝试，并在需要时补足客户预扣。已收到输出的流沿同一尝试结束。路由器与发布扩展见 [ADR 0010](docs/decisions/0010-provider-plugin-architecture.md) 和 [ADR 0012](docs/decisions/0012-plugin-revisions-and-provider-verification.md)。

非流式响应在受限缓冲内验证并提取用量。对话 SSE 与媒体 SSE 使用各自的终态识别、背压和计量路径。成功 JSON 和 SSE 成功终态等待 PostgreSQL 结算确认。事件认领和确认属于结算后的 Outbox 投递。断流、取消、超时和停机触发资源清理，按可信用量、估算规则或 `UNKNOWN` 收口。详见 [资金链路](funds-and-billing.md#请求链路)。

## 协议与请求策略

协议转换覆盖 Chat、Responses、Messages 和 Gemini 已支持的文本、图像、工具、采样和结构化输出字段。严格模式拒绝不可表达的语义，兼容模式按字段策略标记省略。原生请求继续服从上游字段和计量规则。Messages 格式可用于 Bedrock 适配，Vertex 使用 Gemini 适配。当前可用协议和公开路径见 [API 契约](api-contracts.md)。

每条路由可配置 `request_policy`。内容策略处理后的请求体在协议转换和供应商请求改写前接受检查。`allowed_parameters` 检查顶层字段，`blocked_parameters` 禁止指定点分路径，`parameter_ranges` 校验数值范围。路径中的 `*` 展开一层。白名单以传入此阶段的请求字段为准。路由更新替换整个策略，`{}` 清空。权限与插件修订变化按 [配置快照和撤销规则](docs/decisions/0012-plugin-revisions-and-provider-verification.md) 生效。
