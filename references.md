# 参考与取舍

已完整审阅当前项目原始 13 份设计并保留校验清单。用户提供的两个参考项目仅以只读方式研究，性能夹具使用项目自有的隔离副本、数据库和 Redis 命名空间，不修改它们的业务数据。

| 参考版本                                            | 吸收的设计和测试                                                                                                                                            |
| --------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Tillgate `58df84769f4d25872b7786643db3468fc1001c57` | provider/adapter 分层、能力差异、silent-degradation、retry-guard、stream-deep、transport-deep、stream-timeout-scale、inference failover、generation polling |
| new-api `bd6bb53019fb7e21804b31ab7365c9b2097f4b6c`  | relay 转换边界、golden 与 tool_loss_policy、usage_merge/billing_usage、stream_scanner 慢写取消、protected_fetch_client DNS 固定、task ownership/polling     |

参考源码路径为 `/Users/wr/work/Tillgate/apps/gateway`、`/Users/wr/work/Tillgate/packages`、`/Users/wr/work/new-api`。此实现保持自己的商业边界、逐尝试不可变证据、耐久队列完成屏障和无正文持久化约束；按已确认边界独立实现技术钱包与客户结算，不复制商业支付系统。

同一机器的实测可以说明所选工作负载下的吞吐与延迟，不能证明“所有功能和所有负载全面优于两个成熟项目”。比较方法、差异和原始结果见 validation.md。

## 已核对的官方契约

- [Hono Bun 接入](https://hono.dev/docs/getting-started/bun)、[Bun check](https://bun.com/docs/runtime/check)、[Bun fmt](https://bun.com/docs/format)、[Bun SQL](https://bun.com/docs/runtime/sql)、[Bun Redis](https://bun.com/docs/runtime/redis)。
- [OpenAI 图片生成 SSE](https://developers.openai.com/api/reference/resources/images/generation-streaming-events)、[编辑 SSE](https://developers.openai.com/api/reference/resources/images/edit-streaming-events)、[转录 SSE](https://developers.openai.com/api/reference/resources/audio/subresources/transcriptions/streaming-events)、[语音合成](https://developers.openai.com/api/reference/resources/audio/subresources/speech/methods/create)。
- [Batch 创建](https://developers.openai.com/api/reference/resources/batches/methods/create)和[Batch 指南](https://developers.openai.com/api/docs/guides/batch)：模型在 JSONL 子请求中，不能添加无效顶层 model；用量兼容根聚合与结果文件。
- [OpenAI Videos API](https://developers.openai.com/api/reference/resources/videos)：官方服务已于 2026-09-24 停用。本项目视频真实适配目标为 Gemini Veo 和仍提供视频 API 的兼容供应商。
- [Gemini GenerateContentResponse](https://ai.google.dev/api/generate-content#generatecontentresponse)：promptFeedback 可表示输入被拦截且没有 candidates，原生响应应保留，跨协议转换为内容过滤终态。
- [Anthropic Messages](https://platform.claude.com/docs/en/api/messages)。

这些契约检查与 mock 验证不替代真实供应商账号的联调。检查日期为 2026-11-10。

## 云传输实现依据

- Azure 部署寻址、API Key 与版本：[Microsoft REST reference](https://learn.microsoft.com/en-us/azure/ai-services/openai/reference)。
- Bedrock Claude 请求体：[Messages contract](https://docs.aws.amazon.com/bedrock/latest/userguide/model-parameters-anthropic-claude-messages-request-response.html)；[InvokeModelWithResponseStream](https://docs.aws.amazon.com/bedrock/latest/APIReference/API_runtime_InvokeModelWithResponseStream.html)。
- SigV4 canonical request：[AWS signing specification](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv-create-signed-request.html)。签名测试另以 botocore 独立生成包含 session token 和编码模型名的 golden 值。
- Vertex Gemini：[inference reference](https://docs.cloud.google.com/vertex-ai/generative-ai/docs/model-reference/inference)；[service account OAuth](https://developers.google.com/identity/protocols/oauth2/service-account)。

## 当前持续对标

本轮使用 /Users/wrr/work/Tillgate 与 /Users/wrr/work/new-api，版本及差距见[当前参考矩阵](docs/performance/reference-comparison.md)。通过 `bun run references:snapshot` 重新记录HEAD、工作区状态与关键源码哈希；当前性能与失败证据见[资金及容量复验](docs/performance/funds-capacity-2026-11-11.md)。历史参考版本与倍数不用于当前资金链路排名。
