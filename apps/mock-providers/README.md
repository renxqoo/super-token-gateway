# Independent mock providers

独立的 Hono 应用，不导入 Gateway 的协议、SSE、usage 或存储代码。仅在内存保存故障配置及模拟资源；不需要 PG/Redis、不使用真实供应商凭证。

在仓库根目录运行 `bun run mock`；也可单独构建后运行 `MOCK_PORT=9088 MOCK_CONTROL_TOKEN=<随机值> bun dist/mock-providers/main.js`。绑定地址固定127.0.0.1。控制凭证由本地配置提供，不写到日志。

原生前缀为 `/openai/v1`、`/anthropic/v1`、`/gemini/v1beta`。同时支持 `/v1` OpenAI 别名供参考项目压测；媒体、文件、Batch、后台 Responses 与视频任务都在独立 fixture 中实现。

## 厂商对话协议矩阵

集成测试通过 Gateway 的 Chat API 对每个预设分别执行一次 JSON 和一次流式请求，并检查返回内容、结束帧及真实账本的用量。Mock 采用各厂商的请求路径和协议外形，凭证使用合成值。兼容渠道共享响应生成器，各自声明端点及差异。Bedrock、Azure、Vertex 的厂商入口分文件实现。

| 预设              | 本地路径前缀                                                | 协议依据                                                                                                                                                                                                                                                                           |
| ----------------- | ----------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| OpenAI            | `/openai`                                                   | [Chat API](https://developers.openai.com/api/reference/resources/chat)                                                                                                                                                                                                             |
| OpenAI compatible | `/openai-compatible`                                        | [Chat API](https://developers.openai.com/api/reference/resources/chat)                                                                                                                                                                                                             |
| Anthropic         | `/anthropic`                                                | [Messages SSE](https://platform.claude.com/docs/en/build-with-claude/streaming)                                                                                                                                                                                                    |
| Gemini            | `/gemini`                                                   | [generateContent](https://aistudio.google.com/api-reference/generate-content)                                                                                                                                                                                                      |
| Azure             | `/azure`                                                    | [部署型 Chat API 2024-10-21](https://learn.microsoft.com/en-us/azure/foundry/openai/how-to/fine-tuning-deploy?tabs=portal&view=foundry-classic)                                                                                                                                    |
| Bedrock           | `/bedrock`                                                  | [InvokeModelWithResponseStream](https://docs.aws.amazon.com/bedrock/latest/APIReference/API_runtime_InvokeModelWithResponseStream.html) · [Claude 请求响应](https://docs.aws.amazon.com/bedrock/latest/userguide/model-parameters-anthropic-claude-messages-request-response.html) |
| Vertex            | `/vertex/v1/projects/test/locations/test/publishers/google` | [Google Cloud Gemini 接口](https://docs.cloud.google.com/gemini-enterprise-agent-platform/reference/models/inference)                                                                                                                                                              |
| DeepSeek          | `/deepseek`                                                 | [Chat Completions](https://api-docs.deepseek.com/api/create-chat-completion/)                                                                                                                                                                                                      |
| Ollama            | `/ollama`                                                   | [OpenAI 兼容接口](https://docs.ollama.com/api/openai-compatibility)                                                                                                                                                                                                                |
| Qwen              | `/qwen`                                                     | [OpenAI-compatible Chat](https://www.alibabacloud.com/help/en/model-studio/qwen-api-via-openai-chat-completions)                                                                                                                                                                   |
| 火山方舟          | `/volcengine`                                               | [对话 API](https://docs.volcengine.com/docs/ark/chat-api?lang=zh)                                                                                                                                                                                                                  |
| 智谱              | `/zhipu`                                                    | [对话补全](https://docs.bigmodel.cn/api-reference/模型-api/对话补全)                                                                                                                                                                                                               |
| Kimi              | `/moonshot`                                                 | [Chat Completions](https://platform.kimi.com/docs/api/chat)                                                                                                                                                                                                                        |
| MiniMax           | `/minimax`                                                  | [ChatCompletion v2](https://platform.minimaxi.com/document/%E5%AF%B9%E8%AF%9D?key=36954880539f4e2ab8b13067)                                                                                                                                                                        |
| OpenRouter        | `/openrouter`                                               | [Create a chat completion](https://openrouter.ai/docs/api/api-reference/chat/create-a-chat-completion)                                                                                                                                                                             |

控制端点要求 `Authorization: Bearer <MOCK_CONTROL_TOKEN>`：

- POST `/__control/scenarios`：`{"model":"alias","scenario":{"status":503,"failFirst":1}}`。
- POST `/__control/reset`：清理模拟配置、资源和累计统计。
- GET `/__control/stats`：请求/在途/取消/字节量及按模型尝试次数。
- GET `/healthz`：无需凭证的存活探针。

可组合场景：status、failFirst、headerDelayMs、firstEventDelayMs、chunkDelayMs、chunkBytes、truncate、streamError、malformed、tools、output、repeat、omitUsage、pollUsage、whitespace、thoughtSignature、binaryBytes、finishReason、promptBlocked。每个测试可以选择新的模型别名，避免相互污染。

默认 usage 使用不同供应商的实际字段布局：OpenAI 总输入10/缓存3/输出5；Anthropic 未缓存输入7/读缓存3/写缓存2/输出5；Gemini 候选3/思维2。输出包含 Unicode，流可按单字节切割。Batch 默认无根部聚合usage，结果JSONL单独提供usage，以验证真实回收路径。

这是受控的开发/故障夹具。真实供应商账号的字段约束、收费、地域和模型能力仍需真实合约验收，不能把 mock 吞吐当作供应商推理吞吐。
