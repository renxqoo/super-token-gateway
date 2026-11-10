# Independent mock providers

独立的 Hono 应用，不导入 Gateway 的协议、SSE、usage 或存储代码。仅在内存保存故障配置及模拟资源；不需要 PG/Redis、不使用真实供应商凭证。

在仓库根目录运行 `bun run mock`；也可单独构建后运行 `MOCK_PORT=9088 MOCK_CONTROL_TOKEN=<随机值> bun dist/mock-providers/main.js`。绑定地址固定127.0.0.1。控制凭证由本地配置提供，不写到日志。

原生前缀为 `/openai/v1`、`/anthropic/v1`、`/gemini/v1beta`。同时支持 `/v1` OpenAI 别名供参考项目压测；媒体、文件、Batch、后台 Responses 与视频任务都在独立 fixture 中实现。

控制端点要求 `Authorization: Bearer <MOCK_CONTROL_TOKEN>`：

- POST `/__control/scenarios`：`{"model":"alias","scenario":{"status":503,"failFirst":1}}`。
- POST `/__control/reset`：清理模拟配置、资源和累计统计。
- GET `/__control/stats`：请求/在途/取消/字节量及按模型尝试次数。
- GET `/healthz`：无需凭证的存活探针。

可组合场景：status、failFirst、headerDelayMs、firstEventDelayMs、chunkDelayMs、chunkBytes、truncate、streamError、malformed、tools、output、repeat、omitUsage、pollUsage、whitespace、thoughtSignature、binaryBytes、finishReason、promptBlocked。每个测试可以选择新的模型别名，避免相互污染。

默认 usage 使用不同供应商的实际字段布局：OpenAI 总输入10/缓存3/输出5；Anthropic 未缓存输入7/读缓存3/写缓存2/输出5；Gemini 候选3/思维2。输出包含 Unicode，流可按单字节切割。Batch 默认无根部聚合usage，结果JSONL单独提供usage，以验证真实回收路径。

这是受控的开发/故障夹具。真实供应商账号的字段约束、收费、地域和模型能力仍需真实合约验收，不能把 mock 吞吐当作供应商推理吞吐。
