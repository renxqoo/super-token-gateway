# enterprise-v2 T09 文本与云适配迁移

## 1. 当前行为（代码核对）

- v2 Gateway 只公开 `POST /v1/chat/completions`，唯一插件 `gateway.openai-chat` 直连 OpenAI 兼容上游；其余 provider kind（anthropic/gemini/deepseek/ollama/azure/bedrock/vertex）与入站协议 responses/messages/gemini 仍由 legacy gateway 经 `packages/connectors` 的 TS 转换执行。
- ABI v1 body plan 只有 `set`/`remove`（≤32 条、单值 ≤4KiB、合计 ≤64KiB），guest 不读取正文；因此无法表达需要搬移用户内容的协议转换（chat→messages 的 system 提取、tool_calls→tool_use、参数字符串→对象）。
- inspection 每轮 ≤16 个精确指针、≤4 轮；无法在一轮内获得 N 条消息的角色/内容类型。
- 宿主认证只有 none/bearer/api-key；`packages/runtime/src/cloud-auth.ts` 已有 SigV4 签名与 Vertex OAuth token 刷新，但只被 legacy 使用。SSE 之外无二进制流 codec。
- 插件构建工具链（确定性 extism-js）需本地 Rust 1.95.0 源码构建；本机已在 `.local/` 内完成并复现出与提交一致的 `module.wasm`。

## 2. 决策

- **E1 组合计划（ABI v1 增量能力）**：body plan 增加互斥形态 `compose`：`{document, compose: {template, slots}}`。`template` 为 guest 生成的严格 JSON（≤256KiB）；`slots`（≤4096）把模板中的 `null` 叶子替换为源文档内容：`value` 原样字节、`parse` 将源 JSON 字符串解析为值（同一严格索引与深度/字段/数值限制，失败为 `invalid_json`：prepare 400，response/stream 502）、`stringify` 把源节点原文作为 JSON 字符串、`concat` 把 ≤256 个源字符串按 Unicode 拼接为一个字符串。宿主只做通用复制/解析/序列化/拼接，不识别协议字段；输出 ≤ 源文档 + 模板 + 64KiB，并复用同一最终索引、摘要与预算。request、response 与 frame 的 body 都可使用 compose。
- **E2 通配 inspection**：指针段 `*` 在数组展开为各下标、在对象展开为各成员键（`*` 因此成为保留段，不能按字面键查询），单轮展开后 ≤4096 个事实，事实 JSON 上限由 128KiB 调为 256KiB。轮数上限保持 4。guest 借此枚举顶层字段，对未声明字段显式拒绝而不是静默丢弃。
- **E3 帧计划**：frame plan 增加 `disposition: "drop"`（消费帧且无输出，不能带 body/extra，不能作为终态）、`data_only: true`（渲染时去掉 event/id/comment 行）与 `extra`（≤4 个 guest 生成的、各 ≤4KiB 且不含 CR/LF/NUL 的字面 data 事件，追加在该帧之后，如 `[DONE]`）。它们是通用 SSE 组合能力，不带协议分支。
- **E4 云认证**：auth `aws-sigv4`（service/region 来自配置）与 `google-oauth` 由宿主在最终不可变请求上用现有 `cloud-auth` 计算，签名/令牌只进入传输，不返回 guest。
- **E5 内置包集合**：bundled release 从单一 `gateway.openai-chat-1.0.0` 扩展为显式内置集合，每个 release 仍受部署 policy `releases` 列表及 key `release_ids` 约束。
- **E6 入站协议**：`/v1/responses`、`/v1/messages`、Gemini 路径在 v2 Gateway 以同一 ABI 执行，插件声明支持的入站协议；未声明组合在候选准备阶段排除。

## 3. 里程碑

| 里程碑           | 交付                                                              | 完成证据                                                                         |
| ---------------- | ----------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| j1 组合能力      | E1–E3 宿主实现、SDK schema、真实 helper 用例                      | 复制/解析/序列化/拼接、精确数字与 UTF-8、越界/冲突/非 null 槽/预算拒绝；旧包不变 |
| j2 Anthropic     | `gateway.anthropic-messages` 插件 JSON/SSE、工具、usage、内置发布 | fixtures + 真实 Gateway→插件→mock 上游链路                                       |
| j3 OpenAI 兼容族 | deepseek/ollama/azure 由配置覆盖路径/认证                         | Azure deployment 路径与 api-version、api-key；DeepSeek 缓存用量                  |
| j4 Gemini/Vertex | Gemini 插件与 `google-oauth`                                      | Vertex 令牌刷新只在宿主；Gemini SSE                                              |
| j5 Bedrock       | `aws-eventstream` codec 与 `aws-sigv4`                            | 二进制帧边界/CRC/截断、签名绑定最终请求                                          |
| j6 入站协议      | responses/messages/gemini 入口                                    | 与 legacy connectors 回归用例等价                                                |
| j7 验收          | 自审、全量门禁、legacy connectors 退役条件                        | 同 T07 g4                                                                        |

## 4. 实施记录

- **j1 组合能力**：`packages/plugin-host/src/json-documents.ts` 实现 compose（value/parse/stringify/concat）与 `*` 通配展开；`attempts.ts`/`sse-stream.ts` 把 compose 源解析失败映射为 `invalid_json`（prepare 400，其余 502）；`sse-stream.ts` 实现 `drop`、`data_only` 与 `extra`；`packages/plugin-sdk/src/abi.ts` 增加对应 schema。旧 `edits` 计划、openai-chat 包字节与行为不变（本机源码构建的确定性 builder 重建出与提交一致的 `module.wasm`）。
- **j2 Anthropic**：`packages/adapters/anthropic-messages`（`gateway.anthropic-messages-1.0.0`）。chat→messages：system/developer 提升为 `system` 文本块；user 文本与 http(s) 图片 URL；assistant 文本与 `tool_calls`→`tool_use`（空参数为 `{}`，参数串经 compose `parse`，非法为 400）；`tool`→`tool_result`；tools/tool_choice/parallel_tool_calls/stop/temperature/top_p/user 映射；`x-api-key` 与 `anthropic-version: 2023-06-01`。顶层与消息字段按白名单枚举，未声明字段、data URI 图片、`name`、strict 工具、`n>1` 均显式 422。响应与 SSE 转为 chat completion / chunk：多段文本 `concat`、`tool_use.input` `stringify`、stop_reason 与 legacy 相同映射、缓存用量拆为 `cached_input_tokens`/`cache_write_tokens`；SSE 丢弃 ping 与块边界帧，finish 帧与 usage 帧连同 `[DONE]` 作为终态候选保留到耐久屏障之后。
- **E5 内置集合**：`bundledReleaseIds` = openai-chat 与 anthropic-messages；policy `release_ids`/`releases` 接受集合内 ID，Core 校验按 release 选择对应仓库内置文件；`plugins:sign-bundled` 增加必填 `--release-id`。
- **构建**：`scripts/build-plugins.ts` 支持 `--adapter <name...>`，默认构建全部。
- **测试**：`json-documents` 单测 3 例；`packages/adapters/src/__test__/anthropic-messages.test.ts` 4 例（含 14 个 fixture case/20 步经真实 helper）；`tests/integration/gateway-anthropic.test.ts` 1 例（真实 Gateway→插件→TLS 上游 JSON 与 SSE、结算 SUCCEEDED）。

## 5. 已知差异与未完成

- chat 响应 `created` 为 0：guest 无可信时钟，legacy 使用当前时间。
- data URI 图片（需拆分 media type/base64）在 Anthropic 插件中 422，legacy 支持；需要新的通用字符串切分能力，未在 j2 实现。
- 单轮 inspection 展开 ≤4096 个事实且事实 JSON ≤256KiB，超长对话会以宿主预算错误失败，而不是降级。
- j3–j7 尚未开始。
