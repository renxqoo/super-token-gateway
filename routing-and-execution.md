# 路由、重试与流式完成

1. 本地验证请求字节、JSON 深度、项目 Key/操作/模型权限及资源归属。
2. 通过 Redis TIME 和 Lua 原子检查项目 RPM、并发及保守 TPM 预留；租约覆盖总时限与清理余量。
3. 在 PG 创建唯一意图；同 key 的同 payload 返回 409 和原 request_id，payload 冲突返回 idempotency_conflict。
4. 先按优先级，再按配置的 WEIGHTED、ROUND_ROBIN 或 LEAST_LATENCY 选路；开启的熔断器不接新调用，冷却后只允许一个跨实例探测。
5. 协议可表达性检查后取得供应商 RPM/并发许可，持久化 attempt，再发送请求。
6. JSON 先读入有界内存、解析旁路 usage、提交证据，再返回结果。SSE 先验证首事件，再以拉取/背压转发。
7. 数字证据通过 BullMQ 入队并同连接取得 WAITAOF 确认。流式正常终止窗口在落盘确认后才能下发；worker 之后在单个 PG 事务写证据、事件投递和账务；EOF 不合成成功。
8. 释放项目/供应商并发；只有所有尝试有最终计量时才按事实调整保守 token 预留，否则保留至时间桶自然过期。

默认最多 3 次物理尝试，可配置 1–5；候选切换和同渠道重试共享预算。接受用户确定的可用性优先策略：输出前允许对未知执行结果有限重试，每次保留 UNKNOWN；无法确认的远端执行可能继续占用供应商容量，本地连接并发不能证明其已停止。上游 Retry-After 与指数抖动约束同一供应商重试。任何响应内容下发后，不拼接另一轮调用。

客户端取消、总 deadline、上游首包/流空闲、下游停滞、进程退出都触发有界清理。部分计量不被伪装为最终；另一路异步查询已得到 FINAL 时，后到的断流 PARTIAL/UNKNOWN 不得降级最终计量投影。

请求指纹保留对象键无关性、数组顺序、显式 false/0 和 IEEE-754 之外的数字原文。模型别名在发送时替换，Files/Batch 资源固定到原供应商，不能切换到不拥有该资源的账号。

跨协议只转换已能保真的语义：文本、常用图片、工具调用/结果、输出上限、采样、停止词、工具选择及 JSON schema。未支持的签名、思维块、托管工具、多候选、严格模式等返回 conversion_loss。原生接口透传可支持字段，不用通用中间格式裁掉新字段；输出上界只接受本协议原生字段，没有计量维度的服务端托管工具在准入前返回 unmetered_tool（[ADR 0006](docs/decisions/0006-review-remediation.md)）。

模型兼容策略与扩展边界见 [ADR 0007](docs/decisions/0007-money-and-model-compatibility.md)。适配器使用编译期注册表；同协议模型复用实现，Bedrock 当前适配 Anthropic Messages，Vertex 当前适配 Gemini，其他模型家族须新增完整适配与验收后开放。不会自动承诺厂商所有模型。

每条路由可配置 `request_policy`，对转换后的目标原生请求执行 `allowed_parameters` 顶层白名单、`blocked_parameters` 点分路径拒绝和 `parameter_ranges` 数值范围检查。路径中的 `*` 只展开一层数组或对象；例如 `tools.*.function.strict` 和 `generationConfig.temperature`。白名单需包含网关注入的 `model`、输出上界、流式 `stream_options` 等目标字段；Gemini 的 model/stream 不在 JSON body 中。空策略保留原生扩展字段透传。策略限制目标模型，不从名称猜测能力。

例如 DeepSeek 路由可设置 `{"parameter_ranges":{"max_tokens":{"maximum":8192},"temperature":{"minimum":0,"maximum":2}},"blocked_parameters":["response_format.json_schema"]}`。该例演示配置格式，具体约束必须按部署模型版本验收。未兼容候选在报价和预扣前移除；全部拒绝返回 422/unsupported_capability 或 conversion_loss，不调用上游、不产生预扣。更新路由策略替换整个策略对象，`{}` 清空限制，配置缓存沿用最多一秒的跨实例失效边界。
