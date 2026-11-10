# 接口与幂等契约

接口名称均为新系统建议，未实现。所有金额对象为 `{"asset":"USD","atoms":"1000000000000000000"}`，不接受 JSON 浮点金额。

## 1. 身份与请求标识

公网 API key/应用令牌由身份入口验证，付款账户和主体来自可信认证上下文，不接受 X-User-ID 或客户端任意指定 billing_account_id。Finance 最终验证 credential/成员的当前消费许可、scope、policyVersion 与付款归属。

缓存只加速预筛；即时撤销由对应资金组提交权威许可变更生效。控制面修改成功的语义必须包含该确认，不能把异步缓存更新当成已生效。

| 标识 | 对外/内部语义 |
| --- | --- |
| ingressId | 每次 HTTP 服务端新建，用于追踪、入口限流和请求日志 |
| orderId | 同一业务意图稳定，用于资金/任务/状态查询 |
| invocationId | 每次收费性物理上游发送，内部审计 |
| commandId | 一个金融动作的幂等身份 |
| providerRequestId | 外部事实，不能代替本地授权，也不能暴露秘密凭证 |

响应 `X-Request-Id` 为 ingressId，`X-Order-Id` 为稳定订单；CORS allow/expose 同步定义。协议正文 id 按各端点契约处理，异步 jobId 保持稳定。

## 2. 客户端业务幂等

`Idempotency-Key` 建议客户端每个新意图生成随机键并在网络重试时复用。作用域为付款账户+凭证+端点，长度/字符限制由统一 schema 固定。凭证轮换跨键重放若需要，必须通过受控订单查询/委托规则，不能任意扩大读取范围。

先鉴权、静态 schema 验证与旧意图查询，再为新意图动态计价。指纹包含请求产品、内容/文件摘要、采样/工具/输出要求、客户费用上限与替代授权；不包含 ingressId、实时路由、当前价格和日志字段。

| 情况 | 返回行为 |
| --- | --- |
| 同键同指纹，已受理/执行未知 | 返回相同 orderId 及状态，409/可查询，不发第二次调用 |
| 同键异指纹 | 409 idempotency_conflict，无金融变化 |
| 同键已完成但没有响应正文存档 | 返回终态与重放不可用信息，不重新生成正文 |
| 同键异步任务已创建 | 返回同一个 jobId |
| 同键已有 NO_CHARGE/RELEASED | 返回原终态，不复活收费或推理 |
| 期限已过但有旧身份摘要 | 410 idempotency_expired，明确要求新的业务意图 |

入口反滥用每次 HTTP 都计数，不使用稳定 orderId 作唯一 ZSET member。旧结果重放也要当前授权，不能凭知道一个幂等键读取他人请求。

## 3. 公网调用示例

```http
POST /v1/chat/completions
Authorization: Bearer <customer-key>
Idempotency-Key: 8f491570-498c-4547-ab04-173af6cdab62
Content-Type: application/json

{"model":"logical-product","messages":[{"role":"user","content":"hello"}],"stream":true,"max_completion_tokens":1024}
```

保持兼容 JSON/SSE 行为；`stream:false` 返回 JSON，不因内部计量方便强制转成 SSE。产品不允许的参数在授权前拒绝。多模态/文件使用端点专属上界，不能以 chat token 公式代替。

建议资源接口：

- `GET /v1/orders/{orderId}`：返回 execution、charge、resolution 和证据可用状态。
- `GET /v1/balances`：分别显示现金/非现金、posted、reserved 与可用，注明 asOfVersion。
- `GET /v1/jobs/{jobId}`：异步产物状态，必须校验当前付款归属。
- `GET /v1/charges`：游标分页账单，原始收费、估算、免收、退款明确区分。

内部供应商成本、凭证和未脱敏错误不向普通客户暴露。

## 4. 金融命令

首版内部 RPC 使用版本化 HTTPS JSON，通过 Bun.serve 提供接口、fetch 调用，使用工作负载认证与显式权限。金额、精确计量和大序号为十进制字符串，接收端运行时 schema 校验后构造成领域值；禁止把 bigint 直接放进 Response.json。可公开为安全小整数的 revision 必须有范围校验。名称表达业务动作，不暴露数据库句柄或任意 journal accountId。

DTO、schema 和编码器放在 contracts 包，金融命令规则放在 finance 包。TypeScript 类型不代替网络校验；不向客户端返回内部 Error 对象。请求传递绝对/剩余期限，不传可序列化的“取消对象”；各进程本地创建 AbortController。证据收尾与外部客户取消分离，但仍有服务期限。fetch/SDK 重定向与自动重试必须经过物理发送边界审核，不能由框架默认行为增加调用。

同步默认使用合并 AdmitAndPermit；本地异步排队使用 EnqueueOrder（固定价格与客户预留，不发上游）和 PermitQueuedInvocation（验证该预留、取得首次供应商许可），订单/任务仍沿用同一幂等身份。QUEUED 不代表已经调用供应商。

```text
AdmitAndPermit(
  commandId, authAssertion, clientIntent, payloadFingerprint,
  productPriceVersion, finalizedRequestSummary,
  connectorVersion, outboundFingerprint, ownerIdentity, placementEpoch
) -> CREATED_PERMIT | OBSERVE_ONLY | REJECTED | COMMIT_UNKNOWN

PrepareNextInvocation(
  commandId, orderId, priorNotExecutedEvidence,
  nextConnectorVersion, outboundFingerprint, ownerIdentity
) -> CREATED_PERMIT | OBSERVE_ONLY | REJECTED | COMMIT_UNKNOWN

SealEvidence(
  commandId, orderId, invocationId, sourceIdentity,
  finality, rawUsage, evidenceHash, optionalObjectRef
) -> SEALED_BILLABLE | SEALED_COST_ONLY | DUPLICATE | CONFLICT | COMMIT_UNKNOWN

Capture(commandId, orderId, expectedRevision) -> CAPTURED | ALREADY_FINAL | NOT_READY
DecideNoCharge(commandId, orderId, policyOrApproval) -> NO_CHARGE | ALREADY_FINAL
ResolveProviderCost(commandId, invocationId, evidenceId) -> ACCRUED | PROVISIONED | RELEASED | CONFLICT
```

金额与用量由 Finance 根据可信版本/证据重算，不接受调用者传入 cost=某数即直接扣费。authAssertion 只能来自经认证的入口服务，不能客户自填；Finance 仍检查当前 credential 状态和范围。

`COMMIT_UNKNOWN` 是独立状态。调用者读取旧 command/order；不能把超时变为新 commandId 再试。金融回执稳定业务结果，与一次性的发送能力分开：重放不返回 CREATED_PERMIT。

## 5. 证据与状态查询

SealEvidence 是金融事实入口，拒绝非法负数/超范围计量、归属不匹配、未知价格或伪造 finality。来源可为连接器、受认证查询器、发票导入器；它们具有不同权限和验证规则。

对象引用不是成功凭证：最小用量证据必须在本地 PG，额外原件需要可验证持久对象与摘要。重复来源键同 hash 幂等，不同 hash 冲突并保留审计，不覆盖。

状态示例：

```json
{
  "order_id": "<uuid>",
  "execution": "UNKNOWN",
  "charge": "RESERVED",
  "customer_asset": "USD",
  "reserved_atoms": "10000000000000000",
  "captured_atoms": "0",
  "resolution_deadline": "<UTC timestamp>",
  "revision": 7
}
```

不要把 charge=NO_CHARGE 显示成“供应商已取消”；不要把最终收据已保存显示成“钱包已实扣”，也不要把估算费用显示为精确账单。

## 6. 错误与客户重试

| HTTP | code 类别 | 语义 |
| --- | --- | --- |
| 400/422 | invalid_request / unsupported_billing_bound | 未发送，修正新意图 |
| 401/403 | unauthenticated / scope_denied | 当前身份无消费或读取权限 |
| 402 | insufficient_funds | 权威可用资金不足，未发出本次调用 |
| 409 | intent_conflict / in_progress / decision_conflict | 查询旧订单，不盲目换新键 |
| 410 | idempotency_expired | 旧键不能静默变成新收费 |
| 429 | capacity_limited / quota_limited | 有界 Retry-After；是否受理另由订单状态表达 |
| 503 | finance_unavailable / placement_unready / funds_rebalance_pending | 不一定是余额不足；提交未知时必须给旧订单定位 |
| 502/504 | provider_failed / provider_outcome_unknown | HTTP 状态不能推断免费或可安全重发 |

错误 envelope 包含 code、message、ingressId、可得的 orderId、statusUrl、`retry_same_key` 与 `accepted`。accepted 结果未知时用明确 UNKNOWN，不谎报 false。所有原始供应商错误脱敏，不能泄露内部 URL、模型或 key。

流已开始时不能修改原 HTTP 状态；发协议错误事件或异常关闭，正常完成语义必须经过收据屏障。

## 7. 管理、审批与版本

管理 API 分成配置发布、账户政策、预算分配、支付/退款、差异审核。双人审批和工作负载权限通过统一规则验证，不能普通管理员直接调用任意金融写函数。

所有写请求有 commandId/业务键和载荷指纹；相同键异参拒绝。配置发布返回不可变版本与目标资金组生效确认。schema/RPC 升级采用加字段、能力探测和有界旧读窗口；新系统没有必须兼容其他项目状态机的义务。
