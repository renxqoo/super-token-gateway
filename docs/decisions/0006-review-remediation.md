# ADR 0006：整仓审查整改

2026-11-11。状态：用户授权修复整仓审查的全部发现；本决定补充 ADR 0002/0003/0005，不改变资金权威、授信公式与耐久等级。

## 预扣输入

- 输出上界只接受入口协议的原生字段：Chat 为 `max_completion_tokens` 或 `max_tokens`（二选一），Responses 为 `max_output_tokens`，Messages 与 Completions 为 `max_tokens`，Gemini 为 `generationConfig.maxOutputTokens`。显式 `null` 视为未提供，按模型上限预扣并写回原生字段。同时出现多个上界或出现其他协议的上界字段返回 400/invalid_request。原生透传不再允许客户用上游忽略的字段压低预扣，而让上游按另一字段生成。
- 服务端执行、另行计费且没有计量维度的托管工具在准入前返回 422/unmetered_tool。按白名单放行由客户端执行、只按 token 计费的工具：Chat 为 `function`、`custom`，不接受非空 `web_search_options`；Responses 为 `function`、`custom`、`computer_use_preview`、`local_shell`、`shell`、`apply_patch`；Messages 为无类型、`custom` 及 `bash_*`、`text_editor_*`、`computer_*`、`memory_*`；Gemini 为 `functionDeclarations`/`function_declarations` 与 `computerUse`/`computer_use`。`tools: null` 视为未提供。未列入白名单的新工具类型默认拒绝，确认其由客户端执行后再加入。Batch 输入逐行执行相同规则。
- Gemini `toolUsePromptTokenCount` 计入输入 token，与 `promptTokenCount` 合计后再扣除缓存读取。

## 结算与准入

- 无法自行推进的预扣（WAITING、CONFLICT、ERROR）下次对账时间为 `min(1小时, max(30秒, 预扣已存在时长))`，新证据、人工对账和现金唤醒仍立即触发。对账候选先处理待收债务，再处理等待证据的预扣，现金入账后的收债与解冻不排在积压之后。债务唤醒只在钱包存在可用现金批次时进行，且跳过计量冲突未解决的债务，赠额释放不会反复唤醒无法回收的债务而挤占等待证据的预扣。异步资源轮询把计费结果改为成功、取消或未知时，触发器同时把对应预扣设为立即对账，不等待退避。
- 批量准入中凭证、范围、路由、报价、冻结、资金或信用不足、精度溢出及幂等冲突均作为单项结果返回，不回滚整批；组织或主体停用及 Key 角色只由 `gateway_requests` 插入触发器复核，此类拒绝与非预期错误仍按有界二分隔离，结果正确，仅多耗二分时间。带幂等键的请求在全部校验通过后才写入 `gateway_requests`。
- 证据队列准入门控只统计 waiting/active/delayed。failed 作业保留，由 `GET /admin/queue` 暴露数量并告警，人工重试；不因单个钱包的失败作业关闭全部租户准入。failed 作业另有与 `QUEUE_MAX_PENDING_JOBS` 相同的独立上限，达到后关闭准入，限定 noeviction Redis 中保留作业的内存。

## 一致性与权限

- 冻结原因由 `wallet_freeze_reason(disputed, manual, debt, credit_reserved, credit_limit)` 统一计算，所有资金函数、授信调整、人工冻结与对账均调用该函数。
- 运行角色对 `gateway_requests` 只保留 `state`、`updated_at` 列更新权；计费结果、报价与选中尝试只能由安全定义函数修改。运行角色显式获得 `wallet_freeze_reason` 执行权，备份恢复撤销 PUBLIC 后仍可执行人工冻结与对账。

## 迁移

schema 版本升至 2026111102。支持 schema06–10 与 2026111101 显式停机升级：2026111101 不重复执行授信 DDL，只替换函数、补建冻结函数并收窄运行角色的列权限。升级撤销冻结函数的 PUBLIC 执行权并显式授予已存在的运行角色，与全新初始化的 ACL 一致。新增 schema08 与 2026111101 升级夹具及回归。

## 验收门槛

`bench-system --max-p99-ms` 只能收紧不能放宽 1500 ms 容量门槛，超过门槛的参数直接拒绝，验收判定同样拒绝。
