# 历史验收快照：客户钱包加入前

此快照不代表当前代码、资金职责或性能；其中命令和产物哈希仅用于历史追溯，当前验收见根 validation.md。

# 验收与性能报告

验证日期：2026-11-10。最终交付为独立 LLM Gateway 后端及独立 mock；没有业务钱包、支付或客户结算依赖。原始13份设计的归档SHA-256全部重新核对通过。

## 最终验证

- 原生 Bun check、原生 bun fmt --check、oxlint 均通过。
- **24 个单元测试、84 个真实 HTTP/PG/Redis 集成测试，共108项通过，0失败、0跳过。**
- 独立 bundle 在空工作目录运行；SIGKILL后重启追加UNKNOWN、同幂等意图不重执行；SIGTERM在关闭依赖前完成流式计量，均通过。
- Linux arm64 镜像以非root、只读文件系统、cap-drop ALL 和真实PG/Redis运行，JSON/SSE/干净停机均通过。
- Caddy原生validate、Compose配置检查通过；CI流程已提供，未声称已在远程GitHub运行。
- 专用PG备份恢复到随机临时库，**465,651 条attempt与465,651条usage完整一致**，恢复后的不可变触发器有效。备份88,059,594字节；详见[恢复证据](backup-restore.json)。
- 最小权限runtime角色实测可推理、不可UPDATE/TRUNCATE/DROP usage；多实例幂等、分布式准入、过期租约fencing、消费者注册与未提交事件交错均通过。
- 两个独立只读审查代理检查参考实现与本项目，多轮问题均修复并复核；最终未发现剩余可复现实质P1/P2。审查不替代长期生产运行。

## 本机实测

环境为macOS 27.0.1 / arm64、12逻辑CPU、24GiB内存；Docker分配12CPU、约11.73GiB内存。PG 17.11、Redis 7.4.11。Gateway与Tillgate为Bun 1.4.4原生进程；new-api为Go 1.26.10原生Mach-O arm64，CGO关闭、官方PGO和标准优化构建。PG池本项目与Tillgate均为64。

同一独立mock、同一短Chat请求、同一gpt-4o-mini上游别名、max_completion_tokens=32。报价为正，不关闭PG耐久性。每轮先热身1秒；报告实际测量时长包含在途请求收尾。数据为整条请求链路，不是路由纯函数微基准。

| 实现 / 工作负载         | 并发 |    时长 |  成功RPS | P50 ms |  P95 ms |  P99 ms | 成功 / 错误 |
| ----------------------- | ---: | ------: | -------: | -----: | ------: | ------: | ----------: |
| Tillgate / JSON         |   32 | 10.898s |    63.68 | 259.84 | 1896.94 | 2162.84 |     694 / 0 |
| new-api / JSON          |   32 | 10.146s |   330.88 |  76.75 |  233.72 |  329.59 |   3,357 / 0 |
| 本项目 / JSON           |   32 | 15.008s | 2,597.05 |  11.53 |   17.06 |   26.58 |  38,976 / 0 |
| 本项目 / JSON，持续60秒 |  128 | 60.021s | 3,335.84 |  33.19 |   50.85 |  233.84 | 200,220 / 0 |
| 本项目 / SSE            |   32 | 15.008s | 2,551.47 |  11.93 |   16.01 |   20.62 |  38,293 / 0 |

32并发的所测整条JSON链路，本项目RPS约为new-api的 **7.85倍**、Tillgate的 **40.78倍**；这不是“所有功能/所有负载全面优于”的结论。

最终60秒/128并发轮次的采样服务端峰值RSS约 **68.66MiB**，受控原始缓冲峰值48,428字节，计量失败0。P99为233.84ms，明显高于中位数；本机高并发尾延迟与更长稳态仍有优化和专门压测空间。RSS为采样值，不是严格进程峰值；CPU指标包含热身。负载发生器RSS另列在原始报告中。

## 耐久性核对

最终三轮连同热身共 **284,703** 个逻辑请求；PG中的request、attempt、usage事件数逐项一致，全部FINAL且请求SUCCEEDED，没有用异步漏记换RPS。每轮计数和精确金额总和见[JSON32](gateway-release-c32-audit.json)、[JSON128](gateway-release-c128-audit.json)、[SSE32](gateway-release-sse-c32-audit.json)。金额来自mock费率，不代表真实付费调用。

参考账务亦经独立只读核对：Tillgate864条billing_requests全部settled，receipt/usage/结算交易一致，无活动预留、钱包in_flight或渠道reserved；测量结束后约5.478秒才完成最后结算。new-api3707条唯一消费日志，quota扣减7414，与用户/令牌/渠道累计一致。测量外记录包括热身/探针，旧报告字段不足以进一步准确拆分。[聚合证据](reference-audit.json)。

## 公平性与边界

- 两参考基线均是一次约10秒的32并发JSON测试；本项目最终为15秒JSON、60秒高并发和15秒SSE，样本时长不同，没有重复样本置信区间。
- Tillgate使用原生正式进程入口加worker，NODE_ENV=development仅为允许本地HTTP mock，关闭OTEL与全局/预认证IP限流；没有把Node适配器或不结算夹具作为基线。new-api保留正常quota与消费日志流程、正费率、无限额测试令牌及官方PGO。
- 参考产品承担钱包/商业计费等更多工作；本项目按已确认的独立Gateway边界，只提交逐次usage与上游成本。不能把职责差异说成同等业务计算的算法倍数差异。
- 参考项目的SSE、长上下文、协议转换、错误重试、Files/Batch/媒体任务未做吞吐对比。本项目这些行为有功能/故障测试，但没有覆盖所有真实供应商的每个模型/新参数。
- **没有验证10,000 RPS持续60分钟，也没有完成跨主机/机房故障转移或真实供应商账号联调。** 未提供目标服务器与真实凭证，因此未执行公网发布或真实付费请求。

## 重复运行

```sh
bun run verify
bun run bench --model gpt-4o-mini --seconds 15 --concurrency 32 --metrics --output reports/recheck.json
bun --env-file=.env.local scripts/audit-benchmark.ts reports/recheck.json
bun run bench --model gpt-4o-mini --seconds 60 --concurrency 128 --metrics
bun run bench --model gpt-4o-mini --seconds 15 --concurrency 32 --stream --metrics
docker build -f infra/Dockerfile -t super-token-gateway:validation .
bun run test:container
bun run test:backup
```

--metrics仅用于本项目Gateway，读取ADMIN_KEY并采样私网指标；参考项目基线不要带此参数。audit-benchmark只适用于此项目自己的PG schema和独占测试窗口，检查包含热身的落库计数。

[原始JSON32](gateway-c32.json) · [原始JSON128](gateway-c128.json) · [原始SSE32](gateway-sse-c32.json) · [Tillgate](tillgate-c32.json) · [new-api](new-api-c32.json) · [压测源码清单](source-manifest.json) · [发布源码清单](release-source-manifest.json)

最终Gateway bundle SHA-256：`a16414536057772b234cb30d7f7d0cd31272a987af5862ff8252841107f06d63`。三个最终压测报告记录同一bundle与当时的manifest哈希；Gateway bundle已与最终dist重新比对。压测时源码清单另行保留，后续OpenAPI生成器格式修补不改变Gateway运行代码。部署与恢复步骤见[运维手册](reliability-and-operations.md)。
