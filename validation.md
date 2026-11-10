# 当前验证与性能进度

2026-11-10。当前唯一实现为 **PG 调用前预扣 → BullMQ 数字证据与同连接 WAITAOF → worker 同事务写 PG usage / charge / journal → ACK**。业务系统独立，Gateway 执行多币种钱包与客户结算。旧 usage/outbox 入口和旧 schema 已删除，不支持旧代码/旧库兼容。

## 已取得的证据

- 当前schema2026111005完整门禁：39项单元、156项真实PG/Redis/HTTP集成通过（30项资金、126项HTTP），0失败；类型、oxlint与格式门禁通过。
- 编译产物在空工作目录运行：worker 停止时落盘后可成功，恢复后精确结算；Gateway SIGKILL 后不重发上游；SSE 和慢速 JSON 的 SIGTERM 行为通过。
- `test:durability`：真实 PG 未提交事务内与 commit 后未 ACK 两个位置 SIGKILL，重投均只结算一次；主库独立从 AOF 恢复；主库停机时副本独立从自身 AOF 恢复并晋升，仍可结算。缺耐久副本或非严格配置拒绝准入。该测试不证明机房断电、任意仲裁故障或所有 HA 拓扑。
- 独立只读审查已发现并推动修复授权、临时表遮蔽、晚到资源覆盖交付尝试、赠额到期退款、债务退款、异常拒绝释放和 RELEASE 携带价格等回归；没有用降低资金断言消除失败。
- schema2026111004已通过Linux三进程容器、AOF故障和排空后备份恢复验证；schema2026111005已切换单一批量结算入口，其同类部署与故障检查需按当前版本复验，不能沿用旧产物作为新版本通过证据。

## 历史容量基线

本机 macOS/arm64，12逻辑CPU、24GiB；Docker VM约11.73GiB。PG17.11、独立准入Redis与AOF always队列Redis；Bun1.4.4。所有有效完整账务压测均创建临时数据库/namespace、非零预扣、真实worker、外部usage消费者，并逐项核对请求、尝试、精确usage/售价、charge、hold、journal和钱包，最终排空队列。

| 此前队列链路阶段          | 实例 / 钱包 / 并发 | 时长 | 成功HTTP RPS | 账务核对    | 停压排空 |
| ------------------------- | ------------------ | ---- | ------------ | ----------- | -------- |
| 严格入口基线              | 4 / 1000 / 512     | 30s  | 2491.94      | 78506笔一致 | 743ms    |
| 合并唯一结算入口          | 4 / 1000 / 512     | 15s  | 3049.98      | 49758笔一致 | 338ms    |
| 减少重复写入与余额HOT更新 | 4 / 1000 / 512     | 30s  | 2300.48      | 74739笔一致 | 1049ms   |

时长不同且有波动，不能将其中一次最高值视为持续容量。降低worker并发的一轮触发积压并产生未结清预扣，已判失败；它揭示了发布端错误拒收已执行事实的问题，已以回归测试修正。当前瓶颈仍需继续测量，不能只凭 HOT 更新比例或局部函数优化推断性能提升。

当前唯一资金算法为`settle_requests`，已删除标量结算及旧恢复结算入口；队列批次先摄入事实，再按逻辑请求去重结算。NUMERIC38溢出、跨钱包poison、同批冲突、多币种/赠额分配均有回归；新版本吞吐仍待实测。

**当前未达到10000+ RPS生产交付目标。** 尚需当前版本完整回归、稳态与开放到达负载、结算延迟/积压曲线、热钱包容量、多实例长期故障验证和目标部署环境验收。没有真实付费供应商账号联调或公网部署。

## 复验

```sh
bun run verify
bun run test:durability
docker build -f infra/Dockerfile -t super-token-gateway:validation .
bun run test:container
# 停止所有准入/生产者并排空队列后：
bun run test:backup -- --quiesced
bun --env-file=.env.local scripts/bench-system.ts --seconds 30 --instances 4 --wallets 1000 --concurrency 512
```

不要同时运行 PG 集成/故障测试与容量压测。standalone `bench.ts` 只报告HTTP，`bench-system.ts` 才核验完整账务；开放到达使用 `--rate`，并明确报告被负载发生器并发上限拒发的请求。旧参考对比及旧产物数据仅为[历史快照](docs/performance/historical-validation-2026-11-10.md)，不代表当前新资金链路与参考产品的同职责比较。
