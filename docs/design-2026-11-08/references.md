# 依据、选型与借鉴

## 1. 用户需求

本方案依据会话确定的目标：全新模型网关、10,000+ RPS、用户资金预扣与准确收费、供应商智能切换、崩溃恢复、数据库设计和 Markdown 图文交付。Redis 已开启持久化是已知条件，但不是资金必须放入 Redis 的强制约束。

Bun + TypeScript 已由用户明确确定；资金消费单元、provider allowance、本地 PG 金融工作表是本方案的设计建议，不是用户已有实现事实。硬件、日均流量、供应商费用恢复能力和业务时限未给定，文档将其标成待定稿/待实测。

## 2. 仅借鉴的工程原则

从参考代码中有价值的地方提炼出：金额计算集中管理；授权、结算与释放是独立金融原语；价格/资金来源有不可变快照；后台认领有租约与幂等；批处理隔离毒项；路由用优先级、加权和健康证据。

本目录没有引用参考项目 API/表名作为新系统约束，没有移植其状态、包组织、授信策略、默认值或发布流程；也不复用其单元测试结果证明新架构。旧项目和其他目录的设计文档不是本设计的依赖。

## 3. 技术决策摘要

| 决策 | 推荐原因 | 什么证据可能改变 |
| --- | --- | --- |
| Bun + TypeScript 全后端 | 用户已选定；统一网络、领域与后台开发，使用 bigint 精确金额 | 运行时/驱动/观测兼容性按版本实测；先解决具体瓶颈，不重开语言选型 |
| PG 资金权威 | 原子交易、唯一键/锁、同分片金融操作 | 完整负载证实不满足且另一权威日志的故障协议已验证 |
| 先划拨本金再并行消费 | 热点拆分同时保持总额不可重复消费 | 若业务规模很小可只用一个消费 cell，不能靠假复制余额优化 |
| 本地金融工作表 | 收据与任务同事务，减少外部消息依赖 | 工作领取/分发确实成瓶颈时加 broker，财务幂等不变 |
| Redis 软状态 | 快速、可重建；故障不丢唯一预扣 | 不因 AOF 开启就取消 PG 权威 |
| 用户免收与成本分开 | 给客户可结束账单，平台责任仍受预算约束 | 供应商契约/商业政策可改变处置时限，不可复活已免收原始收费 |

## 4. 权威资料

核对日期：2026-11-08。下面资料说明组件保证，不证明本系统已经实现或达到目标吞吐。

- [Bun HTTP](https://bun.sh/docs/runtime/http/server)：Bun.serve、长流超时与服务生命周期。
- [Bun Streams](https://bun.sh/docs/runtime/streams)：Web Streams 与流式处理。
- [Bun SQL](https://bun.sh/docs/runtime/sql)：PG 连接池、事务和 numeric 字符串；提交响应未知仍需业务处理。
- [Bun TypeScript](https://bun.sh/docs/runtime/typescript)：类型定义与编译配置。
- [Bun Workers](https://bun.sh/docs/runtime/workers)：线程 API 的实验状态；不作为首版金融可靠性前提。
- [Bun 依赖安装](https://bun.sh/docs/pm/cli/install)：锁文件与 frozen-lockfile。
- [PG 17 显式锁](https://www.postgresql.org/docs/17/explicit-locking.html)：行锁、事务锁与死锁分析。
- [PG 17 SELECT / SKIP LOCKED](https://www.postgresql.org/docs/17/sql-select.html)：并发领取工作不等于一般一致性查询。
- [PG 17 约束](https://www.postgresql.org/docs/17/ddl-constraints.html)：普通 CHECK 的跨行约束限制。
- [PG 17 分区](https://www.postgresql.org/docs/17/ddl-partitioning.html)：分区表唯一约束与归档考虑。
- [PG 17 WAL/同步提交](https://www.postgresql.org/docs/17/runtime-config-wal.html)：本地/同步副本耐久确认边界。
- [Redis 复制](https://redis.io/docs/latest/operate/oss_and_stack/management/replication/)：异步复制与已确认写在切主时丢失的边界。
- [WHATWG SSE](https://html.spec.whatwg.org/multipage/server-sent-events.html#event-stream-interpretation)：增量事件/行解析。
- [供应商流式 usage 示例限制](https://developers.openai.com/api/reference/resources/chat/subresources/completions/streaming-events)：中断可能缺失最终用量；不据此推断所有供应商有同样恢复能力。
