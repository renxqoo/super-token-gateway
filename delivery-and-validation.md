# 测试与可重复交付

本次交付包括 Gateway 应用、独立供应商 mock、分层基础包、唯一当前 SQL 定义、管理 API / OpenAPI、开发环境脚本、原生 Bun 构建、CI、Linux/systemd/TLS 示例、测试与实测压测报告。业务系统未混入此交付。

## 验证命令

```sh
bun run verify
bun run openapi
bun run test:durability
bun --env-file=.env.local dist/tools/migrate.js
```

verify 顺序执行原生类型检查、oxlint、原生格式检查、单元测试、真实依赖集成、独立构建。CI PostgreSQL 使用专用测试凭证；不复用生产库，也不依赖 Docker 安装脚本。

集成测试创建 `gateway_test_<随机后缀>`，迁移并运行测试后只删除该库；Redis 使用随机 namespace。测试结束等待流清理；不会清空用户的 Redis、其他数据库或参考项目的队列。可使用 `bun --env-file=.env.local test tests/integration --test-name-pattern <名称>` 定位失败场景。

## 高价值测试来源

参考 Tillgate 的静默降级、重试护栏、深流式、超时规模、异步恢复场景；参考 new-api 的转换 golden、tool loss policy、usage merge、真实慢读取消、DNS 固定和资源归属场景。此仓库按独立 fixture 与行为断言重新实现，未拷贝参考项目的商业层或测试运行器。

测试文件：模块单测位于各模块 `__test__/`，如 `packages/kernel/src/__test__/kernel.test.ts`（精度/SSE/加密），`packages/connectors/src/__test__/conversion.test.ts`（损失策略与转换），`packages/runtime/src/__test__/bounded-io.test.ts`（内存/取消）与 `network.test.ts`（出站地址），`apps/gateway/src/__test__/`（准入输入、停机与回归）；跨模块的 `tests/unit/architecture.test.ts`（依赖边界）；`tests/integration/gateway.test.ts`（完整 HTTP/PG/Redis 与故障）。最终运行数量与性能见 validation.md。

没有访问真实付费模型，没有给外部用户发消息，没有部署到未提供的服务器。Linux 配置和 CI 是可重复发布资产，实际公网部署与跨主机/机房故障演练需要目标基础设施。

旧PG usage/outbox、旧业务运行路径和独立旧审计脚本均已删除；schema06 / schema07只作为隔离升级回归夹具保留，不参与运行时兼容。完整容量审计统一由bench-system.ts执行。test:durability只破坏自己创建的临时PG库、Redis容器、AOF卷和网络；不操作项目正常依赖的队列。

性能审计显式区分 paid/free 客户售价、live/none 外部消费者、live/deferred worker。deferred 在转发阶段保留调用前 PG 预扣及耐久证据确认，停压后才启动 worker；只有全部资金与证据排空核对通过才报告数据有效，不能据此声称后台持续结算吞吐达标。报告记录 PG 耐久参数、两类 Redis 配置、入队合批、并发和产物哈希。

当前 Mac 可运行 `bun run test:durability -- --native`，使用 PATH 中的 redis-server 与独立临时 AOF 目录执行相同强杀、重启及晋升断言；控制命令核对 Redis process_id 与本轮子进程 PID 后才执行。默认入口仍使用固定 Docker 镜像。
