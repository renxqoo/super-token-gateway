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

测试文件：`tests/unit/kernel.test.ts`（精度/SSE/安全），`conversion.test.ts`（损失策略与转换），`runtime.test.ts`（内存/取消/停机），`architecture.test.ts`（依赖边界）；`tests/integration/gateway.test.ts`（完整 HTTP/PG/Redis 与故障）。最终运行数量与性能见 validation.md。

没有访问真实付费模型，没有给外部用户发消息，没有部署到未提供的服务器。Linux 配置和 CI 是可重复发布资产，实际公网部署与跨主机/机房故障演练需要目标基础设施。

旧PG usage/outbox、旧版schema和独立旧审计脚本均已删除。完整容量审计统一由bench-system.ts执行。test:durability只破坏自己创建的临时PG库、Redis容器、AOF卷和网络；不操作项目正常依赖的队列。
