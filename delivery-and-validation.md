# 测试与可重复交付

当前交付包含 Gateway、Worker、Console API、独立 mock、各自的迁移和独立运行产物。[系统架构](architecture.md) · [Bun 运行时](bun-runtime.md)

## 验证命令

```sh
bun run verify
bun run test:production
bun run test:alerts
```

`verify` 按 [package.json](package.json) 执行类型、lint、格式、生成的 OpenAPI、单元和集成测试、构建、产物及耐久验证。CI 使用专用测试凭证和 PostgreSQL 17、Redis 7.4，构建原生 Wasmtime 后启动本地测试依赖。见 [CI 工作流](.github/workflows/verify.yml)。

集成夹具使用测试 PostgreSQL 连接创建随机名的独立数据库，按需使用隔离 Redis 实例或命名空间。准备环境后可运行 `bun run test:integration`，也可用 `bun --env-file=.env.local test tests/integration --test-name-pattern <名称>` 选择用例。[Gateway 夹具](tests/integration/gateway-fixture.ts) · [Ledger 夹具](tests/integration/ledger-fixture.ts)

## 高价值测试来源

- 模块用例位于各模块的 `__test__/`，覆盖金额和计价、协议与用量、传输、插件以及 Gateway 准入。
- [架构测试](tests/unit/architecture.test.ts) 核对 workspace 依赖、推理阶段边界、迁移清单与部署单元。
- [Gateway 集成](tests/integration/gateway-v4.test.ts)、[账本结算](tests/integration/ledger-settlement.test.ts)、[Outbox](tests/integration/ledger-outbox.test.ts) 和 [Console 账务投影](tests/integration/console-api-billing-outbox.test.ts) 覆盖真实数据与进程边界。
- [产物验证](scripts/verify-artifact.ts) 在隔离目录使用构建后的应用验证端到端流程与账本归档。
- [耐久验证](scripts/verify-durability.ts) 运行独立产物，强杀 Gateway 后观察预扣回收，验证已提交操作的幂等重放，以及 `pg_dump`、`psql` 的逻辑备份恢复。
- [生产演练](docs/decisions/0011-production-operations.md#复现与计时) 使用临时回环服务验证运行角色、PostgreSQL 主备、PITR 和 Redis Sentinel。验证结论以演练的拓扑和样本为限。

性能验证使用 [现行基准](README.md#验证与基准) 和 [容量及对标契约](docs/performance/capacity-contract.md)。记录构建版本、并发、尾延迟、PG 耐久参数、逐请求结算、余额守恒与投递进度。
