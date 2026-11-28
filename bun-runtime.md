# Bun 运行时与产物

仓库版本由 [.bun-version](.bun-version) 和 [package.json](package.json) 固定为 Bun 1.4.5，依赖由 `bun.lock` 锁定。Hono 处理应用路由，入口使用 `Bun.serve`。PostgreSQL 使用 `Bun.SQL` 与 Drizzle，Redis 使用 `Bun.RedisClient`。

```sh
bun run check
bun run lint
bun run fmt:check
bun run build
```

类型检查和格式脚本用 `--no-script` 调用 Bun 原生命令。编译选项以 [tsconfig.json](tsconfig.json) 为准。

| 应用 | 配置入口 |
| --- | --- |
| Gateway | [config.ts](apps/gateway/src/config.ts) 从环境变量与 `--mode` 读取配置。生产数据、快照和管理连接池默认分别为 32、2、4。默认在途上限 2000、请求体 10 MiB、单次尝试 30000 ms、流总时长 300000 ms。 |
| Worker | [main.ts](apps/worker/src/main.ts) 从环境变量读取连接池与维护间隔。`WORKER_DB_MAX` 默认 8，至少保留选主和工作连接。 |
| Console | [config.ts](apps/console-api/src/config.ts) 读取独立数据库、客户与管理监听、Gateway 管理地址及逐角色凭证。 |

账本连接的默认建立超时为 5 秒，`statement_timeout` 为 8000 ms，`lock_timeout` 为 5000 ms。设置连接数时计入所有应用副本和辅助连接池。HTTP 请求体、连接等待、流输出及插件执行都受各自预算约束。上游传输在 DNS 校验后连接已校验地址并保留 TLS 身份，等待可取消。见 [数据库连接](packages/ledger/src/database.ts) 和 [出站传输](packages/runtime/src/network.ts)。

[构建脚本](scripts/build.ts) 输出 `dist/gateway/main.js`、`dist/worker/main.js`、`dist/console-api/main.js`、`dist/mock-providers/main.js`，以及 Trace/WASM 辅助入口、`dist/tools` 中的迁移与运维工具、`dist/native` 的 Wasmtime 库和 `dist/manifest.json`。清单列出 Bun 与数据库 schema 版本，并记录构建文件的大小和 SHA-256。独立产物使用 Bun 运行。部署模式及私网边界见 [运行手册](reliability-and-operations.md)。
