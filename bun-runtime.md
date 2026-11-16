# Bun 1.4.4 运行时

运行和构建固定 Bun 1.4.4；`.bun-version` 与 packageManager/engines 一致。Hono 使用 Bun 原生 HTTP，不引入 Node HTTP 转接层。PostgreSQL 用 Bun.SQL，Redis 用 Bun.RedisClient，依赖由 bun.lock 锁定。

```sh
bun check --no-script
bun fmt --no-script
bun fmt --no-script --check
bun run lint
bun run build
```

`--no-script` 明确调用 Bun 原生命令，避免 package.json 的同名脚本转发。所有 TypeScript 启用 strict、noUncheckedIndexedAccess 和 exactOptionalPropertyTypes。

v2 Gateway 的 JSON 配置在 `apps/gateway/src/config.ts` 中验证，Core API 的 JSON 配置在 `apps/core-api/src/config.ts`，core-worker 的环境变量在 `apps/core-worker/src/config.ts`。Gateway 关键默认值：总时限 180s、上游响应头/流空闲与下游停滞各 30s、请求 16MiB、JSON 响应 32MiB、在途请求 1024、停机宽限 20s；插件宿主的 SSE 单事件上限 1MiB；Core 默认最多物理尝试 3，Core API 内部与管理连接池各 8，core-worker 连接池 8。

这些上限是受控缓冲与并发预算，不是操作系统 RSS 上限。JSON/字符串/转换对象和运行时也占内存；生产需配合 systemd 内存上限与压测调整。流队列按字节背压，输出不写临时文件。

Redis 命令 1 秒 deadline，连接失败不离线排队；不确定的分布式准入返回 503，租约自行回收。PG 服务端 statement_timeout=8s、lock_timeout=2s、连接建立 5s；不要把连接池总量扩到超过 PG 预算。

Gateway 配置 `production: true` 时禁止 `allow_private_upstream: true`；上游必须 HTTPS。DNS 全地址校验后将请求固定到已检查 IP，保留 Host/SNI/证书验证，禁用环境代理和自动重定向。供应商密钥不发送到跨源视频下载地址。

构建产物包括 gateway/main.js、core-api/main.js、core-worker/main.js、tools/ 下的迁移与运维工具、SQL 迁移、mock-providers/main.js、原生插件 helper 和源码 SHA-256 manifest。生产只需 Bun 和 dist，不需要工作区 node_modules。
