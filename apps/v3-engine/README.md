# V3 High-Concurrency Go Engine (`apps/v3-engine`)

零第三方依赖的 Go 1.27.4 数据面与确定性内存 WAL/Quorum 账本引擎，面向高并发与低尾延迟场景，绕开传统事务型数据库热路径瓶颈，将单机吞吐推至硬件上限。

## 架构组件

- `internal/protocol`：受限大小 JSON 请求/usage 解析、无计量维度工具 `422 unmetered_tool` 拒绝、精确分数上限计费（`CeilingRational`），以及零正文缓冲的 `SSEFramer`（仅拦截 `[DONE]` 直至 Quorum 持久化提交后释放）。
- `internal/ratelimit`：64 分片单调时钟 RPM/TPM/并发限流器与 EWMA 熔断器，不依赖挂钟时间。
- `internal/ledger`：
  - `wal.go`：统一无锁 Group-Commit 二进制 WAL（CRC32C 校验、本地 `fsync` 与 Quorum 复制并行、启动崩溃截断与确定性重放，单请求 Grant+Settle 约 312 B）。
  - `quorum.go`：3 节点 `2-of-3` 同步二进制 TCP WAL 复制与单调 Epoch Fencing（`ErrFenced`），支持活体节点故障切换与 200 ms 故障退避。
  - `ledger.go`：64 分片 Shared-Nothing 双重记账引擎（客户钱包/授信/欠款/冻结与 64 个供应商记账单元、幂等 `AcquireGrant`/`CommitTerminal`/`AbortGrant`、Ed25519 签名验证、64 分片 Outbox `ClaimEvents`/`AckEvents` 与全量守恒审计 `AuditConservation`）。
- `internal/gateway`：原生 HTTP/1.1 + HTTP/2（`h2c` / TLS H2，`MaxConcurrentStreams: 65536`）、池化 HMAC-SHA256 鉴权与 IP CIDR 校验、SSRF/DNS Pinning Dialer、零槽位钉死 SSE 转发与 `/v1/chat/completions`、`/v1/wallets`、`/v1/events/{claim,ack}`、`/livez`、`/readyz`、`/admin/stats` 端点。
- `cmd/v3-engine`：独立进程入口（`-mode=gateway|replica`）。
- `cmd/v3-bench`：硬件极限压测与混沌校验器（含 H2c 上游、3 节点 `2-of-3` Quorum `fsync`、1,000 钱包、Ed25519 签名与校验、外部 HTTP ACK 消费、运行中强杀 Replica 与磁盘 WAL 崩溃恢复核对）。

## 命令

```sh
bun run build:v3
bun run test:v3
bun run test:v3:race
bun run bench:v3 -- -mode=json -concurrency=2048 -duration=6s -kill-replica-at=3s
bun run bench:v3 -- -mode=sse -concurrency=2048 -duration=6s
bun run bench:v3 -- -mode=sse-long -concurrency=20000 -duration=8s -stream-ms=2000
```

## 本机实测基线（Apple M6 12-Core / 24 GiB RAM）

所有窗口均开启 1,000 钱包、3 节点 `2-of-3` Quorum `fsync`、真实 Ed25519 签验、外部 HTTP ACK 与磁盘崩溃恢复守恒核对（`errors: 0`、`audit_active_holds: 0`、`audit_balanced: true`、`crash_recovery_exact: true`）：

| 场景                              | 并发  | 实测 RPS  | P50        | P99        | P99.9    | CPU 核耗   | 单请求 WAL |
| --------------------------------- | ----- | --------- | ---------- | ---------- | -------- | ---------- | ---------- |
| 短 Chat JSON（3s 活杀 Replica 1） | 2048  | 41,829.95 | 47.60 ms   | 72.27 ms   | 99.06 ms | 9.02 cores | 312.2 B    |
| 短 Chat JSON（过载饱和验证）      | 4096  | 41,008.66 | 99.74 ms   | 119.64 ms  | —        | 9.07 cores | 312.2 B    |
| 短 SSE（`[DONE]` 严格后置持久化） | 2048  | 33,922.81 | 60.09 ms   | 80.57 ms   | —        | 8.96 cores | 312.2 B    |
| 2 秒长 SSE（20,000 并发活流）     | 20000 | 6,617.19  | 2,480.7 ms | 3,445.3 ms | —        | 3.25 cores | 312.2 B    |
