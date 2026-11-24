# Gateway 容量复测 2026-11-25

本机 Gateway 在新加坡时间 2026-11-24 23:59 完成优雅重启。Redis 就绪，原有项目 Key 可以查询 `MiniMax-M3.1-Flash`。吞吐试验使用 `cac90e4` 构建产物、本机独立 PostgreSQL 17.11、独立 Redis 7.4 和模拟上游。机器为 Apple M6，12 核、24 GiB，运行 Bun 1.4.5。每轮新建测试库，PostgreSQL 保持 `fsync`、`synchronous_commit` 和 `full_page_writes` 开启。

## 短 JSON 并发阶梯

每档 7 秒，使用已预热的连接、64 个测试项目和 2 个模拟上游。所有组使用同一构建产物和相同的每副本数据库连接池上限 32。负载端在多副本组内保持同一项目的请求亲和，以减少同一钱包跨进程的锁竞争。

|  并发 | 单副本 RPS / P99 ms | 双副本 RPS / P99 ms | 四副本 RPS / P99 ms |
| ----: | ------------------: | ------------------: | ------------------: |
|   512 |    17,645.9 / 43.27 |    22,016.2 / 37.90 |    16,963.1 / 70.03 |
| 1,536 |   19,348.0 / 108.96 |    27,274.4 / 76.08 |    29,577.7 / 82.97 |
| 2,304 |   18,951.1 / 170.41 |   26,020.1 / 177.89 |   30,519.0 / 189.50 |

三组均为零请求错误、零重复结算，排空后活跃租约和未明预扣归零。四副本在 1,536 并发提升 52.9%，P99 降低 23.9%。2,304 并发的提升为 61.0%，P99 上升 11.2%。512 并发由双副本取得最高吞吐和最低 P99。

独立单副本诊断跑到 22,101.8 RPS，1,536 并发的 P99 为 96.09 ms。该诊断使用另一套无 Redis 拓扑，用于定位阶段成本。PostgreSQL 活跃会话采样中，`commit_leased_batch` 有 1,030 个运行样本和 395 个事务锁等待样本，持有写前检查有 195 个运行样本。

## 开启入口防护

单、四副本按 A/B/B/A 顺序使用同一 RPS 压测驱动。IP 配额使用 `enforce`，开启节点请求体总预算。模拟客户端共用本机 IP，因此限流配置给测试流量留出余量。Linux cgroup v2 压力保护使用独立容器验证。

| 1,536 并发、每轮 12 秒 | 单副本 RPS / P99 ms | 四副本 RPS / P99 ms |
| ---------------------- | ------------------: | ------------------: |
| A                      |   16,960.0 / 127.20 |    25,888.9 / 97.21 |
| B                      |   16,511.0 / 128.63 |   23,485.0 / 214.98 |

四轮均零请求错误、零重复结算。四副本两轮平均提升 47.5%，其中 B 轮 P99 升高 67.1%。四副本两轮均发生请求式 PostgreSQL 检查点，单副本两轮均未发生。活跃会话采样与检查点计数用于定位差异，现有数据不足以把尾延迟波动归因到一个因素。

在独立 PostgreSQL 每轮起跑前执行 `CHECKPOINT`，将并发提高到 2,304，并将计量窗口延长到 15 秒。其余条件相同。

| 2,304 并发、每轮 15 秒 | 单副本 RPS / P99 ms | 四副本 RPS / P99 ms |
| ---------------------- | ------------------: | ------------------: |
| A                      |   16,891.8 / 182.36 |   28,411.9 / 118.44 |
| B                      |   16,277.0 / 188.50 |   28,578.3 / 115.06 |
| 两轮平均               |   16,584.4 / 185.43 |   28,495.1 / 116.75 |

四副本吞吐提升 **71.8%**，两轮 P99 的平均值降低 **37.0%**。包括预热在内，四轮共成功结算 1,358,789 笔，零请求错误、重复结算、未明预扣和活跃租约，守恒审计全部通过。单副本负载结束时网关 RSS 为 444.1 MiB（A 轮），四副本合计为 946.5–1,068.6 MiB。50% 目标在该并发档位和副本拓扑下通过。8088 上的现有本地实例仍为单副本。

## 固定到达率混合负载

四副本各跑 35 秒。流量包括短 JSON、短 SSE、Wasm 和 Webhook，开启入口防护及 Outbox 消费确认。P99 从计划发送时刻计起，包含发生器排队。

| 目标 QPS | 成功 RPS | 成功请求 | 未发送 | P99 ms | Outbox 确认 / 剩余 |
| -------: | -------: | -------: | -----: | -----: | -----------------: |
|    3,000 | 2,997.94 |  105,000 |      0 |     38 |        105,200 / 0 |
|    6,000 | 5,992.74 |  210,000 |      0 |    139 |        210,200 / 0 |

两轮均零请求错误，逐笔用量、客户金额、供应商金额和插件审计核对通过。Outbox 数包括各轮 200 笔预热。6,000 QPS 有 3 笔在计量窗口结束后发出，实际 RPS 包含排空时间。

## 4,000 路长 SSE

在双副本开启入口防护，一次冷建 4,000 条连接，每路使用 120 秒的模拟上游时长。4,000 路全部完成，同时在线 114.01 秒，共返回 160,000 个内容帧。连接重置、断流、上游取消均为零。客户端首内容 P99 为 5,026.55 ms，双副本合计峰值 RSS 为 578.3 MiB。

逐笔校对 4,000 笔唯一结算，金额偏差为零，客户合计 740,000 原子单位，供应商合计 148,000 原子单位。结束时 Gateway、模拟上游和 Redis 活跃许可均归零。

## 异常和恢复

- 162 项单测、113 项真实 PG／Redis／HTTP 集成测试通过。实际 HTTP `gzip` 请求返回 415 和 `Accept-Encoding: identity`。集成测试覆盖 Redis 暂停与恢复、分块和 multipart 大小边界、慢速读取取消、级联故障、SSE 中断和结算守恒。
- 离线 Linux 容器限制为 0.5 CPU 和 192 MiB。分别触发 CPU 和内存压力，两项均观察到公网新请求返回 503。Caddy 路由验收 28 项通过。
- 构建产物验收、PostgreSQL 强杀和逻辑备份恢复通过。Sentinel 演练在 2.63 秒后恢复共享配额与入口 IP 配额，副本保留已复制的计数，丢失 1 笔人为安排为未复制的确认更新。PostgreSQL 同步复制演练丢失 0 笔已确认交易，PITR 通过。
- 8088 上的单次真实 MiniMax 推理返回 `503 upstream_unreachable`，请求未发到供应商。本机解析 `api.minimax.cn` 时同时返回公网 IPv4 和保留 IPv6 `2001:2::45`。对实际渠道主机使用 Gateway 自身的连接准入复查，得到 `unsafe_upstream`。本轮保留地址校验规则。

## 复跑入口

使用独立 PostgreSQL，将测试连接填入 `lab.env`。准备同一构建产物后，按以下参数依次用 `BENCH_GATEWAY_NODES=1`、`4`、`4`、`1` 运行。每轮前在实验 PostgreSQL 执行 `CHECKPOINT`。这里的额度用于单源压测，可按实验流量在环境变量中设置。

```bash
export BENCH_DIST_DIR=dist BENCH_DB_MAX_CONNECTIONS=32
export BENCH_MOCK_NODES=2 BENCH_PROJECTS=64 BENCH_TENANT_AFFINITY=1
export BENCH_JSON_CONCURRENCY=2304 BENCH_LIMIT_SECONDS=15
export BENCH_INGRESS_IP_MODE=enforce BENCH_INGRESS_IP_WINDOW_MS=1000
export BENCH_INGRESS_IP_MAX_REQUESTS=100000 BENCH_INGRESS_IP_NODE_MAX_REQUESTS=100000
export BENCH_INGRESS_IP_MAX_PENDING=4096 BENCH_INGRESS_IP_TIMEOUT_MS=250
export BENCH_MAX_BODY_BYTES=262144 BENCH_INGRESS_BODY_BUFFER_BYTES=16777216
BENCH_GATEWAY_NODES=1 bun --env-file=lab.env scripts/bench-prod.ts --limit
```

固定到达率使用 `--mixed-soak`，设置 `BENCH_GATEWAY_NODES=4`、`BENCH_SOAK_SECONDS=35`、`BENCH_SOAK_CONCURRENCY=2048`、`BENCH_VERIFY_OUTBOX_ACK=1`，分别用 `BENCH_SOAK_RPS=3000` 和 `6000`。冷建连 SSE 使用 `--sse-burst-fresh`，设置 `BENCH_GATEWAY_NODES=2`、`BENCH_MOCK_NODES=1`。
