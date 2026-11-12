# 插件运行时探针裁决：2026-11-12

T02已交付并由主协调者核验。源基线为`ce5b4145a18959efcb7cc7cb927db2295cd8dd8f`，探针环境为Darwin arm64、10逻辑CPU、16GiB内存。本文保留技术裁决和本地证据摘要；生产契约、执行顺序与验收状态以[Enterprise v2迁移规格](../migrations/enterprise-v2.md)第6、9、10节为准。本次文档修订没有重跑探针或产品检查。

## 已选方案及否决证据

**采用Bun 1.4.4三主服务，加Gateway所属原生Go 1.27.3 / Extism Go SDK 1.7.1 / wazero 1.9.0受限执行器。** 该GO是进入生产实现的实证依据，不代表产品集成、安全、耐久或最大稳定负载验收通过。

- Bun的`@extism/extism 2.0.0-rc13`不能加载真实TS-PDK所需WASI；无限WASM循环在200ms timeout后仍占CPU，close也不能中止。其`maxPages`只约束host block，不能限制guest linear memory，因此原样NO-GO。
- Node 26.9.0同SDK可运行真实PDK并由deadline中止循环，但close后原调用不settle、进程退出挂起；受监督复现已强杀并reap，原样NO-GO。较快批处理不能抵销清理失败。
- Go受限guest可中止无限循环，100ms deadline实测约102ms返回；active close取消原调用并等待返回，之后新session继续工作。有限8MiB guest通过真实流、OOM、取消及退出，结束sessions0、goroutines1、pending0、exit0。默认任意网络请求被拒绝，本地合成HTTP server命中0；未配置文件preopen、环境或凭证能力。

复用仅限helper、编译物和有界执行槽。真实TS-PDK的`sticky`负向向量在复用guest时让attempt B读到attempt A的合成正文；fresh实例读取不到。没有完整reset/snapshot证明，因此每批只处理一个attempt，创建并销毁新guest，延续状态由宿主持有并显式传入，不能复用未reset的guest。生产还须验证跨租户正文金丝雀和trap/cancel后无残留。

## TS构建与流边界

实跑链路为TypeScript→esbuild 0.28.2（ES2020 CommonJS，打包lossless-json 4.3.3）→官方extism-js release v1.7.0→WASM。该release内二进制自报1.6.1，差异已保留；Binaryen为version_133。完整来源与下载摘要在原始报告，不复制探针依赖进入产品。

真实受限模块为2,496,056字节，初始38页、最大128页/8MiB。六帧561字节SSE分118次push，涵盖中文/emoji的UTF-8字节切分、增量工具参数、usage终态；第76字节已产出首个事件。独立向量匹配文本`你🙂`、工具参数中的`9007199254740993`、input_tokens字符串`9007199254740993`、output_tokens `17`及小数原词法`0.10000000000000001`；无终态的end被拒绝。不得经过普通JSON number中转。

这些是合成生命周期及协议边界探针，尚未验证正式Bedrock binary转换、受控认证、所有adapter或端到端背压。探针memory重写器也不是生产验证器；生产必须检查所有memory/imported memory、finite maximum、table、模块和imports预算，并限制验证/编译的CPU、墙钟、RSS及临时资源。

## 局部成本及性能边界

同缓存模块1→16真实实例的HeapAlloc约8.38→67.32MB，每个额外实例约3.9MB。10000个空闲显式状态序列化约1.08MB、Bun堆增约1.93MB，均不含活跃正文、工具累积和IPC副本，不能推导10000个物理实例可用。64MiB OOM压力后的Go HeapSys高水位约339MiB，guest上限不等于进程RSS；原生supervisor/服务级限制和高水位重启仍须实现并验收。

推荐的fresh-instance-per-batch单次短探针，每lane 2048个预先就绪的合成帧、batch32，包含Bun、JSONL/base64 stdio IPC、真实QuickJS-WASM、instantiate/call/close及输出解析：

| 执行槽 |  帧/秒 | batch p95 ms | 结束RSS MiB |
| ------ | -----: | -----------: | ----------: |
| 1      |  2,518 |        8.577 |        92.8 |
| 2      |  7,488 |        9.474 |        92.9 |
| 4      | 14,097 |        9.532 |        97.4 |
| 8      | 20,186 |       17.651 |       115.0 |

首组含约305ms冷编译/实例化，输入预先就绪，不含凑批等待。单位为帧/秒而非请求RPS；单轮局部结果不证明稳态、长测、无积压或网关容量。真实流必须有字节上限和短flush期限，不能等凑满32帧；全部本机门禁完成后由T17按强耐久、完整完成、无稳态积压测本机最大稳定负载，不使用固定6000RPS门槛。

## 证据与摘要

全部原始证据位于`/Users/wrr/work/super-token-gateway-plugin-probe-20261112/.local/plugin-probe/`。`report.md`是人读报告，`report.json`含实际版本、构建物、命令、结构化结果和检查状态，`raw-index.json`索引32份原始文件，`raw/*.jsonl`保留逐项结果与原始延迟。工作树移除前须按迁移规格第9.2节登记并保留证据；本摘要不能替代已丢失原始记录。

| 相对证据路径                | SHA-256                                                            |
| --------------------------- | ------------------------------------------------------------------ |
| `report.md`                 | `c2bf313d335a9e0c075229238356016e8ff1a9350ed51bbe5e1597270299ca18` |
| `report.json`               | `62c13dce3edf1cbea730556a979cab41e438fbdeb7a2404673627e5e94a8a51c` |
| `raw-index.json`            | `a18a4a1088a44320271bd55603b03676efc79170c2c873c9cd54d0fba99bb978` |
| `raw/fresh-isolation.jsonl` | `9e459df957b1b7cf077697abbdf0ccc0f90ff07c0eef14ca25a386d05f1e4c6d` |
| `raw/functional-8mib.jsonl` | `330d55559c753d816ae9ed3ca6ae2eee6622ccd0c94da323241078b454659502` |
| `raw/bench-fresh-go.jsonl`  | `4a2a839845523cd72720e8b375bd0fdec77c0de06d5fd0bb7061e550bf3256ce` |
| `raw/final-cleanup.json`    | `4bd0465d96baee1e6e8d0aea7de5f815359a84e6c00dc1f654c4fb51ee3affd9` |
| `plugin-bounded-128.wasm`   | `3e25da549acdb4bd25871e9cd82e44d03c9acae8c1839e0b83068cc16d1cfd50` |
| `sticky.wasm`               | `4eea86b54d5785e4978cac03aef588f4d0428ba67191bb81bf20fa392068c2df` |
| `go-host/go.mod`            | `e0230ff1590bc6ecb3b00a7aff8f32292312c8cebd30f060b3900ae67043a58a` |
| `go-host/go.sum`            | `83c6c7aadbdfe66125ac1e8eb173381ad76fc7febdb112307ad31b04df9800ef` |

关键原始入口：`raw/sdk-timeout-supervised.jsonl`和`raw/sdk-close-supervised.jsonl`是Bun否决证据；`raw/real-runtime-check.jsonl`记录真实PDK兼容与Node清理失败；`raw/go-minimal.jsonl`、`raw/functional-8mib.jsonl`、`raw/fresh-isolation.jsonl`分别提供中止/关闭、真实受限流和隐藏global复现；完整局部性能数据在`raw/bench-fresh-go.jsonl`。

独立脚本带真实断言，Go vet通过；没有Bun test-runner用例数，未运行仓库check/lint/fmt/test/integration/build/artifact/durability/backup。探针没有访问PG/Redis、私有Key、其他任务服务、Docker或付费API。报告记录正常子进程等待退出、负向探针精确kill/wait、合成HTTP fixture关闭及最终无探针残留。没有修改产品、提交、push或publish。正式原生运行器T02.a、Bun监督与产品装配T04、上传验证/认证/包生命周期T08和原生部署限制T14仍待完成。
