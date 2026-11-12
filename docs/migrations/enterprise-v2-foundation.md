# T03 基础层迁移

输入为 enterprise-v2 规格修订 1。当前阶段只交付领域/持久化基础与既有行为迁移，完整三进程、上游准入/资金、插件发布和 UI 由后续垂直任务实现。

| 旧位置/行为                            | 新拥有者/验证                                                                                                                  |
| -------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| kernel contracts/billing schema 与 DTO | contracts；领域值、金额、纯预扣规则仍在 kernel，依赖单向                                                                       |
| kernel JSON/SSE/usage 与协议字符数     | connectors；原协议单测按原断言迁移；protocol 重写留 T09                                                                        |
| kernel crypto/Batcher                  | runtime/crypto 与 runtime/batcher；storage 仅可依赖这两个真实复用子路径                                                        |
| HTTP errorResponse                     | Gateway http-errors；contracts 只持有错误分类与 DTO                                                                            |
| Store.quote 的协议请求正文             | Gateway/connectors 先测量字符/图片，storage 只接收数字 manifest；kernel 不解析协议                                             |
| providers 表                           | channels，公开 provider_id 和 API 路径不变；仅创建空账户，不登记充值                                                           |
| 三份 SQL 与运行时正则刷新函数          | Drizzle typed schema + 审阅的生成 SQL + 原子 custom SQL；唯一版本 manifest                                                     |
| 历史 schema06–1102 升级 fixtures       | 按授权删除；lifecycle 转为当前基线重放，并保留 receipt/ACK/open hold、KEY SHARE 并发、微秒、月分区/fallback、不可变与 checksum |

Drizzle ORM 0.45.4、Kit 0.31.11、Bun 1.4.4 通过官方 [Bun.SQL 驱动文档](https://orm.drizzle.team/docs/get-started/bun-sql-new) 和 [迁移文档](https://orm.drizzle.team/docs/migrations) 核对并真实 PG 验证。Drizzle 与显式 SQL 共享 Bun.SQL 池；NUMERIC 为字符串，序号 BigInt；不使用 schema push。

锁定版本的原生 JSONB mapper 存在写入双编码和字符串二次 parse；普通列改用单一 `customType` 的 Bun native 参数映射与 identity 读取，SQL schema 和已接纳迁移不变。显式 JSON null、prepared where encoder、Kit scalar 默认值及精度边界见 [JSONB 映射](enterprise-v2-jsonb.md)。

普通表/视图/索引/外键只在 TS schema 声明，迁移 SQL 是版本产物。分区、延迟约束、触发器、资金函数及 ACL 属于 custom SQL 的明确扩展能力；不在 custom SQL 再声明另一份普通表。Drizzle 的反向生成结果对复合外键列顺序、NULLS NOT DISTINCT、索引 operator class 与 bigint identity 上限存在失真，本基线已按真实 PG 定义纠正并以新库实跑核对；不直接执行 pull 结果。BigInt 默认值用 SQL 字面量，避免 Kit JSON serialization 的 BigInt 限制。

版本 manifest 按已知前缀核对版本、文件路径、每文件 checksum 与已应用 manifest checksum；未知/旧/非空库拒绝。CLI 要求显式 --database 与连接 URL 名称相同，运行时再比对 current_database。首次和后续版本在同一事务中应用，中断后可重试；内置 SQL 资产随工具产物复制并在空 cwd 验收。

新增表为真实 typed schema/约束，不提供成功存根。新增资金表对旧角色没有资金写权限；旧 Gateway 配置角色仅新增创建品牌/空账户所需权限。plugin_packages、financial_vouchers、administrator_credentials 对旧 runtime/worker 角色明确撤权。Core API/worker 的细化授权及 Gateway 无 DB 在 T04/T14 验证。

手算独立预期：1/3+1/3 向上取整为 1 atom，125ms×8 atoms/s 为 1 atom，9007199254740993 精确往返。kernel 与 SQL 分别验证这些结果，music_ms 不复用音频识别维度。resource schema 与 DB 只接收稳定 opaque ID 和既有严格数字字段，不接收 signed 下载地址、正文或任意 metadata。

验收命令与实际用例数由 T03 报告记录；本文件不提前声称完整企业迁移完成。旧容量、Docker 与跨域 HA 证据保留为历史，后续任务按最新原生/本机最大稳定负载要求验收。

原冻结候选 T03 验证：check 117 文件无错误，lint 零警告，fmt:check 179 文件；单测 109 通过，完整 PG/Redis/HTTP 集成 284 通过，构建通过，独立 artifact 8 项通过。Drizzle API 单测与 CLI generate 均证实声明/快照无漂移；新旧实库归一渠道表名后，201 条旧 PK/unique/FK/check 无缺失或语义变化，关键索引相同。此处只记录 T03，不代表后续 T04–T18 已完成。

原资源保存函数允许 URL 作为 opaque ID，隔离复现用原保存函数与原有无 URL guard 的资源约束证明本应拒绝的请求成功持久化；现在 DTO、Store 和 DB 同时拒绝，并保留稳定 file ID。完整集成另暴露旧用例把 Gateway drain 当 PG 结算屏障：实际看到 DISPATCHING；更正为等待 async_final 和 execution 两条事实后，原 SUCCEEDED 和 15 tokens 断言保留。

cost_versions.ceilings 默认空对象，attempts/resources.binding_id 默认 null，只为当前旧行为保留可迁移的基础形状。T04 新执行必须要求真实 binding 和每个收费维度的 ceiling，不能把缺值当零预算；这两个前置条件的执行逻辑属于 T04。

冻结前的独立核对还发现仅 public enum/domain 或额外 pg_catalog 扩展可能被误判为空。真实 PG 红用例证明原迁移错误成功；增加 public pg_type 与非默认 extension 检查后，枚举、domain、额外扩展均拒绝且保持原对象，默认 plpgsql 与全新库仍成功。最终集成 284 项通过。临时自建数据库已清理；没有修改既有数据库或 Redis 配置。

首次独立审查修复覆盖四项基础迁移遗漏：benchmark 的 ANALYZE 消费方改用 channels；协议 duration 与原始大整数归一化在 connectors 边界把 DomainError 转回既有 502/code，kernel 保持纯领域错误；RECHARGE 要求非空正 payment_atoms，ADJUSTMENT 仍允许缺省支付事实；batch_manifest 的必填字段、字段类型和可选 images 必须得到明确真值，SQL NULL 表示无 manifest，JSON null 不是有效 manifest。公开 extractUsage、实际 Gateway fetch、真实 PG 写入及一秒低并发 mock benchmark CLI 验证这些行为，artifact 同时检查结算、ACK、自有库和 Redis namespace 清理。修复前失败与修复后结果记入 T03-fix1 报告。

该候选尚未接纳，也没有人类数据；本次仍纠正版本 2026111202 的 TS 声明、生成 SQL、snapshot 与 manifest checksum，并保留现有分区扩展。接纳后不得再篡改该基线，后续变化必须增加迁移版本。

首次修复完整门禁：check 118 文件、lint 零警告、fmt:check 180 文件；115 单测/1666 断言/21 文件、286 集成/7072 断言/6 文件全部通过，build 及 artifact 9 项通过。Drizzle CLI 无漂移；真实 PG 复核旧 201 条约束与关键索引保持等价。独立复查和主工作区集成由协调者继续执行，本次作者自查不替代独立复查。

## 已接纳基线后的资金追加

2026111202 四 SQL 与 0000 snapshot 已冻结；T04.b.funding 以 2026111203/0001 追加凭证内容摘要一致性、资金凭证组合 FK 和引用索引，不改写历史迁移。当前普通 schema 漂移检查使用迁移 journal 的最新 snapshot；真实 v2→v3 fixture 保留 settled receipt、ACK、open hold、UPLOADED package 和充值账本，再验证重放幂等。详见 [上游资金切片](enterprise-v2-upstream-funding.md)。
