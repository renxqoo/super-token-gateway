# Bun SQL JSONB 映射

锁定 Bun 1.4.4、Drizzle ORM 0.45.4 与 Kit 0.31.11。普通 JSONB 列统一通过 `schema/jsonb.ts` 的公开 `customType` 声明为 PostgreSQL `jsonb`。写入时非 null 值交给 `Bun.sql.json(value)`，读取保留 Bun 已解码的值；不再先 stringify 或二次 parse。资金原子 SQL、连接池和迁移 owner 保持原职责。

原生 Drizzle 映射会先 stringify，再由 Bun 按 JSONB OID 编码，导致对象成为 JSON string，真实插件表的 `plugin_manifest` check 拒绝写入。原读取映射对 `"42"`、`"false"`、`"null"`、`"{}"` 等再次 parse，改变已持久化字符串的类型。真实 PG 回归通过生产 `createRepositories(db)` 和真实列验证这两个症状，保留包字节、签名字段、check 和不可变触发器，不伪造发布状态。

`null` 写入 SQL NULL；显式 `jsonNull` SQL 表达式写入 JSON null。两者读取都表现为 JS null，需要数据库 `IS NULL` / `jsonb_typeof` 区分。JSON-looking 字符串与金额/atoms 字符串保持原值，38 位 atoms 也必须以字符串表示。PostgreSQL 裸 JSON 数值超出 JS 安全整数时，Bun 可能在解码时丢失精度；本映射不能恢复已丢失的信息。

prepared insert/update 的占位符由列 encoder 处理。prepared 等值条件必须显式写作 `eq(column, sql.param(placeholder("value"), column))`；直接 `eq(column, placeholder("value"))` 绕过 encoder，不在已修正形式内。禁止用返回 SQL cast AST 的 mapper 代替原生参数，它在 prepared 执行时会把 AST 当数据持久化。

当前两个 `.default({})` 原样保留，SQL 类型、check、索引、FK、历史 SQL、checksum、manifest 和 snapshot 均不变化。Kit 原生与本 customType 对 `.default("42")` 都可能生成 JSON number；新增 JSON string 默认值应显式使用 `sql\`'"42"'::jsonb\``。`.default(null)`表示 JSON null，SQL NULL 默认值应显式用`sql\`NULL\``。本次不扩展为 Kit 默认值系统修复。

helper 仅在写入参数编码时访问全局 `Bun.sql.json`，schema 加载没有运行时 `bun` import，不新建池。业务查询只通过显式拥有的 `createDatabase` / `createRepositories` 连接池执行，池由原 owner 关闭；默认 singleton 只作纯参数编码。`bun run db:generate` 仍生成待审阅文件，不能执行 schema push。生成验证使用迁移输出的私有副本，比较最新 v3 snapshot，禁止为了映射修正新增无意义 DDL。

验证命令、实际断言数、CLI 输出、资源清理和逐文件摘要保存在 `.local/T03-jsonb-fix2-report.md` / `.json`，作者验证不替代独立审查。

## T04.b.1 生命周期约束下的 mapper fixture

2026111204 为 plugin_versions 添加受控变更 guard。通用 JSONB mapper 的 CRUD/prepared/null 三组私有 admin fixture 现在在独立事务内使用 `set_config('gateway.catalog_mutation','on',true)`；rollback 用例也只在自己的事务设置标记，原编码、类型、SQL NULL/JSON null、upsert 和回滚断言全部保留。它们不发布版本，不构造 PUBLISHED 事实。GUC 不是权限边界：catalog/runtime/worker 的直接表写 ACL 与 owner 成员权限单独拒绝测试，identity immutable 校验不会因标记而放行。生产只由受控 definer 命令设置事务局部标记；详见 `enterprise-v2-plugin-catalog.md`。
