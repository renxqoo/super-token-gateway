# T04.b.1 受控插件发行目录

本切片交付真实 catalog 前置链。Core application 位于 `apps/core-api/src/plugins`，平台凭证验证位于相邻 `platform`；Gateway 暂时装配同一个 HTTP application，T04.c 迁走装配时不复制发布或绑定实现。这里没有实现 T04.b.2 的资金准入、三进程执行、HA、任意包上传或真实供应商验收。旧 Gateway connector 路径仍是迁移中的旧执行口，不能用其测试冒称插件执行新链。

## 发行与信任

唯一发行标识是 `gateway.openai-chat-1.0.0`，内容必须等于已接纳的 `packages/adapters/openai-chat/package`。安装 API 不接服务器文件路径、URL、构建脚本或替代 WASM。Core 的部署配置是 `PLUGIN_RELEASE_POLICY`、`PLUGIN_RELEASE_DIRECTORY`、`PLUGIN_HELPER_EXECUTABLE`，三项必须同时设置；缺少配置时不装配 catalog API，没有 unsigned/dev fallback。配置目录是部署方控制的发行物目录，安装者只能从其中选择枚举 release_id。

发行负责人在 Core 进程之外运行受控签名工具，显式提供权限为 0600 的 Ed25519 私钥文件。Core 只消费公钥和签名，不生成或持有发行私钥：

```sh
bun run plugins:sign-bundled --private-key-file /secure/release-ed25519.pem --key-id approved-release-key --output-directory /release-artifacts --helper-executable /release/native/plugin-host
```

该工具只读取受控 builtin，输出 `<output-directory>/gateway.openai-chat-1.0.0/package.json` 与 `release.json`，已有文件不会覆盖。package signature 签署 SDK 内容 digest；另一独立签名域 `gateway-plugin-release-v1\0` 签署规范 JSON release descriptor。descriptor 包含固定 source/name/version、内容与 wire envelope digest、每文件长度与 SHA256、ABI、schema、fixture、验证策略和 helper 二进制 digest。签名不包含自身，包内公钥不能成为 trust anchor。

工具输出的 descriptor_digest 及 descriptor 字段必须由部署发行管理方审查后写入独立 policy。policy 是严格 JSON：顶层 `format:1`、`expires_at`、`revoked`、`keys`、`releases`；每个 key 包含 `id/public_key/not_before/expires_at/revoked/purposes/release_ids`，purpose 只能为 package/release；每个 release 包含 `release_id/descriptor_digest/package_digest/abi_version/config_schema_digest/fixture_digest/validation_policy_digest/runtime_digest/revoked`。过期、未来生效、撤销、未知 key、目的或 release 范围不符、任何摘要/ABI/schema/source 不一致均拒绝。Core application 启动时读取并固定部署方控制的 policy snapshot；每次注册、验证、发布、绑定及可执行 binding 查询都按该 snapshot 重新核对当前时间与撤销状态。没有 policy 修改或热撤销 API，改写磁盘文件不会改变在运行实例的 snapshot。撤销更新的生效边界是运维先撤出并 drain 所有旧 snapshot 的 Core（等待在途命令完成），再用新 snapshot 启动实例；不能把滚动更新期间旧实例仍持有的 snapshot 声称为已即时跨 Core 撤销。测试通过关闭并重新装配 Core application 验证新撤销配置。历史 PUBLISHED 事实不因撤销而改写，新 snapshot 下 binding 查询会返回 `executable:false, trust_state:UNTRUSTED`。

生产 policy 与私钥归部署发行系统所有，不在仓库提供默认值。集成测试创建并销毁的临时合成密钥只证明程序验证能力，不代表生产发行授权。

## 平台身份与公共入口

保留已确认的 `ADMIN_KEY` 平台管理兼容，审计 actor 固定为 `bootstrap:ADMIN_KEY`。其他 Bearer credential 用现有 pepper verifier 查询 `administrator_credentials` 与 `technical_administrators`，每次请求核对启用、过期和权限。actor 来自已验证上下文，HTTP body 不能指定 actor。项目 API key 即使角色为 ADMIN 也不是平台身份。未来 Gateway mTLS 身份是独立通道，不复用这里的凭证。

权限分别是 `plugins:read`、`plugins:install-bundled`、`plugins:validate`、`plugins:publish`、`plugins:bind`、`plugins:recover`。安装要求 install-bundled/validate/publish 三项。公开路由：

- `POST /admin/plugins/install-bundled-release`：command_id、枚举 release_id、reason；真实注册、验证、发布。
- `POST /admin/plugins/{id}/validate`、`POST /admin/plugins/{id}/publish`：稳定 command_id、expected_revision、reason。
- `POST /admin/plugins/{id}/recover`：command_id、run_id、lease_epoch、reason；有界恢复过期任务，绝不把未知结果变成成功。
- `GET /admin/plugins/{id}`：持久状态及发布回执。
- `PUT /admin/providers/{id}/plugin-binding`：command_id、version_id、expected_revision、真实 account_id/brand_id、`channel:<provider id>` secret_reference、严格 config、reason。
- `GET /admin/providers/{id}/plugin-binding`：当前不可变 binding 与可执行/信任状态。无 binding 不可执行。

安装器调用同一公开 API，没有本地 SQL 或 builtin 直发捷径：

```sh
bun run plugins:install-bundled --admin-url https://core.example/admin --credential-file /secure/operator-token --command-id '<stable uuid>' --release-id gateway.openai-chat-1.0.0 --reason 'Approved deployment'
```

可选 `--provider-id` 与 `--binding-file` 必须成对给出。binding 文件提供上述绑定字段，version_id 由真实 publication 回执填充；绑定失败后发布事实保留，同一 command/body 可以独立重试。所有 JSON 凭据文件与发行私钥都由 operator 显式提供，不能把生产密钥写入源码、参数日志或 fixture。

## 持久生命周期和权限

2026111204 追加真实 Drizzle `0002_plugin_catalog` 与受控 SQL；既有 v2/v3 SQL、checksum 和 snapshot 不变。package_bytes 存完整 SDK wire envelope，因此数据库上限从 decoded 32MiB 修正为 wire 44MiB；SDK 仍独立执行 decoded 总量、单文件与 fixtures 上限。历史 rows 原样保留，未有 signed descriptor 的历史包不能通过新 application 发布。

流程是 immutable UPLOADED → 持久 leased VALIDATING → public `runPluginFixtures` 原生 helper → close/wait/reap 确认 → VALIDATED → 同事务 publication receipt/audit/PUBLISHED revision。实际 builtin fixture 期望是 15 cases、24 steps，涵盖精确数字、缺 usage、错误、SSE 截断和取消等。结果只存有界 cases/steps、cleanup_confirmed 和错误类别，不存请求正文或 helper stdout。

每次显式验证命令至多一个 run；run 固定 package/fixture/runtime/policy digest、requesting actor、owner/epoch/expiry。active slot 不被新 run 替换。失败只能 REJECTED；新命令可显式重验。无法确认清理时为 CLEANUP_PENDING，禁止发布和重叠验证。资源登记只含本 run helper pid/connection；恢复器不杀任意 PID，只在已登记进程组确认不存在时失败收尾，递增 epoch 后旧 epoch 不能提交。资源登记缺失或进程组仍存在时保持待清理，需要运维处理，不无限重试、不自动成功。正常关闭合并为一个 Promise；application shutdown 等待从启动阶段起的验证结束，数据库最后关闭。

发布事务锁 version 与最新成功 run，要求成功结果已确认清理、固定 digest 不变和当前部署 snapshot 的信任校验。服务端验签计算 policy 与 package/release key 的最早 valid-until，作为独立 SQL 参数传入；语义 body_digest 不含该授权时限。授权时间线性化点在锁后、最终状态变更前的同一事务内用 clock_timestamp 核对，锁等待跨过有效期即回滚。已提交的同体命令重放仍取原 receipt，不因重新计算时限产生 digest 冲突。receipt/audit 任一步失败整笔回滚。绑定锁 version/channel/account，校验账户属于品牌、secret reference 属于该 channel、当前 PUBLISHED receipt、严格 schema 和 expected config_revision。历史 binding 不更新，receipt/audit 记录旧/新 binding 和 config digest。相同 kind/verified actor/command_id 同体返回原 receipt；换体 409，stale expected revision 409，重放不多审计或 revision。

`infra/core-catalog-role.sql` 只赋予目录读取与三个窄函数 EXECUTE（凭证验证、命令事务与安全回执查询）。该 role 不能直接 INSERT/UPDATE/DELETE 生命周期表，也不能假借 GUC 获得权限。`gateway.catalog_mutation` 只是受控 definer 事务内部状态标记，授权依赖表 ACL 与函数授予；identity 字段即使该标记为 on 仍 immutable。现有 runtime/worker 不获 catalog EXECUTE 或生命周期表写入，runtime 仅保留已使用 channel enabled/concurrency/rpm/secret 列更新。部署应将 role 授予 Core DB 登录角色，不能授予 owner/superuser 成员关系；过渡装配与旧 gateway role 的组合由部署负责。

## 供应价格与 ceiling 契约

保留 `/admin/providers` 与现有 pricing 字段。provider 创建返回真实 id/brand_id/account_id、零 config_revision、null binding 和未充值状态，不伪造余额。`POST /admin/models/{model}/routes` 的同层 ceilings 现在必填；pricing/ceilings 使用 upstreamCostConfigurationSchema，至少一项 rate，每个 rated dimension 即使 rate 为零也必须显式 ceiling。`PATCH /admin/routes/{id}` 只要替换 pricing 或 ceilings，必须同时提供完整二者。新配置 INSERT 新 immutable cost_version，历史不 UPDATE；routeDefinitions 与 routes 查询都返回捕获 ceilings。旧调用方、seed、基准和验收输入已同步。这是有意的外部契约收紧，缺 ceilings 的旧客户端应升级后使用。

JSONB 新列使用已接纳 native mapper。原 JSONB CRUD/prepared/null/rollback 回归仍保留全部断言；其私有 admin fixture 在事务局部启用 mutation 标记，以测试驱动编码而不冒充生命周期。真实低权限角色另行验证：即使自行设置同名 GUC，也不能更新版本、run、binding/channel binding 列，不能假冒 owner 或执行未授予函数。

本轮部署 snapshot 固定的是受控 builtin 发行信任根与 allowlist，插件正常注册、验证、发布、绑定仍在线走数据库生命周期。T08 的受信 publisher 范围内动态上传不能以每新增 digest 就重启 Core 作为唯一方案；可扩展来源与 publisher policy 规则，复用现有通用存储命令、公开 SDK 验签、validator/runPluginFixtures、publish/bind 事务，本轮不扩上传来源。

## 技术凭证与 lease 的事务边界

Core 认证器私有保存本次实际验证的 credential ID、administrator ID 和 verifier 匹配证明；HTTP body 不能提供或替换该上下文。受控 SQL 命令通过独立参数接收窄身份投影，稳定 caller identity 和 semantic body digest 不包含凭证证明。凭证原文和 verifier 不进入 HTTP 响应、receipt、audit 或日志。SQL EXECUTE 仍只授予受信 Core role，不是匿名 HTTP 接口；bootstrap 仅为服务端显式配置的 ADMIN_KEY 身份。

新 register、validate claim、publish、bind、recover claim 在同一外层事务中先以 credential、administrator 的固定顺序持有 FOR SHARE 锁，复核 exact credential 归属、verifier、enabled、对应 action 的 permission 和当前有效期，直至提交。撤销先提交则拒绝新变更；命令先取得授权共享锁则撤销 UPDATE 等待，命令可以先提交。自然时间到期不依赖行更新，任何业务锁等待后及最终变更/回执提交前使用 PostgreSQL clock_timestamp 再检查；拒绝时整个事务回滚，无新 revision、receipt 或 audit。轮换 verifier 后旧认证快照不能授权新命令。

权限数组只有明确包含所需 permission 才授权。空数组、仅含 SQL NULL、或无所需 permission 且含 NULL 的数组均拒绝；`[所需 permission, NULL]` 仍可授权。SQL 的 `ANY` 可能返回 NULL，拒绝条件使用 `IS NOT TRUE`，不能让三值逻辑中的未知值绕过事务内权限复核。真实 PostgreSQL 锁等待回归覆盖 HTTP 认证读到旧权限、撤销先提交后拒绝绑定，并确认 binding/revision、command、receipt 和 audit 均未改变。

已提交安全 receipt 的查询和精确重放保留原语义，包括 publisher policy 过期或新 snapshot 撤销；正常 HTTP 请求仍先完成当前个人凭证认证。已拥有 lease 的 resources/finish 是内部收尾操作，只凭严格 owner/epoch/expiry fencing 收敛既有任务，不重新要求可能已撤销的发起人，也不能用于获得新的发布或绑定权限。

SQL 对每个 action 的必需字段独立校验，不能只依赖 TypeScript：validate claim 需要 owner 和规范十进制 expected_revision，但不需要旧 epoch；finish/resources/recover 需要非空 UUID owner 与 JSON string 形式的规范正整数 lease_epoch（最多 38 位），拒绝缺失、JSON null、空值、其他类型、非规范或越界表示。revision 使用规范非负整数 string。fencing 使用 NULL 安全比较；finish result 的 cases/steps 为整数 JSON number、cleanup_confirmed 为 JSON boolean，资源登记需要合法 PID 和非空 connection。输入或 fencing 拒绝均不留下部分状态、资源或 command receipt。
