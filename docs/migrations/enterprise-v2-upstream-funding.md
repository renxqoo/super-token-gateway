# T04.b.funding：上游凭证与已完成充值

本切片只提供 `@gateway/storage` 的 `createUpstreamFundingStore` 和可组合的 `recordUpstreamRecharge(tx, accountId, input, trustedActor)`。它登记外部已经完成的财务事实，不发起支付、不计算汇率。HTTP、身份认证、权限校验、调整、对账、报表和持有/结算链留给后续 Core/T06。此处没有 HTTP 入口，storage 测试不能证明管理员授权成立。

## 公开契约

`createUpstreamFundingStore(db)` 是函数工厂，返回 `registerVoucher(input, trustedActor)`、`voucher(id)`、`cleanupVouchers(limit=100)`、`recharge(accountId,input,trustedActor)`、`receipt(accountId,externalRef)` 和 `balances(accountId)`。余额逐币种返回 `balance_atoms/reserved_atoms/spent_atoms/adjustment_atoms/revision/available_atoms`，不跨币种汇总。回执按账户和 RECHARGE 外部引用读取，没有面向未授权用户的公开下载链接。

充值 DTO `upstreamRechargeSchema` 是 strict 对象：`external_ref`、`payment_currency/payment_atoms`、`credited_currency/credited_atoms`、`voucher_id/voucher_digest`、`completed_at`、`reason`。金额只接受 1–38 位规范十进制正整数字符串，不接受 JS Number、负数、0、指数或前导零；事务内部使用 BigInt，数据库使用 NUMERIC(38,0)。币种沿既有契约为 3–8 位大写字母，付款和入账分别保存。付款 CNY `700000000000000000000`、入账 USD `100000000000000000000` 只增加 USD，不生成 CNY 资产或 FX 交易。

时间只接受有效 UTC `YYYY-MM-DDTHH:mm:ss[.ffffff]Z`，小数 0–6 位，年份 0001–9999，拒绝偏移时区、闰秒、无效日期和更高精度；规范化为六位小数。Date 只核对完整秒的日历合法性，已接受的小数以原字符串补零，绝不经毫秒舍入。完成时间必须不晚于 PostgreSQL 校验时的时钟。理由去除首尾空白后必须非空，最大 2000 字符；外部引用 1–200 字符。

唯一键是 `(account_id,kind=RECHARGE,external_ref)`。摘要覆盖账户、类型和全部规范化 DTO 字段，包括两个币种/金额、凭证 ID/摘要、完整微秒完成时间和理由。同 payload 返回原回执，金额、revision、actor、journal ID 均保持原值，不追加审计。不同 payload 返回 409 `upstream_recharge_conflict`。actor 是独立可信参数，业务 DTO 内出现 actor 会被 strict 校验拒绝；重试者不同不会改写原 actor。新充值只增加入账币种 balance 与该余额 revision，不改 reserved、spent、adjustment 或账户状态/revision；DISABLED、QUARANTINED、RECONCILE_REQUIRED 均不会自动恢复。

## 凭证边界与保留

`financialVoucherRegistrationSchema` 接受已有的 `Uint8Array content`、`content_type`、小写 SHA-256 `digest` 和 UTC `expires_at`，不接受 URL、HTML MIME、任意额外字段或 base64 字符串。storage 在复制/存储前检查非空、最大 5 MiB 和 png/jpeg/pdf MIME 白名单，对有界副本检查文件签名字节并计算实际 SHA-256；数据库再检查内容与摘要一致。签名字节检查不是完整文件格式验证。Core 必须在上传解码/分配前限制请求字节、验证完整图片/PDF 内容、验证上传/读取/引用权限，并以安全下载响应（attachment、正确 MIME、nosniff）交付，不能直接渲染不可信内容。storage 不抓取 URL。

首次 staging 到期时间必须在数据库当前时钟之后、24 小时以内。相同 digest/类型重复登记返回同一不可变对象，保留原 actor、到期时间和 ID，不续期、不覆盖内容；传入的新到期时间不会改写原对象。已引用对象即使 staging 已到期仍可读取、重复登记并引用。过期未引用对象返回 409 `voucher_expired`，必须先显式清理；清理后相同 bytes 可以产生新的 staging ID，历史资金所引用的凭证始终保留。

关联复用既有 `upstream_funding_operations.voucher_id` FK，不 UPDATE 不可变凭证来补 linked 标志。追加组合 FK 验证 `voucher_id/voucher_digest` 对应同一对象。`cleanupVouchers` 每次 1–1000 对象，只选择已到期且未引用的凭证，按 expiry/ID 排序 `FOR UPDATE SKIP LOCKED`；DELETE 再检查到期与未引用，FK 最终防止删除已引用财务材料。清理不获取账户锁。

## 事务与锁序

Core 可以先在同一 PG 事务锁定并复核可变 operator 权限，再调用 `recordUpstreamRecharge`。该窄入口内部使用 savepoint，失败时余额、operation/receipt、journal/entries、审计和凭证关联一起回滚，即使调用方捕获领域错误也不留下半笔。函数返回只是当前外层事务内的结果，调用方必须等外层 commit 确认后才能对外报告成功或取得后续清理权。外层 rollback、延迟约束或 commit 失败会撤销已返回的局部结果。工厂返回的 `recharge` 是自提交便利入口，Core 需要授权撤销屏障时应使用可组合入口。

单笔顺序：账户 `FOR SHARE` → 已有回执只读 → 缺失余额建立零行/入账币种余额 `FOR NO KEY UPDATE` → 再查回执 → 凭证 `FOR UPDATE` → operation 唯一仲裁 → balance 更新、平衡 journal/entries、actor 审计。同 payload 必有相同 credited currency，由余额行串行；异币种/异凭证相同 key 在唯一键处裁决，单笔事务此后不获取其他余额或凭证，不形成反向环。已有回执在余额锁前返回。批量调用者仍须按全局表/账户/币种顺序预锁，不得把此单笔接口任意交错为反向多对象事务。

凭证登记只按 digest advisory key → voucher 行，不回取账户。登记与充值先取得凭证行锁，再用独立语句读取当前到期/关联状态，避免 SELECT 表达式在锁等待前计算而放过等待期间已过期的对象。充值与清理竞争同一凭证行：充值先锁且有效则提交 FK 后永久保留；清理先锁已过期未引用对象则删除，等待的充值发现对象不存在并整笔回滚。登记不把大 bytea 带入资金热事务，充值只读凭证摘要/到期和关联状态。

journal 只记入账币种：`upstream:available +credited_atoms`、`external:funding -credited_atoms`，关联 operation/account，channel 为 NULL。每笔在返回前强制检查既有延迟平衡约束，再恢复其默认 DEFERRED 模式。外层事务不应携带尚未完成的其他上游 journal。审计包含独立 actor、action=RECHARGE、operation、余额 revision、payload digest 和理由。

## 追加迁移与验证边界

版本 2026111203 使用 Drizzle 声明及 `0001_upstream_funding.sql`/`0001_snapshot.json`，增加内容摘要 CHECK、凭证复合唯一键/资金复合 FK 和引用查询索引。生成 SQL 经审阅调整约束创建顺序，使被引用唯一键先于 FK。2026111202 的四份 SQL、checksums、0000 snapshot 和旧 journal entry 保持不变。迁移仍只允许已知 manifest 前缀升级，拒绝 checksum 不匹配，不运行 schema push。

真实 PG 用例从 Store.createProvider 建立零余额账户/渠道，通过新凭证和 RECHARGE 入口入账，覆盖三币种各 100 单位、跨币种付款事实、大整数/溢出、严格输入、全部幂等字段、同 key 并发/异币种异凭证竞争、审计/约束回滚、外层 rollback/延迟约束、冻结状态和双方向清理竞态。冻结/缓存测试起始 reserved/spent 为 0，并以拒绝变动的私有触发器核对不修改；非零 UNKNOWN hold 的保留由后续实际 hold/settlement 链验收。

升级用例先执行冻结 v2 四 SQL，实际建立客户 settled receipt/ACK/open hold、UPLOADED 合成插件包/版本及新公开充值入口产生的上游凭证/账本，再执行 v3，逐表保留事实和旧 manifest/checksum，并验证客户/上游重放无追加。合成财务材料不是供应商实际支付，UPLOADED 包也不是插件发布/执行证明。v3 普通 schema 与最新 Drizzle snapshot 零漂移。具体本轮检查结果与资源回收在 `.local/T04b-funding-report.md/json`，不将后续 Core auth、双资金 attempt、三进程、HA、native durability 或真实供应商链算作已验收。
