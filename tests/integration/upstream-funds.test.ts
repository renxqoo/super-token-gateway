import { expect, test } from "bun:test";
import { createHash } from "node:crypto";
import {
  createRepositories,
  migrate,
  recordUpstreamRecharge,
  Store,
  createUpstreamFundingStore,
} from "@gateway/storage";
import { channels } from "@gateway/storage/schema";
import type { UpstreamRechargeInput } from "@gateway/contracts";
import type { SQL } from "bun";
import { withDatabase } from "../support/database.ts";

const registry = ".local/T04b-funding-resources.jsonl";
const hundred = "100000000000000000000";
const completed = "2026-01-01T12:34:56.123456Z";
const actor = "technical-admin:synthetic-finance";
function isolated(run: (db: SQL) => Promise<void>) {
  return withDatabase(
    async (db) => {
      await migrate(db);
      await run(db);
    },
    { max: 8, registry },
  );
}
async function account(db: SQL, name = crypto.randomUUID()) {
  const channel = crypto.randomUUID();
  await new Store(db).createProvider(
    channel,
    { name, kind: "openai", base_url: "http://fixture.invalid/v1", concurrency: 10, rpm: 100 },
    "synthetic-encrypted",
  );
  const row = (await createRepositories(db).select().from(channels)).find(
    (row) => row.id === channel,
  )!;
  return { id: row.accountId, brand: row.brandId, channel };
}
function voucherInput(
  label: string = crypto.randomUUID(),
  expires = new Date(Date.now() + 3600000).toISOString(),
) {
  const content = Buffer.from(`%PDF-1.7\nSynthetic financial material ${label}\n%%EOF\n`);
  return {
    content,
    content_type: "application/pdf" as const,
    digest: createHash("sha256").update(content).digest("hex"),
    expires_at: expires,
  };
}
async function fact(
  store: ReturnType<typeof createUpstreamFundingStore>,
  changes: Partial<UpstreamRechargeInput> = {},
) {
  const voucher = await store.registerVoucher(voucherInput(), actor);
  return {
    external_ref: crypto.randomUUID(),
    payment_currency: "USD",
    payment_atoms: hundred,
    credited_currency: "USD",
    credited_atoms: hundred,
    voucher_id: voucher.id,
    voucher_digest: voucher.digest,
    completed_at: completed,
    reason: "Synthetic completed external recharge",
    ...changes,
  };
}
async function counts(db: SQL) {
  return (
    await db`SELECT (SELECT count(*)::int FROM upstream_funding_operations) AS operations,(SELECT count(*)::int FROM upstream_journals) AS journals,(SELECT count(*)::int FROM upstream_entries) AS entries,(SELECT count(*)::int FROM upstream_audit_events) AS audits`
  )[0];
}

async function blockedBackend(db: SQL, blocker: number): Promise<number> {
  const deadline = Date.now() + 1000;
  while (Date.now() < deadline) {
    const rows =
      await db`SELECT pid FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND ${blocker}=ANY(pg_blocking_pids(pid)) ORDER BY pid LIMIT 1`;
    if (rows[0]) return Number(rows[0].pid);
    await Bun.sleep(5);
  }
  return 0;
}

test("real recharge credits CNY USD HKD independently and shared channels do not duplicate funding", () =>
  isolated(async (db) => {
    const first = await account(db),
      second = await account(db);
    await db`UPDATE channels SET account_id=${first.id},brand_id=${first.brand} WHERE id=${second.channel}`;
    const store = createUpstreamFundingStore(db);
    expect(await store.balances(first.id)).toEqual([]);
    for (const currency of ["CNY", "USD", "HKD"])
      await store.recharge(
        first.id,
        await fact(store, { payment_currency: currency, credited_currency: currency }),
        actor,
      );
    expect(
      (await store.balances(first.id)).map((row) => [
        row.currency,
        row.balance_atoms,
        row.reserved_atoms,
        row.spent_atoms,
        row.available_atoms,
      ]),
    ).toEqual([
      ["CNY", hundred, "0", "0", hundred],
      ["HKD", hundred, "0", "0", hundred],
      ["USD", hundred, "0", "0", hundred],
    ]);
    expect(
      (await db`SELECT count(*)::int AS n FROM channels WHERE account_id=${first.id}`)[0].n,
    ).toBe(2);
    expect(await counts(db)).toEqual({ operations: 3, journals: 3, entries: 6, audits: 3 });
    expect(await store.balances(second.id)).toEqual([]);
  }));

test("CNY payment700 and USD credited100 preserve exact microsecond receipt and balanced USD journal", () =>
  isolated(async (db) => {
    const owner = await account(db),
      store = createUpstreamFundingStore(db);
    const input = await fact(store, {
      payment_currency: "CNY",
      payment_atoms: "700000000000000000000",
    });
    const receipt = await store.recharge(owner.id, input, actor);
    expect(receipt).toMatchObject({
      account_id: owner.id,
      kind: "RECHARGE",
      payment_currency: "CNY",
      payment_atoms: "700000000000000000000",
      credited_currency: "USD",
      credited_atoms: hundred,
      balance_atoms: hundred,
      balance_revision: "1",
      completed_at: completed,
      actor,
    });
    expect(await store.receipt(owner.id, input.external_ref)).toEqual(receipt);
    expect(
      await store.recharge(
        owner.id,
        { ...input, completed_at: "2026-01-01T12:34:56.123456Z" },
        "technical-admin:retry",
      ),
    ).toEqual(receipt);
    expect(
      (await store.balances(owner.id)).map((row) => [row.currency, row.balance_atoms]),
    ).toEqual([["USD", hundred]]);
    expect(
      await db`SELECT currency,account,atoms::text FROM upstream_entries WHERE journal_id=${receipt.journal_id} ORDER BY ordinal`,
    ).toEqual([
      { currency: "USD", account: "upstream:available", atoms: hundred },
      { currency: "USD", account: "external:funding", atoms: "-100000000000000000000" },
    ]);
    expect(
      (
        await db`SELECT sum(atoms)::text AS sum FROM upstream_entries WHERE journal_id=${receipt.journal_id}`
      )[0].sum,
    ).toBe("0");
    expect(
      (await db`SELECT actor,action,operation_id,new_digest,reason FROM upstream_audit_events`)[0],
    ).toEqual({
      actor,
      action: "RECHARGE",
      operation_id: receipt.operation_id,
      new_digest: receipt.payload_digest,
      reason: input.reason,
    });
    expect(
      (
        await db`SELECT to_char(completed_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS at FROM upstream_funding_operations`
      )[0].at,
    ).toBe(completed);
    expect(await counts(db)).toEqual({ operations: 1, journals: 1, entries: 2, audits: 1 });
    await store.recharge(owner.id, await fact(store), actor);
    expect((await store.balances(owner.id))[0]?.balance_atoms).toBe("200000000000000000000");
    expect(await store.recharge(owner.id, input, actor)).toEqual(receipt);
  }));

test("same recharge reference rejects every changed semantic field and cannot spoof actor", () =>
  isolated(async (db) => {
    const owner = await account(db),
      store = createUpstreamFundingStore(db),
      input = await fact(store);
    const receipt = await store.recharge(owner.id, input, actor);
    const other = await store.registerVoucher(voucherInput(), actor);
    for (const change of [
      { payment_currency: "CNY" },
      { payment_atoms: "1" },
      { credited_currency: "HKD" },
      { credited_atoms: "1" },
      { voucher_id: other.id },
      { voucher_digest: other.digest },
      { completed_at: "2026-01-01T12:34:56.123457Z" },
      { reason: "Different confirmed fact" },
    ])
      await expect(store.recharge(owner.id, { ...input, ...change }, actor)).rejects.toMatchObject({
        status: 409,
        code: "upstream_recharge_conflict",
      });
    await expect(
      store.recharge(owner.id, { ...input, actor: "spoofed" } as UpstreamRechargeInput, actor),
    ).rejects.toMatchObject({ status: 400 });
    expect(await store.receipt(owner.id, input.external_ref)).toEqual(receipt);
    expect(await counts(db)).toEqual({ operations: 1, journals: 1, entries: 2, audits: 1 });
  }));

test("strict recharge validation rejects unsafe money currencies calendars and missing facts without money writes", () =>
  isolated(async (db) => {
    const owner = await account(db),
      store = createUpstreamFundingStore(db),
      input = await fact(store);
    for (const change of [
      { payment_atoms: "0" },
      { payment_atoms: "-1" },
      { payment_atoms: 100 },
      { credited_atoms: "0" },
      { credited_atoms: "-1" },
      { credited_atoms: 100 },
      { credited_atoms: "01" },
      { credited_atoms: "1e20" },
      { credited_atoms: "1".repeat(39) },
      { payment_currency: "usd" },
      { credited_currency: "USD/USD" },
      { completed_at: "2026-02-30T12:00:00Z" },
      { completed_at: "0000-01-01T00:00:00Z" },
      { completed_at: "2026-01-01T12:34:56.1234567Z" },
      { completed_at: "2026-01-01T12:34:56+00:00" },
      { completed_at: "2026-01-01T23:59:60Z" },
      { external_ref: "" },
      { voucher_id: null },
      { reason: " " },
    ])
      await expect(
        store.recharge(owner.id, { ...input, ...change } as UpstreamRechargeInput, actor),
      ).rejects.toMatchObject({ status: 400 });
    await expect(
      store.recharge(owner.id, { ...input, completed_at: "9999-01-01T00:00:00Z" }, actor),
    ).rejects.toMatchObject({ code: "recharge_not_completed" });
    await expect(
      store.recharge(owner.id, { ...input, voucher_id: crypto.randomUUID() }, actor),
    ).rejects.toMatchObject({ code: "voucher_not_found" });
    await expect(
      store.recharge(owner.id, { ...input, voucher_digest: "a".repeat(64) }, actor),
    ).rejects.toMatchObject({ code: "voucher_digest_mismatch" });
    expect(await store.balances(owner.id)).toEqual([]);
    expect(await counts(db)).toEqual({ operations: 0, journals: 0, entries: 0, audits: 0 });
  }));

test("recharge accepts exact 38 digit amounts but overflow rolls back the second operation", () =>
  isolated(async (db) => {
    const owner = await account(db),
      store = createUpstreamFundingStore(db);
    await store.recharge(
      owner.id,
      await fact(store, {
        payment_atoms: "99999999999999999999999999999999999999",
        credited_atoms: "99999999999999999999999999999999999999",
      }),
      actor,
    );
    await expect(
      store.recharge(owner.id, await fact(store, { credited_atoms: "1" }), actor),
    ).rejects.toMatchObject({ status: 409, code: "upstream_amount_overflow" });
    expect((await store.balances(owner.id))[0]?.balance_atoms).toBe(
      "99999999999999999999999999999999999999",
    );
    expect(await counts(db)).toEqual({ operations: 1, journals: 1, entries: 2, audits: 1 });
  }));

test("same-key concurrent recharges recover one receipt without duplicate journal or audit", () =>
  isolated(async (db) => {
    const owner = await account(db),
      store = createUpstreamFundingStore(db),
      input = await fact(store);
    const receipts = await Promise.all(
      Array.from({ length: 12 }, () => store.recharge(owner.id, input, actor)),
    );
    for (const receipt of receipts) expect(receipt).toEqual(receipts[0]!);
    expect((await store.balances(owner.id))[0]?.balance_atoms).toBe(hundred);
    expect(await counts(db)).toEqual({ operations: 1, journals: 1, entries: 2, audits: 1 });
  }));

test("same-key different-currency and different-voucher writers produce one winner without lock cycles", () =>
  isolated(async (db) => {
    const owner = await account(db),
      store = createUpstreamFundingStore(db);
    const inputs = await Promise.all(
      ["CNY", "USD", "HKD"].map((currency) =>
        fact(store, { external_ref: "one-key", credited_currency: currency }),
      ),
    );
    const results = await Promise.allSettled(
      inputs.map((input) => store.recharge(owner.id, input, actor)),
    );
    expect(results.filter((result) => result.status === "fulfilled")).toHaveLength(1);
    for (const result of results)
      if (result.status === "rejected")
        expect(result.reason).toMatchObject({ status: 409, code: "upstream_recharge_conflict" });
    const receipt = await store.receipt(owner.id, "one-key");
    expect(
      (await store.balances(owner.id)).map((row) => [row.currency, row.balance_atoms]),
    ).toEqual([[receipt.credited_currency, hundred]]);
    expect(await counts(db)).toEqual({ operations: 1, journals: 1, entries: 2, audits: 1 });
  }));

test("audit failure rolls back balance receipt journal and voucher association inside caller transaction", () =>
  isolated(async (db) => {
    const owner = await account(db),
      store = createUpstreamFundingStore(db),
      input = await fact(store);
    await db.unsafe(
      "CREATE FUNCTION fail_funding_audit() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'injected audit failure'; END$$; CREATE TRIGGER fail_funding_audit BEFORE INSERT ON upstream_audit_events FOR EACH ROW EXECUTE FUNCTION fail_funding_audit()",
    );
    await db.begin(async (tx) => {
      await tx`SELECT id FROM upstream_accounts WHERE id=${owner.id} FOR SHARE`;
      await expect(recordUpstreamRecharge(tx, owner.id, input, actor)).rejects.toMatchObject({
        status: 503,
      });
      expect((await tx`SELECT count(*)::int AS n FROM upstream_funding_operations`)[0].n).toBe(0);
    });
    expect(await store.balances(owner.id)).toEqual([]);
    expect((await store.voucher(input.voucher_id)).referenced).toBe(false);
    expect(await counts(db)).toEqual({ operations: 0, journals: 0, entries: 0, audits: 0 });
    await db`DROP TRIGGER fail_funding_audit ON upstream_audit_events`;
    const receipt = await db.begin((tx) => recordUpstreamRecharge(tx, owner.id, input, actor));
    expect(receipt.balance_atoms).toBe(hundred);
    expect((await store.voucher(input.voucher_id)).referenced).toBe(true);
  }));

test("constraint failure after posting rolls back money and receipt before a successful retry", () =>
  isolated(async (db) => {
    const owner = await account(db),
      store = createUpstreamFundingStore(db),
      input = await fact(store);
    await db.unsafe(
      "ALTER TABLE upstream_entries ADD CONSTRAINT reject_synthetic_funding CHECK(account<>'external:funding')",
    );
    await expect(store.recharge(owner.id, input, actor)).rejects.toMatchObject({
      status: 409,
      code: "upstream_funding_constraint",
    });
    expect(await store.balances(owner.id)).toEqual([]);
    expect(await counts(db)).toEqual({ operations: 0, journals: 0, entries: 0, audits: 0 });
    await db.unsafe("ALTER TABLE upstream_entries DROP CONSTRAINT reject_synthetic_funding");
    expect((await store.recharge(owner.id, input, actor)).balance_atoms).toBe(hundred);
  }));

test("recharge never revives disabled quarantined or reconciliation-required accounts or rewrites reserved and spent", () =>
  isolated(async (db) => {
    const owner = await account(db),
      store = createUpstreamFundingStore(db);
    await db.unsafe(
      "CREATE FUNCTION guard_funding_caches() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN IF NEW.reserved_atoms IS DISTINCT FROM OLD.reserved_atoms OR NEW.spent_atoms IS DISTINCT FROM OLD.spent_atoms OR NEW.adjustment_atoms IS DISTINCT FROM OLD.adjustment_atoms THEN RAISE EXCEPTION 'funding changed exposure caches';END IF; RETURN NEW; END$$; CREATE TRIGGER guard_funding_caches BEFORE UPDATE ON upstream_account_balances FOR EACH ROW EXECUTE FUNCTION guard_funding_caches()",
    );
    for (const state of ["DISABLED", "QUARANTINED", "RECONCILE_REQUIRED"]) {
      await db`UPDATE upstream_accounts SET state=${state} WHERE id=${owner.id}`;
      await store.recharge(owner.id, await fact(store), actor);
      expect(
        (await db`SELECT state,revision::text FROM upstream_accounts WHERE id=${owner.id}`)[0],
      ).toEqual({ state, revision: "0" });
    }
    expect((await store.balances(owner.id))[0]).toMatchObject({
      balance_atoms: "300000000000000000000",
      reserved_atoms: "0",
      spent_atoms: "0",
      adjustment_atoms: "0",
    });
  }));

test("voucher uploads verify bounded allowed bytes digest and immutable content before persistence", () =>
  isolated(async (db) => {
    const store = createUpstreamFundingStore(db),
      input = voucherInput();
    for (const invalid of [
      { ...input, content: new Uint8Array() },
      { ...input, content: new Uint8Array(5242881) },
      { ...input, content_type: "text/html" },
      { ...input, content: Buffer.from("<html>unsafe</html>") },
      { ...input, digest: "a".repeat(64) },
      { ...input, url: "https://example.invalid/voucher.pdf" },
    ])
      await expect(store.registerVoucher(invalid as typeof input, actor)).rejects.toMatchObject({
        status: 400,
      });
    expect((await db`SELECT count(*)::int AS n FROM financial_vouchers`)[0].n).toBe(0);
    const content = Buffer.alloc(5242880, 32);
    content.write("%PDF-1.7");
    const large = await store.registerVoucher(
      { ...input, content, digest: createHash("sha256").update(content).digest("hex") },
      actor,
    );
    expect(large.size).toBe(5242880);
    const uploaded = await store.registerVoucher(input, actor);
    expect(
      await store.registerVoucher(
        { ...input, expires_at: new Date(Date.now() + 7200000).toISOString() },
        "technical-admin:retry",
      ),
    ).toEqual(uploaded);
    expect((await store.voucher(uploaded.id)).content).toEqual(input.content);
    await expect(
      db`UPDATE financial_vouchers SET actor='modified' WHERE id=${uploaded.id}`,
    ).rejects.toThrow("immutable");
    await expect(
      db`INSERT INTO financial_vouchers(id,digest,content_type,content,actor,expires_at) VALUES(${crypto.randomUUID()},${"a".repeat(64)},'application/pdf',${input.content},'test',now()+interval '1 hour')`,
    ).rejects.toMatchObject({ errno: "23514" });
  }));

test("expired unreferenced vouchers reject recharge and reupload until explicit cleanup while referenced evidence survives", () =>
  isolated(async (db) => {
    const owner = await account(db),
      store = createUpstreamFundingStore(db);
    const unreferenced = voucherInput(
      "expired-unreferenced",
      new Date(Date.now() + 300).toISOString(),
    );
    const stale = await store.registerVoucher(unreferenced, actor);
    const linkedInput = voucherInput(
      "expired-referenced",
      new Date(Date.now() + 300).toISOString(),
    );
    const linked = await store.registerVoucher(linkedInput, actor);
    await store.recharge(
      owner.id,
      await fact(store, { voucher_id: linked.id, voucher_digest: linked.digest }),
      actor,
    );
    await Bun.sleep(350);
    await expect(store.registerVoucher(unreferenced, actor)).rejects.toMatchObject({
      code: "voucher_expired",
    });
    await expect(
      store.recharge(
        owner.id,
        await fact(store, { voucher_id: stale.id, voucher_digest: stale.digest }),
        actor,
      ),
    ).rejects.toMatchObject({ code: "voucher_expired" });
    expect((await store.registerVoucher(linkedInput, actor)).id).toBe(linked.id);
    expect(await store.cleanupVouchers()).toEqual([stale.id]);
    await expect(store.voucher(stale.id)).rejects.toMatchObject({ status: 404 });
    expect((await store.voucher(linked.id)).referenced).toBe(true);
    await expect(db`DELETE FROM financial_vouchers WHERE id=${linked.id}`).rejects.toMatchObject({
      errno: "23503",
    });
    const replacement = await store.registerVoucher(
      { ...unreferenced, expires_at: new Date(Date.now() + 3600000).toISOString() },
      actor,
    );
    expect(replacement.id).not.toBe(stale.id);
    expect(replacement.digest).toBe(stale.digest);
  }));

test("cleanup racing an in-flight committed recharge cannot delete its referenced evidence", () =>
  isolated(async (db) => {
    const owner = await account(db),
      store = createUpstreamFundingStore(db);
    const uploaded = await store.registerVoucher(
      voucherInput("race-funding-first", new Date(Date.now() + 400).toISOString()),
      actor,
    );
    const input = await fact(store, { voucher_id: uploaded.id, voucher_digest: uploaded.digest });
    let ready!: () => void, commit!: () => void;
    const locked = new Promise<void>((resolve) => {
      ready = resolve;
    });
    const release = new Promise<void>((resolve) => {
      commit = resolve;
    });
    const funding = db.begin(async (tx) => {
      const receipt = await recordUpstreamRecharge(tx, owner.id, input, actor);
      ready();
      await release;
      return receipt;
    });
    try {
      await locked;
      await Bun.sleep(450);
      expect(await store.cleanupVouchers()).toEqual([]);
    } finally {
      commit();
    }
    expect((await funding).balance_atoms).toBe(hundred);
    expect(await store.cleanupVouchers()).toEqual([]);
    expect((await store.voucher(uploaded.id)).referenced).toBe(true);
  }));

test("outer rollback or deferred constraint failure cannot turn a returned recharge into committed money", () =>
  isolated(async (db) => {
    const owner = await account(db),
      store = createUpstreamFundingStore(db),
      input = await fact(store);
    let returned: string | undefined;
    await expect(
      db.begin(async (tx) => {
        returned = (await recordUpstreamRecharge(tx, owner.id, input, actor)).operation_id;
        expect((await tx`SELECT count(*)::int AS n FROM upstream_funding_operations`)[0].n).toBe(1);
        expect(await counts(db)).toEqual({ operations: 0, journals: 0, entries: 0, audits: 0 });
        throw new Error("caller transaction rolled back");
      }),
    ).rejects.toThrow("caller transaction rolled back");
    expect(returned).toBeString();
    expect(await store.balances(owner.id)).toEqual([]);
    await expect(
      db.begin(async (tx) => {
        returned = (await recordUpstreamRecharge(tx, owner.id, input, actor)).operation_id;
        await tx`INSERT INTO upstream_journals(id,account_id,currency,kind,reference) VALUES(${crypto.randomUUID()},${owner.id},'USD','TEST_OPENING','synthetic-deferred-failure')`;
      }),
    ).rejects.toThrow("unbalanced upstream journal");
    expect(await counts(db)).toEqual({ operations: 0, journals: 0, entries: 0, audits: 0 });
    expect((await store.voucher(input.voucher_id)).referenced).toBe(false);
    expect((await store.recharge(owner.id, input, actor)).balance_atoms).toBe(hundred);
  }));

test("concurrent same digest uploads return one immutable voucher while PNG JPEG and PDF signatures are explicit", () =>
  isolated(async (db) => {
    const store = createUpstreamFundingStore(db),
      input = voucherInput();
    const uploads = await Promise.all(
      Array.from({ length: 8 }, () => store.registerVoucher(input, actor)),
    );
    expect(new Set(uploads.map((value) => value.id)).size).toBe(1);
    const png = Buffer.from([137, 80, 78, 71, 13, 10, 26, 10, 0, 0, 0, 0]);
    const jpeg = Buffer.from([255, 216, 255, 224, 0, 0, 255, 217]);
    for (const [content, content_type] of [
      [png, "image/png"],
      [jpeg, "image/jpeg"],
    ] as const) {
      const registered = await store.registerVoucher(
        {
          ...input,
          content,
          content_type,
          digest: createHash("sha256").update(content).digest("hex"),
        },
        actor,
      );
      expect(registered.content_type).toBe(content_type);
      expect((await store.voucher(registered.id)).content).toEqual(content);
    }
    for (const expires_at of ["2020-01-01T00:00:00Z", "9999-01-01T00:00:00Z"])
      await expect(
        store.registerVoucher({ ...voucherInput(), expires_at }, actor),
      ).rejects.toMatchObject({ status: 400, code: "invalid_voucher_expiry" });
    for (const limit of [0, 1001, 1.5])
      await expect(store.cleanupVouchers(limit)).rejects.toMatchObject({ status: 400 });
  }));

test("second precision completion normalizes to the same microsecond receipt and replay bypasses a locked balance", () =>
  isolated(async (db) => {
    const owner = await account(db),
      store = createUpstreamFundingStore(db);
    const input = await fact(store, { completed_at: "2026-01-01T00:00:00Z" });
    const receipt = await store.recharge(owner.id, input, actor);
    expect(receipt.completed_at).toBe("2026-01-01T00:00:00.000000Z");
    await db.begin(async (tx) => {
      await tx`SELECT account_id FROM upstream_account_balances WHERE account_id=${owner.id} FOR NO KEY UPDATE`;
      expect(
        await store.recharge(
          owner.id,
          { ...input, completed_at: "2026-01-01T00:00:00.000000Z" },
          actor,
        ),
      ).toEqual(receipt);
    });
  }));

test("cleanup that locks an expired voucher first makes a waiting recharge fail without partial funds", () =>
  isolated(async (db) => {
    const owner = await account(db),
      store = createUpstreamFundingStore(db);
    const uploaded = await store.registerVoucher(
      voucherInput("race-cleanup-first", new Date(Date.now() + 150).toISOString()),
      actor,
    );
    const input = await fact(store, { voucher_id: uploaded.id, voucher_digest: uploaded.digest });
    await Bun.sleep(200);
    const blocker = await db.reserve();
    let cleanup: Promise<string[]> | undefined;
    let funding: ReturnType<typeof store.recharge> | undefined;
    try {
      const blockerPid = Number((await blocker`SELECT pg_backend_pid() AS pid`)[0].pid);
      await blocker`SELECT pg_advisory_lock(77120403)`;
      await db.unsafe(
        "CREATE FUNCTION pause_voucher_cleanup() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN PERFORM pg_advisory_xact_lock(77120403); RETURN OLD; END$$; CREATE TRIGGER pause_voucher_cleanup BEFORE DELETE ON financial_vouchers FOR EACH ROW EXECUTE FUNCTION pause_voucher_cleanup()",
      );
      cleanup = store.cleanupVouchers();
      const cleanupPid = await blockedBackend(db, blockerPid);
      expect(cleanupPid).toBeGreaterThan(0);
      funding = store.recharge(owner.id, input, actor);
      const rejected = funding.then(
        () => ({ status: "unexpected_success" }),
        (error: unknown) => error,
      );
      expect(await blockedBackend(db, cleanupPid)).toBeGreaterThan(0);
      await blocker`SELECT pg_advisory_unlock(77120403)`;
      expect(await cleanup).toEqual([uploaded.id]);
      expect(await rejected).toMatchObject({ status: 404, code: "voucher_not_found" });
    } finally {
      await blocker`SELECT pg_advisory_unlock_all()`;
      blocker.release();
      await Promise.allSettled([cleanup, funding]);
    }
    expect(await store.balances(owner.id)).toEqual([]);
    expect(await counts(db)).toEqual({ operations: 0, journals: 0, entries: 0, audits: 0 });
  }));

for (const action of ["recharge", "upload"] as const)
  test(`voucher expiring while ${action} waits for its lock is rejected after locking`, () =>
    isolated(async (db) => {
      const owner = await account(db),
        store = createUpstreamFundingStore(db);
      const upload = voucherInput(
        `expiry-lock-${action}`,
        new Date(Date.now() + 400).toISOString(),
      );
      const voucher = await store.registerVoucher(upload, actor);
      const input = await fact(store, { voucher_id: voucher.id, voucher_digest: voucher.digest });
      let result: Promise<unknown> | undefined;
      await db.begin(async (tx) => {
        const blockerPid = Number((await tx`SELECT pg_backend_pid() AS pid`)[0].pid);
        await tx`SELECT id FROM financial_vouchers WHERE id=${voucher.id} FOR UPDATE`;
        const operation =
          action === "recharge"
            ? store.recharge(owner.id, input, actor)
            : store.registerVoucher(upload, actor);
        result = operation.then(
          () => ({ code: "unexpected_success" }),
          (error: unknown) => error,
        );
        expect(await blockedBackend(db, blockerPid)).toBeGreaterThan(0);
        await Bun.sleep(450);
      });
      expect(await result).toMatchObject({ status: 409, code: "voucher_expired" });
      expect(await store.balances(owner.id)).toEqual([]);
      expect(await counts(db)).toEqual({ operations: 0, journals: 0, entries: 0, audits: 0 });
    }));
