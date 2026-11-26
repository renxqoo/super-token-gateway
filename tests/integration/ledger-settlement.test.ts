import { expect, test } from "bun:test";
import { appendFile, rm } from "node:fs/promises";
import { getTableConfig } from "drizzle-orm/pg-core";
import {
  assertSchemaVersion,
  exportLedgerArchive,
  ledgerSchema,
  migrate,
  prepareLedgerMonth,
  SCHEMA_VERSION,
  verifyLedgerArchive,
} from "@gateway/ledger";
import migrationManifest from "../../packages/ledger/src/migrations/manifest.json";
import { withLedgerDb, seedFixture } from "./ledger-fixture.ts";

test.each([
  ["without a total or the other TTL", { cache_write_5m_tokens: "3" }, "invalid_upstream_usage"],
  [
    "with a TTL detail greater than the total",
    { cache_write_tokens: "0", cache_write_1h_tokens: "3" },
    "usage_alias_conflict",
  ],
] as const)(
  "symptom: aggregate cache settlement charges usage %s before correction",
  (_, usage, code) =>
    withLedgerDb(async (db) => {
      const f = await seedFixture(db);
      const customer = await f.catalog.createPriceVersion({
        side: "CUSTOMER",
        currency: "PTS",
        basis: "PROVIDER_USAGE",
        rates: { cache_write_tokens: { numerator: "3", denominator: "1" } },
      });
      const supplier = await f.catalog.createPriceVersion({
        side: "SUPPLIER",
        currency: "USD",
        basis: "PROVIDER_USAGE",
        rates: { cache_write_tokens: { numerator: "2", denominator: "1" } },
      });
      await f.ledger.creditWallet({
        billing_account_id: f.billingAccount.id,
        currency: "PTS",
        external_ref: "aggregate-cache-write-test",
        amount_atoms: "100",
        kind: "CASH",
      });
      const requestId = crypto.randomUUID();
      const attemptId = crypto.randomUUID();
      expect(
        await f.ledger.acquireHold({
          request_id: requestId,
          attempt_id: attemptId,
          billing_account_id: f.billingAccount.id,
          currency: "PTS",
          operation: "chat",
          model: "gpt-4o",
          idempotency_key: null,
          fingerprint: "aggregate-cache-write",
          customer_atoms: "30",
          supplier_account_id: f.accountA.id,
          supplier_channel_id: f.channelA.id,
          supplier_currency: "USD",
          supplier_atoms: "20",
          expires_at: new Date(Date.now() + 30_000).toISOString(),
        }),
      ).toMatchObject({ ok: true, replayed: false });
      const terminal = {
        request_id: requestId,
        selected_attempt_id: attemptId,
        channel_id: f.channelA.id,
        billing_price_id: customer.id,
        cost_version_id: supplier.id,
        billing_pricing: { currency: "PTS", rates: customer.rates },
        billing_basis: "PROVIDER_USAGE",
        supplier_pricing: { currency: "USD", rates: supplier.rates },
        supplier_currency: "USD",
      } as const;

      expect(await f.ledger.commitTerminal({ ...terminal, usage })).toMatchObject({
        ok: false,
        code,
      });
      expect(await f.ledger.getWallet(f.billingAccount.id, "PTS")).toMatchObject({
        available_atoms: "70",
        reserved_atoms: "30",
        spent_atoms: "0",
      });
      const complete = {
        ...terminal,
        usage: { cache_write_5m_tokens: "3", cache_write_1h_tokens: "2" },
      };
      expect(await f.ledger.commitTerminal(complete)).toMatchObject({
        ok: true,
        replayed: false,
        customer_atoms: "15",
        supplier_cost_atoms: "10",
      });
      expect(await f.ledger.commitTerminal(complete)).toMatchObject({
        ok: true,
        replayed: true,
        customer_atoms: "15",
        supplier_cost_atoms: "10",
      });
      expect(await f.ledger.getWallet(f.billingAccount.id, "PTS")).toMatchObject({
        available_atoms: "85",
        reserved_atoms: "0",
        spent_atoms: "15",
      });
      expect((await f.ledger.auditConservation()).ok).toBe(true);
    }),
);

test("G1.3 100 concurrent micro-batched holds on one single hot wallet have zero 55P03 errors and exact conservation", () =>
  withLedgerDb(async (db) => {
    const f = await seedFixture(db);
    await f.ledger.creditWallet({
      billing_account_id: f.billingAccount.id,
      currency: "PTS",
      external_ref: "hot-wallet-fund",
      amount_atoms: "5000",
      kind: "CASH",
    });

    const requests = Array.from({ length: 100 }, (_, i) => ({
      reqId: crypto.randomUUID(),
      attId: crypto.randomUUID(),
      fp: `hot-${i}`,
    }));

    const holds = await Promise.all(
      requests.map((r) =>
        f.ledger.acquireHold({
          request_id: r.reqId,
          attempt_id: r.attId,
          billing_account_id: f.billingAccount.id,
          currency: "PTS",
          operation: "chat",
          model: "gpt-4o",
          idempotency_key: null,
          fingerprint: r.fp,
          customer_atoms: "100",
          supplier_account_id: f.accountA.id,
          supplier_channel_id: f.channelA.id,
          supplier_currency: "USD",
          supplier_atoms: "50",
          expires_at: new Date(Date.now() + 30000).toISOString(),
        }),
      ),
    );

    const succeeded = holds
      .map((res, idx) => ({ res, req: requests[idx]! }))
      .filter((x) => x.res.ok);
    const rejected = holds.filter((res) => !res.ok);
    expect(succeeded).toHaveLength(50);
    expect(rejected).toHaveLength(50);
    for (const r of rejected) expect(r).toMatchObject({ ok: false, code: "insufficient_balance" });

    const midWallet = await f.ledger.getWallet(f.billingAccount.id, "PTS");
    expect(midWallet).toMatchObject({ available_atoms: "0", reserved_atoms: "5000" });

    const commits = await Promise.all(
      succeeded.map(({ req }) =>
        f.ledger.commitTerminal({
          request_id: req.reqId,
          selected_attempt_id: req.attId,
          channel_id: f.channelA.id,
          billing_price_id: f.flatBillingPrice.id,
          cost_version_id: f.costPrice.id,
          usage: { input_tokens: "40", output_tokens: "30" },
          billing_pricing: { currency: "PTS", rates: f.flatBillingPrice.rates },
          billing_basis: "PROVIDER_USAGE",
          supplier_pricing: { currency: "USD", rates: f.costPrice.rates },
          supplier_currency: "USD",
        }),
      ),
    );
    expect(commits.every((c) => c.ok && !c.replayed && c.customer_atoms === "70")).toBe(true);

    const endWallet = await f.ledger.getWallet(f.billingAccount.id, "PTS");
    expect(endWallet).toMatchObject({
      available_atoms: "1500",
      reserved_atoms: "0",
      spent_atoms: "3500",
      debt_atoms: "0",
    });
    const sup = await f.ledger.getSupplierBalance(f.accountA.id, "USD");
    expect(sup).toEqual({
      balance_atoms: "998250",
      reserved_atoms: "0",
      spent_atoms: "1750",
    });

    const claimed = await f.ledger.claimOutboxEvents("business-consumer", 100);
    expect(claimed.events).toHaveLength(50);
    expect(
      await f.ledger.ackOutboxEvents(
        "business-consumer",
        claimed.lease_token,
        claimed.events.at(-1)!.seq,
      ),
    ).toBe(true);
    expect((await f.ledger.claimOutboxEvents("business-consumer", 100)).events).toHaveLength(0);

    expect(await f.ledger.auditConservation()).toEqual({
      ok: true,
      unbalanced_journals: 0,
      broken_lots: 0,
      wallet_mismatches: 0,
      reservation_mismatches: 0,
      customer_ledger_mismatches: 0,
      supplier_ledger_mismatches: 0,
      open_holds: 0,
      unknown_holds: 0,
    });
  }));

test("G1.4 failed fallback cannot strand a customer top-up, and successful failover preserves UNKNOWN supplier funds", () =>
  withLedgerDb(async (db) => {
    const f = await seedFixture(db);
    await f.ledger.creditWallet({
      billing_account_id: f.billingAccount.id,
      currency: "PTS",
      external_ref: "failover-fund",
      amount_atoms: "500",
      kind: "CASH",
    });

    const reqId = crypto.randomUUID();
    const attA = crypto.randomUUID();
    const attB = crypto.randomUUID();
    const attC = crypto.randomUUID();

    const hold = await f.ledger.acquireHold({
      request_id: reqId,
      attempt_id: attA,
      billing_account_id: f.billingAccount.id,
      currency: "PTS",
      operation: "chat",
      model: "gpt-4o",
      idempotency_key: null,
      fingerprint: "fp-failover",
      customer_atoms: "100",
      supplier_account_id: f.accountA.id,
      supplier_channel_id: f.channelA.id,
      supplier_currency: "USD",
      supplier_atoms: "50",
      expires_at: new Date(Date.now() + 30000).toISOString(),
    });
    expect(hold.ok).toBe(true);

    const rejected = await f.ledger.switchSupplierAttempt({
      request_id: reqId,
      failed_attempt_id: attA,
      failed_outcome: "FAILED",
      next_attempt_id: crypto.randomUUID(),
      next_account_id: f.accountB.id,
      next_channel_id: f.channelB.id,
      next_currency: "USD",
      next_supplier_atoms: "1000001",
      required_customer_atoms: "150",
    });
    expect(rejected).toMatchObject({ ok: false, code: "supplier_insufficient_balance" });
    expect(await f.ledger.getWallet(f.billingAccount.id, "PTS")).toMatchObject({
      available_atoms: "400",
      reserved_atoms: "100",
    });
    expect((await f.ledger.getSupplierBalance(f.accountA.id, "USD")).reserved_atoms).toBe("50");

    const sw1 = await f.ledger.switchSupplierAttempt({
      request_id: reqId,
      failed_attempt_id: attA,
      failed_outcome: "FAILED",
      next_attempt_id: attB,
      next_account_id: f.accountB.id,
      next_channel_id: f.channelB.id,
      next_currency: "USD",
      next_supplier_atoms: "60",
      required_customer_atoms: "150",
    });
    expect(sw1).toMatchObject({ ok: true, held_atoms: "150" });
    expect((await f.ledger.getSupplierBalance(f.accountA.id, "USD")).reserved_atoms).toBe("0");
    expect((await f.ledger.getSupplierBalance(f.accountB.id, "USD")).reserved_atoms).toBe("60");

    const sw2 = await f.ledger.switchSupplierAttempt({
      request_id: reqId,
      failed_attempt_id: attB,
      failed_outcome: "UNKNOWN",
      next_attempt_id: attC,
      next_account_id: f.accountC.id,
      next_channel_id: f.channelC.id,
      next_currency: "USD",
      next_supplier_atoms: "70",
    });
    expect(sw2.ok).toBe(true);
    expect((await f.ledger.getSupplierBalance(f.accountB.id, "USD")).reserved_atoms).toBe("60");
    expect((await f.ledger.getSupplierBalance(f.accountC.id, "USD")).reserved_atoms).toBe("70");

    const settled = await f.ledger.commitTerminal({
      request_id: reqId,
      selected_attempt_id: attC,
      channel_id: f.channelC.id,
      billing_price_id: f.flatBillingPrice.id,
      cost_version_id: f.costPrice.id,
      usage: { input_tokens: "60", output_tokens: "60" },
      billing_pricing: { currency: "PTS", rates: f.flatBillingPrice.rates },
      billing_basis: "PROVIDER_USAGE",
      supplier_pricing: { currency: "USD", rates: f.costPrice.rates },
      supplier_currency: "USD",
    });
    expect(settled).toMatchObject({
      ok: true,
      customer_atoms: "120",
      supplier_cost_atoms: "60",
    });

    expect(await f.ledger.getWallet(f.billingAccount.id, "PTS")).toMatchObject({
      available_atoms: "380",
      reserved_atoms: "0",
      spent_atoms: "120",
    });
    expect(await f.ledger.getSupplierBalance(f.accountA.id, "USD")).toEqual({
      balance_atoms: "1000000",
      reserved_atoms: "0",
      spent_atoms: "0",
    });
    expect(await f.ledger.getSupplierBalance(f.accountB.id, "USD")).toEqual({
      balance_atoms: "1000000",
      reserved_atoms: "60",
      spent_atoms: "0",
    });
    expect(await f.ledger.getSupplierBalance(f.accountC.id, "USD")).toEqual({
      balance_atoms: "999940",
      reserved_atoms: "0",
      spent_atoms: "60",
    });
    expect((await f.ledger.auditConservation()).ok).toBe(true);

    await db.begin(async (tx) => {
      await tx`UPDATE public.wallets SET available_atoms = available_atoms - 1,
        reserved_atoms = reserved_atoms + 1 WHERE billing_account_id = ${f.billingAccount.id}`;
      await tx`UPDATE public.wallet_lots SET available_atoms = available_atoms - 1,
        reserved_atoms = reserved_atoms + 1
        WHERE wallet_id = (SELECT id FROM public.wallets WHERE billing_account_id = ${f.billingAccount.id})`;
    });
    expect((await f.ledger.auditConservation()).ok).toBe(false);
  }));

test("G1.5 moderation PLATFORM_ABSORB, DB immutability/balance triggers, and monthly archive verification", () =>
  withLedgerDb(async (db) => {
    const f = await seedFixture(db);
    await f.ledger.creditWallet({
      billing_account_id: f.billingAccount.id,
      currency: "PTS",
      external_ref: "mod-fund",
      amount_atoms: "300",
      kind: "CASH",
    });

    const reqId = crypto.randomUUID();
    const attId = crypto.randomUUID();
    await f.ledger.acquireHold({
      request_id: reqId,
      attempt_id: attId,
      billing_account_id: f.billingAccount.id,
      currency: "PTS",
      operation: "chat",
      model: "gpt-4o",
      idempotency_key: null,
      fingerprint: "fp-mod",
      customer_atoms: "100",
      supplier_account_id: f.accountA.id,
      supplier_channel_id: f.channelA.id,
      supplier_currency: "USD",
      supplier_atoms: "50",
      expires_at: new Date(Date.now() + 30000).toISOString(),
    });

    const blocked = await f.ledger.commitTerminal({
      request_id: reqId,
      selected_attempt_id: attId,
      channel_id: f.channelA.id,
      billing_price_id: f.flatBillingPrice.id,
      cost_version_id: f.costPrice.id,
      usage: { input_tokens: "30", output_tokens: "30" },
      billing_pricing: { currency: "PTS", rates: f.flatBillingPrice.rates },
      billing_basis: "PROVIDER_USAGE",
      supplier_pricing: { currency: "USD", rates: f.costPrice.rates },
      supplier_currency: "USD",
      charge_policy: "PLATFORM_ABSORB",
      moderation_blocked: true,
      plugin_audit: [
        {
          plugin_id: "mod-1",
          version: "1.0.0",
          hook: "on_response",
          action: "BLOCK",
          rule_code: "PII_BLOCKED",
          latency_us: 45,
        },
      ],
    });
    expect(blocked).toMatchObject({
      ok: true,
      outcome: "MODERATION_BLOCKED",
      customer_atoms: "0",
      funded_atoms: "0",
      supplier_cost_atoms: "30",
    });
    expect(await f.ledger.getWallet(f.billingAccount.id, "PTS")).toMatchObject({
      available_atoms: "300",
      reserved_atoms: "0",
      spent_atoms: "0",
    });
    expect((await f.ledger.getSupplierBalance(f.accountA.id, "USD")).spent_atoms).toBe("30");

    const badJournal = crypto.randomUUID();
    await expect(
      db.begin(async (tx) => {
        await tx`
          INSERT INTO public.ledger_entries (journal_id, ordinal, side, entity_id, reference, kind, currency, account, atoms)
          VALUES (${badJournal}, 0, 'CUSTOMER', ${f.billingAccount.id}, 'bad-1', 'ADJUST', 'PTS', 'system:revenue', 100)
        `;
      }),
    ).rejects.toThrow("unbalanced ledger journal");

    await expect(db`DELETE FROM public.ledger_entries`).rejects.toThrow("immutable record");
    await expect(db`UPDATE public.price_versions SET currency = 'EUR'`).rejects.toThrow(
      "immutable record",
    );

    const month = new Date().toISOString().slice(0, 7);
    await prepareLedgerMonth(db, month);
    const archiveDir = new URL(`../../.local/v4-archive-${crypto.randomUUID()}/`, import.meta.url)
      .pathname;
    try {
      const manifestPath = await exportLedgerArchive(db, month, archiveDir);
      const verified = await verifyLedgerArchive(manifestPath);
      expect(verified.files.find((x) => x.kind === "charges")?.rows).toBe(1);
      expect(verified.files.find((x) => x.kind === "entries")?.rows).toBeGreaterThanOrEqual(4);
      const entriesFile = verified.files.find((file) => file.kind === "entries")!.file;
      await appendFile(`${archiveDir}/${entriesFile}`, "tampered");
      await expect(verifyLedgerArchive(manifestPath)).rejects.toThrow("checksum");
    } finally {
      await rm(archiveDir, { recursive: true, force: true });
    }

    expect((await f.ledger.auditConservation()).ok).toBe(true);
  }));

test("G1.6 Drizzle schema declarations match migrated PostgreSQL tables and columns 1-to-1 with zero drift", () =>
  withLedgerDb(async (db) => {
    expect(await assertSchemaVersion(db)).toBe(SCHEMA_VERSION);
    const pgRows = await db`
      SELECT c.table_name, c.column_name
      FROM information_schema.columns c
      JOIN pg_class cl ON cl.relname = c.table_name
      JOIN pg_namespace n ON n.oid = cl.relnamespace AND n.nspname = c.table_schema
      WHERE c.table_schema = 'public'
        AND NOT cl.relispartition
      ORDER BY c.table_name, c.column_name
    `;
    const actualByTable = new Map<string, string[]>();
    for (const row of pgRows) {
      const t = String(row.table_name);
      const col = String(row.column_name);
      const arr = actualByTable.get(t);
      if (arr) arr.push(col);
      else actualByTable.set(t, [col]);
    }

    const declaredByTable = new Map<string, string[]>();
    for (const table of Object.values(ledgerSchema)) {
      const cfg = getTableConfig(table);
      declaredByTable.set(cfg.name, cfg.columns.map((c) => c.name).sort());
    }

    expect([...declaredByTable.keys()].sort()).toEqual([...actualByTable.keys()].sort());
    for (const [tableName, cols] of declaredByTable) {
      expect(actualByTable.get(tableName)).toEqual(cols);
    }
  }));

test("a settlement constraint error does not roll back another request in the same microbatch", () =>
  withLedgerDb(async (db) => {
    const f = await seedFixture(db);
    const badRequest = "00000000-0000-4000-8000-000000000012";
    await db`ALTER TABLE public.billing_charges
      ADD CONSTRAINT test_bad_charge CHECK (request_id <> '00000000-0000-4000-8000-000000000012')`;
    await f.ledger.creditWallet({
      billing_account_id: f.billingAccount.id,
      currency: "PTS",
      external_ref: "batch-isolation-fund",
      amount_atoms: "500",
      kind: "CASH",
    });
    const items = [badRequest, crypto.randomUUID()].map((request_id) => ({
      request_id,
      attempt_id: crypto.randomUUID(),
    }));
    for (const item of items) {
      const hold = await f.ledger.acquireHold({
        ...item,
        billing_account_id: f.billingAccount.id,
        currency: "PTS",
        operation: "chat",
        model: "gpt-4o",
        idempotency_key: null,
        fingerprint: "",
        customer_atoms: "100",
        supplier_account_id: f.accountA.id,
        supplier_channel_id: f.channelA.id,
        supplier_currency: "USD",
        supplier_atoms: "50",
        expires_at: new Date(Date.now() + 1000).toISOString(),
      });
      expect(hold.ok).toBe(true);
    }
    const [bad, good] = await Promise.allSettled(
      items.map((item) =>
        f.ledger.commitTerminal({
          request_id: item.request_id,
          selected_attempt_id: item.attempt_id,
          channel_id: f.channelA.id,
          billing_price_id: f.flatBillingPrice.id,
          cost_version_id: f.costPrice.id,
          usage: { input_tokens: "60", output_tokens: "20" },
          billing_pricing: { currency: "PTS", rates: f.flatBillingPrice.rates },
          billing_basis: "PROVIDER_USAGE",
          supplier_pricing: { currency: "USD", rates: f.costPrice.rates },
          supplier_currency: "USD",
        }),
      ),
    );
    expect(bad).toMatchObject({ status: "rejected" });
    expect(good).toMatchObject({
      status: "fulfilled",
      value: { ok: true, customer_atoms: "80", supplier_cost_atoms: "40" },
    });
    expect(await f.ledger.releaseHold(badRequest)).toBe(true);
    expect(await f.ledger.getWallet(f.billingAccount.id, "PTS")).toMatchObject({
      available_atoms: "420",
      reserved_atoms: "0",
      spent_atoms: "80",
    });
    await f.ledger.drain();
  }));

test.each(["replaced attempt", "wrong channel"] as const)(
  "symptom: direct settlement can charge a hold using the %s",
  (scenario) =>
    withLedgerDb(async (db) => {
      const f = await seedFixture(db);
      try {
        await f.ledger.creditWallet({
          billing_account_id: f.billingAccount.id,
          currency: "PTS",
          external_ref: "attempt-identity-funding",
          amount_atoms: "500",
          kind: "CASH",
        });
        const requestId = crypto.randomUUID();
        const primary = crypto.randomUUID();
        const backup = crypto.randomUUID();
        const acquired = await f.ledger.acquireHold({
          request_id: requestId,
          attempt_id: primary,
          billing_account_id: f.billingAccount.id,
          currency: "PTS",
          operation: "chat",
          model: "gpt-4o",
          idempotency_key: "direct-settlement-identity",
          fingerprint: "attempt-identity",
          customer_atoms: "100",
          supplier_account_id: f.accountA.id,
          supplier_channel_id: f.channelA.id,
          supplier_currency: "USD",
          supplier_atoms: "40",
          expires_at: new Date(Date.now() + 30000).toISOString(),
        });
        expect(acquired.ok).toBe(true);
        const switched = await f.ledger.switchSupplierAttempt({
          request_id: requestId,
          failed_attempt_id: primary,
          failed_outcome: "FAILED",
          next_attempt_id: backup,
          next_account_id: f.accountB.id,
          next_channel_id: f.channelB.id,
          next_currency: "USD",
          next_supplier_atoms: "60",
        });
        expect(switched.ok).toBe(true);
        const terminal = {
          request_id: requestId,
          selected_attempt_id: backup,
          channel_id: f.channelB.id,
          billing_price_id: f.flatBillingPrice.id,
          cost_version_id: f.costPrice.id,
          usage: { input_tokens: "20", output_tokens: "10" },
          billing_pricing: { currency: "PTS", rates: f.flatBillingPrice.rates },
          billing_basis: "PROVIDER_USAGE" as const,
          supplier_pricing: { currency: "USD", rates: f.costPrice.rates },
          supplier_currency: "USD",
        };
        const rejected = await f.ledger.commitTerminal({
          ...terminal,
          selected_attempt_id: scenario === "replaced attempt" ? primary : backup,
          channel_id: f.channelA.id,
        });
        expect(rejected).toMatchObject({ ok: false, code: "invalid_state" });
        expect(await f.ledger.getWallet(f.billingAccount.id, "PTS")).toMatchObject({
          reserved_atoms: "100",
          spent_atoms: "0",
        });
        expect(await f.ledger.getSupplierBalance(f.accountB.id, "USD")).toMatchObject({
          reserved_atoms: "60",
          spent_atoms: "0",
        });
        const [before] = await db`
        SELECT count(*)::int AS charges FROM public.billing_charges WHERE request_id = ${requestId}::uuid
      `;
        expect(before?.charges).toBe(0);

        expect(await f.ledger.commitTerminal(terminal)).toMatchObject({
          ok: true,
          replayed: false,
          customer_atoms: "30",
          supplier_cost_atoms: "15",
        });
        expect(await f.ledger.getWallet(f.billingAccount.id, "PTS")).toMatchObject({
          reserved_atoms: "0",
          spent_atoms: "30",
        });
        expect(await f.ledger.getSupplierBalance(f.accountB.id, "USD")).toMatchObject({
          reserved_atoms: "0",
          spent_atoms: "15",
        });
        expect(await f.ledger.auditConservation()).toMatchObject({ ok: true, open_holds: 0 });
      } finally {
        await f.ledger.drain();
      }
    }),
);

test("0001 ledger hot path migration upgrades a 0000 database with pre-existing monthly ledger_entries partitions", () =>
  withLedgerDb(
    async (db) => {
      const initial = migrationManifest.migrations[0]!;
      const initialSql = await Bun.file(
        "packages/ledger/src/migrations/0000_billing_accounts.sql",
      ).text();
      const checksum = new Bun.CryptoHasher("sha256").update(JSON.stringify(initial)).digest("hex");
      await db.begin(async (tx) => {
        await tx.unsafe(initialSql);
        await tx`INSERT INTO public.schema_migrations(version, checksum, manifest) VALUES (${initial.version}, ${checksum}, ${initial}::jsonb)`;
      });
      const now = new Date();
      const fmtMonth = (year: number, monthIndex: number) =>
        `${year}-${String(monthIndex + 1).padStart(2, "0")}`;
      const currentMonth = fmtMonth(now.getUTCFullYear(), now.getUTCMonth());
      const nextDate = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth() + 1, 1));
      const nextMonth = fmtMonth(nextDate.getUTCFullYear(), nextDate.getUTCMonth());
      await prepareLedgerMonth(db, currentMonth);
      await prepareLedgerMonth(db, nextMonth);
      const preTimestamp = `${currentMonth}-15T00:00:00Z`;
      const postTimestamp = `${currentMonth}-16T00:00:00Z`;
      const jid = crypto.randomUUID();
      const entityId = crypto.randomUUID();
      const lotId = crypto.randomUUID();
      await db.begin(async (tx) => {
        await tx`
          INSERT INTO public.ledger_entries(journal_id, ordinal, created_at, side, entity_id, reference, kind, currency, account, atoms)
          VALUES
            (${jid}::uuid, 0, ${preTimestamp}::timestamptz, 'CUSTOMER', ${entityId}::uuid, 'pre-upgrade', 'CREDIT', 'PTS', 'system:funding', -100),
            (${jid}::uuid, 1, ${preTimestamp}::timestamptz, 'CUSTOMER', ${entityId}::uuid, 'pre-upgrade', 'CREDIT', 'PTS', ${`lot:${lotId}:available`}, 100)
        `;
      });

      await migrate(db);
      expect(await assertSchemaVersion(db)).toBe(SCHEMA_VERSION);

      const postJid = crypto.randomUUID();
      await db.begin(async (tx) => {
        await tx`
          INSERT INTO public.ledger_entries(journal_id, ordinal, created_at, side, entity_id, reference, kind, currency, account, atoms)
          VALUES
            (${postJid}::uuid, 0, ${postTimestamp}::timestamptz, 'CUSTOMER', ${entityId}::uuid, 'post-upgrade', 'CREDIT', 'PTS', 'system:funding', -50),
            (${postJid}::uuid, 1, ${postTimestamp}::timestamptz, 'CUSTOMER', ${entityId}::uuid, 'post-upgrade', 'CREDIT', 'PTS', ${`lot:${lotId}:available`}, 50)
        `;
      });
      const badAccountJid = crypto.randomUUID();
      await expect(
        db.begin(async (tx) => {
          await tx`
            INSERT INTO public.ledger_entries(journal_id, ordinal, created_at, side, entity_id, reference, kind, currency, account, atoms)
            VALUES
              (${badAccountJid}::uuid, 0, ${postTimestamp}::timestamptz, 'CUSTOMER', ${entityId}::uuid, 'bad-account', 'CREDIT', 'PTS', 'system:topup', -10),
              (${badAccountJid}::uuid, 1, ${postTimestamp}::timestamptz, 'CUSTOMER', ${entityId}::uuid, 'bad-account', 'CREDIT', 'PTS', ${`lot:${lotId}:available`}, 10)
          `;
        }),
      ).rejects.toThrow();
      const badCurrencyJid = crypto.randomUUID();
      await expect(
        db.begin(async (tx) => {
          await tx`
            INSERT INTO public.ledger_entries(journal_id, ordinal, created_at, side, entity_id, reference, kind, currency, account, atoms)
            VALUES
              (${badCurrencyJid}::uuid, 0, ${postTimestamp}::timestamptz, 'CUSTOMER', ${entityId}::uuid, 'bad-cur', 'CREDIT', 'pts', 'system:funding', -10),
              (${badCurrencyJid}::uuid, 1, ${postTimestamp}::timestamptz, 'CUSTOMER', ${entityId}::uuid, 'bad-cur', 'CREDIT', 'pts', ${`lot:${lotId}:available`}, 10)
          `;
        }),
      ).rejects.toThrow();
    },
    async () => {},
  ));
