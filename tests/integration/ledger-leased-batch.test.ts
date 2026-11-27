import { expect, test } from "bun:test";
import {
  createLedgerEngine,
  type AcquireHoldInput,
  type CommitTerminalInput,
  type PluginAuditEntry,
} from "@gateway/ledger";
import { withLedgerDb, seedFixture } from "./ledger-fixture.ts";

type Fixture = Awaited<ReturnType<typeof seedFixture>>;
type LedgerEngine = ReturnType<typeof createLedgerEngine>;

const CONSERVATION_CLEAN = {
  ok: true,
  unbalanced_journals: 0,
  broken_lots: 0,
  wallet_mismatches: 0,
  reservation_mismatches: 0,
  customer_ledger_mismatches: 0,
  supplier_ledger_mismatches: 0,
  open_holds: 0,
  unknown_holds: 0,
};

function makeCommit(
  f: Fixture,
  requestId: string,
  attemptId: string,
  billingAccountId: string,
  inputTokens = "60",
  outputTokens = "40",
  extra: Partial<CommitTerminalInput> = {},
): CommitTerminalInput {
  return {
    request_id: requestId,
    billing_account_id: billingAccountId,
    selected_attempt_id: attemptId,
    channel_id: f.channelA.id,
    billing_price_id: f.flatBillingPrice.id,
    cost_version_id: f.costPrice.id,
    usage: { input_tokens: inputTokens, output_tokens: outputTokens },
    billing_pricing: { currency: "PTS", rates: f.flatBillingPrice.rates },
    billing_basis: "PROVIDER_USAGE",
    supplier_pricing: { currency: "USD", rates: f.costPrice.rates },
    supplier_currency: "USD",
    ...extra,
  };
}

async function acquireChatHold(
  ledger: LedgerEngine,
  f: Fixture,
  billingAccountId: string,
  expiresAt: string,
  overrides: Partial<AcquireHoldInput> = {},
) {
  const requestId = overrides.request_id ?? crypto.randomUUID();
  const attemptId = overrides.attempt_id ?? crypto.randomUUID();
  const res = await ledger.acquireHold({
    request_id: requestId,
    attempt_id: attemptId,
    billing_account_id: billingAccountId,
    currency: "PTS",
    operation: "chat",
    model: "gpt-4o",
    idempotency_key: null,
    fingerprint: "",
    customer_atoms: "200",
    supplier_account_id: f.accountA.id,
    supplier_channel_id: f.channelA.id,
    supplier_currency: "USD",
    supplier_atoms: "100",
    expires_at: expiresAt,
    ...overrides,
  });
  return { res, requestId, attemptId };
}

async function warmLeases(
  ledger: LedgerEngine,
  f: Fixture,
  billingAccountId: string,
  expiresAt: string,
  count = 16,
) {
  const warm = await Promise.all(
    Array.from({ length: count }, () => acquireChatHold(ledger, f, billingAccountId, expiresAt)),
  );
  expect(warm.every((w) => w.res.ok)).toBe(true);
  await Promise.all(warm.map((w) => ledger.releaseHold(w.requestId)));
}

test("mixed-null typed arrays and special-character JSONB payloads round-trip cleanly on fast and slow leased batch paths", () =>
  withLedgerDb(async (db) => {
    const f = await seedFixture(db);
    const ledger = createLedgerEngine(db, {
      leaseIdleReleaseMs: 60_000,
      leaseTtlMs: 60_000,
      batchWindowMs: 10,
      maxBatchSize: 64,
    });
    const expiry = new Date(Date.now() + 60_000).toISOString();
    const apiKey = await f.catalog.upsertApiKey({
      billing_account_id: f.billingAccount.id,
      name: "mixed-null-key",
      verifier: "v-mixed-null-key",
      prefix: "stg_mix",
    });
    const override = await f.catalog.upsertPricingOverride({
      billing_account_id: f.billingAccount.id,
      model_name: "gpt-4o",
      operation: "chat",
      override_kind: "MULTIPLIER",
      multiplier_numerator: "9",
      multiplier_denominator: "10",
    });
    const specialAudit: PluginAuditEntry[] = [
      {
        plugin_id: 'plug"comma,slash\\brace{NULL}',
        version: "1.0.0",
        hook: "on_response",
        action: "TRANSFORM",
        rule_code: 'r"1,2\\3{4}NULL',
        outcome_kind: "content_decision",
        fail_open: false,
        latency_us: 42,
      },
    ];

    try {
      await f.ledger.creditWallet({
        billing_account_id: f.billingAccount.id,
        currency: "PTS",
        external_ref: "mixed-null-cash",
        amount_atoms: "100000000",
        kind: "CASH",
      });
      await f.ledger.configureWalletCredit({
        billing_account_id: f.billingAccount.id,
        currency: "PTS",
        expected_version: "0",
        credit_limit_atoms: "1000",
      });
      await f.ledger.rechargeSupplier({
        account_id: f.accountA.id,
        currency: "USD",
        external_ref: "mixed-null-sup",
        amount_atoms: "500000000",
      });
      await warmLeases(ledger, f, f.billingAccount.id, expiry);

      const [fastA, fastB] = await Promise.all([
        acquireChatHold(ledger, f, f.billingAccount.id, expiry, {
          api_key_id: null,
          pricing_override_id: override.id,
          discount_numerator: "9",
          discount_denominator: "10",
          supplier_cost_version_id: f.costPrice.id,
          customer_price_version_id: f.flatBillingPrice.id,
        }),
        acquireChatHold(ledger, f, f.billingAccount.id, expiry, {
          api_key_id: apiKey.id,
          pricing_override_id: null,
          discount_numerator: null,
          discount_denominator: null,
          supplier_cost_version_id: f.costPrice.id,
          customer_price_version_id: f.flatBillingPrice.id,
        }),
      ]);
      expect(fastA.res.ok && fastB.res.ok).toBe(true);

      const [fastCommitA, fastCommitB] = await Promise.all([
        ledger.commitTerminal(
          makeCommit(f, fastA.requestId, fastA.attemptId, f.billingAccount.id, "60", "40", {
            api_key_id: null,
            pricing_override_id: override.id,
            discount_numerator: "9",
            discount_denominator: "10",
            plugin_audit: specialAudit,
          }),
        ),
        ledger.commitTerminal(
          makeCommit(f, fastB.requestId, fastB.attemptId, f.billingAccount.id, "70", "30", {
            api_key_id: apiKey.id,
            pricing_override_id: null,
            discount_numerator: null,
            discount_denominator: null,
            plugin_audit: [],
          }),
        ),
      ]);
      expect(fastCommitA.ok && fastCommitB.ok).toBe(true);

      const [walletAfterFast] = await db<{ available_atoms: string; spent_atoms: string }[]>`
        SELECT available_atoms::text, spent_atoms::text
        FROM public.wallets WHERE billing_account_id = ${f.billingAccount.id}::uuid AND currency = 'PTS'
      `;
      expect(walletAfterFast?.spent_atoms).toBe("0");

      await f.ledger.adjustWallet({
        billing_account_id: f.billingAccount.id,
        currency: "PTS",
        external_ref: "mixed-null-force-slow",
        delta_atoms: (-(BigInt(walletAfterFast!.available_atoms) + 10n)).toString(),
        force_debt: true,
        reason: "trigger slow fallback path",
      });

      const [slowA, slowB] = await Promise.all([
        acquireChatHold(ledger, f, f.billingAccount.id, expiry, {
          api_key_id: apiKey.id,
          pricing_override_id: null,
          discount_numerator: null,
          discount_denominator: null,
          supplier_cost_version_id: f.costPrice.id,
          customer_price_version_id: f.flatBillingPrice.id,
        }),
        acquireChatHold(ledger, f, f.billingAccount.id, expiry, {
          api_key_id: null,
          pricing_override_id: override.id,
          discount_numerator: "9",
          discount_denominator: "10",
          supplier_cost_version_id: f.costPrice.id,
          customer_price_version_id: f.flatBillingPrice.id,
        }),
      ]);
      expect(slowA.res.ok && slowB.res.ok).toBe(true);

      const [slowCommitA, slowCommitB] = await Promise.all([
        ledger.commitTerminal(
          makeCommit(f, slowA.requestId, slowA.attemptId, f.billingAccount.id, "55", "45", {
            api_key_id: apiKey.id,
            pricing_override_id: null,
            discount_numerator: null,
            discount_denominator: null,
            plugin_audit: [],
          }),
        ),
        ledger.commitTerminal(
          makeCommit(f, slowB.requestId, slowB.attemptId, f.billingAccount.id, "65", "35", {
            api_key_id: null,
            pricing_override_id: override.id,
            discount_numerator: "9",
            discount_denominator: "10",
            plugin_audit: specialAudit,
          }),
        ),
      ]);
      expect(slowCommitA.ok && slowCommitB.ok).toBe(true);

      const [leaseAfterSlow] = await db<{ state: string; spent_atoms: string }[]>`
        SELECT state, spent_atoms::text FROM public.wallet_leases
        WHERE wallet_id = (SELECT id FROM public.wallets WHERE billing_account_id = ${f.billingAccount.id}::uuid AND currency = 'PTS')
          AND state = 'ACTIVE'
      `;
      expect(leaseAfterSlow).toEqual({ state: "ACTIVE", spent_atoms: "0" });

      const expected = [
        {
          requestId: fastA.requestId,
          apiKeyId: null,
          overrideId: override.id,
          discNum: "9",
          discDen: "10",
          usage: { input_tokens: "60", output_tokens: "40" },
          audit: specialAudit,
        },
        {
          requestId: fastB.requestId,
          apiKeyId: apiKey.id,
          overrideId: null,
          discNum: null,
          discDen: null,
          usage: { input_tokens: "70", output_tokens: "30" },
          audit: [],
        },
        {
          requestId: slowA.requestId,
          apiKeyId: apiKey.id,
          overrideId: null,
          discNum: null,
          discDen: null,
          usage: { input_tokens: "55", output_tokens: "45" },
          audit: [],
        },
        {
          requestId: slowB.requestId,
          apiKeyId: null,
          overrideId: override.id,
          discNum: "9",
          discDen: "10",
          usage: { input_tokens: "65", output_tokens: "35" },
          audit: specialAudit,
        },
      ];

      for (const exp of expected) {
        const [holdRow] = await db<
          {
            sh_type: string;
            supplier_holds: { customer_price_adjustment?: Record<string, string | null> }[];
            api_key_id: string | null;
            pricing_override_id: string | null;
            discount_numerator: string | null;
            discount_denominator: string | null;
          }[]
        >`
          SELECT jsonb_typeof(supplier_holds) AS sh_type, supplier_holds,
            api_key_id::text, pricing_override_id::text,
            discount_numerator::text, discount_denominator::text
          FROM public.billing_holds WHERE request_id = ${exp.requestId}::uuid
        `;
        expect(holdRow?.sh_type).toBe("array");
        expect(holdRow?.api_key_id).toBe(exp.apiKeyId);
        expect(holdRow?.pricing_override_id).toBe(exp.overrideId);
        expect(holdRow?.discount_numerator).toBe(exp.discNum);
        expect(holdRow?.discount_denominator).toBe(exp.discDen);
        expect(holdRow?.supplier_holds[0]?.customer_price_adjustment).toEqual({
          override_id: exp.overrideId,
          discount_numerator: exp.discNum,
          discount_denominator: exp.discDen,
        });

        const [chargeRow] = await db<
          {
            usage_type: string;
            audit_type: string;
            usage: Record<string, string>;
            plugin_audit: PluginAuditEntry[];
            api_key_id: string | null;
            pricing_override_id: string | null;
            discount_numerator: string | null;
            discount_denominator: string | null;
          }[]
        >`
          SELECT jsonb_typeof(usage) AS usage_type, jsonb_typeof(plugin_audit) AS audit_type,
            usage, plugin_audit, api_key_id::text, pricing_override_id::text,
            discount_numerator::text, discount_denominator::text
          FROM public.billing_charges WHERE request_id = ${exp.requestId}::uuid
        `;
        expect(chargeRow?.usage_type).toBe("object");
        expect(chargeRow?.audit_type).toBe("array");
        expect(chargeRow?.usage).toEqual(exp.usage);
        expect(chargeRow?.plugin_audit).toEqual(exp.audit);
        expect(chargeRow?.api_key_id).toBe(exp.apiKeyId);
        expect(chargeRow?.pricing_override_id).toBe(exp.overrideId);
        expect(chargeRow?.discount_numerator).toBe(exp.discNum);
        expect(chargeRow?.discount_denominator).toBe(exp.discDen);

        const [outboxRow] = await db<
          {
            payload_type: string;
            payload_usage_type: string;
            payload: Record<string, unknown>;
          }[]
        >`
          SELECT jsonb_typeof(payload) AS payload_type,
            jsonb_typeof(payload->'usage') AS payload_usage_type,
            payload
          FROM public.outbox_events WHERE request_id = ${exp.requestId}::uuid
        `;
        expect(outboxRow?.payload_type).toBe("object");
        expect(outboxRow?.payload_usage_type).toBe("object");
        expect(outboxRow?.payload.usage).toEqual(exp.usage);
        expect(outboxRow?.payload.api_key_id).toBe(exp.apiKeyId);
        expect(outboxRow?.payload.pricing_override_id).toBe(exp.overrideId);
      }

      expect(await ledger.auditConservation()).toEqual(CONSERVATION_CLEAN);
    } finally {
      await ledger.drain();
      await f.ledger.drain();
    }
  }));

test("commit_leased_batch falls back to slow path when wallets.debt_atoms > 0 and debt adjustments flush active lease spend", () =>
  withLedgerDb(async (db) => {
    const f = await seedFixture(db);
    const ledger = createLedgerEngine(db, {
      leaseIdleReleaseMs: 60_000,
      leaseTtlMs: 60_000,
      batchWindowMs: 1,
    });
    const expiry = new Date(Date.now() + 60_000).toISOString();
    const secondAccount = await f.catalog.upsertBillingAccount({
      name: "debt-fallback-charge-adjust",
      currency: "PTS",
      rpm: 1000,
      tpm: 100000,
      concurrency: 64,
    });

    try {
      await f.ledger.rechargeSupplier({
        account_id: f.accountA.id,
        currency: "USD",
        external_ref: "debt-fallback-sup",
        amount_atoms: "500000000",
      });
      await f.ledger.creditWallet({
        billing_account_id: f.billingAccount.id,
        currency: "PTS",
        external_ref: "debt-fallback-w1",
        amount_atoms: "100000000",
        kind: "CASH",
      });
      await f.ledger.configureWalletCredit({
        billing_account_id: f.billingAccount.id,
        currency: "PTS",
        expected_version: "0",
        credit_limit_atoms: "1000",
      });
      await warmLeases(ledger, f, f.billingAccount.id, expiry);

      const h1 = await acquireChatHold(ledger, f, f.billingAccount.id, expiry);
      expect(h1.res.ok).toBe(true);
      expect(
        await ledger.commitTerminal(makeCommit(f, h1.requestId, h1.attemptId, f.billingAccount.id)),
      ).toMatchObject({ ok: true, customer_atoms: "100" });

      const [w1Before] = await db<
        { available_atoms: string; reserved_atoms: string; spent_atoms: string }[]
      >`
        SELECT available_atoms::text, reserved_atoms::text, spent_atoms::text
        FROM public.wallets WHERE billing_account_id = ${f.billingAccount.id}::uuid AND currency = 'PTS'
      `;
      expect(w1Before?.spent_atoms).toBe("0");

      const drainW1 = BigInt(w1Before!.available_atoms);
      await f.ledger.adjustWallet({
        billing_account_id: f.billingAccount.id,
        currency: "PTS",
        external_ref: "debt-fallback-adjust-wallet",
        delta_atoms: (-(drainW1 + 50n)).toString(),
        force_debt: true,
        reason: "create wallet debt while lease active",
      });

      const [w1AfterAdjust] = await db<
        {
          available_atoms: string;
          reserved_atoms: string;
          spent_atoms: string;
          debt_atoms: string;
        }[]
      >`
        SELECT available_atoms::text, reserved_atoms::text, spent_atoms::text, debt_atoms::text
        FROM public.wallets WHERE billing_account_id = ${f.billingAccount.id}::uuid AND currency = 'PTS'
      `;
      expect(w1AfterAdjust).toEqual({
        available_atoms: "0",
        reserved_atoms: (BigInt(w1Before!.reserved_atoms) - 100n).toString(),
        spent_atoms: (drainW1 + 100n).toString(),
        debt_atoms: "50",
      });

      const h2 = await acquireChatHold(ledger, f, f.billingAccount.id, expiry);
      expect(h2.res.ok).toBe(true);
      expect(
        await ledger.commitTerminal(makeCommit(f, h2.requestId, h2.attemptId, f.billingAccount.id)),
      ).toMatchObject({ ok: true, customer_atoms: "100" });

      const [w1AfterCommit] = await db<
        { reserved_atoms: string; spent_atoms: string; debt_atoms: string }[]
      >`
        SELECT reserved_atoms::text, spent_atoms::text, debt_atoms::text
        FROM public.wallets WHERE billing_account_id = ${f.billingAccount.id}::uuid AND currency = 'PTS'
      `;
      expect(w1AfterCommit).toEqual({
        reserved_atoms: (BigInt(w1AfterAdjust!.reserved_atoms) - 100n).toString(),
        spent_atoms: (BigInt(w1AfterAdjust!.spent_atoms) + 100n).toString(),
        debt_atoms: "50",
      });
      const [wl1State] = await db<{ state: string; spent_atoms: string }[]>`
        SELECT state, spent_atoms::text FROM public.wallet_leases
        WHERE wallet_id = (SELECT id FROM public.wallets WHERE billing_account_id = ${f.billingAccount.id}::uuid AND currency = 'PTS')
          AND state = 'ACTIVE'
      `;
      expect(wl1State).toEqual({ state: "ACTIVE", spent_atoms: "0" });

      await f.ledger.creditWallet({
        billing_account_id: secondAccount.id,
        currency: "PTS",
        external_ref: "debt-fallback-w2",
        amount_atoms: "100000000",
        kind: "CASH",
      });
      await f.ledger.configureWalletCredit({
        billing_account_id: secondAccount.id,
        currency: "PTS",
        expected_version: "0",
        credit_limit_atoms: "1000",
      });
      await warmLeases(ledger, f, secondAccount.id, expiry);

      const [w2Init] = await db<{ available_atoms: string }[]>`
        SELECT available_atoms::text FROM public.wallets
        WHERE billing_account_id = ${secondAccount.id}::uuid AND currency = 'PTS'
      `;
      await f.ledger.adjustWallet({
        billing_account_id: secondAccount.id,
        currency: "PTS",
        external_ref: "debt-fallback-drain-w2",
        delta_atoms: (-BigInt(w2Init!.available_atoms)).toString(),
        reason: "drain available to zero before charge adjust",
      });

      const h3 = await acquireChatHold(ledger, f, secondAccount.id, expiry);
      expect(h3.res.ok).toBe(true);
      const c3 = await ledger.commitTerminal(
        makeCommit(f, h3.requestId, h3.attemptId, secondAccount.id),
      );
      expect(c3.ok).toBe(true);
      if (!c3.ok) throw new Error("unreachable");

      const [wl2BeforeAdj] = await db<{ spent_atoms: string }[]>`
        SELECT spent_atoms::text FROM public.wallet_leases
        WHERE wallet_id = (SELECT id FROM public.wallets WHERE billing_account_id = ${secondAccount.id}::uuid AND currency = 'PTS')
          AND state = 'ACTIVE'
      `;
      expect(wl2BeforeAdj?.spent_atoms).toBe("100");

      await f.ledger.adjustCharge({
        charge_id: c3.charge_id,
        external_ref: "debt-fallback-pos-charge-adjust",
        delta_atoms: "25",
        reason: "positive charge adjustment while lease has deferred spend",
      });

      const [wl2AfterAdj] = await db<
        { state: string; spent_atoms: string; spent_lot_id: string | null }[]
      >`
        SELECT state, spent_atoms::text, spent_lot_id::text FROM public.wallet_leases
        WHERE wallet_id = (SELECT id FROM public.wallets WHERE billing_account_id = ${secondAccount.id}::uuid AND currency = 'PTS')
          AND state = 'ACTIVE'
      `;
      expect(wl2AfterAdj).toEqual({ state: "ACTIVE", spent_atoms: "0", spent_lot_id: null });

      const [w2AfterAdj] = await db<
        { reserved_atoms: string; spent_atoms: string; debt_atoms: string }[]
      >`
        SELECT reserved_atoms::text, spent_atoms::text, debt_atoms::text
        FROM public.wallets WHERE billing_account_id = ${secondAccount.id}::uuid AND currency = 'PTS'
      `;
      expect(w2AfterAdj?.debt_atoms).toBe("25");

      const h4 = await acquireChatHold(ledger, f, secondAccount.id, expiry);
      expect(h4.res.ok).toBe(true);
      expect(
        await ledger.commitTerminal(makeCommit(f, h4.requestId, h4.attemptId, secondAccount.id)),
      ).toMatchObject({ ok: true, customer_atoms: "100" });

      const [w2AfterSlow] = await db<{ reserved_atoms: string; spent_atoms: string }[]>`
        SELECT reserved_atoms::text, spent_atoms::text
        FROM public.wallets WHERE billing_account_id = ${secondAccount.id}::uuid AND currency = 'PTS'
      `;
      expect(w2AfterSlow).toEqual({
        reserved_atoms: (BigInt(w2AfterAdj!.reserved_atoms) - 100n).toString(),
        spent_atoms: (BigInt(w2AfterAdj!.spent_atoms) + 100n).toString(),
      });
      expect(await ledger.auditConservation()).toEqual(CONSERVATION_CLEAN);
    } finally {
      await ledger.drain();
      await f.ledger.drain();
    }
  }));
