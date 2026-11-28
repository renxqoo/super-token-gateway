import { expect, test } from "bun:test";
import {
  createAsyncTasksRepository,
  createLedgerEngine,
  type AcquireHoldInput,
} from "@gateway/ledger";
import { seedFixture, withLedgerDb } from "./ledger-fixture.ts";

test("channel updates retain same-origin credentials and require a replacement for a new origin", () =>
  withLedgerDb(async (db) => {
    const { catalog, channelA, ledger } = await seedFixture(db);
    const { secret_ciphertext, ...channel } = channelA;
    try {
      expect(
        await catalog.upsertChannel({ ...channel, base_url: "https://a.invalid/v2" }),
      ).toMatchObject({ base_url: "https://a.invalid/v2", secret_ciphertext });
      await expect(
        catalog.upsertChannel({ ...channel, base_url: "https://new.invalid/v2" }),
      ).rejects.toMatchObject({ code: "invalid_channel_secret" });
      expect(
        await catalog.upsertChannel({
          ...channel,
          base_url: "https://new.invalid/v2",
          secret_ciphertext: "new-fixture-credential",
        }),
      ).toMatchObject({
        base_url: "https://new.invalid/v2",
        secret_ciphertext: "new-fixture-credential",
      });
    } finally {
      await ledger.drain();
    }
  }));

test("live Console admission rejects an earlier policy while accepting the current policy", () =>
  withLedgerDb(async (db) => {
    const { catalog, ledger } = await seedFixture(db);
    const orgId = crypto.randomUUID();
    const accountId = crypto.randomUUID();
    const reference = `console:org:${orgId}:billing-account:${accountId}`;
    try {
      const account = await catalog.upsertBillingAccount({
        id: accountId,
        external_ref: reference,
        name: "Console policy",
        rpm: 5,
        tpm: 500,
        concurrency: 2,
      });
      const original = await catalog.upsertApiKey({
        billing_account_id: account.id,
        name: "policy key",
        verifier: "fixture policy verifier",
        prefix: "fixture",
      });
      const updated = await catalog.upsertApiKey({
        ...original,
        source_revision: 1,
        models: ["gpt-4o"],
        operations: ["chat"],
      });
      expect(
        await catalog.getConsoleAdmission(orgId, reference, account.id, original),
      ).toMatchObject({
        billing_account_enabled: true,
        key_enabled: true,
        key_policy_matches: false,
      });
      expect(
        await catalog.getConsoleAdmission(orgId, reference, account.id, updated),
      ).toMatchObject({
        billing_account_enabled: true,
        key_enabled: true,
        key_policy_matches: true,
      });
    } finally {
      await ledger.drain();
    }
  }));

test.each([false, true])(
  "a newly configured key limit governs holds with leases enabled=%s",
  (enableLeases) =>
    withLedgerDb(async (db) => {
      const f = await seedFixture(db);
      const ledger = createLedgerEngine(db, { enableLeases, leaseIdleReleaseMs: 60_000 });
      const held: string[] = [];
      const acquire = async (keyId: string, amount: string) => {
        const input: AcquireHoldInput = {
          request_id: crypto.randomUUID(),
          attempt_id: crypto.randomUUID(),
          billing_account_id: f.billingAccount.id,
          api_key_id: keyId,
          currency: "PTS",
          operation: "chat",
          model: "gpt-4o",
          idempotency_key: null,
          fingerprint: "",
          customer_atoms: amount,
          supplier_account_id: f.accountA.id,
          supplier_channel_id: f.channelA.id,
          supplier_currency: "USD",
          supplier_atoms: "10",
          expires_at: new Date(Date.now() + 60_000).toISOString(),
        };
        const result = await ledger.acquireHold(input);
        if (result.ok) held.push(input.request_id);
        return { result, id: input.request_id };
      };
      try {
        const original = await f.catalog.upsertApiKey({
          billing_account_id: f.billingAccount.id,
          name: "limited key",
          verifier: "limit verifier",
          prefix: "fixture",
        });
        await ledger.creditWallet({
          billing_account_id: f.billingAccount.id,
          currency: "PTS",
          kind: "CASH",
          external_ref: "limit-wallet",
          amount_atoms: "100000000",
        });
        await ledger.rechargeSupplier({
          account_id: f.accountA.id,
          currency: "USD",
          external_ref: "limit-supplier",
          amount_atoms: "500000000",
        });
        const warm = await Promise.all(
          Array.from({ length: 16 }, () => acquire(original.id, "100")),
        );
        expect(warm.every(({ result }) => result.ok)).toBe(true);
        const control = await acquire(original.id, "100");
        expect(control.result.ok).toBe(true);
        if (enableLeases) {
          const [row] =
            await db`SELECT wallet_lease_id FROM public.billing_holds WHERE request_id = ${control.id}`;
          expect(row?.wallet_lease_id).toBeTruthy();
        }
        await f.catalog.upsertApiKey({ ...original, spend_limit_atoms: "20" });
        expect((await acquire(original.id, "21")).result).toMatchObject({
          ok: false,
          code: "key_spend_limit_exceeded",
        });
        expect((await acquire(original.id, "20")).result.ok).toBe(true);
      } finally {
        for (const id of held) await ledger.releaseHold(id);
        await ledger.drain();
        await f.ledger.drain();
      }
    }),
);

test("concurrent video admissions share one account slot through task creation and release", () =>
  withLedgerDb(async (db) => {
    const f = await seedFixture(db);
    const tasks = createAsyncTasksRepository(db);
    const reserve = (requestId: string, limit = 1) =>
      tasks.reserveTaskSlot({
        requestId,
        billingAccountId: f.billingAccount.id,
        limit,
        cancelCooldownMs: 0,
        leaseMs: 60_000,
      });
    try {
      const ids = [crypto.randomUUID(), crypto.randomUUID()];
      const outcomes = await Promise.all(ids.map((id) => reserve(id)));
      expect(outcomes.filter(Boolean)).toHaveLength(1);
      const approved = ids[outcomes.indexOf(true)]!;
      const waiting = ids[outcomes.indexOf(false)]!;
      expect(await reserve(approved)).toBe(true);
      await tasks.releaseTaskSlot(approved, f.billingAccount.id);
      expect(await reserve(waiting)).toBe(true);
      const task = await tasks.registerAdmittedTask({
        id: `vid_${waiting}`,
        request_id: waiting,
        charge_id: null,
        billing_account_id: f.billingAccount.id,
        api_key_id: null,
        channel_id: f.channelA.id,
        model: "fixture",
        operation: "videos",
        upstream_task_id: "provider-fixture",
      });
      expect(task.request_id).toBe(waiting);
      expect(await reserve(approved)).toBe(false);
      expect(await reserve(approved, 2)).toBe(true);
    } finally {
      await f.ledger.drain();
    }
  }));
