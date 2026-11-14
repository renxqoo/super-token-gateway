import { expect, test } from "bun:test";
import { resolve } from "node:path";
import type { SQL } from "bun";
import { sha256 } from "@gateway/runtime/crypto";
import { createDatabase } from "@gateway/storage";
import { createSettlementStore } from "@gateway/storage/settlement";
import { identityAudience } from "../support/internal-identity.ts";
import { prepareFinancialAdmission, withFinancial } from "../support/financial-execution.ts";
import { committed, event, granted, type Fixture } from "../support/core-settlement.ts";
import { startCoreWorker, withSettlementRole } from "../support/gateway-process.ts";

async function eventually(check: () => Promise<boolean>, label: string) {
  const end = Date.now() + 20000;
  while (!(await check())) {
    if (Date.now() > end) throw new Error(`Timed out waiting for ${label}`);
    await Bun.sleep(50);
  }
}

function recoveryEnvironment(f: Fixture): Record<string, string> {
  const identity = f.identity,
    core = identity.certificates.get("coreA")!;
  return {
    CORE_ORIGIN: `https://127.0.0.1:${identity.a.port}`,
    CORE_SERVERNAME: "localhost",
    CORE_AUDIENCE: identityAudience,
    CORE_PEER_SHA256: sha256(core.x509.raw),
    CORE_SUBJECT_ALTNAME: core.x509.subjectAltName!,
    CORE_KEY_FILE: resolve(identity.directory, "worker.key"),
    CORE_CERT_FILE: resolve(identity.directory, "worker.pem"),
    CORE_CA_FILE: resolve(identity.directory, "ca.pem"),
    RECONCILE_INTERVAL_MS: "100",
    RECOVERY_INTERVAL_MS: "100",
  };
}

async function wallet(db: SQL, project: string) {
  return (
    await db`SELECT available_atoms::text available,reserved_atoms::text reserved,spent_atoms::text spent FROM wallets WHERE project_id=${project} AND currency='USD'`
  )[0];
}

test("core-worker re-settles a disputed v2 request after an admin RELEASE resolution and returns the customer reservation", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap(),
      client = f.client();
    await client.startup();
    const disputed = await granted(db, setup, client);
    const usage = (input: string) =>
      event(disputed.owner, disputed.prepared.attemptId, {
        source: input === "30" ? "execution" : "execution_outcome",
        finality: "FINAL",
        usage: { input_tokens: input, output_tokens: "15" },
        final_dimensions: ["input_tokens", "output_tokens"],
        request_state: "SUCCEEDED",
      });
    const [group] = await createSettlementStore(db).persist([
      { events: [usage("30"), usage("31")] },
    ]);
    expect(committed(group).receipts.map((r) => r.outcome)).toEqual(["CONFLICT", "CONFLICT"]);
    const held = await wallet(db, setup.project.id);
    expect(held.reserved).not.toBe("0");
    const resolution = await f.request(`/requests/${disputed.prepared.requestId}/resolutions`, {
      external_ref: "write-off",
      action: "RELEASE",
      reason: "ADMIN_WRITE_OFF",
    });
    expect(resolution.status).toBe(201);
    expect((await wallet(db, setup.project.id)).reserved).toBe(held.reserved);
    await withSettlementRole(db, async (databaseUrl) => {
      const worker = await startCoreWorker(f.namespace, {
        databaseUrl,
        env: { RECONCILE_INTERVAL_MS: "100" },
      });
      try {
        await eventually(
          async () =>
            (
              await db`SELECT state FROM billing_holds WHERE request_id=${disputed.prepared.requestId}`
            )[0].state === "RELEASED",
          "core-worker reconciliation",
        );
      } finally {
        expect((await worker.stop()).reconciled).toBe(1);
      }
    });
    expect(await wallet(db, setup.project.id)).toEqual({
      available: "1000",
      reserved: "0",
      spent: "0",
    });
    expect(
      await db`SELECT amount_atoms::text,debt_atoms::text FROM billing_charges WHERE request_id=${disputed.prepared.requestId}`,
    ).toEqual([{ amount_atoms: "0", debt_atoms: "0" }]);
  }));

test("core-worker aborts an expired never-granted reservation through Core and releases customer, supplier and quota holds once", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap(),
      owner = f.client();
    await owner.startup();
    const deadline = new Date(Date.now() + 300).toISOString(),
      prepared = await prepareFinancialAdmission(setup, owner, { deadline });
    await prepared.admit();
    expect((await wallet(db, setup.project.id)).reserved).not.toBe("0");
    await Bun.sleep(Math.max(0, Date.parse(deadline) - Date.now() + 20));
    await withSettlementRole(db, async (databaseUrl) => {
      const worker = await startCoreWorker(f.namespace, {
        databaseUrl,
        env: recoveryEnvironment(f),
      });
      try {
        await eventually(
          async () =>
            (
              await db`SELECT count(*)::integer n FROM execution_quota_leases WHERE request_id=${prepared.requestId} AND state<>'RELEASED'`
            )[0].n === 0,
          "core-worker recovery",
        );
      } finally {
        const stopped = await worker.stop();
        expect(stopped.aborted).toBe(1);
        expect(stopped.released).toBe(3);
      }
    });
    expect(
      (await db`SELECT state FROM execution_permits WHERE attempt_id=${prepared.attemptId}`)[0]
        .state,
    ).toBe("ABORTED");
    expect(await wallet(db, setup.project.id)).toEqual({
      available: "1000",
      reserved: "0",
      spent: "0",
    });
    expect(
      (
        await db`SELECT balance_atoms::text,reserved_atoms::text FROM upstream_account_balances WHERE account_id=${setup.provider.account_id}`
      )[0],
    ).toEqual({ balance_atoms: "1000", reserved_atoms: "0" });
    expect(
      (
        await db`SELECT count(*)::integer n FROM upstream_journals WHERE kind='RELEASE' AND attempt_id=${prepared.attemptId}`
      )[0].n,
    ).toBe(1);
  }));

test("a v2 overrun keeps the full receivable with a DEBT freeze until core-worker collects it from a later cash credit", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap("USD", "1000", 100, "PROVIDER_USAGE"),
      client = f.client();
    await client.startup();
    const overrun = await granted(db, setup, client);
    const [group] = await createSettlementStore(db).persist([
      {
        events: [
          event(overrun.owner, overrun.prepared.attemptId, {
            finality: "FINAL",
            usage: { input_tokens: "600", output_tokens: "300" },
            final_dimensions: ["input_tokens", "output_tokens"],
            attempt_state: "SUCCEEDED",
            request_state: "SUCCEEDED",
          }),
        ],
      },
    ]);
    committed(group);
    expect(
      await db`SELECT amount_atoms::text,funded_atoms::text,debt_atoms::text FROM billing_charges WHERE request_id=${overrun.prepared.requestId}`,
    ).toEqual([{ amount_atoms: "1200", funded_atoms: "1000", debt_atoms: "200" }]);
    expect(
      (
        await db`SELECT available_atoms::text,debt_atoms::text,freeze_reason FROM wallets WHERE project_id=${setup.project.id}`
      )[0],
    ).toEqual({ available_atoms: "0", debt_atoms: "200", freeze_reason: "DEBT" });
    expect(
      (
        await f.request(`/projects/${setup.project.id}/credits`, {
          currency: "USD",
          external_ref: "repay",
          amount_atoms: "450",
          kind: "CASH",
        })
      ).status,
    ).toBe(201);
    await withSettlementRole(db, async (databaseUrl) => {
      const worker = await startCoreWorker(f.namespace, {
        databaseUrl,
        env: { RECONCILE_INTERVAL_MS: "100" },
      });
      try {
        await eventually(
          async () =>
            (await db`SELECT debt_atoms::text FROM wallets WHERE project_id=${setup.project.id}`)[0]
              .debt_atoms === "0",
          "core-worker debt collection",
        );
      } finally {
        await worker.stop();
      }
    });
    expect(
      (
        await db`SELECT available_atoms::text,debt_atoms::text,freeze_reason FROM wallets WHERE project_id=${setup.project.id}`
      )[0],
    ).toEqual({ available_atoms: "250", debt_atoms: "0", freeze_reason: null });
    expect(
      await db`SELECT amount_atoms::text,debt_atoms::text FROM billing_charges WHERE request_id=${overrun.prepared.requestId}`,
    ).toEqual([{ amount_atoms: "1200", debt_atoms: "0" }]);
  }));

test("the settlement worker role reconciles only through the controlled function and cannot touch customer ledgers directly", () =>
  withFinancial(async (_f, db) => {
    await withSettlementRole(db, async (databaseUrl) => {
      const connection = createDatabase(databaseUrl, 1);
      try {
        expect((await connection`SELECT core_reconcile_holds_v1(32) AS settled`)[0].settled).toBe(
          0,
        );
        for (const statement of [
          connection`SELECT settle_requests(ARRAY[${crypto.randomUUID()}::uuid])`,
          connection`SELECT available_atoms FROM wallets LIMIT 1`,
          connection`UPDATE billing_holds SET next_reconcile_at=now()`,
        ])
          await expect(statement).rejects.toMatchObject({ errno: "42501" });
        await expect(connection`SELECT core_reconcile_holds_v1(257)`).rejects.toMatchObject({
          errno: "P0422",
        });
      } finally {
        await connection.close();
      }
    });
  }));
