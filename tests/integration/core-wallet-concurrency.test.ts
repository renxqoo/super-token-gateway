import { expect, test } from "bun:test";
import { CoreRpcError } from "@gateway/core-client";
import { prepareFinancialAdmission, withFinancial } from "../support/financial-execution.ts";

type Admission = Awaited<ReturnType<typeof prepareFinancialAdmission>>;

async function settle(admission: Admission): Promise<string> {
  for (let tries = 0; ; tries++) {
    const outcome = await admission.admit().then(
      () => "ACCEPTED",
      (error: unknown) => {
        if (!(error instanceof CoreRpcError)) throw error;
        if (error.detail.retryability === "SAME_COMMAND" && tries < 50) return undefined;
        return `${error.status}:${error.detail.code}`;
      },
    );
    if (outcome) return outcome;
    await Bun.sleep(20);
  }
}

test("concurrent v2 admissions spend cash first and never promise more credit than the wallet limit", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap("USD", "1000", 100, "PROVIDER_USAGE"),
      client = f.client();
    await client.startup();
    expect(
      await f.request(`/projects/${setup.project.id}/wallets/USD/credit-policy`, {
        external_ref: "limit",
        expected_version: "0",
        credit_limit_atoms: "150",
      }),
    ).toMatchObject({ status: 200, value: { credit_limit_atoms: "150" } });
    const admissions = await Promise.all(
      Array.from({ length: 9 }, () => prepareFinancialAdmission(setup, client)),
    );
    const results = await Promise.all(admissions.map(settle));
    expect(results.filter((r) => r === "ACCEPTED")).toHaveLength(7);
    expect(results.filter((r) => r !== "ACCEPTED")).toEqual([
      "402:insufficient_funds",
      "402:insufficient_funds",
    ]);
    expect(
      (
        await db`SELECT available_atoms::text available,reserved_atoms::text reserved,credit_reserved_atoms::text credit FROM wallets WHERE project_id=${setup.project.id}`
      )[0],
    ).toEqual({ available: "0", reserved: "1000", credit: "120" });
    expect(
      (
        await db`SELECT count(*)::integer n,sum(amount_atoms)::text amount,sum(credit_atoms)::text credit FROM billing_holds h JOIN wallets w ON w.id=h.wallet_id WHERE w.project_id=${setup.project.id}`
      )[0],
    ).toEqual({ n: 7, amount: "1120", credit: "120" });
  }));

test("a v2 admission for one wallet is not blocked by a transaction holding another wallet", () =>
  withFinancial(async (f, db) => {
    const hot = await f.bootstrap(),
      cold = await f.bootstrap(),
      client = f.client();
    await client.startup();
    const reserved = async (project: string) =>
      (await db`SELECT reserved_atoms::text FROM wallets WHERE project_id=${project}`)[0]
        .reserved_atoms;
    const locked = Promise.withResolvers<void>(),
      release = Promise.withResolvers<void>();
    const holder = db.begin(async (tx) => {
      await tx`SELECT id FROM wallets WHERE project_id=${hot.project.id} FOR UPDATE`;
      locked.resolve();
      await release.promise;
    });
    await locked.promise;
    const busy = await prepareFinancialAdmission(hot, client);
    try {
      const started = performance.now();
      const blocked = await busy.admit().catch((error: unknown) => error);
      expect(blocked).toBeInstanceOf(CoreRpcError);
      expect((blocked as CoreRpcError).detail).toMatchObject({
        code: "identity_busy",
        retryability: "SAME_COMMAND",
      });
      await (await prepareFinancialAdmission(cold, client)).admit();
      expect(performance.now() - started).toBeLessThan(5000);
      expect(await reserved(cold.project.id)).toBe("100");
      expect(await reserved(hot.project.id)).toBe("0");
    } finally {
      release.resolve();
      await holder;
    }
    await busy.admit();
    expect(await reserved(hot.project.id)).toBe("100");
  }));

test("an admission meeting a briefly held wallet lock is reserved after the lock is released instead of being refused with identity_busy", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap(),
      client = f.client();
    await client.startup();
    const locked = Promise.withResolvers<void>(),
      release = Promise.withResolvers<void>();
    const holder = db.begin(async (tx) => {
      await tx`SELECT id FROM wallets WHERE project_id=${setup.project.id} FOR UPDATE`;
      locked.resolve();
      await release.promise;
    });
    await locked.promise;
    const prepared = await prepareFinancialAdmission(setup, client);
    const admission = prepared.admit();
    setTimeout(() => release.resolve(), 300);
    try {
      expect((await admission).receipt.state).toBe("RESERVED");
    } finally {
      release.resolve();
      await holder;
    }
    expect(
      (await db`SELECT reserved_atoms::text FROM wallets WHERE project_id=${setup.project.id}`)[0]
        .reserved_atoms,
    ).toBe("100");
  }));
