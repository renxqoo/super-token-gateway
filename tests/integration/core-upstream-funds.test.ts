import { expect, test } from "bun:test";
import type { SQL } from "bun";
import { CoreRpcError } from "@gateway/core-client";
import { verifier } from "@gateway/runtime/crypto";
import { createUpstreamFundingStore } from "@gateway/storage";
import { prepareFinancialAdmission, withFinancial } from "../support/financial-execution.ts";
import { syntheticPepper } from "../support/catalog.ts";

async function administrator(db: SQL, permissions: string[]) {
  const id = crypto.randomUUID(),
    token = `synthetic-upstream-${crypto.randomUUID()}`;
  await db`INSERT INTO technical_administrators(id,name,permissions) VALUES(${id},'Synthetic upstream administrator',${permissions}::text[])`;
  await db`INSERT INTO administrator_credentials(id,administrator_id,verifier,prefix) VALUES(${crypto.randomUUID()},${id},${verifier(token, syntheticPepper)},'synthetic')`;
  return { actor: `technical-admin:${id}`, token };
}

async function upstreamAudits(db: SQL, account: string): Promise<[string, string][]> {
  return (
    await db`SELECT action,actor FROM upstream_audit_events WHERE account_id=${account} ORDER BY created_at,id`
  ).map((row: { action: string; actor: string }) => [row.action, row.actor]);
}

const denied = { status: 403, value: { error: { code: "platform_scope_denied" } } };

test("supplier write guards require the matching lock and keep the admission snapshot immutable", () =>
  withFinancial(async (fixture, db) => {
    const setup = await fixture.bootstrap();
    const client = fixture.client();
    await client.startup();
    const admission = await prepareFinancialAdmission(setup, client);
    await admission.admit();
    const account = setup.provider.account_id;
    const [{ funding_cell: cell }] =
      await db`SELECT funding_cell FROM upstream_holds WHERE attempt_id=${admission.attemptId}`;
    const otherAccount = crypto.randomUUID();
    await expect(
      db.begin(async (tx) => {
        await tx`SELECT core_funding.exclusive_v1(ARRAY[${otherAccount}::uuid])`;
        await tx`UPDATE core_funding.balance_cells SET revision=revision+1 WHERE account_id=${account} AND cell=${cell}`;
      }),
    ).rejects.toMatchObject({ errno: "P0403", message: "funding_fence_required" });
    await expect(
      db.begin(async (tx) => {
        await tx`SELECT core_funding.share_v1(ARRAY[${account}::uuid])`;
        await tx`UPDATE core_funding.balance_cells SET revision_limit=revision_limit-1 WHERE account_id=${account} AND cell=${cell}`;
      }),
    ).rejects.toMatchObject({ errno: "P0F01", message: "funding_outer_transaction_required" });
    await expect(
      db.begin(async (tx) => {
        await tx`SELECT core_funding.exclusive_v1(ARRAY[${account}::uuid])`;
        await tx`UPDATE upstream_holds SET estimated_quantities_digest=NULL WHERE attempt_id=${admission.attemptId}`;
      }),
    ).rejects.toMatchObject({ errno: "P0403", message: "hold_snapshot_immutable" });
    await expect(
      db`UPDATE upstream_holds SET current_reserved_atoms=current_reserved_atoms-1 WHERE attempt_id=${admission.attemptId}`,
    ).rejects.toMatchObject({ errno: "P0403", message: "funding_fence_required" });
    expect(
      await db`SELECT current_reserved_atoms::text reserved,estimated_atoms::text estimated FROM upstream_holds WHERE attempt_id=${admission.attemptId}`,
    ).toEqual([{ reserved: "30", estimated: "30" }]);
  }));

test("Core admin manages supplier brands and upstream accounts under catalog and funding permissions with audit", () =>
  withFinancial(async (f, db) => {
    const reader = await administrator(db, ["funding:read"]),
      manager = await administrator(db, ["catalog:manage"]),
      adjuster = await administrator(db, ["funding:adjust"]);
    const name = `brand-${crypto.randomUUID()}`;
    expect(await f.request("/supplier-brands", { name }, reader.token)).toMatchObject(denied);
    const brand = await f.request("/supplier-brands", { name }, manager.token);
    expect(brand).toMatchObject({ status: 201, value: { name, enabled: true } });
    const brandId = String(brand.value.id);
    expect(await f.request("/supplier-brands", { name }, manager.token)).toMatchObject({
      status: 409,
      value: { error: { code: "upstream_identity_conflict" } },
    });
    const created = await f.request(
      "/upstream-accounts",
      { brand_id: brandId, name: "primary" },
      manager.token,
    );
    expect(created).toMatchObject({
      status: 201,
      value: {
        brand_id: brandId,
        name: "primary",
        mode: "PREPAID",
        state: "ACTIVE",
        revision: "0",
      },
    });
    const account = String(created.value.id);
    expect(await f.request(`/upstream-accounts/${account}`, undefined, reader.token)).toEqual({
      status: 200,
      value: created.value,
    });
    expect(
      (
        (await f.request("/upstream-accounts", undefined, reader.token)).value.data as {
          id: string;
        }[]
      ).some((a) => a.id === account),
    ).toBe(true);
    expect(
      await f.request(
        `/upstream-accounts/${account}`,
        { state: "DISABLED", reason: "supplier maintenance" },
        manager.token,
        "PATCH",
      ),
    ).toMatchObject(denied);
    expect(
      await f.request(
        `/upstream-accounts/${account}`,
        { state: "DISABLED", reason: "supplier maintenance" },
        adjuster.token,
        "PATCH",
      ),
    ).toMatchObject({ status: 200, value: { state: "DISABLED", revision: "1" } });
    expect(
      await f.request(
        `/upstream-accounts/${account}`,
        { state: "ACTIVE", reason: "maintenance over" },
        adjuster.token,
        "PATCH",
      ),
    ).toMatchObject({ status: 200, value: { state: "ACTIVE", revision: "2" } });
    expect(
      await f.request(
        `/upstream-accounts/${account}/adjustments`,
        {
          external_ref: "opening-fee",
          currency: "EUR",
          delta_atoms: "-10",
          expected_revision: "0",
          reason: "fee charged before any recharge",
        },
        adjuster.token,
      ),
    ).toMatchObject({
      status: 201,
      value: { balance_atoms: "-10", balance_revision: "1", account_state: "RECONCILE_REQUIRED" },
    });
    expect(
      await f.request(
        `/upstream-accounts/${account}`,
        { state: "ACTIVE", reason: "try to skip reconciliation" },
        adjuster.token,
        "PATCH",
      ),
    ).toMatchObject({ status: 409, value: { error: { code: "upstream_account_state_locked" } } });
    expect(
      await f.request(`/supplier-brands/${brandId}`, { enabled: false }, manager.token, "PATCH"),
    ).toMatchObject({ status: 200, value: { enabled: false } });
    expect(
      await f.request(
        "/upstream-accounts",
        { brand_id: brandId, name: "secondary" },
        manager.token,
      ),
    ).toMatchObject({ status: 404, value: { error: { code: "supplier_brand_not_found" } } });
    expect(await upstreamAudits(db, account)).toEqual([
      ["ACCOUNT_CREATE", manager.actor],
      ["ACCOUNT_DISABLE", adjuster.actor],
      ["ACCOUNT_ENABLE", adjuster.actor],
      ["ADJUSTMENT", adjuster.actor],
    ]);
  }));

test("a negative adjustment below reservations freezes new admissions until a manual reconciliation resumes the account", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap(),
      client = f.client(),
      account = setup.provider.account_id,
      adjuster = await administrator(db, ["funding:adjust", "funding:read"]);
    await client.startup();
    const first = await prepareFinancialAdmission(setup, client);
    await first.admit();
    const adjustment = {
      external_ref: "supplier-fee",
      currency: "USD",
      delta_atoms: "-980",
      expected_revision: "2",
      reason: "supplier deducted a service fee",
    };
    const adjusted = await f.request(
      `/upstream-accounts/${account}/adjustments`,
      adjustment,
      adjuster.token,
    );
    expect(adjusted).toMatchObject({
      status: 201,
      value: {
        kind: "ADJUSTMENT",
        delta_atoms: "-980",
        balance_atoms: "20",
        balance_revision: "3",
        account_state: "RECONCILE_REQUIRED",
        actor: adjuster.actor,
      },
    });
    expect(
      await f.request(`/upstream-accounts/${account}/adjustments`, adjustment, adjuster.token),
    ).toEqual(adjusted);
    expect(
      await f.request(
        `/upstream-accounts/${account}/adjustments`,
        { ...adjustment, delta_atoms: "-981" },
        adjuster.token,
      ),
    ).toMatchObject({ status: 409, value: { error: { code: "upstream_adjustment_conflict" } } });
    expect(
      await f.request(
        `/upstream-accounts/${account}/adjustments`,
        { ...adjustment, external_ref: "stale" },
        adjuster.token,
      ),
    ).toMatchObject({ status: 409, value: { error: { code: "upstream_revision_conflict" } } });
    const journal = await f.request(
      `/upstream-accounts/${account}/journal?currency=USD`,
      undefined,
      adjuster.token,
    );
    expect((journal.value.data as unknown[])[0]).toMatchObject({
      kind: "ADJUSTMENT",
      operation_id: adjusted.value.operation_id,
      entries: [
        { account: "upstream:available", atoms: "-980" },
        { account: "external:adjustment", atoms: "980" },
      ],
    });
    const holds = await f.request(
      `/upstream-accounts/${account}/holds?currency=USD`,
      undefined,
      adjuster.token,
    );
    expect(holds.value.data).toMatchObject([
      { attempt_id: first.attemptId, state: "HELD", estimated_atoms: "30" },
    ]);
    const frozen = () =>
      prepareFinancialAdmission(setup, client).then(
        () => "ADMITTABLE",
        (error: unknown) => (error as CoreRpcError).detail?.code,
      );
    expect(await frozen()).toBe("no_route");
    expect(
      await f.request(
        `/upstream-accounts/${account}/reconciliations`,
        { currency: "USD", expected_revision: "3", invoice_atoms: "20", resume: true, reason: "x" },
        adjuster.token,
      ),
    ).toMatchObject({ status: 409, value: { error: { code: "upstream_balance_insufficient" } } });
    const observed = await f.request(
      `/upstream-accounts/${account}/reconciliations`,
      {
        currency: "USD",
        expected_revision: "3",
        observed_atoms: "15",
        adjustment_operation_id: adjusted.value.operation_id,
        reason: "supplier console shows 15",
      },
      adjuster.token,
    );
    expect(observed).toMatchObject({
      status: 201,
      value: {
        balance_revision: "3",
        book_atoms: "20",
        observed_atoms: "15",
        invoice_atoms: null,
        difference_atoms: "-5",
        account_state: "RECONCILE_REQUIRED",
        replayed: false,
      },
    });
    expect(
      await f.request(
        `/upstream-accounts/${account}/reconciliations`,
        {
          currency: "USD",
          expected_revision: "3",
          observed_atoms: "15",
          adjustment_operation_id: adjusted.value.operation_id,
          reason: "supplier console shows 15",
        },
        adjuster.token,
      ),
    ).toMatchObject({ status: 200, value: { id: observed.value.id, replayed: true } });
    expect(
      await f.request(
        `/upstream-accounts/${account}/reconciliations`,
        { currency: "USD", expected_revision: "3", observed_atoms: "16", reason: "changed" },
        adjuster.token,
      ),
    ).toMatchObject({
      status: 409,
      value: { error: { code: "upstream_reconciliation_conflict" } },
    });
    expect(
      (
        await db`SELECT balance_atoms::text,reserved_atoms::text,revision::text FROM core_upstream_balances_v1 WHERE account_id=${account}`
      )[0],
    ).toEqual({ balance_atoms: "20", reserved_atoms: "30", revision: "3" });
    const recharge = await f.request(`/upstream-accounts/${account}/recharges`, {
      ...setup.rechargeInput,
      external_ref: "top-up",
      credited_atoms: "500",
    });
    expect(recharge).toMatchObject({ status: 201, value: { balance_atoms: "520" } });
    expect(await frozen()).toBe("no_route");
    expect(
      await f.request(
        `/upstream-accounts/${account}/reconciliations`,
        {
          currency: "USD",
          expected_revision: "4",
          invoice_atoms: "520",
          resume: true,
          reason: "invoice matches the book after top-up",
        },
        adjuster.token,
      ),
    ).toMatchObject({
      status: 201,
      value: { difference_atoms: "0", account_state: "ACTIVE", replayed: false },
    });
    await (await prepareFinancialAdmission(setup, client)).admit();
    expect(
      (
        await db`SELECT balance_atoms::text,reserved_atoms::text FROM core_upstream_balances_v1 WHERE account_id=${account}`
      )[0],
    ).toEqual({ balance_atoms: "520", reserved_atoms: "60" });
    const plus = await f.request(
      `/upstream-accounts/${account}/adjustments`,
      {
        external_ref: "rebate",
        currency: "USD",
        delta_atoms: "5",
        expected_revision: "5",
        reason: "supplier rebate",
      },
      adjuster.token,
    );
    expect(plus).toMatchObject({
      status: 201,
      value: { balance_atoms: "525", balance_revision: "6", account_state: "ACTIVE" },
    });
    expect(
      await f.request(
        `/upstream-accounts/${account}/reconciliations`,
        { currency: "USD", expected_revision: "6", resume: true, reason: "nothing to resume" },
        adjuster.token,
      ),
    ).toMatchObject({
      status: 409,
      value: { error: { code: "upstream_account_not_reconcilable" } },
    });
    expect(
      (
        (await f.request(`/upstream-accounts/${account}/recharges`, undefined, adjuster.token))
          .value.data as { external_ref: string; kind: string }[]
      ).map((r) => [r.kind, r.external_ref]),
    ).toEqual([
      ["RECHARGE", "top-up"],
      ["RECHARGE", setup.rechargeInput.external_ref],
    ]);
    expect(
      (
        (
          await f.request(
            `/upstream-accounts/${account}/reconciliations`,
            undefined,
            adjuster.token,
          )
        ).value.data as { balance_revision: string }[]
      ).map((r) => r.balance_revision),
    ).toEqual(["4", "3"]);
    expect(
      (await upstreamAudits(db, account))
        .filter(([, actor]) => actor === adjuster.actor)
        .map(([action]) => action),
    ).toEqual(["ADJUSTMENT", "RECONCILED", "RECONCILED_RESUME", "ADJUSTMENT"]);
  }));

test("financial voucher download returns the exact immutable bytes only to funding readers", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap(),
      reader = await administrator(db, ["funding:read"]),
      outsider = await administrator(db, ["customer:read"]);
    const download = (token: string) =>
      f.management.request(
        `http://management.test/financial-vouchers/${setup.rechargeInput.voucher_id}`,
        {
          headers: { authorization: `Bearer ${token}` },
        },
      );
    const response = await download(reader.token);
    expect(response.status).toBe(200);
    expect(response.headers.get("content-type")).toBe("application/pdf");
    expect(response.headers.get("content-disposition")).toBe(
      `attachment; filename="voucher-${setup.rechargeInput.voucher_id}"`,
    );
    expect(response.headers.get("x-content-type-options")).toBe("nosniff");
    expect(Buffer.from(await response.arrayBuffer()).toString()).toBe(
      "%PDF-synthetic-financial-fixture",
    );
    const refused = await download(outsider.token);
    expect(refused.status).toBe(403);
    const missing = await f.management.request(
      `http://management.test/financial-vouchers/${crypto.randomUUID()}`,
      { headers: { authorization: `Bearer ${reader.token}` } },
    );
    expect(missing.status).toBe(404);
  }));

test("the management role can adjust and reconcile but cannot rewrite upstream history", () =>
  withFinancial(async (f) => {
    const setup = await f.bootstrap();
    for (const statement of [
      `UPDATE upstream_journals SET kind='RECHARGE' WHERE account_id='${setup.provider.account_id}'`,
      `UPDATE upstream_accounts SET brand_id=brand_id WHERE id='${setup.provider.account_id}'`,
      `DELETE FROM reconciliations`,
      `UPDATE upstream_holds SET state='RELEASED'`,
    ])
      await expect(f.catalog.appDb.unsafe(statement)).rejects.toMatchObject({ errno: "42501" });
  }));

test("a voucher cited only by a reconciliation survives expiry cleanup instead of failing the cleanup batch", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap(),
      account = setup.provider.account_id,
      adjuster = await administrator(db, ["funding:adjust", "funding:recharge"]);
    const upload = async (text: string, ttlMs: number) => {
      const bytes = Buffer.from(`%PDF-${text}`);
      const response = await f.request(
        "/financial-vouchers",
        {
          content_base64: bytes.toString("base64"),
          content_type: "application/pdf",
          digest: new Bun.CryptoHasher("sha256").update(bytes).digest("hex"),
          expires_at: new Date(Date.now() + ttlMs).toISOString(),
        },
        adjuster.token,
      );
      expect(response.status).toBe(201);
      return { id: String(response.value.id), digest: String(response.value.digest) };
    };
    const cited = await upload(`statement-${crypto.randomUUID()}`, 1500),
      stale = await upload(`stale-${crypto.randomUUID()}`, 1500);
    expect(
      await f.request(
        `/upstream-accounts/${account}/reconciliations`,
        {
          currency: "USD",
          expected_revision: "1",
          observed_atoms: "1000",
          voucher_id: cited.id,
          voucher_digest: cited.digest,
          reason: "statement attached",
        },
        adjuster.token,
      ),
    ).toMatchObject({ status: 201, value: { voucher_id: cited.id } });
    await Bun.sleep(1600);
    expect(
      await f.request(
        `/upstream-accounts/${account}/adjustments`,
        {
          external_ref: "late-evidence",
          currency: "USD",
          delta_atoms: "1",
          expected_revision: "1",
          reason: "cites an expired staging voucher",
          voucher_id: stale.id,
          voucher_digest: stale.digest,
        },
        adjuster.token,
      ),
    ).toMatchObject({ status: 409, value: { error: { code: "voucher_expired" } } });
    const removed = await createUpstreamFundingStore(db).cleanupVouchers(1000);
    expect(removed).toContain(stale.id);
    expect(removed).not.toContain(cited.id);
    expect(
      (await db`SELECT count(*)::integer n FROM financial_vouchers WHERE id=${cited.id}`)[0].n,
    ).toBe(1);
  }));
