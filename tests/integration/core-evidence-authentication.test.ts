import { expect, test } from "bun:test";
import type { SQL } from "bun";
import type {
  CoreEvidenceEventV1,
  CoreEvidenceGroupResult,
  ExecutionReply,
} from "@gateway/contracts";
import { Store } from "@gateway/storage";
import { createSettlementStore } from "@gateway/storage/settlement";
import { evidenceProofFields } from "@gateway/contracts";
import { canonical } from "@gateway/kernel";
import { sha256 } from "@gateway/runtime/crypto";
import { prepareFinancialAdmission, withFinancial } from "../support/financial-execution.ts";

type Fixture = Parameters<Parameters<typeof withFinancial>[0]>[0];
type Setup = Awaited<ReturnType<Fixture["bootstrap"]>>;
type Client = ReturnType<Fixture["client"]>;
type EventBody = Omit<
  CoreEvidenceEventV1,
  "origin_identity" | "origin_incarnation" | "origin_session_id"
>;

async function admitted(setup: Setup, client: Client, begin = true) {
  const prepared = await prepareFinancialAdmission(setup, client);
  const admission = (await prepared.admit()) as ExecutionReply;
  if (begin)
    await client.execution("execution_begin_v1", prepared.beginData(admission.receipt), {
      deadlineAt: prepared.deadline,
    });
  return prepared;
}

function body(attempt: string, fields: Partial<EventBody>): EventBody {
  return {
    event_id: crypto.randomUUID(),
    attempt_id: attempt,
    permit_epoch: "1",
    source: "execution",
    source_revision: "1",
    finality: "PARTIAL",
    usage: {},
    final_dimensions: [],
    ...fields,
  };
}

async function supplier(db: SQL, account: string) {
  return (
    await db`SELECT balance_atoms::text balance,reserved_atoms::text reserved,spent_atoms::text spent FROM upstream_account_balances WHERE account_id=${account}`
  )[0];
}

async function owner(db: SQL, attempt: string) {
  return (
    await db`SELECT gateway_instance_id,owner_session_id::text,s.certificate_sha256 FROM execution_permits p JOIN internal_startup_sessions s ON s.session_id=p.owner_session_id WHERE p.attempt_id=${attempt}`
  )[0] as { gateway_instance_id: string; owner_session_id: string; certificate_sha256: string };
}

function outcomes(result: CoreEvidenceGroupResult | undefined) {
  if (result?.status !== "COMMITTED") throw new Error(JSON.stringify(result));
  return result.receipts.map((r) => [r.outcome, r.duplicate]);
}

test("a tampered signature is rejected without claiming the event and the genuine fact still settles", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap(),
      client = f.client();
    await client.startup();
    const prepared = await admitted(setup, client);
    const store = createSettlementStore(db);
    const genuine = client.signEvidence(
      body(prepared.attemptId, {
        usage: { input_tokens: "31", output_tokens: "2" },
        final_dimensions: ["input_tokens"],
      }),
    );
    const tampered = { ...genuine, usage: { input_tokens: "3", output_tokens: "2" } };
    expect(await store.persist([{ events: [tampered] }])).toEqual([
      { status: "REJECTED", error: "evidence_signature_invalid" },
    ]);
    expect(
      (await db`SELECT count(*)::integer n FROM core_evidence_receipts`)[0].n +
        (
          await db`SELECT count(*)::integer n FROM usage_event_index WHERE attempt_id=${prepared.attemptId}`
        )[0].n,
    ).toBe(0);
    expect(await supplier(db, setup.provider.account_id)).toEqual({
      balance: "1000",
      reserved: "30",
      spent: "0",
    });
    expect(outcomes((await store.persist([{ events: [genuine] }]))[0])).toEqual([
      ["ACCEPTED", false],
    ]);
    expect(await supplier(db, setup.provider.account_id)).toEqual({
      balance: "989",
      reserved: "10",
      spent: "11",
    });
  }));

test("a security-revoked session keeps its anchored receipt but quarantines new facts without money", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap(),
      client = f.client();
    await client.startup();
    const prepared = await admitted(setup, client);
    const store = createSettlementStore(db);
    const anchored = client.signEvidence(
      body(prepared.attemptId, {
        usage: { input_tokens: "31", output_tokens: "2" },
        final_dimensions: ["input_tokens"],
      }),
    );
    expect(outcomes((await store.persist([{ events: [anchored] }]))[0])).toEqual([
      ["ACCEPTED", false],
    ]);
    const identity = await owner(db, prepared.attemptId);
    await db`SELECT internal_identity_change_v1('gateway',${identity.gateway_instance_id},NULL,${identity.owner_session_id}::uuid,'SECURITY_REVOKE')`;

    const late = client.signEvidence(
      body(prepared.attemptId, {
        source_revision: "2",
        finality: "FINAL",
        usage: { input_tokens: "31", output_tokens: "2" },
        final_dimensions: ["input_tokens", "output_tokens"],
        attempt_state: "SUCCEEDED",
        request_state: "SUCCEEDED",
      }),
    );
    const [replay, quarantined] = await store.persist([{ events: [anchored] }, { events: [late] }]);
    expect(outcomes(replay)).toEqual([["ACCEPTED", true]]);
    expect(outcomes(quarantined)).toEqual([["QUARANTINED", false]]);
    expect(await supplier(db, setup.provider.account_id)).toEqual({
      balance: "989",
      reserved: "10",
      spent: "11",
    });
    expect(
      (
        await db`SELECT authentication_disposition,authentication_reason,canonical_evidence_id FROM core_evidence_receipts WHERE event_id=${late.event_id}`
      )[0],
    ).toEqual({
      authentication_disposition: "QUARANTINED",
      authentication_reason: "SECURITY_REVOKED",
      canonical_evidence_id: null,
    });
    expect(
      (
        await db`SELECT billing_outcome,selected_attempt_id FROM gateway_requests WHERE id=${prepared.requestId}`
      )[0],
    ).toEqual({ billing_outcome: "PENDING", selected_attempt_id: null });
    expect(
      (await new Store(db).usage(setup.project.id)).map((row) => row.snapshot_revision),
    ).toEqual(["2"]);
  }));

test("an uncommitted certificate revocation makes ingestion busy and the retry after commit is quarantined", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap(),
      client = f.client();
    await client.startup();
    const prepared = await admitted(setup, client);
    const store = createSettlementStore(db);
    const identity = await owner(db, prepared.attemptId);
    const fact = client.signEvidence(
      body(prepared.attemptId, {
        finality: "FINAL",
        usage: { input_tokens: "30", output_tokens: "15" },
        final_dimensions: ["input_tokens", "output_tokens"],
      }),
    );
    let busy: CoreEvidenceGroupResult[] | undefined;
    await db.begin(async (tx) => {
      await tx`SELECT internal_identity_change_v1('gateway',${identity.gateway_instance_id},${identity.certificate_sha256},NULL,'SECURITY_REVOKE')`;
      busy = await store.persist([{ events: [fact] }]);
    });
    expect(busy).toEqual([{ status: "ERROR", error: "55P03" }]);
    expect(
      (
        await db`SELECT count(*)::integer n FROM core_evidence_receipts WHERE event_id=${fact.event_id}`
      )[0].n,
    ).toBe(0);
    expect(outcomes((await store.persist([{ events: [fact] }]))[0])).toEqual([
      ["QUARANTINED", false],
    ]);
    expect(await supplier(db, setup.provider.account_id)).toEqual({
      balance: "1000",
      reserved: "30",
      spent: "0",
    });
  }));

test("usage for a permit that was never granted is quarantined and keeps the reservation", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap(),
      client = f.client();
    await client.startup();
    const prepared = await admitted(setup, client, false);
    const fact = client.signEvidence(
      body(prepared.attemptId, {
        finality: "FINAL",
        usage: { input_tokens: "3", output_tokens: "3" },
        final_dimensions: ["input_tokens", "output_tokens"],
      }),
    );
    expect(outcomes((await createSettlementStore(db).persist([{ events: [fact] }]))[0])).toEqual([
      ["QUARANTINED", false],
    ]);
    expect(
      (
        await db`SELECT authentication_reason FROM core_evidence_receipts WHERE event_id=${fact.event_id}`
      )[0].authentication_reason,
    ).toBe("UNGRANTED");
    expect(await supplier(db, setup.provider.account_id)).toEqual({
      balance: "1000",
      reserved: "30",
      spent: "0",
    });
  }));

test("an in-group duplicate of a contested fact returns the contested receipt instead of a missing one", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap(),
      client = f.client();
    await client.startup();
    const prepared = await admitted(setup, client);
    const first = client.signEvidence(
      body(prepared.attemptId, {
        finality: "FINAL",
        usage: { input_tokens: "30", output_tokens: "15" },
        final_dimensions: ["input_tokens", "output_tokens"],
      }),
    );
    const contradiction = client.signEvidence(
      body(prepared.attemptId, {
        source: "execution_outcome",
        finality: "FINAL",
        usage: { input_tokens: "31", output_tokens: "15" },
        final_dimensions: ["input_tokens", "output_tokens"],
      }),
    );
    const [result] = await createSettlementStore(db).persist([
      { events: [first, contradiction, first] },
    ]);
    if (result?.status !== "COMMITTED") throw new Error(JSON.stringify(result));
    const firstReceipts = result.receipts.filter((r) => r.event_id === first.event_id);
    expect(firstReceipts.map((r) => [r.outcome, r.duplicate]).sort()).toEqual([
      ["CONFLICT", false],
      ["CONFLICT", true],
    ]);
    expect(new Set(firstReceipts.map((r) => r.receipt_id)).size).toBe(1);
    expect(result.receipts.find((r) => r.event_id === contradiction.event_id)?.outcome).toBe(
      "CONFLICT",
    );
  }));

async function verifiedPayload(db: SQL, fact: ReturnType<Client["signEvidence"]>) {
  const { signature, ...signedBody } = fact;
  const proof = JSON.stringify(evidenceProofFields(signedBody, sha256(canonical(signedBody))));
  const key = (
    await db`SELECT public_key_sha256 FROM internal_startup_sessions WHERE session_id=${fact.origin_session_id}`
  )[0].public_key_sha256 as string;
  return {
    ...signedBody,
    signature: Buffer.from(signature, "base64url").toString("hex"),
    signature_payload_digest: sha256(proof),
    producer_key_sha256: key,
  };
}

test("an instance revocation waits for ingestion that already holds the identity share lock and only later facts are quarantined", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap(),
      client = f.client();
    await client.startup();
    const prepared = await admitted(setup, client);
    const identity = await owner(db, prepared.attemptId);
    const first = client.signEvidence(
      body(prepared.attemptId, {
        usage: { input_tokens: "31", output_tokens: "2" },
        final_dimensions: ["input_tokens"],
      }),
    );
    const payload = await verifiedPayload(db, first);
    let release!: () => void, ingested!: () => void;
    const gate = new Promise<void>((resolve) => (release = resolve));
    const holding = new Promise<void>((resolve) => (ingested = resolve));
    let result: unknown;
    const ingestion = db.begin(async (tx) => {
      result = (
        await tx`SELECT persist_core_evidence_groups_v1(${[{ events: [payload] }]}::jsonb) value`
      )[0].value;
      ingested();
      await gate;
    });
    await holding;
    let revoked = false;
    const revocation =
      db`SELECT internal_identity_change_v1('gateway',${identity.gateway_instance_id},NULL,NULL,'SECURITY_REVOKE')`.then(
        () => {
          revoked = true;
        },
      );
    await Bun.sleep(300);
    const waited = !revoked;
    release();
    await ingestion;
    await revocation;
    expect(waited).toBe(true);
    expect(outcomes((result as CoreEvidenceGroupResult[])[0])).toEqual([["ACCEPTED", false]]);
    expect(
      (
        await db`SELECT security_revoked_at IS NOT NULL revoked FROM internal_service_instances WHERE service='gateway' AND instance_id=${identity.gateway_instance_id}`
      )[0].revoked,
    ).toBe(true);
    const store = createSettlementStore(db);
    const late = client.signEvidence(
      body(prepared.attemptId, {
        source_revision: "2",
        finality: "FINAL",
        usage: { input_tokens: "31", output_tokens: "2" },
        final_dimensions: ["input_tokens", "output_tokens"],
        attempt_state: "SUCCEEDED",
        request_state: "SUCCEEDED",
      }),
    );
    const [replay, quarantined] = await store.persist([{ events: [first] }, { events: [late] }]);
    expect(outcomes(replay)).toEqual([["ACCEPTED", true]]);
    expect(outcomes(quarantined)).toEqual([["QUARANTINED", false]]);
    expect(
      (
        await db`SELECT authentication_reason FROM core_evidence_receipts WHERE event_id=${late.event_id}`
      )[0].authentication_reason,
    ).toBe("SECURITY_REVOKED");
    expect(await supplier(db, setup.provider.account_id)).toEqual({
      balance: "989",
      reserved: "10",
      spent: "11",
    });
  }));

test("a group with facts from two producers settles both together and rolls back whole when either identity is busy", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap(),
      a = f.client(),
      b = f.client(undefined, "gatewayB");
    await a.startup();
    await b.startup();
    const first = await admitted(setup, a),
      second = await admitted(setup, b);
    const fact = (client: Client, attempt: string) =>
      client.signEvidence(
        body(attempt, {
          finality: "FINAL",
          usage: { input_tokens: "30", output_tokens: "15" },
          final_dimensions: ["input_tokens", "output_tokens"],
        }),
      );
    const group = { events: [fact(b, second.attemptId), fact(a, first.attemptId)] };
    const peer = await owner(db, second.attemptId);
    const store = createSettlementStore(db);
    const held = new Error("release certificate lock");
    let busy: CoreEvidenceGroupResult[] | undefined;
    await db
      .begin(async (tx) => {
        await tx`SELECT 1 FROM internal_service_certificates WHERE certificate_sha256=${peer.certificate_sha256} FOR UPDATE`;
        busy = await store.persist([group]);
        throw held;
      })
      .catch((error) => {
        if (error !== held) throw error;
      });
    expect(busy).toEqual([{ status: "ERROR", error: "55P03" }]);
    expect(
      (
        await db`SELECT count(*)::integer n FROM core_evidence_receipts WHERE attempt_id IN (${first.attemptId},${second.attemptId})`
      )[0].n,
    ).toBe(0);
    expect(await supplier(db, setup.provider.account_id)).toEqual({
      balance: "1000",
      reserved: "60",
      spent: "0",
    });
    expect(outcomes((await store.persist([group]))[0])).toEqual([
      ["ACCEPTED", false],
      ["ACCEPTED", false],
    ]);
    expect(await supplier(db, setup.provider.account_id)).toEqual({
      balance: "970",
      reserved: "0",
      spent: "30",
    });
  }));
