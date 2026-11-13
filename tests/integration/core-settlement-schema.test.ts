import { expect, test } from "bun:test";
import type { SQL } from "bun";
import { Store } from "@gateway/storage";
import { prepareFinancialAdmission, withFinancial } from "../support/financial-execution.ts";

const digest = (seed: string) => new Bun.CryptoHasher("sha256").update(seed).digest("hex");

async function insertSnapshot(
  db: SQL,
  attempt: string,
  row: {
    kind: "RAW" | "CANONICAL" | "DISPUTED";
    revision: string;
    cost?: string | null;
    parent?: string | null;
    input?: string | null;
    source?: string;
  },
) {
  const id = crypto.randomUUID();
  await db.begin(async (tx) => {
    await tx`INSERT INTO usage_event_index(id,project_id,request_id,attempt_id,source,finality,cost_atoms,currency,cost_complete,record_kind,snapshot_revision,snapshot_digest,final_dimensions,parent_evidence_id,input_fact_id)
      SELECT ${id},q.project_id,q.id,a.id,${row.source ?? "execution"},'PARTIAL',${row.cost ?? null}::numeric,'USD',false,${row.kind},${row.revision}::numeric,${digest(id)},ARRAY['requests']::text[],${row.parent ?? null}::uuid,${row.input ?? null}::uuid
      FROM attempts a JOIN gateway_requests q ON q.id=a.request_id WHERE a.id=${attempt}`;
    await tx`INSERT INTO usage_payloads(id,created_at,usage) SELECT id,created_at,${{ requests: "1" }}::jsonb FROM usage_event_index WHERE id=${id}`;
  });
  return id;
}

test("core attempts accept only signed snapshot lineage and public usage queries never expose RAW", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap(),
      client = f.client();
    await client.startup();
    const prepared = await prepareFinancialAdmission(setup, client);
    await prepared.admit();
    const attempt = prepared.attemptId;

    const direct = await db
      .begin(
        (tx) =>
          tx`INSERT INTO usage_events(id,project_id,request_id,attempt_id,source,finality,usage,cost_atoms,currency) SELECT ${crypto.randomUUID()},q.project_id,q.id,a.id,'execution','FINAL','{"requests":"1"}',1,'USD' FROM attempts a JOIN gateway_requests q ON q.id=a.request_id WHERE a.id=${attempt}`,
      )
      .catch((error) => error as Error);
    expect((direct as Error).message).toContain("core_attempt_requires_signed_evidence");

    const raw = await insertSnapshot(db, attempt, { kind: "RAW", revision: "1" });
    const canonical = await insertSnapshot(db, attempt, {
      kind: "CANONICAL",
      revision: "2",
      cost: "0",
      input: raw,
    });
    const secondRaw = await insertSnapshot(db, attempt, { kind: "RAW", revision: "3" });

    for (const invalid of [
      { kind: "CANONICAL" as const, revision: "4", cost: "0", input: raw, parent: secondRaw },
      { kind: "CANONICAL" as const, revision: "4", cost: "0", input: canonical },
      { kind: "CANONICAL" as const, revision: "2", cost: "0", input: raw, parent: null },
      { kind: "DISPUTED" as const, revision: "5", cost: null, input: secondRaw, parent: secondRaw },
    ]) {
      const failure = await insertSnapshot(db, attempt, invalid).catch((error) => error as Error);
      expect(failure).toBeInstanceOf(Error);
    }
    const reused = await insertSnapshot(db, attempt, {
      kind: "CANONICAL",
      revision: "2",
      cost: "0",
      input: raw,
    }).catch((error) => error);
    expect(reused).toMatchObject({ errno: "23505" });
    const unsupported = await insertSnapshot(db, attempt, {
      kind: "RAW",
      revision: "9",
      source: "execution_9f0a",
    }).catch((error) => error as Error);
    expect((unsupported as Error).message).toContain("evidence_snapshot_invalid");

    const disputed = await insertSnapshot(db, attempt, {
      kind: "DISPUTED",
      revision: "4",
      cost: null,
      input: secondRaw,
      parent: canonical,
    });
    expect(
      (
        await db`SELECT id,record_kind FROM usage_events WHERE attempt_id=${attempt} ORDER BY seq`
      ).map((row: { id: string; record_kind: string }) => [row.id, row.record_kind]),
    ).toEqual([
      [canonical, "CANONICAL"],
      [disputed, "DISPUTED"],
    ]);
    const store = new Store(db),
      project = String(
        (await db`SELECT project_id FROM gateway_requests WHERE id=${prepared.requestId}`)[0]
          .project_id,
      );
    const published = await store.usage(project);
    expect(published.map((row) => row.id)).toEqual([canonical, disputed]);
    const request = (await store.request(prepared.requestId, project)) as {
      usage: { attempt_id: string }[];
    };
    expect(request.usage).toHaveLength(2);

    const update =
      await db`UPDATE usage_event_index SET snapshot_digest=${digest("x")} WHERE id=${raw}`.catch(
        (error) => error,
      );
    expect(update).toBeInstanceOf(Error);
  }));
