import { expect, test } from "bun:test";
import { financialRejection } from "../support/financial-rejection.ts";
import { prepareFinancialAdmission, withFinancial } from "../support/financial-execution.ts";

test("short Chat preparation leaves no durable context until admission and a replay cannot create another hold", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap("USD", "1000");
    const client = f.client();
    await client.startup();
    const p = await prepareFinancialAdmission(setup, client, { routeProjection: true });
    const ticket = p.context.route_ticket!;
    expect(typeof ticket).toBe("string");
    expect(
      await db`SELECT 1 FROM execution_contexts WHERE id=${p.context.context_id}`,
    ).toHaveLength(0);
    await expect(
      client.execution(
        "execution_admit_v1",
        { ...p.data, context_id: crypto.randomUUID() },
        { commandId: p.command, deadlineAt: p.deadline, routeTicket: ticket },
      ),
    ).rejects.toMatchObject({ detail: { code: "context_invalid" } });
    const admitted = await p.admit();
    expect(admitted.receipt.state).toBe("RESERVED");
    expect((await p.admit()).receipt).toEqual(admitted.receipt);
    expect(
      await db`SELECT count(*)::int n FROM upstream_holds WHERE attempt_id=${p.attemptId}`,
    ).toEqual([{ n: 1 }]);
    expect(
      await db`SELECT key_id,fingerprint FROM execution_contexts WHERE id=${p.context.context_id}`,
    ).toEqual([{ key_id: setup.key.id, fingerprint: p.data.fingerprint }]);
  }));

test("admission succeeds when the context writer can insert anchors but cannot read them", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap("USD", "1000");
    const client = f.client();
    await client.startup();
    const prepared = await prepareFinancialAdmission(setup, client, { routeProjection: true });
    const owner = `route_writer_${crypto.randomUUID().replaceAll("-", "")}`;
    const helper = "core_funding.bind_chat_context_v1(jsonb,jsonb,jsonb)";
    await db.unsafe(`CREATE ROLE ${owner} NOLOGIN`);
    try {
      await db.unsafe(`GRANT USAGE,CREATE ON SCHEMA core_funding TO ${owner}`);
      await db.unsafe(`GRANT SELECT(id,project_id,verifier) ON api_keys TO ${owner}`);
      await db.unsafe(`GRANT INSERT ON execution_contexts TO ${owner}`);
      await db.unsafe(`ALTER FUNCTION ${helper} OWNER TO ${owner}`);
      expect((await prepared.admit()).receipt.state).toBe("RESERVED");
      const duplicate = await prepareFinancialAdmission(setup, client, {
        intent: prepared.data.intent_digest,
        routeProjection: true,
      });
      const rejected = await financialRejection(
        client.execution(
          "execution_admit_v1",
          {
            ...duplicate.data,
            context_id: prepared.context.context_id,
          },
          {
            commandId: duplicate.command,
            deadlineAt: duplicate.deadline,
            routeTicket: prepared.context.route_ticket!,
          },
        ),
        409,
      );
      expect(rejected.receipt).toMatchObject({
        code: "already_accepted",
        request_id: prepared.requestId,
      });
      expect(await db`SELECT count(*)::int n FROM upstream_holds`).toEqual([{ n: 1 }]);
    } finally {
      await db.unsafe(`ALTER FUNCTION ${helper} OWNER TO CURRENT_USER`);
      await db.unsafe(`DROP OWNED BY ${owner}`);
      await db.unsafe(`DROP ROLE ${owner}`);
    }
  }));
