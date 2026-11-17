import { expect, test } from "bun:test";
import { granted } from "../support/core-settlement.ts";
import { withFinancial } from "../support/financial-execution.ts";

test("request quota reads do bounded work despite unrelated released lease history", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap();
    const client = f.client();
    await client.startup();
    const target = await granted(db, setup, client);
    const peer = await granted(db, setup, client);
    await db`INSERT INTO execution_quota_leases(lease_id,attempt_id,command_id,request_id,scope_kind,scope_id,state,revision,descriptor)
      SELECT 'historical-'||n,l.attempt_id,l.command_id,l.request_id,'channel','historical-'||n,'RELEASED',1,
        l.descriptor||jsonb_build_object('lease_id','historical-'||n,'scope_id','historical-'||n,'state','RELEASED','revision','1')
      FROM execution_quota_leases l CROSS JOIN generate_series(1,30000) n
      WHERE l.attempt_id=${peer.prepared.attemptId} AND l.scope_kind='channel'`;
    await db`ANALYZE execution_quota_leases`;
    const result =
      await db`EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) SELECT l.state,l.attempt_id,ep.permit_epoch::text,l.descriptor
        FROM execution_quota_leases l JOIN execution_permits ep ON ep.attempt_id=l.attempt_id
        WHERE l.request_id=${target.prepared.requestId}`;
    const plan = result[0]["QUERY PLAN"][0].Plan;
    expect(plan["Actual Rows"]).toBe(3);
    expect(plan["Shared Hit Blocks"] + plan["Shared Read Blocks"]).toBeLessThanOrEqual(128);
    expect(
      (
        await db`SELECT count(*)::int n FROM execution_quota_leases WHERE request_id=${peer.prepared.requestId} AND state='RELEASED'`
      )[0].n,
    ).toBe(30000);
  }));
