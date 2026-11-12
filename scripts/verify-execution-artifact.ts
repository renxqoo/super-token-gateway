import assert from "node:assert/strict";
import { resolve } from "node:path";
import type { ExecutionReply } from "@gateway/contracts";
import type { createCoreManagementApplication } from "../apps/core-api/src/platform/management.ts";
import { withFinancial, prepareFinancialAdmission } from "../tests/support/financial-execution.ts";
const compiled = (await import(resolve("dist/core-api/platform/management.js"))) as {
  createCoreManagementApplication: typeof createCoreManagementApplication;
};
assert.equal(typeof compiled.createCoreManagementApplication, "function");
let checks = 1;
await withFinancial(
  async (f, db) => {
    const setup = await f.bootstrap(),
      client = f.client();
    await client.startup();
    const prepared = await prepareFinancialAdmission(setup, client),
      admitted = await prepared.admit();
    assert.equal(admitted.receipt.state, "RESERVED");
    checks++;
    assert.equal(admitted.receipt.supplier_atoms, "30");
    checks++;
    assert.equal(admitted.receipt.customer_atoms, "100");
    checks++;
    const command = crypto.randomUUID(),
      begin = (await client.execution("execution_begin_v1", prepared.beginData(admitted.receipt), {
        commandId: command,
        deadlineAt: prepared.deadline,
      })) as ExecutionReply;
    assert.equal(begin.transient_send?.auth_material.type, "bearer");
    checks++;
    const replay = (await client.execution(
      "execution_begin_v1",
      prepared.beginData(admitted.receipt),
      { commandId: command, deadlineAt: prepared.deadline },
    )) as ExecutionReply;
    assert.equal(replay.transient_send, undefined);
    checks++;
    assert.equal(replay.receipt.state, "SEND_GRANTED");
    checks++;
    await client.execution(
      "execution_quota_v1",
      { attempt_id: prepared.attemptId, permit_epoch: "1", action: "CLOSE", reason: "COMPLETED" },
      { deadlineAt: prepared.deadline },
    );
    assert.equal(
      (
        await db`SELECT reserved_atoms::text FROM upstream_account_balances WHERE account_id=${setup.provider.account_id}`
      )[0].reserved_atoms,
      "30",
    );
    checks++;
    assert.equal(
      (await db`SELECT count(*)::integer n FROM execution_quota_leases WHERE state='RELEASED'`)[0]
        .n,
      3,
    );
    checks++;
  },
  {
    entry: "dist/core-api/identity-main.js",
    managementFactory: compiled.createCoreManagementApplication,
  },
);
console.log(`Compiled Core financial admission artifact passed ${checks} checks`);
