import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import { RedisClient } from "bun";
import { UsageProducer, evidenceChannel } from "@gateway/runtime";
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
    const namespace = `execution-artifact_${crypto.randomUUID().replaceAll("-", "")}`,
      queue = { url: process.env.QUEUE_REDIS_URL!, namespace, replicas: 0, timeoutMs: 1000 },
      producer = new UsageProducer({ ...queue, channel: evidenceChannel }),
      cwd = await mkdtemp(resolve(tmpdir(), "core-worker-artifact-")),
      url = new URL(process.env.DATABASE_URL!);
    url.pathname = `/${(await db`SELECT current_database() AS name`)[0].name}`;
    const worker = Bun.spawn(["bun", resolve("dist/core-worker/main.js")], {
      cwd,
      env: {
        PATH: process.env.PATH,
        DATABASE_URL: url.toString(),
        QUEUE_REDIS_URL: queue.url,
        REDIS_NAMESPACE: namespace,
        WORKER_PORT: "0",
        SHUTDOWN_GRACE_MS: "5000",
      },
      stdout: "pipe",
      stderr: "pipe",
    });
    const stderr = new Response(worker.stderr).text();
    try {
      const reader = worker.stdout.getReader();
      let output = "";
      while (!output.includes("\n")) {
        const next = await reader.read();
        if (next.done) throw new Error(`Compiled worker exited: ${await stderr}`);
        output += new TextDecoder().decode(next.value);
      }
      const ready = JSON.parse(output.split("\n")[0]!) as { event: string; port: number };
      assert.equal(ready.event, "ready");
      const health = `http://127.0.0.1:${ready.port}`;
      assert.equal((await fetch(`${health}/readyz`)).status, 200);
      checks++;
      await producer.ready();
      await producer.publish({
        events: [
          client.signEvidence({
            event_id: crypto.randomUUID(),
            attempt_id: prepared.attemptId,
            permit_epoch: "1",
            source: "execution",
            source_revision: "1",
            finality: "FINAL",
            usage: { input_tokens: "30", output_tokens: "15" },
            final_dimensions: ["input_tokens", "output_tokens"],
            attempt_state: "SUCCEEDED",
            request_state: "SUCCEEDED",
          }),
        ],
      });
      const end = Date.now() + 15000;
      while (
        (
          await db`SELECT count(*)::integer n FROM billing_charges WHERE request_id=${prepared.requestId}`
        )[0].n === 0
      ) {
        if (Date.now() > end) throw new Error("Compiled worker did not settle the evidence");
        await Bun.sleep(25);
      }
      assert.deepEqual(
        [
          ...(await db`SELECT amount_atoms::text FROM billing_charges WHERE request_id=${prepared.requestId}`),
        ],
        [{ amount_atoms: "100" }],
      );
      checks++;
      assert.deepEqual(
        (
          await db`SELECT balance_atoms::text balance,reserved_atoms::text reserved,spent_atoms::text spent FROM upstream_account_balances WHERE account_id=${setup.provider.account_id}`
        )[0],
        { balance: "985", reserved: "0", spent: "15" },
      );
      checks++;
      worker.kill("SIGTERM");
      assert.equal(await worker.exited, 0);
      let rest = output.slice(output.indexOf("\n") + 1);
      for (;;) {
        const next = await reader.read();
        if (next.done) break;
        rest += new TextDecoder().decode(next.value);
      }
      const stopped = JSON.parse(rest.trim().split("\n").at(-1)!) as Record<string, unknown>;
      assert.equal(stopped.event, "stopped");
      assert.equal(stopped.committed, 1);
      checks++;
      assert.equal(await stderr, "");
      checks++;
    } finally {
      if (worker.exitCode === null) worker.kill("SIGKILL");
      await worker.exited;
      producer.close();
      await rm(cwd, { recursive: true, force: true });
      const redis = new RedisClient(queue.url);
      try {
        const keys = (await redis.send("KEYS", [`*${namespace}*`])) as string[];
        if (keys.length) await redis.send("DEL", keys);
      } finally {
        redis.close();
      }
    }
  },
  {
    entry: "dist/core-api/main.js",
    managementFactory: compiled.createCoreManagementApplication,
  },
);
console.log(`Compiled Core admission and settlement worker artifacts passed ${checks} checks`);
