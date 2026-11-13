import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import type { createCoreManagementApplication } from "../apps/core-api/src/platform/management.ts";
import { withFinancial } from "../tests/support/financial-execution.ts";
import {
  clearQueueNamespace,
  eventually,
  startCoreWorker,
  startGateway,
  startTlsUpstream,
  withSettlementRole,
} from "../tests/support/gateway-process.ts";

const compiled = (await import(resolve("dist/core-api/platform/management.js"))) as {
  createCoreManagementApplication: typeof createCoreManagementApplication;
};
const usage = { prompt_tokens: 30, completion_tokens: 15 };
const json = JSON.stringify({
  id: "artifact",
  model: "synthetic",
  choices: [{ message: { role: "assistant", content: "ok" }, finish_reason: "stop" }],
  usage,
});
const sse = [
  { id: "artifact", model: "synthetic", choices: [{ index: 0, delta: { content: "ok" } }] },
  {
    id: "artifact",
    model: "synthetic",
    choices: [{ index: 0, delta: {}, finish_reason: "stop" }],
  },
  { id: "artifact", model: "synthetic", choices: [], usage },
]
  .map((chunk) => `data: ${JSON.stringify(chunk)}\n\n`)
  .concat("data: [DONE]\n\n")
  .join("");
let checks = 0;
await withFinancial(
  (f, db) =>
    withSettlementRole(db, async (workerDatabaseUrl) => {
      const upstream = await startTlsUpstream(async (request) => {
        const body = (await request.json()) as { stream?: boolean };
        return new Response(body.stream ? sse : json, {
          headers: { "content-type": body.stream ? "text/event-stream" : "application/json" },
        });
      });
      const namespace = `gateway-artifact_${crypto.randomUUID().replaceAll("-", "")}`,
        cwd = await mkdtemp(resolve(tmpdir(), "gateway-artifact-"));
      try {
        const setup = await f.bootstrap(
          undefined,
          undefined,
          undefined,
          undefined,
          {
            rates: {
              input_tokens: { numerator: "1", denominator: "3" },
              output_tokens: { numerator: "1", denominator: "3" },
            },
            ceilings: { input_tokens: "1000", output_tokens: "50" },
          },
          { baseUrl: upstream.baseUrl, maxInputTokens: 1000 },
        );
        const worker = await startCoreWorker(namespace, {
          databaseUrl: workerDatabaseUrl,
          entry: "dist/core-worker/main.js",
          cwd,
        });
        const gateway = await startGateway(f, {
          namespace,
          upstreamCertificate: upstream.certificate,
          entry: "dist/gateway/main.js",
          cwd,
        });
        try {
          assert.equal((await fetch(`${gateway.origin}/readyz`)).status, 200);
          checks++;
          for (const stream of [false, true]) {
            const response = await fetch(`${gateway.origin}/v1/chat/completions`, {
              method: "POST",
              headers: {
                authorization: `Bearer ${setup.key.key}`,
                "content-type": "application/json",
              },
              body: JSON.stringify({
                model: setup.model,
                messages: [{ role: "user", content: "artifact" }],
                max_completion_tokens: 30,
                ...(stream ? { stream: true } : {}),
              }),
            });
            const text = await response.text();
            assert.equal(response.status, 200, text);
            assert.ok(stream ? text.trimEnd().endsWith("data: [DONE]") : text.includes('"ok"'));
            checks++;
            const requestId = response.headers.get("x-request-id")!;
            await eventually(
              async () =>
                (
                  await db`SELECT count(*)::integer n FROM billing_charges WHERE request_id=${requestId}`
                )[0].n === 1,
              "customer charge",
            );
            assert.deepEqual(
              [
                ...(await db`SELECT amount_atoms::text FROM billing_charges WHERE request_id=${requestId}`),
              ],
              [{ amount_atoms: "100" }],
            );
            checks++;
          }
          assert.deepEqual(
            (
              await db`SELECT balance_atoms::text balance,reserved_atoms::text reserved,spent_atoms::text spent FROM upstream_account_balances WHERE account_id=${setup.provider.account_id}`
            )[0],
            { balance: "970", reserved: "0", spent: "30" },
          );
          checks++;
          assert.equal(
            (
              await fetch(`${gateway.origin}/v1/chat/completions`, {
                method: "POST",
                headers: { authorization: "Bearer not-a-key", "content-type": "application/json" },
                body: "{}",
              })
            ).status,
            401,
          );
          checks++;
          assert.equal((await gateway.stop()).event, "stopped");
          checks++;
          assert.equal(await gateway.stderr(), "");
          checks++;
          assert.equal((await worker.stop()).event, "stopped");
          checks++;
        } finally {
          gateway.kill();
          worker.kill();
        }
      } finally {
        await upstream.stop();
        await clearQueueNamespace(namespace);
        await rm(cwd, { recursive: true, force: true });
      }
    }),
  {
    entry: "dist/core-api/main.js",
    managementFactory: compiled.createCoreManagementApplication,
  },
);
console.log(`Compiled Gateway, Core API and settlement worker artifacts passed ${checks} checks`);
