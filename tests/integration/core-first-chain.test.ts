import { expect, test } from "bun:test";
import type { ExecutionReply } from "@gateway/contracts";
import { createPluginAttempts, createPluginSupervisor, observeAttempt } from "@gateway/plugin-host";
import { UsageProducer, evidenceChannel } from "@gateway/runtime";
import { Store } from "@gateway/storage";
import { createSettlementWorker } from "../../apps/core-api/src/settlement/worker.ts";
import { prepareFinancialAdmission, withFinancial } from "../support/financial-execution.ts";
import { syntheticChatPackage } from "../support/plugin-package.ts";

async function eventually(check: () => Promise<boolean>, label: string) {
  const end = Date.now() + 15000;
  while (!(await check())) {
    if (Date.now() > end) throw new Error(`Timed out waiting for ${label}`);
    await Bun.sleep(25);
  }
}

test("real upstream HTTP through the plugin host is signed, queued and settled for both parties exactly once", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap(),
      client = f.client();
    await client.startup();
    const prepared = await prepareFinancialAdmission(setup, client);
    const admission = (await prepared.admit()) as ExecutionReply;
    const begun = (await client.execution(
      "execution_begin_v1",
      prepared.beginData(admission.receipt),
      { deadlineAt: prepared.deadline },
    )) as ExecutionReply;
    const send = begun.transient_send;
    if (send?.auth_material.type !== "bearer") throw new Error("bearer send authority expected");

    const received: { path: string; authorization: string | null; body: unknown }[] = [];
    const upstream = Bun.serve({
      port: 0,
      async fetch(request) {
        received.push({
          path: new URL(request.url).pathname,
          authorization: request.headers.get("authorization"),
          body: await request.json(),
        });
        return new Response(
          '{"id":"chain","model":"synthetic","choices":[{"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":30,"completion_tokens":15}}',
          { headers: { "content-type": "application/json" } },
        );
      },
    });
    const host = createPluginAttempts(
      createPluginSupervisor({
        executable: new URL("../../.local/plugin-host", import.meta.url).pathname,
      }),
    );
    const options = {
      url: process.env.QUEUE_REDIS_URL!,
      namespace: `core-chain:${crypto.randomUUID()}`,
      replicas: 0,
      timeoutMs: 1000,
    };
    const producer = new UsageProducer({ ...options, channel: evidenceChannel });
    const worker = createSettlementWorker({ ...options, db, concurrency: 1 });
    try {
      await producer.ready();
      await worker.ready();
      const bridge = observeAttempt(
        host.create((await syntheticChatPackage()).pkg, {
          tenant: setup.project.id,
          attempt: prepared.attemptId,
          incarnation: send.incarnation_id,
          config: {
            model_map: { [setup.model]: "synthetic" },
            max_output_tokens: 30,
            max_input_tokens: 10000,
            auth: "bearer",
          },
          deadlineAt: Date.parse(send.deadline_at),
        }),
        {
          attemptId: prepared.attemptId,
          permitEpoch: send.permit_epoch,
          record: (evidence) => producer.publish({ events: [client.signEvidence(evidence)] }),
        },
      );
      try {
        const outbound = await bridge.transformJSON(
          "prepare",
          { protocol: "chat", operation: "chat", model: setup.model },
          new TextEncoder().encode(
            JSON.stringify({ model: setup.model, messages: [{ role: "user", content: "合成" }] }),
          ),
        );
        if (outbound.result.kind !== "prepared" || !outbound.body)
          throw new Error("prepared request expected");
        const response = await fetch(new URL(outbound.result.request.path, upstream.url), {
          method: outbound.result.request.method,
          headers: {
            ...outbound.result.request.headers,
            "content-type": "application/json",
            authorization: `Bearer ${send.auth_material.value}`,
          },
          body: outbound.body.slice(),
        });
        const inbound = await bridge.transformJSON(
          "response",
          { model: setup.model },
          new Uint8Array(await response.arrayBuffer()),
        );
        expect(inbound.result.kind).toBe("output");
        await bridge.finish({ attempt_state: "SUCCEEDED", request_state: "SUCCEEDED" });
      } finally {
        bridge.dispose();
      }
      await eventually(
        async () =>
          Object.values(await producer.stats()).reduce((sum, count) => sum + count, 0) === 0 &&
          (
            await db`SELECT count(*)::integer n FROM billing_charges WHERE request_id=${prepared.requestId}`
          )[0].n === 1,
        "settled acknowledgement",
      );
    } finally {
      await worker.close();
      producer.close();
      await host.close();
      await upstream.stop(true);
    }

    expect(received).toHaveLength(1);
    expect(received[0]!.authorization).toBe("Bearer synthetic-upstream-secret");
    expect(received[0]!.body).toMatchObject({ model: "synthetic", max_completion_tokens: 30 });
    expect(
      await db`SELECT outcome,authentication_disposition,source_revision::text FROM core_evidence_receipts WHERE attempt_id=${prepared.attemptId} ORDER BY source_revision`,
    ).toEqual([
      { outcome: "ACCEPTED", authentication_disposition: "VERIFIED", source_revision: "1" },
      { outcome: "ACCEPTED", authentication_disposition: "VERIFIED", source_revision: "2" },
    ]);
    expect(
      (
        await db`SELECT balance_atoms::text balance,reserved_atoms::text reserved,spent_atoms::text spent FROM upstream_account_balances WHERE account_id=${setup.provider.account_id}`
      )[0],
    ).toEqual({ balance: "985", reserved: "0", spent: "15" });
    expect(
      await db`SELECT amount_atoms::text FROM billing_charges WHERE request_id=${prepared.requestId}`,
    ).toEqual([{ amount_atoms: "100" }]);
    expect(
      (
        await db`SELECT state,billing_outcome,selected_attempt_id FROM gateway_requests WHERE id=${prepared.requestId}`
      )[0],
    ).toEqual({
      state: "SUCCEEDED",
      billing_outcome: "SUCCESS",
      selected_attempt_id: prepared.attemptId,
    });
    expect(
      (await new Store(db).usage(setup.project.id)).map((row) => [
        row.finality,
        row.usage,
        row.cost_atoms,
      ])[0],
    ).toEqual(["FINAL", { input_tokens: "30", output_tokens: "15" }, "15"]);
  }));
