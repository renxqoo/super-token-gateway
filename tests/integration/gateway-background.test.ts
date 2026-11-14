import { expect, test } from "bun:test";
import { CoreRpcError } from "@gateway/core-client";
import { createSettlementStore } from "@gateway/storage/settlement";
import {
  allowOperation,
  eventually,
  settledCharge,
  withGatewayStack,
  type UpstreamCall,
} from "../support/gateway-process.ts";

const usage = { input_tokens: 9, output_tokens: 6 };

function response(id: string, status: string) {
  return {
    id,
    object: "response",
    status,
    error: null,
    model: "synthetic",
    output: [],
    usage: status === "completed" ? usage : null,
  };
}

function background(polls: { remaining: number }) {
  const deleted = new Set<string>();
  return (call: UpstreamCall) => {
    if (call.path === "/v1/responses" && call.method === "POST")
      return Response.json(response(`resp_bg_${call.index}`, "queued"));
    const match = /^\/v1\/responses\/(resp_bg_\d+)$/.exec(call.path);
    if (match && call.method === "DELETE") {
      deleted.add(match[1]!);
      return Response.json({ id: match[1], object: "response", deleted: true });
    }
    if (match && call.method === "GET" && !deleted.has(match[1]!))
      return Response.json(
        response(match[1]!, polls.remaining-- > 0 ? "in_progress" : "completed"),
      );
    return new Response("not found", { status: 404 });
  };
}

async function start(stack: Parameters<Parameters<typeof withGatewayStack>[1]>[0]) {
  await allowOperation(stack, "responses");
  const created = await fetch(`${stack.gateway.origin}/v1/responses`, {
    method: "POST",
    headers: {
      authorization: `Bearer ${stack.setup.key.key}`,
      "content-type": "application/json",
    },
    body: JSON.stringify({
      model: stack.setup.model,
      max_output_tokens: 30,
      input: "后台",
      background: true,
    }),
  });
  const requestId = created.headers.get("x-request-id")!,
    id = `response_${requestId.replaceAll("-", "")}`;
  const text = await created.text();
  expect(created.status).toBe(200);
  expect(JSON.parse(text)).toMatchObject({ id, status: "queued", model: stack.setup.model });
  expect(text).not.toContain("resp_bg");
  const attempt = async () =>
    (
      await stack.db`SELECT a.id::text,a.state,p.permit_epoch::text epoch FROM attempts a JOIN execution_permits p ON p.attempt_id=a.id WHERE a.request_id=${requestId}::uuid`
    )[0] as { id: string; state: string; epoch: string };
  await eventually(async () => (await attempt()).state === "PARTIAL", "pending attempt");
  expect(
    (await stack.db`SELECT count(*)::int n FROM billing_charges WHERE request_id=${requestId}`)[0]
      .n,
  ).toBe(0);
  expect((await stack.db`SELECT state FROM resources WHERE id=${id}`)[0].state).toBe("ACTIVE");
  return { requestId, id, attempt: await attempt() };
}

test("a background response is settled from its terminal usage by another Gateway session without the client polling", () => {
  const polls = { remaining: 1 };
  return withGatewayStack(
    { reply: background(polls), supplierCredit: "1000000", gateway: { resource_poll_ms: 600000 } },
    async (stack) => {
      const created = await start(stack);
      const before = stack.received.length;
      await stack.startGateway({ resource_poll_ms: 200 }, "gatewayB");
      expect(await settledCharge(stack.db, created.requestId)).toMatchObject({
        state: "SUCCEEDED",
        billing_outcome: "SUCCESS",
      });
      const polled = stack.received.slice(before);
      expect(polled.length).toBeGreaterThanOrEqual(2);
      for (const call of polled)
        expect(call).toMatchObject({
          method: "GET",
          path: "/v1/responses/resp_bg_0",
          authorization: "Bearer synthetic-upstream-secret",
        });
      expect(
        await stack.db`SELECT r.source,r.origin_identity,r.outcome,r.finality FROM core_evidence_receipts r WHERE r.attempt_id=${created.attempt.id}::uuid AND r.source='async_final'`,
      ).toEqual([
        {
          source: "async_final",
          origin_identity: "gateway:gatewayB",
          outcome: "ACCEPTED",
          finality: "FINAL",
        },
      ]);
      expect(
        (await stack.db`SELECT state FROM attempts WHERE id=${created.attempt.id}::uuid`)[0].state,
      ).toBe("SUCCEEDED");
      expect((await stack.db`SELECT state FROM resources WHERE id=${created.id}`)[0].state).toBe(
        "COMPLETED",
      );
      const calls = stack.received.length;
      await Bun.sleep(1500);
      expect(stack.received.length).toBe(calls);
    },
  );
}, 180000);

test(
  "only the current lease holder of a background response may obtain its credential or sign its terminal usage",
  () =>
    withGatewayStack(
      {
        reply: background({ remaining: 0 }),
        supplierCredit: "1000000",
        gateway: { resource_poll_ms: 600000 },
      },
      async (stack) => {
        const created = await start(stack);
        const holder = stack.f.client(undefined, "gatewayB");
        await holder.startup();
        const store = createSettlementStore(stack.db);
        const final = (revision: string) =>
          holder.signEvidence({
            event_id: crypto.randomUUID(),
            attempt_id: created.attempt.id,
            permit_epoch: created.attempt.epoch,
            source: "async_final",
            source_revision: revision,
            finality: "FINAL",
            usage: { input_tokens: "9", output_tokens: "6" },
            final_dimensions: ["input_tokens", "output_tokens"],
            attempt_state: "SUCCEEDED",
            request_state: "SUCCEEDED",
          });
        expect(await store.persist([{ events: [final("1")] }])).toEqual([
          { status: "REJECTED", error: "evidence_subject_invalid" },
        ]);
        const claim = async () =>
          (
            (await holder.execution(
              "execution_resource_claim_v1",
              { limit: 1 },
              { deadlineAt: new Date(Date.now() + 10000).toISOString() },
            )) as { leases: { resource_id: string; lease_epoch: string }[] }
          ).leases;
        expect((await claim()).map((lease) => [lease.resource_id, lease.lease_epoch])).toEqual([
          [created.id, "1"],
        ]);
        expect(await claim()).toEqual([]);
        await stack.db`UPDATE resources SET poll_until=now()-interval '1 second' WHERE id=${created.id}`;
        expect((await claim()).map((lease) => lease.lease_epoch)).toEqual(["2"]);
        const stale = await holder
          .execution(
            "execution_resource_poll_v1",
            { resource_id: created.id, lease_epoch: "1", auth: { type: "bearer" } },
            {
              commandId: crypto.randomUUID(),
              deadlineAt: new Date(Date.now() + 10000).toISOString(),
            },
          )
          .catch((error: unknown) => error);
        expect(stale).toBeInstanceOf(CoreRpcError);
        expect((stale as CoreRpcError).detail.code).toBe("internal_owner_mismatch");
        expect(await store.persist([{ events: [final("1")] }])).toEqual([
          { status: "REJECTED", error: "evidence_subject_invalid" },
        ]);
        expect(
          (
            await stack.db`SELECT count(*)::int n FROM billing_charges WHERE request_id=${created.requestId}`
          )[0].n,
        ).toBe(0);
        const [accepted] = await store.persist([{ events: [final("2")] }]);
        expect(accepted).toMatchObject({
          status: "COMMITTED",
          receipts: [{ outcome: "ACCEPTED", duplicate: false }],
        });
        expect(await settledCharge(stack.db, created.requestId)).toMatchObject({
          state: "SUCCEEDED",
        });
        await holder.execution(
          "execution_resource_release_v1",
          { resource_id: created.id, lease_epoch: "2", state: "COMPLETED", retry: false },
          { deadlineAt: new Date(Date.now() + 10000).toISOString() },
        );
        expect(
          (await stack.db`SELECT state,poll_until FROM resources WHERE id=${created.id}`)[0],
        ).toEqual({ state: "COMPLETED", poll_until: null });
        await stack.db`UPDATE resources SET next_poll_at=now() WHERE id=${created.id}`;
        expect(await claim()).toEqual([]);
      },
    ),
  180000,
);

test(
  "a background response the client deletes before it finishes closes its request as failed instead of holding funds forever",
  () =>
    withGatewayStack(
      {
        reply: background({ remaining: 1000 }),
        supplierCredit: "1000000",
        gateway: { resource_poll_ms: 600000 },
      },
      async (stack) => {
        const created = await start(stack);
        const deleted = await fetch(`${stack.gateway.origin}/v1/responses/${created.id}`, {
          method: "DELETE",
          headers: { authorization: `Bearer ${stack.setup.key.key}` },
        });
        expect(deleted.status).toBe(200);
        await stack.startGateway({ resource_poll_ms: 200 }, "gatewayB");
        expect(await settledCharge(stack.db, created.requestId)).toMatchObject({
          state: "FAILED",
          billing_outcome: "FAILED",
        });
        expect(
          (
            await stack.db`SELECT state,error_code FROM attempts WHERE id=${created.attempt.id}::uuid`
          )[0],
        ).toEqual({ state: "FAILED", error_code: "resource_deleted" });
        expect((await stack.db`SELECT state FROM resources WHERE id=${created.id}`)[0].state).toBe(
          "DELETED",
        );
      },
    ),
  180000,
);
