import { expect, test } from "bun:test";
import { RedisClient } from "bun";
import type { SignedCoreEvidenceEventV1 } from "@gateway/contracts";
import { supplier } from "../support/core-settlement.ts";
import {
  chatReplies,
  eventually,
  settledCharge,
  withGatewayStack,
} from "../support/gateway-process.ts";

test(
  "a successful JSON response durably queues both signed facts together and settles customer and supplier exactly once",
  () =>
    withGatewayStack({ reply: () => chatReplies.json() }, async (stack) => {
      const { db, namespace, setup, worker, send, received } = stack;
      await worker.stop();
      expect(
        (
          await stack.f.request(
            `/providers/${setup.provider.id}`,
            { concurrency: 1 },
            undefined,
            "PATCH",
          )
        ).status,
      ).toBe(200);
      const response = await send();
      expect(response.status).toBe(200);
      expect(await response.text()).toContain("ok");
      const requestId = response.headers.get("x-request-id")!;
      expect(
        await db`SELECT execution_closed_at FROM gateway_requests WHERE id=${requestId}`,
      ).toEqual([{ execution_closed_at: null }]);
      expect(
        await db`SELECT count(*)::integer n FROM execution_commands WHERE request_id=${requestId} AND kind='execution_quota_v1'`,
      ).toEqual([{ n: 0 }]);
      expect(
        await db`SELECT state,count(*)::integer n FROM execution_quota_leases WHERE request_id=${requestId} GROUP BY state`,
      ).toEqual([{ state: "ACTIVE", n: 3 }]);
      const blocked = await send();
      expect(blocked.status).toBe(429);
      await blocked.text();
      expect(received).toHaveLength(1);
      expect(
        await db`SELECT amount_atoms FROM billing_charges WHERE request_id=${requestId}`,
      ).toHaveLength(0);
      expect(
        (
          await db`SELECT reserved_atoms::text reserved FROM wallets WHERE project_id=${setup.project.id}`
        )[0].reserved,
      ).toBe("100");
      const redis = new RedisClient(process.env.QUEUE_REDIS_URL!);
      const payloads: { version: number; groups: { events: SignedCoreEvidenceEventV1[] }[] }[] = [];
      try {
        for (const key of (await redis.send("KEYS", [`*${namespace}*`])) as string[]) {
          if ((await redis.send("TYPE", [key])) !== "hash") continue;
          const data = await redis.send("HGET", [key, "data"]);
          if (typeof data === "string") payloads.push(JSON.parse(data));
        }
      } finally {
        redis.close();
      }
      expect(payloads).toHaveLength(1);
      expect(payloads[0]!.version).toBe(1);
      expect(payloads[0]!.groups).toHaveLength(1);
      const events = payloads[0]!.groups[0]!.events;
      expect(
        events.map((event) => [
          event.source_revision,
          event.finality,
          event.usage,
          event.request_state,
        ]),
      ).toEqual([
        ["1", "FINAL", { input_tokens: "30", output_tokens: "15" }, undefined],
        ["2", "FINAL", { input_tokens: "30", output_tokens: "15" }, "SUCCEEDED"],
      ]);
      expect(new Set(events.map((event) => event.event_id)).size).toBe(2);
      expect(events[0]!.origin_session_id).toBe(events[1]!.origin_session_id);
      await stack.gateway.stop();
      await stack.startWorker();
      expect(await settledCharge(db, requestId)).toEqual({
        amount: "100",
        state: "SUCCEEDED",
        billing_outcome: "SUCCESS",
      });
      expect(await supplier(db, setup.provider.account_id)).toEqual({
        balance: "985",
        reserved: "0",
        spent: "15",
        state: "ACTIVE",
      });
      const attempt = events[0]!.attempt_id;
      expect(
        (
          await db`SELECT count(*)::integer n FROM core_evidence_receipts WHERE attempt_id=${attempt}`
        )[0].n,
      ).toBe(2);
      expect(
        (
          await db`SELECT count(*)::integer n FROM usage_event_index WHERE attempt_id=${attempt} AND record_kind='RAW'`
        )[0].n,
      ).toBe(2);
      expect(
        await db`SELECT cost_atoms::text cost FROM upstream_costs WHERE attempt_id=${attempt}`,
      ).toEqual([{ cost: "15" }]);
      expect(received).toHaveLength(1);
      await eventually(
        async () =>
          (
            await db`SELECT count(*)::integer n FROM execution_quota_leases WHERE request_id=${requestId} AND state='RELEASED'`
          )[0].n === 3,
        "successful request quota recovery after Gateway exits",
      );
      const nextGateway = await stack.startGateway({}, "gatewayB");
      const next = await send({ via: nextGateway });
      expect(next.status).toBe(200);
      await next.text();
      expect(received).toHaveLength(2);
    }),
  30000,
);
