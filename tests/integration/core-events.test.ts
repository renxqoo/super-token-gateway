import { expect, test } from "bun:test";
import type { SQL } from "bun";
import { verifier } from "@gateway/runtime/crypto";
import { syntheticPepper } from "../support/catalog.ts";
import { chatReplies, settledCharge, withGatewayStack } from "../support/gateway-process.ts";

async function credential(db: SQL, permissions: string[]) {
  const administrator = crypto.randomUUID(),
    token = `synthetic-events-${crypto.randomUUID()}`;
  await db`INSERT INTO technical_administrators(id,name,permissions) VALUES(${administrator},'Synthetic events administrator',${permissions}::text[])`;
  await db`INSERT INTO administrator_credentials(id,administrator_id,verifier,prefix) VALUES(${crypto.randomUUID()},${administrator},${verifier(token, syntheticPepper)},'synthetic')`;
  return { administrator, token };
}

const eventFields = [
  "attempt_id",
  "cost_atoms",
  "cost_complete",
  "cost_revision",
  "currency",
  "finality",
  "id",
  "project_id",
  "request_id",
  "semantics",
  "seq",
  "snapshot_revision",
  "source",
  "usage",
];

test(
  "a platform consumer replays committed Gateway usage once through Core, keeps unacknowledged events after a stale lease, and receives only digital fields",
  () =>
    withGatewayStack({ reply: () => chatReplies.json("机密回答") }, async (stack) => {
      const { f, db, setup } = stack;
      const requests: string[] = [];
      for (let index = 0; index < 2; index++) {
        const response = await stack.send({ content: `机密提示-${index}` });
        expect(response.status).toBe(200);
        requests.push(response.headers.get("x-request-id")!);
      }
      for (const request of requests) await settledCharge(db, request);
      const consumer = await credential(db, ["events:consume"]);
      const created = await f.request(
        "/consumers",
        { name: "synthetic-replay", replay: true },
        consumer.token,
      );
      expect(created.status).toBe(201);
      const consumerId = String(created.value.id);
      expect(
        await db`SELECT action,actor FROM audit_events WHERE resource_id=${consumerId}`,
      ).toEqual([
        {
          action: "event_consumer.create",
          actor: `technical-admin:${consumer.administrator}`,
        },
      ]);

      const first = await f.request(
        `/consumers/${consumerId}/claim`,
        { limit: 500 },
        consumer.token,
      );
      expect(first.status).toBe(200);
      const events = first.value.events as Record<string, unknown>[];
      const own = events.filter((event) => requests.includes(String(event.request_id)));
      expect(new Set(own.map((event) => event.request_id))).toEqual(new Set(requests));
      for (const event of own) {
        expect(Object.keys(event).sort()).toEqual(eventFields);
        expect(event.project_id).toBe(setup.project.id);
      }
      expect(JSON.stringify(events)).not.toContain("机密");
      const ids = events.map((event) => String(event.id));

      expect(
        (
          await f.request(
            `/consumers/${consumerId}/ack`,
            { lease_token: crypto.randomUUID(), event_ids: ids },
            consumer.token,
          )
        ).value,
      ).toEqual({ acknowledged: 0 });
      await db`UPDATE event_deliveries SET lease_until=now()-interval '1 second' WHERE consumer_id=${consumerId}`;
      const renewed = await f.request(
        `/consumers/${consumerId}/claim`,
        { limit: 500 },
        consumer.token,
      );
      expect((renewed.value.events as { id: string }[]).map((event) => event.id).sort()).toEqual(
        [...ids].sort(),
      );
      expect(
        (
          await f.request(
            `/consumers/${consumerId}/ack`,
            { lease_token: first.value.lease_token, event_ids: ids },
            consumer.token,
          )
        ).value,
      ).toEqual({ acknowledged: 0 });
      expect(
        (
          await f.request(
            `/consumers/${consumerId}/ack`,
            { lease_token: renewed.value.lease_token, event_ids: ids },
            consumer.token,
          )
        ).value,
      ).toEqual({ acknowledged: ids.length });
      expect(
        (await f.request(`/consumers/${consumerId}/claim`, { limit: 500 }, consumer.token)).value
          .events,
      ).toEqual([]);

      const live = await f.request(
        "/consumers",
        { name: "synthetic-live", replay: false },
        consumer.token,
      );
      expect(
        (
          await f.request(
            `/consumers/${String(live.value.id)}/claim`,
            { limit: 500 },
            consumer.token,
          )
        ).value.events,
      ).toEqual([]);
      const next = await stack.send({ content: "机密提示-live" });
      const nextRequest = next.headers.get("x-request-id")!;
      await settledCharge(db, nextRequest);
      const delivered = (
        await f.request(`/consumers/${String(live.value.id)}/claim`, { limit: 500 }, consumer.token)
      ).value.events as { request_id: string }[];
      expect(delivered.map((event) => event.request_id)).toContain(nextRequest);
      expect(delivered.map((event) => event.request_id)).not.toContain(requests[0]);
    }),
  180000,
);

test(
  "event APIs are separated by platform permission and unknown consumers are not found",
  () =>
    withGatewayStack({ reply: () => chatReplies.json("ok") }, async (stack) => {
      const { f, db, setup } = stack;
      const response = await stack.send();
      const requestId = response.headers.get("x-request-id")!;
      await settledCharge(db, requestId);
      const reader = await credential(db, ["events:read"]),
        unrelated = await credential(db, ["customer:read"]);
      const listed = await f.request(
        `/usage?project_id=${setup.project.id}`,
        undefined,
        reader.token,
      );
      expect(listed.status).toBe(200);
      const rows = listed.value.data as { request_id: string }[];
      expect(rows.length).toBeGreaterThan(0);
      expect(new Set(rows.map((row) => row.request_id))).toEqual(new Set([requestId]));
      expect((await f.request(`/usage?before=0`, undefined, reader.token)).status).toBe(400);
      expect((await f.request("/usage", undefined, unrelated.token)).status).toBe(403);
      expect(
        (await f.request("/consumers", { name: "denied", replay: true }, reader.token)).status,
      ).toBe(403);
      expect(
        (await db`SELECT count(*)::integer n FROM event_consumers WHERE name='denied'`)[0].n,
      ).toBe(0);
      expect(
        (await f.request(`/consumers/${crypto.randomUUID()}/claim`, { limit: 1 })).status,
      ).toBe(404);
    }),
  180000,
);
