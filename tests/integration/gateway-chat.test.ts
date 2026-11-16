import { expect, test } from "bun:test";
import { RedisClient, SQL } from "bun";
import { request } from "node:http";
import {
  boundedSupplier,
  chatReplies,
  eventually,
  requestOutcome,
  settledCharge,
  stall,
  withGatewayStack,
} from "../support/gateway-process.ts";

async function expectStatus(response: Response, status: number) {
  const text = await response.text();
  expect({ status: response.status, text: response.status === status ? "" : text }).toEqual({
    status,
    text: "",
  });
  return text;
}

async function supplier(db: SQL, account: string) {
  return (
    await db`SELECT balance_atoms::text balance,reserved_atoms::text reserved,spent_atoms::text spent FROM upstream_account_balances WHERE account_id=${account}`
  )[0];
}

const chat = (stream: boolean) =>
  withGatewayStack(
    { reply: (call) => (call.body.stream ? chatReplies.sse() : chatReplies.json()) },
    async ({ db, setup, received, send }) => {
      const response = await send({ stream });
      const text = await expectStatus(response, 200);
      if (stream) {
        expect(response.headers.get("content-type")).toContain("text/event-stream");
        expect(text).toContain('"content":"o"');
        expect(text.trimEnd().endsWith("data: [DONE]")).toBe(true);
      } else {
        const body = JSON.parse(text);
        expect(body.model).toBe(setup.model);
        expect(body.choices[0].message.content).toBe("ok");
      }
      const requestId = response.headers.get("x-request-id")!;
      expect(await settledCharge(db, requestId)).toEqual({
        amount: "100",
        state: "SUCCEEDED",
        billing_outcome: "SUCCESS",
      });
      expect(await supplier(db, setup.provider.account_id)).toEqual({
        balance: "985",
        reserved: "0",
        spent: "15",
      });
      expect(
        (
          await db`SELECT source,finality,usage,cost_atoms::text FROM usage_events WHERE request_id=${requestId} AND record_kind='CANONICAL' ORDER BY seq DESC LIMIT 1`
        )[0],
      ).toEqual({
        source: "execution",
        finality: "FINAL",
        usage: { input_tokens: "30", output_tokens: "15" },
        cost_atoms: "15",
      });
      expect(received).toHaveLength(1);
      expect(received[0]!.authorization).toBe("Bearer synthetic-upstream-secret");
      expect(received[0]!.body).toMatchObject({
        model: "synthetic",
        max_completion_tokens: 30,
        ...(stream ? { stream: true, stream_options: { include_usage: true } } : {}),
      });
    },
  );

test("a JSON chat through Gateway, Core and the settlement worker processes sends once and settles both parties", () =>
  chat(false));

test("an SSE chat through the three processes streams through [DONE] and settles once", () =>
  chat(true));

test("a retryable upstream 503 moves the same request to the other channel and charges the customer once", () =>
  withGatewayStack(
    {
      reply: (call) =>
        call.index === 0
          ? Response.json({ error: { message: "busy" } }, { status: 503 })
          : chatReplies.json(),
      channels: 2,
    },
    async ({ db, setup, received, send }) => {
      const response = await send();
      expect(JSON.parse(await expectStatus(response, 200)).choices[0].message.content).toBe("ok");
      const requestId = response.headers.get("x-request-id")!;
      expect((await settledCharge(db, requestId)).amount).toBe("100");
      expect(received).toHaveLength(2);
      expect(new Set(received.map((r) => r.authorization))).toEqual(
        new Set(["Bearer synthetic-upstream-secret", "Bearer synthetic-upstream-secret-2"]),
      );
      await eventually(
        async () =>
          (
            await db`SELECT count(*)::integer n FROM attempts WHERE request_id=${requestId} AND evidence_id IS NOT NULL`
          )[0].n === 2,
        "evidence for both attempts",
      );
      expect(
        await db`SELECT a.ordinal,a.error_code,p.state AS permit,h.state AS hold FROM attempts a JOIN execution_permits p ON p.attempt_id=a.id JOIN upstream_holds h ON h.attempt_id=a.id WHERE a.request_id=${requestId} ORDER BY a.ordinal`,
      ).toEqual([
        { ordinal: 1, error_code: "upstream_http_503", permit: "UNKNOWN", hold: "PARTIAL" },
        { ordinal: 2, error_code: null, permit: "SEND_GRANTED", hold: "SETTLED" },
      ]);
      expect(await supplier(db, setup.provider.account_id)).toEqual({
        balance: "985",
        reserved: "344",
        spent: "15",
      });
      const [request] =
        await db`SELECT r.state,r.selected_attempt_id=a.id AS second_selected FROM gateway_requests r JOIN attempts a ON a.request_id=r.id AND a.ordinal=2 WHERE r.id=${requestId}`;
      expect(request).toEqual({ state: "SUCCEEDED", second_selected: true });
    },
  ));

test("a client that cancels after a retryable upstream refusal, while the next attempt waits for admission, is never admitted or sent to the next channel and the request closes CANCELLED with its leases released and no charge", () => {
  const unlock = stall();
  let lockWallet = async () => ({ transaction: Promise.resolve() as Promise<unknown> });
  let holder: Awaited<ReturnType<typeof lockWallet>> | undefined;
  return withGatewayStack(
    {
      channels: 2,
      reply: async (call) => {
        if (call.index > 0) return chatReplies.json();
        holder = await lockWallet();
        return Response.json({ error: { message: "busy" } }, { status: 503 });
      },
    },
    async ({ db, setup, received, send }) => {
      lockWallet = async () => {
        const locked = stall();
        const transaction = db.begin(async (tx) => {
          await tx`SELECT w.id FROM wallets w JOIN api_keys k ON k.project_id=w.project_id WHERE k.id=${setup.key.id} FOR UPDATE OF w`;
          locked.release();
          await unlock.released;
        });
        await locked.released;
        return { transaction };
      };
      const controller = new AbortController();
      const pending = send({ signal: controller.signal }).catch((error: unknown) => error);
      await eventually(async () => received.length === 1, "first send");
      await Bun.sleep(600);
      controller.abort();
      expect(await pending).toBeInstanceOf(Error);
      await Bun.sleep(300);
      unlock.release();
      await holder?.transaction;
      const [{ id: requestId }] = await db`SELECT id FROM gateway_requests`;
      expect(await requestOutcome(db, requestId)).toEqual({
        state: "CANCELLED",
        billing_outcome: "CANCELLED",
        charged: "0",
      });
      await Bun.sleep(500);
      expect(received).toHaveLength(1);
      expect(
        await db`SELECT ordinal,error_code FROM attempts WHERE request_id=${requestId} ORDER BY ordinal`,
      ).toEqual([{ ordinal: 1, error_code: "upstream_http_503" }]);
      await eventually(
        async () =>
          (
            await db`SELECT count(*)::integer n FROM execution_quota_leases WHERE request_id=${requestId} AND state<>'RELEASED'`
          )[0].n === 0,
        "released quota leases",
      );
      expect(
        await db`SELECT state FROM billing_holds WHERE request_id=${requestId}`,
      ).not.toContainEqual({ state: "HELD" });
      const next = await send();
      expect(next.status).toBe(200);
      await next.text();
    },
  );
});

test("a manually frozen wallet is refused with 402 insufficient_funds before any request record, hold or upstream call, and unfreezing restores service", () =>
  withGatewayStack(
    { reply: () => chatReplies.json(), channels: 2 },
    async ({ f, db, setup, received, send }) => {
      const wallet = `/projects/${setup.project.id}/wallets/USD`;
      expect((await f.request(wallet, { frozen: true }, undefined, "PATCH")).status).toBe(200);
      const refused = await send();
      expect(JSON.parse(await expectStatus(refused, 402)).error.code).toBe("insufficient_funds");
      expect(received).toHaveLength(0);
      expect(
        await db`SELECT (SELECT count(*) FROM gateway_requests)::integer AS requests,(SELECT count(*) FROM billing_holds)::integer AS holds,(SELECT count(*) FROM upstream_holds)::integer AS supplier_holds`,
      ).toEqual([{ requests: 0, holds: 0, supplier_holds: 0 }]);
      expect(await supplier(db, setup.provider.account_id)).toMatchObject({ reserved: "0" });
      expect(
        (
          await db`SELECT reserved_atoms::text FROM wallets WHERE project_id=${setup.project.id}`
        )[0],
      ).toEqual({ reserved_atoms: "0" });
      expect((await f.request(wallet, { frozen: false }, undefined, "PATCH")).status).toBe(200);
      await expectStatus(await send(), 200);
      expect(received).toHaveLength(1);
    },
  ));

test("a customer wallet briefly locked by another transaction (identity_busy at admission) retries the admission instead of failing the request with 503", () =>
  withGatewayStack({ reply: () => chatReplies.json() }, async ({ db, setup, received, send }) => {
    let locked: () => void = () => undefined;
    const ready = new Promise<void>((resolve) => (locked = resolve));
    const holder = db.begin(async (tx) => {
      await tx`SELECT w.id FROM wallets w JOIN api_keys k ON k.project_id=w.project_id WHERE k.id=${setup.key.id} FOR UPDATE OF w`;
      locked();
      await Bun.sleep(1500);
    });
    await ready;
    const response = await send();
    await holder;
    expect(JSON.parse(await expectStatus(response, 200)).choices[0].message.content).toBe("ok");
    expect(received).toHaveLength(1);
  }));

test("a catalog row briefly locked by another transaction (identity_busy) delays the context instead of failing the request with 503", () =>
  withGatewayStack({ reply: () => chatReplies.json() }, async ({ db, setup, received, send }) => {
    let locked: () => void = () => undefined;
    const ready = new Promise<void>((resolve) => (locked = resolve));
    const holder = db.begin(async (tx) => {
      await tx`SELECT id FROM channels WHERE id=${setup.provider.id} FOR UPDATE`;
      locked();
      await Bun.sleep(400);
    });
    await ready;
    const response = await send();
    await holder;
    expect(JSON.parse(await expectStatus(response, 200)).choices[0].message.content).toBe("ok");
    expect(received).toHaveLength(1);
  }));

test("a row briefly locked by another transaction when the Gateway begins the send (identity_busy at begin) retries the begin instead of failing the request with 503", () =>
  withGatewayStack({ reply: () => chatReplies.json() }, async ({ db, received, send }) => {
    await db.unsafe(
      "CREATE TABLE begin_contention(id int PRIMARY KEY);INSERT INTO begin_contention VALUES(1);GRANT SELECT,UPDATE ON begin_contention TO PUBLIC;CREATE FUNCTION begin_contention() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.action='execution_begin_v1' THEN PERFORM 1 FROM begin_contention FOR UPDATE NOWAIT;END IF;RETURN NEW;END $$;CREATE TRIGGER begin_contention BEFORE INSERT ON audit_events FOR EACH ROW EXECUTE FUNCTION begin_contention()",
    );
    let locked: () => void = () => undefined;
    const ready = new Promise<void>((resolve) => (locked = resolve));
    const holder = db.begin(async (tx) => {
      await tx`SELECT id FROM begin_contention FOR UPDATE`;
      locked();
      await Bun.sleep(400);
    });
    await ready;
    const response = await send();
    await holder;
    expect(JSON.parse(await expectStatus(response, 200)).choices[0].message.content).toBe("ok");
    expect(received).toHaveLength(1);
    expect(
      await db`SELECT p.state FROM execution_permits p JOIN attempts a ON a.id=p.attempt_id WHERE a.request_id=${response.headers.get("x-request-id")}`,
    ).toEqual([{ state: "SEND_GRANTED" }]);
  }));

test("a non-retryable upstream 400 fails the request after one send and settles a zero customer charge", () =>
  withGatewayStack(
    {
      reply: () => Response.json({ error: { message: "bad request" } }, { status: 400 }),
      channels: 2,
    },
    async ({ db, received, send }) => {
      const response = await send();
      const body = JSON.parse(await expectStatus(response, 400));
      expect(body.error.code).toBe("upstream_rejected");
      const requestId = response.headers.get("x-request-id")!;
      expect(body.request_id).toBe(requestId);
      expect(received).toHaveLength(1);
      expect(await settledCharge(db, requestId)).toEqual({
        amount: "0",
        state: "FAILED",
        billing_outcome: "FAILED",
      });
      expect(
        (
          await db`SELECT execution_closed_at IS NOT NULL AS closed FROM gateway_requests WHERE id=${requestId}`
        )[0],
      ).toEqual({ closed: true });
      expect(await db`SELECT ordinal,state,error_code FROM attempts ORDER BY ordinal`).toEqual([
        { ordinal: 1, state: "FAILED", error_code: "upstream_http_400" },
      ]);
    },
  ));

test("the settlement worker's least-privilege role settles a chat but cannot read customer data, startup secrets or call execution functions", () =>
  withGatewayStack({ reply: () => chatReplies.json() }, async ({ db, workerDatabaseUrl, send }) => {
    const response = await send();
    await expectStatus(response, 200);
    expect((await settledCharge(db, response.headers.get("x-request-id")!)).amount).toBe("100");
    const worker = new SQL(workerDatabaseUrl, { max: 1 });
    try {
      for (const statement of [
        "SELECT * FROM wallets LIMIT 1",
        "SELECT * FROM gateway_requests LIMIT 1",
        "SELECT * FROM api_keys LIMIT 1",
        "SELECT * FROM channels LIMIT 1",
        "SELECT boot_nonce FROM internal_startup_sessions LIMIT 1",
        "UPDATE upstream_account_balances SET reserved_atoms=0",
        "SELECT core_execution_batch_v1('[]'::jsonb)",
        "SELECT core_quota_close_v1('{}'::jsonb,'{}'::jsonb,'{}'::jsonb)",
      ])
        await expect(worker.unsafe(statement)).rejects.toThrow("permission denied");
    } finally {
      await worker.close();
    }
  }));

test("a route request policy refusing the prepared request skips that channel before admission, and when every route refuses nothing is reserved (route request_policy was ignored by the v2 Gateway)", () =>
  withGatewayStack(
    { reply: () => chatReplies.json(), channels: 2 },
    async ({ f, db, setup, received, send }) => {
      const routes = Object.fromEntries(
        (await db`SELECT id,provider_id FROM routes WHERE model_name=${setup.model}`).map(
          (row: { id: string; provider_id: string }) => [
            row.provider_id === setup.provider.id ? "first" : "second",
            row.id,
          ],
        ),
      ) as { first: string; second: string };
      const patch = async (route: string, body: Record<string, unknown>) =>
        expect((await f.request(`/routes/${route}`, body, undefined, "PATCH")).status).toBe(200);
      await patch(routes.first, {
        priority: 10,
        request_policy: { parameter_ranges: { max_completion_tokens: { maximum: 10 } } },
      });
      const skipped = await send();
      await expectStatus(skipped, 200);
      expect(received.map((call) => call.authorization)).toEqual([
        "Bearer synthetic-upstream-secret-2",
      ]);
      expect(
        await db`SELECT count(*)::integer n FROM attempts WHERE request_id=${skipped.headers.get("x-request-id")}`,
      ).toEqual([{ n: 1 }]);

      await patch(routes.second, {
        request_policy: { blocked_parameters: ["max_completion_tokens"] },
      });
      const holds = (await db`SELECT count(*)::integer n FROM billing_holds`)[0].n;
      const refused = await send();
      expect(JSON.parse(await expectStatus(refused, 422)).error.code).toBe(
        "unsupported_capability",
      );
      expect(received).toHaveLength(1);
      expect((await db`SELECT count(*)::integer n FROM gateway_requests`)[0].n).toBe(1);
      expect((await db`SELECT count(*)::integer n FROM billing_holds`)[0].n).toBe(holds);

      await patch(routes.first, { request_policy: {} });
      await expectStatus(await send(), 200);
      expect(received.map((call) => call.authorization)).toEqual([
        "Bearer synthetic-upstream-secret-2",
        "Bearer synthetic-upstream-secret",
      ]);
    },
  ));

test("a disabled route or a disabled channel is excluded before any send, and when every candidate is disabled the request answers 503 no_route without a request record, a hold or an upstream call", () =>
  withGatewayStack(
    { reply: () => chatReplies.json(), channels: 2 },
    async ({ f, db, setup, received, send }) => {
      const routes = Object.fromEntries(
        (await db`SELECT id,provider_id FROM routes WHERE model_name=${setup.model}`).map(
          (row: { id: string; provider_id: string }) => [
            row.provider_id === setup.provider.id ? "first" : "second",
            row.id,
          ],
        ),
      ) as { first: string; second: string };
      const [second] = await db`SELECT provider_id AS id FROM routes WHERE id=${routes.second}`;
      const patch = async (path: string, body: Record<string, unknown>) =>
        expect((await f.request(path, body, undefined, "PATCH")).status).toBe(200);

      await patch(`/routes/${routes.first}`, { enabled: false });
      for (let i = 0; i < 2; i++) await expectStatus(await send(), 200);
      expect(received.map((call) => call.authorization)).toEqual([
        "Bearer synthetic-upstream-secret-2",
        "Bearer synthetic-upstream-secret-2",
      ]);

      await patch(`/routes/${routes.first}`, { enabled: true });
      await patch(`/providers/${second.id}`, { enabled: false });
      for (let i = 0; i < 2; i++) await expectStatus(await send(), 200);
      expect(received.slice(2).map((call) => call.authorization)).toEqual([
        "Bearer synthetic-upstream-secret",
        "Bearer synthetic-upstream-secret",
      ]);

      await patch(`/routes/${routes.first}`, { enabled: false });
      const requests = (await db`SELECT count(*)::integer n FROM gateway_requests`)[0].n;
      const holds = (await db`SELECT count(*)::integer n FROM billing_holds`)[0].n;
      const refused = await send();
      expect(JSON.parse(await expectStatus(refused, 503)).error.code).toBe("no_route");
      expect(received).toHaveLength(4);
      expect((await db`SELECT count(*)::integer n FROM gateway_requests`)[0].n).toBe(requests);
      expect((await db`SELECT count(*)::integer n FROM billing_holds`)[0].n).toBe(holds);
    },
  ));

test("ROUND_ROBIN alternates real upstream calls between same-priority channels with an offset shared by Gateways on different Core instances", () =>
  withGatewayStack(
    { reply: () => chatReplies.json(), channels: 2 },
    async ({ f, setup, received, send, gateway, startGateway }) => {
      expect(
        (
          await f.request(
            `/models/${encodeURIComponent(setup.model)}`,
            { routing_strategy: "ROUND_ROBIN" },
            undefined,
            "PATCH",
          )
        ).status,
      ).toBe(200);
      const other = await startGateway({}, "gatewayB", "coreB");
      for (const via of [gateway, other, gateway, other, gateway, other])
        await expectStatus(await send({ via }), 200);
      const order = received.map((call) => call.authorization);
      const [first, second] = order as [string, string];
      expect([first, second].sort()).toEqual([
        "Bearer synthetic-upstream-secret",
        "Bearer synthetic-upstream-secret-2",
      ]);
      expect(order).toEqual([first, second, first, second, first, second]);
    },
  ));

test("WEIGHTED routing follows a route weight update on the next request instead of a stale preference", () =>
  withGatewayStack(
    { reply: () => chatReplies.json(), channels: 2 },
    async ({ f, db, setup, received, send }) => {
      const routes = Object.fromEntries(
        (await db`SELECT id,provider_id FROM routes WHERE model_name=${setup.model}`).map(
          (row: { id: string; provider_id: string }) => [
            row.provider_id === setup.provider.id ? "first" : "second",
            row.id,
          ],
        ),
      ) as { first: string; second: string };
      const weigh = async (first: number, second: number) => {
        for (const [route, weight] of [
          [routes.first, first],
          [routes.second, second],
        ] as const)
          expect((await f.request(`/routes/${route}`, { weight }, undefined, "PATCH")).status).toBe(
            200,
          );
      };
      await weigh(1000, 0.001);
      for (let i = 0; i < 3; i++) await expectStatus(await send(), 200);
      await weigh(0.001, 1000);
      for (let i = 0; i < 3; i++) await expectStatus(await send(), 200);
      expect(received.map((call) => call.authorization)).toEqual([
        ...Array(3).fill("Bearer synthetic-upstream-secret"),
        ...Array(3).fill("Bearer synthetic-upstream-secret-2"),
      ]);
    },
  ));

test("a channel whose own concurrency is full is skipped at admission and the request is served by the next channel instead of 429 rate_limited, without the refused admission holding a customer concurrency slot; when every channel is full the request gets 429 and nothing is reserved", () => {
  const gates = new Map<string | null, ReturnType<typeof stall>>();
  return withGatewayStack(
    {
      reply: async (call) => {
        await gates.get(call.authorization)?.released;
        return chatReplies.json();
      },
      channels: 2,
      supplierCredit: "1000000",
    },
    async ({ f, db, setup, received, send }) => {
      const [first] =
        await db`SELECT id FROM routes WHERE model_name=${setup.model} AND provider_id=${setup.provider.id}`;
      const [second] =
        await db`SELECT provider_id FROM routes WHERE model_name=${setup.model} AND provider_id<>${setup.provider.id}`;
      const patch = async (path: string, body: Record<string, unknown>) =>
        expect((await f.request(path, body, undefined, "PATCH")).status).toBe(200);
      await patch(`/routes/${first.id}`, { priority: 10 });
      await patch(`/providers/${setup.provider.id}`, { concurrency: 1 });
      await patch(`/projects/${setup.project.id}`, { concurrency: 2 });
      const count = async (table: "gateway_requests" | "billing_holds") =>
        (await db`SELECT count(*)::integer n FROM ${db(table)}`)[0].n as number;
      try {
        gates.set("Bearer synthetic-upstream-secret", stall());
        const holding = send();
        holding.catch(() => undefined);
        await eventually(async () => received.length === 1, "first channel occupied");
        const fallback = await send();
        await expectStatus(fallback, 200);
        expect(received.map((call) => call.authorization)).toEqual([
          "Bearer synthetic-upstream-secret",
          "Bearer synthetic-upstream-secret-2",
        ]);
        expect(
          await db`SELECT count(*)::integer n FROM attempts WHERE request_id=${fallback.headers.get("x-request-id")}`,
        ).toEqual([{ n: 1 }]);

        await patch(`/providers/${second.provider_id}`, { concurrency: 1 });
        await patch(`/projects/${setup.project.id}`, { concurrency: 3 });
        gates.set("Bearer synthetic-upstream-secret-2", stall());
        const occupying = send();
        occupying.catch(() => undefined);
        await eventually(async () => received.length === 3, "second channel occupied");
        const requests = await count("gateway_requests"),
          holds = await count("billing_holds");
        const saturated = await send();
        expect(JSON.parse(await expectStatus(saturated, 429)).error.code).toBe("rate_limited");
        expect(received).toHaveLength(3);
        expect(await count("gateway_requests")).toBe(requests);
        expect(await count("billing_holds")).toBe(holds);
        for (const gate of gates.values()) gate.release();
        await expectStatus(await holding, 200);
        await expectStatus(await occupying, 200);
      } finally {
        for (const gate of gates.values()) gate.release();
      }
    },
  );
}, 30000);

test("transient and authorization upstream failures move to the other channel while 400 and 422 stop after one send", () => {
  let next: number | undefined;
  return withGatewayStack(
    {
      reply: () => {
        const status = next;
        next = undefined;
        return status === undefined
          ? chatReplies.json()
          : Response.json({ error: { message: "synthetic" } }, { status });
      },
      channels: 2,
      supplierCredit: "1000000",
    },
    async ({ f, db, setup, received, send }) => {
      expect(
        (
          await f.request(`/projects/${setup.project.id}/credits`, {
            external_ref: crypto.randomUUID(),
            currency: "USD",
            amount_atoms: "100000",
            kind: "CASH",
          })
        ).status,
      ).toBe(201);
      const redis = new RedisClient(process.env.REDIS_URL!);
      try {
        const channels = (await db`SELECT id FROM channels`).map((row: { id: string }) => row.id);
        for (const [status, moves, failed] of [
          ...[408, 409, 429, 500, 502, 503, 504, 529].map((s) => [s, true, "UNKNOWN"] as const),
          ...[401, 403, 404].map((s) => [s, true, "FAILED"] as const),
          ...[400, 422].map((s) => [s, false, "FAILED"] as const),
        ]) {
          for (const channel of channels) await redis.del(`${f.namespace}:health:${channel}`);
          next = status;
          const before = received.length;
          const response = await send();
          const requestId = response.headers.get("x-request-id")!;
          if (moves) {
            await expectStatus(response, 200);
            expect(received.length - before).toBe(2);
            expect(await settledCharge(db, requestId)).toMatchObject({ amount: "100" });
          } else {
            expect(JSON.parse(await expectStatus(response, 400)).error.code).toBe(
              "upstream_rejected",
            );
            expect(received.length - before).toBe(1);
            expect(await settledCharge(db, requestId)).toMatchObject({ amount: "0" });
          }
          await eventually(
            async () =>
              (
                await db`SELECT count(*)::integer n FROM attempts WHERE request_id=${requestId} AND evidence_id IS NOT NULL`
              )[0].n === (moves ? 2 : 1),
            `attempt evidence for ${status}`,
          );
          expect(
            await db`SELECT state,error_code FROM attempts WHERE request_id=${requestId} ORDER BY ordinal`,
          ).toEqual([
            { state: failed, error_code: `upstream_http_${status}` },
            ...(moves ? [{ state: "SUCCEEDED", error_code: null }] : []),
          ]);
        }
      } finally {
        redis.close();
      }
    },
  );
}, 90000);

test(
  "six failing channels stop at Core's default three-attempt budget and the request is finalized FAILED with a zero charge instead of leaving the customer hold HELD",
  () =>
    withGatewayStack(
      {
        reply: () => Response.json({ error: { message: "busy" } }, { status: 503 }),
        channels: 6,
        supplierCredit: "1000000",
      },
      async ({ db, received, send }) => {
        const response = await send();
        expect(JSON.parse(await expectStatus(response, 502)).error.code).toBe("upstream_rejected");
        const requestId = response.headers.get("x-request-id")!;
        expect(received).toHaveLength(3);
        expect(new Set(received.map((call) => call.authorization)).size).toBe(3);
        await eventually(
          async () =>
            (
              await db`SELECT count(*)::integer n FROM attempts WHERE request_id=${requestId} AND evidence_id IS NOT NULL`
            )[0].n === 3,
          "attempt evidence",
        );
        expect(
          (await db`SELECT state FROM attempts WHERE request_id=${requestId} ORDER BY ordinal`).map(
            (row: { state: string }) => row.state,
          ),
        ).toEqual(["UNKNOWN", "UNKNOWN", "UNKNOWN"]);
        expect(await requestOutcome(db, requestId)).toEqual({
          state: "FAILED",
          billing_outcome: "FAILED",
          charged: "0",
        });
      },
    ),
  30000,
);

for (const maxAttempts of [1, 5])
  test(
    `Core's attempt budget of ${maxAttempts} bounds the sends across six failing channels, each channel sent at most once, and the request is finalized FAILED with a zero charge`,
    () =>
      withGatewayStack(
        {
          reply: () => Response.json({ error: { message: "busy" } }, { status: 503 }),
          channels: 6,
          supplierCredit: "1000000",
          maxAttempts,
        },
        async ({ db, received, send }) => {
          const response = await send();
          expect(JSON.parse(await expectStatus(response, 502)).error.code).toBe(
            "upstream_rejected",
          );
          const requestId = response.headers.get("x-request-id")!;
          expect(received).toHaveLength(maxAttempts);
          expect(new Set(received.map((call) => call.authorization)).size).toBe(maxAttempts);
          await eventually(
            async () =>
              (
                await db`SELECT count(*)::integer n FROM attempts WHERE request_id=${requestId} AND evidence_id IS NOT NULL`
              )[0].n === maxAttempts,
            "attempt evidence",
          );
          expect(
            (
              await db`SELECT state FROM attempts WHERE request_id=${requestId} ORDER BY ordinal`
            ).map((row: { state: string }) => row.state),
          ).toEqual(Array(maxAttempts).fill("UNKNOWN"));
          expect(await requestOutcome(db, requestId)).toEqual({
            state: "FAILED",
            billing_outcome: "FAILED",
            charged: "0",
          });
        },
      ),
    60000,
  );

test("a model whose channel carries a large plugin model map, or that is routed to six channels, still answers instead of 503 core_unavailable (the context reply exceeded the 8 KiB internal budget)", () =>
  withGatewayStack(
    { reply: () => chatReplies.json(), supplierCredit: "1000000" },
    async ({ f, db, setup, received, send }) => {
      const [{ base_url: baseUrl }] =
        await db`SELECT base_url FROM channels WHERE id=${setup.provider.id}`;
      const channel = (apiKey: string, modelMap?: Record<string, string>) =>
        f.addChannel(setup, {
          baseUrl,
          apiKey,
          maxInputTokens: 1000,
          ceilings: boundedSupplier.ceilings,
          ...(modelMap ? { modelMap } : {}),
        });
      await channel(
        "synthetic-upstream-secret-large",
        Object.fromEntries(
          Array.from({ length: 600 }, (_, i) => [`catalog-model-${i}`, `upstream-model-${i}`]),
        ),
      );
      await expectStatus(await send(), 200);
      for (let i = 3; i <= 6; i++) await channel(`synthetic-upstream-secret-${i}`);
      await expectStatus(await send(), 200);
      expect(received).toHaveLength(2);
    },
  ));

test(
  "a retryable upstream failure on the only channel finalizes the request FAILED with a zero charge instead of leaving the customer hold HELD",
  () =>
    withGatewayStack(
      { reply: () => Response.json({ error: { message: "busy" } }, { status: 503 }) },
      async ({ db, received, send }) => {
        const response = await send();
        expect(JSON.parse(await expectStatus(response, 502)).error.code).toBe("upstream_rejected");
        const requestId = response.headers.get("x-request-id")!;
        expect(received).toHaveLength(1);
        expect(await requestOutcome(db, requestId)).toEqual({
          state: "FAILED",
          billing_outcome: "FAILED",
          charged: "0",
        });
        expect(
          await db`SELECT state,error_code FROM attempts WHERE request_id=${requestId}`,
        ).toEqual([{ state: "UNKNOWN", error_code: "upstream_http_503" }]);
      },
    ),
  30000,
);

test(
  "when the only remaining route refuses the request by policy after a retryable failure, the failed attempt finalizes the request instead of leaving the customer hold HELD",
  () =>
    withGatewayStack(
      {
        reply: () => Response.json({ error: { message: "busy" } }, { status: 503 }),
        channels: 2,
        supplierCredit: "1000000",
      },
      async ({ f, db, setup, received, send }) => {
        const routes = Object.fromEntries(
          (await db`SELECT id,provider_id FROM routes WHERE model_name=${setup.model}`).map(
            (row: { id: string; provider_id: string }) => [
              row.provider_id === setup.provider.id ? "first" : "second",
              row.id,
            ],
          ),
        ) as { first: string; second: string };
        expect(
          (await f.request(`/routes/${routes.first}`, { priority: 10 }, undefined, "PATCH")).status,
        ).toBe(200);
        expect(
          (
            await f.request(
              `/routes/${routes.second}`,
              { request_policy: { blocked_parameters: ["max_completion_tokens"] } },
              undefined,
              "PATCH",
            )
          ).status,
        ).toBe(200);
        const response = await send();
        expect(JSON.parse(await expectStatus(response, 502)).error.code).toBe("upstream_rejected");
        expect(received.map((call) => call.authorization)).toEqual([
          "Bearer synthetic-upstream-secret",
        ]);
        expect(await requestOutcome(db, response.headers.get("x-request-id")!)).toEqual({
          state: "FAILED",
          billing_outcome: "FAILED",
          charged: "0",
        });
      },
    ),
  30000,
);

test(
  "a refused upstream connection is never sent or charged, does not count against that channel's health, and the request is served by the next channel",
  () =>
    withGatewayStack(
      { reply: () => chatReplies.json(), supplierCredit: "1000000" },
      async ({ f, db, setup, received, send }) => {
        const closed = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: () => new Response() });
        const deadUrl = `https://127.0.0.1:${closed.port}`;
        await closed.stop(true);
        const dead = await f.addChannel(setup, {
          baseUrl: deadUrl,
          apiKey: "synthetic-refused-secret",
          maxInputTokens: 1000,
          ceilings: boundedSupplier.ceilings,
        });
        const [preferred] =
          await db`SELECT id FROM routes WHERE model_name=${setup.model} AND provider_id=${dead.id}`;
        expect(
          (await f.request(`/routes/${preferred.id}`, { priority: 10 }, undefined, "PATCH")).status,
        ).toBe(200);
        const response = await send();
        expect(JSON.parse(await expectStatus(response, 200)).choices[0].message.content).toBe("ok");
        expect(received).toHaveLength(1);
        const requestId = response.headers.get("x-request-id")!;
        expect((await settledCharge(db, requestId)).amount).toBe("100");
        await eventually(
          async () =>
            (
              await db`SELECT count(*)::integer n FROM attempts WHERE request_id=${requestId} AND evidence_id IS NOT NULL`
            )[0].n === 2,
          "attempt evidence",
        );
        expect(
          await db`SELECT a.ordinal,a.state,a.error_code,h.final_cost_atoms::text AS cost FROM attempts a JOIN upstream_holds h ON h.attempt_id=a.id WHERE a.request_id=${requestId} ORDER BY a.ordinal`,
        ).toEqual([
          { ordinal: 1, state: "NOT_EXECUTED", error_code: "upstream_not_dispatched", cost: "0" },
          { ordinal: 2, state: "SUCCEEDED", error_code: null, cost: "15" },
        ]);
        const redis = new RedisClient(process.env.REDIS_URL!);
        try {
          expect(await redis.send("HGET", [`${f.namespace}:health:${dead.id}`, "fails"])).not.toBe(
            "1",
          );
        } finally {
          redis.close();
        }
      },
    ),
  60000,
);

test("a send refused because every local upstream connection slot and wait slot is taken is never sent or charged and does not open the channel's breaker", () => {
  const gate = stall();
  return withGatewayStack(
    {
      reply: async (call) => {
        if (call.index === 0) await gate.released;
        return chatReplies.json();
      },
      gateway: { upstream_pool_size: 1 },
      supplierCredit: "1000000",
    },
    async ({ f, db, setup, received, gateway }) => {
      const dedicated = () =>
        new Promise<{ status: number; id: string; text: string }>((resolve, reject) => {
          const outgoing = request(
            `${gateway.origin}/v1/chat/completions`,
            {
              method: "POST",
              agent: false,
              headers: {
                authorization: `Bearer ${setup.key.key}`,
                "content-type": "application/json",
              },
            },
            (response) => {
              let text = "";
              response.on("data", (chunk) => (text += chunk));
              response.on("end", () =>
                resolve({
                  status: response.statusCode!,
                  id: String(response.headers["x-request-id"]),
                  text,
                }),
              );
            },
          );
          outgoing.on("error", reject);
          outgoing.end(
            JSON.stringify({
              model: setup.model,
              messages: [{ role: "user", content: "合成" }],
              max_completion_tokens: 30,
            }),
          );
        });
      const holding = dedicated();
      await eventually(async () => received.length === 1, "first send holds the only slot");
      const waiting = dedicated();
      await Bun.sleep(300);
      const refused = await dedicated();
      expect([refused.status, JSON.parse(refused.text).error.code]).toEqual([
        502,
        "upstream_unavailable",
      ]);
      const requestId = refused.id;
      expect(await requestOutcome(db, requestId)).toEqual({
        state: "FAILED",
        billing_outcome: "FAILED",
        charged: "0",
      });
      expect(
        await db`SELECT a.state,a.error_code,h.final_cost_atoms::text AS cost FROM attempts a JOIN upstream_holds h ON h.attempt_id=a.id WHERE a.request_id=${requestId}`,
      ).toEqual([{ state: "NOT_EXECUTED", error_code: "upstream_not_dispatched", cost: "0" }]);
      gate.release();
      expect((await Promise.all([holding, waiting])).map((r) => r.status)).toEqual([200, 200]);
      expect(received).toHaveLength(2);
      const redis = new RedisClient(process.env.REDIS_URL!);
      try {
        expect(
          await redis.send("HMGET", [
            `${f.namespace}:health:${setup.provider.id}`,
            "fails",
            "open_until",
          ]),
        ).not.toContain("1");
      } finally {
        redis.close();
      }
    },
  );
}, 60000);

test("48 concurrent requests on one Gateway complete instead of being refused by internal stream limits", () =>
  withGatewayStack(
    {
      reply: async () => {
        await Bun.sleep(300);
        return chatReplies.json();
      },
      supplierCredit: "1000000000",
    },
    async ({ f, setup, send }) => {
      const credit = await f.request(`/projects/${setup.project.id}/credits`, {
        external_ref: `r2-${setup.project.id}`,
        currency: "USD",
        amount_atoms: "1000000000",
        kind: "CASH",
      });
      expect(credit.status).toBe(201);
      for (const [path, limits] of [
        [`/providers/${setup.provider.id}`, { concurrency: 1000, rpm: 1000000 }],
        [`/projects/${setup.project.id}`, { concurrency: 1000, rpm: 1000000, tpm: 1000000000 }],
      ] as const)
        expect((await f.request(path, limits, undefined, "PATCH")).status).toBe(200);
      const responses = await Promise.all(Array.from({ length: 48 }, () => send()));
      expect(responses.map((response) => response.status)).toEqual(
        Array.from({ length: 48 }, () => 200),
      );
    },
  ));

test("requests beyond the Gateway plugin attempt budget are refused as 503 overloaded instead of 500 internal_error", () =>
  withGatewayStack(
    {
      reply: async () => {
        await Bun.sleep(1500);
        return chatReplies.json();
      },
      supplierCredit: "1000000000",
    },
    async ({ f, setup, send }) => {
      const credit = await f.request(`/projects/${setup.project.id}/credits`, {
        external_ref: `overload-${setup.project.id}`,
        currency: "USD",
        amount_atoms: "1000000000",
        kind: "CASH",
      });
      expect(credit.status).toBe(201);
      for (const [path, limits] of [
        [`/providers/${setup.provider.id}`, { concurrency: 1000, rpm: 1000000 }],
        [`/projects/${setup.project.id}`, { concurrency: 1000, rpm: 1000000, tpm: 1000000000 }],
      ] as const)
        expect((await f.request(path, limits, undefined, "PATCH")).status).toBe(200);
      const outcomes = await Promise.all(
        Array.from({ length: 160 }, async (_, index) => {
          await Bun.sleep(index < 80 ? 0 : 200);
          const response = await send();
          if (response.status === 200) return "200";
          const body = (await response.json()) as { error: { code: string } };
          return `${response.status} ${body.error.code}`;
        }),
      );
      expect([...new Set(outcomes)].sort()).toEqual(["200", "503 overloaded"]);
    },
  ));
