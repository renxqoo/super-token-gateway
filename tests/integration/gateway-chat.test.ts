import { expect, test } from "bun:test";
import { RedisClient, SQL } from "bun";
import {
  boundedSupplier,
  chatReplies,
  eventually,
  requestOutcome,
  settledCharge,
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

test("upstream statuses are classified like legacy: transient and authorization failures move to the other channel, 400 and 422 stop after one send", () => {
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
