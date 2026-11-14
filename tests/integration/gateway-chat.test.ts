import { expect, test } from "bun:test";
import { SQL } from "bun";
import {
  chatReplies,
  eventually,
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
