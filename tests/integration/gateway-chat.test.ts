import { expect, test } from "bun:test";
import type { SQL } from "bun";
import { withFinancial } from "../support/financial-execution.ts";
import {
  clearQueueNamespace,
  eventually,
  startCoreWorker,
  startGateway,
  startTlsUpstream,
} from "../support/gateway-process.ts";

const jsonReply =
  '{"id":"chain","model":"synthetic","choices":[{"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":30,"completion_tokens":15}}';
const sseReply = [
  { id: "chain", model: "synthetic", choices: [{ index: 0, delta: { content: "o" } }] },
  {
    id: "chain",
    model: "synthetic",
    choices: [{ index: 0, delta: { content: "k" }, finish_reason: "stop" }],
  },
  {
    id: "chain",
    model: "synthetic",
    choices: [],
    usage: { prompt_tokens: 30, completion_tokens: 15 },
  },
]
  .map((chunk) => `data: ${JSON.stringify(chunk)}\n\n`)
  .concat("data: [DONE]\n\n")
  .join("");
const supplierBounds = {
  rates: {
    input_tokens: { numerator: "1", denominator: "3" },
    output_tokens: { numerator: "1", denominator: "3" },
  },
  ceilings: { input_tokens: "1000", output_tokens: "50" },
};

type Received = { authorization: string | null; body: Record<string, unknown> };
type Fixture = Parameters<Parameters<typeof withFinancial>[0]>[0];
type Stack = {
  db: SQL;
  setup: Awaited<ReturnType<Fixture["bootstrap"]>>;
  received: Received[];
  send: (stream: boolean) => Promise<Response>;
};

function withChatStack(
  reply: (index: number, stream: boolean) => Response,
  channels: number,
  run: (stack: Stack) => Promise<void>,
) {
  return withFinancial(async (f, db) => {
    const received: Received[] = [];
    const upstream = await startTlsUpstream(async (request) => {
      const body = (await request.json()) as Record<string, unknown>;
      received.push({ authorization: request.headers.get("authorization"), body });
      return reply(received.length - 1, body.stream === true);
    });
    const namespace = `gateway-chat_${crypto.randomUUID().replaceAll("-", "")}`;
    try {
      const setup = await f.bootstrap(undefined, undefined, undefined, undefined, supplierBounds, {
        baseUrl: upstream.baseUrl,
        maxInputTokens: 1000,
      });
      for (let channel = 2; channel <= channels; channel++)
        await f.addChannel(setup, {
          baseUrl: upstream.baseUrl,
          apiKey: `synthetic-upstream-secret-${channel}`,
          maxInputTokens: 1000,
          ceilings: supplierBounds.ceilings,
        });
      const worker = await startCoreWorker(db, namespace);
      const gateway = await startGateway(f, {
        namespace,
        upstreamCertificate: upstream.certificate,
      });
      try {
        await run({
          db,
          setup,
          received,
          send: (stream) =>
            fetch(`${gateway.origin}/v1/chat/completions`, {
              method: "POST",
              headers: {
                authorization: `Bearer ${setup.key.key}`,
                "content-type": "application/json",
              },
              body: JSON.stringify({
                model: setup.model,
                messages: [{ role: "user", content: "合成" }],
                max_completion_tokens: 30,
                ...(stream ? { stream: true } : {}),
              }),
            }),
        });
        expect((await gateway.stop()).event).toBe("stopped");
        expect((await worker.stop()).event).toBe("stopped");
      } finally {
        gateway.kill();
        worker.kill();
      }
    } finally {
      await upstream.stop();
      await clearQueueNamespace(namespace);
    }
  });
}

function success(stream: boolean) {
  return new Response(stream ? sseReply : jsonReply, {
    headers: { "content-type": stream ? "text/event-stream" : "application/json" },
  });
}

async function settled(db: SQL, requestId: string) {
  await eventually(
    async () =>
      (await db`SELECT count(*)::integer n FROM billing_charges WHERE request_id=${requestId}`)[0]
        .n === 1,
    "customer charge",
  );
}

async function expectStatus(response: Response, status: number) {
  const text = await response.text();
  expect({ status: response.status, text: response.status === status ? "" : text }).toEqual({
    status,
    text: "",
  });
  return text;
}

const chat = (stream: boolean) =>
  withChatStack(
    (_index, isStream) => success(isStream),
    1,
    async ({ db, setup, received, send }) => {
      const response = await send(stream);
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
      await settled(db, requestId);
      expect(
        await db`SELECT amount_atoms::text FROM billing_charges WHERE request_id=${requestId}`,
      ).toEqual([{ amount_atoms: "100" }]);
      expect(
        (
          await db`SELECT balance_atoms::text balance,reserved_atoms::text reserved,spent_atoms::text spent FROM upstream_account_balances WHERE account_id=${setup.provider.account_id}`
        )[0],
      ).toEqual({ balance: "985", reserved: "0", spent: "15" });
      expect(
        (await db`SELECT state,billing_outcome FROM gateway_requests WHERE id=${requestId}`)[0],
      ).toEqual({ state: "SUCCEEDED", billing_outcome: "SUCCESS" });
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
  withChatStack(
    (index, stream) =>
      index === 0
        ? Response.json({ error: { message: "busy" } }, { status: 503 })
        : success(stream),
    2,
    async ({ db, received, send }) => {
      const response = await send(false);
      expect(JSON.parse(await expectStatus(response, 200)).choices[0].message.content).toBe("ok");
      const requestId = response.headers.get("x-request-id")!;
      await settled(db, requestId);
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
        await db`SELECT ordinal,error_code FROM attempts WHERE request_id=${requestId} ORDER BY ordinal`,
      ).toEqual([
        { ordinal: 1, error_code: "upstream_http_503" },
        { ordinal: 2, error_code: null },
      ]);
      expect(
        await db`SELECT amount_atoms::text FROM billing_charges WHERE request_id=${requestId}`,
      ).toEqual([{ amount_atoms: "100" }]);
      const [request] =
        await db`SELECT r.state,r.selected_attempt_id=a.id AS second_selected FROM gateway_requests r JOIN attempts a ON a.request_id=r.id AND a.ordinal=2 WHERE r.id=${requestId}`;
      expect(request).toEqual({ state: "SUCCEEDED", second_selected: true });
    },
  ));

test("a non-retryable upstream 400 fails the request after one send and settles a zero customer charge", () =>
  withChatStack(
    () => Response.json({ error: { message: "bad request" } }, { status: 400 }),
    2,
    async ({ db, received, send }) => {
      const response = await send(false);
      const body = JSON.parse(await expectStatus(response, 400));
      expect(body.error.code).toBe("upstream_rejected");
      const requestId = response.headers.get("x-request-id")!;
      expect(body.request_id).toBe(requestId);
      expect(received).toHaveLength(1);
      await settled(db, requestId);
      expect(
        (
          await db`SELECT state,execution_closed_at IS NOT NULL AS closed FROM gateway_requests WHERE id=${requestId}`
        )[0],
      ).toEqual({ state: "FAILED", closed: true });
      expect(await db`SELECT ordinal,state,error_code FROM attempts ORDER BY ordinal`).toEqual([
        { ordinal: 1, state: "FAILED", error_code: "upstream_http_400" },
      ]);
      expect(
        await db`SELECT c.amount_atoms::text,r.billing_outcome FROM billing_charges c JOIN gateway_requests r ON r.id=c.request_id`,
      ).toEqual([{ amount_atoms: "0", billing_outcome: "FAILED" }]);
    },
  ));
