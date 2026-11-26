import assert from "node:assert/strict";
import type { SQL } from "bun";

export async function verifyCommittedReplay({
  db,
  chatUrl,
  billingAccountId,
  key,
  model,
  restartGateway,
  upstreamAttempts,
}: {
  db: SQL;
  chatUrl: string;
  billingAccountId: string;
  key: string;
  model: string;
  restartGateway: () => Promise<void>;
  upstreamAttempts: () => Promise<number>;
}): Promise<void> {
  const headers = {
    authorization: `Bearer ${key}`,
    "content-type": "application/json",
    "idempotency-key": crypto.randomUUID(),
  };
  const body = JSON.stringify({
    model,
    messages: [{ role: "user", content: "replay after the response was lost" }],
    max_tokens: 32,
  });
  const gatewayResponse = Promise.withResolvers<{ status: number; requestId: string | null }>();
  const releaseResponse = Promise.withResolvers<void>();
  const proxy = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(req) {
      try {
        const result = await fetch(chatUrl, {
          method: "POST",
          headers: req.headers,
          body: await req.arrayBuffer(),
          signal: AbortSignal.timeout(5000),
        });
        const bytes = await result.arrayBuffer();
        gatewayResponse.resolve({
          status: result.status,
          requestId: result.headers.get("x-request-id"),
        });
        await releaseResponse.promise;
        return new Response(bytes, { status: result.status, headers: result.headers });
      } catch (error) {
        gatewayResponse.reject(error);
        throw error;
      }
    },
  });
  const clientResponse = fetch(proxy.url, {
    method: "POST",
    headers,
    body,
    signal: AbortSignal.timeout(10_000),
  }).then(
    async (response) => {
      await response.arrayBuffer();
      return { status: response.status };
    },
    (error: unknown) => ({ error }),
  );

  const charges = () => db`
    SELECT request_id::text, customer_atoms::text, supplier_cost_atoms::text
    FROM public.billing_charges
    WHERE billing_account_id = ${billingAccountId} AND model = ${model}
  `;
  let expectedCharge;
  try {
    const response = await Promise.race([
      gatewayResponse.promise,
      clientResponse.then(() => {
        throw new Error("Client request ended before Gateway committed");
      }),
    ]);
    assert.equal(response.status, 200);
    assert.ok(response.requestId);
    expectedCharge = {
      request_id: response.requestId,
      customer_atoms: "185",
      supplier_cost_atoms: "37",
    };
    assert.deepEqual(Array.from(await charges()), [expectedCharge]);
    assert.equal(await upstreamAttempts(), 1);
    const waiting = Symbol("client is still waiting");
    assert.equal(await Promise.race([clientResponse, waiting]), waiting);
    await restartGateway();
    assert.equal(await Promise.race([clientResponse, waiting]), waiting);
  } finally {
    const stopped = proxy.stop(true);
    releaseResponse.resolve();
    await stopped;
  }
  assert.ok("error" in (await clientResponse), "Client must see a lost response");

  const replay = await fetch(chatUrl, {
    method: "POST",
    headers,
    body,
    signal: AbortSignal.timeout(5000),
  });
  const payload: unknown = await replay.json();
  assert.equal(replay.status, 409);
  assert.equal(replay.headers.get("x-request-id"), expectedCharge.request_id);
  assert.partialDeepStrictEqual(payload, { error: { code: "idempotency_replayed" } });
  assert.equal(await upstreamAttempts(), 1);
  assert.deepEqual(Array.from(await charges()), [expectedCharge]);
}
