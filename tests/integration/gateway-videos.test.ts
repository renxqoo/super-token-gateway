import { expect, test } from "bun:test";
import {
  allowOperation,
  settledCharge,
  withGatewayStack,
  type UpstreamCall,
} from "../support/gateway-process.ts";

type Stack = Parameters<Parameters<typeof withGatewayStack>[1]>[0];

const secondsTariff = {
  pricing: { currency: "USD", rates: { video_seconds: { numerator: "10", denominator: "1" } } },
  ceilings: { video_seconds: "20" },
  basis: "PROVIDER_USAGE",
};

function upstream(final: "completed" | "failed", polls: number) {
  const seen = new Map<string, number>();
  const deleted = new Set<string>();
  return (call: UpstreamCall) => {
    if (call.path === "/v1/videos" && call.method === "POST")
      return Response.json({
        id: `video_up_${call.index}`,
        object: "video",
        model: "synthetic",
        status: "queued",
        seconds: String(call.body.seconds ?? "4"),
      });
    const match = /^\/v1\/videos\/(video_up_\d+)(\/content)?$/.exec(call.path);
    if (!match || deleted.has(match[1]!)) return new Response("not found", { status: 404 });
    if (match[2]) return new Response("合成视频字节", { headers: { "content-type": "video/mp4" } });
    if (call.method === "DELETE") {
      deleted.add(match[1]!);
      return Response.json({ id: match[1], object: "video.deleted", deleted: true });
    }
    const count = seen.get(match[1]!) ?? 0;
    seen.set(match[1]!, count + 1);
    const status = count < polls ? "in_progress" : final;
    return Response.json({
      id: match[1],
      object: "video",
      model: "synthetic",
      status,
      seconds: "8",
      ...(status === "failed" ? { error: { code: "moderation_blocked", message: "x" } } : {}),
    });
  };
}

async function create(stack: Stack, body: Record<string, unknown>) {
  await allowOperation(stack, "videos", secondsTariff);
  return fetch(`${stack.gateway.origin}/v1/videos`, {
    method: "POST",
    headers: {
      authorization: `Bearer ${stack.setup.key.key}`,
      "content-type": "application/json",
    },
    body: JSON.stringify({ model: stack.setup.model, ...body }),
  });
}

test(
  "a video is created with a gateway identity, settled from its final seconds without client polling, and retrieved, streamed, listed and deleted on its original channel",
  () =>
    withGatewayStack(
      { reply: upstream("completed", 1), supplierCredit: "1000000" },
      async (stack) => {
        const authorization = `Bearer ${stack.setup.key.key}`;
        const call = (path: string, method = "GET") =>
          fetch(`${stack.gateway.origin}${path}`, { method, headers: { authorization } });
        const created = await create(stack, { prompt: "海浪", seconds: "8" });
        expect(created.status).toBe(200);
        const requestId = created.headers.get("x-request-id")!,
          id = `video_${requestId.replaceAll("-", "")}`;
        const text = await created.text();
        expect(JSON.parse(text)).toMatchObject({ id, status: "queued", model: stack.setup.model });
        expect(text).not.toContain("video_up");
        expect(
          stack.received.filter((call) => call.method === "POST" && call.path === "/v1/videos"),
        ).toMatchObject([{ body: { model: "synthetic", seconds: "8" } }]);

        expect(await settledCharge(stack.db, requestId)).toEqual({
          amount: "80",
          state: "SUCCEEDED",
          billing_outcome: "SUCCESS",
        });
        expect(
          await stack.db`SELECT r.source,r.finality FROM core_evidence_receipts r JOIN attempts a ON a.id=r.attempt_id WHERE a.request_id=${requestId}::uuid AND r.source='async_final'`,
        ).toEqual([{ source: "async_final", finality: "FINAL" }]);

        const retrieved = await call(`/v1/videos/${id}`);
        expect(retrieved.status).toBe(200);
        const retrievedText = await retrieved.text();
        expect(JSON.parse(retrievedText)).toMatchObject({
          id,
          status: "completed",
          model: stack.setup.model,
        });
        expect(retrievedText).not.toContain("video_up");
        const content = await call(`/v1/videos/${id}/content`);
        expect(content.status).toBe(200);
        expect(content.headers.get("content-type")).toBe("video/mp4");
        expect(content.headers.get("cache-control")).toBe("no-store");
        expect(await content.text()).toBe("合成视频字节");
        const listed = (await (await call("/v1/videos")).json()) as { data: { id: string }[] };
        expect(listed.data.map((row) => row.id)).toEqual([id]);

        const deleted = await call(`/v1/videos/${id}`, "DELETE");
        expect(deleted.status).toBe(200);
        expect(await deleted.json()).toMatchObject({ id, deleted: true });
        expect(stack.received.at(-1)).toMatchObject({
          method: "DELETE",
          path: "/v1/videos/video_up_0",
        });
        const calls = stack.received.length;
        expect((await call(`/v1/videos/${id}/content`)).status).toBe(404);
        expect(stack.received.length).toBe(calls);
      },
    ),
  180000,
);

test(
  "a video that fails upstream closes its request as failed and releases the reservation",
  () =>
    withGatewayStack({ reply: upstream("failed", 0), supplierCredit: "1000000" }, async (stack) => {
      const created = await create(stack, { prompt: "海浪" });
      expect(created.status).toBe(200);
      const requestId = created.headers.get("x-request-id")!;
      expect(await settledCharge(stack.db, requestId)).toEqual({
        amount: "0",
        state: "FAILED",
        billing_outcome: "FAILED",
      });
      expect(
        (
          await stack.db`SELECT state,error_code FROM attempts WHERE request_id=${requestId}::uuid`
        )[0],
      ).toEqual({ state: "FAILED", error_code: "upstream_error" });
    }),
  180000,
);

test(
  "a streaming video request is refused before reaching the upstream",
  () =>
    withGatewayStack(
      { reply: upstream("completed", 0), supplierCredit: "1000000" },
      async (stack) => {
        const refused = await create(stack, { prompt: "x", stream: true });
        expect(refused.status).toBe(400);
        expect(stack.received.length).toBe(0);
      },
    ),
  180000,
);
