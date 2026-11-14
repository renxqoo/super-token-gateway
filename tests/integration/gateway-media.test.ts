import { expect, test } from "bun:test";
import {
  allowOperation,
  eventually,
  requestOutcome,
  settledCharge,
  withGatewayStack,
  type UpstreamCall,
} from "../support/gateway-process.ts";

const imageTariff = {
  pricing: { currency: "USD", rates: { images: { numerator: "50", denominator: "1" } } },
  ceilings: { images: "8" },
  basis: "PROVIDER_USAGE",
};
const png = new Uint8Array([137, 80, 78, 71, 0, 255, 13, 10, 45, 45, 13, 10]);
const completed = {
  type: "image_generation.completed",
  b64_json: "QUJD",
  usage: { input_tokens: 5, output_tokens: 7 },
};
const partial = { type: "image_generation.partial_image", b64_json: "AAA", partial_image_index: 0 };

function imageReply(call: UpstreamCall) {
  if (call.path === "/v1/images/generations" && call.body.stream)
    return new Response(
      [partial, completed]
        .map((data) => `event: ${data.type}\ndata: ${JSON.stringify(data)}\n\n`)
        .join(""),
      { headers: { "content-type": "text/event-stream" } },
    );
  const count =
    call.path === "/v1/images/generations" ? Number(call.body.prompt === "两张" ? 2 : 1) : 1;
  return Response.json({
    created: 1,
    data: Array.from({ length: count }, (_, index) => ({ b64_json: `aW1n${index}` })),
  });
}

test(
  "images are reserved per requested image and settled per delivered image; multipart edits reach the upstream with the original file bytes; image SSE holds the completed event",
  () =>
    withGatewayStack({ reply: imageReply, supplierCredit: "1000000" }, async (stack) => {
      await allowOperation(stack, "images", imageTariff);
      await allowOperation(stack, "image_edits", imageTariff);
      const headers = { authorization: `Bearer ${stack.setup.key.key}` };
      const generate = (body: Record<string, unknown>) =>
        fetch(`${stack.gateway.origin}/v1/images/generations`, {
          method: "POST",
          headers: { ...headers, "content-type": "application/json" },
          body: JSON.stringify({ model: stack.setup.model, ...body }),
        });

      const two = await generate({ prompt: "两张", n: 2 });
      expect(two.status).toBe(200);
      expect(((await two.json()) as { data: unknown[] }).data).toHaveLength(2);
      expect(stack.received.at(-1)).toMatchObject({
        path: "/v1/images/generations",
        body: { model: "synthetic", prompt: "两张", n: 2 },
      });
      expect(await settledCharge(stack.db, two.headers.get("x-request-id")!)).toMatchObject({
        amount: "100",
        state: "SUCCEEDED",
      });
      const reserved = await stack.db`
        SELECT billing_quote->>'amount_atoms' AS amount FROM gateway_requests
        WHERE id=${two.headers.get("x-request-id")!}`;
      expect(reserved).toEqual([{ amount: "100" }]);

      const fewer = await generate({ prompt: "一张", n: 2 });
      expect(fewer.status).toBe(200);
      expect(await settledCharge(stack.db, fewer.headers.get("x-request-id")!)).toMatchObject({
        amount: "50",
        state: "SUCCEEDED",
      });

      expect((await generate({ prompt: "太多", n: 9 })).status).toBe(422);

      const form = new FormData();
      form.append("model", stack.setup.model);
      form.append("prompt", "加帽子");
      form.append("n", "1");
      form.append("image", new File([png], "cat.png", { type: "image/png" }));
      const edited = await fetch(`${stack.gateway.origin}/v1/images/edits`, {
        method: "POST",
        headers,
        body: form,
      });
      expect(edited.status).toBe(200);
      const call = stack.received.at(-1)!;
      expect(call.path).toBe("/v1/images/edits");
      const upstreamForm = await new Response(call.bytes.slice(), {
        headers: { "content-type": call.headers["content-type"]! },
      }).formData();
      expect(upstreamForm.get("model")).toBe("synthetic");
      expect(upstreamForm.get("prompt")).toBe("加帽子");
      const file = upstreamForm.get("image") as File;
      expect(file.name).toBe("cat.png");
      expect(file.type).toBe("image/png");
      expect(new Uint8Array(await file.arrayBuffer())).toEqual(png);
      expect(await settledCharge(stack.db, edited.headers.get("x-request-id")!)).toMatchObject({
        amount: "50",
        state: "SUCCEEDED",
      });

      const streamed = await generate({ prompt: "流", stream: true });
      expect(streamed.status).toBe(200);
      expect(await streamed.text()).toBe(
        [partial, completed]
          .map((data) => `event: ${data.type}\ndata: ${JSON.stringify(data)}\n\n`)
          .join(""),
      );
      expect(await settledCharge(stack.db, streamed.headers.get("x-request-id")!)).toMatchObject({
        amount: "50",
        state: "SUCCEEDED",
      });
    }),
  120000,
);

const audio = new Uint8Array([73, 68, 51, 4, 0, 255, 254, 0, 13, 10]);

function audioReply(call: UpstreamCall) {
  if (call.path === "/v1/audio/speech" && call.body.input === "中断")
    return new Response(
      new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(audio);
        },
        async pull(controller) {
          await Bun.sleep(200);
          controller.error(new Error("upstream reset"));
        },
      }),
      { headers: { "content-type": "audio/mpeg" } },
    );
  if (call.path === "/v1/audio/speech")
    return new Response(audio, { headers: { "content-type": "audio/mpeg" } });
  if (call.path === "/v1/audio/transcriptions")
    return new Response('{"text":"你好","usage":{"type":"duration","seconds":0.125}}', {
      headers: { "content-type": "application/json" },
    });
  return new Response("hello\n", { headers: { "content-type": "text/plain; charset=utf-8" } });
}

test(
  "speech audio is relayed byte for byte and billed by input characters; transcription bills exact audio milliseconds; text translations are relayed and billed per request",
  () =>
    withGatewayStack({ reply: audioReply, supplierCredit: "1000000" }, async (stack) => {
      await allowOperation(stack, "speech", {
        pricing: { currency: "USD", rates: { characters: { numerator: "3", denominator: "1" } } },
        ceilings: { characters: "100" },
        basis: "INPUT_CHARACTERS",
      });
      await allowOperation(stack, "transcription", {
        pricing: { currency: "USD", rates: { audio_ms: { numerator: "1", denominator: "1" } } },
        ceilings: { audio_ms: "500" },
        basis: "PROVIDER_USAGE",
      });
      await allowOperation(stack, "translation");
      const headers = { authorization: `Bearer ${stack.setup.key.key}` };

      const spoken = await fetch(`${stack.gateway.origin}/v1/audio/speech`, {
        method: "POST",
        headers: { ...headers, "content-type": "application/json" },
        body: JSON.stringify({ model: stack.setup.model, input: "你好世界", voice: "alloy" }),
      });
      expect(spoken.status).toBe(200);
      expect(spoken.headers.get("content-type")).toBe("audio/mpeg");
      expect(new Uint8Array(await spoken.arrayBuffer())).toEqual(audio);
      expect(stack.received.at(-1)).toMatchObject({
        path: "/v1/audio/speech",
        body: { model: "synthetic", input: "你好世界", voice: "alloy" },
      });
      expect(await settledCharge(stack.db, spoken.headers.get("x-request-id")!)).toMatchObject({
        amount: "12",
        state: "SUCCEEDED",
      });

      const upload = (path: string, fields: Record<string, string>) => {
        const form = new FormData();
        form.append("model", stack.setup.model);
        for (const [name, value] of Object.entries(fields)) form.append(name, value);
        form.append("file", new File([audio], "a.mp3", { type: "audio/mpeg" }));
        return fetch(`${stack.gateway.origin}${path}`, { method: "POST", headers, body: form });
      };
      const transcribed = await upload("/v1/audio/transcriptions", {});
      expect(transcribed.status).toBe(200);
      expect(await transcribed.json()).toEqual({
        text: "你好",
        usage: { type: "duration", seconds: 0.125 },
      });
      expect(await settledCharge(stack.db, transcribed.headers.get("x-request-id")!)).toMatchObject(
        { amount: "125", state: "SUCCEEDED" },
      );

      const translated = await upload("/v1/audio/translations", { response_format: "text" });
      expect(translated.status).toBe(200);
      expect(translated.headers.get("content-type")).toBe("text/plain; charset=utf-8");
      expect(await translated.text()).toBe("hello\n");
      const call = stack.received.at(-1)!;
      const sent = await new Response(call.bytes.slice(), {
        headers: { "content-type": call.headers["content-type"]! },
      }).formData();
      expect(sent.get("model")).toBe("synthetic");
      expect(sent.get("response_format")).toBe("text");
      expect(new Uint8Array(await (sent.get("file") as File).arrayBuffer())).toEqual(audio);
      expect(await settledCharge(stack.db, translated.headers.get("x-request-id")!)).toMatchObject({
        amount: "100",
        state: "SUCCEEDED",
      });

      const unsupported = await upload("/v1/audio/translations", { stream: "true" });
      expect(unsupported.status).toBe(422);

      await expect(
        fetch(`${stack.gateway.origin}/v1/audio/speech`, {
          method: "POST",
          headers: { ...headers, "content-type": "application/json" },
          body: JSON.stringify({ model: stack.setup.model, input: "中断", voice: "alloy" }),
        }).then((response) => response.arrayBuffer()),
      ).rejects.toThrow();
      let cutId = "";
      await eventually(async () => {
        const rows = await stack.db`
          SELECT request_id FROM attempts WHERE error_code='binary_interrupted'`;
        cutId = rows[0]?.request_id ?? "";
        return rows.length === 1;
      }, "interrupted speech attempt");
      expect(await requestOutcome(stack.db, cutId)).toEqual({
        state: "FAILED",
        billing_outcome: "FAILED",
        charged: "0",
      });
      expect(
        await stack.db`SELECT state,error_code FROM attempts WHERE request_id=${cutId}`,
      ).toEqual([{ state: "UNKNOWN", error_code: "binary_interrupted" }]);
    }),
  120000,
);
