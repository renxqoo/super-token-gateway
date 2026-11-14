import { expect, test } from "bun:test";
import { geminiGenerateReleaseId } from "@gateway/plugin-sdk";
import { syntheticAdminKey } from "../support/catalog.ts";
import { settledCharge, withGatewayStack, type UpstreamCall } from "../support/gateway-process.ts";

const usage = { promptTokenCount: 30, candidatesTokenCount: 12, thoughtsTokenCount: 3 };
const frames = [
  { responseId: "gem-1", candidates: [{ content: { role: "model", parts: [{ text: "合成" }] } }] },
  {
    responseId: "gem-1",
    candidates: [{ content: { role: "model", parts: [{ text: "回复" }] }, finishReason: "STOP" }],
    usageMetadata: usage,
  },
];

function reply(call: UpstreamCall) {
  if (call.path.includes(":streamGenerateContent"))
    return new Response(frames.map((frame) => `data: ${JSON.stringify(frame)}\r\n\r\n`).join(""), {
      headers: { "content-type": "text/event-stream" },
    });
  return Response.json({
    responseId: "gem-1",
    candidates: [
      { content: { role: "model", parts: [{ text: "合成回复" }] }, finishReason: "STOP" },
    ],
    usageMetadata: usage,
  });
}

test(
  "a channel bound to the bundled Gemini plugin serves chat JSON and SSE through the Gateway with x-goog-api-key and exact settlement",
  () =>
    withGatewayStack({ reply }, async (stack) => {
      const { f, setup } = stack,
        catalog = f.catalog;
      const installed = await catalog.application.installBundled(await catalog.actor(), {
        command_id: crypto.randomUUID(),
        release_id: geminiGenerateReleaseId,
        reason: "Synthetic Gemini installation",
      });
      const bound = await catalog.request(
        `/providers/${setup.provider.id}/plugin-binding`,
        {
          ...catalog.binding(installed.version_id),
          expected_revision: "1",
          account_id: setup.provider.account_id,
          brand_id: setup.provider.brand_id,
          secret_reference: `channel:${setup.provider.id}`,
          config: {
            model_map: { [setup.model]: "synthetic" },
            max_output_tokens: 100,
            max_input_tokens: 1000,
            auth: "api-key",
          },
        },
        syntheticAdminKey,
        "PUT",
      );
      expect(bound.status).toBe(201);
      const json = await stack.send({ content: "你好" });
      expect(json.status).toBe(200);
      expect(await json.json()).toMatchObject({
        id: "gem-1",
        object: "chat.completion",
        model: setup.model,
        choices: [
          { index: 0, message: { role: "assistant", content: "合成回复" }, finish_reason: "stop" },
        ],
        usage: {
          prompt_tokens: 30,
          completion_tokens: 15,
          total_tokens: 45,
          completion_tokens_details: { reasoning_tokens: 3 },
        },
      });
      const call = stack.received.at(-1)!;
      expect(call.path).toBe("/v1beta/models/synthetic:generateContent");
      expect(call.googKey).toBeString();
      expect(call.googKey!.length).toBeGreaterThan(0);
      expect(call.authorization).toBeNull();
      expect(call.body).toEqual({
        contents: [{ role: "user", parts: [{ text: "你好" }] }],
        generationConfig: { maxOutputTokens: 30 },
      });
      expect(await settledCharge(stack.db, json.headers.get("x-request-id")!)).toMatchObject({
        state: "SUCCEEDED",
      });
      const streamed = await stack.send({ stream: true });
      expect(streamed.status).toBe(200);
      const text = await streamed.text();
      expect(stack.received.at(-1)!.path).toBe(
        "/v1beta/models/synthetic:streamGenerateContent?alt=sse",
      );
      const wire = text
        .split("\n\n")
        .filter(Boolean)
        .map((frame) => frame.replace(/^data: /, ""));
      expect(wire.at(-1)).toBe("[DONE]");
      const chunks = wire.slice(0, -1).map((frame) => JSON.parse(frame));
      expect(chunks.map((c) => c.choices[0]?.delta?.content ?? "").join("")).toBe("合成回复");
      expect(chunks.at(-2).choices[0].finish_reason).toBe("stop");
      expect(chunks.at(-1).usage).toMatchObject({ prompt_tokens: 30, completion_tokens: 15 });
      expect(await settledCharge(stack.db, streamed.headers.get("x-request-id")!)).toMatchObject({
        state: "SUCCEEDED",
      });
    }),
  120000,
);
