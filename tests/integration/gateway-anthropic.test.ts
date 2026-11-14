import { expect, test } from "bun:test";
import { anthropicMessagesReleaseId } from "@gateway/plugin-sdk";
import { syntheticAdminKey } from "../support/catalog.ts";
import { settledCharge, withGatewayStack, type UpstreamCall } from "../support/gateway-process.ts";

const message = {
  id: "msg_synthetic",
  type: "message",
  role: "assistant",
  content: [{ type: "text", text: "合成回复" }],
  model: "claude-synthetic",
  stop_reason: "end_turn",
  usage: { input_tokens: 30, output_tokens: 15 },
};
const events = [
  [
    "message_start",
    {
      type: "message_start",
      message: { ...message, content: [], usage: { input_tokens: 30, output_tokens: 1 } },
    },
  ],
  ["ping", { type: "ping" }],
  [
    "content_block_start",
    { type: "content_block_start", index: 0, content_block: { type: "text", text: "" } },
  ],
  [
    "content_block_delta",
    { type: "content_block_delta", index: 0, delta: { type: "text_delta", text: "合成" } },
  ],
  [
    "content_block_delta",
    { type: "content_block_delta", index: 0, delta: { type: "text_delta", text: "回复" } },
  ],
  ["content_block_stop", { type: "content_block_stop", index: 0 }],
  [
    "message_delta",
    {
      type: "message_delta",
      delta: { stop_reason: "end_turn", stop_sequence: null },
      usage: { output_tokens: 15 },
    },
  ],
  ["message_stop", { type: "message_stop" }],
] as const;

function reply(call: UpstreamCall) {
  if (call.body.stream)
    return new Response(
      events.map(([name, data]) => `event: ${name}\ndata: ${JSON.stringify(data)}\n\n`).join(""),
      { headers: { "content-type": "text/event-stream" } },
    );
  return Response.json(message);
}

test(
  "a channel bound to the bundled Anthropic plugin serves chat JSON and SSE through the Gateway with x-api-key, message bodies and exact settlement",
  () =>
    withGatewayStack({ reply }, async (stack) => {
      const { f, setup } = stack,
        catalog = f.catalog;
      const installed = await catalog.application.installBundled(await catalog.actor(), {
        command_id: crypto.randomUUID(),
        release_id: anthropicMessagesReleaseId,
        reason: "Synthetic Anthropic installation",
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
      const body = (await json.json()) as Record<string, unknown>;
      expect(body).toMatchObject({
        id: "msg_synthetic",
        object: "chat.completion",
        model: setup.model,
        choices: [
          { index: 0, message: { role: "assistant", content: "合成回复" }, finish_reason: "stop" },
        ],
        usage: { prompt_tokens: 30, completion_tokens: 15, total_tokens: 45 },
      });
      const call = stack.received.at(-1)!;
      expect(call.path).toBe("/v1/messages");
      expect(call.apiKey).toBeString();
      expect(call.authorization).toBeNull();
      expect(call.body).toEqual({
        model: "synthetic",
        max_tokens: 30,
        messages: [{ role: "user", content: "你好" }],
      });
      expect(await settledCharge(stack.db, json.headers.get("x-request-id")!)).toMatchObject({
        state: "SUCCEEDED",
      });
      const streamed = await stack.send({ stream: true });
      expect(streamed.status).toBe(200);
      const text = await streamed.text();
      const frames = text
        .split("\n\n")
        .filter(Boolean)
        .map((frame) => frame.replace(/^data: /, ""));
      expect(text).not.toContain("event:");
      expect(frames.at(-1)).toBe("[DONE]");
      const chunks = frames.slice(0, -1).map((frame) => JSON.parse(frame));
      expect(chunks.map((c) => c.choices[0]?.delta?.content ?? "").join("")).toBe("合成回复");
      expect(chunks.at(-2).choices[0].finish_reason).toBe("stop");
      expect(chunks.at(-1).usage).toMatchObject({ prompt_tokens: 30, completion_tokens: 15 });
      expect(stack.received.at(-1)!.body.stream).toBe(true);
      expect(await settledCharge(stack.db, streamed.headers.get("x-request-id")!)).toMatchObject({
        state: "SUCCEEDED",
      });
    }),
  120000,
);
