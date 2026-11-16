import { expect, test } from "bun:test";
import { bedrockAnthropicReleaseId } from "@gateway/plugin-sdk";
import { awsSigningKey, signAwsRequest } from "@gateway/runtime";
import { bedrockChunk } from "../support/aws-eventstream.ts";
import { syntheticAdminKey } from "../support/catalog.ts";
import { settledCharge, withGatewayStack, type UpstreamCall } from "../support/gateway-process.ts";

const credentials = {
  accessKeyId: "AKIDSYNTHETIC",
  secretAccessKey: "c3ludGhldGljLXNlY3JldC1hY2Nlc3Mta2V5",
  region: "us-east-1",
};
const message = {
  id: "msg_bedrock",
  type: "message",
  role: "assistant",
  content: [{ type: "text", text: "合成回复" }],
  model: "claude",
  stop_reason: "end_turn",
  usage: { input_tokens: 30, output_tokens: 15 },
};
const events = [
  {
    type: "message_start",
    message: { ...message, content: [], usage: { input_tokens: 30, output_tokens: 1 } },
  },
  { type: "content_block_start", index: 0, content_block: { type: "text", text: "" } },
  { type: "content_block_delta", index: 0, delta: { type: "text_delta", text: "合成" } },
  { type: "content_block_delta", index: 0, delta: { type: "text_delta", text: "回复" } },
  { type: "content_block_stop", index: 0 },
  { type: "message_delta", delta: { stop_reason: "end_turn" }, usage: { output_tokens: 15 } },
  { type: "message_stop", "amazon-bedrock-invocationMetrics": { inputTokenCount: 30 } },
];

function reply(call: UpstreamCall) {
  if (call.path.endsWith("/invoke-with-response-stream"))
    return new Response(Buffer.concat(events.map(bedrockChunk)), {
      headers: { "content-type": "application/vnd.amazon.eventstream" },
    });
  return Response.json(message);
}

function expectedSignature(call: UpstreamCall) {
  const authorization = call.headers.authorization!;
  const signed = /SignedHeaders=([^,]+)/.exec(authorization)![1]!.split(";");
  const headers = new Headers(
    signed.filter((name) => name !== "host").map((name) => [name, call.headers[name]!]),
  );
  const amzDate = call.headers["x-amz-date"]!;
  signAwsRequest(new URL(call.url), call.raw, headers, {
    accessKeyId: credentials.accessKeyId,
    region: credentials.region,
    service: "bedrock",
    amzDate,
    signingKey: awsSigningKey(
      credentials.secretAccessKey,
      amzDate.slice(0, 8),
      credentials.region,
      "bedrock",
    ),
  });
  return headers.get("authorization");
}

test(
  "a Bedrock channel signs the final request in the Gateway with a Core-derived key and decodes the binary event stream into chat SSE with exact settlement",
  () =>
    withGatewayStack({ reply }, async (stack) => {
      const { f, setup } = stack,
        catalog = f.catalog;
      const installed = await catalog.application.installBundled(await catalog.actor(), {
        command_id: crypto.randomUUID(),
        release_id: bedrockAnthropicReleaseId,
        reason: "Synthetic Bedrock installation",
      });
      expect(
        (
          await f.request(
            `/providers/${setup.provider.id}`,
            { api_key: JSON.stringify(credentials) },
            syntheticAdminKey,
            "PATCH",
          )
        ).status,
      ).toBe(200);
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
            auth: "aws-sigv4",
          },
        },
        syntheticAdminKey,
        "PUT",
      );
      expect(bound.status).toBe(201);
      const json = await stack.send({ content: "你好" });
      expect(json.status).toBe(200);
      expect(await json.json()).toMatchObject({
        id: "msg_bedrock",
        choices: [{ message: { content: "合成回复" }, finish_reason: "stop" }],
        usage: { prompt_tokens: 30, completion_tokens: 15 },
      });
      const call = stack.received.at(-1)!;
      expect(call.path).toBe("/model/synthetic/invoke");
      expect(call.body).toEqual({
        anthropic_version: "bedrock-2023-05-31",
        max_tokens: 30,
        messages: [{ role: "user", content: "你好" }],
      });
      expect(call.authorization).toStartWith(
        `AWS4-HMAC-SHA256 Credential=AKIDSYNTHETIC/${call.headers["x-amz-date"]!.slice(0, 8)}/us-east-1/bedrock/aws4_request`,
      );
      expect(call.authorization).toBe(expectedSignature(call));
      expect(call.raw).not.toContain(credentials.secretAccessKey);
      expect(await settledCharge(stack.db, json.headers.get("x-request-id")!)).toMatchObject({
        state: "SUCCEEDED",
      });
      const streamed = await stack.send({ stream: true });
      expect(streamed.status).toBe(200);
      const text = await streamed.text();
      const streamCall = stack.received.at(-1)!;
      expect(streamCall.path).toBe("/model/synthetic/invoke-with-response-stream");
      expect(streamCall.headers["x-amzn-bedrock-accept"]).toBe("application/json");
      expect(streamCall.authorization).toBe(expectedSignature(streamCall));
      const frames = text
        .split("\n\n")
        .filter(Boolean)
        .map((frame) => frame.replace(/^data: /, ""));
      expect(frames.at(-1)).toBe("[DONE]");
      const chunks = frames.slice(0, -1).map((frame) => JSON.parse(frame));
      expect(chunks.map((c) => c.choices[0]?.delta?.content ?? "").join("")).toBe("合成回复");
      expect(chunks.at(-1).usage).toMatchObject({ prompt_tokens: 30, completion_tokens: 15 });
      expect(await settledCharge(stack.db, streamed.headers.get("x-request-id")!)).toMatchObject({
        state: "SUCCEEDED",
      });
    }),
  120000,
);
