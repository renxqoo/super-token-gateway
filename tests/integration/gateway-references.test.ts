import { expect, test } from "bun:test";
import { allowOperation, chatReplies, withGatewayStack } from "../support/gateway-process.ts";

const unmanaged: [string, Record<string, unknown>][] = [
  [
    "chat file_id",
    { messages: [{ role: "user", content: [{ type: "file", file: { file_id: "file-other" } }] }] },
  ],
  [
    "responses input_file",
    { input: [{ role: "user", content: [{ type: "input_file", file_id: "file-other" }] }] },
  ],
  ["responses conversation", { input: "hi", conversation: "conv_other" }],
  ["stored prompt", { input: "hi", prompt: { id: "pmpt_other" } }],
  [
    "vector store tool",
    { input: "hi", tools: [{ type: "file_search", vector_store_ids: ["vs_other"] }] },
  ],
];

test(
  "upstream resource identities supplied by a client are refused before the shared upstream account sees them",
  () =>
    withGatewayStack({ reply: () => chatReplies.json() }, async (stack) => {
      await allowOperation(stack, "responses");
      for (const [label, body] of unmanaged) {
        const path = label.startsWith("chat") ? "/v1/chat/completions" : "/v1/responses";
        const response = await fetch(`${stack.gateway.origin}${path}`, {
          method: "POST",
          headers: {
            authorization: `Bearer ${stack.setup.key.key}`,
            "content-type": "application/json",
          },
          body: JSON.stringify({
            model: stack.setup.model,
            ...(label.startsWith("chat") ? { max_tokens: 30 } : { max_output_tokens: 30 }),
            ...body,
          }),
        });
        expect([label, response.status]).toEqual([label, 422]);
        expect(((await response.json()) as { error: { code: string } }).error.code).toBe(
          "unmanaged_resource",
        );
      }
      expect(stack.received).toHaveLength(0);
    }),
  120000,
);
