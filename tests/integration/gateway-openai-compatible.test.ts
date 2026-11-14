import { expect, test } from "bun:test";
import { syntheticAdminKey } from "../support/catalog.ts";
import { settledCharge, withGatewayStack } from "../support/gateway-process.ts";

test(
  "a channel bound with the Azure path style reaches the deployment path with api-version and api-key and settles",
  () =>
    withGatewayStack(
      {
        reply: () =>
          Response.json({
            id: "azure-synthetic",
            object: "chat.completion",
            model: "gpt-4o-prod",
            choices: [
              { index: 0, message: { role: "assistant", content: "ok" }, finish_reason: "stop" },
            ],
            usage: { prompt_tokens: 7, completion_tokens: 2, total_tokens: 9 },
          }),
      },
      async (stack) => {
        const { f, setup } = stack;
        const bound = await f.catalog.request(
          `/providers/${setup.provider.id}/plugin-binding`,
          {
            ...f.catalog.binding(setup.publication.version_id),
            expected_revision: "1",
            account_id: setup.provider.account_id,
            brand_id: setup.provider.brand_id,
            secret_reference: `channel:${setup.provider.id}`,
            config: {
              model_map: { [setup.model]: "synthetic" },
              max_output_tokens: 100,
              max_input_tokens: 1000,
              auth: "api-key",
              path_style: "azure",
              api_version: "2024-10-21",
            },
          },
          syntheticAdminKey,
          "PUT",
        );
        expect(bound.status).toBe(201);
        const response = await stack.send({ content: "hi" });
        expect(response.status).toBe(200);
        expect(await response.json()).toMatchObject({
          choices: [{ message: { content: "ok" }, finish_reason: "stop" }],
          usage: { prompt_tokens: 7, completion_tokens: 2 },
        });
        const call = stack.received.at(-1)!;
        expect(call.path).toBe(
          "/openai/deployments/synthetic/chat/completions?api-version=2024-10-21",
        );
        expect(call.authorization).toBeNull();
        expect(call.azureKey).toBeString();
        expect(call.azureKey!.length).toBeGreaterThan(0);
        expect(call.body).toMatchObject({ model: "synthetic", max_completion_tokens: 30 });
        expect(await settledCharge(stack.db, response.headers.get("x-request-id")!)).toMatchObject({
          state: "SUCCEEDED",
        });
      },
    ),
  120000,
);
