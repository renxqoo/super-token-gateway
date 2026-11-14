import { expect, test } from "bun:test";
import { geminiGenerateReleaseId } from "@gateway/plugin-sdk";
import { syntheticAdminKey } from "../support/catalog.ts";
import { requestOutcome, settledCharge, withGatewayStack } from "../support/gateway-process.ts";

const encoded = (value: unknown) => Buffer.from(JSON.stringify(value)).toString("base64url");

test(
  "a Vertex channel exchanges its google-oauth credential in Core, sends only a bearer token to the regional publisher path and never sends with an expired credential",
  () =>
    withGatewayStack(
      {
        reply: () =>
          Response.json({
            responseId: "vertex-1",
            candidates: [
              { content: { role: "model", parts: [{ text: "ok" }] }, finishReason: "STOP" },
            ],
            usageMetadata: { promptTokenCount: 6, candidatesTokenCount: 1 },
          }),
      },
      async (stack) => {
        const { f, setup } = stack,
          catalog = f.catalog;
        const installed = await catalog.application.installBundled(await catalog.actor(), {
          command_id: crypto.randomUUID(),
          release_id: geminiGenerateReleaseId,
          reason: "Synthetic Vertex installation",
        });
        const rotate = async (credential: unknown) =>
          expect(
            (
              await f.request(
                `/providers/${setup.provider.id}`,
                { api_key: encoded(credential) },
                syntheticAdminKey,
                "PATCH",
              )
            ).status,
          ).toBe(200);
        await rotate({
          access_token: "ya29.synthetic-vertex-token",
          expires_at: new Date(Date.now() + 3600000).toISOString(),
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
              auth: "google-oauth",
              path_style: "vertex",
              project: "synthetic-project",
              location: "us-central1",
            },
          },
          syntheticAdminKey,
          "PUT",
        );
        expect(bound.status).toBe(201);
        const response = await stack.send({ content: "hi" });
        expect(response.status).toBe(200);
        expect(await response.json()).toMatchObject({
          id: "vertex-1",
          choices: [{ message: { content: "ok" }, finish_reason: "stop" }],
          usage: { prompt_tokens: 6, completion_tokens: 1 },
        });
        const call = stack.received.at(-1)!;
        expect(call.path).toBe(
          "/v1/projects/synthetic-project/locations/us-central1/publishers/google/models/synthetic:generateContent",
        );
        expect(call.authorization).toBe("Bearer ya29.synthetic-vertex-token");
        expect(call.googKey).toBeNull();
        expect(await settledCharge(stack.db, response.headers.get("x-request-id")!)).toMatchObject({
          state: "SUCCEEDED",
        });
        await rotate({
          access_token: "ya29.expired",
          expires_at: new Date(Date.now() - 1000).toISOString(),
        });
        const sent = stack.received.length;
        const refused = await stack.send({ content: "hi" });
        expect(refused.status).toBe(503);
        expect(stack.received.length).toBe(sent);
        const refusedId = refused.headers.get("x-request-id")!;
        expect(await requestOutcome(stack.db, refusedId)).toMatchObject({ charged: "0" });
        expect(
          await stack.db`SELECT state,error_code FROM attempts WHERE request_id=${refusedId}`,
        ).toEqual([{ state: "NOT_EXECUTED", error_code: "send_not_authorized" }]);
      },
    ),
  120000,
);
