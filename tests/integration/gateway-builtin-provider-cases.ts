import { expect, test } from "bun:test";
import { createGatewayRuntime } from "@gateway/gateway";
import { createMockApp } from "../../apps/mock-providers/src/app.ts";
import { listBuiltinProviders } from "@gateway/plugin-engine";
import { makeConfig, db, internalCall } from "./gateway-fixture.ts";

export function registerGatewayBuiltinProviderCases() {
  test("all builtin provider presets complete JSON and SSE requests through independent upstream fixtures", async () => {
    const upstream = createMockApp("builtin-matrix");
    const listener = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: upstream.app.fetch });
    const runtime = createGatewayRuntime({ config: makeConfig(), db });
    const paths: Record<string, string> = {
      openai: "/openai",
      openai_compatible: "/openai-compatible",
      anthropic: "/anthropic",
      gemini: "/gemini",
      azure: "/azure",
      bedrock: "/bedrock",
      vertex: "/vertex/v1/projects/test/locations/test/publishers/google",
      deepseek: "/deepseek",
      ollama: "/ollama",
      qwen: "/qwen",
      volcengine: "/volcengine",
      zhipu: "/zhipu",
      moonshot: "/moonshot",
      minimax: "/minimax",
      openrouter: "/openrouter",
    };
    const credentials: Record<string, string> = {
      vertex: JSON.stringify({
        access_token: "fixture-vertex",
        expires_at: new Date(Date.now() + 60_000).toISOString(),
      }),
      bedrock: JSON.stringify({
        accessKeyId: "AKIAFIXTURE00000000",
        secretAccessKey: "fixtureSecretKeyForSignatureTesting",
        region: "us-east-1",
      }),
    };

    try {
      await runtime.start();
      const put = async (path: string, body: unknown) => {
        const result = await internalCall(runtime.internalApp, "PUT", path, body);
        expect(result.status, JSON.stringify(result.data.error)).toBe(200);
        return result.data;
      };
      const billingAccount = await put("/internal/v1/billing-accounts", {
        name: "builtin-matrix",
        currency: "USD",
      });
      const key = await put("/internal/v1/api-keys", {
        billing_account_id: billingAccount.id,
        name: "fixture",
      });
      const account = await put("/internal/v1/upstream-accounts", { name: "builtin-matrix" });
      await runtime.ledger.creditWallet({
        billing_account_id: String(billingAccount.id),
        kind: "CASH",
        currency: "USD",
        external_ref: "builtin-matrix-credit",
        amount_atoms: "10000000",
      });
      await runtime.ledger.rechargeSupplier({
        account_id: String(account.id),
        currency: "USD",
        external_ref: "builtin-matrix-recharge",
        amount_atoms: "10000000",
      });
      const prices = await Promise.all(
        ["CUSTOMER", "SUPPLIER"].map((side) =>
          runtime.catalog.createPriceVersion({
            side: side as "CUSTOMER" | "SUPPLIER",
            currency: "USD",
            rates: {
              input_tokens: { numerator: "2", denominator: "1" },
              cached_input_tokens: { numerator: "1", denominator: "1" },
              output_tokens: { numerator: "3", denominator: "1" },
            },
            ceilings: { input_tokens: "1000", cached_input_tokens: "1000", output_tokens: "128" },
          }),
        ),
      );
      await runtime.snapshot.load();

      expect(
        listBuiltinProviders()
          .map((provider) => provider.name)
          .sort(),
      ).toEqual(Object.keys(paths).sort());
      for (const [provider, path] of Object.entries(paths)) {
        const model = `builtin-${provider}`;
        const upstreamModel = `fixture-${provider}`;
        const output = `response from ${provider}`;
        upstream.scenarios.set(upstreamModel, { output });
        const channel = await put("/internal/v1/channels", {
          account_id: account.id,
          name: model,
          kind: provider,
          base_url: `http://127.0.0.1:${listener.port}${path}`,
          api_key: credentials[provider] ?? "fixture-upstream-key",
        });
        await put("/internal/v1/models", {
          name: model,
          operations: ["chat"],
          max_output_tokens: 128,
        });
        await put("/internal/v1/routes", {
          model_name: model,
          upstream_model: upstreamModel,
          channel_id: channel.id,
          operations: ["chat"],
          billing_price_id: prices[0]!.id,
          cost_version_id: prices[1]!.id,
        });

        for (const stream of [false, true]) {
          const response = await runtime.publicApp.fetch(
            new Request("http://gw.local/v1/chat/completions", {
              method: "POST",
              headers: { authorization: `Bearer ${key.key}`, "content-type": "application/json" },
              body: JSON.stringify({
                model,
                stream,
                max_completion_tokens: 32,
                messages: [{ role: "user", content: "Hello" }],
              }),
            }),
          );
          const wire = await response.text();
          const mode = `${provider} ${stream ? "stream" : "JSON"}`;
          expect(response.status, `${mode}: ${wire}`).toBe(200);
          if (stream) {
            const data = wire.split("\n").filter((line) => line.startsWith("data: "));
            const content = data
              .filter((line) => line !== "data: [DONE]")
              .map((line) => {
                const frame = JSON.parse(line.slice(6)) as {
                  choices?: { delta?: { content?: string } }[];
                };
                return frame.choices?.[0]?.delta?.content ?? "";
              })
              .join("");
            expect(content, mode).toBe(output);
            expect(data.at(-1), mode).toBe("data: [DONE]");
          } else {
            expect(JSON.parse(wire).choices[0].message.content, mode).toBe(output);
          }
          const billing = await runtime.queries.getRequestBilling(
            response.headers.get("x-request-id")!,
          );
          expect(billing.charge?.usage, mode).toMatchObject({
            input_tokens: "7",
            cached_input_tokens: "3",
            output_tokens: "5",
          });
        }
      }

      expect(upstream.stats.requests).toBe(30);
      expect(await runtime.ledger.auditConservation()).toMatchObject({
        ok: true,
        open_holds: 0,
        unknown_holds: 0,
      });
    } finally {
      await runtime.close();
      await listener.stop(true);
    }
  });
}
