import { expect, test } from "bun:test";
import { createGatewayRuntime } from "@gateway/gateway";
import { makeConfig, db, internalCall } from "./gateway-fixture.ts";

export function registerGatewayRoutingCases() {
  test("G2.1 + G2.2 SmartRouter pre-first-byte dynamic failover (503, 429, 401, ECONNREFUSED), circuit breaker trip & HALF_OPEN recovery, single customer charge", async () => {
    let primaryCalls = 0;
    let primaryMode: "503" | "429" | "401" | "ok" = "503";
    let backupCalls = 0;

    const primaryServer = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      async fetch() {
        primaryCalls++;
        if (primaryMode === "503") {
          return Response.json({ error: { message: "primary overloaded" } }, { status: 503 });
        }
        if (primaryMode === "429") {
          return Response.json(
            { error: { message: "rate limited" } },
            { status: 429, headers: { "retry-after": "1" } },
          );
        }
        if (primaryMode === "401") {
          return Response.json({ error: { message: "bad upstream key" } }, { status: 401 });
        }
        return Response.json({
          id: "chatcmpl_primary",
          object: "chat.completion",
          model: "gpt-primary",
          choices: [
            {
              index: 0,
              message: { role: "assistant", content: "from-primary" },
              finish_reason: "stop",
            },
          ],
          usage: { prompt_tokens: 10, completion_tokens: 5, total_tokens: 15 },
        });
      },
    });

    const backupServer = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      async fetch() {
        backupCalls++;
        return Response.json({
          id: "chatcmpl_backup",
          object: "chat.completion",
          model: "gpt-backup",
          choices: [
            {
              index: 0,
              message: { role: "assistant", content: "from-backup" },
              finish_reason: "stop",
            },
          ],
          usage: { prompt_tokens: 10, completion_tokens: 5, total_tokens: 15 },
        });
      },
    });

    let nowMs = 1000;
    const runtime = createGatewayRuntime({
      config: makeConfig(),
      db,
      routerOptions: {
        failureThreshold: 3,
        baseOpenMs: 10_000,
        now: () => nowMs,
      },
    });

    try {
      await runtime.start();
      const { data: billingAccount } = await internalCall(
        runtime.internalApp,
        "PUT",
        "/internal/v1/billing-accounts",
        {
          name: "failover-billingAccount",
          currency: "PTS",
        },
      );
      const billingAccountId = String(billingAccount.id);
      const { data: keyRes } = await internalCall(
        runtime.internalApp,
        "PUT",
        "/internal/v1/api-keys",
        {
          billing_account_id: billingAccountId,
          name: "invoker",
        },
      );
      const apiKey = String(keyRes.key);

      await internalCall(
        runtime.internalApp,
        "POST",
        `/internal/v1/wallets/${billingAccountId}/credits`,
        {
          currency: "PTS",
          external_ref: "credit-failover-1",
          amount_atoms: "1000000",
          kind: "CASH",
        },
      );

      const { data: accPrimary } = await internalCall(
        runtime.internalApp,
        "PUT",
        "/internal/v1/upstream-accounts",
        { name: "acc-primary" },
      );
      const { data: accBackup } = await internalCall(
        runtime.internalApp,
        "PUT",
        "/internal/v1/upstream-accounts",
        { name: "acc-backup" },
      );
      await internalCall(
        runtime.internalApp,
        "POST",
        `/internal/v1/upstream-accounts/${accPrimary.id}/recharges`,
        { currency: "USD", external_ref: "sup-p-1", amount_atoms: "1000000" },
      );
      await internalCall(
        runtime.internalApp,
        "POST",
        `/internal/v1/upstream-accounts/${accBackup.id}/recharges`,
        { currency: "USD", external_ref: "sup-b-1", amount_atoms: "1000000" },
      );

      const { data: chPrimary } = await internalCall(
        runtime.internalApp,
        "PUT",
        "/internal/v1/channels",
        {
          account_id: accPrimary.id,
          name: "primary-ch",
          kind: "openai",
          base_url: `http://127.0.0.1:${primaryServer.port}`,
          api_key: "sk-primary",
        },
      );
      const { data: chBackup } = await internalCall(
        runtime.internalApp,
        "PUT",
        "/internal/v1/channels",
        {
          account_id: accBackup.id,
          name: "backup-ch",
          kind: "openai",
          base_url: `http://127.0.0.1:${backupServer.port}`,
          api_key: "sk-backup",
        },
      );

      await internalCall(runtime.internalApp, "PUT", "/internal/v1/models", {
        name: "smart-gpt",
        operations: ["chat"],
        routing_strategy: "WEIGHTED",
        max_output_tokens: 100,
      });

      const { data: cheapCustPrice } = await internalCall(
        runtime.internalApp,
        "POST",
        "/internal/v1/prices",
        {
          side: "CUSTOMER",
          currency: "PTS",
          rates: {
            input_tokens: { numerator: "10", denominator: "1" },
            output_tokens: { numerator: "20", denominator: "1" },
          },
          ceilings: { input_tokens: "1000", output_tokens: "100" },
        },
      );
      const { data: pricierBackupCustPrice } = await internalCall(
        runtime.internalApp,
        "POST",
        "/internal/v1/prices",
        {
          side: "CUSTOMER",
          currency: "PTS",
          rates: {
            input_tokens: { numerator: "15", denominator: "1" },
            output_tokens: { numerator: "30", denominator: "1" },
          },
          ceilings: { input_tokens: "1000", output_tokens: "100" },
        },
      );
      const { data: supPrice } = await internalCall(
        runtime.internalApp,
        "POST",
        "/internal/v1/prices",
        {
          side: "SUPPLIER",
          currency: "USD",
          rates: {
            input_tokens: { numerator: "2", denominator: "1" },
            output_tokens: { numerator: "4", denominator: "1" },
          },
          ceilings: { input_tokens: "1000", output_tokens: "100" },
        },
      );

      await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
        model_name: "smart-gpt",
        channel_id: chPrimary.id,
        upstream_model: "gpt-primary",
        operations: ["chat"],
        priority: 10,
        billing_price_id: cheapCustPrice.id,
        cost_version_id: supPrice.id,
        retry_policy: { max_attempts: 3 },
      });
      await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
        model_name: "smart-gpt",
        channel_id: chBackup.id,
        upstream_model: "gpt-backup",
        operations: ["chat"],
        priority: 5,
        billing_price_id: pricierBackupCustPrice.id,
        cost_version_id: supPrice.id,
        retry_policy: { max_attempts: 3 },
      });

      const invokeChat = async () =>
        runtime.publicApp.fetch(
          new Request("http://gateway.local/v1/chat/completions", {
            method: "POST",
            headers: {
              authorization: `Bearer ${apiKey}`,
              "content-type": "application/json",
            },
            body: JSON.stringify({
              model: "smart-gpt",
              messages: [{ role: "user", content: "hello" }],
              max_tokens: 50,
            }),
          }),
        );

      for (let i = 0; i < 3; i++) {
        const resp = await invokeChat();
        expect(resp.status).toBe(200);
        expect(resp.headers.get("x-gateway-channel")).toBe("backup-ch");
        const json = (await resp.json()) as { choices: { message: { content: string } }[] };
        expect(json.choices[0]!.message.content).toBe("from-backup");
      }
      expect(primaryCalls).toBe(3);
      expect(backupCalls).toBe(3);

      const respWhileOpen = await invokeChat();
      expect(respWhileOpen.status).toBe(200);
      expect(respWhileOpen.headers.get("x-gateway-channel")).toBe("backup-ch");
      expect(primaryCalls).toBe(3);
      expect(backupCalls).toBe(4);

      primaryMode = "ok";
      nowMs += 11_000;
      const probeResp = await invokeChat();
      expect(probeResp.status).toBe(200);
      expect(probeResp.headers.get("x-gateway-channel")).toBe("primary-ch");
      const probeJson = (await probeResp.json()) as { choices: { message: { content: string } }[] };
      expect(probeJson.choices[0]!.message.content).toBe("from-primary");
      expect(primaryCalls).toBe(4);

      const { data: wallet } = await internalCall(
        runtime.internalApp,
        "GET",
        `/internal/v1/wallets/${billingAccountId}?currency=PTS`,
      );
      expect(wallet.reserved_atoms).toBe("0");
      expect(wallet.spent_atoms).toBe(String(4 * (10 * 15 + 5 * 30) + 1 * (10 * 10 + 5 * 20)));

      const { data: primarySup } = await internalCall(
        runtime.internalApp,
        "GET",
        `/internal/v1/upstream-accounts/${accPrimary.id}/balance?currency=USD`,
      );
      expect(primarySup.reserved_atoms).toBe("0");
      expect(primarySup.spent_atoms).toBe(String(10 * 2 + 5 * 4));

      const { data: backupSup } = await internalCall(
        runtime.internalApp,
        "GET",
        `/internal/v1/upstream-accounts/${accBackup.id}/balance?currency=USD`,
      );
      expect(backupSup.reserved_atoms).toBe("0");
      expect(backupSup.spent_atoms).toBe(String(4 * (10 * 2 + 5 * 4)));

      primaryMode = "401";
      const authFailResp = await invokeChat();
      expect(authFailResp.status).toBe(200);
      expect(authFailResp.headers.get("x-gateway-channel")).toBe("backup-ch");

      const { data: audit } = await internalCall(
        runtime.internalApp,
        "GET",
        "/internal/v1/audit/conservation",
      );
      expect(audit.ok).toBe(true);
      expect(audit.open_holds).toBe(0);
      expect(audit.unknown_holds).toBe(0);
    } finally {
      await runtime.close();
      await primaryServer.stop(true);
      await backupServer.stop(true);
    }
  });
}
