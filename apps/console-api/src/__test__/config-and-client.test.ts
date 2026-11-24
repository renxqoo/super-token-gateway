import { expect, test } from "bun:test";
import { GatewayError } from "@gateway/contracts";
import { loadConsoleConfigFromEnv } from "../config.ts";
import { GatewayClient } from "../gateway-client.ts";

const baseEnv = {
  CONSOLE_DATABASE_URL: "postgres://u:p@127.0.0.1:5432/console_db",
  GATEWAY_ADMIN_TOKEN: "0123456789abcdef0123456789abcdef",
  OUTBOX_WEBHOOK_TOKEN: "webhook-token-0123456789abcdef",
};

test("loadConsoleConfigFromEnv enforces required tokens, bootstrap secret, CIDR list, and 8092/8093 default ports", () => {
  expect(() => loadConsoleConfigFromEnv({})).toThrow(/CONSOLE_DATABASE_URL/);
  expect(() =>
    loadConsoleConfigFromEnv({
      ...baseEnv,
      OUTBOX_WEBHOOK_TOKEN: "short",
    }),
  ).toThrow(/OUTBOX_WEBHOOK_TOKEN/);
  expect(() =>
    loadConsoleConfigFromEnv({
      CONSOLE_DATABASE_URL: baseEnv.CONSOLE_DATABASE_URL,
      OUTBOX_WEBHOOK_TOKEN: baseEnv.OUTBOX_WEBHOOK_TOKEN,
      GATEWAY_CATALOG_TOKEN: "tok-cat",
      GATEWAY_SECURITY_TOKEN: "tok-sec",
      GATEWAY_FINANCE_TOKEN: "tok-fin",
      GATEWAY_AUDITOR_TOKEN: "tok-aud",
    }),
  ).toThrow(/OPS/);
  expect(() =>
    loadConsoleConfigFromEnv({
      CONSOLE_DATABASE_URL: baseEnv.CONSOLE_DATABASE_URL,
      OUTBOX_WEBHOOK_TOKEN: baseEnv.OUTBOX_WEBHOOK_TOKEN,
      GATEWAY_CATALOG_TOKEN: "tok-cat",
      GATEWAY_SECURITY_TOKEN: "tok-sec",
      GATEWAY_FINANCE_TOKEN: "tok-fin",
      GATEWAY_AUDITOR_TOKEN: "tok-aud",
      GATEWAY_OPS_TOKEN: "tok-ops",
      CONSOLE_OUTBOX_PULL_INTERVAL_MS: "1000",
    }),
  ).toThrow(/GATEWAY_EVENTS_TOKEN/);
  expect(() =>
    loadConsoleConfigFromEnv({
      ...baseEnv,
      BOOTSTRAP_ADMIN_EMAIL: "admin@example.com",
    }),
  ).toThrow(/BOOTSTRAP_ADMIN_TOKEN/);
  expect(() =>
    loadConsoleConfigFromEnv({
      ...baseEnv,
      CONSOLE_TRUSTED_PROXY_CIDRS: "not-a-cidr",
    }),
  ).toThrow(/CONSOLE_TRUSTED_PROXY_CIDRS/);

  const cfg = loadConsoleConfigFromEnv({
    ...baseEnv,
    BOOTSTRAP_ADMIN_EMAIL: "Admin@Example.com",
    BOOTSTRAP_ADMIN_TOKEN: "bootstrap-secret-0123456789",
    CONSOLE_TRUSTED_PROXY_CIDRS: "10.0.0.0/8, 127.0.0.1",
    CONSOLE_CLIENT_CORS_ORIGINS: "https://app.example.com",
    CONSOLE_ADMIN_CORS_ORIGINS: "https://admin.internal",
  });
  expect(cfg.port).toBe(8092);
  expect(cfg.adminHost).toBe("127.0.0.1");
  expect(cfg.adminPort).toBe(8093);
  expect(cfg.alertDeliveryIntervalMs).toBe(5_000);
  expect(cfg.bootstrapAdminEmail).toBe("admin@example.com");
  expect(cfg.trustedProxyCidrs).toEqual(["10.0.0.0/8", "127.0.0.1/32"]);
  expect(cfg.clientCorsOrigins).toEqual(["https://app.example.com"]);
  expect(cfg.adminCorsOrigins).toEqual(["https://admin.internal"]);
});

test("GatewayClient sends least-privilege role tokens and maps timeout, auth, non-JSON, and conservation errors", async () => {
  const seenAuth = new Map<string, string>();
  const client = new GatewayClient({
    baseUrl: "http://gateway.internal/",
    tokens: {
      catalog: "tok-catalog",
      security: "tok-security",
      finance: "tok-finance",
      auditor: "tok-auditor",
      ops: "tok-ops",
      events: "tok-events",
    },
    fetchImpl: async (input, init) => {
      const url =
        typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
      const pathname = new URL(url).pathname;
      const auth = new Headers(init?.headers).get("authorization") ?? "";
      seenAuth.set(pathname, auth);
      if (pathname === "/internal/v1/health") return Response.json({ ok: true });
      if (pathname === "/internal/v1/upstream-accounts") return Response.json({ items: [] });
      if (pathname === "/internal/v1/plugins") return Response.json({ items: [] });
      if (pathname === "/internal/v1/events/claim") {
        return Response.json({ lease_token: "l1", events: [] });
      }
      if (pathname === "/internal/v1/audit/conservation") {
        return Response.json({ ok: false, open_holds: 2, imbalance_atoms: "50" }, { status: 500 });
      }
      return Response.json({});
    },
  });

  await client.getHealth();
  await client.listUpstreamAccounts();
  await client.listPlugins();
  await client.claimOutboxEvents("c1", 10);
  expect(seenAuth.get("/internal/v1/health")).toBe("Bearer tok-ops");
  expect(seenAuth.get("/internal/v1/upstream-accounts")).toBe("Bearer tok-auditor");
  expect(seenAuth.get("/internal/v1/plugins")).toBe("Bearer tok-security");
  expect(seenAuth.get("/internal/v1/events/claim")).toBe("Bearer tok-events");

  const conservation = await client.getConservationAudit();
  expect(conservation.status).toBe(500);
  expect(conservation.data).toEqual({ ok: false, open_holds: 2, imbalance_atoms: "50" });

  const timeoutClient = new GatewayClient({
    baseUrl: "http://gateway.internal",
    tokens: { ops: "tok-ops" },
    fetchImpl: async () => {
      const err = new Error("aborted");
      err.name = "TimeoutError";
      throw err;
    },
  });
  await expect(timeoutClient.getHealth()).rejects.toMatchObject({
    status: 504,
    code: "gateway_timeout",
  } satisfies Partial<GatewayError>);

  const bodyTimeoutClient = new GatewayClient({
    baseUrl: "http://gateway.internal",
    tokens: { ops: "tok-ops" },
    fetchImpl: async () =>
      new Response(
        new ReadableStream({
          pull(controller) {
            controller.error(new DOMException("Response timed out", "TimeoutError"));
          },
        }),
      ),
  });
  await expect(bodyTimeoutClient.getHealth()).rejects.toMatchObject({
    status: 504,
    code: "gateway_timeout",
  });

  const authErrorClient = new GatewayClient({
    baseUrl: "http://gateway.internal",
    tokens: { ops: "tok-ops" },
    fetchImpl: async () => new Response("Unauthorized", { status: 401 }),
  });
  await expect(authErrorClient.getHealth()).rejects.toMatchObject({
    status: 502,
    code: "gateway_auth_error",
  } satisfies Partial<GatewayError>);

  const nonJsonClient = new GatewayClient({
    baseUrl: "http://gateway.internal",
    tokens: { ops: "tok-ops" },
    fetchImpl: async () => new Response("<html>proxy ok</html>", { status: 200 }),
  });
  await expect(nonJsonClient.getHealth()).rejects.toMatchObject({
    status: 502,
    code: "gateway_invalid_response",
  } satisfies Partial<GatewayError>);
});

test("disabling webhook delivery leaves alert evaluation enabled by default", () => {
  const config = loadConsoleConfigFromEnv({ ...baseEnv, CONSOLE_ALERT_DELIVERY_INTERVAL_MS: "0" });
  expect(config.alertDeliveryIntervalMs).toBe(0);
  expect(config.alertEvaluationIntervalMs).toBeGreaterThan(0);
});
