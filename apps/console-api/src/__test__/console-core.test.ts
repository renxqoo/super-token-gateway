import { expect, test } from "bun:test";
import type { Pricing } from "@gateway/kernel";
import {
  constantTimeEquals,
  isSafePublicWebhookUrl,
  isValidCidr,
  LoginRateLimiter,
  maskSecret,
} from "../auth.ts";
import { loadConsoleConfigFromEnv } from "../config.ts";
import {
  buildSdkSnippets,
  resolveMarketplacePricing,
  type MarketplacePriceSnapshot,
} from "../marketplace.ts";

test("maskSecret, isValidCidr, and isSafePublicWebhookUrl enforce input safety", () => {
  expect(maskSecret("")).toBe("");
  expect(maskSecret("short")).toBe("sh***");
  expect(maskSecret("sk-proj-1234567890abcdef")).toBe("sk-pro***cdef");

  expect(isValidCidr("203.0.113.10")).toBe(true);
  expect(isValidCidr("203.0.113.0/24")).toBe(true);
  expect(isValidCidr("2001:db8::/32")).toBe(true);
  expect(isValidCidr("not-a-cidr")).toBe(false);
  expect(isValidCidr("203.0.113.0/99")).toBe(false);

  expect(isSafePublicWebhookUrl("https://hooks.example.com/alerts")).toBe(true);
  expect(isSafePublicWebhookUrl("http://hooks.example.com/alerts")).toBe(false);
  expect(isSafePublicWebhookUrl("https://127.0.0.1/hook")).toBe(false);
  expect(isSafePublicWebhookUrl("https://169.254.169.254/latest")).toBe(false);
  expect(isSafePublicWebhookUrl("https://localhost/hook")).toBe(false);
});

test("LoginRateLimiter blocks brute-force bursts, resists flood eviction, and resets on success", () => {
  const limiter = new LoginRateLimiter(3, 60_000, 5);
  const t0 = 1_000_000;
  limiter.recordFailure("email:victim@b.com", t0);
  limiter.recordFailure("email:victim@b.com", t0 + 100);
  limiter.recordFailure("email:victim@b.com", t0 + 200);
  for (let i = 0; i < 10; i++) {
    limiter.recordFailure(`email:flood${i}@b.com`, t0 + 250);
  }
  expect(() => limiter.check("email:victim@b.com", t0 + 300)).toThrow(/Too many/);
  expect(() => limiter.check("email:victim@b.com", t0 + 61_000)).not.toThrow();
  limiter.recordFailure("email:x@y.com", t0);
  limiter.reset("email:x@y.com");
  expect(() => limiter.check("email:x@y.com", t0 + 10)).not.toThrow();
});

test("resolveMarketplacePricing applies exact rational multipliers and ignores mismatched EXACT_PRICE without shadowing wildcard MULTIPLIER", () => {
  const listRates: Pricing["rates"] = {
    input_tokens: { numerator: "10", denominator: "1" },
    output_tokens: { numerator: "30", denominator: "1" },
  };
  const customRates: Pricing["rates"] = {
    input_tokens: { numerator: "4", denominator: "1" },
    output_tokens: { numerator: "12", denominator: "1" },
  };
  const exactMap = new Map<string, MarketplacePriceSnapshot>([
    ["price-exact-1", { side: "CUSTOMER", currency: "USD", rates: customRates }],
    ["price-wrong-curr", { side: "CUSTOMER", currency: "EUR", rates: customRates }],
  ]);

  const overrides = [
    {
      id: "ovr-global",
      modelName: "*",
      operation: "*",
      overrideKind: "MULTIPLIER" as const,
      billingPriceId: null,
      multiplierNumerator: "9",
      multiplierDenominator: "10",
      enabled: true,
    },
    {
      id: "ovr-model-mult",
      modelName: "gpt-4o",
      operation: "*",
      overrideKind: "MULTIPLIER" as const,
      billingPriceId: null,
      multiplierNumerator: "75",
      multiplierDenominator: "100",
      enabled: true,
    },
    {
      id: "ovr-exact",
      modelName: "claude-sonnet",
      operation: "messages",
      overrideKind: "EXACT_PRICE" as const,
      billingPriceId: "price-exact-1",
      multiplierNumerator: null,
      multiplierDenominator: null,
      enabled: true,
    },
    {
      id: "ovr-exact-wrong-curr",
      modelName: "mismatched-model",
      operation: "*",
      overrideKind: "EXACT_PRICE" as const,
      billingPriceId: "price-wrong-curr",
      multiplierNumerator: null,
      multiplierDenominator: null,
      enabled: true,
    },
  ];

  const exactRes = resolveMarketplacePricing({
    modelName: "claude-sonnet",
    operation: "messages",
    currency: "USD",
    listRates,
    overrides,
    exactPricesById: exactMap,
  });
  expect(exactRes.effective_rates).toEqual(customRates);
  expect(exactRes.override_applied?.override_kind).toBe("EXACT_PRICE");

  const multRes = resolveMarketplacePricing({
    modelName: "gpt-4o",
    operation: "chat",
    currency: "USD",
    listRates,
    overrides,
    exactPricesById: exactMap,
  });
  expect(multRes.effective_rates.input_tokens).toEqual({ numerator: "15", denominator: "2" });
  expect(multRes.effective_rates.output_tokens).toEqual({ numerator: "45", denominator: "2" });

  const fallbackRes = resolveMarketplacePricing({
    modelName: "mismatched-model",
    operation: "chat",
    currency: "USD",
    listRates,
    overrides,
    exactPricesById: exactMap,
  });
  expect(fallbackRes.override_applied?.id).toBe("ovr-global");
  expect(fallbackRes.effective_rates.input_tokens).toEqual({ numerator: "9", denominator: "1" });
});

test("buildSdkSnippets and loadConsoleConfigFromEnv enforce required config", () => {
  const snippets = buildSdkSnippets("https://api.example.com/", "gpt-4o", [
    "chat",
    "messages",
    "videos",
  ]);
  expect(snippets.curl).toContain("https://api.example.com/v1/chat/completions");
  expect(snippets.anthropic_curl).toContain("https://api.example.com/v1/messages");
  expect(snippets.video_curl).toContain("https://api.example.com/v1/videos");

  expect(constantTimeEquals("secret-token", "secret-token")).toBe(true);
  expect(constantTimeEquals("secret-token", "other-token")).toBe(false);

  expect(() => loadConsoleConfigFromEnv({})).toThrow(/CONSOLE_DATABASE_URL/);
  const cfg = loadConsoleConfigFromEnv({
    CONSOLE_DATABASE_URL: "postgres://u:p@127.0.0.1:5432/console_db",
    GATEWAY_ADMIN_TOKEN: "0123456789abcdef0123456789abcdef",
    OUTBOX_WEBHOOK_TOKEN: "webhook-token-0123456789abcdef",
    BOOTSTRAP_ADMIN_EMAIL: "Admin@Example.com",
  });
  expect(cfg.bootstrapAdminEmail).toBe("admin@example.com");
  expect(cfg.port).toBe(8090);
});
