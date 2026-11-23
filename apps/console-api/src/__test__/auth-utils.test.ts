import { expect, test } from "bun:test";
import type { LookupAddress } from "node:dns";
import { GatewayError } from "@gateway/contracts";
import {
  assertPublicWebhookDns,
  constantTimeEquals,
  isSafePublicWebhookUrl,
  isValidCidr,
  LoginRateLimiter,
  maskSecret,
  normalizeAndValidateCidr,
  resolveClientIp,
} from "../auth.ts";

test("maskSecret handles empty, whitespace, short boundary, and standard keys", () => {
  expect(maskSecret("")).toBe("");
  expect(maskSecret("   ")).toBe("");
  expect(maskSecret("short")).toBe("sh***");
  expect(maskSecret("12345678")).toBe("12***");
  expect(maskSecret("123456789")).toBe("123456***6789");
  expect(maskSecret("  sk-proj-1234567890abcdef  ")).toBe("sk-pro***cdef");
});

test("normalizeAndValidateCidr and isValidCidr enforce IPv4/IPv6 prefix boundaries and trim input", () => {
  expect(normalizeAndValidateCidr(" 203.0.113.10 ")).toBe("203.0.113.10/32");
  expect(normalizeAndValidateCidr("203.0.113.0/0")).toBe("203.0.113.0/0");
  expect(normalizeAndValidateCidr("203.0.113.10/32")).toBe("203.0.113.10/32");
  expect(normalizeAndValidateCidr("2001:db8::/32")).toBe("2001:db8::/32");
  expect(normalizeAndValidateCidr("2001:db8::1")).toBe("2001:db8::1/128");
  expect(normalizeAndValidateCidr("2001:db8::/0")).toBe("2001:db8::/0");
  expect(normalizeAndValidateCidr("::ffff:203.0.113.5")).toBe("203.0.113.5/32");

  expect(isValidCidr("203.0.113.0/24")).toBe(true);
  expect(isValidCidr("203.0.113.0/33")).toBe(false);
  expect(isValidCidr("2001:db8::/129")).toBe(false);
  expect(isValidCidr("203.0.113.0/-1")).toBe(false);
  expect(isValidCidr("203.0.113.0/24/8")).toBe(false);
  expect(isValidCidr("not-a-cidr")).toBe(false);
});

test("isSafePublicWebhookUrl blocks SSRF private ranges, IPv6 encodings, trailing dots, and credentials", () => {
  expect(isSafePublicWebhookUrl("https://hooks.example.com/alerts")).toBe(true);
  expect(isSafePublicWebhookUrl("https://1.1.1.1/hook")).toBe(true);
  expect(isSafePublicWebhookUrl("https://[2606:4700:4700::1111]/hook")).toBe(true);

  const blockedUrls = [
    "http://hooks.example.com/alerts",
    "https://user:pass@hooks.example.com/alerts",
    "https://127.0.0.1/hook",
    "https://0.0.0.0/hook",
    "https://10.0.0.1/hook",
    "https://100.64.0.1/hook",
    "https://169.254.169.254/latest",
    "https://172.16.0.1/hook",
    "https://192.168.1.1/hook",
    "https://[::1]/hook",
    "https://[fc00::1]/hook",
    "https://[fd12:3456::1]/hook",
    "https://[fe80::1]/hook",
    "https://[::ffff:127.0.0.1]/hook",
    "https://[::ffff:7f00:1]/hook",
    "https://[::ffff:169.254.169.254]/hook",
    "https://localhost/hook",
    "https://localhost./hook",
    "https://sub.localhost./hook",
    "https://service.local/hook",
    "https://gateway.internal./hook",
    "https://2130706433/hook",
    "https://0x7f000001/hook",
    "not-a-url",
  ];
  for (const url of blockedUrls) {
    expect(isSafePublicWebhookUrl(url)).toBe(false);
  }
});

test("assertPublicWebhookDns validates resolved addresses and rejects private DNS rebinding targets", async () => {
  const publicLookup = (async () => [
    { address: "93.184.216.34", family: 4 },
  ]) as unknown as Parameters<typeof assertPublicWebhookDns>[1];
  expect(await assertPublicWebhookDns("https://hooks.example.com/a", publicLookup)).toEqual([
    "93.184.216.34",
  ]);

  const privateLookup = (async () => [
    { address: "93.184.216.34", family: 4 },
    { address: "::ffff:10.0.0.8", family: 6 },
  ]) as unknown as Parameters<typeof assertPublicWebhookDns>[1];
  await expect(
    assertPublicWebhookDns("https://rebind.example.com/a", privateLookup),
  ).rejects.toThrow(GatewayError);

  const emptyLookup = (async () => [] as LookupAddress[]) as unknown as Parameters<
    typeof assertPublicWebhookDns
  >[1];
  await expect(assertPublicWebhookDns("https://empty.example.com/a", emptyLookup)).rejects.toThrow(
    /did not resolve/,
  );
});

test("resolveClientIp strips only trusted right-to-left proxy hops and ignores untrusted X-Forwarded-For", () => {
  expect(resolveClientIp("198.51.100.4", "1.2.3.4", [])).toBe("198.51.100.4");
  expect(resolveClientIp("198.51.100.4", "1.2.3.4", ["10.0.0.0/8"])).toBe("198.51.100.4");
  expect(resolveClientIp("10.0.0.5", "203.0.113.7, 198.51.100.9, 10.0.0.2", ["10.0.0.0/8"])).toBe(
    "198.51.100.9",
  );
  expect(resolveClientIp("::ffff:10.0.0.5", "198.51.100.9", ["10.0.0.0/8"])).toBe("198.51.100.9");
  expect(resolveClientIp("10.0.0.5", "not-an-ip, 198.51.100.9", ["10.0.0.0/8"])).toBe("10.0.0.5");
  expect(resolveClientIp(undefined, "198.51.100.9", ["10.0.0.0/8"])).toBeNull();
});

test("constantTimeEquals checks exact UTF-8 equality across equal and differing lengths", () => {
  expect(constantTimeEquals("secret-token", "secret-token")).toBe(true);
  expect(constantTimeEquals("secret-token", "secret-tokeN")).toBe(false);
  expect(constantTimeEquals("secret-token", "short")).toBe(false);
  expect(constantTimeEquals("密码-1", "密码-1")).toBe(true);
  expect(constantTimeEquals("密码-1", "密码-2")).toBe(false);
});

test("LoginRateLimiter enforces thresholds, fails closed under flood capacity, resets blocked keys, and expires windows", () => {
  const limiter = new LoginRateLimiter(3, 60_000, 3);
  const t0 = 1_000_000;

  limiter.recordFailure("email:a@test.com", t0);
  limiter.recordFailure("email:a@test.com", t0 + 10);
  expect(() => limiter.check("email:a@test.com", t0 + 20)).not.toThrow();

  limiter.recordFailure("email:a@test.com", t0 + 30);
  expect(() => limiter.check("email:a@test.com", t0 + 40)).toThrow(/Too many/);

  for (const key of ["email:b@test.com", "email:c@test.com"]) {
    limiter.recordFailure(key, t0 + 50);
    limiter.recordFailure(key, t0 + 60);
    limiter.recordFailure(key, t0 + 70);
  }
  expect(() => limiter.check("email:new@test.com", t0 + 80)).toThrow(/capacity full/);
  expect(() => limiter.recordFailure("email:new@test.com", t0 + 80)).toThrow(/capacity full/);
  expect(() => limiter.check("email:a@test.com", t0 + 90)).toThrow(/Too many/);

  limiter.reset("email:a@test.com");
  expect(() => limiter.check("email:a@test.com", t0 + 100)).not.toThrow();

  expect(() => limiter.check("email:b@test.com", t0 + 61_000)).not.toThrow();
});
