import { expect, test } from "bun:test";
import { generateKeyPairSync, verify } from "node:crypto";
import { VertexTokens, signBedrock, validateCloudCredentials } from "../cloud-auth.ts";
test("Vertex signs service-account assertions and coalesces refresh before token expiry", async () => {
  const pair = generateKeyPairSync("rsa", { modulusLength: 2048 });
  const secret = JSON.stringify({
    type: "service_account",
    client_email: "fixture@example.invalid",
    private_key: pair.privateKey.export({ format: "pem", type: "pkcs8" }).toString(),
  });
  let now = Date.now(),
    calls = 0;
  const tokens = new VertexTokens(
    async (url, init) => {
      expect(url.href).toBe("https://oauth2.googleapis.com/token");
      calls++;
      const assertion = new URLSearchParams(String(init.body)).get("assertion")!;
      const parts = assertion.split(".");
      expect(
        verify(
          "RSA-SHA256",
          Buffer.from(`${parts[0]}.${parts[1]}`),
          pair.publicKey,
          Buffer.from(parts[2]!, "base64url"),
        ),
      ).toBe(true);
      const claims = JSON.parse(Buffer.from(parts[1]!, "base64url").toString());
      expect(claims.iss).toBe("fixture@example.invalid");
      expect(claims.aud).toBe(url.href);
      expect(claims.exp - claims.iat).toBe(3600);
      return Response.json({
        access_token: `token-${calls}`,
        token_type: "Bearer",
        expires_in: 3600,
      });
    },
    () => now,
  );
  expect(
    await Promise.all(
      Array.from({ length: 8 }, () => tokens.get(secret, new AbortController().signal)),
    ),
  ).toEqual(Array(8).fill("token-1"));
  expect(calls).toBe(1);
  now += 3550 * 1000;
  expect(await tokens.get(secret, new AbortController().signal)).toBe("token-2");
  expect(calls).toBe(2);
});

test("Vertex rejects expired access tokens and malformed service-account credentials", () => {
  expect(() =>
    validateCloudCredentials(
      "vertex",
      JSON.stringify({ access_token: "expired", expires_at: "2000-01-01" }),
    ),
  ).toThrow();
  expect(() =>
    validateCloudCredentials(
      "vertex",
      JSON.stringify({ type: "service_account", client_email: "invalid", private_key: "invalid" }),
    ),
  ).toThrow();
});

test("Bedrock signing binds payload, session, region and encoded model path", () => {
  const url = new URL(
    "https://bedrock-runtime.us-east-1.amazonaws.com/model/anthropic.claude-fixture%3A0/invoke",
  );
  const headers = new Headers({ "content-type": "application/json" });
  signBedrock(
    url,
    "{}",
    headers,
    {
      accessKeyId: "AKIDEXAMPLE",
      secretAccessKey: "SECRET",
      region: "us-east-1",
      sessionToken: "SESSION",
    },
    new Date("2026-01-01T00:00:00Z"),
  );
  expect(headers.get("authorization")).toContain(
    "Credential=AKIDEXAMPLE/20260101/us-east-1/bedrock/aws4_request",
  );
  expect(headers.get("authorization")).toContain(
    "Signature=a03e43a6b86536546f0d3f0a4875646843e6ef7593cadca51e4f8b1372bfc507",
  );
  expect(headers.get("x-amz-security-token")).toBe("SESSION");
  expect(headers.get("x-amz-content-sha256")).toBe(
    "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a",
  );
});
