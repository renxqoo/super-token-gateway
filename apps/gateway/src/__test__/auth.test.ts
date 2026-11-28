import { expect, test } from "bun:test";
import { createKey } from "@gateway/runtime";
import { authenticateRequest } from "../inference/auth.ts";
import { handleInferenceRequest } from "../proxy.ts";
import {
  makeApiKey,
  makeBillingAccount,
  makeConfig,
  makeFakeDeps,
  makeSnapshot,
} from "./proxy-fixture.ts";

function makeContext(url: string, headers: Record<string, string>, body?: string) {
  const parsed = new URL(url);
  return {
    req: {
      raw: new Request(url, {
        method: body === undefined ? "GET" : "POST",
        headers,
        ...(body === undefined ? {} : { body }),
      }),
      query: (name: string) => parsed.searchParams.get(name) ?? undefined,
    },
    json(data: unknown, status?: number, extra?: Record<string, string>) {
      return new Response(JSON.stringify(data), {
        status: status ?? 200,
        headers: { "content-type": "application/json", ...extra },
      });
    },
  };
}

test("Bearer authorization scheme is case-insensitive and rejects malformed schemes", () => {
  const proj = makeBillingAccount("11111111-1111-4111-8111-111111111101", 10);
  const rawKey = createKey();
  const snap = makeSnapshot(1, [proj], [makeApiKey(rawKey, proj.id)]);
  const config = makeConfig();

  for (const scheme of ["Bearer", "bearer", "BEARER", "bEaReR"]) {
    const auth = authenticateRequest(
      makeContext("http://127.0.0.1/v1/chat/completions", { authorization: `${scheme} ${rawKey}` }),
      snap,
      config,
      "chat",
    );
    expect(auth.billingAccount.id).toBe(proj.id);
  }

  for (const badHeader of [
    `Basic ${rawKey}`,
    `Token ${rawKey}`,
    `Bearer${rawKey}`,
    "bearer",
    "bearer   ",
    "",
  ]) {
    expect(() =>
      authenticateRequest(
        makeContext("http://127.0.0.1/v1/chat/completions", { authorization: badHeader }),
        snap,
        config,
        "chat",
      ),
    ).toThrow(expect.objectContaining({ status: 401, code: "unauthorized" }));
  }
});

test("protocol-specific auth headers stay isolated to their operations", () => {
  const proj = makeBillingAccount("11111111-1111-4111-8111-111111111102", 10);
  const rawKey = createKey();
  const snap = makeSnapshot(1, [proj], [makeApiKey(rawKey, proj.id)]);
  const config = makeConfig();

  expect(
    authenticateRequest(
      makeContext("http://127.0.0.1/v1/messages", { "x-api-key": rawKey }),
      snap,
      config,
      "messages",
    ).billingAccount.id,
  ).toBe(proj.id);
  for (const op of ["chat", "responses", "gemini"] as const) {
    expect(() =>
      authenticateRequest(
        makeContext("http://127.0.0.1/v1/test", { "x-api-key": rawKey }),
        snap,
        config,
        op,
      ),
    ).toThrow(expect.objectContaining({ status: 401, code: "unauthorized" }));
  }

  expect(
    authenticateRequest(
      makeContext("http://127.0.0.1/v1beta/models/g:generateContent", { "x-goog-api-key": rawKey }),
      snap,
      config,
      "gemini",
    ).billingAccount.id,
  ).toBe(proj.id);
  expect(
    authenticateRequest(
      makeContext(`http://127.0.0.1/v1beta/models/g:generateContent?key=${rawKey}`, {}),
      snap,
      config,
      "gemini",
    ).billingAccount.id,
  ).toBe(proj.id);
  for (const op of ["chat", "responses", "messages"] as const) {
    expect(() =>
      authenticateRequest(
        makeContext(`http://127.0.0.1/v1/test?key=${rawKey}`, { "x-goog-api-key": rawKey }),
        snap,
        config,
        op,
      ),
    ).toThrow(expect.objectContaining({ status: 401, code: "unauthorized" }));
  }
});

test("unknown, disabled, oversized, and dual-header keys return generic protocol-native 401 envelopes without echoing secrets", async () => {
  const proj = makeBillingAccount("11111111-1111-4111-8111-111111111103", 10);
  const validKey = createKey();
  const disabledKey = createKey();
  const disabledRec = { ...makeApiKey(disabledKey, proj.id), enabled: false };
  const snap = makeSnapshot(1, [proj], [makeApiKey(validKey, proj.id), disabledRec]);
  const deps = makeFakeDeps(snap, makeConfig());
  const oversizedKey = `sk-forged-${"Z".repeat(5120)}-TAIL99`;
  const unknownValidFormat = createKey();

  try {
    const cases: Array<{
      operation: "chat" | "responses" | "messages" | "gemini";
      url: string;
      headers: Record<string, string>;
      secretMarkers: string[];
    }> = [
      {
        operation: "chat",
        url: "http://127.0.0.1/v1/chat/completions",
        headers: { authorization: `Bearer ${oversizedKey}`, "content-type": "application/json" },
        secretMarkers: ["sk-forged-", "TAIL99", "ZZZZZZ"],
      },
      {
        operation: "responses",
        url: "http://127.0.0.1/v1/responses",
        headers: {
          authorization: `Bearer ${unknownValidFormat}`,
          "content-type": "application/json",
        },
        secretMarkers: [unknownValidFormat, unknownValidFormat.slice(-4)],
      },
      {
        operation: "messages",
        url: "http://127.0.0.1/v1/messages",
        headers: {
          authorization: `Bearer ${disabledKey}`,
          "x-api-key": unknownValidFormat,
          "content-type": "application/json",
        },
        secretMarkers: [
          disabledKey,
          disabledKey.slice(-4),
          disabledRec.verifier,
          unknownValidFormat,
        ],
      },
      {
        operation: "gemini",
        url: "http://127.0.0.1/v1beta/models/g:generateContent",
        headers: { "x-goog-api-key": disabledKey, "content-type": "application/json" },
        secretMarkers: [disabledKey, disabledKey.slice(-4), disabledRec.verifier],
      },
    ];

    for (const item of cases) {
      const resp = await handleInferenceRequest(
        makeContext(item.url, item.headers, JSON.stringify({ model: "m1" })),
        deps,
        item.operation,
      );
      expect(resp.status).toBe(401);
      const rawText = await resp.text();
      for (const marker of item.secretMarkers) {
        expect(rawText).not.toContain(marker);
      }
      const parsed = JSON.parse(rawText) as Record<string, unknown>;
      if (item.operation === "messages") {
        expect(parsed).toMatchObject({
          type: "error",
          error: { type: "authentication_error", code: "unauthorized", message: "Invalid API key" },
        });
      } else if (item.operation === "gemini") {
        expect(parsed).toMatchObject({
          error: {
            code: 401,
            status: "UNAUTHENTICATED",
            gateway_code: "unauthorized",
            message: "Invalid API key",
          },
        });
      } else {
        expect(parsed).toMatchObject({
          error: { type: "authentication_error", code: "unauthorized", message: "Invalid API key" },
        });
      }
    }
  } finally {
    deps.state?.close();
    deps.transport.close();
  }
});

test("expired key, viewer role, disabled billing account, operation scope, and IP allowlist enforce exact rejection codes with valid controls", () => {
  const activeProj = makeBillingAccount("11111111-1111-4111-8111-111111111104", 10);
  const disabledProj = {
    ...makeBillingAccount("22222222-2222-4222-8222-222222222204", 10),
    enabled: false,
  };
  const okKey = createKey();
  const expiredKey = createKey();
  const viewerKey = createKey();
  const disabledAcctKey = createKey();
  const scopedKey = createKey();
  const snap = makeSnapshot(
    1,
    [activeProj, disabledProj],
    [
      makeApiKey(okKey, activeProj.id),
      {
        ...makeApiKey(expiredKey, activeProj.id),
        expires_at: new Date(Date.now() - 60_000).toISOString(),
      },
      { ...makeApiKey(viewerKey, activeProj.id), role: "VIEWER" },
      makeApiKey(disabledAcctKey, disabledProj.id),
      {
        ...makeApiKey(scopedKey, activeProj.id),
        operations: ["chat"],
        allowed_cidrs: ["198.51.100.0/24"],
      },
    ],
  );
  const config = makeConfig();

  expect(
    authenticateRequest(
      makeContext("http://127.0.0.1/v1/chat/completions", { authorization: `Bearer ${scopedKey}` }),
      snap,
      config,
      "chat",
      "198.51.100.42",
    ).billingAccount.id,
  ).toBe(activeProj.id);

  expect(() =>
    authenticateRequest(
      makeContext("http://127.0.0.1/v1/chat/completions", {
        authorization: `Bearer ${expiredKey}`,
      }),
      snap,
      config,
      "chat",
    ),
  ).toThrow(expect.objectContaining({ status: 401, code: "key_expired" }));

  expect(() =>
    authenticateRequest(
      makeContext("http://127.0.0.1/v1/chat/completions", { authorization: `Bearer ${viewerKey}` }),
      snap,
      config,
      "chat",
    ),
  ).toThrow(expect.objectContaining({ status: 403, code: "forbidden" }));

  expect(() =>
    authenticateRequest(
      makeContext("http://127.0.0.1/v1/chat/completions", {
        authorization: `Bearer ${disabledAcctKey}`,
      }),
      snap,
      config,
      "chat",
    ),
  ).toThrow(expect.objectContaining({ status: 403, code: "billing_account_disabled" }));

  expect(() =>
    authenticateRequest(
      makeContext("http://127.0.0.1/v1/responses", { authorization: `Bearer ${scopedKey}` }),
      snap,
      config,
      "responses",
      "198.51.100.42",
    ),
  ).toThrow(expect.objectContaining({ status: 403, code: "operation_not_allowed" }));

  expect(() =>
    authenticateRequest(
      makeContext("http://127.0.0.1/v1/chat/completions", { authorization: `Bearer ${scopedKey}` }),
      snap,
      config,
      "chat",
      "203.0.113.9",
    ),
  ).toThrow(expect.objectContaining({ status: 403, code: "ip_not_allowed" }));
});
