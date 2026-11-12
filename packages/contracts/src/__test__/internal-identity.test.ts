import { expect, test } from "bun:test";
import { dispatchProjectionV1Schema } from "../internal-identity.ts";
function dispatch() {
  const id = "123e4567-e89b-42d3-a456-426614174000",
    hash = "b".repeat(64);
  return {
    version: 1,
    proposed_request_id: id,
    proposed_attempt_id: id,
    origin: "https://api.example.test",
    method: "POST",
    operation: "chat",
    request_target_sha256: hash,
    final_headers_digest: hash,
    final_body_sha256: hash,
    candidate_ref: {
      route_id: id,
      channel_id: id,
      binding_id: id,
      plugin_version_id: id,
      package_digest: hash,
      config_revision: "1",
      config_digest: hash,
      cost_version_id: id,
      customer_price_id: id,
      candidate_digest: hash,
    },
    auth: { type: "bearer" },
    credential_ciphertext_sha256: hash,
    billable_manifest_sha256: hash,
    deadline_at: "2026-11-12T12:00:00.000Z",
  };
}
for (const field of [
  "headers",
  "path",
  "query",
  "raw_uri",
  "body",
  "metadata",
  "request_target",
  "url",
])
  test(`dispatch rejects body canary carried in extra ${field}`, () => {
    const candidate = {
      ...dispatch(),
      [field]:
        field === "headers"
          ? { "x-legitimate": "BODY_CANARY_A" }
          : "/v1/chat/completions?x=BODY_CANARY_B",
    };
    expect(dispatchProjectionV1Schema.safeParse(candidate).success).toBe(false);
  });
test("dispatch preserves all supported SDK auth descriptors and rejects alternate secret channels", () => {
  for (const auth of [
    { type: "none" },
    { type: "bearer" },
    { type: "api-key", header: "api-key" },
    { type: "api-key", header: "x-api-key" },
  ] as const)
    expect(dispatchProjectionV1Schema.parse({ ...dispatch(), auth }).auth).toEqual(auth);
  for (const auth of [
    { type: "api-key" },
    { type: "api-key", header: "authorization" },
    { type: "bearer", value: "secret" },
    { type: "none", metadata: { x: "canary" } },
  ])
    expect(dispatchProjectionV1Schema.safeParse({ ...dispatch(), auth }).success).toBe(false);
  expect(
    dispatchProjectionV1Schema.safeParse({
      ...dispatch(),
      candidate_ref: { ...dispatch().candidate_ref, metadata: "canary" },
    }).success,
  ).toBe(false);
});
test("dispatch numeric versions and origin cannot smuggle raw request text", () => {
  for (const config_revision of ["0", "01", "1.1", "-1", "1e3", "9".repeat(39)])
    expect(
      dispatchProjectionV1Schema.safeParse({
        ...dispatch(),
        candidate_ref: { ...dispatch().candidate_ref, config_revision },
      }).success,
    ).toBe(false);
  for (const origin of [
    "https://api.example.test/",
    "https://api.example.test?canary=BODY",
    "https://u:p@api.example.test",
    "https://api.example.test:443",
    "http://api.example.test",
    "https://api.example.test/#BODY",
  ])
    expect(dispatchProjectionV1Schema.safeParse({ ...dispatch(), origin }).success).toBe(false);
  for (const deadline_at of [
    "2026-11-12T00:00:00Z",
    "2026-02-30T00:00:00.000Z",
    "2026-11-12T00:00:60.000Z",
  ])
    expect(dispatchProjectionV1Schema.safeParse({ ...dispatch(), deadline_at }).success).toBe(
      false,
    );
});
