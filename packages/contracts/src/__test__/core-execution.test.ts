import { expect, test } from "bun:test";
import { billableManifestV1Schema, executionDataSchemas } from "../core-execution.ts";
const attempt = "11111111-1111-4111-8111-111111111111";
const manifest = {
  version: 1,
  operation: "chat",
  model: "synthetic",
  upstream_model: "synthetic-provider",
  stream: false,
  input_bytes: 10,
  quantities: {},
  resource_ids: [],
  ceilings: { input_tokens: "9007199254740993", output_tokens: "0", audio_seconds: "0.1250" },
};
test("billable numeric manifest retains large exact integers and fractional seconds without coercion", () => {
  const result = billableManifestV1Schema.parse(manifest);
  expect(result.ceilings.input_tokens).toBe("9007199254740993");
  expect(result.ceilings.audio_seconds).toBe("0.125");
  expect(result.ceilings.output_tokens).toBe("0");
});
test("billable manifest rejects raw content unknown dimensions lossy numbers and fractional token counters", () => {
  for (const invalid of [
    { ...manifest, prompt: "sensitive-canary" },
    { ...manifest, ceilings: { input_tokens: 9007199254740992 } },
    { ...manifest, ceilings: { input_tokens: "1.5" } },
    { ...manifest, ceilings: { unknown: "0" } },
    { ...manifest, ceilings: { requests: "0001" } },
  ])
    expect(billableManifestV1Schema.safeParse(invalid).success).toBe(false);
});
test("quota RPC rejects arbitrary token reconciliation URL keys and unbounded recovery batches", () => {
  for (const invalid of [
    { attempt_id: attempt, permit_epoch: "1", action: "RECONCILE", target_tokens: "0" },
    { attempt_id: attempt, permit_epoch: "1", action: "RENEW", redis_url: "redis://untrusted" },
    { attempt_id: attempt, permit_epoch: "1", action: "RELEASE", key: "arbitrary" },
  ])
    expect(executionDataSchemas.execution_quota_v1.safeParse(invalid).success).toBe(false);
  expect(executionDataSchemas.execution_quota_recovery_v1.safeParse({ limit: 101 }).success).toBe(
    false,
  );
  expect(
    executionDataSchemas.execution_abort_recovery_v1.safeParse({
      attempt_id: attempt,
      revoked: true,
    }).success,
  ).toBe(false);
});
test("retry refuses caller-visible output and excessive global ordinals before financial mutation", () => {
  const hash = "a".repeat(64),
    ref = {
      route_id: attempt,
      channel_id: attempt,
      binding_id: attempt,
      plugin_version_id: attempt,
      package_digest: hash,
      config_revision: "1",
      config_digest: hash,
      cost_version_id: attempt,
      customer_price_id: attempt,
      candidate_digest: hash,
    };
  const projection = {
    version: 1,
    proposed_request_id: attempt,
    proposed_attempt_id: attempt,
    origin: "https://synthetic.test",
    method: "POST",
    operation: "chat",
    request_target_sha256: hash,
    final_headers_digest: hash,
    final_body_sha256: hash,
    candidate_ref: ref,
    auth: { type: "none" },
    credential_ciphertext_sha256: hash,
    billable_manifest_sha256: hash,
    deadline_at: "2099-01-01T00:00:00.000Z",
  };
  const valid = {
    context_id: attempt,
    dispatch_projection: projection,
    dispatched_digest: hash,
    manifest,
    prep_session_id: attempt,
    prep_binding_digest: hash,
    request_id: attempt,
    previous_attempt_id: attempt,
    expected_ordinal: 2,
    visible_output: false,
  };
  expect(executionDataSchemas.execution_next_v1.safeParse(valid).success).toBe(true);
  expect(
    executionDataSchemas.execution_next_v1.safeParse({ ...valid, expected_ordinal: 6 }).success,
  ).toBe(false);
  expect(
    executionDataSchemas.execution_next_v1.safeParse({ ...valid, visible_output: true }).success,
  ).toBe(false);
});

import { internalErrorSchema } from "../internal-identity.ts";
test("typed financial error DTO preserves safe request identity but rejects grant secret and duplicate receipt channels", () => {
  const rejection = {
    code: "already_accepted",
    category: "VALIDATION",
    retryability: "NONE",
    command_state: "REJECTED",
    request_id: attempt,
    message: "Internal request rejected",
  };
  expect(internalErrorSchema.parse(rejection).request_id).toBe(attempt);
  for (const forbidden of [
    { receipt: { command_state: "REJECTED" } },
    { transient_send: { auth_material: "sensitive-canary" } },
    { secret: "sensitive-canary" },
    { body: "sensitive-canary" },
  ])
    expect(internalErrorSchema.safeParse({ ...rejection, ...forbidden }).success).toBe(false);
});
