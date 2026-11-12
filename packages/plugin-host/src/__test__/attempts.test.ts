import { afterAll, beforeAll, expect, test } from "bun:test";
import { createPluginAttempts, createPluginSupervisor } from "../index.ts";
import { syntheticChatPackage } from "../../../../tests/support/plugin-package.ts";
const supervisor = createPluginSupervisor({
  executable: new URL("../../../../.local/plugin-host", import.meta.url).pathname,
});
const host = createPluginAttempts(supervisor, { maxAttempts: 3, maxStateBytes: 4096 });
let signed: Awaited<ReturnType<typeof syntheticChatPackage>>;
beforeAll(async () => {
  signed = await syntheticChatPackage();
});
afterAll(async () => {
  await host.close();
});
const config = {
  model_map: { logical: "upstream" },
  max_output_tokens: 10,
  max_input_tokens: 10000,
  auth: "bearer",
};
const data = new TextEncoder().encode('{"model":"logical"}');
function create(
  overrides: { maxStateBytes?: number; batchOutputBytes?: number; deadlineAt?: number } = {},
) {
  return host.create(signed.pkg, {
    tenant: "synthetic",
    attempt: crypto.randomUUID(),
    incarnation: crypto.randomUUID(),
    config,
    deadlineAt: Date.now() + 10000,
    maxStateBytes: 1024,
    ...overrides,
  });
}

test("attempt session state is owned, sequenced, bounded and discarded on explicit disposal", async () => {
  const a = create();
  await a.invoke("stream_open", data);
  expect(a.snapshot().sequence).toBe(1);
  expect(a.snapshot().stateBytes).toBeGreaterThan(0);
  expect(host.snapshot().stateBytes).toBeGreaterThan(0);
  a.dispose();
  expect(host.snapshot().attempts).toBe(0);
  expect(host.snapshot().stateBytes).toBe(0);
  expect(host.snapshot().moduleBytes).toBe(0);
  await expect(a.invoke("stream_feed", new Uint8Array())).rejects.toMatchObject({
    code: "ATTEMPT_CLOSED",
  });
});
test("concurrent and out of order stream calls fail without borrowing another attempt state", async () => {
  const a = create();
  await expect(a.invoke("stream_feed", data)).rejects.toMatchObject({ code: "STREAM_NOT_OPEN" });
  const first = a.invoke("stream_open", data);
  await expect(a.invoke("stream_open", data)).rejects.toMatchObject({ code: "BATCH_CONCURRENT" });
  await first;
  await expect(a.invoke("stream_open", data)).rejects.toMatchObject({
    code: "STREAM_ALREADY_OPEN",
  });
  a.dispose();
});
test("attempt count and global state reservations reject before guest work and return all quota", async () => {
  const a = create(),
    b = create(),
    c = create();
  expect(() => create()).toThrow("ATTEMPT_BUDGET");
  await expect(a.invoke("stream_open", data)).resolves.toMatchObject({ kind: "output" });
  const tooLarge = createPluginAttempts(supervisor, { maxStateBytes: 100 });
  const t = tooLarge.create(signed.pkg, {
    tenant: "synthetic",
    attempt: crypto.randomUUID(),
    incarnation: crypto.randomUUID(),
    config,
    deadlineAt: Date.now() + 10000,
    maxStateBytes: 101,
  });
  await expect(t.invoke("describe")).rejects.toMatchObject({ code: "STATE_BUDGET" });
  t.dispose();
  tooLarge.dispose();
  a.dispose();
  b.dispose();
  c.dispose();
  expect(host.snapshot().reservedStateBytes).toBe(0);
  expect(host.snapshot().stateBytes).toBe(0);
});
test("deadline and cancellation clear attempt timers and state, and later hooks cannot reuse them", async () => {
  const a = create({ deadlineAt: Date.now() + 20 });
  await Bun.sleep(30);
  await expect(a.invoke("describe")).rejects.toMatchObject({ code: "ATTEMPT_CLOSED" });
  expect(a.snapshot().disposed).toBe(true);
  const b = create(),
    abort = new AbortController();
  abort.abort();
  await expect(b.invoke("stream_open", data, abort.signal)).rejects.toMatchObject({
    code: "CALL_CANCELLED",
  });
  expect(host.snapshot().attempts).toBe(0);
});
test("a state budget failure disposes the session and returns the in-flight reservation", async () => {
  const a = create({ maxStateBytes: 1 });
  await expect(a.invoke("stream_open", data)).rejects.toMatchObject({ code: "STATE_BUDGET" });
  expect(a.snapshot().disposed).toBe(true);
  expect(host.snapshot().reservedStateBytes).toBe(0);
  expect(host.snapshot().stateBytes).toBe(0);
});
test("mutable public package views cannot substitute code or manifest after signature verification", async () => {
  const fresh = await syntheticChatPackage();
  fresh.pkg.module.fill(0);
  fresh.pkg.manifest.entrypoint = "not_the_signed_function";
  const a = host.create(fresh.pkg, {
    tenant: "synthetic",
    attempt: crypto.randomUUID(),
    incarnation: crypto.randomUUID(),
    config,
    deadlineAt: Date.now() + 10000,
    maxStateBytes: 1024,
  });
  expect((await a.invoke("describe")).kind).toBe("description");
  a.dispose();
  expect(() =>
    host.create(
      { ...signed.pkg },
      {
        tenant: "synthetic",
        attempt: crypto.randomUUID(),
        incarnation: crypto.randomUUID(),
        config,
        deadlineAt: Date.now() + 10000,
      },
    ),
  ).toThrow("PACKAGE_NOT_VERIFIED");
});
test("JSON metadata cannot carry headers, targets or credential fields into a guest", async () => {
  for (const extra of [
    { headers: { authorization: "synthetic-secret" } },
    { url: "https://synthetic.invalid" },
    { credential_reference: "synthetic" },
  ]) {
    const a = create();
    await expect(
      a.transformJSON(
        "prepare",
        { model: "logical", protocol: "chat", operation: "chat", ...extra },
        new TextEncoder().encode("{}"),
      ),
    ).rejects.toBeInstanceOf(Error);
    expect(a.snapshot().sequence).toBe(0);
    expect(host.snapshot().json.bytes).toBe(0);
  }
});
