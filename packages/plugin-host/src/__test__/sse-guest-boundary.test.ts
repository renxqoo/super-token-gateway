import { afterAll, beforeAll, expect, test } from "bun:test";
import { createPluginAttempts, createPluginSupervisor } from "../index.ts";
import { syntheticAdversarialPackage } from "../../../../tests/support/plugin-package.ts";
const supervisor = createPluginSupervisor({
    executable: new URL("../../../../.local/plugin-host", import.meta.url).pathname,
    native: { slots: 1 },
  }),
  host = createPluginAttempts(supervisor);
let signed: Awaited<ReturnType<typeof syntheticAdversarialPackage>>;
beforeAll(async () => {
  signed = await syntheticAdversarialPackage();
});
afterAll(async () => {
  await host.close();
});
const encode = (text: string) => new TextEncoder().encode(text),
  frame = encode('data: {"text":"{\\"ok\\":true}"}\n\n');
function create(
  mode: string,
  foreign: string = crypto.randomUUID(),
  tenant = "synthetic-sse-boundary",
) {
  return host.create(signed.pkg, {
    tenant,
    attempt: crypto.randomUUID(),
    incarnation: crypto.randomUUID(),
    deadlineAt: Date.now() + 10000,
    config: {
      model_map: { logical: "synthetic" },
      max_input_tokens: 10000,
      max_output_tokens: 1,
      auth: "bearer",
      fixture_mode: mode,
      foreign_document: foreign,
    },
  });
}
async function opened(mode: string, foreign?: string, tenant?: string) {
  const a = create(mode, foreign, tenant);
  await a.invoke("stream_open", encode("{}"));
  return a;
}
function clean() {
  expect(host.snapshot().json.bytes).toBe(0);
  expect(host.snapshot().stateBytes).toBe(0);
  expect(host.snapshot().reservedStateBytes).toBe(0);
  expect(host.snapshot().attempts).toBe(0);
  expect(supervisor.snapshot().pending).toBe(0);
}
test("guest JSON document handles expire at frame consumption and another frame cannot inspect them", async () => {
  const a = await opened("stream_replay_doc");
  expect((await a.invoke("stream_feed", frame)).kind).toBe("output");
  await expect(a.invoke("stream_feed", frame)).rejects.toMatchObject({ code: "JSON_HANDLE" });
  expect(a.snapshot().disposed).toBe(true);
  clean();
  const b = await opened("stream_foreign_doc");
  await expect(b.invoke("stream_feed", frame)).rejects.toMatchObject({ code: "JSON_HANDLE" });
  clean();
});
test("a string accumulator handle exported by one guest is inaccessible across tenant and stream owners", async () => {
  const a = await opened("stream_export_buffer"),
    result = await a.invoke("stream_feed", frame);
  expect(result.kind).toBe("output");
  if (result.kind !== "output") throw new Error("unexpected result");
  const handle = JSON.parse(
    Buffer.from(result.chunks[0]!, "base64").toString().slice(6).trim(),
  ).handle;
  expect(typeof handle).toBe("string");
  const b = await opened("stream_foreign_buffer", handle, "other-tenant");
  await expect(b.invoke("stream_feed", frame)).rejects.toMatchObject({ code: "STRING_HANDLE" });
  a.dispose();
  clean();
  const c = await opened("stream_foreign_buffer", handle);
  await expect(c.invoke("stream_feed", frame)).rejects.toMatchObject({ code: "STRING_HANDLE" });
  clean();
});
test("repeating a JSON string append in the same frame is rejected rather than double charging or changing content", async () => {
  const a = await opened("stream_repeat_append");
  await expect(a.invoke("stream_feed", frame)).rejects.toMatchObject({ code: "STRING_REPLAY" });
  expect(a.snapshot().disposed).toBe(true);
  clean();
});
test("a trap or cancellation immediately after host append closes all strings and the next guest remains fresh", async () => {
  const a = await opened("stream_append_trap");
  await expect(a.invoke("stream_feed", frame)).rejects.toMatchObject({ code: "GUEST_TRAP" });
  clean();
  const b = await opened("stream_append_loop"),
    controller = new AbortController(),
    pending = b.invoke("stream_feed", frame, controller.signal);
  setTimeout(() => controller.abort(), 20);
  await expect(pending).rejects.toMatchObject({ code: "CALL_CANCELLED" });
  clean();
  const c = await opened("stream_export_buffer"),
    result = await c.invoke("stream_feed", encode('data: {"text":"{\\"canaryB\\":true}"}\n\n'));
  expect(result.kind).toBe("output");
  if (result.kind !== "output") throw new Error("unexpected result");
  expect(Buffer.from(result.chunks[0]!, "base64").toString()).not.toContain('"ok"');
  c.dispose();
  clean();
  expect(supervisor.snapshot().starts).toBe(1);
});
test("a failed guest frame cannot introduce usage or FINAL that was not previously validated", async () => {
  const a = await opened("stream_failure_usage");
  expect(await a.invoke("stream_feed", frame)).toEqual({
    abi_version: 1,
    kind: "failure",
    status: 502,
    code: "invalid_content",
  });
  clean();
});
test("idle helper death invalidates every accumulated owner before any later pure transform restarts it", async () => {
  const a = await opened("stream_export_buffer");
  expect((await a.invoke("stream_feed", frame)).kind).toBe("output");
  expect(a.snapshot().stream?.buffers).toBe(1);
  const starts = supervisor.snapshot().starts;
  process.kill(supervisor.snapshot().pid!, "SIGKILL");
  const until = Date.now() + 2000;
  while (!a.snapshot().disposed && Date.now() < until) await Bun.sleep(10);
  expect(a.snapshot().disposed).toBe(true);
  clean();
  await expect(a.invoke("stream_feed", frame)).rejects.toMatchObject({ code: "ATTEMPT_CLOSED" });
  const b = await opened("stream_export_buffer");
  expect((await b.invoke("stream_feed", frame)).kind).toBe("output");
  b.dispose();
  clean();
  expect(supervisor.snapshot().starts).toBe(starts + 1);
});
