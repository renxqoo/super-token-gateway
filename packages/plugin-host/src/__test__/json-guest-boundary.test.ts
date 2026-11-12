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
const body = new TextEncoder().encode('{"a":{"b":1}}'),
  metadata = { protocol: "chat", operation: "chat", model: "logical" };
function create(
  mode: string,
  foreign: string = crypto.randomUUID(),
  tenant = "synthetic-boundary",
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
test("a real guest cannot inspect or materialize an invented document handle", async () => {
  for (const mode of ["inspect_foreign", "plan_foreign"]) {
    const a = create(mode);
    await expect(a.transformJSON("prepare", metadata, body)).rejects.toMatchObject({
      code: "JSON_HANDLE",
    });
    expect(a.snapshot().disposed).toBe(true);
    expect(host.snapshot().json.bytes).toBe(0);
  }
});
test("a real guest cannot repeat inspections, overlap edits or exceed literal budgets", async () => {
  for (const mode of ["repeat_inspection", "overlap", "oversize"]) {
    const a = create(mode);
    await expect(a.transformJSON("prepare", metadata, body)).rejects.toBeInstanceOf(Error);
    expect(a.snapshot().disposed).toBe(true);
    expect(host.snapshot().json.bytes).toBe(0);
    expect(host.snapshot().reservedStateBytes).toBe(0);
  }
});
test("a consumed document cannot be replayed by the same guest into its next JSON epoch", async () => {
  const a = create("replay");
  expect((await a.transformJSON("prepare", metadata, body)).result.kind).toBe("prepared");
  await expect(
    a.transformJSON("prepare", metadata, new TextEncoder().encode('{"other":true}')),
  ).rejects.toMatchObject({ code: "JSON_HANDLE" });
  expect(host.snapshot().json.bytes).toBe(0);
});
test("another session or tenant cannot use a document plan issued for an earlier session", async () => {
  const a = create("replay"),
    first = await a.transformJSON("prepare", metadata, body);
  if (first.result.kind !== "prepared") throw new Error("synthetic preparation failed");
  const b = create("plan_foreign", first.result.request.body.document, "other-tenant");
  await expect(b.transformJSON("prepare", metadata, body)).rejects.toMatchObject({
    code: "JSON_HANDLE",
  });
  a.dispose();
  expect(host.snapshot().json.bytes).toBe(0);
});
test("guest trap and active cancellation release JSON indexes and explicit state without contaminating future calls", async () => {
  const a = create("trap");
  await expect(a.transformJSON("prepare", metadata, body)).rejects.toMatchObject({
    code: "GUEST_TRAP",
  });
  expect(host.snapshot().json.bytes).toBe(0);
  const b = create("loop"),
    controller = new AbortController(),
    pending = b.transformJSON("prepare", metadata, body, controller.signal);
  setTimeout(() => controller.abort(), 20);
  await expect(pending).rejects.toMatchObject({ code: "CALL_CANCELLED" });
  expect(host.snapshot().json.bytes).toBe(0);
  expect(host.snapshot().reservedStateBytes).toBe(0);
  const c = create("replay");
  try {
    expect((await c.transformJSON("prepare", metadata, body)).body).toEqual(body);
  } finally {
    c.dispose();
  }
  expect(supervisor.snapshot().starts).toBe(1);
});
test("cancellation and disposal invalidate JSON documents before later guest invocation", async () => {
  const a = create("replay"),
    controller = new AbortController();
  controller.abort();
  await expect(a.transformJSON("prepare", metadata, body, controller.signal)).rejects.toMatchObject(
    { code: "CALL_CANCELLED" },
  );
  await expect(a.transformJSON("prepare", metadata, body)).rejects.toMatchObject({
    code: "ATTEMPT_CLOSED",
  });
  expect(host.snapshot().json.bytes).toBe(0);
  expect(supervisor.snapshot().pending).toBe(0);
});
test("invalid guest output cannot put guest-chosen text into an outward error message", async () => {
  for (const mode of ["invalid_result", "invalid_failure"]) {
    const a = create(mode);
    let caught: unknown;
    try {
      await a.transformJSON("prepare", metadata, body);
    } catch (error) {
      caught = error;
    }
    expect(caught).toMatchObject({ code: "ABI_INVALID", message: "ABI_INVALID" });
    expect((caught as Error).message).not.toContain("canary");
    expect(host.snapshot().json.bytes).toBe(0);
  }
});
