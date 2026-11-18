import { expect, test } from "bun:test";
import { resolve } from "node:path";
import { bundledPackageFiles } from "@gateway/adapters";
import { bundledReleaseIds, type VerifiedPackage } from "@gateway/plugin-sdk";
import {
  createBuiltinWorkerExecutor,
  createPluginAttempts,
  createPluginSupervisor,
  runPluginFixtures,
  type PluginExecutor,
} from "@gateway/plugin-host";
import { syntheticSignedPackage } from "../../../../tests/support/plugin-package.ts";

async function setup() {
  const registry = await Bun.file(resolve(".local/builtin-adapters/registry.json")).json();
  const code = await Bun.file(resolve(".local/builtin-adapters/worker.js")).bytes();
  const worker = createBuiltinWorkerExecutor({
    code,
    digest: registry.worker_digest,
    modules: registry.records.map((record: { module_hash: string }) => record.module_hash),
    slots: 4,
  });
  const packages: VerifiedPackage[] = [];
  for (const id of bundledReleaseIds)
    packages.push(syntheticSignedPackage(await bundledPackageFiles(id)).pkg);
  return { worker, packages };
}

test("every bundled fixture runs through the real Worker ABI with separate attempt state and no retained documents", async () => {
  const { worker, packages } = await setup();
  try {
    const results = await Promise.all(packages.map((pkg) => runPluginFixtures(worker, pkg)));
    for (const result of results)
      expect(result.state).toMatchObject({
        attempts: 0,
        stateBytes: 0,
        reservedStateBytes: 0,
        json: { bytes: 0 },
      });
    expect(results.reduce((total, result) => total + result.cases, 0)).toBe(152);
    expect(results.reduce((total, result) => total + result.steps, 0)).toBe(212);
  } finally {
    await worker.close();
  }
  expect(worker.snapshot()).toMatchObject({ workers: 0, pending: 0, pendingBytes: 0, timers: 0 });
}, 30000);

test("bundled Worker and Wasm produce byte-identical output and explicit state for every fixture call", async () => {
  const { worker, packages } = await setup();
  const wasm = createPluginSupervisor({
    executable: resolve(".local/plugin-host"),
    maxPending: 128,
  });
  let comparisons = 0;
  const shadow: PluginExecutor = {
    onDisconnect: (listener) => wasm.onDisconnect(listener),
    async load(module, signal) {
      const expected = await wasm.load(module, signal);
      expect((await worker.load(module, signal)).hash).toBe(expected.hash);
      return expected;
    },
    async call(hash, input, state, options) {
      const expected = await wasm.call(hash, input, state, options);
      const actual = await worker.call(hash, input, state, options);
      expect(Buffer.from(actual.output)).toEqual(Buffer.from(expected.output));
      expect(Buffer.from(actual.state)).toEqual(Buffer.from(expected.state));
      comparisons++;
      return expected;
    },
    close: async () => {
      await Promise.all([wasm.close(), worker.close()]);
    },
  };
  try {
    for (const pkg of packages) await runPluginFixtures(shadow, pkg);
    expect(comparisons).toBeGreaterThan(0);
  } finally {
    await shadow.close();
  }
  expect(worker.snapshot()).toMatchObject({ workers: 0, pending: 0, timers: 0 });
}, 30000);

test("parallel builtin tenants preserve their own prepared model, response and state across worker slots", async () => {
  const { worker, packages } = await setup();
  const host = createPluginAttempts(worker);
  const pkg = packages.find((value) => value.manifest.id === "gateway.openai-chat")!;
  const encode = (value: unknown) => new TextEncoder().encode(JSON.stringify(value));
  try {
    await Promise.all(
      Array.from({ length: 24 }, async (_, n) => {
        const model = `tenant_${n}`;
        const attempt = host.create(pkg, {
          tenant: model,
          attempt: crypto.randomUUID(),
          incarnation: crypto.randomUUID(),
          deadlineAt: Date.now() + 10000,
          config: {
            model_map: { [model]: `upstream_${n}` },
            max_output_tokens: 100,
            max_input_tokens: 10000,
            auth: "bearer",
          },
        });
        try {
          const prepared = await attempt.transformJSON(
            "prepare",
            { protocol: "chat", operation: "chat", model },
            encode({
              model,
              messages: [{ role: "user", content: `private_${n}` }],
              max_completion_tokens: 30,
            }),
          );
          const request = JSON.parse(new TextDecoder().decode(prepared.body));
          expect(request.model).toBe(`upstream_${n}`);
          expect(request.messages[0].content).toBe(`private_${n}`);
          const response = await attempt.transformJSON(
            "response",
            { model },
            encode({
              id: `reply_${n}`,
              model: `upstream_${n}`,
              choices: [
                { message: { role: "assistant", content: `result_${n}` }, finish_reason: "stop" },
              ],
              usage: { prompt_tokens: n + 1, completion_tokens: n + 2 },
            }),
          );
          const body = JSON.parse(new TextDecoder().decode(response.body));
          expect(body.model).toBe(model);
          expect(body.choices[0].message.content).toBe(`result_${n}`);
          expect(response.result).toMatchObject({
            usage: { input_tokens: String(n + 1), output_tokens: String(n + 2) },
            finality: "FINAL",
          });
        } finally {
          attempt.dispose();
        }
      }),
    );
    expect(host.snapshot()).toMatchObject({ attempts: 0, stateBytes: 0, json: { bytes: 0 } });
  } finally {
    await host.close();
  }
}, 30000);

test("interleaved Worker streams preserve each tenant's text and usage after another tenant fails", async () => {
  const { worker, packages } = await setup();
  const host = createPluginAttempts(worker);
  const pkg = packages.find((value) => value.manifest.id === "gateway.openai-chat")!;
  const encode = (value: string) => new TextEncoder().encode(value);
  const open = (name: string) =>
    host.create(pkg, {
      tenant: name,
      attempt: crypto.randomUUID(),
      incarnation: crypto.randomUUID(),
      deadlineAt: Date.now() + 10000,
      config: {
        model_map: { logical: "upstream" },
        max_output_tokens: 100,
        max_input_tokens: 10000,
        auth: "bearer",
      },
    });
  const wire = (value: unknown) => `data: ${JSON.stringify(value)}\n\n`;
  const attempts = Array.from({ length: 24 }, (_, n) => open(`tenant_${n}`));
  try {
    await Promise.all(
      attempts.map((attempt, n) =>
        attempt.invoke("stream_open", encode(JSON.stringify({ model: `logical_${n}` }))),
      ),
    );
    const broken = open("broken-canary");
    try {
      await broken.invoke("stream_open", encode('{"model":"broken-canary"}'));
      expect((await broken.invoke("stream_feed", new Uint8Array([255]))).kind).toBe("failure");
    } finally {
      broken.dispose();
    }
    const first = await Promise.all(
      attempts.map((attempt, n) =>
        attempt.invoke(
          "stream_feed",
          encode(
            wire({
              model: "upstream",
              choices: [{ delta: { content: `private_${n}_canary` } }],
              usage: { prompt_tokens: n + 1 },
            }),
          ),
        ),
      ),
    );
    for (const [n, result] of first.entries()) {
      if (result.kind !== "output") throw new Error("EXPECTED_STREAM_OUTPUT");
      const body = result.chunks.map((chunk) => Buffer.from(chunk, "base64").toString()).join("");
      expect(body).toContain(`private_${n}_canary`);
      expect(body).toContain(`logical_${n}`);
      for (let other = 0; other < attempts.length; other++)
        if (other !== n) expect(body).not.toContain(`private_${other}_canary`);
    }
    const completed = await Promise.all(
      attempts.map((attempt, n) =>
        attempt.invoke(
          "stream_feed",
          encode(
            wire({
              choices: [{ delta: {}, finish_reason: "stop" }],
              usage: { completion_tokens: n + 2 },
            }) + "data: [DONE]\n\n",
          ),
        ),
      ),
    );
    for (const [n, result] of completed.entries())
      expect(result).toMatchObject({
        kind: "output",
        finality: "FINAL",
        usage: { input_tokens: String(n + 1), output_tokens: String(n + 2) },
      });
  } finally {
    for (const attempt of attempts) attempt.dispose();
    await host.close();
  }
  expect(host.snapshot()).toMatchObject({
    attempts: 0,
    stateBytes: 0,
    reservedStateBytes: 0,
    json: { bytes: 0 },
  });
  expect(worker.snapshot()).toMatchObject({ workers: 0, pending: 0, pendingBytes: 0, timers: 0 });
}, 30000);

test("a closed alternative executor cannot leave an observer attached when the shared attempt owner fails construction", async () => {
  const { worker } = await setup();
  const wasm = createPluginSupervisor({ executable: resolve(".local/plugin-host") });
  await worker.close();
  try {
    const before = wasm.snapshot().observers;
    expect(() => createPluginAttempts(wasm, { additionalExecutors: [worker] })).toThrow();
    expect(wasm.snapshot().observers).toBe(before);
  } finally {
    await wasm.close();
  }
});

test("Worker and Wasm attempts share one global attempt/state budget and close every registered executor", async () => {
  const { worker, packages } = await setup();
  const wasm = createPluginSupervisor({ executable: resolve(".local/plugin-host") });
  const foreign = createPluginSupervisor({ executable: resolve(".local/plugin-host") });
  const host = createPluginAttempts(wasm, {
    maxAttempts: 2,
    maxStateBytes: 8192,
    additionalExecutors: [worker],
  });
  const pkg = packages.find((value) => value.manifest.id === "gateway.openai-chat")!;
  const options = () => ({
    tenant: "shared-runtime-budget",
    attempt: crypto.randomUUID(),
    incarnation: crypto.randomUUID(),
    deadlineAt: Date.now() + 10000,
    maxStateBytes: 2048,
    config: {
      model_map: { logical: "upstream" },
      max_output_tokens: 100,
      max_input_tokens: 10000,
      auth: "bearer",
    },
  });
  const encode = (value: unknown) => new TextEncoder().encode(JSON.stringify(value));
  try {
    expect(() => host.create(pkg, options(), foreign)).toThrow();
    const native = host.create(pkg, options(), worker),
      sandbox = host.create(pkg, options());
    expect(() => host.create(pkg, options(), worker)).toThrow("ATTEMPT_BUDGET");
    const results = await Promise.all(
      [native, sandbox].map((attempt) =>
        attempt.transformJSON(
          "prepare",
          { protocol: "chat", operation: "chat", model: "logical" },
          encode({
            model: "logical",
            messages: [{ role: "user", content: "shared budget" }],
            max_completion_tokens: 30,
          }),
        ),
      ),
    );
    for (const result of results) expect(result.result.kind).toBe("prepared");
    native.dispose();
    const replacement = host.create(pkg, options(), worker);
    expect(host.snapshot().attempts).toBe(2);
    replacement.dispose();
    sandbox.dispose();
    expect(host.snapshot()).toMatchObject({
      attempts: 0,
      stateBytes: 0,
      reservedStateBytes: 0,
      json: { bytes: 0 },
    });
  } finally {
    await host.close();
    await foreign.close();
  }
  expect(worker.snapshot()).toMatchObject({ workers: 0, pending: 0, timers: 0 });
  expect(wasm.snapshot()).toMatchObject({ pending: 0, timers: 0, observers: 0 });
});
