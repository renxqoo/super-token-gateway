import { afterAll, expect, test } from "bun:test";
import { generateKeyPairSync } from "node:crypto";
import {
  createPluginAttempts,
  createPluginSupervisor,
  runPluginFixtures,
} from "@gateway/plugin-host";
import { syntheticSignedPackage } from "../../../../tests/support/plugin-package.ts";
import { anthropicMessagesPackageFiles } from "../index.ts";

const supervisor = createPluginSupervisor({
  executable: new URL("../../../../.local/plugin-host", import.meta.url).pathname,
  native: { slots: 2 },
});
const host = createPluginAttempts(supervisor);
const signed = syntheticSignedPackage(
  await anthropicMessagesPackageFiles(),
  generateKeyPairSync("ed25519"),
);
afterAll(async () => {
  await host.close();
});
const config = {
  model_map: { logical: "claude-upstream" },
  max_output_tokens: 100,
  max_input_tokens: 10000,
  auth: "api-key",
};
function attempt() {
  return host.create(signed.pkg, {
    tenant: "synthetic-tenant",
    attempt: crypto.randomUUID(),
    incarnation: crypto.randomUUID(),
    config,
    deadlineAt: Date.now() + 30000,
  });
}
async function prepare(body: unknown) {
  const a = attempt();
  try {
    return await a.transformJSON(
      "prepare",
      { protocol: "chat", operation: "chat", model: "logical" },
      new TextEncoder().encode(JSON.stringify(body)),
    );
  } finally {
    a.dispose();
  }
}

test("package synthetic fixtures execute through the production WASM ABI", async () => {
  const result = await runPluginFixtures(supervisor, signed.pkg);
  expect(result.cases).toBe(22);
  expect(result.steps).toBe(33);
});

test("a long conversation is converted in one inspection pass with every message in order", async () => {
  const messages = Array.from({ length: 200 }, (_, i) => ({
    role: i % 2 ? "assistant" : "user",
    content: `t${i}`,
  }));
  const out = await prepare({ model: "logical", messages, stream: true });
  expect(out.result.kind).toBe("prepared");
  const body = JSON.parse(new TextDecoder().decode(out.body!));
  expect(body.messages.length).toBe(200);
  expect(body.messages[198]).toEqual({ role: "user", content: "t198" });
  expect(body.messages[199]).toEqual({
    role: "assistant",
    content: [{ type: "text", text: "t199" }],
  });
  expect(body.stream).toBe(true);
  expect(body.max_tokens).toBe(100);
});

test("a malformed tool argument string is a 400 invalid_json rather than a host fault", async () => {
  const out = await prepare({
    model: "logical",
    messages: [
      {
        role: "assistant",
        content: null,
        tool_calls: [
          { id: "c", type: "function", function: { name: "f", arguments: "{not json" } },
        ],
      },
      { role: "tool", tool_call_id: "c", content: "x" },
    ],
  });
  expect(out.result).toEqual({
    abi_version: 1,
    kind: "failure",
    status: 400,
    code: "invalid_json",
  });
});

test("zero-argument tool calls become an empty input object and strict tools are refused", async () => {
  const call = {
    role: "assistant",
    content: "",
    tool_calls: [{ id: "c", type: "function", function: { name: "now", arguments: "" } }],
  };
  const out = await prepare({ model: "logical", messages: [{ role: "user", content: "t" }, call] });
  expect(JSON.parse(new TextDecoder().decode(out.body!)).messages[1]).toEqual({
    role: "assistant",
    content: [{ type: "tool_use", id: "c", name: "now", input: {} }],
  });
  const strict = await prepare({
    model: "logical",
    messages: [{ role: "user", content: "t" }],
    tools: [{ type: "function", function: { name: "f", strict: true, parameters: {} } }],
  });
  expect(strict.result).toMatchObject({
    kind: "failure",
    status: 422,
    code: "unsupported_semantics",
  });
});
