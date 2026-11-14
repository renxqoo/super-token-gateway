import { afterAll, expect, test } from "bun:test";
import { generateKeyPairSync } from "node:crypto";
import {
  createPluginAttempts,
  createPluginSupervisor,
  runPluginFixtures,
} from "@gateway/plugin-host";
import { syntheticSignedPackage } from "../../../../tests/support/plugin-package.ts";
import { geminiGeneratePackageFiles } from "../index.ts";

const supervisor = createPluginSupervisor({
  executable: new URL("../../../../.local/plugin-host", import.meta.url).pathname,
  native: { slots: 2 },
});
const host = createPluginAttempts(supervisor);
const signed = syntheticSignedPackage(
  await geminiGeneratePackageFiles(),
  generateKeyPairSync("ed25519"),
);
afterAll(async () => {
  await host.close();
});
const config = {
  model_map: { logical: "gemini-upstream" },
  max_output_tokens: 100,
  max_input_tokens: 10000,
  auth: "api-key",
};
async function run(
  hook: "prepare" | "response",
  body: unknown,
  overrides: Record<string, unknown> = {},
) {
  const a = host.create(signed.pkg, {
    tenant: "synthetic-tenant",
    attempt: crypto.randomUUID(),
    incarnation: crypto.randomUUID(),
    config: { ...config, ...overrides },
    deadlineAt: Date.now() + 30000,
  });
  try {
    const out = await a.transformJSON(
      hook,
      hook === "prepare"
        ? { protocol: "chat", operation: "chat", model: "logical" }
        : { model: "logical" },
      new TextEncoder().encode(typeof body === "string" ? body : JSON.stringify(body)),
    );
    return {
      result: out.result,
      body: out.body ? JSON.parse(new TextDecoder().decode(out.body)) : undefined,
    };
  } finally {
    a.dispose();
  }
}

test("package synthetic fixtures execute through the production WASM ABI", async () => {
  const result = await runPluginFixtures(supervisor, signed.pkg);
  expect(result.cases).toBe(9);
  expect(result.steps).toBe(14);
});

test("chat with system, tools and a tool round trip becomes generateContent with grouped function responses", async () => {
  const { result, body } = await run("prepare", {
    model: "logical",
    messages: [
      { role: "system", content: "be brief" },
      { role: "user", content: [{ type: "text", text: "weather?" }] },
      {
        role: "assistant",
        content: null,
        tool_calls: [
          {
            id: "c1",
            type: "function",
            function: { name: "weather", arguments: '{"city":"北京"}' },
          },
          { id: "c2", type: "function", function: { name: "time", arguments: "" } },
        ],
      },
      { role: "tool", tool_call_id: "c1", content: "晴" },
      {
        role: "tool",
        tool_call_id: "c2",
        content: [
          { type: "text", text: "1" },
          { type: "text", text: "2" },
        ],
      },
    ],
    tools: [
      {
        type: "function",
        function: { name: "weather", description: "w", parameters: { type: "object" } },
      },
      { type: "function", function: { name: "time" } },
    ],
    tool_choice: { type: "function", function: { name: "weather" } },
    temperature: 0.5,
    top_p: 0.9,
    stop: "END",
    max_tokens: 40,
  });
  expect(result).toMatchObject({
    kind: "prepared",
    request: {
      path: "/v1beta/models/gemini-upstream:generateContent",
      auth: { type: "api-key", header: "x-goog-api-key" },
      manifest: { upstream_model: "gemini-upstream", ceilings: { output_tokens: "40" } },
    },
  });
  expect(body).toEqual({
    contents: [
      { role: "user", parts: [{ text: "weather?" }] },
      {
        role: "model",
        parts: [
          { functionCall: { id: "c1", name: "weather", args: { city: "北京" } } },
          { functionCall: { id: "c2", name: "time", args: {} } },
        ],
      },
      {
        role: "user",
        parts: [
          { functionResponse: { id: "c1", name: "weather", response: { result: "晴" } } },
          { functionResponse: { id: "c2", name: "time", response: { result: "12" } } },
        ],
      },
    ],
    systemInstruction: { parts: [{ text: "be brief" }] },
    tools: [
      {
        functionDeclarations: [
          { name: "weather", description: "w", parametersJsonSchema: { type: "object" } },
          { name: "time" },
        ],
      },
    ],
    toolConfig: { functionCallingConfig: { mode: "ANY", allowedFunctionNames: ["weather"] } },
    generationConfig: { maxOutputTokens: 40, temperature: 0.5, topP: 0.9, stopSequences: ["END"] },
  });
});

test("streaming prepare targets streamGenerateContent with SSE and refuses usage opt-out", async () => {
  const { result } = await run("prepare", {
    model: "logical",
    messages: [{ role: "user", content: "hi" }],
    stream: true,
  });
  expect(result).toMatchObject({
    request: { path: "/v1beta/models/gemini-upstream:streamGenerateContent?alt=sse" },
  });
  expect(
    (
      await run("prepare", {
        model: "logical",
        messages: [{ role: "user", content: "hi" }],
        stream: true,
        stream_options: { include_usage: false },
      })
    ).result,
  ).toMatchObject({ kind: "failure", status: 422, code: "usage_required" });
});

test("Vertex path style targets the project and location publisher model with host google-oauth", async () => {
  const vertex = {
    path_style: "vertex",
    project: "synthetic-project",
    location: "us-central1",
    auth: "google-oauth",
  };
  const request = { model: "logical", messages: [{ role: "user", content: "hi" }] };
  expect((await run("prepare", request, vertex)).result).toMatchObject({
    kind: "prepared",
    request: {
      path: "/v1/projects/synthetic-project/locations/us-central1/publishers/google/models/gemini-upstream:generateContent",
      auth: { type: "google-oauth" },
    },
  });
  expect((await run("prepare", { ...request, stream: true }, vertex)).result).toMatchObject({
    request: {
      path: "/v1/projects/synthetic-project/locations/us-central1/publishers/google/models/gemini-upstream:streamGenerateContent?alt=sse",
    },
  });
  const { project: _project, ...projectless } = vertex;
  for (const invalid of [
    projectless,
    { ...vertex, project: "Bad_Project" },
    { ...vertex, location: "us/central1" },
  ])
    expect((await run("prepare", request, invalid)).result).toMatchObject({
      kind: "failure",
      status: 422,
      code: "unsupported_capability",
    });
});

test("unrepresentable chat semantics are refused instead of silently dropped", async () => {
  const user = { role: "user", content: "hi" };
  const cases: [unknown, number, string][] = [
    [
      { model: "logical", messages: [user, { role: "system", content: "late" }] },
      422,
      "unsupported_semantics",
    ],
    [
      { model: "logical", messages: [user], parallel_tool_calls: false },
      422,
      "unsupported_semantics",
    ],
    [{ model: "logical", messages: [user], user: "u1" }, 422, "unsupported_semantics"],
    [
      {
        model: "logical",
        messages: [
          { role: "user", content: [{ type: "image_url", image_url: { url: "https://x/y.png" } }] },
        ],
      },
      422,
      "unsupported_semantics",
    ],
    [
      {
        model: "logical",
        messages: [user, { role: "tool", tool_call_id: "unknown", content: "x" }],
      },
      400,
      "invalid_messages",
    ],
  ];
  for (const [body, status, code] of cases)
    expect((await run("prepare", body)).result).toMatchObject({ kind: "failure", status, code });
});

test("response text, function calls, cache and thinking usage map to a chat completion", async () => {
  const { result, body } = await run("response", {
    responseId: "resp-1",
    candidates: [
      {
        content: {
          role: "model",
          parts: [
            { text: "你" },
            { text: "好" },
            { functionCall: { name: "weather", args: { city: "上海" } }, thoughtSignature: "sig" },
          ],
        },
        finishReason: "STOP",
      },
    ],
    usageMetadata: {
      promptTokenCount: 20,
      cachedContentTokenCount: 5,
      candidatesTokenCount: 7,
      thoughtsTokenCount: 3,
      totalTokenCount: 30,
    },
  });
  expect(result).toMatchObject({
    kind: "output",
    finality: "FINAL",
    usage: {
      input_tokens: "15",
      cached_input_tokens: "5",
      output_tokens: "10",
      reasoning_tokens: "3",
    },
  });
  expect(body).toEqual({
    id: "resp-1",
    object: "chat.completion",
    created: 0,
    model: "logical",
    choices: [
      {
        index: 0,
        message: {
          role: "assistant",
          content: "你好",
          tool_calls: [
            {
              id: "call_0",
              type: "function",
              function: { name: "weather", arguments: '{"city":"上海"}' },
            },
          ],
        },
        finish_reason: "tool_calls",
      },
    ],
    usage: {
      prompt_tokens: 20,
      completion_tokens: 10,
      total_tokens: 30,
      prompt_tokens_details: { cached_tokens: 5 },
      completion_tokens_details: { reasoning_tokens: 3 },
    },
  });
});

test("blocked prompts, safety stops, missing usage and multiple candidates are classified", async () => {
  const blocked = await run("response", {
    promptFeedback: { blockReason: "SAFETY" },
    usageMetadata: { promptTokenCount: 4 },
  });
  expect(blocked.result).toMatchObject({
    kind: "output",
    usage: { input_tokens: "4", output_tokens: "0" },
    terminal: { kind: "incomplete" },
  });
  expect(blocked.body.choices[0]).toEqual({
    index: 0,
    message: { role: "assistant", content: null },
    finish_reason: "content_filter",
  });
  const text = { content: { parts: [{ text: "x" }] }, finishReason: "MAX_TOKENS" };
  expect(
    (
      await run("response", {
        candidates: [text],
        usageMetadata: { promptTokenCount: 1, candidatesTokenCount: 1 },
      })
    ).body.choices[0].finish_reason,
  ).toBe("length");
  for (const bad of [
    { candidates: [text] },
    { candidates: [text, text], usageMetadata: { promptTokenCount: 1, candidatesTokenCount: 1 } },
    { candidates: [{ ...text, finishReason: "OTHER" }], usageMetadata: { promptTokenCount: 1 } },
    { error: { code: 400 } },
  ])
    expect((await run("response", bad)).result).toMatchObject({ kind: "failure", status: 502 });
});
