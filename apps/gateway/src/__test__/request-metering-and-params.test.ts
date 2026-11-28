import { expect, test } from "bun:test";
import { getBuiltinProvider } from "@gateway/plugin-engine";
import { parseClientJson, stringifyExact } from "@gateway/protocol";
import {
  buildPreparedPayload,
  capKnownOutput,
  mergeOverridesForResolved,
} from "../upstream-payload.ts";
import { invokeInference, jsonBody } from "./proxy-response-harness.ts";

const geminiOkBody = JSON.stringify({
  candidates: [{ content: { role: "model", parts: [{ text: "ok" }] }, finishReason: "STOP" }],
  usageMetadata: { promptTokenCount: 4, candidatesTokenCount: 2 },
});

const messagesOkBody = JSON.stringify({
  id: "msg_1",
  type: "message",
  role: "assistant",
  model: "test-chat",
  content: [{ type: "text", text: "ok" }],
  stop_reason: "end_turn",
  usage: { input_tokens: 4, output_tokens: 2 },
});

const responsesOkBody = JSON.stringify({
  id: "resp_1",
  object: "response",
  status: "completed",
  model: "test-chat",
  output: [
    {
      id: "msg_1",
      type: "message",
      role: "assistant",
      status: "completed",
      content: [{ type: "output_text", text: "ok" }],
    },
  ],
  usage: { input_tokens: 4, output_tokens: 2 },
});

const imagesOkBody = JSON.stringify({
  created: 1,
  data: [{ b64_json: "AAAA" }, { b64_json: "AAAB" }],
});

test("symptom: non-default service_tier bypasses billing ceiling instead of returning 422 before hold", async () => {
  for (const tier of ["priority", "flex", "scale", 123, true]) {
    const res = await invokeInference(jsonBody, {
      operation: "chat",
      fields: { service_tier: tier },
    });
    expect(res.status, String(tier)).toBe(422);
    expect(res.wire).toContain("billing_ceiling");
    expect(res.holds).toBe(0);
    expect(res.dispatches).toBe(0);
  }

  for (const tier of ["auto", "default", "standard_only", null]) {
    const res = await invokeInference(jsonBody, {
      operation: "chat",
      fields: { service_tier: tier },
    });
    expect(res.status, String(tier)).toBe(200);
    expect(res.finalized).toEqual(["SETTLED"]);
  }

  const msgPriority = await invokeInference(messagesOkBody, {
    operation: "messages",
    fields: { max_tokens: 32, service_tier: "priority" },
  });
  expect(msgPriority.status).toBe(422);
  expect(msgPriority.wire).toContain("billing_ceiling");
  expect(msgPriority.holds).toBe(0);

  for (const geminiBad of [
    { serviceTier: "PRIORITY" },
    { serviceTier: "priority" },
    { serviceTier: "FLEX" },
    { service_tier: "priority" },
  ]) {
    const res = await invokeInference(geminiOkBody, {
      operation: "gemini",
      fields: {
        contents: [{ role: "user", parts: [{ text: "Hi" }] }],
        ...geminiBad,
      },
    });
    expect(res.status, JSON.stringify(geminiBad)).toBe(422);
    expect(res.wire).toContain("billing_ceiling");
    expect(res.holds).toBe(0);
    expect(res.dispatches).toBe(0);
  }

  for (const geminiOk of [
    { serviceTier: "STANDARD" },
    { service_tier: "standard" },
    { serviceTier: "UNSPECIFIED" },
  ]) {
    const res = await invokeInference(geminiOkBody, {
      operation: "gemini",
      fields: {
        contents: [{ role: "user", parts: [{ text: "Hi" }] }],
        ...geminiOk,
      },
    });
    expect(res.status, JSON.stringify(geminiOk)).toBe(200);
    expect(res.finalized).toEqual(["SETTLED"]);
  }
});

test("symptom: resolveOutputBound accepts cross-protocol, duplicate, or non-positive output bounds", async () => {
  const crossProtocolCases: Array<{
    operation: "chat" | "responses" | "messages" | "gemini";
    body: string;
    fields: Record<string, unknown>;
  }> = [
    { operation: "chat", body: jsonBody, fields: { max_output_tokens: 20 } },
    { operation: "responses", body: responsesOkBody, fields: { input: "hi", max_tokens: 20 } },
    {
      operation: "messages",
      body: messagesOkBody,
      fields: { max_completion_tokens: 20 },
    },
    {
      operation: "gemini",
      body: geminiOkBody,
      fields: { contents: [{ role: "user", parts: [{ text: "hi" }] }], max_tokens: 20 },
    },
    {
      operation: "chat",
      body: jsonBody,
      fields: { max_tokens: 20, max_completion_tokens: 40 },
    },
    {
      operation: "gemini",
      body: geminiOkBody,
      fields: {
        contents: [{ role: "user", parts: [{ text: "hi" }] }],
        generationConfig: { maxOutputTokens: 20 },
        generation_config: { max_output_tokens: 40 },
      },
    },
    {
      operation: "gemini",
      body: geminiOkBody,
      fields: {
        contents: [{ role: "user", parts: [{ text: "hi" }] }],
        generationConfig: { temperature: 0.2 },
        generation_config: { max_output_tokens: 40 },
      },
    },
    { operation: "chat", body: jsonBody, fields: { max_tokens: 0 } },
    { operation: "chat", body: jsonBody, fields: { max_completion_tokens: -5 } },
    {
      operation: "responses",
      body: responsesOkBody,
      fields: { input: "hi", max_output_tokens: 1.5 },
    },
    { operation: "messages", body: messagesOkBody, fields: { max_tokens: "32" } },
  ];

  for (const tc of crossProtocolCases) {
    const res = await invokeInference(tc.body, {
      operation: tc.operation,
      fields: tc.fields,
    });
    expect(res.status, `${tc.operation}:${JSON.stringify(tc.fields)}`).toBe(400);
    expect(res.wire).toContain("invalid_request");
    expect(res.holds).toBe(0);
    expect(res.dispatches).toBe(0);
  }
});

test("symptom: Gemini snake_case generation_config.max_output_tokens is ignored by reservation and duplicated by capKnownOutput", async () => {
  const unbounded = await invokeInference(geminiOkBody, {
    operation: "gemini",
    modelMaxOutputTokens: 500,
    fields: {
      contents: [{ role: "user", parts: [{ text: "Hi" }] }],
      generation_config: { temperature: 0.1 },
    },
  });
  expect(unbounded.status).toBe(200);

  const res = await invokeInference(geminiOkBody, {
    operation: "gemini",
    modelMaxOutputTokens: 500,
    fields: {
      contents: [{ role: "user", parts: [{ text: "Hi" }] }],
      generation_config: { max_output_tokens: 40, temperature: 0.1 },
    },
  });
  expect(res.status).toBe(200);
  expect(res.finalized).toEqual(["SETTLED"]);
  expect(Number(res.requestedCustomerAtoms)).toBe(
    Buffer.byteLength(res.dispatchedBodies[0]!) + 128 + 32 + 40,
  );
  expect(Number(unbounded.requestedCustomerAtoms)).toBe(
    Buffer.byteLength(unbounded.dispatchedBodies[0]!) + 128 + 32 + 500,
  );

  const sent = JSON.parse(res.dispatchedBodies[0]!) as Record<string, unknown>;
  expect(sent.generationConfig).toBeUndefined();
  expect(sent.generation_config).toEqual({ max_output_tokens: 40, temperature: 0.1 });

  const withNullCamel = await invokeInference(geminiOkBody, {
    operation: "gemini",
    modelMaxOutputTokens: 100,
    fields: {
      contents: [{ role: "user", parts: [{ text: "Hi" }] }],
      generationConfig: null,
      generation_config: { max_output_tokens: 250 },
    },
  });
  expect(withNullCamel.status).toBe(200);
  const sentNullCamel = JSON.parse(withNullCamel.dispatchedBodies[0]!) as Record<string, unknown>;
  expect(sentNullCamel.generationConfig).toBeUndefined();
  expect(sentNullCamel.generation_config).toEqual({ max_output_tokens: 100 });

  const mixedSnake: Record<string, unknown> = {
    generation_config: { maxOutputTokens: 40 },
  };
  capKnownOutput(mixedSnake, "gemini", 100);
  expect(mixedSnake).toEqual({ generation_config: { max_output_tokens: 40 } });

  const mixedCamelNull: Record<string, unknown> = {
    generationConfig: { maxOutputTokens: null, max_output_tokens: 40 },
  };
  capKnownOutput(mixedCamelNull, "gemini", 100);
  expect(mixedCamelNull).toEqual({ generationConfig: { maxOutputTokens: 40 } });
});

test("symptom: chat stream_options injection wipes sibling options like include_obfuscation", async () => {
  const sseBody =
    'data: {"id":"c1","object":"chat.completion.chunk","model":"test-chat","choices":[{"index":0,"delta":{"content":"hi"}}],"usage":{"prompt_tokens":3,"completion_tokens":2}}\n\n' +
    "data: [DONE]\n\n";
  const res = await invokeInference(sseBody, {
    operation: "chat",
    stream: true,
    fast: true,
    fields: { stream_options: { include_obfuscation: false } },
  });
  expect(res.status).toBe(200);
  const sent = JSON.parse(res.dispatchedBodies[0]!) as Record<string, unknown>;
  expect(sent.stream_options).toEqual({ include_obfuscation: false, include_usage: true });
});

test("symptom: cross-protocol chat to messages clamps omitted max_tokens to hardcoded 32768 instead of model max_output_tokens", async () => {
  const res = await invokeInference(messagesOkBody, {
    operation: "chat",
    upstreamKind: "anthropic",
    modelMaxOutputTokens: 65536,
    heldAtoms: "100000",
  });
  expect(res.status).toBe(200);
  expect(res.finalized).toEqual(["SETTLED"]);
  const sent = JSON.parse(res.dispatchedBodies[0]!) as Record<string, unknown>;
  expect(sent.max_tokens).toBe(65536);
});

test("symptom: images and multipart image_edits accept invalid or duplicate n values instead of returning 400", async () => {
  for (const badN of [0, -1, 1.5, "2", true, {}]) {
    const res = await invokeInference(imagesOkBody, {
      operation: "images",
      fields: { prompt: "a cat", n: badN },
    });
    expect(res.status, String(badN)).toBe(400);
    expect(res.wire).toContain("invalid_request");
    expect(res.holds).toBe(0);
    expect(res.dispatches).toBe(0);
  }

  const nullN = await invokeInference(
    JSON.stringify({ created: 1, data: [{ b64_json: "AAAA" }] }),
    {
      operation: "images",
      fields: { prompt: "a cat", n: null },
    },
  );
  expect(nullN.status).toBe(200);
  expect(nullN.requestedCustomerAtoms).toBe("10");

  const dupForm = new FormData();
  dupForm.append("model", "test-chat");
  dupForm.append("prompt", "edit");
  dupForm.append("n", "5");
  dupForm.append("n", "1");
  dupForm.append("image", new File(["png"], "a.png", { type: "image/png" }));
  const dupRes = await invokeInference(imagesOkBody, {
    operation: "image_edits",
    multipartBody: dupForm,
  });
  expect(dupRes.status).toBe(400);
  expect(dupRes.wire).toContain("invalid_request");
  expect(dupRes.holds).toBe(0);

  const badMultipart = new FormData();
  badMultipart.append("model", "test-chat");
  badMultipart.append("prompt", "edit");
  badMultipart.append("n", "0");
  badMultipart.append("image", new File(["png"], "a.png", { type: "image/png" }));
  const badMultipartRes = await invokeInference(imagesOkBody, {
    operation: "image_edits",
    multipartBody: badMultipart,
  });
  expect(badMultipartRes.status).toBe(400);
  expect(badMultipartRes.holds).toBe(0);

  const validMultipart = new FormData();
  validMultipart.append("model", "test-chat");
  validMultipart.append("prompt", "edit");
  validMultipart.append("n", "2");
  validMultipart.append("image", new File(["png"], "a.png", { type: "image/png" }));
  const validMultipartRes = await invokeInference(imagesOkBody, {
    operation: "image_edits",
    multipartBody: validMultipart,
  });
  expect(validMultipartRes.status).toBe(200);
  expect(validMultipartRes.requestedCustomerAtoms).toBe("20");
});

test("uint64 overflow output token limits and image counts fail with 400 before hold across protocols", async () => {
  const uint64OverflowCases: Array<{
    operation: "chat" | "responses" | "messages" | "gemini" | "images";
    body: string;
    rawJsonBody: string;
  }> = [
    {
      operation: "chat",
      body: jsonBody,
      rawJsonBody:
        '{"model":"test-chat","messages":[{"role":"user","content":"hi"}],"max_tokens":18446744073686646784}',
    },
    {
      operation: "chat",
      body: jsonBody,
      rawJsonBody:
        '{"model":"test-chat","messages":[{"role":"user","content":"hi"}],"max_completion_tokens":18446744073686646784}',
    },
    {
      operation: "responses",
      body: responsesOkBody,
      rawJsonBody: '{"model":"test-chat","input":"hi","max_output_tokens":18446744073686646784}',
    },
    {
      operation: "messages",
      body: messagesOkBody,
      rawJsonBody:
        '{"model":"test-chat","messages":[{"role":"user","content":"hi"}],"max_tokens":18446744073686646784}',
    },
    {
      operation: "gemini",
      body: geminiOkBody,
      rawJsonBody:
        '{"model":"test-chat","contents":[{"role":"user","parts":[{"text":"hi"}]}],"generationConfig":{"maxOutputTokens":18446744073686646784}}',
    },
    {
      operation: "images",
      body: imagesOkBody,
      rawJsonBody: '{"model":"test-chat","prompt":"cat","n":18446744073686646784}',
    },
  ];
  for (const tc of uint64OverflowCases) {
    const res = await invokeInference(tc.body, {
      operation: tc.operation,
      rawJsonBody: tc.rawJsonBody,
    });
    expect(res.status, tc.operation).toBe(400);
    expect(res.wire).toContain("invalid_request");
    expect(res.holds).toBe(0);
    expect(res.dispatches).toBe(0);
  }
});

test("symptom: extended-precision decimal literals on images.n and max_tokens and invalid multipart stream bypass 400 validation", async () => {
  const exactImg = await invokeInference(imagesOkBody, {
    operation: "images",
    rawJsonBody: '{"model":"test-chat","prompt":"cat","n":1.0000000000000000001}',
  });
  expect(exactImg.status).toBe(400);
  expect(exactImg.wire).toContain("invalid_request");
  expect(exactImg.holds).toBe(0);
  expect(exactImg.dispatches).toBe(0);

  const exactChatMax = await invokeInference(jsonBody, {
    operation: "chat",
    rawJsonBody:
      '{"model":"test-chat","messages":[{"role":"user","content":"hi"}],"max_tokens":10.000000000000000001}',
  });
  expect(exactChatMax.status).toBe(400);
  expect(exactChatMax.wire).toContain("invalid_request");
  expect(exactChatMax.holds).toBe(0);
  expect(exactChatMax.dispatches).toBe(0);

  const exactGeminiMax = await invokeInference(geminiOkBody, {
    operation: "gemini",
    rawJsonBody:
      '{"model":"test-chat","contents":[{"role":"user","parts":[{"text":"hi"}]}],"generation_config":{"max_output_tokens":10.000000000000000001}}',
  });
  expect(exactGeminiMax.status).toBe(400);
  expect(exactGeminiMax.holds).toBe(0);

  const badStreamForm = new FormData();
  badStreamForm.append("model", "test-chat");
  badStreamForm.append("prompt", "edit");
  badStreamForm.append("stream", "notabool");
  badStreamForm.append("image", new File(["png"], "a.png", { type: "image/png" }));
  const badStreamRes = await invokeInference(imagesOkBody, {
    operation: "image_edits",
    multipartBody: badStreamForm,
  });
  expect(badStreamRes.status).toBe(400);
  expect(badStreamRes.wire).toContain("invalid_request");
  expect(badStreamRes.holds).toBe(0);
});

test("image delivery reconciliation bills actual fewer images and rejects excess, duplicate, empty, or conflicting image counts with 502", async () => {
  const fewer = await invokeInference(imagesOkBody, {
    operation: "images",
    fields: { prompt: "four cats", n: 4 },
  });
  expect(fewer.status).toBe(200);
  expect(fewer.requestedCustomerAtoms).toBe("40");
  expect(fewer.settledUsages).toEqual([{ images: "2" }]);

  const excess = await invokeInference(
    JSON.stringify({
      created: 1,
      data: [{ b64_json: "AAAA" }, { b64_json: "AAAB" }, { b64_json: "AAAC" }],
    }),
    {
      operation: "images",
      fields: { prompt: "two cats", n: 2 },
    },
  );
  expect(excess.status).toBe(502);
  expect(excess.wire).toContain("invalid_upstream_usage");
  expect(excess.finalized).toEqual(["UNKNOWN"]);

  const duplicate = await invokeInference(
    JSON.stringify({
      created: 1,
      data: [{ b64_json: "AAAA" }, { b64_json: "AAAA" }],
    }),
    {
      operation: "images",
      fields: { prompt: "two cats", n: 2 },
    },
  );
  expect(duplicate.status).toBe(502);
  expect(duplicate.wire).toContain("invalid_upstream_usage");
  expect(duplicate.finalized).toEqual(["UNKNOWN"]);

  const empty = await invokeInference(JSON.stringify({ created: 1, data: [] }), {
    operation: "images",
    fields: { prompt: "one cat", n: 1 },
  });
  expect(empty.status).toBe(502);
  expect(empty.finalized).toEqual(["UNKNOWN"]);

  const conflicting = await invokeInference(
    JSON.stringify({
      created: 1,
      data: [{ b64_json: "AAAA" }],
      usage: { images: 2 },
    }),
    {
      operation: "images",
      fields: { prompt: "two cats", n: 2 },
      overrides: {
        usage_aliases: [{ target: "images", source_path: "usage.images" }],
      },
    },
  );
  expect(conflicting.status).toBe(502);
  expect(conflicting.wire).toContain("usage_alias_conflict");
  expect(conflicting.finalized).toEqual(["UNKNOWN"]);
});

test("buildPreparedPayload re-caps output tokens raised by request_transforms, caps completions, and clears exact literals on Gemini snake_case cap", () => {
  const openaiPreset = getBuiltinProvider("openai")!;
  const openaiResolved = {
    source: "builtin" as const,
    name: openaiPreset.name,
    version: openaiPreset.version,
    plugin_id: null,
    kind: "DECLARATIVE" as const,
    config: openaiPreset.provider_config,
    preset: openaiPreset,
  };
  const chatTransform = mergeOverridesForResolved(openaiResolved, undefined, {
    request_transforms: [{ op: "force_set", path: "max_completion_tokens", value: 999 }],
  });
  const chatPrepared = buildPreparedPayload({
    resolved: openaiResolved,
    merged: chatTransform,
    operation: "chat",
    upstreamProtocol: "chat",
    upstreamModel: "gpt-5",
    clientBody: { model: "alias", messages: [{ role: "user", content: "hi" }], max_tokens: 20 },
    isStream: false,
    outputBound: 50,
  });
  expect(chatPrepared.payload.max_completion_tokens).toBe(50);
  expect(chatPrepared.payload.max_tokens).toBeUndefined();

  const completionsPrepared = buildPreparedPayload({
    resolved: openaiResolved,
    merged: mergeOverridesForResolved(openaiResolved),
    operation: "completions",
    upstreamProtocol: null,
    upstreamModel: "gpt-3.5-turbo-instruct",
    clientBody: { model: "alias", prompt: "hi", max_tokens: 500 },
    isStream: false,
    outputBound: 64,
  });
  expect(completionsPrepared.payload.max_tokens).toBe(64);

  const geminiPreset = getBuiltinProvider("gemini")!;
  const geminiResolved = {
    source: "builtin" as const,
    name: geminiPreset.name,
    version: geminiPreset.version,
    plugin_id: null,
    kind: "DECLARATIVE" as const,
    config: geminiPreset.provider_config,
    preset: geminiPreset,
  };
  const rawGemini = parseClientJson(
    '{"contents":[{"role":"user","parts":[{"text":"hi"}]}],"generation_config":{"max_output_tokens":9007199254740993}}',
  ) as Record<string, unknown>;
  const geminiPrepared = buildPreparedPayload({
    resolved: geminiResolved,
    merged: mergeOverridesForResolved(geminiResolved),
    operation: "gemini",
    upstreamProtocol: "gemini",
    upstreamModel: "gemini-2.5-pro",
    clientBody: rawGemini,
    isStream: false,
    outputBound: 128,
  });
  expect(stringifyExact(geminiPrepared.payload)).toContain('"max_output_tokens":128');
  expect(stringifyExact(geminiPrepared.payload)).not.toContain("9007199254740993");
});

test("metered ingress blocks multi-candidate, server-state, and hosted tool payloads across completions, gemini, messages, and responses while allowing local shell", async () => {
  const completionsOk = JSON.stringify({
    id: "cmpl_1",
    object: "text_completion",
    model: "test-chat",
    choices: [{ text: "ok", index: 0, finish_reason: "stop" }],
    usage: { prompt_tokens: 2, completion_tokens: 1 },
  });
  const blockedCases: Array<{
    label: string;
    operation: "completions" | "gemini" | "messages" | "responses";
    body: string;
    fields: Record<string, unknown>;
    code: string;
  }> = [
    {
      label: "completions best_of",
      operation: "completions",
      body: completionsOk,
      fields: { prompt: "hi", best_of: 2 },
      code: "billing_ceiling",
    },
    {
      label: "completions batch prompt array",
      operation: "completions",
      body: completionsOk,
      fields: { prompt: ["a", "b"] },
      code: "billing_ceiling",
    },
    {
      label: "gemini cachedContent",
      operation: "gemini",
      body: geminiOkBody,
      fields: {
        contents: [{ role: "user", parts: [{ text: "hi" }] }],
        cachedContent: "cachedContents/1",
      },
      code: "unmetered_context",
    },
    {
      label: "gemini cached_content",
      operation: "gemini",
      body: geminiOkBody,
      fields: {
        contents: [{ role: "user", parts: [{ text: "hi" }] }],
        cached_content: "cachedContents/1",
      },
      code: "unmetered_context",
    },
    {
      label: "gemini candidateCount",
      operation: "gemini",
      body: geminiOkBody,
      fields: {
        contents: [{ role: "user", parts: [{ text: "hi" }] }],
        generationConfig: { candidateCount: 2 },
      },
      code: "billing_ceiling",
    },
    {
      label: "gemini candidate_count",
      operation: "gemini",
      body: geminiOkBody,
      fields: {
        contents: [{ role: "user", parts: [{ text: "hi" }] }],
        generation_config: { candidate_count: 2 },
      },
      code: "billing_ceiling",
    },
    {
      label: "messages container",
      operation: "messages",
      body: messagesOkBody,
      fields: { max_tokens: 32, container: "cntr_1" },
      code: "unmetered_context",
    },
    {
      label: "responses item_reference",
      operation: "responses",
      body: responsesOkBody,
      fields: { input: [{ type: "item_reference", id: "item_1" }] },
      code: "unmetered_context",
    },
    {
      label: "responses hosted shell",
      operation: "responses",
      body: responsesOkBody,
      fields: {
        input: "hi",
        tools: [{ type: "shell", environment: { type: "container_auto" } }],
      },
      code: "unmetered_tool",
    },
  ];
  for (const tc of blockedCases) {
    const res = await invokeInference(tc.body, {
      operation: tc.operation,
      fields: tc.fields,
    });
    expect(res.status, tc.label).toBe(422);
    expect(res.wire, tc.label).toContain(tc.code);
    expect(res.holds, tc.label).toBe(0);
    expect(res.dispatches, tc.label).toBe(0);
  }

  const localShell = await invokeInference(responsesOkBody, {
    operation: "responses",
    fields: {
      input: "hi",
      tools: [{ type: "shell", environment: { type: "local" } }],
    },
  });
  expect(localShell.status).toBe(200);
  expect(localShell.finalized).toEqual(["SETTLED"]);
});
