import { expect, test } from "bun:test";
import { requestPolicySchema, object } from "@gateway/contracts";
import { parseClientJson } from "../index.ts";
import { prepareOperation } from "../index.ts";

test("route policy rejects incompatible native parameters while retaining unconfigured extensions", () => {
  const body = { model: "alias", messages: [], temperature: 0, custom_extension: { value: false } };
  expect(prepareOperation(body, "chat", "openai", "target").body.custom_extension).toEqual({
    value: false,
  });
  expect(() =>
    prepareOperation(body, "chat", "openai", "target", { blocked_parameters: ["temperature"] }),
  ).toThrow("temperature");
  expect(() =>
    prepareOperation(body, "chat", "openai", "target", {
      allowed_parameters: ["model", "messages", "temperature"],
    }),
  ).toThrow("custom_extension");
  expect(body.model).toBe("alias");
});

test("route policy checks converted target field names and inclusive model limits", () => {
  const policy = {
    parameter_ranges: { max_tokens: { maximum: 64 }, temperature: { minimum: 0, maximum: 1 } },
  };
  const body = { messages: [], max_completion_tokens: 64, temperature: 0 };
  expect(prepareOperation(body, "chat", "deepseek", "target", policy).body.max_tokens).toBe(64);
  expect(() =>
    prepareOperation({ ...body, max_completion_tokens: 65 }, "chat", "deepseek", "target", policy),
  ).toThrow("max_tokens");
  expect(() =>
    prepareOperation({ ...body, temperature: 1.1 }, "chat", "deepseek", "target", policy),
  ).toThrow("temperature");
  expect(() =>
    prepareOperation({ ...body, temperature: "0" }, "chat", "deepseek", "target", policy),
  ).toThrow("temperature");
});

test("route policy matches nested array controls without scanning user tool arguments", () => {
  const policy = { blocked_parameters: ["tools.*.function.strict"] };
  const body = {
    messages: [{ role: "user", content: "strict" }],
    tools: [
      {
        type: "function",
        function: { name: "run", strict: false, parameters: { type: "object" } },
      },
    ],
  };
  expect(() => prepareOperation(body, "chat", "openai", "target", policy)).toThrow("strict");
  expect(
    prepareOperation(
      { messages: [{ role: "tool", content: '{"strict":true}', tool_call_id: "call" }] },
      "chat",
      "openai",
      "target",
      policy,
    ).protocol,
  ).toBe("chat");
});

test("invalid policy paths and reversed or empty ranges fail configuration validation", () => {
  for (const policy of [
    { blocked_parameters: ["tools..strict"] },
    { parameter_ranges: { temperature: { minimum: 2, maximum: 1 } } },
    { parameter_ranges: { temperature: {} } },
  ])
    expect(requestPolicySchema.safeParse(policy).success).toBe(false);
});

test("native extended precision numbers cannot cross a model range through binary rounding", () => {
  for (const [literal, range] of [
    ["1.0000000000000000001", { maximum: 1 }],
    ["0.9999999999999999999", { minimum: 1 }],
    ["-1.0000000000000000001", { minimum: -1 }],
    ["5e-99999", { maximum: 0 }],
    ["9007199254740993", { maximum: 9007199254740992 }],
  ] as const) {
    const body = object(parseClientJson(`{"messages":[],"temperature":${literal}}`));
    expect(() =>
      prepareOperation(body, "chat", "openai", "target", {
        parameter_ranges: { temperature: range },
      }),
    ).toThrow("temperature");
  }
  const body = object(
    parseClientJson('{"contents":[],"generationConfig":{"temperature":0.9999999999999999999}}'),
  );
  expect(
    prepareOperation(body, "gemini", "gemini", "target", {
      parameter_ranges: { "generationConfig.temperature": { maximum: 1 } },
    }).protocol,
  ).toBe("gemini");
});
