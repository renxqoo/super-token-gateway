import { expect, test } from "bun:test";
import { requestPolicySchema, object } from "@gateway/contracts";
import { parseClientJson, validateRequestPolicy } from "../index.ts";

test("route policy rejects blocked and unlisted native parameters and accepts unconfigured extensions", () => {
  const body = { model: "alias", messages: [], temperature: 0, custom_extension: { value: false } };
  expect(() => validateRequestPolicy(body)).not.toThrow();
  expect(() => validateRequestPolicy(body, { blocked_parameters: ["temperature"] })).toThrow(
    "temperature",
  );
  expect(() =>
    validateRequestPolicy(body, { allowed_parameters: ["model", "messages", "temperature"] }),
  ).toThrow("custom_extension");
});

test("route policy limits are inclusive and reject out-of-range or non-numeric values", () => {
  const policy = {
    parameter_ranges: { max_tokens: { maximum: 64 }, temperature: { minimum: 0, maximum: 1 } },
  };
  const body = { messages: [], max_tokens: 64, temperature: 0 };
  expect(() => validateRequestPolicy(body, policy)).not.toThrow();
  expect(() => validateRequestPolicy({ ...body, max_tokens: 65 }, policy)).toThrow("max_tokens");
  expect(() => validateRequestPolicy({ ...body, temperature: 1.1 }, policy)).toThrow("temperature");
  expect(() => validateRequestPolicy({ ...body, temperature: "0" }, policy)).toThrow("temperature");
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
  expect(() => validateRequestPolicy(body, policy)).toThrow("strict");
  expect(() =>
    validateRequestPolicy(
      { messages: [{ role: "tool", content: '{"strict":true}', tool_call_id: "call" }] },
      policy,
    ),
  ).not.toThrow();
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
    expect(() => validateRequestPolicy(body, { parameter_ranges: { temperature: range } })).toThrow(
      "temperature",
    );
  }
  const body = object(
    parseClientJson('{"contents":[],"generationConfig":{"temperature":0.9999999999999999999}}'),
  );
  expect(() =>
    validateRequestPolicy(body, {
      parameter_ranges: { "generationConfig.temperature": { maximum: 1 } },
    }),
  ).not.toThrow();
});

test("route policy enforces nested extra_body blocks, multi-level array wildcards, non-finite range rejections, single-point bounds, and empty allowlists", () => {
  expect(() =>
    validateRequestPolicy(
      { model: "m", messages: [], extra_body: { internal_flag: true } },
      { blocked_parameters: ["extra_body.internal_flag"] },
    ),
  ).toThrow("extra_body.internal_flag");

  expect(() =>
    validateRequestPolicy(
      {
        contents: [],
        generationConfig: { thinkingConfig: { thinkingBudget: 1024 } },
      },
      { blocked_parameters: ["generationConfig.thinkingConfig"] },
    ),
  ).toThrow("generationConfig.thinkingConfig");

  expect(() =>
    validateRequestPolicy(
      {
        messages: [
          { role: "user", content: [{ type: "text", text: "ok" }] },
          {
            role: "user",
            content: [{ type: "image_url", image_url: { url: "https://example.com/a.png" } }],
          },
        ],
      },
      { blocked_parameters: ["messages.*.content.*.image_url"] },
    ),
  ).toThrow("messages.*.content.*.image_url");

  const rangePolicy = { parameter_ranges: { temperature: { minimum: 0, maximum: 1 } } };
  for (const invalidTemp of [
    true,
    false,
    "0.5",
    null,
    Number.NaN,
    Number.POSITIVE_INFINITY,
    -0.01,
  ]) {
    expect(() =>
      validateRequestPolicy({ messages: [], temperature: invalidTemp }, rangePolicy),
    ).toThrow("temperature");
  }
  const negTiny = object(parseClientJson('{"messages":[],"temperature":-0.0000000000000000001}'));
  expect(() => validateRequestPolicy(negTiny, rangePolicy)).toThrow("temperature");

  const pointPolicy = { parameter_ranges: { temperature: { minimum: 0.5, maximum: 0.5 } } };
  expect(() =>
    validateRequestPolicy({ messages: [], temperature: 0.5 }, pointPolicy),
  ).not.toThrow();
  expect(() => validateRequestPolicy({ messages: [], temperature: 0.51 }, pointPolicy)).toThrow(
    "temperature",
  );

  expect(() => validateRequestPolicy({ model: "m" }, { allowed_parameters: [] })).toThrow("model");
  expect(() =>
    validateRequestPolicy(
      { model: "m", secret_param: 1 },
      { allowed_parameters: ["model", "extra_body.safe"] },
    ),
  ).toThrow("secret_param");
});
