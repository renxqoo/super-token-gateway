import { expect, test } from "bun:test";
import type { Route, RequestPolicy } from "@gateway/kernel";
import type { Auth, Dependencies } from "../context.ts";
import type { BatchInput } from "../batch-input.ts";
import { prepareBatchInput, rewriteBatchInput } from "../batch-input.ts";

function candidate(model: string, request_policy: RequestPolicy = {}): Route {
  return {
    id: model,
    model_name: "alias",
    provider_id: "provider",
    kind: "openai",
    base_url: "http://provider.invalid",
    secret: "fixture",
    upstream_model: model,
    operations: ["chat"],
    priority: 1,
    weight: 1,
    pricing_id: "price",
    pricing: { currency: "USD", rates: {} },
    concurrency: 1,
    rpm: 1,
    request_policy,
  };
}

const auth = { project: { id: "project" }, credential: { models: [] } } as unknown as Auth;

test("Batch rewriting retains the accepted model version and output limit after configuration changes", async () => {
  let routes = [candidate("revision-1")];
  let limit = 8;
  const deps = {
    store: { routes: async () => routes, model: async () => ({ max_output_tokens: limit }) },
  } as unknown as Dependencies;
  const input: BatchInput = {
    model: "alias",
    operation: "chat",
    endpoint: "/v1/chat/completions",
    manifest: { items: 1, idsSha256: "fixture" },
    requests: [
      {
        custom_id: "row",
        method: "POST",
        url: "/v1/chat/completions",
        body: { model: "alias", messages: [], temperature: 0 },
      },
    ],
  };
  const preparation = await prepareBatchInput(deps, auth, input, routes[0]!);
  routes = [candidate("revision-2", { blocked_parameters: ["temperature"] })];
  limit = 1;
  const rewritten = await rewriteBatchInput(deps, auth, input, preparation);
  expect(rewritten.upstreamModel).toBe("revision-1");
  expect(JSON.parse(await rewritten.file.text()).body).toMatchObject({
    model: "revision-1",
    temperature: 0,
    max_completion_tokens: 8,
  });
});

test("Batch preparation considers compatible model revisions on the same provider for the entire file", async () => {
  const routes = [
    candidate("revision-1", { blocked_parameters: ["top_p"] }),
    candidate("revision-2"),
  ];
  const deps = {
    store: { routes: async () => routes, model: async () => ({ max_output_tokens: 8 }) },
  } as unknown as Dependencies;
  const input: BatchInput = {
    model: "alias",
    operation: "chat",
    endpoint: "/v1/chat/completions",
    manifest: { items: 2, idsSha256: "fixture" },
    requests: [
      {
        custom_id: "first",
        method: "POST",
        url: "/v1/chat/completions",
        body: { model: "alias", messages: [], temperature: 0 },
      },
      {
        custom_id: "second",
        method: "POST",
        url: "/v1/chat/completions",
        body: { model: "alias", messages: [], top_p: 1 },
      },
    ],
  };
  const preparation = await prepareBatchInput(deps, auth, input, routes[1]!);
  const rewritten = await rewriteBatchInput(deps, auth, input, preparation);
  const rows = (await rewritten.file.text())
    .trim()
    .split("\n")
    .map((line) => JSON.parse(line) as { custom_id: string; body: { model: string } });
  expect(rewritten.upstreamModel).toBe("revision-2");
  expect(rows.map((row) => [row.custom_id, row.body.model])).toEqual([
    ["first", "revision-2"],
    ["second", "revision-2"],
  ]);
});
