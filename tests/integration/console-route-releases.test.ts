import { expect, test } from "bun:test";
import { createGatewayRuntime } from "@gateway/gateway";
import {
  accountId,
  adminAccess,
  billingAccountId,
  channelId,
  customerAccess,
  customerPriceId,
  h,
  registerSyncFixture,
  supplierPriceId,
} from "./console-sync-fixture.ts";

registerSyncFixture();

const model = "sync-gpt";
const releases = "/admin/v1/models/sync-gpt/route-releases";
const routing = "/admin/v1/models/sync-gpt/routing";

type RoutePair = {
  id: string;
  source_revision: number;
  priority: number;
  weight: number;
};

type RoutingView = { config_epoch: number; routes: RoutePair[] };

async function createRoute(priority: number, weight: number, channel = channelId) {
  const input = {
    model_name: model,
    channel_id: channel,
    upstream_model: "gpt-4o",
    operations: ["chat"],
    billing_price_id: customerPriceId,
    cost_version_id: supplierPriceId,
    priority,
    weight,
  };
  const result = await h.adminApi<{ id: string }>("POST", "/admin/v1/routes", adminAccess, input);
  expect(result.status).toBe(201);
  return { ...input, id: result.data.id };
}

async function releaseInput(first: string, second: string) {
  const view = await h.adminApi<RoutingView>("GET", routing, adminAccess);
  expect(view.status).toBe(200);
  const revisions = new Map(view.data.routes.map((route) => [route.id, route.source_revision]));
  return {
    id: crypto.randomUUID(),
    expected_epoch: view.data.config_epoch,
    targets: [
      { route_id: first, expected_revision: revisions.get(first)!, priority: 0, weight: 30 },
      { route_id: second, expected_revision: revisions.get(second)!, priority: 10, weight: 70 },
    ],
  };
}

async function liveRoutes(first: string, second: string) {
  const view = await h.adminApi<RoutingView>("GET", routing, adminAccess);
  expect(view.status).toBe(200);
  const current = new Map(view.data.routes.map((route) => [route.id, route]));
  return [current.get(first), current.get(second)];
}

test("a two-route release survives a lost response, confirms node application, and reverses as one change", async () => {
  const backup = await h.adminApi<{ id: string }>("POST", "/admin/v1/channels", adminAccess, {
    account_id: accountId,
    name: "route-release-backup",
    kind: "openai",
    base_url: h.mockUrl,
    api_key: "route-release-backup-key",
  });
  expect(backup.status).toBe(201);
  const first = await createRoute(10, 70);
  const second = await createRoute(0, 30, backup.data.id);
  const key = await h.clientApi<{ key: string }>(
    "POST",
    "/api/v1/billing-accounts/" + billingAccountId + "/api-keys",
    customerAccess,
    { name: "route-release-test" },
  );
  expect(key.status).toBe(201);
  const chat = async (upstreamKey: string) => {
    const result = await fetch(h.gatewayPublicUrl + "/v1/chat/completions", {
      method: "POST",
      headers: { authorization: "Bearer " + key.data.key, "content-type": "application/json" },
      body: JSON.stringify({
        model,
        messages: [{ role: "user", content: "hello" }],
        max_tokens: 100,
      }),
    });
    expect(result.status).toBe(200);
    await result.json();
    expect(h.getLastUpstreamAuth()).toBe("Bearer " + upstreamKey);
    return result.headers.get("x-request-id")!;
  };
  const originalRequest = await chat("sk-sync-initial");
  const input = await releaseInput(first.id, second.id);
  const denied = await h.adminApi("POST", releases, customerAccess, input);
  expect(denied.status).toBe(401);
  const preview = await h.adminApi("POST", releases + "/validate", adminAccess, input);
  expect(preview.status).toBe(200);
  expect(await liveRoutes(first.id, second.id)).toMatchObject([
    { priority: 10, weight: 70, source_revision: 1 },
    { priority: 0, weight: 30, source_revision: 1 },
  ]);

  let lost = false;
  h.setGatewayInterceptor(async (url, init, forward) => {
    const response = await forward();
    if (init?.method === "POST" && new URL(url).pathname.endsWith("/route-releases")) {
      expect(response.status).toBe(200);
      await response.text();
      lost = true;
      throw new Error("Response lost after commit");
    }
    return response;
  });
  try {
    const result = await h.adminApi("POST", releases, adminAccess, input);
    expect(result.status).toBe(502);
    expect(lost).toBe(true);
    const blocked = await h.adminApi("PATCH", "/admin/v1/routes/" + first.id, adminAccess, {
      weight: 50,
    });
    expect(blocked.status).toBe(409);
  } finally {
    h.setGatewayInterceptor(null);
  }

  const committed = await h.adminApi("GET", releases + "/" + input.id, adminAccess);
  expect(committed.status).toBe(200);
  expect(committed.data).toMatchObject({
    id: input.id,
    config_epoch: input.expected_epoch + 1,
  });
  const nodes = committed.data.nodes as { status: string }[];
  expect(nodes.length).toBeGreaterThan(0);
  expect(nodes.some((node) => node.status === "APPLIED")).toBe(true);
  expect(await liveRoutes(first.id, second.id)).toMatchObject([
    { priority: 0, weight: 30, source_revision: 2 },
    { priority: 10, weight: 70, source_revision: 2 },
  ]);
  const releasedRequest = await chat("route-release-backup-key");
  const charged = await h.gwDb`
    SELECT customer_atoms::text AS atoms FROM public.billing_charges
    WHERE request_id IN (${originalRequest}, ${releasedRequest})
  `;
  expect(charged).toEqual([{ atoms: "2000" }, { atoms: "2000" }]);
  const projected = await h.adminApi("GET", "/admin/v1/routes", adminAccess);
  for (const target of input.targets) {
    expect(
      (projected.data.items as { id: string }[]).find((item) => item.id === target.route_id),
    ).toMatchObject({
      priority: target.priority,
      weight: target.weight,
      syncedRevision: 2,
    });
  }

  const stale = await h.gatewayInternalApi("PUT", "/internal/v1/routes", {
    ...first,
    source_revision: 2,
    expected_source_revision: 1,
  });
  expect(stale.status).toBe(409);

  const repeated = await h.adminApi("POST", releases, adminAccess, input);
  expect(repeated.status).toBe(200);
  expect(repeated.data.config_epoch).toBe(input.expected_epoch + 1);
  const conflict = await h.adminApi("POST", releases, adminAccess, {
    ...input,
    targets: [{ ...input.targets[0], weight: 31 }, input.targets[1]],
  });
  expect(conflict.status).toBe(409);

  const undo = { id: crypto.randomUUID(), expected_epoch: committed.data.config_epoch };
  const rollback = await h.adminApi(
    "POST",
    releases + "/" + input.id + "/rollback",
    adminAccess,
    undo,
  );
  const replayedRollback = await h.gatewayInternalApi(
    "POST",
    "/internal/v1/models/sync-gpt/route-releases/" + input.id.toUpperCase() + "/rollback",
    undo,
  );
  expect(replayedRollback.status).toBe(200);
  expect(replayedRollback.data.config_epoch).toBe(input.expected_epoch + 2);
  expect(rollback.status).toBe(200);
  expect(rollback.data).toMatchObject({
    rollback_of: input.id,
    config_epoch: input.expected_epoch + 2,
  });
  expect(await liveRoutes(first.id, second.id)).toMatchObject([
    { priority: 10, weight: 70, source_revision: 3 },
    { priority: 0, weight: 30, source_revision: 3 },
  ]);
});

test("a stale member blocks the entire release and leaves both deployed values intact", async () => {
  const first = await createRoute(10, 70);
  const second = await createRoute(0, 30);
  const input = await releaseInput(first.id, second.id);
  const updated = await h.adminApi("PATCH", "/admin/v1/routes/" + second.id, adminAccess, {
    weight: 35,
  });
  expect(updated.status).toBe(200);
  const stale = await h.adminApi("POST", releases, adminAccess, input);
  expect(stale.status).toBe(409);
  expect(await liveRoutes(first.id, second.id)).toMatchObject([
    { priority: 10, weight: 70, source_revision: 1 },
    { priority: 0, weight: 35, source_revision: 2 },
  ]);
});

test("parallel publishes leave one committed pair and replicas confirm the exact epoch", async () => {
  const first = await createRoute(10, 70);
  const second = await createRoute(0, 30);
  const peer = createGatewayRuntime({ config: h.gatewayConfig, db: h.gwDb, wasmExecutor: null });
  try {
    await peer.start();
    const input = await releaseInput(first.id, second.id);
    const competing = {
      ...input,
      id: crypto.randomUUID(),
      targets: [
        { ...input.targets[0]!, weight: 20 },
        { ...input.targets[1]!, weight: 80 },
      ],
    };
    const results = await Promise.all([
      h.adminApi("POST", releases, adminAccess, input),
      h.adminApi("POST", releases, adminAccess, competing),
    ]);
    expect(results.map((result) => result.status).sort()).toEqual([200, 409]);
    const winner = results[0]!.status === 200 ? input : competing;
    const epoch = input.expected_epoch + 1;
    await peer.snapshot.refreshTo(epoch);
    const routes = peer.snapshot.get().routesByModel.get(model) ?? [];
    expect(
      winner.targets.map((target) => routes.find((route) => route.id === target.route_id)),
    ).toMatchObject(winner.targets.map(({ priority, weight }) => ({ priority, weight })));
    const observed = await h.adminApi("GET", releases + "/" + winner.id, adminAccess);
    expect(observed.data.config_epoch).toBe(epoch);
    expect(observed.data.nodes).toMatchObject([{ status: "APPLIED" }, { status: "APPLIED" }]);
    const loser = results[0]!.status === 409 ? input : competing;
    expect((await h.adminApi("GET", releases + "/" + loser.id, adminAccess)).status).toBe(404);
  } finally {
    await peer.close();
  }
});

test("an unavailable Gateway leaves a pending intent that the Console reconciler submits", async () => {
  const first = await createRoute(10, 70);
  const second = await createRoute(0, 30);
  const input = await releaseInput(first.id, second.id);
  const statusPath = releases + "/" + input.id;
  h.setGatewayInterceptor(async (url, init, forward) =>
    init?.method === "POST" && new URL(url).pathname.endsWith("/route-releases")
      ? new Response("unavailable", { status: 503 })
      : forward(),
  );
  try {
    expect((await h.adminApi("POST", releases, adminAccess, input)).status).toBe(503);
    const pending = await h.adminApi("GET", statusPath, adminAccess);
    expect(pending.status).toBe(202);
    expect(pending.data).toEqual({ id: input.id, state: "PENDING" });
  } finally {
    h.setGatewayInterceptor(null);
  }
  expect(await h.consoleRuntime.sync.routeReleases.reconcilePending()).toEqual({
    synced: 1,
    failed: 0,
  });
  const routes = await h.adminApi("GET", "/admin/v1/routes", adminAccess);
  expect((routes.data.items as { id: string }[]).find(({ id }) => id === first.id)).toMatchObject({
    weight: 30,
    syncedRevision: 2,
  });
  expect((await h.adminApi("GET", statusPath, adminAccess)).data.config_epoch).toBe(
    input.expected_epoch + 1,
  );
  const amended = await h.adminApi("PATCH", "/admin/v1/routes/" + first.id, adminAccess, {
    weight: 29,
  });
  expect(amended.status).toBe(200);
  const live = await liveRoutes(first.id, second.id);
  expect(live[0]).toMatchObject({ weight: 29, source_revision: 3 });
  const rollback = await h.adminApi("POST", statusPath + "/rollback", adminAccess, {
    id: crypto.randomUUID(),
    expected_epoch: input.expected_epoch + 2,
  });
  expect(rollback.status).toBe(409);
  expect(await liveRoutes(first.id, second.id)).toEqual(live);
});

test("Console refuses to project a different Gateway request that used the pending identifier", async () => {
  const first = await createRoute(10, 70);
  const second = await createRoute(0, 30);
  const input = await releaseInput(first.id, second.id);
  const alternate = {
    ...input,
    targets: [
      { ...input.targets[0]!, weight: 25 },
      { ...input.targets[1]!, weight: 75 },
    ],
  };
  h.setGatewayInterceptor(async (url, init, forward) => {
    if (init?.method !== "POST" || !new URL(url).pathname.endsWith("/route-releases"))
      return forward();
    const changed = await h.gatewayInternalApi(
      "POST",
      "/internal/v1/models/sync-gpt/route-releases",
      alternate,
    );
    expect(changed.status).toBe(200);
    throw new Error("Console could not observe which request committed");
  });
  try {
    expect((await h.adminApi("POST", releases, adminAccess, input)).status).toBe(502);
  } finally {
    h.setGatewayInterceptor(null);
  }
  const report = await h.adminApi("GET", releases + "/" + input.id, adminAccess);
  expect(report.status).toBe(409);
  expect(report.data.error).toMatchObject({ code: "route_release_conflict" });
  const listed = await h.adminApi("GET", "/admin/v1/routes", adminAccess);
  expect((listed.data.items as { id: string }[]).find(({ id }) => id === first.id)).toMatchObject({
    weight: 70,
    syncedRevision: 1,
  });
  expect(await h.consoleRuntime.sync.routeReleases.reconcilePending()).toEqual({
    synced: 0,
    failed: 1,
  });
  expect((await h.adminApi("GET", releases + "/" + input.id, adminAccess)).status).toBe(409);
  const amended = await h.adminApi("PATCH", "/admin/v1/routes/" + first.id, adminAccess, {
    weight: 24,
  });
  expect(amended.status).toBe(200);
  expect(await liveRoutes(first.id, second.id)).toMatchObject([
    { priority: 0, weight: 24, source_revision: 3 },
    { priority: 10, weight: 75, source_revision: 2 },
  ]);
});

test("a direct Gateway release can be adopted into matching Console route versions", async () => {
  const first = await createRoute(10, 70);
  const second = await createRoute(0, 30);
  const input = await releaseInput(first.id, second.id);
  const changed = await h.gatewayInternalApi(
    "POST",
    "/internal/v1/models/sync-gpt/route-releases",
    input,
  );
  expect(changed.status).toBe(200);
  expect((await h.adminApi("GET", releases + "/" + input.id, adminAccess)).status).toBe(200);
  const amended = await h.adminApi("PATCH", "/admin/v1/routes/" + first.id, adminAccess, {
    weight: 28,
  });
  expect(amended.status).toBe(200);
  expect(await liveRoutes(first.id, second.id)).toMatchObject([
    { priority: 0, weight: 28, source_revision: 3 },
    { priority: 10, weight: 70, source_revision: 2 },
  ]);
});

test("a stale route edit cannot claim success or block adoption of two direct releases", async () => {
  const first = await createRoute(10, 70);
  const second = await createRoute(0, 30);
  const gatewayPath = "/internal/v1/models/sync-gpt/route-releases";
  const firstRelease = await releaseInput(first.id, second.id);
  expect((await h.gatewayInternalApi("POST", gatewayPath, firstRelease)).status).toBe(200);
  const secondRelease = await releaseInput(first.id, second.id);
  secondRelease.targets[0]!.weight = 31;
  expect((await h.gatewayInternalApi("POST", gatewayPath, secondRelease)).status).toBe(200);

  const routePath = "/admin/v1/routes/" + first.id;
  const stale = await h.adminApi("PATCH", routePath, adminAccess, { weight: 28 });
  expect(stale.status, JSON.stringify(stale.data)).toBe(409);
  expect(stale.data.error).toMatchObject({ code: "route_release_conflict" });
  expect(await liveRoutes(first.id, second.id)).toMatchObject([
    { weight: 31, source_revision: 3 },
    { weight: 70, source_revision: 3 },
  ]);

  for (const { id } of [firstRelease, secondRelease]) {
    expect((await h.adminApi("GET", releases + "/" + id, adminAccess)).status).toBe(200);
  }
  const updated = await h.adminApi("PATCH", routePath, adminAccess, { weight: 28 });
  expect(updated.status).toBe(200);
  expect(await liveRoutes(first.id, second.id)).toMatchObject([
    { weight: 28, source_revision: 4 },
    { weight: 70, source_revision: 3 },
  ]);
});

test("a second route edit can sync after the first edit commits but loses its reply", async () => {
  const first = await createRoute(10, 70);
  const second = await createRoute(0, 30);
  const input = await releaseInput(first.id, second.id);
  expect((await h.adminApi("POST", releases, adminAccess, input)).status).toBe(200);

  const routePath = "/admin/v1/routes/" + first.id;
  h.setGatewayInterceptor(async (url, init, forward) => {
    const response = await forward();
    if (init?.method === "PUT" && new URL(url).pathname === "/internal/v1/routes") {
      expect(response.status).toBe(200);
      await response.text();
      throw new Error("Route update reply lost after commit");
    }
    return response;
  });
  try {
    expect((await h.adminApi("PATCH", routePath, adminAccess, { weight: 40 })).status).toBe(502);
  } finally {
    h.setGatewayInterceptor(null);
  }

  const latest = await h.adminApi("PATCH", routePath, adminAccess, { priority: 20 });
  expect(latest.status, JSON.stringify(latest.data)).toBe(200);
  expect(latest.data).toMatchObject({
    weight: 40,
    priority: 20,
    syncedRevision: 4,
    syncStatus: "SYNCED",
  });
  expect(await liveRoutes(first.id, second.id)).toMatchObject([
    { priority: 20, weight: 40, source_revision: 4 },
    { priority: 10, weight: 70, source_revision: 2 },
  ]);
});

test("a shared character price does not allow a release across different enabled content pipelines", async () => {
  const modelName = "route-release-character-pricing";
  const path = `/admin/v1/models/${modelName}/route-releases/validate`;
  expect(
    (
      await h.adminApi("POST", "/admin/v1/models", adminAccess, {
        name: modelName,
        display_name: "Character priced model",
        operations: ["chat"],
      })
    ).status,
  ).toBe(201);
  const price = await h.gatewayInternalApi<{ id: string }>("POST", "/internal/v1/prices", {
    side: "CUSTOMER",
    basis: "INPUT_CHARACTERS",
    currency: "USD",
    rates: { characters: { numerator: "1", denominator: "1" } },
    ceilings: { characters: "1000" },
  });
  expect(price.status).toBe(201);
  const routes: string[] = [];
  for (const priority of [10, 0]) {
    const route = await h.adminApi<{ id: string }>("POST", "/admin/v1/routes", adminAccess, {
      model_name: modelName,
      channel_id: channelId,
      upstream_model: "gpt-4o",
      operations: ["chat"],
      priority,
      billing_price_id: price.data.id,
      cost_version_id: supplierPriceId,
    });
    expect(route.status).toBe(201);
    routes.push(route.data.id);
  }
  const validate = () =>
    h.adminApi("POST", path, adminAccess, {
      id: crypto.randomUUID(),
      expected_epoch: h.gateway.snapshot.get().epoch,
      targets: routes.map((route_id, index) => ({
        route_id,
        expected_revision: 1,
        priority: index * 10,
        weight: 1,
      })),
    });
  expect((await validate()).status).toBe(200);

  const plugin = await h.gatewayInternalApi<{ id: string }>("PUT", "/internal/v1/plugins", {
    name: "route-release-character-transform",
    version: "1",
    kind: "WEBHOOK",
    manifest: { hooks: ["on_request"] },
    webhook_config: { url: h.mockUrl + "/policy" },
  });
  expect(plugin.status).toBe(200);
  const bind = (route_id: string, priority: number) =>
    h.gatewayInternalApi("POST", "/internal/v1/plugin-releases", {
      id: crypto.randomUUID(),
      expected_epoch: h.gateway.snapshot.get().epoch,
      targets: [{ route_id, plugin_pipeline: [{ plugin_id: plugin.data.id, priority }] }],
      samples: { [plugin.data.id]: { webhook_reply: { action: "TRANSFORM", text: "short" } } },
    });
  const bound = await bind(routes[0]!, 0);
  expect(bound.status, JSON.stringify(bound.data)).toBe(200);
  const rejected = await validate();
  expect(rejected.status, JSON.stringify(rejected.data)).toBe(400);
  expect(rejected.data.error).toMatchObject({ code: "invalid_route_release" });
  expect((await bind(routes[1]!, 10)).status).toBe(200);
  expect((await validate()).status).toBe(400);
  expect((await bind(routes[1]!, 0)).status).toBe(200);
  expect((await validate()).status).toBe(200);
});

test("a different price on a competing third route prevents changing the two target routes", async () => {
  const first = await createRoute(10, 70);
  const second = await createRoute(0, 30);
  const third = await createRoute(5, 25);
  const price = await h.gatewayInternalApi<{ id: string }>("POST", "/internal/v1/prices", {
    side: "CUSTOMER",
    basis: "REQUEST_COMPLETION",
    currency: "USD",
    rates: { requests: { numerator: "11", denominator: "1" } },
    ceilings: { requests: "1" },
  });
  expect(price.status).toBe(201);
  const update = await h.adminApi("PATCH", "/admin/v1/routes/" + third.id, adminAccess, {
    billing_price_id: price.data.id,
  });
  expect(update.status).toBe(200);
  const input = await releaseInput(first.id, second.id);
  for (const endpoint of [releases + "/validate", releases]) {
    const result = await h.adminApi("POST", endpoint, adminAccess, input);
    expect(result.status).toBe(400);
    expect(result.data.error).toMatchObject({ code: "invalid_route_release" });
  }
  expect(await liveRoutes(first.id, second.id)).toMatchObject([
    { priority: 10, weight: 70, source_revision: 1 },
    { priority: 0, weight: 30, source_revision: 1 },
  ]);
  const amended = await h.adminApi("PATCH", "/admin/v1/routes/" + first.id, adminAccess, {
    weight: 69,
  });
  expect(amended.status).toBe(200);
});
