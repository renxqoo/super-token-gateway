import { expect, spyOn, test } from "bun:test";
import { createGatewayRuntime } from "@gateway/gateway";
import type { ModelOnboardingResult } from "@gateway/contracts";
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

import {
  draft,
  onboarding,
  statusPath,
  waitForCommit,
} from "./console-model-onboarding-fixture.ts";

registerSyncFixture();

test("draft price matches settlement and all deployments become routable together", async () => {
  const key = await h.clientApi<{ key: string }>(
    "POST",
    "/api/v1/billing-accounts/" + billingAccountId + "/api-keys",
    customerAccess,
    { name: "onboarding" },
  );
  expect(key.status).toBe(201);
  const input = await draft("onboarding/public");
  const market = "/api/v1/models/" + encodeURIComponent(input.model.name);
  const headers = { authorization: "Bearer " + key.data.key, "content-type": "application/json" };
  const visible = async () => {
    const response = await fetch(h.gatewayPublicUrl + "/v1/models", { headers });
    expect(response.status).toBe(200);
    const result = (await response.json()) as { data: { id: string }[] };
    return result.data.some(({ id }) => id === input.model.name);
  };

  const validated = await h.adminApi("POST", onboarding + "/validate", adminAccess, input);
  expect(validated.status).toBe(200);
  expect(validated.data).toMatchObject({ config_epoch: input.expected_epoch });
  const preview = await h.adminApi("POST", onboarding + "/preview", adminAccess, {
    release: input,
    quote: {
      route_id: input.deployments[0]!.id,
      operation: "chat",
      currency: "USD",
      billing_account_id: billingAccountId,
      usage: { input_tokens: "100", output_tokens: "50" },
      estimated_input_tokens: 100,
      output_bound: 100,
    },
  });
  expect(preview.status).toBe(200);
  expect(preview.data).toMatchObject({
    config_epoch: input.expected_epoch,
    route_id: input.deployments[0]!.id,
    customer_breakdown: { total_atoms: "2000" },
    supplier_breakdown: { total_atoms: "200" },
  });
  expect(await visible()).toBe(false);
  expect((await h.clientApi("GET", market, customerAccess)).status).toBe(404);

  const published = await h.adminApi<ModelOnboardingResult>("POST", onboarding, adminAccess, input);
  expect([200, 202]).toContain(published.status);
  expect(await waitForCommit(input)).toMatchObject({
    id: input.id,
    config_epoch: input.expected_epoch + 1,
    customer_price_id: customerPriceId,
  });
  expect(await visible()).toBe(true);
  const routing = await h.adminApi<{ routes: { id: string }[] }>(
    "GET",
    "/admin/v1/models/" + encodeURIComponent(input.model.name) + "/routing",
    adminAccess,
  );
  expect(routing.status).toBe(200);
  expect(routing.data.routes.map(({ id }) => id).sort()).toEqual(
    input.deployments.map(({ id }) => id).sort(),
  );
  expect((await h.clientApi("GET", market, customerAccess)).status).toBe(200);
  const inference = await fetch(h.gatewayPublicUrl + "/v1/chat/completions", {
    method: "POST",
    headers,
    body: JSON.stringify({
      model: input.model.name,
      messages: [{ role: "user", content: "hello" }],
      max_tokens: 100,
    }),
  });
  expect(inference.status).toBe(200);
  await inference.json();
  const requestId = inference.headers.get("x-request-id")!;
  const charged = await h.gwDb`
    SELECT customer_atoms::text AS atoms FROM public.billing_charges WHERE request_id = ${requestId}
  `;
  expect(charged).toEqual([{ atoms: "2000" }]);
});

test("the Gateway model list hides a model until it has an enabled deployment and after the last one is disabled", async () => {
  const name = "onboarding/deployment-visibility";
  const key = await h.clientApi<{ key: string }>(
    "POST",
    "/api/v1/billing-accounts/" + billingAccountId + "/api-keys",
    customerAccess,
    { name: "model-list-deployments" },
  );
  expect(key.status).toBe(201);
  const listed = async () => {
    const response = await fetch(h.gatewayPublicUrl + "/v1/models", {
      headers: { authorization: "Bearer " + key.data.key },
    });
    expect(response.status).toBe(200);
    const result = (await response.json()) as { data: { id: string }[] };
    return result.data.some(({ id }) => id === name);
  };

  expect(
    (
      await h.adminApi("POST", "/admin/v1/models", adminAccess, {
        name,
        display_name: "Deployment visibility",
        operations: ["chat"],
      })
    ).status,
  ).toBe(201);
  const visibleBeforeDeploy = await listed();
  const route = await h.adminApi<{ id: string }>("POST", "/admin/v1/routes", adminAccess, {
    model_name: name,
    channel_id: channelId,
    upstream_model: "gpt-4o",
    operations: ["chat"],
    billing_price_id: customerPriceId,
    cost_version_id: supplierPriceId,
  });
  expect(route.status).toBe(201);
  expect(await listed()).toBe(true);
  expect(
    (
      await h.adminApi("PATCH", "/admin/v1/routes/" + route.data.id, adminAccess, {
        enabled: false,
      })
    ).status,
  ).toBe(200);
  expect([visibleBeforeDeploy, await listed()]).toEqual([false, false]);
});

test("a lost response leaves the full model hidden until the second serving node applies the release", async () => {
  const catalogToken = "onboarding-test-catalog-only-0123456789abcdef";
  const peer = createGatewayRuntime({
    config: {
      ...h.gatewayConfig,
      internalCredentials: [
        {
          id: "onboarding-catalog-only",
          token_sha256: new Bun.CryptoHasher("sha256").update(catalogToken).digest("hex"),
          roles: ["catalog"],
          consumers: [],
        },
      ],
    },
    db: h.gwDb,
    wasmExecutor: null,
  });
  const holdPeer = spyOn(peer.snapshot, "pollEpoch").mockResolvedValue(undefined);
  try {
    await peer.start();
    const input = await draft("onboarding/recover");
    const market = "/api/v1/models/" + encodeURIComponent(input.model.name);
    const denied = await peer.internalApp.fetch(
      new Request("http://internal.test/internal/v1/models/onboarding/preview", {
        method: "POST",
        headers: { authorization: "Bearer " + catalogToken, "content-type": "application/json" },
        body: JSON.stringify({
          release: input,
          quote: { route_id: input.deployments[0]!.id, operation: "chat", currency: "USD" },
        }),
      }),
    );
    expect(denied.status).toBe(403);
    const deniedConsole = await h.adminApi("POST", onboarding, customerAccess, input);
    expect(deniedConsole.status).toBe(401);

    h.setGatewayInterceptor(async (url, init, forward) => {
      const response = await forward();
      if (init?.method === "POST" && new URL(url).pathname === "/internal/v1/models/onboarding") {
        expect(response.status).toBe(200);
        await response.text();
        throw new Error("Response lost after commit");
      }
      return response;
    });
    try {
      const lost = await h.adminApi("POST", onboarding, adminAccess, input);
      expect(lost.status).toBe(502);
    } finally {
      h.setGatewayInterceptor(null);
    }

    const waiting = await h.adminApi<ModelOnboardingResult>("GET", statusPath(input), adminAccess);
    expect(waiting.status).toBe(202);
    expect(waiting.data.nodes.some(({ status }) => status === "PENDING")).toBe(true);
    expect((await h.clientApi("GET", market, customerAccess)).status).toBe(404);
    const modelWrite = await h.adminApi("POST", "/admin/v1/models", adminAccess, input.model);
    expect(modelWrite.status).toBe(409);
    const routeWrite = await h.adminApi("POST", "/admin/v1/routes", adminAccess, {
      channel_id: channelId,
      model_name: input.model.name,
      upstream_model: "gpt-4o",
      operations: ["chat"],
      billing_price_id: customerPriceId,
      cost_version_id: supplierPriceId,
    });
    expect(routeWrite.status).toBe(409);
    const updateDraft = await h.adminApi(
      "PATCH",
      "/admin/v1/routes/" + input.deployments[0]!.id,
      adminAccess,
      { weight: 3 },
    );
    expect(updateDraft.status).toBe(409);

    const epoch = input.expected_epoch + 1;
    await peer.snapshot.refreshTo(epoch);
    expect(peer.snapshot.get().models.get(input.model.name)?.enabled).toBe(true);
    expect(
      peer.snapshot
        .get()
        .routesByModel.get(input.model.name)
        ?.map(({ id }) => id)
        .sort(),
    ).toEqual(input.deployments.map(({ id }) => id).sort());
    const projected = await waitForCommit(input);
    expect(projected.nodes.map(({ status }) => status)).toEqual(["APPLIED", "APPLIED"]);
    const repeated = await h.adminApi("POST", onboarding, adminAccess, {
      ...input,
      deployments: [...input.deployments].reverse(),
    });
    expect(repeated.status).toBe(200);
    expect(repeated.data).toMatchObject({ id: input.id, config_epoch: epoch, state: "COMMITTED" });
    const different = await h.adminApi("POST", onboarding, adminAccess, {
      ...input,
      model: { ...input.model, display_name: "Another model label" },
    });
    expect(different.status).toBe(409);
    expect((await h.clientApi("GET", market, customerAccess)).status).toBe(200);
  } finally {
    holdPeer.mockRestore();
    await peer.close();
  }
});

test("an unsupported deployment rejects the release and leaves no partial model on either database", async () => {
  const anthropic = await h.adminApi<{ id: string }>("POST", "/admin/v1/channels", adminAccess, {
    account_id: accountId,
    name: "unsupported-embeddings",
    kind: "anthropic",
    base_url: h.mockUrl,
    api_key: "synthetic-onboarding-key",
  });
  expect(anthropic.status).toBe(201);
  const input = await draft("onboarding/invalid-provider");
  input.model.operations = ["embeddings"];
  input.deployments[1]!.channel_id = anthropic.data.id;

  const invalid = await h.adminApi("POST", onboarding, adminAccess, input);
  expect(invalid.status).toBe(400);
  expect(invalid.data.error).toMatchObject({ code: "invalid_model_onboarding" });
  const later = await h.adminApi("GET", statusPath(input), adminAccess);
  expect(later.status).toBe(400);
  expect(later.data.error).toEqual(invalid.data.error);
  const config = await h.adminApi("GET", onboarding, adminAccess);
  expect(config.data.config_epoch).toBe(input.expected_epoch);
  const gatewayModels =
    await h.gwDb`SELECT name FROM public.models WHERE name = ${input.model.name}`;
  const projected =
    await h.conDb`SELECT name FROM public.model_catalog WHERE name = ${input.model.name}`;
  const routes = await h.gwDb`SELECT id FROM public.routes WHERE model_name = ${input.model.name}`;
  expect(gatewayModels).toEqual([]);
  expect(projected).toEqual([]);
  expect(routes).toEqual([]);
});

test("editing an onboarded model without a price preserves its market rate", async () => {
  const input = await draft("onboarding/retain-market-rate");
  expect([200, 202]).toContain((await h.adminApi("POST", onboarding, adminAccess, input)).status);
  await waitForCommit(input);
  const market = "/api/v1/models/" + encodeURIComponent(input.model.name);
  const initial = await h.clientApi("GET", market, customerAccess);
  expect(initial.status).toBe(200);
  expect(initial.data.pricing).toMatchObject({
    currency: "USD",
    list_rates: { input_tokens: { numerator: "10", denominator: "1" } },
  });

  const edited = await h.adminApi("POST", "/admin/v1/models", adminAccess, {
    ...input.model,
    display_name: "Renamed model",
  });
  expect(edited.status).toBe(200);
  const current = await h.clientApi("GET", market, customerAccess);
  expect(current.status).toBe(200);
  expect(current.data.display_name).toBe("Renamed model");
  expect(current.data.pricing).toEqual(initial.data.pricing);
});

test("marketplace stays hidden when a later Gateway configuration supersedes the committed model", async () => {
  const peer = createGatewayRuntime({ config: h.gatewayConfig, db: h.gwDb, wasmExecutor: null });
  const holdPeer = spyOn(peer.snapshot, "pollEpoch").mockResolvedValue(undefined);
  try {
    await peer.start();
    const input = await draft("onboarding/superseded");
    const published = await h.adminApi("POST", onboarding, adminAccess, input);
    expect(published.status).toBe(202);
    const gatewayModel = {
      name: input.model.name,
      operations: ["chat"],
      max_output_tokens: 100,
    };
    const stale = await h.gatewayInternalApi("PUT", "/internal/v1/models", {
      ...gatewayModel,
      enabled: false,
      source_revision: 2,
    });
    expect(stale.status).toBe(409);
    const disable = await h.gatewayInternalApi("PUT", "/internal/v1/models", {
      ...gatewayModel,
      enabled: false,
      expected_source_revision: 1,
      source_revision: 2,
    });
    expect(disable.status).toBe(200);
    await peer.snapshot.refreshTo(disable.data.config_epoch as number);
    const pending = await h.adminApi<ModelOnboardingResult>("GET", statusPath(input), adminAccess);
    expect(pending.status).toBe(202);
    expect(pending.data.current_configuration).toBe("CHANGED");
    const market = "/api/v1/models/" + encodeURIComponent(input.model.name);
    expect((await h.clientApi("GET", market, customerAccess)).status).toBe(404);
    expect(pending.data.observed_revisions?.model).toBe(2);
    const restore = await h.gatewayInternalApi("PUT", "/internal/v1/models", {
      ...gatewayModel,
      enabled: true,
      expected_source_revision: 2,
      source_revision: 3,
    });
    expect(restore.status).toBe(200);
    await peer.snapshot.refreshTo(restore.data.config_epoch as number);
    const recovered = await waitForCommit(input);
    expect(recovered.observed_revisions?.model).toBe(3);
    const modelWrite = await h.adminApi("POST", "/admin/v1/models", adminAccess, {
      ...input.model,
      enabled: false,
    });
    expect(modelWrite.status).toBe(200);
    expect(modelWrite.data).toMatchObject({
      syncedRevision: 4,
      enabled: false,
      syncStatus: "SYNCED",
    });
    expect(h.gateway.snapshot.get().models.get(input.model.name)?.enabled).toBe(false);
  } finally {
    holdPeer.mockRestore();
    await peer.close();
  }
});

test("the draft quote applies FX and account-specific pricing like the settled request", async () => {
  const name = "onboarding/fx-account";
  const price = await h.gatewayInternalApi<{ id: string }>("POST", "/internal/v1/prices", {
    currency: "EUR",
    side: "CUSTOMER",
    rates: {
      input_tokens: { numerator: "3", denominator: "1" },
      output_tokens: { numerator: "2", denominator: "1" },
    },
    ceilings: { input_tokens: "10000", output_tokens: "1000" },
  });
  expect(price.status).toBe(201);
  const fx = await h.gatewayInternalApi("POST", "/internal/v1/fx-rates", {
    base_currency: "EUR",
    quote_currency: "USD",
    rate_numerator: "2",
    rate_denominator: "1",
    effective_from: "2020-01-01T00:00:00.000Z",
  });
  expect(fx.status).toBe(201);
  const override = await h.adminApi("PUT", "/admin/v1/pricing-overrides", adminAccess, {
    billing_account_id: billingAccountId,
    model_name: name,
    operation: "chat",
    override_kind: "MULTIPLIER",
    multiplier_numerator: "3",
    multiplier_denominator: "4",
  });
  expect(override.status).toBe(200);
  const key = await h.clientApi<{ key: string }>(
    "POST",
    "/api/v1/billing-accounts/" + billingAccountId + "/api-keys",
    customerAccess,
    { name: "onboarding-fx" },
  );
  expect(key.status).toBe(201);

  const input = { ...(await draft(name)), billing_price_id: price.data.id };
  const quote = {
    release: input,
    quote: {
      route_id: input.deployments[0]!.id,
      operation: "chat",
      currency: "USD",
      usage: { input_tokens: "100", output_tokens: "50" },
    },
  };
  const baseQuote = await h.adminApi("POST", onboarding + "/preview", adminAccess, quote);
  expect(baseQuote.status).toBe(200);
  expect(baseQuote.data.customer_breakdown).toMatchObject({ total_atoms: "800", currency: "USD" });
  const accountQuote = await h.adminApi("POST", onboarding + "/preview", adminAccess, {
    ...quote,
    quote: { ...quote.quote, billing_account_id: billingAccountId },
  });
  expect(accountQuote.status).toBe(200);
  expect(accountQuote.data.customer_breakdown).toMatchObject({
    total_atoms: "600",
    currency: "USD",
  });

  const publish = await h.adminApi("POST", onboarding, adminAccess, input);
  expect([200, 202]).toContain(publish.status);
  expect((await waitForCommit(input)).customer_price_id).toBe(price.data.id);
  const market = await h.clientApi(
    "GET",
    "/api/v1/models/" + encodeURIComponent(name),
    customerAccess,
  );
  expect(market.status).toBe(200);
  expect(market.data.pricing).toMatchObject({
    currency: "EUR",
    list_rates: { input_tokens: { numerator: "3", denominator: "1" } },
  });
  const inference = await fetch(h.gatewayPublicUrl + "/v1/chat/completions", {
    method: "POST",
    headers: { authorization: "Bearer " + key.data.key, "content-type": "application/json" },
    body: JSON.stringify({
      model: name,
      messages: [{ role: "user", content: "hello" }],
      max_tokens: 100,
    }),
  });
  expect(inference.status).toBe(200);
  await inference.json();
  const requestId = inference.headers.get("x-request-id")!;
  const charge = await h.gwDb`
    SELECT customer_atoms::text AS atoms, currency FROM public.billing_charges WHERE request_id = ${requestId}
  `;
  expect(charge).toEqual([{ atoms: "600", currency: "USD" }]);
});

test("the reconciler publishes a durable draft when the initial request never reaches Gateway", async () => {
  const input = await draft("onboarding/reconcile");
  h.setGatewayInterceptor(async (url, init, forward) => {
    if (init?.method === "POST" && new URL(url).pathname === "/internal/v1/models/onboarding") {
      return new Response("Temporarily unavailable", { status: 503 });
    }
    return forward();
  });
  try {
    const unavailable = await h.adminApi("POST", onboarding, adminAccess, input);
    expect(unavailable.status).toBe(503);
  } finally {
    h.setGatewayInterceptor(null);
  }

  const pending = await h.adminApi("GET", statusPath(input), adminAccess);
  expect(pending.status).toBe(202);
  expect(pending.data).toEqual({ id: input.id, state: "PENDING" });
  await h.consoleRuntime.sync.routeReleases.reconcilePending();
  const committed = await waitForCommit(input);
  expect(committed).toMatchObject({ id: input.id, config_epoch: input.expected_epoch + 1 });
});

test("a second model edit can sync after the first edit commits but loses its reply", async () => {
  const input = await draft("onboarding/lost-model-edit");
  expect([200, 202]).toContain((await h.adminApi("POST", onboarding, adminAccess, input)).status);
  await waitForCommit(input);

  h.setGatewayInterceptor(async (url, init, forward) => {
    const response = await forward();
    if (init?.method === "PUT" && new URL(url).pathname === "/internal/v1/models") {
      expect(response.status).toBe(200);
      await response.text();
      throw new Error("Model update reply lost after commit");
    }
    return response;
  });
  try {
    const first = await h.adminApi("POST", "/admin/v1/models", adminAccess, {
      ...input.model,
      max_output_tokens: 200,
    });
    expect(first.status).toBe(502);
  } finally {
    h.setGatewayInterceptor(null);
  }

  const latest = await h.adminApi("POST", "/admin/v1/models", adminAccess, {
    ...input.model,
    max_output_tokens: 200,
    enabled: false,
  });
  expect(latest.status, JSON.stringify(latest.data)).toBe(200);
  expect(latest.data).toMatchObject({ enabled: false, syncedRevision: 3, syncStatus: "SYNCED" });
  expect(h.gateway.snapshot.get().models.get(input.model.name)).toMatchObject({
    enabled: false,
    max_output_tokens: 200,
  });
});

test("reconciliation clears a hidden draft when Gateway accepted a different onboarding with its ID", async () => {
  const input = await draft("onboarding/conflicting-id");
  h.setGatewayInterceptor(async (url, init, forward) => {
    if (init?.method === "POST" && new URL(url).pathname === "/internal/v1/models/onboarding") {
      return new Response("Temporarily unavailable", { status: 503 });
    }
    return forward();
  });
  try {
    expect((await h.adminApi("POST", onboarding, adminAccess, input)).status).toBe(503);
  } finally {
    h.setGatewayInterceptor(null);
  }

  const other = await h.gatewayInternalApi("POST", "/internal/v1/models/onboarding", {
    ...input,
    model: { ...input.model, max_output_tokens: 200 },
  });
  expect(other.status).toBe(200);
  await h.consoleRuntime.sync.routeReleases.reconcilePending();

  const result = await h.adminApi("GET", statusPath(input), adminAccess);
  expect(result.status).toBe(409);
  expect(result.data.error).toMatchObject({ code: "model_onboarding_conflict" });
  expect(
    await h.conDb`SELECT name FROM public.model_catalog WHERE name = ${input.model.name}`,
  ).toEqual([]);
  expect(
    await h.conDb`SELECT id FROM public.routes WHERE model_name = ${input.model.name}`,
  ).toEqual([]);
  expect(h.gateway.snapshot.get().models.get(input.model.name)?.max_output_tokens).toBe(200);
});

test("a model disabled during the immutable price read stays hidden at first projection", async () => {
  const input = await draft("onboarding/during-price-read");
  h.setGatewayInterceptor(async (url, init, forward) => {
    const response = await forward();
    if (init?.method === "POST" && new URL(url).pathname === "/internal/v1/models/onboarding") {
      expect(response.status).toBe(200);
      await response.text();
      throw new Error("Reply dropped before projection");
    }
    return response;
  });
  try {
    expect((await h.adminApi("POST", onboarding, adminAccess, input)).status).toBe(502);
  } finally {
    h.setGatewayInterceptor(null);
  }

  const gatewayStatus = statusPath(input).replace("/admin/v1/", "/internal/v1/");
  const deadline = performance.now() + 5000;
  let active = false;
  do {
    const status = await h.gatewayInternalApi<ModelOnboardingResult>("GET", gatewayStatus);
    expect(status.status).toBe(200);
    active =
      status.data.current_configuration === "ACTIVE" &&
      status.data.nodes.every((node) => node.status !== "PENDING");
    if (!active) await Bun.sleep(20);
  } while (!active && performance.now() < deadline);
  expect(active).toBe(true);

  const priceRequested = Promise.withResolvers<void>();
  const continuePrice = Promise.withResolvers<void>();
  h.setGatewayInterceptor(async (url, init, forward) => {
    if (
      init?.method === "GET" &&
      new URL(url).pathname === "/internal/v1/prices/" + customerPriceId
    ) {
      priceRequested.resolve();
      await continuePrice.promise;
    }
    return forward();
  });
  const projection = h.adminApi<ModelOnboardingResult>("GET", statusPath(input), adminAccess);
  try {
    await Promise.race([
      priceRequested.promise,
      projection.then(() => {
        throw new Error("Projection did not read the customer price");
      }),
    ]);
    const disabled = await h.gatewayInternalApi("PUT", "/internal/v1/models", {
      name: input.model.name,
      operations: input.model.operations,
      max_output_tokens: input.model.max_output_tokens,
      enabled: false,
      expected_source_revision: 1,
      source_revision: 2,
    });
    expect(disabled.status).toBe(200);
    continuePrice.resolve();

    const result = await projection;
    expect(result.status).toBe(202);
    expect(result.data.current_configuration).toBe("CHANGED");
    const market = "/api/v1/models/" + encodeURIComponent(input.model.name);
    expect((await h.clientApi("GET", market, customerAccess)).status).toBe(404);
  } finally {
    continuePrice.resolve();
    h.setGatewayInterceptor(null);
    await projection.catch(() => undefined);
  }
});
