import {
  GatewayError,
  modelOnboardingPreviewSchema,
  modelOnboardingSchema,
  type ModelOnboardingInput,
} from "@gateway/contracts";
import { canonical, type Usage } from "@gateway/kernel";
import {
  createModelOnboardingRepository,
  type ConfigSnapshot,
  type RouteRecord,
} from "@gateway/ledger";
import { parseUuid } from "./internal-request.ts";
import type { InternalApiDeps, InternalJsonRoute, InternalRoute } from "./internal-route-types.ts";
import { simulatePricing } from "./pricing-simulate.ts";
import { validateRouteAdapter } from "./upstream.ts";

function draftRoutes(input: ModelOnboardingInput): RouteRecord[] {
  return input.deployments.map((deployment) => ({
    ...deployment,
    model_name: input.model.name,
    operations: input.model.operations,
    billing_price_id: input.billing_price_id,
    request_policy: {},
    adapter_overrides: {},
    plugin_pipeline: [],
    retry_policy: {},
    enabled: true,
  }));
}

type OnboardingRepository = ReturnType<typeof createModelOnboardingRepository>;
type OnboardingRelease = NonNullable<Awaited<ReturnType<OnboardingRepository["get"]>>>;

function matchesRelease(
  snapshot: ConfigSnapshot,
  name: string,
  available: (snapshot: ConfigSnapshot, item: RouteRecord) => boolean,
  plan: OnboardingRelease["changes"][number],
) {
  const model = snapshot.models.get(name);
  const routes = snapshot.routesByModel.get(name) ?? [];
  if (
    !model?.enabled ||
    (model.adaptive_routing !== undefined && model.adaptive_routing !== null) ||
    model.routing_strategy !== plan.model.routing_strategy ||
    model.max_output_tokens !== plan.model.max_output_tokens ||
    canonical(model.operations) !== canonical(plan.model.operations) ||
    routes.length !== plan.deployments.length
  )
    return false;
  const byId = new Map(routes.map((route) => [route.id, route]));
  return plan.deployments.every((deployment) => {
    const item = byId.get(deployment.id);
    return (
      item &&
      item.billing_price_id === plan.customer_price_id &&
      item.channel_id === deployment.channel_id &&
      item.upstream_model === deployment.upstream_model &&
      item.cost_version_id === deployment.cost_version_id &&
      item.priority === deployment.priority &&
      item.weight === deployment.weight &&
      canonical(item.operations) === canonical(plan.model.operations) &&
      !Object.keys(item.request_policy).length &&
      !Object.keys(item.adapter_overrides).length &&
      !Object.keys(item.retry_policy).length &&
      !item.plugin_pipeline.length &&
      available(snapshot, item)
    );
  });
}

export function registerInternalModelOnboarding(
  deps: InternalApiDeps,
  route: InternalRoute,
  jsonRoute: InternalJsonRoute,
  onConfigUpdated: (epoch: number) => Promise<void>,
) {
  const path = "/internal/v1/models/onboarding";
  const repository = (name: string) => createModelOnboardingRepository(deps.db, name);

  function adapterAvailable(snapshot: ConfigSnapshot, item: RouteRecord): boolean {
    const channel = snapshot.channels.get(item.channel_id);
    if (!channel?.enabled || snapshot.accounts.get(channel.account_id)?.state !== "ACTIVE")
      return false;
    try {
      validateRouteAdapter(snapshot, channel, item, {
        secret: deps.snapshot.decryptChannelSecret(channel),
        wasmExecutor: deps.wasmExecutor,
        signingPublicKeyPem: deps.config.pluginSigningPublicKeyPem,
      });
      return true;
    } catch {
      return false;
    }
  }

  async function report(name: string, repo: OnboardingRepository, release: OnboardingRelease) {
    const {
      changes: [plan],
      rollback_of: _,
      ...record
    } = release;
    const nodes = await repo.confirmations(release);
    const { epoch, lineage, ...revisions } = await repo.observedRevisions();
    const snapshot = deps.snapshot.isFresh() ? deps.snapshot.get() : null;
    const authoritative =
      snapshot?.epoch === epoch &&
      snapshot.lineageToken === lineage &&
      epoch >= release.config_epoch;
    const current = !authoritative
      ? "PENDING"
      : matchesRelease(snapshot, name, adapterAvailable, plan!)
        ? "ACTIVE"
        : "CHANGED";
    return {
      ...record,
      model_name: name,
      local_applied_epoch: snapshot?.epoch ?? 0,
      nodes,
      customer_price_id: plan!.customer_price_id,
      observed_revisions: authoritative ? revisions : null,
      current_configuration: current,
    };
  }

  function validateAdapters(
    input: ModelOnboardingInput,
    snapshot: ConfigSnapshot,
    lineage: string,
  ) {
    if (snapshot.epoch !== input.expected_epoch || snapshot.lineageToken !== lineage) {
      throw new GatewayError(409, "model_onboarding_conflict", "Configuration epoch changed");
    }
    const routes = draftRoutes(input);
    for (const item of routes) {
      if (!adapterAvailable(snapshot, item)) {
        throw new GatewayError(
          400,
          "invalid_model_onboarding",
          `Provider adapter for deployment ${item.id} cannot serve all model operations`,
        );
      }
    }
    return routes;
  }

  async function snapshotAt(epoch: number) {
    await deps.snapshot.refreshTo(epoch);
    if (
      deps.snapshot.isDiverged() ||
      !deps.snapshot.isLoaded() ||
      deps.snapshot.get().epoch < epoch
    ) {
      throw new GatewayError(503, "snapshot_unavailable", "Configuration snapshot is not ready");
    }
    return deps.snapshot.get();
  }

  async function validate(input: ModelOnboardingInput) {
    const lineage = await repository(input.model.name).validate(input);
    const snapshot = await snapshotAt(input.expected_epoch);
    return { snapshot, routes: validateAdapters(input, snapshot, lineage) };
  }

  route("GET", path, "catalog", async (c) =>
    c.json({ config_epoch: await deps.catalog.getEpoch() }),
  );

  jsonRoute("POST", path + "/validate", "catalog", modelOnboardingSchema, async (c, input) => {
    await validate(input);
    return c.json({
      config_epoch: input.expected_epoch,
      customer_price_id: input.billing_price_id,
    });
  });

  jsonRoute("POST", path + "/preview", "auditor", modelOnboardingPreviewSchema, async (c, body) => {
    const quote = body.quote;
    const { snapshot, routes } = await validate(body.release);
    if (quote.billing_account_id) {
      const account = snapshot.billingAccounts.get(quote.billing_account_id);
      if (!account?.enabled || account.currency !== quote.currency) {
        throw new GatewayError(
          400,
          "invalid_model_onboarding",
          "Billing account or currency does not match the quote",
        );
      }
    }
    const model = body.release.model.name;
    const pricing = simulatePricing(
      snapshot,
      { ...quote, model, usage: quote.usage as Usage },
      routes,
    );
    return c.json({ config_epoch: body.release.expected_epoch, ...pricing });
  });

  jsonRoute("POST", path, "catalog", modelOnboardingSchema, async (c, input) => {
    const name = input.model.name;
    const repo = repository(name);
    const currentEpoch = await deps.catalog.getEpoch();
    const snapshot = currentEpoch === input.expected_epoch ? await snapshotAt(currentEpoch) : null;
    const release = await repo.publish(input, c.get("principal").id, (lineage) => {
      if (!snapshot)
        throw new GatewayError(409, "model_onboarding_conflict", "Configuration epoch changed");
      validateAdapters(input, snapshot, lineage);
    });
    await onConfigUpdated(release.config_epoch);
    return c.json(await report(name, repo, release));
  });

  route("GET", "/internal/v1/models/:model/onboarding/:id", "catalog", async (c) => {
    const name = c.req.param("model")!;
    const repo = repository(name);
    const release = await repo.get(parseUuid(c.req.param("id"), "id"));
    if (!release) throw new GatewayError(404, "not_found", "Model onboarding release not found");
    return c.json(await report(name, repo, release));
  });
}
