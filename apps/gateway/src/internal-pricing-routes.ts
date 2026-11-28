import {
  assertKnownQueryParameters,
  GatewayError,
  simulatePricingSchema,
} from "@gateway/contracts";
import type { ModifierLayer, ModifierMatch, Operation, TimeWindow, Usage } from "@gateway/kernel";
import { simulatePricing } from "./pricing-simulate.ts";
import { createFxRateSchema, upsertPricingModifierSchema } from "./internal-pricing-schemas.ts";
import { currencySchema } from "./internal-request.ts";
import type { InternalApiDeps, InternalJsonRoute, InternalRoute } from "./internal-route-types.ts";

export function registerInternalPricingRoutes({
  jsonRoute,
  route,
  deps,
  onConfigUpdated,
}: {
  jsonRoute: InternalJsonRoute;
  route: InternalRoute;
  deps: InternalApiDeps;
  onConfigUpdated: (epoch: number) => Promise<void>;
}) {
  jsonRoute(
    "PUT",
    "/internal/v1/pricing-modifiers",
    "finance",
    upsertPricingModifierSchema,
    async (c, body) => {
      const rec = await deps.catalog.upsertPricingModifier({
        ...(body.id ? { id: body.id } : {}),
        layer: body.layer,
        kind: body.kind,
        stacking: body.stacking,
        priority: body.priority,
        enabled: body.enabled,
        effective_from: body.effective_from ?? null,
        effective_to: body.effective_to ?? null,
        time_windows: (body.time_windows as TimeWindow[] | null | undefined) ?? null,
        match: body.match as ModifierMatch,
        multiplier_numerator: body.multiplier_numerator ?? null,
        multiplier_denominator: body.multiplier_denominator ?? null,
        rate_card_price_version_id: body.rate_card_price_version_id ?? null,
        ...(body.source_revision !== undefined ? { source_revision: body.source_revision } : {}),
      });
      await onConfigUpdated(rec.config_epoch);
      return c.json(rec);
    },
  );

  route("GET", "/internal/v1/pricing-modifiers", "auditor", async (c) => {
    assertKnownQueryParameters(c.req.query(), ["layer", "enabled"]);
    const rawLayer = c.req.query("layer");
    const rawEnabled = c.req.query("enabled");
    let layer: ModifierLayer | undefined;
    if (rawLayer !== undefined) {
      if (rawLayer !== "PRODUCT" && rawLayer !== "COMMERCIAL") {
        throw new GatewayError(400, "validation_error", "layer must be PRODUCT or COMMERCIAL");
      }
      layer = rawLayer;
    }
    let enabled: boolean | undefined;
    if (rawEnabled !== undefined) {
      if (rawEnabled !== "true" && rawEnabled !== "false") {
        throw new GatewayError(400, "validation_error", "enabled must be true or false");
      }
      enabled = rawEnabled === "true";
    }
    const items = await deps.catalog.listPricingModifiers({
      ...(layer !== undefined ? { layer } : {}),
      ...(enabled !== undefined ? { enabled } : {}),
    });
    return c.json({ items });
  });

  jsonRoute("POST", "/internal/v1/fx-rates", "finance", createFxRateSchema, async (c, body) => {
    const rec = await deps.catalog.createFxRateVersion({
      ...(body.id ? { id: body.id } : {}),
      base_currency: body.base_currency,
      quote_currency: body.quote_currency,
      rate_numerator: body.rate_numerator,
      rate_denominator: body.rate_denominator,
      effective_from: body.effective_from,
      effective_to: body.effective_to ?? null,
    });
    if (!rec.replayed) await onConfigUpdated(rec.config_epoch);
    return c.json(rec, rec.replayed ? 200 : 201);
  });

  route("GET", "/internal/v1/fx-rates", "auditor", async (c) => {
    assertKnownQueryParameters(c.req.query(), ["base_currency", "quote_currency"]);
    const rawBase = c.req.query("base_currency");
    const rawQuote = c.req.query("quote_currency");
    if (rawBase !== undefined && !currencySchema.safeParse(rawBase).success) {
      throw new GatewayError(400, "validation_error", "Invalid base_currency");
    }
    if (rawQuote !== undefined && !currencySchema.safeParse(rawQuote).success) {
      throw new GatewayError(400, "validation_error", "Invalid quote_currency");
    }
    const items = await deps.catalog.listFxRateVersions({
      ...(rawBase ? { base_currency: rawBase } : {}),
      ...(rawQuote ? { quote_currency: rawQuote } : {}),
    });
    return c.json({ items });
  });

  jsonRoute(
    "POST",
    "/internal/v1/pricing/simulate",
    "auditor",
    simulatePricingSchema,
    async (c, body) => {
      const snap = deps.snapshot.isLoaded()
        ? deps.snapshot.get()
        : await deps.catalog.loadSnapshot();
      const result = simulatePricing(snap, {
        model: body.model,
        operation: body.operation as Operation,
        currency: body.currency,
        route_id: body.route_id,
        billing_account_id: body.billing_account_id,
        customer_group_id: body.customer_group_id,
        channel_id: body.channel_id,
        upstream_account_id: body.upstream_account_id,
        rated_at: body.rated_at,
        execution_mode: body.execution_mode,
        service_tier: body.service_tier,
        region: body.region,
        attrs: body.attrs,
        usage: body.usage as Usage,
        input_characters: body.input_characters,
        output_bound: body.output_bound,
        estimated_input_tokens: body.estimated_input_tokens,
        images: body.images,
        margin_policy: body.margin_policy,
      });
      return c.json(result);
    },
  );
}
