import { and, asc, desc, eq, sql } from "drizzle-orm";
import {
  canonical,
  dimensions,
  DomainError,
  validateFxRateVersion,
  validatePriceModifier,
  validatePricing,
  validateRateCard,
  type ModifierKind,
  type ModifierLayer,
  type ModifierMatch,
  type PriceModifier,
  type Pricing,
  type PricingSpec,
  type RateCard,
  type StackingPolicy,
  type TimeWindow,
} from "@gateway/kernel";
import {
  configEpoch,
  fxRateVersions,
  priceVersions,
  pricingModifiers,
  type LedgerDrizzle,
} from "./schema.ts";
import type {
  FxRateVersionRecord,
  PriceBasis,
  PriceSide,
  PriceVersionRecord,
  PricingModifierRecord,
} from "./types.ts";
import { bumpEpoch, catalogDigest } from "./catalog-epoch.ts";

export function hasPricingSpec(spec: PricingSpec | null | undefined): spec is PricingSpec {
  if (!spec) return false;
  return (
    spec.quantizers !== undefined ||
    spec.cliff !== undefined ||
    spec.progressive !== undefined ||
    spec.matrix !== undefined ||
    spec.reasoning_rate !== undefined ||
    spec.min_charge_atoms !== undefined ||
    spec.max_charge_atoms !== undefined
  );
}

export function rateCardFromPriceVersion(price: PriceVersionRecord): RateCard {
  const spec = price.pricing_spec;
  const card: RateCard = {
    id: price.id,
    side: price.side,
    currency: price.currency,
    basis: price.basis,
    rates: price.rates,
    ceilings: price.ceilings,
  };
  if (hasPricingSpec(spec)) {
    if (spec.quantizers !== undefined) card.quantizers = spec.quantizers;
    if (spec.cliff !== undefined) card.cliff = spec.cliff;
    if (spec.progressive !== undefined) card.progressive = spec.progressive;
    if (spec.matrix !== undefined) card.matrix = spec.matrix;
    if (spec.reasoning_rate !== undefined) card.reasoning_rate = spec.reasoning_rate;
    if (spec.min_charge_atoms !== undefined) card.min_charge_atoms = spec.min_charge_atoms;
    if (spec.max_charge_atoms !== undefined) card.max_charge_atoms = spec.max_charge_atoms;
  } else {
    card.rates = {};
    card.ceilings = {};
    for (const dim of dimensions) {
      if (
        price.basis !== "PROVIDER_USAGE" &&
        dim !== "requests" &&
        !(price.basis === "INPUT_CHARACTERS" && dim === "characters")
      )
        continue;
      const rate = price.rates[dim];
      if (rate !== undefined) card.rates[dim] = rate;
      const ceiling = price.ceilings[dim];
      if (ceiling !== undefined) card.ceilings[dim] = ceiling;
    }
  }
  return card;
}

function toPriceVersionRecord(row: typeof priceVersions.$inferSelect): PriceVersionRecord {
  return {
    id: row.id,
    side: row.side,
    currency: row.currency,
    basis: row.basis,
    rates: row.rates,
    ceilings: row.ceilings,
    ...(row.pricingSpec ? { pricing_spec: row.pricingSpec } : {}),
    source_digest: row.sourceDigest,
  };
}

function toPricingModifierRecord(row: typeof pricingModifiers.$inferSelect): PricingModifierRecord {
  return {
    id: row.id,
    layer: row.layer,
    kind: row.kind,
    stacking: row.stacking,
    priority: row.priority,
    enabled: row.enabled,
    effective_from: row.effectiveFrom ? row.effectiveFrom.toISOString() : null,
    effective_to: row.effectiveTo ? row.effectiveTo.toISOString() : null,
    time_windows: row.timeWindows ?? null,
    match: row.match,
    multiplier_numerator: row.multiplierNumerator,
    multiplier_denominator: row.multiplierDenominator,
    rate_card_price_version_id: row.rateCardPriceVersionId,
  };
}

function toFxRateVersionRecord(row: typeof fxRateVersions.$inferSelect): FxRateVersionRecord {
  return {
    id: row.id,
    base_currency: row.baseCurrency,
    quote_currency: row.quoteCurrency,
    rate_numerator: row.rateNumerator,
    rate_denominator: row.rateDenominator,
    effective_from: row.effectiveFrom.toISOString(),
    effective_to: row.effectiveTo ? row.effectiveTo.toISOString() : null,
    source_digest: row.sourceDigest,
  };
}

function validateCardRulePresence(rates: Pricing["rates"], spec: PricingSpec | null): void {
  const hasRates = Object.keys(rates).length > 0;
  const hasCliff = spec?.cliff !== undefined;
  const hasProgressive =
    spec?.progressive !== undefined && Object.keys(spec.progressive).length > 0;
  const hasMatrix = spec?.matrix !== undefined && spec.matrix.length > 0;
  const hasReasoning = spec?.reasoning_rate !== undefined;
  if (!hasRates && !hasCliff && !hasProgressive && !hasMatrix && !hasReasoning) {
    throw new TypeError("Rate card must define at least one pricing rule");
  }
}

export function createCatalogPricingOperations({ orm }: { orm: LedgerDrizzle }) {
  return {
    async getPriceVersion(id: string): Promise<PriceVersionRecord | null> {
      const [r] = await orm.select().from(priceVersions).where(eq(priceVersions.id, id)).limit(1);
      return r ? toPriceVersionRecord(r) : null;
    },

    async createPriceVersion(input: {
      id?: string;
      side: PriceSide;
      currency: string;
      basis?: PriceBasis;
      rates: Pricing["rates"];
      ceilings?: Record<string, string>;
      pricing_spec?: PricingSpec | null;
    }): Promise<PriceVersionRecord & { config_epoch: number; replayed?: boolean }> {
      const id = input.id ?? crypto.randomUUID();
      const basis = input.basis ?? "PROVIDER_USAGE";
      const ceilings = input.ceilings ?? {};
      const pricingSpec = hasPricingSpec(input.pricing_spec) ? input.pricing_spec : null;
      const card = rateCardFromPriceVersion({
        id,
        side: input.side,
        currency: input.currency,
        basis,
        rates: input.rates,
        ceilings,
        ...(pricingSpec ? { pricing_spec: pricingSpec } : {}),
        source_digest: "",
      });
      try {
        if (pricingSpec === null) {
          validatePricing({ currency: input.currency, rates: input.rates });
        } else {
          validateCardRulePresence(input.rates, pricingSpec);
        }
        validateRateCard(card);
      } catch (err) {
        throw new DomainError(
          "invalid_price",
          err instanceof Error ? err.message : "Invalid price version",
        );
      }
      const sourceDigest = catalogDigest(
        canonical(
          pricingSpec === null
            ? {
                side: input.side,
                currency: input.currency,
                basis,
                rates: input.rates,
                ceilings,
              }
            : {
                side: input.side,
                currency: input.currency,
                basis,
                rates: input.rates,
                ceilings,
                pricing_spec: pricingSpec,
              },
        ),
      );
      return orm.transaction(async (tx) => {
        const rows = await tx
          .insert(priceVersions)
          .values({
            id,
            side: input.side,
            currency: input.currency,
            basis,
            rates: input.rates,
            ceilings,
            pricingSpec,
            sourceDigest,
          })
          .onConflictDoNothing({ target: priceVersions.id })
          .returning();
        if (rows[0]) {
          const epoch = await bumpEpoch(tx);
          return {
            ...toPriceVersionRecord(rows[0]),
            config_epoch: epoch,
            replayed: false,
          };
        }
        const [existing] = await tx
          .select()
          .from(priceVersions)
          .where(eq(priceVersions.id, id))
          .limit(1);
        if (!existing || existing.sourceDigest !== sourceDigest) {
          throw new DomainError(
            "price_version_conflict",
            "Price version identifier already exists with different rates",
          );
        }
        const [epochRow] = await tx
          .select({ version: configEpoch.version })
          .from(configEpoch)
          .where(eq(configEpoch.id, 1));
        return {
          ...toPriceVersionRecord(existing),
          config_epoch: epochRow?.version ?? 0,
          replayed: true,
        };
      });
    },

    async upsertPricingModifier(input: {
      id?: string;
      layer: ModifierLayer;
      kind: ModifierKind;
      stacking: StackingPolicy;
      priority?: number;
      enabled?: boolean;
      effective_from?: string | null;
      effective_to?: string | null;
      time_windows?: TimeWindow[] | null;
      match?: ModifierMatch;
      multiplier_numerator?: string | null;
      multiplier_denominator?: string | null;
      rate_card_price_version_id?: string | null;
      source_revision?: number;
    }): Promise<PricingModifierRecord & { config_epoch: number; source_revision: number }> {
      const id = input.id ?? crypto.randomUUID();
      const priority = input.priority ?? 0;
      const enabled = input.enabled ?? true;
      const match = input.match ?? {};
      const timeWindows =
        input.time_windows && input.time_windows.length > 0 ? input.time_windows : null;
      const effectiveFromStr = input.effective_from ?? null;
      const effectiveToStr = input.effective_to ?? null;
      const multNum = input.kind === "MULTIPLIER" ? (input.multiplier_numerator ?? null) : null;
      const multDen = input.kind === "MULTIPLIER" ? (input.multiplier_denominator ?? null) : null;
      const rateCardPriceVersionId =
        input.kind === "EXACT_RATE_CARD" ? (input.rate_card_price_version_id ?? null) : null;
      const sourceRev = input.source_revision;

      return orm.transaction(async (tx) => {
        let hydratedRateCard: RateCard | undefined;
        if (input.kind === "EXACT_RATE_CARD") {
          if (!rateCardPriceVersionId) {
            throw new DomainError(
              "invalid_pricing_modifier",
              "EXACT_RATE_CARD modifier requires rate_card_price_version_id",
            );
          }
          const [priceRow] = await tx
            .select()
            .from(priceVersions)
            .where(eq(priceVersions.id, rateCardPriceVersionId))
            .limit(1);
          if (!priceRow) {
            throw new DomainError(
              "invalid_pricing_modifier",
              "Referenced rate_card_price_version_id does not exist",
            );
          }
          hydratedRateCard = rateCardFromPriceVersion(toPriceVersionRecord(priceRow));
        }

        const candidateModifier: PriceModifier = {
          id,
          layer: input.layer,
          kind: input.kind,
          stacking: input.stacking,
          priority,
          enabled,
          match,
          ...(effectiveFromStr !== null ? { effective_from: effectiveFromStr } : {}),
          ...(effectiveToStr !== null ? { effective_to: effectiveToStr } : {}),
          ...(timeWindows !== null ? { time_windows: timeWindows } : {}),
          ...(input.kind === "MULTIPLIER" && multNum !== null && multDen !== null
            ? { multiplier: { numerator: multNum, denominator: multDen } }
            : {}),
          ...(hydratedRateCard !== undefined ? { rate_card: hydratedRateCard } : {}),
        };
        try {
          validatePriceModifier(candidateModifier);
        } catch (err) {
          throw new DomainError(
            "invalid_pricing_modifier",
            err instanceof Error ? err.message : "Invalid pricing modifier",
          );
        }

        const effectiveFrom = effectiveFromStr !== null ? new Date(effectiveFromStr) : null;
        const effectiveTo = effectiveToStr !== null ? new Date(effectiveToStr) : null;

        const rows = await tx
          .insert(pricingModifiers)
          .values({
            id,
            layer: input.layer,
            kind: input.kind,
            stacking: input.stacking,
            priority,
            enabled,
            effectiveFrom,
            effectiveTo,
            timeWindows,
            match,
            multiplierNumerator: multNum,
            multiplierDenominator: multDen,
            rateCardPriceVersionId,
            ...(sourceRev !== undefined ? { sourceRevision: sourceRev } : {}),
          })
          .onConflictDoUpdate({
            target: pricingModifiers.id,
            ...(sourceRev !== undefined
              ? { setWhere: sql`${pricingModifiers.sourceRevision} <= ${sourceRev}` }
              : {}),
            set: {
              layer: input.layer,
              kind: input.kind,
              stacking: input.stacking,
              priority,
              enabled,
              effectiveFrom,
              effectiveTo,
              timeWindows,
              match,
              multiplierNumerator: multNum,
              multiplierDenominator: multDen,
              rateCardPriceVersionId,
              ...(sourceRev !== undefined ? { sourceRevision: sourceRev } : {}),
              updatedAt: sql`now()`,
            },
          })
          .returning();
        if (!rows[0]) {
          throw new DomainError(
            "stale_revision",
            "Incoming pricing modifier revision is older than current Gateway revision",
          );
        }
        const epoch = await bumpEpoch(tx);
        return {
          ...toPricingModifierRecord(rows[0]),
          source_revision: rows[0].sourceRevision,
          config_epoch: epoch,
        };
      });
    },

    async listPricingModifiers(filter?: {
      layer?: ModifierLayer;
      enabled?: boolean;
    }): Promise<PricingModifierRecord[]> {
      const conds = [];
      if (filter?.layer !== undefined) conds.push(eq(pricingModifiers.layer, filter.layer));
      if (filter?.enabled !== undefined) conds.push(eq(pricingModifiers.enabled, filter.enabled));
      const base = orm.select().from(pricingModifiers);
      const rows = await (conds.length > 0 ? base.where(and(...conds)) : base).orderBy(
        asc(pricingModifiers.layer),
        asc(pricingModifiers.priority),
        asc(pricingModifiers.id),
      );
      return rows.map(toPricingModifierRecord);
    },

    async createFxRateVersion(input: {
      id?: string;
      base_currency: string;
      quote_currency: string;
      rate_numerator: string;
      rate_denominator: string;
      effective_from: string;
      effective_to?: string | null;
    }): Promise<FxRateVersionRecord & { config_epoch: number; replayed?: boolean }> {
      const id = input.id ?? crypto.randomUUID();
      const effectiveToStr = input.effective_to ?? null;
      try {
        validateFxRateVersion({
          id,
          base_currency: input.base_currency,
          quote_currency: input.quote_currency,
          rate: { numerator: input.rate_numerator, denominator: input.rate_denominator },
          effective_from: input.effective_from,
          ...(effectiveToStr !== null ? { effective_to: effectiveToStr } : {}),
        });
      } catch (err) {
        throw new DomainError(
          "invalid_fx_rate",
          err instanceof Error ? err.message : "Invalid FX rate version",
        );
      }
      const effectiveFrom = new Date(input.effective_from);
      const effectiveTo = effectiveToStr !== null ? new Date(effectiveToStr) : null;
      const sourceDigest = catalogDigest(
        canonical({
          base_currency: input.base_currency,
          quote_currency: input.quote_currency,
          rate_numerator: input.rate_numerator,
          rate_denominator: input.rate_denominator,
          effective_from: effectiveFrom.toISOString(),
          effective_to: effectiveTo ? effectiveTo.toISOString() : null,
        }),
      );
      return orm.transaction(async (tx) => {
        const rows = await tx
          .insert(fxRateVersions)
          .values({
            id,
            baseCurrency: input.base_currency,
            quoteCurrency: input.quote_currency,
            rateNumerator: input.rate_numerator,
            rateDenominator: input.rate_denominator,
            effectiveFrom,
            effectiveTo,
            sourceDigest,
          })
          .onConflictDoNothing()
          .returning();
        if (rows[0]) {
          const epoch = await bumpEpoch(tx);
          return {
            ...toFxRateVersionRecord(rows[0]),
            config_epoch: epoch,
            replayed: false,
          };
        }
        const [existing] = await tx
          .select()
          .from(fxRateVersions)
          .where(eq(fxRateVersions.id, id))
          .limit(1);
        if (!existing || existing.sourceDigest !== sourceDigest) {
          throw new DomainError(
            "fx_rate_version_conflict",
            "FX rate version conflicts with an existing record for this identifier or effective window",
          );
        }
        const [epochRow] = await tx
          .select({ version: configEpoch.version })
          .from(configEpoch)
          .where(eq(configEpoch.id, 1));
        return {
          ...toFxRateVersionRecord(existing),
          config_epoch: epochRow?.version ?? 0,
          replayed: true,
        };
      });
    },

    async listFxRateVersions(filter?: {
      base_currency?: string;
      quote_currency?: string;
    }): Promise<FxRateVersionRecord[]> {
      const conds = [];
      if (filter?.base_currency !== undefined) {
        conds.push(eq(fxRateVersions.baseCurrency, filter.base_currency));
      }
      if (filter?.quote_currency !== undefined) {
        conds.push(eq(fxRateVersions.quoteCurrency, filter.quote_currency));
      }
      const base = orm.select().from(fxRateVersions);
      const rows = await (conds.length > 0 ? base.where(and(...conds)) : base).orderBy(
        desc(fxRateVersions.effectiveFrom),
        desc(fxRateVersions.id),
      );
      return rows.map(toFxRateVersionRecord);
    },
  };
}
