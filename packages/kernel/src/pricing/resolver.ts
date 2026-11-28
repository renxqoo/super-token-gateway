import { DomainError } from "../errors.ts";
import { compareRational, formatRational, multiplyRational, UNIT_RATIONAL } from "./rational.ts";
import { matchesCompiledTimeWindow, matchesEffectiveRange, parseIsoTimestampMs } from "./time.ts";
import type { CompiledFxRate, CompiledModifier, CompiledRatingPlan } from "./compiler.ts";
import type {
  AppliedModifier,
  ExactRational,
  MatrixEntry,
  PricingContext,
  PricingSnapshot,
  RateCard,
} from "./types.ts";

function cardSpecificity(card: RateCard, model: string, operation: string): number {
  const exactModel = card.model === model;
  const wildModel = !card.model || card.model === "*";
  if (!exactModel && !wildModel) return 99;
  const exactOp = card.operation === operation;
  const wildOp = !card.operation || card.operation === "*";
  if (!exactOp && !wildOp) return 99;
  if (exactModel && exactOp) return 1;
  if (exactModel && wildOp) return 2;
  if (wildModel && exactOp) return 3;
  return 4;
}

function findActiveFx(
  plan: CompiledRatingPlan,
  baseCurrency: string,
  quoteCurrency: string,
  ratedAtMs: number,
): CompiledFxRate | null {
  const candidates = plan.fxByQuote.get(quoteCurrency);
  if (!candidates) return null;
  let best: CompiledFxRate | null = null;
  let tie = false;
  for (const fx of candidates) {
    if (fx.raw.base_currency !== baseCurrency) continue;
    if (!matchesEffectiveRange(ratedAtMs, fx.fromMs, fx.toMs)) continue;
    if (!best) {
      best = fx;
      continue;
    }
    const bestFrom = best.fromMs ?? -Infinity;
    const curFrom = fx.fromMs ?? -Infinity;
    if (curFrom > bestFrom) {
      best = fx;
      tie = false;
    } else if (curFrom === bestFrom) {
      tie = true;
    }
  }
  if (tie) {
    throw new DomainError(
      "ambiguous_pricing",
      `Multiple active FX rates match ${baseCurrency}->${quoteCurrency}`,
    );
  }
  return best;
}

function selectBaseCard(
  plan: CompiledRatingPlan,
  context: PricingContext,
  ratedAtMs: number,
): { card: RateCard; fx: CompiledFxRate | null } {
  const exactModelCards = plan.cardsBySideModel[context.side].get(context.model) ?? [];
  const wildcardCards = plan.wildcardCardsBySide[context.side];
  let bestDirectPrio = 99;
  let bestDirect: RateCard | null = null;
  let directTie = false;

  let bestFxPrio = 99;
  let bestFxCard: RateCard | null = null;
  let bestFx: CompiledFxRate | null = null;
  let fxTie = false;

  const inspectDirect = (list: readonly RateCard[]) => {
    for (let i = 0; i < list.length; i++) {
      const card = list[i]!;
      if (card.currency !== context.currency) continue;
      const prio = cardSpecificity(card, context.model, context.operation);
      if (prio === 99) continue;
      if (prio < bestDirectPrio) {
        bestDirectPrio = prio;
        bestDirect = card;
        directTie = false;
      } else if (prio === bestDirectPrio && bestDirect && bestDirect.id !== card.id) {
        directTie = true;
      }
    }
  };

  inspectDirect(exactModelCards);
  inspectDirect(wildcardCards);

  if (bestDirect) {
    if (directTie) {
      throw new DomainError(
        "ambiguous_pricing",
        `Multiple rate cards match ${context.model}/${context.operation} in ${context.currency}`,
      );
    }
    return { card: bestDirect, fx: null };
  }

  const inspectFx = (list: readonly RateCard[]) => {
    for (let i = 0; i < list.length; i++) {
      const card = list[i]!;
      if (card.currency === context.currency) continue;
      const prio = cardSpecificity(card, context.model, context.operation);
      if (prio === 99) continue;
      const fx = findActiveFx(plan, card.currency, context.currency, ratedAtMs);
      if (!fx) continue;
      if (prio < bestFxPrio) {
        bestFxPrio = prio;
        bestFxCard = card;
        bestFx = fx;
        fxTie = false;
      } else if (prio === bestFxPrio && bestFxCard && bestFxCard.id !== card.id) {
        fxTie = true;
      }
    }
  };

  inspectFx(exactModelCards);
  inspectFx(wildcardCards);

  if (bestFxCard && bestFx) {
    if (fxTie) {
      throw new DomainError(
        "ambiguous_pricing",
        `Multiple FX-derived rate cards match ${context.model}/${context.operation} for ${context.currency}`,
      );
    }
    return { card: bestFxCard, fx: bestFx };
  }
  throw new DomainError(
    "tariff_unavailable",
    `No rate card available for ${context.model}/${context.operation} in ${context.currency}`,
  );
}

function applyMatrix(
  card: RateCard,
  attrs: Record<string, string | number | boolean> | undefined,
): { card: RateCard; matchedIndex: number | null } {
  const matrix = card.matrix;
  if (!matrix || matrix.length === 0) {
    return { card, matchedIndex: null };
  }
  let bestSpec = -1;
  let bestIndex = -1;
  let tie = false;
  for (let i = 0; i < matrix.length; i++) {
    const row: MatrixEntry = matrix[i]!;
    const keys = Object.keys(row.match);
    let matched = true;
    for (let j = 0; j < keys.length; j++) {
      const k = keys[j]!;
      if (!attrs || !Object.hasOwn(attrs, k) || attrs[k] !== row.match[k]) {
        matched = false;
        break;
      }
    }
    if (!matched) continue;
    if (keys.length > bestSpec) {
      bestSpec = keys.length;
      bestIndex = i;
      tie = false;
    } else if (keys.length === bestSpec) {
      tie = true;
    }
  }
  if (bestIndex < 0) {
    const hasBaseRates =
      Object.keys(card.rates).length > 0 ||
      card.reasoning_rate !== undefined ||
      card.cliff !== undefined ||
      (card.progressive !== undefined && Object.keys(card.progressive).length > 0);
    if (!hasBaseRates) {
      throw new DomainError(
        "tariff_unavailable",
        `No pricing matrix entry matched request attributes on ${card.id}`,
      );
    }
    const stripped: RateCard = { ...card };
    delete stripped.matrix;
    return { card: stripped, matchedIndex: null };
  }
  if (tie) {
    throw new DomainError(
      "ambiguous_pricing",
      `Multiple equally specific matrix entries matched on ${card.id}`,
    );
  }
  const winner = matrix[bestIndex]!;
  let nextProgressive = card.progressive;
  if (card.progressive) {
    const filtered: NonNullable<RateCard["progressive"]> = { ...card.progressive };
    for (const dim of Object.keys(winner.rates)) {
      delete filtered[dim as keyof typeof filtered];
    }
    nextProgressive = Object.keys(filtered).length > 0 ? filtered : undefined;
  }
  const merged: RateCard = {
    ...card,
    rates: { ...card.rates, ...winner.rates },
    ...(winner.quantizers ? { quantizers: { ...card.quantizers, ...winner.quantizers } } : {}),
    ...(winner.reasoning_rate ? { reasoning_rate: winner.reasoning_rate } : {}),
    ...(winner.min_charge_atoms !== undefined ? { min_charge_atoms: winner.min_charge_atoms } : {}),
    ...(winner.max_charge_atoms !== undefined ? { max_charge_atoms: winner.max_charge_atoms } : {}),
  };
  delete merged.matrix;
  if (nextProgressive) merged.progressive = nextProgressive;
  else delete merged.progressive;
  return { card: merged, matchedIndex: bestIndex };
}

function modifierMatches(
  mod: CompiledModifier,
  context: PricingContext,
  ratedAtMs: number,
): boolean {
  if (!matchesEffectiveRange(ratedAtMs, mod.fromMs, mod.toMs)) return false;
  if (mod.timeWindows.length > 0) {
    let inWindow = false;
    for (let i = 0; i < mod.timeWindows.length; i++) {
      if (matchesCompiledTimeWindow(ratedAtMs, mod.timeWindows[i]!)) {
        inWindow = true;
        break;
      }
    }
    if (!inWindow) return false;
  }
  const m = mod.raw.match;
  if (m.operation !== undefined && m.operation !== "*" && m.operation !== context.operation) {
    return false;
  }
  if (m.execution_mode !== undefined && m.execution_mode !== (context.execution_mode ?? "SYNC")) {
    return false;
  }
  if (m.service_tier !== undefined && m.service_tier !== (context.service_tier ?? "standard")) {
    return false;
  }
  if (m.region !== undefined && m.region !== context.region) return false;
  if (m.billing_account_id !== undefined && m.billing_account_id !== context.billing_account_id) {
    return false;
  }
  if (m.customer_group_id !== undefined && m.customer_group_id !== context.customer_group_id) {
    return false;
  }
  if (m.channel_id !== undefined && m.channel_id !== context.channel_id) return false;
  if (
    m.upstream_account_id !== undefined &&
    m.upstream_account_id !== context.upstream_account_id
  ) {
    return false;
  }
  if (mod.attrEntries.length > 0) {
    const attrs = context.attrs;
    if (!attrs) return false;
    for (let i = 0; i < mod.attrEntries.length; i++) {
      const [k, v] = mod.attrEntries[i]!;
      if (!Object.hasOwn(attrs, k) || attrs[k] !== v) return false;
    }
  }
  return true;
}

function applyLayerModifiers(
  mods: readonly CompiledModifier[],
  layer: "PRODUCT" | "COMMERCIAL",
): {
  exactCardMod: CompiledModifier | null;
  multiplier: ExactRational;
  applied: AppliedModifier[];
} {
  let bestExclusive: CompiledModifier | null = null;
  let exclusiveTie = false;
  for (let i = 0; i < mods.length; i++) {
    const mod = mods[i]!;
    if (mod.raw.stacking !== "EXCLUSIVE") continue;
    if (!bestExclusive) {
      bestExclusive = mod;
    } else if (mod.raw.priority < bestExclusive.raw.priority) {
      bestExclusive = mod;
      exclusiveTie = false;
    } else if (mod.raw.priority === bestExclusive.raw.priority) {
      exclusiveTie = true;
    }
  }
  if (bestExclusive) {
    if (exclusiveTie) {
      throw new DomainError(
        "ambiguous_pricing",
        `Multiple exclusive ${layer} modifiers share priority ${bestExclusive.raw.priority}`,
      );
    }
    if (bestExclusive.raw.kind === "EXACT_RATE_CARD") {
      return {
        exactCardMod: bestExclusive,
        multiplier: UNIT_RATIONAL,
        applied: [
          {
            id: bestExclusive.raw.id,
            layer,
            kind: "EXACT_RATE_CARD",
            stacking: "EXCLUSIVE",
            rate_card_id: bestExclusive.raw.rate_card!.id,
          },
        ],
      };
    }
    return {
      exactCardMod: null,
      multiplier: bestExclusive.multiplier!,
      applied: [
        {
          id: bestExclusive.raw.id,
          layer,
          kind: "MULTIPLIER",
          stacking: "EXCLUSIVE",
          multiplier: bestExclusive.raw.multiplier!,
        },
      ],
    };
  }

  let bestExactCard: CompiledModifier | null = null;
  let exactCardTie = false;
  let bestCustomerMult: CompiledModifier | null = null;
  let multFactor = UNIT_RATIONAL;
  const applied: AppliedModifier[] = [];

  for (let i = 0; i < mods.length; i++) {
    const mod = mods[i]!;
    if (mod.raw.kind === "EXACT_RATE_CARD") {
      if (!bestExactCard || mod.raw.priority < bestExactCard.raw.priority) {
        bestExactCard = mod;
        exactCardTie = false;
      } else if (mod.raw.priority === bestExactCard.raw.priority) {
        exactCardTie = true;
      }
      continue;
    }
    if (mod.raw.stacking === "BEST_FOR_CUSTOMER") {
      if (!bestCustomerMult || compareRational(mod.multiplier!, bestCustomerMult.multiplier!) < 0) {
        bestCustomerMult = mod;
      }
    } else {
      multFactor = multiplyRational(multFactor, mod.multiplier!);
      applied.push({
        id: mod.raw.id,
        layer,
        kind: "MULTIPLIER",
        stacking: "MULTIPLY",
        multiplier: mod.raw.multiplier!,
      });
    }
  }
  if (exactCardTie) {
    throw new DomainError(
      "ambiguous_pricing",
      `Multiple EXACT_RATE_CARD ${layer} modifiers share equal priority`,
    );
  }
  if (bestExactCard) {
    applied.unshift({
      id: bestExactCard.raw.id,
      layer,
      kind: "EXACT_RATE_CARD",
      stacking: bestExactCard.raw.stacking,
      rate_card_id: bestExactCard.raw.rate_card!.id,
    });
  }
  if (bestCustomerMult) {
    multFactor = multiplyRational(multFactor, bestCustomerMult.multiplier!);
    applied.push({
      id: bestCustomerMult.raw.id,
      layer,
      kind: "MULTIPLIER",
      stacking: "BEST_FOR_CUSTOMER",
      multiplier: bestCustomerMult.raw.multiplier!,
    });
  }
  return { exactCardMod: bestExactCard, multiplier: multFactor, applied };
}

function resolveFromBaseCard(
  plan: CompiledRatingPlan,
  baseCard: RateCard,
  initialFx: CompiledFxRate | null,
  context: PricingContext,
  ratedAtMs: number,
): PricingSnapshot {
  let activeCard = baseCard;
  let activeFx = initialFx;

  const modelMods = plan.modifiersBySideModel[context.side].get(context.model);
  const wildMods = plan.wildcardModifiersBySide[context.side];
  const productMods: CompiledModifier[] = [];
  const commercialMods: CompiledModifier[] = [];

  const collect = (list: readonly CompiledModifier[] | undefined) => {
    if (!list) return;
    for (let i = 0; i < list.length; i++) {
      const mod = list[i]!;
      if (!modifierMatches(mod, context, ratedAtMs)) continue;
      if (mod.raw.layer === "PRODUCT") productMods.push(mod);
      else commercialMods.push(mod);
    }
  };
  collect(modelMods);
  collect(wildMods);

  const productRes = applyLayerModifiers(productMods, "PRODUCT");
  const commercialRes = applyLayerModifiers(commercialMods, "COMMERCIAL");

  let appliedProduct = productRes.applied;
  if (commercialRes.exactCardMod?.raw.rate_card) {
    activeCard = commercialRes.exactCardMod.raw.rate_card;
    if (productRes.exactCardMod) {
      appliedProduct = productRes.applied.filter((m) => m.id !== productRes.exactCardMod!.raw.id);
    }
  } else if (productRes.exactCardMod?.raw.rate_card) {
    activeCard = productRes.exactCardMod.raw.rate_card;
  }

  if (activeCard.currency === context.currency) {
    activeFx = null;
  } else {
    activeFx = findActiveFx(plan, activeCard.currency, context.currency, ratedAtMs);
    if (!activeFx) {
      throw new DomainError(
        "tariff_unavailable",
        `No active FX rate from ${activeCard.currency} to ${context.currency}`,
      );
    }
  }

  const matrixApplied = applyMatrix(activeCard, context.attrs);
  const effectiveCard = matrixApplied.card;

  let combined = multiplyRational(productRes.multiplier, commercialRes.multiplier);
  if (activeFx) {
    combined = multiplyRational(combined, activeFx.rate);
  }

  return {
    version: 1,
    rated_at: context.rated_at,
    side: context.side,
    currency: context.currency,
    base_currency: effectiveCard.currency,
    rate_card_id: effectiveCard.id,
    basis: effectiveCard.basis,
    effective_card: effectiveCard,
    matched_matrix_index: matrixApplied.matchedIndex,
    applied_modifiers: [...appliedProduct, ...commercialRes.applied],
    fx_version_id: activeFx ? activeFx.raw.id : null,
    fx_rate: activeFx ? activeFx.raw.rate : null,
    combined_multiplier: formatRational(combined),
  };
}

export function resolvePricingSnapshot(
  plan: CompiledRatingPlan,
  context: PricingContext,
): PricingSnapshot {
  const ratedAtMs = parseIsoTimestampMs(context.rated_at);
  const base = selectBaseCard(plan, context, ratedAtMs);
  return resolveFromBaseCard(plan, base.card, base.fx, context, ratedAtMs);
}

export function resolvePricingSnapshotForCard(
  plan: CompiledRatingPlan,
  baseCard: RateCard,
  context: PricingContext,
): PricingSnapshot {
  if (baseCard.side !== context.side) {
    throw new DomainError(
      "tariff_unavailable",
      `Rate card side ${baseCard.side} does not match context side ${context.side}`,
    );
  }
  const ratedAtMs = parseIsoTimestampMs(context.rated_at);
  return resolveFromBaseCard(plan, baseCard, null, context, ratedAtMs);
}
