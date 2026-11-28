import { DomainError } from "../errors.ts";
import type { Dimension, Usage } from "../values.ts";
import {
  compileSnapshot,
  computeCliffDriverSum,
  evaluateCompiledSnapshotInternal,
  type CompiledSnapshot,
} from "./evaluator.ts";
import {
  compareRational,
  isDecimalString,
  isNonNegIntString,
  parseDecimalToRational,
  quantizeQuantity,
} from "./rational.ts";
import type {
  HostedToolDimension,
  PricingSnapshot,
  SnapshotReservationMeasurement,
  SnapshotReservationQuote,
} from "./types.ts";

const textInputDimensions = [
  "input_tokens",
  "cached_input_tokens",
  "cache_write_tokens",
  "cache_write_5m_tokens",
  "cache_write_1h_tokens",
] as const;

const hostedToolDimensions: readonly HostedToolDimension[] = [
  "web_search_calls",
  "code_interpreter_sessions",
  "file_search_calls",
];

function tightenIntDim(usage: Usage, dim: Dimension, value: unknown, allowZero = false): void {
  if (value === undefined) return;
  if (
    typeof value !== "number" ||
    !Number.isSafeInteger(value) ||
    (allowZero ? value < 0 : value < 1)
  ) {
    throw new DomainError("billing_ceiling", `Invalid reservation measurement for ${dim}`);
  }
  const ceilingRaw = usage[dim];
  if (ceilingRaw !== undefined && BigInt(value) > BigInt(ceilingRaw)) {
    throw new DomainError("billing_ceiling", `Requested ${dim} exceeds reservation ceiling`);
  }
  usage[dim] = String(value);
}

function buildBaseUpperUsage(
  compiled: CompiledSnapshot,
  measurement: SnapshotReservationMeasurement,
  outputBound: number,
): Usage {
  if (!Number.isSafeInteger(outputBound) || outputBound < 0) {
    throw new DomainError("billing_ceiling", "outputBound must be a nonnegative safe integer");
  }
  const { snapshot, card } = compiled;
  if (snapshot.basis === "REQUEST_COMPLETION") {
    const reqCeil = card.raw.ceilings.requests;
    if (reqCeil !== undefined && BigInt(reqCeil) < 1n) {
      throw new DomainError("invalid_tariff", "Request tariff requires positive ceiling");
    }
    return { requests: "1" };
  }
  if (snapshot.basis === "INPUT_CHARACTERS") {
    const chars = measurement.inputCharacters;
    if (chars === undefined || !isNonNegIntString(chars)) {
      throw new DomainError(
        "billing_ceiling",
        "Character pricing requires valid input character count",
      );
    }
    const charCeil = card.raw.ceilings.characters;
    if (charCeil !== undefined && BigInt(chars) > BigInt(charCeil)) {
      throw new DomainError("billing_ceiling", "Input characters exceed reservation ceiling");
    }
    if (card.activeDimensions.includes("requests")) {
      const reqCeil = card.raw.ceilings.requests;
      if (reqCeil !== undefined && BigInt(reqCeil) < 1n) {
        throw new DomainError("invalid_tariff", "Request tariff requires positive ceiling");
      }
    }
    return { characters: chars, requests: "1" };
  }

  const usage: Usage = {};
  for (const dim of card.activeDimensions) {
    const ceil = card.raw.ceilings[dim];
    if (ceil !== undefined) usage[dim] = ceil;
  }
  if (card.cliff) {
    for (const dim of card.cliff.driver) {
      if (usage[dim] !== undefined) continue;
      const hasAgg = usage.cache_write_tokens !== undefined;
      const hasTtl =
        usage.cache_write_5m_tokens !== undefined && usage.cache_write_1h_tokens !== undefined;
      if (dim === "cache_write_tokens" && hasTtl) continue;
      if ((dim === "cache_write_5m_tokens" || dim === "cache_write_1h_tokens") && hasAgg) continue;
      const ceil = card.raw.ceilings[dim];
      if (ceil !== undefined) {
        usage[dim] = ceil;
      } else if (
        dim === "cache_write_tokens" &&
        card.raw.ceilings.cache_write_5m_tokens !== undefined &&
        card.raw.ceilings.cache_write_1h_tokens !== undefined
      ) {
        usage.cache_write_5m_tokens ??= card.raw.ceilings.cache_write_5m_tokens;
        usage.cache_write_1h_tokens ??= card.raw.ceilings.cache_write_1h_tokens;
      }
    }
  }
  if (card.activeDimensions.includes("requests")) {
    if (BigInt(usage.requests ?? "0") < 1n) {
      throw new DomainError("invalid_tariff", "Request tariff requires positive ceiling");
    }
    usage.requests = "1";
  }
  if (card.activeDimensions.includes("output_tokens") && outputBound > 0) {
    if (usage.output_tokens === undefined || BigInt(outputBound) > BigInt(usage.output_tokens)) {
      throw new DomainError("billing_ceiling", "Output bound exceeds reservation ceiling");
    }
    usage.output_tokens = String(outputBound);
  }
  if (card.activeDimensions.includes("images") && measurement.images !== undefined) {
    tightenIntDim(usage, "images", measurement.images, false);
  }
  if (card.activeDimensions.includes("pages") && measurement.pages !== undefined) {
    tightenIntDim(usage, "pages", measurement.pages, false);
  }
  if (card.activeDimensions.includes("search_units") && measurement.searchUnits !== undefined) {
    tightenIntDim(usage, "search_units", measurement.searchUnits, false);
  }
  if (measurement.maxToolCalls) {
    for (const toolDim of hostedToolDimensions) {
      const cap = measurement.maxToolCalls[toolDim];
      if (cap !== undefined && card.activeDimensions.includes(toolDim)) {
        tightenIntDim(usage, toolDim, cap, true);
      }
    }
  }
  if (measurement.durationMs) {
    for (const dim of ["audio_ms", "music_ms", "video_ms"] as const) {
      const ms = measurement.durationMs[dim];
      if (ms !== undefined && card.activeDimensions.includes(dim)) {
        tightenIntDim(usage, dim, ms, true);
      }
    }
  }
  if (measurement.durationSeconds) {
    for (const dim of ["audio_seconds", "video_seconds"] as const) {
      const sec = measurement.durationSeconds[dim];
      if (sec !== undefined && card.activeDimensions.includes(dim)) {
        if (!isDecimalString(sec)) {
          throw new DomainError("billing_ceiling", `Invalid reservation measurement for ${dim}`);
        }
        const parsed = parseDecimalToRational(sec);
        const ceilRaw = usage[dim];
        if (ceilRaw !== undefined && compareRational(parsed, parseDecimalToRational(ceilRaw)) > 0) {
          throw new DomainError("billing_ceiling", `Requested ${dim} exceeds reservation ceiling`);
        }
        usage[dim] = sec;
      }
    }
  }
  for (const dim of card.activeDimensions) {
    if (usage[dim] === undefined) {
      throw new DomainError("invalid_tariff", `Missing reservation ceiling for ${dim}`);
    }
  }
  return usage;
}

function reachableCliffIndices(compiled: CompiledSnapshot, maxDriver: bigint): (number | null)[] {
  const cliff = compiled.card.cliff;
  if (!cliff) return [null];
  const indices: number[] = [];
  for (let i = 0; i < cliff.tiers.length; i++) {
    if (maxDriver >= cliff.tiers[i]!.minDriver) indices.push(i);
    else break;
  }
  return indices.length > 0 ? indices : [0];
}

function canTightenTextInput(
  compiled: CompiledSnapshot,
  baseUsage: Usage,
  estimatedInputTokens: number | undefined,
): boolean {
  if (
    compiled.snapshot.basis !== "PROVIDER_USAGE" ||
    estimatedInputTokens === undefined ||
    !Number.isSafeInteger(estimatedInputTokens) ||
    estimatedInputTokens <= 0
  ) {
    return false;
  }
  const activeTextDims = textInputDimensions.filter((d) =>
    compiled.card.activeDimensions.includes(d),
  );
  if (activeTextDims.length === 0) return false;
  for (const dim of activeTextDims) {
    if (compiled.card.quantizers[dim]) return false;
    const prog = compiled.card.progressive[dim];
    if (prog && activeTextDims.length > 1) {
      for (let i = 1; i < prog.length; i++) {
        if (compareRational(prog[i]!.rate, prog[i - 1]!.rate) < 0) return false;
      }
    }
  }
  const est = BigInt(estimatedInputTokens);
  for (const dim of activeTextDims) {
    const ceil = baseUsage[dim];
    if (ceil !== undefined && est > BigInt(ceil)) return false;
  }
  return true;
}

export function quoteCompiledSnapshotReservation(
  compiled: CompiledSnapshot,
  measurement: SnapshotReservationMeasurement,
  outputBound: number,
): SnapshotReservationQuote {
  const baseUsage = buildBaseUpperUsage(compiled, measurement, outputBound);
  const { card } = compiled;

  const tightened = canTightenTextInput(compiled, baseUsage, measurement.estimatedInputTokens);
  const candidateUsages: Usage[] = [];
  if (tightened) {
    const est = String(measurement.estimatedInputTokens!);
    const stripped: Usage = { ...baseUsage };
    for (const dim of textInputDimensions) delete stripped[dim];
    const activeTextDims = textInputDimensions.filter((d) => card.activeDimensions.includes(d));
    const hasBothTtl =
      activeTextDims.includes("cache_write_5m_tokens") &&
      activeTextDims.includes("cache_write_1h_tokens");
    for (const dim of activeTextDims) {
      const candidate: Usage = { ...stripped, [dim]: est };
      if (hasBothTtl && (dim === "cache_write_5m_tokens" || dim === "cache_write_1h_tokens")) {
        const other =
          dim === "cache_write_5m_tokens" ? "cache_write_1h_tokens" : "cache_write_5m_tokens";
        candidate[other] = "0";
      }
      if (activeTextDims.includes("input_tokens") && candidate.input_tokens === undefined) {
        candidate.input_tokens = "0";
      }
      candidateUsages.push(candidate);
    }
  } else {
    candidateUsages.push(baseUsage);
  }

  let globalMaxDriver = 0n;
  if (card.cliff) {
    let hasUnboundedDriver = false;
    if (tightened) {
      const textDriverSet = new Set<string>(textInputDimensions);
      let hasTextDriver = false;
      for (const dim of card.cliff.driver) {
        if (textDriverSet.has(dim)) {
          hasTextDriver = true;
        } else if (baseUsage[dim] !== undefined) {
          globalMaxDriver += BigInt(baseUsage[dim]!);
        } else {
          hasUnboundedDriver = true;
        }
      }
      if (hasTextDriver) {
        globalMaxDriver += BigInt(measurement.estimatedInputTokens!);
      }
    } else {
      const hasAgg = baseUsage.cache_write_tokens !== undefined;
      const hasTtl =
        baseUsage.cache_write_5m_tokens !== undefined &&
        baseUsage.cache_write_1h_tokens !== undefined;
      for (const dim of card.cliff.driver) {
        if (dim === "cache_write_tokens" && hasTtl) continue;
        if ((dim === "cache_write_5m_tokens" || dim === "cache_write_1h_tokens") && hasAgg)
          continue;
        if (baseUsage[dim] === undefined) {
          hasUnboundedDriver = true;
          break;
        }
      }
      globalMaxDriver = computeCliffDriverSum(
        baseUsage,
        card.cliff.driver,
        card.cliff.hasAggregateCacheWrite,
        card.cliff.hasTtlCacheWrite,
      );
      if (hasAgg && !card.cliff.hasAggregateCacheWrite && card.cliff.hasTtlCacheWrite) {
        const aggregate = BigInt(baseUsage.cache_write_tokens!);
        let ttlUpper = 0n;
        for (const dim of card.cliff.driver) {
          if (dim === "cache_write_5m_tokens" || dim === "cache_write_1h_tokens") {
            ttlUpper += BigInt(card.raw.ceilings[dim] ?? aggregate);
          }
        }
        globalMaxDriver += ttlUpper < aggregate ? ttlUpper : aggregate;
      }
    }
    if (hasUnboundedDriver) {
      globalMaxDriver = card.cliff.tiers[card.cliff.tiers.length - 1]!.minDriver;
    }
  }
  const reachableTiers = reachableCliffIndices(compiled, globalMaxDriver);

  const expandedCandidates: Usage[] = [];
  const hasReasoningRate =
    card.reasoningRate !== null ||
    (card.cliff?.tiers.some((t) => t.reasoningRate !== null) ?? false);
  const outProgBands = card.progressive.output_tokens;
  const outQuantizer = card.quantizers.output_tokens;
  for (const cand of candidateUsages) {
    expandedCandidates.push(cand);
    if (hasReasoningRate && cand.output_tokens !== undefined && cand.output_tokens !== "0") {
      const outInt = BigInt(cand.output_tokens);
      const seenReasoning = new Set<bigint>([0n, outInt]);
      expandedCandidates.push({ ...cand, reasoning_tokens: cand.output_tokens });
      if (outProgBands) {
        const quantizedOut = outQuantizer
          ? quantizeQuantity(
              { numerator: outInt, denominator: 1n },
              outQuantizer.minUnits,
              outQuantizer.step,
            )
          : { numerator: outInt, denominator: 1n };
        const qOutInt = quantizedOut.numerator / quantizedOut.denominator;
        for (const band of outProgBands) {
          if (band.upTo === null) continue;
          const bpInt = band.upTo.numerator / band.upTo.denominator;
          if (bpInt < qOutInt) {
            const candidateReason = qOutInt - bpInt;
            if (
              candidateReason > 0n &&
              candidateReason < outInt &&
              !seenReasoning.has(candidateReason)
            ) {
              seenReasoning.add(candidateReason);
              expandedCandidates.push({ ...cand, reasoning_tokens: candidateReason.toString() });
            }
          }
        }
      }
    }
  }

  let bestAtoms = -1n;
  let bestUsage: Usage = expandedCandidates[0]!;
  let bestCliffIndex: number | null = null;

  for (let c = 0; c < expandedCandidates.length; c++) {
    const cand = expandedCandidates[c]!;
    for (let t = 0; t < reachableTiers.length; t++) {
      const tierIdx = reachableTiers[t]!;
      const res = evaluateCompiledSnapshotInternal(compiled, cand, {
        inputCharacters: measurement.inputCharacters,
        forcedCliffTierIndex: tierIdx,
        collectBreakdown: false,
        skipRequiredTokenCheck: true,
      });
      if (res.total_atoms > bestAtoms) {
        bestAtoms = res.total_atoms;
        bestUsage = cand;
        bestCliffIndex = tierIdx;
      }
    }
  }

  return {
    currency: compiled.snapshot.currency,
    rate_card_id: compiled.snapshot.rate_card_id,
    amount_atoms: (bestAtoms < 0n ? 0n : bestAtoms).toString(),
    matched_cliff_tier_index: bestCliffIndex,
    worst_case_usage: bestUsage,
  };
}

export function quoteSnapshotReservation(
  snapshot: PricingSnapshot,
  measurement: SnapshotReservationMeasurement,
  outputBound: number,
): SnapshotReservationQuote {
  return quoteCompiledSnapshotReservation(compileSnapshot(snapshot), measurement, outputBound);
}
