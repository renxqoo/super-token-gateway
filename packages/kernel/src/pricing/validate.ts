import { dimensions, operations, type Dimension } from "../values.ts";
import {
  isDecimalString,
  isNonNegIntString,
  isPosIntString,
  parseDecimalToRational,
  parseDerivedPositiveRational,
  parseRational,
} from "./rational.ts";
import { isValidIanaTimeZone, parseIsoTimestampMs } from "./time.ts";
import type {
  CliffRule,
  FxRateVersion,
  MatrixEntry,
  PriceModifier,
  PricingSnapshot,
  ProgressiveBand,
  Quantizer,
  RateCard,
  Rational,
  TimeWindow,
} from "./types.ts";

const currencyRegex = /^[A-Z]{3,8}$/;
const validDimensions = new Set<string>(dimensions);
const validOperations = new Set<string>(operations);
const decimalDimensions = new Set<string>(["audio_seconds", "video_seconds"]);

export function isDecimalDimension(dim: string): boolean {
  return decimalDimensions.has(dim);
}

function validateCurrency(currency: string, label: string): void {
  if (typeof currency !== "string" || !currencyRegex.test(currency)) {
    throw new TypeError(`Invalid ${label}`);
  }
}

function validateRatesMap(rates: Partial<Record<Dimension, Rational>>, label: string): void {
  for (const [key, rate] of Object.entries(rates)) {
    if (!validDimensions.has(key) || !rate) {
      throw new TypeError(`Invalid dimension in ${label}: ${key}`);
    }
    parseRational(rate);
  }
  if (rates.cache_write_tokens && (rates.cache_write_5m_tokens || rates.cache_write_1h_tokens)) {
    throw new TypeError("Choose aggregate or TTL-specific cache write rates");
  }
  if (rates.audio_ms && rates.audio_seconds) {
    throw new TypeError("Choose one duration unit per media type");
  }
  if (rates.video_ms && rates.video_seconds) {
    throw new TypeError("Choose one duration unit per media type");
  }
}

function validateQuantizers(quantizers: Partial<Record<Dimension, Quantizer>>): void {
  for (const [dim, q] of Object.entries(quantizers)) {
    if (!validDimensions.has(dim) || !q) {
      throw new TypeError(`Invalid quantizer dimension: ${dim}`);
    }
    const decimal = isDecimalDimension(dim);
    if (q.min_units !== undefined) {
      if (!(decimal ? isDecimalString(q.min_units) : isNonNegIntString(q.min_units))) {
        throw new TypeError(`Invalid quantizer min_units for ${dim}`);
      }
    }
    if (q.step !== undefined) {
      if (!(decimal ? isDecimalString(q.step) : isPosIntString(q.step))) {
        throw new TypeError(`Invalid quantizer step for ${dim}`);
      }
      if (parseDecimalToRational(q.step).numerator <= 0n) {
        throw new TypeError(`Quantizer step must be positive for ${dim}`);
      }
    }
  }
}

function validateCliff(cliff: CliffRule): void {
  if (!Array.isArray(cliff.driver) || cliff.driver.length === 0) {
    throw new TypeError("Cliff rule requires at least one driver dimension");
  }
  const seenDrivers = new Set<string>();
  for (const dim of cliff.driver) {
    if (!validDimensions.has(dim) || isDecimalDimension(dim) || seenDrivers.has(dim)) {
      throw new TypeError(`Invalid cliff driver dimension: ${String(dim)}`);
    }
    seenDrivers.add(dim);
  }
  if (!Array.isArray(cliff.tiers) || cliff.tiers.length === 0) {
    throw new TypeError("Cliff rule requires at least one tier");
  }
  let prev = -1n;
  for (let i = 0; i < cliff.tiers.length; i++) {
    const tier = cliff.tiers[i]!;
    if (!isNonNegIntString(tier.min_driver)) {
      throw new TypeError("Cliff tier min_driver must be a nonnegative integer string");
    }
    const cur = BigInt(tier.min_driver);
    if (i === 0 && cur !== 0n) {
      throw new TypeError("First cliff tier must start at min_driver 0");
    }
    if (cur <= prev) {
      throw new TypeError("Cliff tiers must have strictly ascending min_driver thresholds");
    }
    prev = cur;
    validateRatesMap(tier.rates, "cliff.tier.rates");
    if (tier.reasoning_rate) parseRational(tier.reasoning_rate);
  }
}

function validateProgressive(
  progressive: Partial<Record<Dimension, readonly ProgressiveBand[]>>,
): void {
  for (const [dim, bands] of Object.entries(progressive)) {
    if (!validDimensions.has(dim) || !Array.isArray(bands) || bands.length === 0) {
      throw new TypeError(`Invalid progressive bands for ${dim}`);
    }
    const decimal = isDecimalDimension(dim);
    let prev: { numerator: bigint; denominator: bigint } | null = null;
    for (let i = 0; i < bands.length; i++) {
      const band = bands[i]!;
      parseRational(band.rate);
      const isLast = i === bands.length - 1;
      if (isLast) {
        if (band.up_to !== null) {
          throw new TypeError(`Final progressive band for ${dim} must have up_to: null`);
        }
      } else {
        if (band.up_to === null) {
          throw new TypeError(`Non-final progressive band for ${dim} requires finite up_to`);
        }
        if (!(decimal ? isDecimalString(band.up_to) : isPosIntString(band.up_to))) {
          throw new TypeError(`Invalid progressive band up_to for ${dim}`);
        }
        const cur = parseDecimalToRational(band.up_to);
        if (cur.numerator <= 0n) {
          throw new TypeError(`Progressive band up_to must be positive for ${dim}`);
        }
        if (prev && cur.numerator * prev.denominator <= prev.numerator * cur.denominator) {
          throw new TypeError(`Progressive bands for ${dim} must be strictly ascending`);
        }
        prev = cur;
      }
    }
  }
}

const forbiddenAttrKeys = new Set(["__proto__", "constructor", "prototype"]);

function validateAttrMap(attrs: Record<string, unknown>, label: string): void {
  for (const [k, v] of Object.entries(attrs)) {
    if (forbiddenAttrKeys.has(k) || k.length === 0) {
      throw new TypeError(`Invalid attribute key in ${label}: ${k}`);
    }
    const valid =
      typeof v === "string" ||
      typeof v === "boolean" ||
      (typeof v === "number" && Number.isFinite(v));
    if (!valid) {
      throw new TypeError(`Invalid attribute value in ${label}.${k}`);
    }
  }
}

function validateDimensionExclusivity(dims: ReadonlySet<string>): void {
  if (
    dims.has("cache_write_tokens") &&
    (dims.has("cache_write_5m_tokens") || dims.has("cache_write_1h_tokens"))
  ) {
    throw new TypeError("Choose aggregate or TTL-specific cache write rates");
  }
  if (dims.has("audio_ms") && dims.has("audio_seconds")) {
    throw new TypeError("Choose one duration unit per media type");
  }
  if (dims.has("video_ms") && dims.has("video_seconds")) {
    throw new TypeError("Choose one duration unit per media type");
  }
}

function validateMatrix(matrix: readonly MatrixEntry[]): void {
  for (const entry of matrix) {
    if (!entry.match || Object.keys(entry.match).length === 0) {
      throw new TypeError("Matrix entry requires at least one match attribute");
    }
    validateAttrMap(entry.match, "matrix.match");
    validateRatesMap(entry.rates, "matrix.rates");
    if (entry.quantizers) validateQuantizers(entry.quantizers);
    if (entry.reasoning_rate) parseRational(entry.reasoning_rate);
    validateChargeBounds(entry.min_charge_atoms, entry.max_charge_atoms);
  }
}

function validateChargeBounds(minAtoms?: string, maxAtoms?: string): void {
  if (minAtoms !== undefined && !isNonNegIntString(minAtoms)) {
    throw new TypeError("min_charge_atoms must be a nonnegative integer string");
  }
  if (maxAtoms !== undefined && !isNonNegIntString(maxAtoms)) {
    throw new TypeError("max_charge_atoms must be a nonnegative integer string");
  }
  if (minAtoms !== undefined && maxAtoms !== undefined && BigInt(minAtoms) > BigInt(maxAtoms)) {
    throw new TypeError("min_charge_atoms cannot exceed max_charge_atoms");
  }
}

export function validateRateCard(card: RateCard, options?: { requireCeilings?: boolean }): void {
  if (!card.id || typeof card.id !== "string") throw new TypeError("RateCard requires id");
  if (card.side !== "CUSTOMER" && card.side !== "SUPPLIER") {
    throw new TypeError("Invalid RateCard side");
  }
  validateCurrency(card.currency, "currency");
  if (
    card.basis !== "PROVIDER_USAGE" &&
    card.basis !== "INPUT_CHARACTERS" &&
    card.basis !== "REQUEST_COMPLETION"
  ) {
    throw new TypeError("Invalid RateCard basis");
  }
  if (
    card.operation !== undefined &&
    card.operation !== "*" &&
    !validOperations.has(card.operation)
  ) {
    throw new TypeError("Invalid RateCard operation");
  }
  validateRatesMap(card.rates, "rates");
  if (card.reasoning_rate) parseRational(card.reasoning_rate);
  if (card.quantizers) validateQuantizers(card.quantizers);
  if (card.cliff) validateCliff(card.cliff);
  if (card.progressive) validateProgressive(card.progressive);
  if (card.matrix) validateMatrix(card.matrix);
  validateChargeBounds(card.min_charge_atoms, card.max_charge_atoms);

  for (const [dim, val] of Object.entries(card.ceilings)) {
    if (!validDimensions.has(dim) || val === undefined) {
      throw new TypeError(`Invalid ceiling dimension: ${dim}`);
    }
    const decimal = isDecimalDimension(dim);
    if (!(decimal ? isDecimalString(val) : isNonNegIntString(val))) {
      throw new TypeError(`Invalid ceiling value for ${dim}`);
    }
  }

  const baseDims = new Set<string>([
    ...Object.keys(card.rates),
    ...(card.progressive ? Object.keys(card.progressive) : []),
  ]);
  validateDimensionExclusivity(baseDims);

  const allCliffDims = new Set<string>();
  if (card.cliff) {
    for (const tier of card.cliff.tiers) {
      for (const dim of Object.keys(tier.rates)) allCliffDims.add(dim);
      const mergedCliffDims = new Set<string>([...baseDims, ...Object.keys(tier.rates)]);
      validateDimensionExclusivity(mergedCliffDims);
    }
    validateDimensionExclusivity(new Set<string>([...baseDims, ...allCliffDims]));
  }
  if (card.matrix) {
    for (const entry of card.matrix) {
      const mergedMatrixDims = new Set<string>([...baseDims, ...Object.keys(entry.rates)]);
      validateDimensionExclusivity(mergedMatrixDims);
      validateChargeBounds(
        entry.min_charge_atoms ?? card.min_charge_atoms,
        entry.max_charge_atoms ?? card.max_charge_atoms,
      );
      if (card.cliff) {
        validateDimensionExclusivity(new Set<string>([...mergedMatrixDims, ...allCliffDims]));
      }
    }
  }

  const allPricedDims = new Set<string>(baseDims);
  if (card.reasoning_rate) allPricedDims.add("output_tokens");
  if (card.cliff) {
    for (const tier of card.cliff.tiers) {
      for (const dim of Object.keys(tier.rates)) allPricedDims.add(dim);
      if (tier.reasoning_rate) allPricedDims.add("output_tokens");
    }
  }
  if (card.matrix) {
    for (const entry of card.matrix) {
      for (const dim of Object.keys(entry.rates)) allPricedDims.add(dim);
      if (entry.reasoning_rate) allPricedDims.add("output_tokens");
    }
  }

  if (card.basis === "INPUT_CHARACTERS") {
    for (const dim of allPricedDims) {
      if (dim !== "characters" && dim !== "requests") {
        throw new TypeError("INPUT_CHARACTERS basis only supports characters and requests");
      }
    }
  }
  if (card.basis === "REQUEST_COMPLETION") {
    for (const dim of allPricedDims) {
      if (dim !== "requests") {
        throw new TypeError("REQUEST_COMPLETION basis only supports requests");
      }
    }
  }

  if (options?.requireCeilings) {
    for (const dim of allPricedDims) {
      if (card.ceilings[dim as Dimension] === undefined) {
        throw new TypeError(`Missing reservation ceiling for dimension ${dim}`);
      }
    }
  }
}

export function validateTimeWindow(window: TimeWindow): void {
  if (!isValidIanaTimeZone(window.timezone)) {
    throw new TypeError(`Invalid IANA timezone: ${String(window.timezone)}`);
  }
  if (
    !Number.isInteger(window.start_minute) ||
    !Number.isInteger(window.end_minute) ||
    window.start_minute < 0 ||
    window.start_minute >= 1440 ||
    window.end_minute < 0 ||
    window.end_minute >= 1440 ||
    window.start_minute === window.end_minute
  ) {
    throw new TypeError("TimeWindow requires valid non-empty minute boundaries in [0, 1439]");
  }
  if (window.days_of_week !== undefined) {
    if (!Array.isArray(window.days_of_week) || window.days_of_week.length === 0) {
      throw new TypeError("TimeWindow days_of_week must be non-empty when specified");
    }
    for (const day of window.days_of_week) {
      if (!Number.isInteger(day) || day < 0 || day > 6) {
        throw new TypeError("TimeWindow days_of_week entries must be integers in 0..6");
      }
    }
  }
}

function validateEffectiveWindow(from?: string, to?: string): void {
  const fromMs = from !== undefined ? parseIsoTimestampMs(from) : null;
  const toMs = to !== undefined ? parseIsoTimestampMs(to) : null;
  if (fromMs !== null && toMs !== null && fromMs >= toMs) {
    throw new TypeError("effective_from must be strictly earlier than effective_to");
  }
}

export function validatePriceModifier(
  modifier: PriceModifier,
  options?: { requireCeilings?: boolean },
): void {
  if (!modifier.id || typeof modifier.id !== "string") {
    throw new TypeError("PriceModifier requires id");
  }
  if (modifier.layer !== "PRODUCT" && modifier.layer !== "COMMERCIAL") {
    throw new TypeError("Invalid modifier layer");
  }
  if (modifier.kind !== "MULTIPLIER" && modifier.kind !== "EXACT_RATE_CARD") {
    throw new TypeError("Invalid modifier kind");
  }
  if (
    modifier.stacking !== "MULTIPLY" &&
    modifier.stacking !== "BEST_FOR_CUSTOMER" &&
    modifier.stacking !== "EXCLUSIVE"
  ) {
    throw new TypeError("Invalid modifier stacking policy");
  }
  if (!Number.isInteger(modifier.priority)) {
    throw new TypeError("Modifier priority must be an integer");
  }
  validateEffectiveWindow(modifier.effective_from, modifier.effective_to);
  if (modifier.time_windows) {
    for (const tw of modifier.time_windows) validateTimeWindow(tw);
  }
  if (modifier.match.operation !== undefined && modifier.match.operation !== "*") {
    if (!validOperations.has(modifier.match.operation)) {
      throw new TypeError("Invalid modifier match.operation");
    }
  }
  if (modifier.match.attrs) {
    validateAttrMap(modifier.match.attrs, "modifier.match.attrs");
  }
  if (modifier.kind === "MULTIPLIER") {
    if (!modifier.multiplier) throw new TypeError("MULTIPLIER modifier requires multiplier");
    const parsed = parseRational(modifier.multiplier);
    if (parsed.numerator <= 0n) {
      throw new TypeError("Modifier multiplier must be positive");
    }
  } else {
    if (modifier.stacking === "BEST_FOR_CUSTOMER") {
      throw new TypeError("EXACT_RATE_CARD modifier cannot use BEST_FOR_CUSTOMER stacking");
    }
    if (!modifier.rate_card) throw new TypeError("EXACT_RATE_CARD modifier requires rate_card");
    if (modifier.match.side !== undefined && modifier.match.side !== modifier.rate_card.side) {
      throw new TypeError("EXACT_RATE_CARD modifier match.side must match rate_card.side");
    }
    validateRateCard(modifier.rate_card, options);
  }
}

export function validateFxRateVersion(fx: FxRateVersion): void {
  if (!fx.id || typeof fx.id !== "string") throw new TypeError("FxRateVersion requires id");
  validateCurrency(fx.base_currency, "base_currency");
  validateCurrency(fx.quote_currency, "quote_currency");
  if (fx.base_currency === fx.quote_currency) {
    throw new TypeError("FX base_currency and quote_currency must differ");
  }
  const parsed = parseRational(fx.rate);
  if (parsed.numerator <= 0n) throw new TypeError("FX rate must be positive");
  validateEffectiveWindow(fx.effective_from, fx.effective_to);
}

export function validatePricingSnapshot(snapshot: PricingSnapshot): void {
  if (snapshot.version !== 1) throw new TypeError("Unsupported PricingSnapshot version");
  parseIsoTimestampMs(snapshot.rated_at);
  validateCurrency(snapshot.currency, "currency");
  validateCurrency(snapshot.base_currency, "base_currency");
  validateRateCard(snapshot.effective_card);
  parseDerivedPositiveRational(snapshot.combined_multiplier);
  if (snapshot.fx_rate) {
    const fx = parseRational(snapshot.fx_rate);
    if (fx.numerator <= 0n) throw new TypeError("Snapshot fx_rate must be positive");
  }
}
