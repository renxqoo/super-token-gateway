import { GatewayError, optionalObject } from "@gateway/contracts";
import {
  cacheWritePartitionMatches,
  calculateCost,
  evaluateSnapshotAtoms,
  hasPricedTokenUsage,
  type Dimension,
  type HostedToolDimension,
  type Operation,
  type Pricing,
  type PricingSnapshot,
  type Protocol,
  type RateCard,
  type Usage,
} from "@gateway/kernel";
import { rateCardFromPriceVersion, type PriceVersionRecord } from "@gateway/ledger";
import {
  estimateTokensFromPlaintext,
  observePlaintext,
  stringifyExact,
  type PlaintextStats,
} from "@gateway/protocol";
import { cardPricesDimension } from "../pricing.ts";

export const mediaCompletionEvents: Partial<Record<Operation, string>> = {
  images: "image_generation.completed",
  image_edits: "image_edit.completed",
  speech: "speech.audio.done",
  transcription: "transcript.text.done",
};

function imageArtifact(value: unknown): string | undefined {
  const { b64_json, url } = optionalObject(value);
  if (
    typeof b64_json === "string" &&
    /^[A-Za-z0-9+/]+={0,2}$/.test(b64_json) &&
    b64_json.length % 4 === 0
  )
    return b64_json;
  if (typeof url !== "string" || !URL.canParse(url)) return;
  const parsed = new URL(url);
  if (parsed.protocol === "https:" || parsed.protocol === "http:") return parsed.href;
}

export function recordImageArtifact(artifacts: Set<string>, value: unknown): void {
  const artifact = imageArtifact(value);
  if (!artifact) return;
  const digest = new Bun.CryptoHasher("sha256").update(artifact).digest("hex");
  if (artifacts.has(digest)) {
    throw new GatewayError(502, "invalid_upstream_usage", "Provider repeated an image result");
  }
  artifacts.add(digest);
}

export function countImageArtifacts(images: unknown[]): number {
  const artifacts = new Set<string>();
  for (const image of images) recordImageArtifact(artifacts, image);
  return artifacts.size;
}

function effectiveTokenRatesView(
  price: PriceVersionRecord,
  snapshot?: PricingSnapshot,
): Pricing["rates"] {
  if (!snapshot && !price.pricing_spec) return price.rates;
  const card = snapshot ? snapshot.effective_card : rateCardFromPriceVersion(price);
  const view: Pricing["rates"] = {};
  if (cardPricesDimension(card, "input_tokens")) {
    view.input_tokens = { numerator: "1", denominator: "1" };
  }
  if (cardPricesDimension(card, "output_tokens")) {
    view.output_tokens = { numerator: "1", denominator: "1" };
  }
  return view;
}

function effectivePricesImages(price: PriceVersionRecord, snapshot?: PricingSnapshot): boolean {
  if (!snapshot && !price.pricing_spec) return Boolean(price.rates.images);
  const card = snapshot ? snapshot.effective_card : rateCardFromPriceVersion(price);
  return cardPricesDimension(card, "images");
}

function completeUnrequestedHostedToolUsage(
  observed: Usage | undefined,
  customerSnapshot?: PricingSnapshot,
  supplierSnapshot?: PricingSnapshot,
  authorizedHostedTools?: ReadonlySet<HostedToolDimension> | null,
): Usage | undefined {
  if (authorizedHostedTools === undefined || (!customerSnapshot && !supplierSnapshot)) {
    return observed;
  }
  let usage = observed;
  for (const dim of [
    "web_search_calls",
    "code_interpreter_sessions",
    "file_search_calls",
  ] as const) {
    if (usage?.[dim] !== undefined || authorizedHostedTools?.has(dim)) continue;
    if (
      (customerSnapshot?.basis === "PROVIDER_USAGE" &&
        cardPricesDimension(customerSnapshot.effective_card, dim)) ||
      (supplierSnapshot?.basis === "PROVIDER_USAGE" &&
        cardPricesDimension(supplierSnapshot.effective_card, dim))
    ) {
      usage = { ...usage, [dim]: "0" };
    }
  }
  return usage;
}

export function buildUsageForOperation(
  operation: Operation,
  customerPrice: PriceVersionRecord,
  supplierPrice: PriceVersionRecord,
  rawUsage: Usage | undefined,
  deliveredImages?: number,
  authorizedImages?: number,
  customerSnapshot?: PricingSnapshot,
  supplierSnapshot?: PricingSnapshot,
  authorizedHostedTools?: ReadonlySet<HostedToolDimension> | null,
): Usage | null {
  if (
    rawUsage &&
    rawUsage.output_tokens === undefined &&
    (operation === "embeddings" || operation === "moderations")
  ) {
    rawUsage = { ...rawUsage, output_tokens: "0" };
  }

  const customerBasis = customerSnapshot?.basis ?? customerPrice.basis;
  const supplierBasis = supplierSnapshot?.basis ?? supplierPrice.basis;
  const customerUsesProviderUsage = customerBasis === "PROVIDER_USAGE";
  const supplierUsesProviderUsage = supplierBasis === "PROVIDER_USAGE";
  if (
    (customerUsesProviderUsage &&
      !hasPricedTokenUsage(
        rawUsage ?? {},
        effectiveTokenRatesView(customerPrice, customerSnapshot),
      )) ||
    (supplierUsesProviderUsage &&
      !hasPricedTokenUsage(
        rawUsage ?? {},
        effectiveTokenRatesView(supplierPrice, supplierSnapshot),
      ))
  )
    return null;

  if (operation === "images" || operation === "image_edits") {
    const declared = rawUsage?.images;
    const measured = deliveredImages === undefined ? undefined : String(deliveredImages);
    if (declared !== undefined && measured !== undefined && declared !== measured) {
      throw new GatewayError(
        502,
        "usage_alias_conflict",
        "Provider image count disagrees with delivered images",
      );
    }
    const quantity = measured ?? declared;
    if (
      quantity !== undefined &&
      (authorizedImages === undefined ||
        !Number.isSafeInteger(authorizedImages) ||
        BigInt(quantity) > BigInt(authorizedImages))
    ) {
      throw new GatewayError(
        502,
        "invalid_upstream_usage",
        "Provider returned more images than authorized",
      );
    }
    if (
      (!quantity || quantity === "0") &&
      ((customerUsesProviderUsage && effectivePricesImages(customerPrice, customerSnapshot)) ||
        (supplierUsesProviderUsage && effectivePricesImages(supplierPrice, supplierSnapshot)))
    )
      return null;
    if (quantity !== undefined) return { ...rawUsage, images: quantity };
  }
  if (rawUsage) {
    return (
      completeUnrequestedHostedToolUsage(
        rawUsage,
        customerSnapshot,
        supplierSnapshot,
        authorizedHostedTools ?? null,
      ) ?? rawUsage
    );
  }
  if (customerUsesProviderUsage || supplierUsesProviderUsage) return null;
  return { requests: "1" };
}

const textTokenDimensions = new Set<string>([
  "input_tokens",
  "output_tokens",
  "cached_input_tokens",
  "cache_write_tokens",
  "cache_write_5m_tokens",
  "cache_write_1h_tokens",
]);

function hasPositiveNonTokenDimension(card: RateCard, observed: Usage | undefined): boolean {
  const checkRates = (rates: RateCard["rates"]) => {
    for (const [dim, rate] of Object.entries(rates)) {
      if (textTokenDimensions.has(dim) || !rate || rate.numerator === "0") continue;
      if (observed?.[dim as Dimension] === undefined) return true;
    }
    return false;
  };
  if (checkRates(card.rates)) return true;
  if (card.progressive) {
    for (const [dim, bands] of Object.entries(card.progressive)) {
      if (textTokenDimensions.has(dim) || !bands?.some((b) => b.rate.numerator !== "0")) continue;
      if (observed?.[dim as Dimension] === undefined) return true;
    }
  }
  if (card.cliff) {
    for (const tier of card.cliff.tiers) {
      if (checkRates(tier.rates)) return true;
    }
  }
  return false;
}

function hasUnobservedNonTokenPricing(
  observed: Usage | undefined,
  customerSnapshot?: PricingSnapshot,
  supplierSnapshot?: PricingSnapshot,
  authorizedHostedTools?: ReadonlySet<HostedToolDimension> | null,
): boolean {
  if (authorizedHostedTools && authorizedHostedTools.size > 0) {
    for (const toolDim of authorizedHostedTools) {
      if (observed?.[toolDim] === undefined) return true;
    }
  }
  if (
    customerSnapshot?.basis === "PROVIDER_USAGE" &&
    hasPositiveNonTokenDimension(customerSnapshot.effective_card, observed)
  ) {
    return true;
  }
  if (
    supplierSnapshot?.basis === "PROVIDER_USAGE" &&
    hasPositiveNonTokenDimension(supplierSnapshot.effective_card, observed)
  ) {
    return true;
  }
  return false;
}

export function buildModerationUsageFallback(
  observed: Usage | undefined,
  inputChars: number,
  outputChars: number,
  options?: {
    preparedBody?: string | Uint8Array;
    outputStats?: PlaintextStats;
    customerPrice?: PriceVersionRecord;
    supplierPrice?: PriceVersionRecord;
    customerPricing?: Pricing;
    supplierPricing?: Pricing;
    customerSnapshot?: PricingSnapshot;
    supplierSnapshot?: PricingSnapshot;
    authorizedHostedTools?: ReadonlySet<HostedToolDimension> | null;
    heldCustomerAtoms?: bigint;
    heldSupplierAtoms?: bigint;
  },
): Usage | null {
  observed = completeUnrequestedHostedToolUsage(
    observed,
    options?.customerSnapshot,
    options?.supplierSnapshot,
    options?.authorizedHostedTools,
  );
  if (
    hasUnobservedNonTokenPricing(
      observed,
      options?.customerSnapshot,
      options?.supplierSnapshot,
      options?.authorizedHostedTools,
    )
  ) {
    return null;
  }
  const estimateInput = observed?.input_tokens === undefined;
  const estimateOutput = observed?.output_tokens === undefined;
  const observedReasoning = estimateOutput ? Number(observed?.reasoning_tokens ?? "0") : 0;
  if (!Number.isSafeInteger(observedReasoning) || observedReasoning < 0) return null;
  let estInput = estimateInput
    ? options?.preparedBody !== undefined
      ? estimateInputTokensFromBody(options.preparedBody, inputChars)
      : Math.max(1, Math.ceil(inputChars / 4))
    : 0;
  let estOutput = estimateOutput
    ? options?.outputStats && options.outputStats.chunks > 0
      ? estimateTokensFromPlaintext(
          options.outputStats.asciiChars,
          options.outputStats.nonAsciiChars,
        )
      : Math.max(1, Math.ceil(outputChars / 4))
    : 0;
  estOutput = Math.max(estOutput, observedReasoning);
  if (options?.customerPrice && options.supplierPrice) {
    if (estimateInput) {
      const ceiling = minPositiveCeiling(
        options.customerPrice,
        options.supplierPrice,
        "input_tokens",
        options.customerSnapshot,
        options.supplierSnapshot,
      );
      if (ceiling !== undefined && estInput > ceiling) estInput = ceiling;
    }
    if (estimateOutput) {
      const ceiling = minPositiveCeiling(
        options.customerPrice,
        options.supplierPrice,
        "output_tokens",
        options.customerSnapshot,
        options.supplierSnapshot,
      );
      if (ceiling !== undefined && estOutput > ceiling) estOutput = ceiling;
    }
  }
  if (estOutput < observedReasoning) return null;
  const assemble = (inTokens: number, outTokens: number): Usage => ({
    ...observed,
    ...(estimateInput ? { input_tokens: String(inTokens) } : {}),
    ...(estimateOutput ? { output_tokens: String(Math.max(observedReasoning, outTokens)) } : {}),
  });
  let candidate = assemble(estInput, estOutput);
  if (
    !cacheWritePartitionMatches(
      candidate.cache_write_tokens,
      candidate.cache_write_5m_tokens,
      candidate.cache_write_1h_tokens,
    )
  ) {
    return null;
  }
  const customerBasis = options?.customerSnapshot?.basis ?? options?.customerPrice?.basis;
  const supplierBasis = options?.supplierSnapshot?.basis ?? options?.supplierPrice?.basis;
  if (
    options?.customerPrice &&
    options.supplierPrice &&
    options.customerPricing &&
    options.supplierPricing
  ) {
    if (
      (!options.customerSnapshot &&
        customerBasis === "PROVIDER_USAGE" &&
        !hasValidSettlementCacheWrites(candidate, options.customerPricing)) ||
      (!options.supplierSnapshot &&
        supplierBasis === "PROVIDER_USAGE" &&
        !hasValidSettlementCacheWrites(candidate, options.supplierPricing))
    ) {
      return null;
    }
  }
  if (
    (estimateInput || estimateOutput) &&
    options?.customerPrice &&
    options.supplierPrice &&
    options.customerPricing &&
    options.supplierPricing &&
    options.heldCustomerAtoms !== undefined &&
    options.heldSupplierAtoms !== undefined
  ) {
    const customerPricing = options.customerPricing;
    const supplierPricing = options.supplierPricing;
    const customerSnapshot = options.customerSnapshot;
    const supplierSnapshot = options.supplierSnapshot;
    const heldCustomerAtoms = options.heldCustomerAtoms;
    const heldSupplierAtoms = options.heldSupplierAtoms;
    const fitsHold = (usage: Usage): boolean => {
      try {
        if (customerBasis === "PROVIDER_USAGE") {
          const custAtoms = customerSnapshot
            ? evaluateSnapshotAtoms(customerSnapshot, usage)
            : pricedSettlementCost(usage, customerPricing);
          if (custAtoms > heldCustomerAtoms) return false;
        }
        if (supplierBasis === "PROVIDER_USAGE") {
          const supAtoms = supplierSnapshot
            ? evaluateSnapshotAtoms(supplierSnapshot, usage)
            : pricedSettlementCost(usage, supplierPricing);
          if (supAtoms > heldSupplierAtoms) return false;
        }
        return true;
      } catch {
        return false;
      }
    };
    if (!fitsHold(candidate)) {
      let low = 0;
      let high = 10_000;
      while (low < high) {
        const mid = (low + high + 1) >> 1;
        const nextIn = estimateInput ? Math.floor((estInput * mid) / 10_000) : estInput;
        const nextOut = estimateOutput ? Math.floor((estOutput * mid) / 10_000) : estOutput;
        if (fitsHold(assemble(nextIn, nextOut))) low = mid;
        else high = mid - 1;
      }
      candidate = assemble(
        estimateInput ? Math.floor((estInput * low) / 10_000) : estInput,
        estimateOutput ? Math.floor((estOutput * low) / 10_000) : estOutput,
      );
      if (!fitsHold(candidate)) return null;
    }
  }
  return candidate;
}

export function canEstimateStreamSettlementError(code: string): boolean {
  return (
    code === "stream_interrupted" ||
    code === "stream_truncated" ||
    code === "client_aborted" ||
    code === "upstream_timeout" ||
    code === "upstream_stream_idle_timeout"
  );
}

export function observeClientChunkPlaintext(
  stats: PlaintextStats,
  protocol: Protocol,
  chunk: Record<string, unknown>,
): void {
  if (protocol === "chat") {
    if (!Array.isArray(chunk.choices)) return;
    for (const rawChoice of chunk.choices) {
      const delta = optionalObject(optionalObject(rawChoice).delta);
      if (typeof delta.content === "string") observePlaintext(stats, delta.content);
      if (typeof delta.refusal === "string") observePlaintext(stats, delta.refusal);
      if (typeof delta.reasoning_content === "string")
        observePlaintext(stats, delta.reasoning_content);
      else if (typeof delta.reasoning === "string") observePlaintext(stats, delta.reasoning);
      if (Array.isArray(delta.tool_calls)) {
        for (const rawCall of delta.tool_calls) {
          const fn = optionalObject(optionalObject(rawCall).function);
          if (typeof fn.name === "string") observePlaintext(stats, fn.name);
          if (typeof fn.arguments === "string") observePlaintext(stats, fn.arguments);
        }
      }
    }
    return;
  }
  if (protocol === "responses") {
    const type = typeof chunk.type === "string" ? chunk.type : "";
    if (
      type === "response.output_text.delta" ||
      type === "response.refusal.delta" ||
      type === "response.reasoning_summary_text.delta" ||
      type === "response.reasoning_text.delta" ||
      type === "response.function_call_arguments.delta"
    ) {
      if (typeof chunk.delta === "string") observePlaintext(stats, chunk.delta);
    } else if (type === "response.output_item.added") {
      const item = optionalObject(chunk.item);
      if (item.type === "function_call" && typeof item.name === "string") {
        observePlaintext(stats, item.name);
      }
    }
    return;
  }
  if (protocol === "messages") {
    const type = typeof chunk.type === "string" ? chunk.type : "";
    if (type === "content_block_start") {
      const block = optionalObject(chunk.content_block);
      if (typeof block.text === "string") observePlaintext(stats, block.text);
      if (typeof block.thinking === "string") observePlaintext(stats, block.thinking);
      if (block.type === "tool_use" && typeof block.name === "string") {
        observePlaintext(stats, block.name);
      }
    } else if (type === "content_block_delta") {
      const delta = optionalObject(chunk.delta);
      if (typeof delta.text === "string") observePlaintext(stats, delta.text);
      if (typeof delta.thinking === "string") observePlaintext(stats, delta.thinking);
      if (typeof delta.partial_json === "string") observePlaintext(stats, delta.partial_json);
    }
    return;
  }
  if (Array.isArray(chunk.candidates)) {
    for (const rawCandidate of chunk.candidates) {
      const parts = optionalObject(optionalObject(rawCandidate).content).parts;
      if (!Array.isArray(parts)) continue;
      for (const rawPart of parts) {
        const part = optionalObject(rawPart);
        if (typeof part.text === "string") observePlaintext(stats, part.text);
        const fnCall = part.functionCall ?? part.function_call;
        if (fnCall && typeof fnCall === "object") {
          const call = optionalObject(fnCall);
          if (typeof call.name === "string") observePlaintext(stats, call.name);
          if (call.args !== undefined) observePlaintext(stats, stringifyExact(call.args));
        }
      }
    }
  }
}

function estimateInputTokensFromBody(
  body: string | Uint8Array,
  fallbackChars: number | undefined,
): number {
  let asciiChars = 0;
  let nonAsciiChars = 0;
  if (typeof body === "string") {
    for (let i = 0; i < body.length; i++) {
      if (body.charCodeAt(i) <= 0x7f) asciiChars++;
      else nonAsciiChars++;
    }
  } else {
    for (let i = 0; i < body.byteLength; i++) {
      const b = body[i]!;
      if (b < 0x80) asciiChars++;
      else if (b >= 0xc0) nonAsciiChars++;
    }
  }
  if (asciiChars === 0 && nonAsciiChars === 0) {
    return Math.max(1, Math.ceil((fallbackChars ?? 1) / 4));
  }
  return estimateTokensFromPlaintext(asciiChars, nonAsciiChars);
}

function hasValidSettlementCacheWrites(usage: Usage, pricing: Pricing): boolean {
  const total = usage.cache_write_tokens;
  const write5m = usage.cache_write_5m_tokens;
  const write1h = usage.cache_write_1h_tokens;
  if (!cacheWritePartitionMatches(total, write5m, write1h)) return false;
  const cacheWritesReported = total !== undefined || write5m !== undefined || write1h !== undefined;
  const noCacheWrites = total === "0" && (write5m ?? "0") === "0" && (write1h ?? "0") === "0";
  const totalRate = pricing.rates.cache_write_tokens;
  const rate5m = pricing.rates.cache_write_5m_tokens;
  const rate1h = pricing.rates.cache_write_1h_tokens;
  const needsTotal = totalRate !== undefined && totalRate.numerator !== "0" && total === undefined;
  if (
    cacheWritesReported &&
    !noCacheWrites &&
    ((needsTotal && (write5m === undefined || write1h === undefined)) ||
      (rate5m && rate5m.numerator !== "0" && write5m === undefined) ||
      (rate1h && rate1h.numerator !== "0" && write1h === undefined))
  ) {
    return false;
  }
  return true;
}

function pricedSettlementCost(usage: Usage, pricing: Pricing): bigint {
  const totalRate = pricing.rates.cache_write_tokens;
  const needsTotal =
    totalRate !== undefined &&
    totalRate.numerator !== "0" &&
    usage.cache_write_tokens === undefined;
  const pricedUsage =
    needsTotal &&
    usage.cache_write_5m_tokens !== undefined &&
    usage.cache_write_1h_tokens !== undefined
      ? {
          ...usage,
          cache_write_tokens: (
            BigInt(usage.cache_write_5m_tokens) + BigInt(usage.cache_write_1h_tokens)
          ).toString(),
        }
      : usage;
  return BigInt(calculateCost(pricedUsage, pricing));
}

function minPositiveCeiling(
  customerPrice: PriceVersionRecord,
  supplierPrice: PriceVersionRecord,
  metric: "input_tokens" | "output_tokens",
  customerSnapshot?: PricingSnapshot,
  supplierSnapshot?: PricingSnapshot,
): number | undefined {
  let limit: number | undefined;
  const pairs: [PriceVersionRecord, PricingSnapshot | undefined][] = [
    [customerPrice, customerSnapshot],
    [supplierPrice, supplierSnapshot],
  ];
  for (const [price, snapshot] of pairs) {
    const basis = snapshot?.basis ?? price.basis;
    if (basis !== "PROVIDER_USAGE") continue;
    const raw = snapshot ? snapshot.effective_card.ceilings[metric] : price.ceilings[metric];
    if (raw === undefined) continue;
    const value = Number(raw);
    if (Number.isSafeInteger(value) && value >= 0) {
      limit = limit === undefined ? value : Math.min(limit, value);
    }
  }
  return limit;
}

export function buildEstimatedStreamUsage(input: {
  operation: Operation;
  customerPrice: PriceVersionRecord;
  supplierPrice: PriceVersionRecord;
  customerPricing: Pricing;
  supplierPricing: Pricing;
  customerSnapshot?: PricingSnapshot;
  supplierSnapshot?: PricingSnapshot;
  authorizedHostedTools?: ReadonlySet<HostedToolDimension> | null;
  observedUsage: Usage | undefined;
  preparedBody: string | Uint8Array;
  inputChars: number | undefined;
  outputStats: PlaintextStats;
  streamCompleted: boolean;
  heldCustomerAtoms: bigint;
  heldSupplierAtoms: bigint;
}): { usage: Usage; estimated: boolean } | null {
  const {
    operation,
    customerPrice,
    supplierPrice,
    customerPricing,
    supplierPricing,
    customerSnapshot,
    supplierSnapshot,
    authorizedHostedTools,
    observedUsage: rawObservedUsage,
    preparedBody,
    inputChars,
    outputStats,
    streamCompleted,
    heldCustomerAtoms,
    heldSupplierAtoms,
  } = input;
  const observedUsage = completeUnrequestedHostedToolUsage(
    rawObservedUsage,
    customerSnapshot,
    supplierSnapshot,
    authorizedHostedTools,
  );
  if (
    hasUnobservedNonTokenPricing(
      observedUsage,
      customerSnapshot,
      supplierSnapshot,
      authorizedHostedTools,
    )
  ) {
    return null;
  }
  const customerBasis = customerSnapshot?.basis ?? customerPrice.basis;
  const supplierBasis = supplierSnapshot?.basis ?? supplierPrice.basis;
  if (
    operation !== "chat" &&
    operation !== "responses" &&
    operation !== "messages" &&
    operation !== "gemini"
  ) {
    return null;
  }
  if (!streamCompleted) {
    if (outputStats.chunks === 0) return null;
    if (customerBasis !== "PROVIDER_USAGE" || supplierBasis !== "PROVIDER_USAGE") {
      return null;
    }
  }

  const estimateInput = observedUsage?.input_tokens === undefined;
  const estimateOutput = observedUsage?.output_tokens === undefined;
  const observedReasoning = estimateOutput ? Number(observedUsage?.reasoning_tokens ?? "0") : 0;
  if (!Number.isSafeInteger(observedReasoning) || observedReasoning < 0) return null;
  let estInput = estimateInput ? estimateInputTokensFromBody(preparedBody, inputChars) : 0;
  let estOutput = estimateOutput
    ? outputStats.chunks > 0
      ? estimateTokensFromPlaintext(outputStats.asciiChars, outputStats.nonAsciiChars)
      : 0
    : 0;
  estOutput = Math.max(estOutput, observedReasoning);

  if (estimateInput) {
    const ceiling = minPositiveCeiling(
      customerPrice,
      supplierPrice,
      "input_tokens",
      customerSnapshot,
      supplierSnapshot,
    );
    if (ceiling !== undefined && estInput > ceiling) estInput = ceiling;
  }
  if (estimateOutput) {
    const ceiling = minPositiveCeiling(
      customerPrice,
      supplierPrice,
      "output_tokens",
      customerSnapshot,
      supplierSnapshot,
    );
    if (ceiling !== undefined && estOutput > ceiling) estOutput = ceiling;
  }

  if (estOutput < observedReasoning) return null;
  const assemble = (inTokens: number, outTokens: number): Usage => ({
    ...observedUsage,
    ...(estimateInput ? { input_tokens: String(inTokens) } : {}),
    ...(estimateOutput ? { output_tokens: String(Math.max(observedReasoning, outTokens)) } : {}),
  });

  const fitsHold = (usage: Usage): boolean => {
    try {
      if (customerBasis === "PROVIDER_USAGE") {
        const custAtoms = customerSnapshot
          ? evaluateSnapshotAtoms(customerSnapshot, usage)
          : pricedSettlementCost(usage, customerPricing);
        if (custAtoms > heldCustomerAtoms) return false;
      }
      if (supplierBasis === "PROVIDER_USAGE") {
        const supAtoms = supplierSnapshot
          ? evaluateSnapshotAtoms(supplierSnapshot, usage)
          : pricedSettlementCost(usage, supplierPricing);
        if (supAtoms > heldSupplierAtoms) return false;
      }
      return true;
    } catch {
      return false;
    }
  };

  let candidate = assemble(estInput, estOutput);
  if (
    (!customerSnapshot &&
      customerBasis === "PROVIDER_USAGE" &&
      !hasValidSettlementCacheWrites(candidate, customerPricing)) ||
    (!supplierSnapshot &&
      supplierBasis === "PROVIDER_USAGE" &&
      !hasValidSettlementCacheWrites(candidate, supplierPricing))
  ) {
    return null;
  }
  if ((estimateInput || estimateOutput) && !fitsHold(candidate)) {
    let low = 0;
    let high = 10_000;
    while (low < high) {
      const mid = (low + high + 1) >> 1;
      const nextIn = estimateInput ? Math.floor((estInput * mid) / 10_000) : estInput;
      const nextOut = estimateOutput ? Math.floor((estOutput * mid) / 10_000) : estOutput;
      if (fitsHold(assemble(nextIn, nextOut))) low = mid;
      else high = mid - 1;
    }
    candidate = assemble(
      estimateInput ? Math.floor((estInput * low) / 10_000) : estInput,
      estimateOutput ? Math.floor((estOutput * low) / 10_000) : estOutput,
    );
    if (
      estimateOutput &&
      outputStats.chunks > 0 &&
      estOutput > 0 &&
      candidate.output_tokens === "0"
    ) {
      return null;
    }
  }
  if (!fitsHold(candidate)) return null;
  const built = buildUsageForOperation(
    operation,
    customerPrice,
    supplierPrice,
    candidate,
    undefined,
    undefined,
    customerSnapshot,
    supplierSnapshot,
    authorizedHostedTools,
  );
  if (!built) return null;
  return { usage: built, estimated: estimateInput || estimateOutput };
}
