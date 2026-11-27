import { GatewayError, optionalObject } from "@gateway/contracts";
import { hasPricedTokenUsage, type Operation, type Usage } from "@gateway/kernel";
import type { PriceVersionRecord } from "@gateway/ledger";

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

export function buildUsageForOperation(
  operation: Operation,
  customerPrice: PriceVersionRecord,
  supplierPrice: PriceVersionRecord,
  rawUsage: Usage | undefined,
  deliveredImages?: number,
  authorizedImages?: number,
): Usage | null {
  if (
    rawUsage &&
    rawUsage.output_tokens === undefined &&
    (operation === "embeddings" || operation === "moderations")
  ) {
    rawUsage = { ...rawUsage, output_tokens: "0" };
  }

  const customerUsesProviderUsage = customerPrice.basis === "PROVIDER_USAGE";
  const supplierUsesProviderUsage = supplierPrice.basis === "PROVIDER_USAGE";
  if (
    (customerUsesProviderUsage && !hasPricedTokenUsage(rawUsage ?? {}, customerPrice.rates)) ||
    (supplierUsesProviderUsage && !hasPricedTokenUsage(rawUsage ?? {}, supplierPrice.rates))
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
      ((customerUsesProviderUsage && customerPrice.rates.images) ||
        (supplierUsesProviderUsage && supplierPrice.rates.images))
    )
      return null;
    if (quantity !== undefined) return { ...rawUsage, images: quantity };
  }
  if (rawUsage) return rawUsage;
  if (customerUsesProviderUsage || supplierUsesProviderUsage) return null;
  return { requests: "1" };
}

export function buildModerationUsageFallback(
  observed: Usage | undefined,
  inputChars: number,
  outputChars: number,
): Usage {
  return {
    ...observed,
    input_tokens: observed?.input_tokens ?? String(Math.max(1, Math.ceil(inputChars / 4))),
    output_tokens: observed?.output_tokens ?? String(Math.max(1, Math.ceil(outputChars / 4))),
  };
}
