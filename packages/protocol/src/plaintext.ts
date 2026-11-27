export type PlaintextStats = {
  asciiChars: number;
  nonAsciiChars: number;
  chunks: number;
};

export function observePlaintext(stats: PlaintextStats, value: string): void {
  if (!value.length) return;
  stats.chunks++;
  for (let i = 0; i < value.length; i++) {
    if (value.charCodeAt(i) <= 0x7f) stats.asciiChars++;
    else stats.nonAsciiChars++;
  }
}

export function estimateTokensFromPlaintext(asciiChars: number, nonAsciiChars: number): number {
  return Math.max(1, Math.ceil(asciiChars / 4 + nonAsciiChars));
}
