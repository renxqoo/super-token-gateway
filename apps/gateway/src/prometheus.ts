const DEFAULT_BUCKETS_MS = [5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 15000];
export const STREAM_BUCKETS_SECONDS = [
  0.0001, 0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 20, 30, 60, 120,
] as const;

export class Histogram {
  private readonly counts: number[];
  private sum = 0;
  private count = 0;

  constructor(private readonly buckets: readonly number[] = DEFAULT_BUCKETS_MS) {
    this.counts = Array.from({ length: buckets.length }, () => 0);
  }

  observe(value: number): void {
    const v = Number.isFinite(value) && value >= 0 ? value : 0;
    this.sum += v;
    this.count++;
    const buckets = this.buckets;
    for (let i = 0; i < buckets.length; i++) {
      if (v <= buckets[i]!) {
        this.counts[i]!++;
        return;
      }
    }
  }

  render(name: string, help: string, labels = ""): string[] {
    const lines = [`# HELP ${name} ${help}`, `# TYPE ${name} histogram`];
    let cumulative = 0;
    for (let i = 0; i < this.buckets.length; i++) {
      cumulative += this.counts[i]!;
      lines.push(
        `${name}_bucket{${labels ? labels + "," : ""}le="${this.buckets[i]}"} ${cumulative}`,
      );
    }
    lines.push(`${name}_bucket{${labels ? labels + "," : ""}le="+Inf"} ${this.count}`);
    const series = labels ? `{${labels}}` : "";
    lines.push(`${name}_sum${series} ${this.sum.toFixed(labels ? 9 : 3)}`);
    lines.push(`${name}_count${series} ${this.count}`);
    return lines;
  }
}

export function escapeLabel(value: string): string {
  return value
    .replaceAll("\\", "\\\\")
    .replaceAll('"', '\\"')
    .replace(/\r\n|[\r\n\u2028\u2029]/g, " ");
}

export type LabeledCounter<T extends readonly string[]> = {
  labels: T;
  count: number;
};

export function bumpLabeled<T extends readonly string[]>(
  map: Map<string, LabeledCounter<T>>,
  key: string,
  labels: () => T,
  maxEntries = 2048,
): boolean {
  const existing = map.get(key);
  if (existing !== undefined) {
    existing.count++;
    return true;
  }
  if (map.size >= maxEntries) return false;
  map.set(key, { labels: labels(), count: 1 });
  return true;
}
