const bucketMs = 10_000;
const bucketCount = 6;
const minAttempts = 12;

type Bucket = {
  attempts: number;
  complete: number;
  timed: number;
  onTime: number;
  outputs: number;
  outputTokens: number;
};

type Summary = Bucket & { latestEpoch: number };

type Series = {
  buckets: Map<number, Bucket>;
  summary?: Summary;
  healthTier?: number;
  latencyTier?: number;
};

const healthWeights = [1, 0.7, 0.4, 0.2] as const;
const latencyWeights = [1, 0.7, 0.4] as const;

function advanceTier(current: number | undefined, target: number): number {
  if (current === undefined || current === target) return target;
  return current + Math.sign(target - current);
}

export function routingLatencySample(
  elapsedMs: number,
  localWaitMs: number,
  headersMs: number,
  targetMs: number,
): boolean | undefined {
  if (elapsedMs <= targetMs) return true;
  if (Math.max(headersMs, elapsedMs - localWaitMs) > targetMs) return false;
  return undefined;
}

function refreshSeries(series: Series, epoch: number) {
  const summary: Summary = {
    latestEpoch: -1,
    attempts: 0,
    complete: 0,
    timed: 0,
    onTime: 0,
    outputs: 0,
    outputTokens: 0,
  };
  for (const [start, bucket] of series.buckets) {
    if (start <= epoch - bucketCount) {
      series.buckets.delete(start);
      continue;
    }
    summary.latestEpoch = Math.max(summary.latestEpoch, start);
    summary.attempts += bucket.attempts;
    summary.complete += bucket.complete;
    summary.timed += bucket.timed;
    summary.onTime += bucket.onTime;
    summary.outputs += bucket.outputs;
    summary.outputTokens += bucket.outputTokens;
  }
  series.summary = summary;
  if (summary.attempts < minAttempts) {
    delete series.healthTier;
    delete series.latencyTier;
    return;
  }
  const completeRate = summary.complete / summary.attempts;
  const onTimeRate = summary.timed >= 4 ? summary.onTime / summary.timed : 1;
  series.healthTier = advanceTier(
    series.healthTier,
    completeRate >= 0.98 ? 0 : completeRate >= 0.9 ? 1 : completeRate >= 0.75 ? 2 : 3,
  );
  series.latencyTier = advanceTier(
    series.latencyTier,
    onTimeRate >= 0.95 ? 0 : onTimeRate >= 0.8 ? 1 : 2,
  );
}

function writableBucket(
  map: Map<string, Series>,
  key: string,
  epoch: number,
  limit: number,
): { series: Series; bucket: Bucket } {
  let series = map.get(key);
  if (series) map.delete(key);
  else {
    if (map.size === limit) map.delete(map.keys().next().value!);
    series = { buckets: new Map() };
  }
  map.set(key, series);
  let bucket = series.buckets.get(epoch);
  if (!bucket) {
    for (const start of series.buckets.keys()) {
      if (start <= epoch - bucketCount) series.buckets.delete(start);
    }
    bucket = { attempts: 0, complete: 0, timed: 0, onTime: 0, outputs: 0, outputTokens: 0 };
    series.buckets.set(epoch, bucket);
  }
  return { series, bucket };
}

export class AdaptiveRoutingStats {
  private readonly routes = new Map<string, Series>();
  private readonly models = new Map<string, Series>();
  private timer: ReturnType<typeof setInterval> | undefined;
  private closed = false;

  constructor(private readonly now = () => performance.now()) {}

  observe(input: {
    routeKey: string;
    modelKey: string;
    complete: boolean;
    onTime?: boolean;
    outputTokens?: number;
  }) {
    if (this.closed) return;
    const epoch = this.epoch();
    const { series, bucket } = writableBucket(this.routes, input.routeKey, epoch, 256);
    bucket.attempts++;
    if (input.complete) bucket.complete++;
    if (input.onTime !== undefined) {
      bucket.timed++;
      if (input.onTime) bucket.onTime++;
    }
    if (series.healthTier === undefined && bucket.attempts >= minAttempts)
      refreshSeries(series, epoch);

    if (input.complete && input.outputTokens !== undefined) {
      const output = writableBucket(this.models, input.modelKey, epoch, 32);
      output.bucket.outputs++;
      output.bucket.outputTokens += input.outputTokens;
      if (!output.series.summary?.outputs) refreshSeries(output.series, epoch);
    }
    if (!this.timer) {
      this.timer = setInterval(() => this.refresh(), 1000);
      this.timer.unref();
    }
  }

  quality(routeKey: string): number | undefined {
    const series = this.routes.get(routeKey);
    if (
      !series?.summary ||
      series.healthTier === undefined ||
      series.latencyTier === undefined ||
      series.summary.latestEpoch <= this.epoch() - bucketCount ||
      (series.summary.complete >= 4 && series.summary.timed < 4)
    ) {
      return undefined;
    }
    return healthWeights[series.healthTier]! * latencyWeights[series.latencyTier]!;
  }

  outputEstimate(modelKey: string, bound: number): number | undefined {
    const summary = this.models.get(modelKey)?.summary;
    if (!summary?.outputs || summary.latestEpoch <= this.epoch() - bucketCount) return undefined;
    return Math.min(bound, Math.round(summary.outputTokens / summary.outputs));
  }

  close(): void {
    this.closed = true;
    if (this.timer) clearInterval(this.timer);
    this.timer = undefined;
    this.routes.clear();
    this.models.clear();
  }

  private epoch(): number {
    return Math.floor(this.now() / bucketMs);
  }

  private refresh(): void {
    const epoch = this.epoch();
    for (const map of [this.routes, this.models]) {
      for (const [key, series] of map) {
        refreshSeries(series, epoch);
        if (!series.buckets.size) map.delete(key);
      }
    }
    if (!this.routes.size && !this.models.size && this.timer) {
      clearInterval(this.timer);
      this.timer = undefined;
    }
  }
}
