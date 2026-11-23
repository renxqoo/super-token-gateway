import type { Operation, Pricing } from "@gateway/kernel";
import type {
  ApiKeyRecord,
  ChannelRecord,
  ConfigSnapshot,
  PriceVersionRecord,
  RouteRecord,
  UpstreamAccountRecord,
} from "@gateway/ledger";
import { quoteReservation, type Usage } from "@gateway/kernel";
import { verifier } from "@gateway/runtime";

const UUID_POOL = Buffer.allocUnsafe(4096);
let uuidHex = "";
let uuidHexOffset = 8192;

export function fastUuid(): string {
  if (uuidHexOffset + 32 > 8192) {
    crypto.getRandomValues(UUID_POOL);
    for (let i = 0; i < 4096; i += 16) {
      UUID_POOL[i + 6] = (UUID_POOL[i + 6]! & 0x0f) | 0x40;
      UUID_POOL[i + 8] = (UUID_POOL[i + 8]! & 0x3f) | 0x80;
    }
    uuidHex = UUID_POOL.toString("hex");
    uuidHexOffset = 0;
  }
  const o = uuidHexOffset;
  uuidHexOffset += 32;
  return `${uuidHex.slice(o, o + 8)}-${uuidHex.slice(o + 8, o + 12)}-${uuidHex.slice(o + 12, o + 16)}-${uuidHex.slice(o + 16, o + 20)}-${uuidHex.slice(o + 20, o + 32)}`;
}

export class LruMap<K, V> {
  private readonly map = new Map<K, V>();

  constructor(private readonly maxEntries: number) {}

  get size(): number {
    return this.map.size;
  }

  get(key: K): V | undefined {
    const val = this.map.get(key);
    if (val === undefined) return undefined;
    this.map.delete(key);
    this.map.set(key, val);
    return val;
  }

  set(key: K, value: V): void {
    if (this.map.has(key)) {
      this.map.delete(key);
    } else if (this.map.size >= this.maxEntries) {
      const oldest = this.map.keys().next().value;
      if (oldest !== undefined) this.map.delete(oldest);
    }
    this.map.set(key, value);
  }

  clear(): void {
    this.map.clear();
  }
}

type CoarseDeadline = {
  deadlineMs: number;
  fired: boolean;
  cancelled: boolean;
  onExpire: () => void;
};

export class DeadlineWheel {
  private readonly coarse = new Set<CoarseDeadline>();
  private readonly exact = new Set<ReturnType<typeof setTimeout>>();
  private timer: ReturnType<typeof setInterval> | null = null;
  private closed = false;

  private ensureTimer(): void {
    if (this.timer || this.closed) return;
    this.timer = setInterval(() => {
      if (this.coarse.size === 0) return;
      const now = performance.now();
      for (const entry of this.coarse) {
        if (entry.cancelled || entry.fired) {
          this.coarse.delete(entry);
          continue;
        }
        if (now >= entry.deadlineMs) {
          entry.fired = true;
          this.coarse.delete(entry);
          entry.onExpire();
        }
      }
    }, 100);
    this.timer.unref();
  }

  schedule(timeoutMs: number, onExpire: () => void): () => void {
    if (this.closed) return () => {};
    if (timeoutMs < 1000) {
      let done = false;
      const handle = setTimeout(() => {
        if (done) return;
        done = true;
        this.exact.delete(handle);
        onExpire();
      }, timeoutMs);
      handle.unref();
      this.exact.add(handle);
      return () => {
        if (done) return;
        done = true;
        this.exact.delete(handle);
        clearTimeout(handle);
      };
    }
    const entry: CoarseDeadline = {
      deadlineMs: performance.now() + timeoutMs,
      fired: false,
      cancelled: false,
      onExpire,
    };
    this.coarse.add(entry);
    this.ensureTimer();
    return () => {
      if (entry.cancelled) return;
      entry.cancelled = true;
      this.coarse.delete(entry);
    };
  }

  close(): void {
    this.closed = true;
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
    this.coarse.clear();
    for (const handle of this.exact) clearTimeout(handle);
    this.exact.clear();
  }
}

export type HydratedCandidate = {
  id: string;
  channel_id: string;
  account_id: string;
  priority: number;
  weight: number;
  operations: readonly Operation[];
  enabled: boolean;
  channel_enabled: boolean;
  account_active: boolean;
  same_protocol: boolean;
  route: RouteRecord;
  channel: ChannelRecord;
  account: UpstreamAccountRecord;
  billingPrice: PriceVersionRecord;
  costPrice: PriceVersionRecord;
};

export class ProxyRuntimeState {
  readonly deadlines = new DeadlineWheel();
  private readonly verifiedKeyCache: LruMap<string, ApiKeyRecord>;
  private readonly quoteCache: LruMap<string, string>;
  private authCacheEpoch = -1;
  private authCachePepper = "";
  private quoteCacheEpoch = -1;
  private readonly localProjectInFlight = new Map<string, number>();
  private readonly hydratedBySnapshot = new WeakMap<
    ConfigSnapshot,
    Map<string, HydratedCandidate[]>
  >();
  private readonly pricingWrappers = new WeakMap<PriceVersionRecord, Pricing>();
  private expiresBucket = -1;
  private readonly expiresByTtl = new Map<number, string>();

  constructor(cacheSize = 4096) {
    this.verifiedKeyCache = new LruMap<string, ApiKeyRecord>(cacheSize);
    this.quoteCache = new LruMap<string, string>(cacheSize);
  }

  lookupVerifiedKey(
    rawKey: string,
    snap: ConfigSnapshot,
    keyPepper: string,
  ): ApiKeyRecord | undefined {
    if (this.authCacheEpoch !== snap.epoch || this.authCachePepper !== keyPepper) {
      this.authCacheEpoch = snap.epoch;
      this.authCachePepper = keyPepper;
      this.verifiedKeyCache.clear();
    }
    const hit = this.verifiedKeyCache.get(rawKey);
    if (hit !== undefined) return hit;
    const digest = verifier(rawKey, keyPepper);
    const key = snap.keysByVerifier.get(digest);
    if (key) {
      this.verifiedKeyCache.set(rawKey, key);
    }
    return key;
  }

  quoteForPrice(
    snapEpoch: number,
    price: PriceVersionRecord,
    measurement: { inputCharacters?: string | undefined; images?: unknown },
    outputBound: number,
  ): string {
    if (this.quoteCacheEpoch !== snapEpoch) {
      this.quoteCacheEpoch = snapEpoch;
      this.quoteCache.clear();
    }
    const cacheable = price.basis === "PROVIDER_USAGE" && measurement.images === undefined;
    if (cacheable) {
      const ck = `${price.id}:${outputBound}`;
      const hit = this.quoteCache.get(ck);
      if (hit !== undefined) return hit;
      const quote = quoteReservation(
        [
          {
            provider_id: price.id,
            id: price.id,
            currency: price.currency,
            rates: price.rates,
            ceilings: price.ceilings as Usage,
            basis: price.basis,
          },
        ],
        price.currency,
        measurement,
        outputBound,
      );
      this.quoteCache.set(ck, quote.amount_atoms);
      return quote.amount_atoms;
    }
    const quote = quoteReservation(
      [
        {
          provider_id: price.id,
          id: price.id,
          currency: price.currency,
          rates: price.rates,
          ceilings: price.ceilings as Usage,
          basis: price.basis,
        },
      ],
      price.currency,
      measurement,
      outputBound,
    );
    return quote.amount_atoms;
  }

  tryEnterProject(projectId: string, concurrency: number): (() => void) | null {
    const cur = this.localProjectInFlight.get(projectId) ?? 0;
    if (cur >= concurrency) return null;
    this.localProjectInFlight.set(projectId, cur + 1);
    let released = false;
    return () => {
      if (released) return;
      released = true;
      const next = (this.localProjectInFlight.get(projectId) ?? 1) - 1;
      if (next <= 0) this.localProjectInFlight.delete(projectId);
      else this.localProjectInFlight.set(projectId, next);
    };
  }

  projectInFlight(projectId: string): number {
    return this.localProjectInFlight.get(projectId) ?? 0;
  }

  getHydratedCandidates(
    snap: ConfigSnapshot,
    modelName: string,
    operation: Operation,
    currency: string,
    build: () => HydratedCandidate[],
  ): HydratedCandidate[] {
    let byKey = this.hydratedBySnapshot.get(snap);
    if (!byKey) {
      byKey = new Map();
      this.hydratedBySnapshot.set(snap, byKey);
    }
    const key = `${modelName}\0${operation}\0${currency}`;
    let list = byKey.get(key);
    if (!list) {
      list = build();
      byKey.set(key, list);
    }
    return list;
  }

  pricingOf(version: PriceVersionRecord): Pricing {
    let cached = this.pricingWrappers.get(version);
    if (!cached) {
      cached = { currency: version.currency, rates: version.rates };
      this.pricingWrappers.set(version, cached);
    }
    return cached;
  }

  cachedExpiresAt(ttlMs: number): string {
    const bucket = Math.floor(Date.now() / 100);
    if (bucket !== this.expiresBucket) {
      this.expiresBucket = bucket;
      this.expiresByTtl.clear();
    }
    let iso = this.expiresByTtl.get(ttlMs);
    if (!iso) {
      iso = new Date((bucket + 1) * 100 + ttlMs).toISOString();
      this.expiresByTtl.set(ttlMs, iso);
    }
    return iso;
  }

  close(): void {
    this.deadlines.close();
    this.verifiedKeyCache.clear();
    this.quoteCache.clear();
    this.localProjectInFlight.clear();
  }
}
