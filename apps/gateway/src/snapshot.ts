import type { ChannelRecord, ConfigSnapshot, createCatalogRepository } from "@gateway/ledger";
import { decrypt } from "@gateway/runtime";

type CatalogRepo = ReturnType<typeof createCatalogRepository>;

export class SnapshotManager {
  private current: ConfigSnapshot | null = null;
  private timer: ReturnType<typeof setInterval> | null = null;
  private stopped = false;
  private lastVerifiedAt = 0;
  private pollInFlight: Promise<void> | null = null;
  private refreshing: Promise<ConfigSnapshot> | null = null;
  private readonly secretCache = new Map<string, { revision: number; secret: string }>();

  constructor(
    private readonly catalog: CatalogRepo,
    private readonly encryptionKey: string,
    private readonly pollMs = 2000,
  ) {}

  async load(): Promise<ConfigSnapshot> {
    if (this.refreshing) return this.refreshing;
    this.refreshing = this.catalog
      .loadSnapshot()
      .then((snap) => {
        if (this.current && snap.epoch < this.current.epoch) {
          throw new Error("Configuration epoch moved backwards");
        }
        this.current = snap;
        this.lastVerifiedAt = performance.now();
        for (const id of this.secretCache.keys()) {
          const ch = snap.channels.get(id);
          if (!ch || ch.config_revision !== this.secretCache.get(id)?.revision) {
            this.secretCache.delete(id);
          }
        }
        return snap;
      })
      .finally(() => {
        this.refreshing = null;
      });
    return this.refreshing;
  }

  async refreshTo(epoch: number): Promise<void> {
    if (this.stopped || (this.current && this.current.epoch >= epoch)) return;
    const inProgress = this.refreshing !== null;
    const snap = await this.load();
    if (!this.stopped && inProgress && snap.epoch < epoch) await this.load();
  }

  isFresh(): boolean {
    return (
      this.isLoaded() &&
      (this.pollMs <= 0 ||
        performance.now() - this.lastVerifiedAt <= Math.max(5000, 3 * this.pollMs))
    );
  }

  verifiedAtSeconds(): number {
    return this.current ? (performance.timeOrigin + this.lastVerifiedAt) / 1000 : 0;
  }

  get(): ConfigSnapshot {
    if (!this.current) throw new Error("ConfigSnapshot has not been loaded");
    return this.current;
  }

  isLoaded(): boolean {
    return this.current !== null;
  }

  decryptChannelSecret(channel: ChannelRecord): string {
    const cached = this.secretCache.get(channel.id);
    if (cached && cached.revision === channel.config_revision) {
      return cached.secret;
    }
    const secret = decrypt(channel.secret_ciphertext, this.encryptionKey, channel.id);
    this.secretCache.set(channel.id, { revision: channel.config_revision, secret });
    return secret;
  }

  start(): void {
    this.stopped = false;
    if (this.timer || this.pollMs <= 0) return;
    this.timer = setInterval(() => {
      if (this.pollInFlight) return;
      this.pollInFlight = this.pollEpoch().finally(() => {
        this.pollInFlight = null;
      });
    }, this.pollMs);
  }

  private async pollEpoch(): Promise<void> {
    try {
      const epoch = await this.catalog.getEpoch();
      if (!this.current || epoch > this.current.epoch) {
        await this.refreshTo(epoch);
      } else if (epoch === this.current.epoch) {
        this.lastVerifiedAt = performance.now();
      }
    } catch {
      void 0;
    }
  }

  async stop(): Promise<void> {
    this.stopped = true;
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
    await Promise.allSettled([this.pollInFlight, this.refreshing]);
  }
}
