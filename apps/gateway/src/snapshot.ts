import type { ChannelRecord, ConfigSnapshot, createCatalogRepository } from "@gateway/ledger";
import { decrypt } from "@gateway/runtime";

type CatalogRepo = ReturnType<typeof createCatalogRepository>;

export class SnapshotManager {
  private current: ConfigSnapshot | null = null;
  private timer: ReturnType<typeof setInterval> | null = null;
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
        this.current = snap;
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
    if (this.timer || this.pollMs <= 0) return;
    this.timer = setInterval(() => {
      void this.pollEpoch();
    }, this.pollMs);
  }

  private async pollEpoch(): Promise<void> {
    try {
      const epoch = await this.catalog.getEpoch();
      if (!this.current || epoch > this.current.epoch) {
        await this.load();
      }
    } catch {
      void 0;
    }
  }

  stop(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }
}
