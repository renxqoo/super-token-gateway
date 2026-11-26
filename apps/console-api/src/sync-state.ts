import type { SQL } from "bun";
import { createConsoleDrizzle, type ConsoleDrizzle } from "./db/schema.ts";
import { GatewayError } from "@gateway/contracts";

export const LOCK_NS = {
  billingAccount: 88421101,
  apiKey: 88421102,
  account: 88421103,
  channel: 88421104,
  template: 88421105,
  model: 88421106,
  route: 88421107,
  override: 88421108,
  organization: 88421109,
} as const;

export function errMsg(err: unknown): string {
  return err instanceof Error ? err.message.slice(0, 500) : String(err).slice(0, 500);
}

export function isStaleRevisionError(err: unknown): boolean {
  return err instanceof GatewayError && err.status === 409 && err.code === "stale_revision";
}

export function createEntityLocker(db: SQL) {
  async function withEntityLock<T>(
    namespace: number,
    entityKey: string,
    fn: (orm: ConsoleDrizzle) => Promise<T>,
  ): Promise<T> {
    const conn = await db.reserve();
    let locked = false;
    try {
      await conn`SELECT pg_advisory_lock(${namespace}::int, hashtext(${entityKey}))`;
      locked = true;
      return await fn(createConsoleDrizzle(conn));
    } finally {
      if (locked) {
        await conn`SELECT pg_advisory_unlock(${namespace}::int, hashtext(${entityKey}))`.catch(
          () => undefined,
        );
      }
      conn.release();
    }
  }
  return withEntityLock;
}

export type EntityLocker = ReturnType<typeof createEntityLocker>;
