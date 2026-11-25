import { eq, sql } from "drizzle-orm";
import { outboxConsumers, outboxConsumerShards, type LedgerDrizzle } from "./schema.ts";
import type { OutboxEventRecord } from "./types.ts";
import { OUTBOX_SHARDS, parseAtoms } from "./funds.ts";

export function createOutbox(orm: LedgerDrizzle) {
  return {
    async claimOutboxEvents(
      consumerName: string,
      limit = 100,
    ): Promise<{ lease_token: string; events: OutboxEventRecord[] }> {
      const leaseToken = crypto.randomUUID();
      return orm.transaction(async (tx) => {
        const lockConsumer = () =>
          tx
            .select({ name: outboxConsumers.name })
            .from(outboxConsumers)
            .where(eq(outboxConsumers.name, consumerName))
            .for("update");
        const [consumer] = await lockConsumer();
        if (!consumer) {
          await tx
            .insert(outboxConsumers)
            .values({ name: consumerName })
            .onConflictDoNothing({ target: outboxConsumers.name });
          await lockConsumer();
        }
        const readShards = () =>
          tx
            .select({
              leaseActive: sql<boolean>`${outboxConsumerShards.leaseToken} IS NOT NULL
                AND ${outboxConsumerShards.leasedUntil} > clock_timestamp()`,
            })
            .from(outboxConsumerShards)
            .where(eq(outboxConsumerShards.name, consumerName));
        let shardRows = await readShards();
        if (shardRows.length < OUTBOX_SHARDS) {
          await tx
            .insert(outboxConsumerShards)
            .values(
              Array.from({ length: OUTBOX_SHARDS }, (_, shard) => ({ name: consumerName, shard })),
            )
            .onConflictDoNothing();
          shardRows = await readShards();
        }

        if (shardRows.some((row) => row.leaseActive)) {
          return { lease_token: leaseToken, events: [] };
        }

        const evRows = (await tx.execute(sql`
          SELECT o.seq, o.event_id, o.kind, o.request_id, o.billing_account_id, o.shard, o.payload, o.created_at
          FROM public.outbox_consumer_shards cs
          CROSS JOIN LATERAL (
            SELECT e.seq, e.event_id, e.kind, e.request_id, e.billing_account_id, e.shard, e.payload, e.created_at
            FROM public.outbox_events e
            WHERE e.shard = cs.shard AND e.seq > cs.acked_seq
            ORDER BY e.seq ASC
            LIMIT ${limit}
          ) o
          WHERE cs.name = ${consumerName}
          ORDER BY o.seq ASC
          LIMIT ${limit}
        `)) as unknown as {
          seq: string | bigint;
          event_id: string;
          kind: string;
          request_id: string | null;
          billing_account_id: string | null;
          shard: number;
          payload: Record<string, unknown>;
          created_at: string | Date;
        }[];

        if (evRows.length > 0) {
          const maxByShard = new Map<number, string>();
          for (const ev of evRows) {
            maxByShard.set(Number(ev.shard), String(ev.seq));
          }
          const leaseUpdates = JSON.stringify(
            [...maxByShard].map(([shard, leased_through_seq]) => ({ shard, leased_through_seq })),
          );
          await tx.execute(sql`
            UPDATE public.outbox_consumer_shards cs
            SET lease_token = ${leaseToken}::uuid,
                leased_through_seq = u.leased_through_seq,
                leased_until = clock_timestamp() + interval '30 seconds',
                updated_at = now()
            FROM jsonb_to_recordset(${leaseUpdates}::text::jsonb) AS u(shard smallint, leased_through_seq bigint)
            WHERE cs.name = ${consumerName} AND cs.shard = u.shard
          `);
        }

        return {
          lease_token: leaseToken,
          events: evRows.map((row) => ({
            seq: String(row.seq),
            event_id: row.event_id,
            kind: row.kind,
            request_id: row.request_id,
            billing_account_id: row.billing_account_id,
            payload: row.payload,
            created_at: new Date(row.created_at).toISOString(),
          })),
        };
      });
    },

    async ackOutboxEvents(
      consumerName: string,
      leaseToken: string,
      throughSeq: string,
    ): Promise<boolean> {
      const seq = parseAtoms(throughSeq);
      if (seq === null || seq > 9223372036854775807n) return false;
      return orm.transaction(async (tx) => {
        const [consumer] = await tx
          .select({ name: outboxConsumers.name })
          .from(outboxConsumers)
          .where(eq(outboxConsumers.name, consumerName))
          .for("update");
        if (!consumer) return false;

        const rows = (await tx.execute(sql`
          SELECT shard, acked_seq::text AS acked_seq, leased_through_seq::text AS leased_through_seq
          FROM public.outbox_consumer_shards
          WHERE name = ${consumerName}
            AND lease_token = ${leaseToken}::uuid
            AND leased_until > clock_timestamp()
        `)) as unknown as {
          shard: number;
          acked_seq: string;
          leased_through_seq: string | null;
        }[];
        if (rows.length === 0) return false;
        let maxLeased = -1n;
        let minClaimedAck = -1n;
        for (const r of rows) {
          const acked = BigInt(r.acked_seq);
          const leased = r.leased_through_seq !== null ? BigInt(r.leased_through_seq) : acked;
          if (leased > acked) {
            if (leased > maxLeased) maxLeased = leased;
            if (minClaimedAck === -1n || acked < minClaimedAck) minClaimedAck = acked;
          }
        }
        if (maxLeased < 0n || seq > maxLeased || seq < minClaimedAck) return false;

        await tx.execute(sql`
          UPDATE public.outbox_consumer_shards
          SET acked_seq = LEAST(COALESCE(leased_through_seq, acked_seq), GREATEST(acked_seq, ${seq.toString()}::bigint)),
              lease_token = NULL,
              leased_through_seq = NULL,
              leased_until = NULL,
              updated_at = now()
          WHERE name = ${consumerName} AND lease_token = ${leaseToken}::uuid
        `);
        return true;
      });
    },
  };
}
