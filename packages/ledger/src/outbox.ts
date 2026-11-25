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
            .select({
              ackedSeq: outboxConsumers.ackedSeq,
              leaseToken: outboxConsumers.leaseToken,
              leasedThroughSeq: outboxConsumers.leasedThroughSeq,
              leasedUntil: outboxConsumers.leasedUntil,
            })
            .from(outboxConsumers)
            .where(eq(outboxConsumers.name, consumerName))
            .for("update");
        let [legacy] = await lockConsumer();
        if (!legacy) {
          await tx
            .insert(outboxConsumers)
            .values({ name: consumerName, ackedSeq: 0n })
            .onConflictDoNothing({ target: outboxConsumers.name });
          [legacy] = await lockConsumer();
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
              Array.from({ length: OUTBOX_SHARDS }, (_, shard) => ({
                name: consumerName,
                shard,
                ackedSeq: legacy?.ackedSeq ?? 0n,
                leaseToken: legacy?.leaseToken ?? null,
                leasedThroughSeq: legacy?.leasedThroughSeq ?? null,
                leasedUntil: legacy?.leasedUntil ?? null,
              })),
            )
            .onConflictDoNothing();
          shardRows = await readShards();
        }

        if (shardRows.some((row) => row.leaseActive)) {
          return { lease_token: leaseToken, events: [] };
        }

        const evRows = (await tx.execute(sql`
          SELECT o.seq, o.event_id, o.kind, o.request_id, o.project_id, o.shard, o.payload, o.created_at
          FROM public.outbox_consumer_shards cs
          CROSS JOIN LATERAL (
            SELECT e.seq, e.event_id, e.kind, e.request_id, e.project_id, e.shard, e.payload, e.created_at
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
          project_id: string | null;
          shard: number;
          payload: Record<string, unknown>;
          created_at: string | Date;
        }[];

        const last = evRows.at(-1);
        if (last) {
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
          await tx
            .update(outboxConsumers)
            .set({
              leaseToken,
              leasedThroughSeq: BigInt(String(last.seq)),
              leasedUntil: sql`clock_timestamp() + interval '30 seconds'`,
              updatedAt: sql`now()`,
            })
            .where(eq(outboxConsumers.name, consumerName));
        }

        return {
          lease_token: leaseToken,
          events: evRows.map((r) => ({
            seq: String(r.seq),
            event_id: String(r.event_id),
            kind: String(r.kind),
            request_id: r.request_id ? String(r.request_id) : null,
            project_id: r.project_id ? String(r.project_id) : null,
            payload: r.payload,
            created_at: new Date(r.created_at).toISOString(),
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
        const [legacy] = await tx
          .select({
            ackedSeq: outboxConsumers.ackedSeq,
            leaseToken: outboxConsumers.leaseToken,
            leasedThroughSeq: outboxConsumers.leasedThroughSeq,
            leasedUntil: outboxConsumers.leasedUntil,
            leaseActive: sql<boolean>`${outboxConsumers.leaseToken} IS NOT NULL
              AND ${outboxConsumers.leasedUntil} > clock_timestamp()`,
          })
          .from(outboxConsumers)
          .where(eq(outboxConsumers.name, consumerName))
          .for("update");
        if (!legacy) return false;

        let rows = (await tx.execute(sql`
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
        if (rows.length === 0 && legacy.leaseActive && legacy.leaseToken === leaseToken) {
          await tx
            .insert(outboxConsumerShards)
            .values(
              Array.from({ length: OUTBOX_SHARDS }, (_, shard) => ({
                name: consumerName,
                shard,
                ackedSeq: legacy.ackedSeq,
                leaseToken: legacy.leaseToken,
                leasedThroughSeq: legacy.leasedThroughSeq,
                leasedUntil: legacy.leasedUntil,
              })),
            )
            .onConflictDoNothing();
          rows = (await tx.execute(sql`
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
        }
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
        await tx.execute(sql`
          UPDATE public.outbox_consumers
          SET acked_seq = COALESCE(
                (SELECT min(acked_seq) FROM public.outbox_consumer_shards WHERE name = ${consumerName}),
                acked_seq
              ),
              lease_token = NULL,
              leased_through_seq = NULL,
              leased_until = NULL,
              updated_at = now()
          WHERE name = ${consumerName}
        `);
        return true;
      });
    },
  };
}
