import { and, asc, eq, gt, lte, sql } from "drizzle-orm";
import { outboxConsumers, outboxEvents, type LedgerDrizzle } from "./schema.ts";
import type { OutboxEventRecord } from "./types.ts";
import { parseAtoms } from "./funds.ts";

export function createOutbox(orm: LedgerDrizzle) {
  return {
    async claimOutboxEvents(
      consumerName: string,
      limit = 100,
    ): Promise<{ lease_token: string; events: OutboxEventRecord[] }> {
      const leaseToken = crypto.randomUUID();
      return orm.transaction(async (tx) => {
        await tx
          .insert(outboxConsumers)
          .values({ name: consumerName, ackedSeq: 0n })
          .onConflictDoNothing({ target: outboxConsumers.name });
        const [consumer] = await tx
          .select({
            ackedSeq: outboxConsumers.ackedSeq,
            leaseActive: sql<boolean>`${outboxConsumers.leaseToken} IS NOT NULL
              AND ${outboxConsumers.leasedUntil} > clock_timestamp()`,
          })
          .from(outboxConsumers)
          .where(eq(outboxConsumers.name, consumerName))
          .for("update");
        if (consumer!.leaseActive) return { lease_token: leaseToken, events: [] };
        const evRows = await tx
          .select()
          .from(outboxEvents)
          .where(gt(outboxEvents.seq, consumer!.ackedSeq))
          .orderBy(asc(outboxEvents.seq))
          .limit(limit);
        const last = evRows.at(-1);
        if (last) {
          await tx
            .update(outboxConsumers)
            .set({
              leaseToken,
              leasedThroughSeq: last.seq,
              leasedUntil: sql`clock_timestamp() + interval '30 seconds'`,
              updatedAt: sql`now()`,
            })
            .where(eq(outboxConsumers.name, consumerName));
        }
        return {
          lease_token: leaseToken,
          events: evRows.map((r) => ({
            seq: r.seq.toString(),
            event_id: r.eventId,
            kind: r.kind,
            request_id: r.requestId,
            project_id: r.projectId,
            payload: r.payload,
            created_at: new Date(r.createdAt).toISOString(),
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
      const rows = await orm
        .update(outboxConsumers)
        .set({
          ackedSeq: seq,
          leaseToken: null,
          leasedThroughSeq: null,
          leasedUntil: null,
          updatedAt: sql`now()`,
        })
        .where(
          and(
            eq(outboxConsumers.name, consumerName),
            eq(outboxConsumers.leaseToken, leaseToken),
            sql`${outboxConsumers.leasedUntil} > clock_timestamp()`,
            lte(outboxConsumers.ackedSeq, seq),
            sql`${outboxConsumers.leasedThroughSeq} >= ${seq}`,
          ),
        )
        .returning({ ackedSeq: outboxConsumers.ackedSeq });
      return rows.length > 0;
    },
  };
}
