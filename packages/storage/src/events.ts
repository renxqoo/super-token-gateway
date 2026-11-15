import type { SQL, TransactionSQL } from "bun";
import { GatewayError } from "@gateway/contracts";
import type { Finality, Operation, Usage } from "@gateway/kernel";

export interface UsageRecord {
  id: string;
  seq: string;
  project_id: string;
  request_id: string;
  attempt_id: string;
  source: string;
  semantics: "attempt_snapshot" | "disputed_snapshot";
  snapshot_revision: string | null;
  cost_revision: string | null;
  finality: Finality;
  usage: Usage;
  cost_atoms: string | null;
  currency: string;
  cost_complete: boolean;
}

export async function recordEventConsumer(
  tx: TransactionSQL,
  name: string,
  replay: boolean,
): Promise<string> {
  const id = crypto.randomUUID();
  await tx`SELECT pg_advisory_xact_lock(81442592)`;
  await tx`INSERT INTO event_consumers(id,name,replay_until) VALUES(${id},${name},${replay}::boolean::integer*(SELECT coalesce(max(seq),0) FROM usage_events))`;
  return id;
}

export function createEventStore(db: SQL | TransactionSQL) {
  async function backfill(consumerId: string): Promise<void> {
    const cursor = await db<{ complete: boolean }[]>`
      SELECT replay_cursor>=replay_until AS complete FROM event_consumers WHERE id=${consumerId}`;
    if (!cursor[0] || cursor[0].complete) return;
    await db`WITH owned AS (SELECT id,replay_cursor,replay_until FROM event_consumers WHERE id=${consumerId} AND replay_cursor<replay_until FOR UPDATE SKIP LOCKED),
    page AS (SELECT e.id,e.seq FROM usage_events e,owned o WHERE e.seq>o.replay_cursor AND e.seq<=o.replay_until ORDER BY e.seq LIMIT 1000),
    inserted AS (INSERT INTO event_deliveries(consumer_id,event_id) SELECT ${consumerId},id FROM page ON CONFLICT DO NOTHING RETURNING event_id)
    UPDATE event_consumers c SET replay_cursor=coalesce((SELECT max(seq) FROM page),o.replay_until) FROM owned o WHERE c.id=o.id`;
  }
  return {
    usage(filter: {
      projectId: string | null;
      limit: number;
      before?: string | undefined;
      models?: string[];
      operations?: Operation[];
    }): Promise<UsageRecord[]> {
      const { projectId, limit, before } = filter,
        models = filter.models ?? [],
        operations = filter.operations ?? [];
      if (
        before !== undefined &&
        (!/^[1-9][0-9]{0,18}$/.test(before) || BigInt(before) > 9223372036854775807n)
      )
        throw new GatewayError(400, "invalid_cursor", "Usage cursor must be a positive bigint");
      return db<
        UsageRecord[]
      >`SELECT id,seq::text,project_id,request_id,attempt_id,source,CASE record_kind WHEN 'DISPUTED' THEN 'disputed_snapshot' ELSE 'attempt_snapshot' END AS semantics,snapshot_revision::text,(SELECT c.revision::text FROM upstream_costs c WHERE c.evidence_id=usage_events.id) AS cost_revision,finality,usage,cost_atoms::text,currency,cost_complete
      FROM usage_events WHERE
      EXISTS(SELECT 1 FROM gateway_requests q WHERE q.id=usage_events.request_id AND (${models.length === 0} OR q.model=ANY(${models})) AND (${operations.length === 0} OR q.operation=ANY(${operations}))) AND (${projectId}::uuid IS NULL OR project_id=${projectId})
      AND (${before ?? null}::bigint IS NULL OR seq<${before ?? null}::bigint) ORDER BY seq DESC LIMIT ${limit}`;
    },
    backfill,
    async claim(
      consumerId: string,
      limit: number,
    ): Promise<{ lease_token: string; events: UsageRecord[] }> {
      await backfill(consumerId);
      const token = crypto.randomUUID();
      const events = await db<UsageRecord[]>`
      WITH selected AS (SELECT consumer_id,event_id FROM event_deliveries
        WHERE consumer_id=${consumerId} AND state<>'ACKED' AND (lease_until IS NULL OR lease_until<now())
        ORDER BY event_id LIMIT ${limit} FOR UPDATE SKIP LOCKED), claimed AS (
        UPDATE event_deliveries d SET state='LEASED',lease_token=${token},lease_until=now()+interval '30 seconds',attempts=d.attempts+1
        FROM selected s WHERE d.consumer_id=s.consumer_id AND d.event_id=s.event_id RETURNING d.event_id)
      SELECT e.id,e.seq::text,e.project_id,e.request_id,e.attempt_id,e.source,CASE e.record_kind WHEN 'DISPUTED' THEN 'disputed_snapshot' ELSE 'attempt_snapshot' END AS semantics,e.snapshot_revision::text,(SELECT c.revision::text FROM upstream_costs c WHERE c.evidence_id=e.id) AS cost_revision,e.finality,e.usage,e.cost_atoms::text,e.currency,e.cost_complete
      FROM usage_events e JOIN claimed c ON e.id=c.event_id`;
      return { lease_token: token, events };
    },
    async ack(consumerId: string, token: string, ids: string[]): Promise<number> {
      const rows =
        await db`UPDATE event_deliveries SET state='ACKED',lease_until=NULL WHERE consumer_id=${consumerId}
      AND lease_token=${token} AND event_id=ANY(${ids}) AND state='LEASED' AND lease_until>now() RETURNING event_id`;
      return rows.length;
    },
  };
}
