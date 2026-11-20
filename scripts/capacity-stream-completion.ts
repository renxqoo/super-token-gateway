import type { SQL, TransactionSQL } from "bun";

export async function confirmedStreamRequests(
  db: SQL | TransactionSQL,
  requests: string[],
  consumer: string,
  requireAck: boolean,
  expected: { charge: string; cost: string; usage: Record<string, string> },
): Promise<{ request_id: string }[]> {
  return db<{ request_id: string }[]>`
    WITH funded AS MATERIALIZED (
      SELECT q.id,q.project_id,q.selected_attempt_id,c.evidence_id,c.currency
      FROM unnest(${[...new Set(requests)]}::uuid[]) input(id)
      JOIN gateway_requests q ON q.id=input.id
      JOIN billing_charges c ON c.request_id=input.id
      JOIN billing_holds b ON (b.id,b.request_id)=(c.hold_id,input.id)
      JOIN wallets w ON (w.id,w.project_id,w.currency)=(b.wallet_id,q.project_id,c.currency)
      JOIN LATERAL (
        SELECT m.kind,m.currency,m.lines FROM money_journal m
        WHERE m.wallet_id=w.id AND m.reference='settle:'||input.id
        LIMIT 1
      ) m ON true
      WHERE q.operation='chat' AND q.state='SUCCEEDED' AND q.billing_outcome='SUCCESS'
        AND q.execution_closed_at IS NOT NULL AND c.currency='USD' AND b.state='SETTLED'
        AND c.amount_atoms=${expected.charge}::numeric AND c.funded_atoms=c.amount_atoms
        AND c.debt_atoms=0 AND c.refunded_atoms=0 AND m.kind='SETTLE' AND m.currency=c.currency
        AND (SELECT sum((leg->>'atoms')::numeric)=0 AND
               sum((leg->>'atoms')::numeric) FILTER(WHERE leg->>'account'='system:revenue')=c.amount_atoms
             FROM jsonb_array_elements(m.lines) leg)
    ), candidates AS MATERIALIZED (
      SELECT q.id AS request_id,q.project_id,a.id AS attempt_id,h.account_id,h.channel_id,h.currency
      FROM funded q JOIN attempts a ON (a.id,a.request_id)=(q.selected_attempt_id,q.id)
      JOIN usage_event_index buyer ON (buyer.id,buyer.attempt_id,buyer.request_id)=(q.evidence_id,a.id,q.id)
      JOIN upstream_holds h ON h.attempt_id=a.id
      JOIN upstream_cost_heads head ON head.attempt_id=a.id
      JOIN upstream_costs cost ON (cost.attempt_id,cost.revision)=(a.id,head.revision)
      JOIN usage_event_index supplier ON (supplier.id,supplier.attempt_id,supplier.request_id)=(cost.evidence_id,a.id,q.id)
      WHERE a.state='SUCCEEDED' AND buyer.record_kind='CANONICAL'
        AND h.state='SETTLED' AND h.currency=q.currency AND h.current_reserved_atoms=0
        AND h.final_cost_atoms=${expected.cost}::numeric AND cost.complete AND NOT cost.disputed
        AND (cost.cost_version_id,cost.currency,cost.cost_atoms)=(h.cost_version_id,h.currency,h.final_cost_atoms)
        AND supplier.record_kind='CANONICAL'
        AND EXISTS (
          SELECT 1 FROM core_evidence_receipts r
          JOIN usage_events u ON (u.id,u.attempt_id,u.request_id,u.project_id)=(r.canonical_evidence_id,a.id,q.id,q.project_id)
          WHERE r.attempt_id=a.id AND r.source='execution' AND r.authentication_disposition='VERIFIED'
            AND r.outcome='ACCEPTED' AND r.finality='FINAL' AND r.attempt_state='SUCCEEDED' AND r.request_state='SUCCEEDED'
            AND u.record_kind='CANONICAL' AND u.finality='FINAL' AND u.cost_complete AND u.currency=h.currency
            AND u.usage=${expected.usage}::jsonb AND u.cost_atoms=h.final_cost_atoms
        )
        AND NOT EXISTS (
          SELECT 1 FROM attempts subject JOIN usage_event_index i ON i.attempt_id=subject.id
          LEFT JOIN event_deliveries d ON d.event_id=i.id AND d.consumer_id=${consumer}::uuid
          WHERE subject.request_id=q.id AND i.record_kind<>'RAW' AND (
            (i.request_id,i.project_id,i.record_kind) IS DISTINCT FROM (q.id,q.project_id,'CANONICAL'::text)
            OR d.event_id IS NULL OR (${requireAck}::boolean AND d.state<>'ACKED')
          )
        )
    ), journals AS (
      SELECT s.request_id,j.id,sum(e.atoms) AS net,
        sum(e.atoms) FILTER(WHERE e.account='upstream:expense') AS expense,
        bool_and(j.account_id=s.account_id AND j.channel_id IS NOT DISTINCT FROM s.channel_id
          AND j.currency=s.currency AND e.currency=s.currency) AS owned
      FROM candidates s JOIN upstream_journals j ON j.attempt_id=s.attempt_id
      JOIN upstream_entries e ON e.journal_id=j.id GROUP BY s.request_id,j.id
    )
    SELECT request_id FROM journals GROUP BY request_id
    HAVING bool_and(net=0 AND owned) AND sum(expense)=${expected.cost}::numeric`;
}

export async function verifiedStreamProgress(
  db: SQL | TransactionSQL,
  events: {
    id: string;
    request_id: string;
    attempt_id: string;
    cost_atoms: string | null;
    currency: string;
    usage: Record<string, string>;
    cost_complete: boolean;
    finality: string;
  }[],
) {
  const partial = events.filter((event) => event.finality === "PARTIAL");
  if (!partial.length) return new Set<string>();
  const rows = await db<{ id: string }[]>`
    SELECT u.id FROM jsonb_to_recordset(${partial}::jsonb) p(id uuid,request_id uuid,attempt_id uuid,
      cost_atoms numeric,currency text,usage jsonb,cost_complete boolean)
    JOIN usage_events u ON (u.id,u.request_id,u.attempt_id)=(p.id,p.request_id,p.attempt_id)
    JOIN attempts a ON (a.id,a.request_id)=(u.attempt_id,u.request_id)
    JOIN gateway_requests q ON (q.id,q.project_id)=(u.request_id,u.project_id)
    WHERE u.record_kind='CANONICAL' AND u.finality='PARTIAL' AND u.currency='USD'
      AND (u.cost_atoms,u.currency,u.usage,u.cost_complete) IS NOT DISTINCT FROM
          (p.cost_atoms,p.currency,p.usage,p.cost_complete)
      AND EXISTS (SELECT 1 FROM core_evidence_receipts r WHERE r.attempt_id=a.id
        AND r.canonical_evidence_id=u.id AND r.source='execution' AND r.outcome='ACCEPTED'
        AND r.authentication_disposition='VERIFIED')`;
  return new Set(rows.map((row) => row.id));
}
