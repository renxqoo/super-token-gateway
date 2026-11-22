CREATE OR REPLACE FUNCTION public.outbox_shard(p_project_id UUID) RETURNS SMALLINT
LANGUAGE sql
IMMUTABLE
SET search_path TO 'pg_catalog', 'public', 'pg_temp'
AS $$
  SELECT CASE
    WHEN p_project_id IS NULL THEN 0::smallint
    ELSE ((('x' || left(p_project_id::text, 1))::bit(4) # ('x' || right(p_project_id::text, 1))::bit(4))::int)::smallint
  END;
$$;

REVOKE ALL ON FUNCTION public.outbox_shard(UUID) FROM PUBLIC;

ALTER TABLE public.outbox_events
  ADD COLUMN IF NOT EXISTS shard SMALLINT NOT NULL DEFAULT 0 CHECK (shard BETWEEN 0 AND 15);

UPDATE public.outbox_events
SET shard = public.outbox_shard(project_id)
WHERE shard <> public.outbox_shard(project_id);

CREATE INDEX IF NOT EXISTS outbox_events_shard_seq_idx
  ON public.outbox_events (shard, seq);

CREATE TABLE IF NOT EXISTS public.outbox_consumer_shards (
  name TEXT NOT NULL,
  shard SMALLINT NOT NULL CHECK (shard BETWEEN 0 AND 15),
  acked_seq BIGINT NOT NULL DEFAULT 0 CHECK (acked_seq >= 0),
  lease_token UUID,
  leased_through_seq BIGINT CHECK (leased_through_seq >= 0),
  leased_until TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (name, shard)
);

INSERT INTO public.outbox_consumer_shards (
  name, shard, acked_seq, lease_token, leased_through_seq, leased_until, updated_at
)
SELECT
  c.name, s.shard::smallint, c.acked_seq, c.lease_token, c.leased_through_seq, c.leased_until, c.updated_at
FROM public.outbox_consumers c
CROSS JOIN generate_series(0, 15) AS s(shard)
ON CONFLICT (name, shard) DO NOTHING;

ALTER TABLE public.outbox_events
  ALTER COLUMN seq SET DEFAULT 0;

DROP TRIGGER IF EXISTS order_outbox_publish ON public.outbox_events;

CREATE OR REPLACE FUNCTION public.order_outbox_publish() RETURNS trigger
LANGUAGE plpgsql
SET search_path TO 'pg_catalog', 'public', 'pg_temp'
AS $$
BEGIN
  NEW.shard := public.outbox_shard(NEW.project_id);
  PERFORM pg_advisory_xact_lock(81442591, 100 + NEW.shard::integer);
  IF NEW.seq IS NULL OR NEW.seq <= 0 THEN
    NEW.seq := nextval('public.outbox_events_seq_seq');
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER order_outbox_publish
  BEFORE INSERT ON public.outbox_events
  FOR EACH ROW EXECUTE FUNCTION public.order_outbox_publish();

CREATE OR REPLACE FUNCTION public.check_ledger_journal_balance() RETURNS trigger
LANGUAGE plpgsql
SET search_path TO 'pg_catalog', 'public', 'pg_temp'
AS $$
DECLARE
  line_count INTEGER;
  total NUMERIC(38,0);
BEGIN
  SELECT count(*), coalesce(sum(atoms), 0) INTO line_count, total
  FROM public.ledger_entries
  WHERE journal_id = NEW.journal_id AND created_at = NEW.created_at;
  IF line_count < 2 OR total <> 0 THEN
    RAISE EXCEPTION 'unbalanced ledger journal: %', NEW.journal_id USING ERRCODE = 'P0422';
  END IF;
  RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION public.operations_health(p_consumer TEXT) RETURNS TABLE (
  expired_hold DOUBLE PRECISION, customer_unknown DOUBLE PRECISION, supplier_unknown DOUBLE PRECISION,
  expired_wallet_lease DOUBLE PRECISION, expired_supplier_lease DOUBLE PRECISION, outbox_head DOUBLE PRECISION,
  admin_audit_pending DOUBLE PRECISION
)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path TO pg_catalog, pg_temp
AS $$
  SELECT
    COALESCE((SELECT extract(epoch FROM h.expires_at)::float8 FROM public.billing_holds h
      WHERE h.state = 'HELD' AND h.expires_at <= now() ORDER BY h.expires_at LIMIT 1), 0),
    COALESCE((SELECT extract(epoch FROM h.expires_at)::float8 FROM public.billing_holds h
      WHERE h.state = 'UNKNOWN' ORDER BY h.expires_at LIMIT 1), 0),
    COALESCE((SELECT extract(epoch FROM h.expires_at)::float8 FROM public.billing_holds h
      WHERE h.supplier_holds @> '[{"state":"UNKNOWN"}]'::jsonb ORDER BY h.expires_at LIMIT 1), 0),
    COALESCE((SELECT extract(epoch FROM l.expires_at)::float8 FROM public.wallet_leases l
      WHERE l.state = 'ACTIVE' AND l.expires_at <= now() ORDER BY l.expires_at LIMIT 1), 0),
    COALESCE((SELECT extract(epoch FROM l.expires_at)::float8 FROM public.supplier_leases l
      WHERE l.state = 'ACTIVE' AND l.expires_at <= now() ORDER BY l.expires_at LIMIT 1), 0),
    CASE WHEN p_consumer IS NULL THEN 0::float8 ELSE COALESCE((
      SELECT min(sub.ts) FROM (
        SELECT (
          SELECT extract(epoch FROM o.created_at)::float8
          FROM public.outbox_events o
          WHERE o.shard = s.shard
            AND o.seq > COALESCE(
              (SELECT cs.acked_seq FROM public.outbox_consumer_shards cs WHERE cs.name = p_consumer AND cs.shard = s.shard),
              (SELECT c.acked_seq FROM public.outbox_consumers c WHERE c.name = p_consumer),
              0
            )
          ORDER BY o.seq LIMIT 1
        ) AS ts
        FROM generate_series(0, 15) AS s(shard)
      ) sub WHERE sub.ts IS NOT NULL
    ), 0) END,
    COALESCE((SELECT extract(epoch FROM a.created_at)::float8 FROM public.admin_audit_pending a
      ORDER BY a.created_at LIMIT 1), 0)
$$;

ALTER TABLE public.wallet_leases SET (fillfactor = 60);
ALTER TABLE public.supplier_leases SET (fillfactor = 60);
ALTER TABLE public.wallets SET (fillfactor = 70);
ALTER TABLE public.wallet_lots SET (fillfactor = 70);
ALTER TABLE public.supplier_cells SET (fillfactor = 70);

CREATE OR REPLACE FUNCTION public.commit_leased_batch(
  p_items JSONB,
  p_next_expires_at TIMESTAMPTZ,
  p_renew_threshold TIMESTAMPTZ,
  p_now TIMESTAMPTZ
) RETURNS JSONB
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path TO 'pg_catalog', 'public', 'pg_temp'
AS $$
DECLARE
  v_item_count INTEGER;
  v_ok_holds INTEGER;
  v_needs_slow BOOLEAN;
  v_wl_exp JSONB;
  v_sl_exp JSONB;
  v_req_ids UUID[];
  v_wl_ids UUID[];
  v_sl_ids UUID[];
  v_w_ids UUID[];
  v_lot_ids UUID[];
  v_cell_accounts UUID[];
  v_cell_currencies TEXT[];
  v_cell_nums SMALLINT[];
  v_shards SMALLINT[];
BEGIN
  IF p_items IS NULL THEN
    RETURN jsonb_build_object('fast', true, 'wl_expiries', '{}'::jsonb, 'sl_expiries', '{}'::jsonb);
  END IF;
  v_item_count := jsonb_array_length(p_items);
  IF v_item_count = 0 THEN
    RETURN jsonb_build_object('fast', true, 'wl_expiries', '{}'::jsonb, 'sl_expiries', '{}'::jsonb);
  END IF;

  WITH it AS MATERIALIZED (
    SELECT
      x.request_id,
      x.project_id,
      x.wallet_id,
      x.wallet_lease_id,
      x.supplier_lease_id
    FROM jsonb_to_recordset(p_items) AS x(
      request_id UUID,
      project_id UUID,
      wallet_id UUID,
      wallet_lease_id UUID,
      supplier_lease_id UUID
    )
  )
  SELECT
    array_agg(DISTINCT it.request_id ORDER BY it.request_id),
    array_agg(DISTINCT it.wallet_lease_id ORDER BY it.wallet_lease_id),
    array_agg(DISTINCT it.supplier_lease_id ORDER BY it.supplier_lease_id),
    array_agg(DISTINCT it.wallet_id ORDER BY it.wallet_id),
    array_agg(DISTINCT public.outbox_shard(it.project_id) ORDER BY public.outbox_shard(it.project_id)),
    EXISTS (
      SELECT 1
      FROM (SELECT wallet_id, count(DISTINCT wallet_lease_id) AS n FROM it GROUP BY wallet_id) wm
      WHERE wm.n > 1
    )
  INTO v_req_ids, v_wl_ids, v_sl_ids, v_w_ids, v_shards, v_needs_slow
  FROM it;

  IF v_needs_slow THEN
    RETURN jsonb_build_object('fast', false);
  END IF;

  WITH locked AS MATERIALIZED (
    SELECT h.request_id, h.state, h.wallet_lease_id, h.supplier_lease_id
    FROM public.billing_holds h
    WHERE h.request_id = ANY(v_req_ids)
    ORDER BY h.request_id
    FOR NO KEY UPDATE
  )
  SELECT count(*) FILTER (
    WHERE l.state = 'HELD'
      AND l.wallet_lease_id IS NOT DISTINCT FROM it.wallet_lease_id
      AND l.supplier_lease_id IS NOT DISTINCT FROM it.supplier_lease_id
  )
  INTO v_ok_holds
  FROM jsonb_to_recordset(p_items) AS it(
    request_id UUID,
    wallet_lease_id UUID,
    supplier_lease_id UUID
  )
  JOIN locked l ON l.request_id = it.request_id;

  IF v_ok_holds <> v_item_count THEN
    RETURN jsonb_build_object('fast', false);
  END IF;

  WITH locked AS MATERIALIZED (
    SELECT wl.id, wl.state, wl.earliest_lot_expires_at, wl.remaining_atoms, wl.lot_allocations
    FROM public.wallet_leases wl
    WHERE wl.id = ANY(v_wl_ids)
    ORDER BY wl.id
    FOR NO KEY UPDATE
  ), req AS (
    SELECT wallet_lease_id AS lid, sum(customer_atoms) AS need
    FROM jsonb_to_recordset(p_items) AS it(wallet_lease_id UUID, customer_atoms NUMERIC)
    GROUP BY wallet_lease_id
  )
  SELECT
    EXISTS (
      SELECT 1
      FROM req
      LEFT JOIN locked wl ON wl.id = req.lid
      WHERE wl.id IS NULL
         OR wl.state <> 'ACTIVE'
         OR (wl.earliest_lot_expires_at IS NOT NULL AND wl.earliest_lot_expires_at <= p_now)
         OR req.need > wl.remaining_atoms
         OR (
           req.need > 0
           AND (
             jsonb_array_length(wl.lot_allocations) <> 1
             OR req.need > COALESCE((wl.lot_allocations->0->>'amount_atoms')::numeric, 0)
           )
         )
    ),
    ARRAY(
      SELECT DISTINCT (la->>'lot_id')::uuid
      FROM locked wl
      CROSS JOIN LATERAL jsonb_array_elements(wl.lot_allocations) la
      ORDER BY 1
    )
  INTO v_needs_slow, v_lot_ids;

  IF v_needs_slow THEN
    RETURN jsonb_build_object('fast', false);
  END IF;

  WITH locked AS MATERIALIZED (
    SELECT sl.id, sl.state, sl.remaining_atoms, sl.account_id, sl.currency, sl.cell
    FROM public.supplier_leases sl
    WHERE sl.id = ANY(v_sl_ids)
    ORDER BY sl.id
    FOR NO KEY UPDATE
  ), req AS (
    SELECT supplier_lease_id AS lid, sum(supplier_atoms) AS need
    FROM jsonb_to_recordset(p_items) AS it(supplier_lease_id UUID, supplier_atoms NUMERIC)
    GROUP BY supplier_lease_id
  ), cells AS (
    SELECT DISTINCT account_id, currency, cell
    FROM locked
    ORDER BY account_id, currency, cell
  )
  SELECT
    EXISTS (
      SELECT 1
      FROM req
      LEFT JOIN locked sl ON sl.id = req.lid
      WHERE sl.id IS NULL
         OR sl.state <> 'ACTIVE'
         OR req.need > sl.remaining_atoms
    ),
    COALESCE((SELECT array_agg(account_id) FROM cells), '{}'::uuid[]),
    COALESCE((SELECT array_agg(currency) FROM cells), '{}'::text[]),
    COALESCE((SELECT array_agg(cell) FROM cells), '{}'::smallint[])
  INTO v_needs_slow, v_cell_accounts, v_cell_currencies, v_cell_nums;

  IF v_needs_slow THEN
    RETURN jsonb_build_object('fast', false);
  END IF;

  PERFORM 1
  FROM public.wallets w
  WHERE w.id = ANY(v_w_ids)
  ORDER BY w.id
  FOR NO KEY UPDATE;

  IF cardinality(v_lot_ids) > 0 THEN
    PERFORM 1
    FROM public.wallet_lots l
    WHERE l.id = ANY(v_lot_ids)
    ORDER BY l.id
    FOR NO KEY UPDATE;
  END IF;

  IF cardinality(v_cell_accounts) > 0 THEN
    PERFORM 1
    FROM public.supplier_cells c
    JOIN unnest(v_cell_accounts, v_cell_currencies, v_cell_nums) AS k(account_id, currency, cell)
      ON c.account_id = k.account_id AND c.currency = k.currency AND c.cell = k.cell
    ORDER BY c.account_id, c.currency, c.cell
    FOR NO KEY UPDATE OF c;
  END IF;

  WITH
    items AS MATERIALIZED (
      SELECT
        it.charge_id,
        it.event_id,
        it.request_id,
        it.project_id,
        it.wallet_id,
        it.wallet_lease_id,
        it.supplier_lease_id,
        it.selected_attempt_id,
        it.channel_id,
        it.model,
        it.operation,
        it.currency,
        it.billing_price_id,
        it.cost_version_id,
        it.usage,
        it.usage_digest,
        it.customer_atoms,
        it.supplier_atoms,
        it.supplier_currency,
        it.plugin_audit,
        it.charge_policy,
        (wl.lot_allocations->0->>'lot_id')::uuid AS lot_id,
        sl.account_id AS supplier_account_id,
        sl.currency AS sl_currency,
        sl.cell AS supplier_cell
      FROM jsonb_to_recordset(p_items) AS it(
        charge_id UUID,
        event_id UUID,
        request_id UUID,
        project_id UUID,
        wallet_id UUID,
        wallet_lease_id UUID,
        supplier_lease_id UUID,
        selected_attempt_id UUID,
        channel_id UUID,
        model TEXT,
        operation TEXT,
        currency TEXT,
        billing_price_id UUID,
        cost_version_id UUID,
        usage JSONB,
        usage_digest TEXT,
        customer_atoms NUMERIC,
        supplier_atoms NUMERIC,
        supplier_currency TEXT,
        plugin_audit JSONB,
        charge_policy TEXT
      )
      JOIN public.wallet_leases wl ON wl.id = it.wallet_lease_id
      JOIN public.supplier_leases sl ON sl.id = it.supplier_lease_id
    ),
    cust_groups AS MATERIALIZED (
      SELECT
        wallet_id,
        currency,
        gen_random_uuid() AS journal_id,
        CASE WHEN count(*) = 1 THEN min(request_id::text)::uuid ELSE NULL END AS single_req_id,
        sum(customer_atoms) AS total_atoms,
        min(lot_id::text)::uuid AS lot_id
      FROM items
      WHERE customer_atoms > 0
      GROUP BY wallet_id, currency
    ),
    sup_groups AS MATERIALIZED (
      SELECT
        supplier_account_id AS account_id,
        sl_currency AS currency,
        gen_random_uuid() AS journal_id,
        CASE WHEN count(*) = 1 THEN min(request_id::text)::uuid ELSE NULL END AS single_req_id,
        CASE WHEN count(*) = 1 THEN min(selected_attempt_id::text)::uuid ELSE NULL END AS single_attempt_id,
        sum(supplier_atoms) AS total_atoms
      FROM items
      WHERE supplier_atoms > 0
      GROUP BY supplier_account_id, sl_currency
    ),
    upd_wl AS (
      UPDATE public.wallet_leases AS l SET
        lot_allocations = CASE
          WHEN req.need = 0 THEN l.lot_allocations
          WHEN (l.lot_allocations->0->>'amount_atoms')::numeric - req.need > 0
            THEN jsonb_build_array(
              jsonb_build_object(
                'lot_id', l.lot_allocations->0->>'lot_id',
                'amount_atoms', ((l.lot_allocations->0->>'amount_atoms')::numeric - req.need)::text,
                'expires_at_ms', l.lot_allocations->0->'expires_at_ms'
              )
            )
          ELSE '[]'::jsonb
        END,
        remaining_atoms = l.remaining_atoms - req.need,
        earliest_lot_expires_at = CASE
          WHEN req.need = 0 THEN l.earliest_lot_expires_at
          WHEN (l.lot_allocations->0->>'amount_atoms')::numeric - req.need > 0 THEN l.earliest_lot_expires_at
          ELSE NULL
        END,
        expires_at = CASE
          WHEN l.expires_at < p_renew_threshold THEN GREATEST(l.expires_at, p_next_expires_at)
          ELSE l.expires_at
        END,
        updated_at = now()
      FROM (
        SELECT wallet_lease_id AS id, sum(customer_atoms) AS need
        FROM items
        GROUP BY wallet_lease_id
      ) req
      WHERE l.id = req.id AND (req.need > 0 OR l.expires_at < p_renew_threshold)
      RETURNING l.id, (extract(epoch FROM l.expires_at) * 1000)::bigint AS expires_ms
    ),
    upd_sl AS (
      UPDATE public.supplier_leases AS l SET
        remaining_atoms = l.remaining_atoms - req.need,
        expires_at = CASE
          WHEN l.expires_at < p_renew_threshold THEN GREATEST(l.expires_at, p_next_expires_at)
          ELSE l.expires_at
        END,
        updated_at = now()
      FROM (
        SELECT supplier_lease_id AS id, sum(supplier_atoms) AS need
        FROM items
        GROUP BY supplier_lease_id
      ) req
      WHERE l.id = req.id AND (req.need > 0 OR l.expires_at < p_renew_threshold)
      RETURNING l.id, (extract(epoch FROM l.expires_at) * 1000)::bigint AS expires_ms
    ),
    upd_w AS (
      UPDATE public.wallets AS w SET
        reserved_atoms = w.reserved_atoms - req.spent,
        spent_atoms = w.spent_atoms + req.spent
      FROM (
        SELECT wallet_id AS id, sum(customer_atoms) AS spent
        FROM items
        WHERE customer_atoms > 0
        GROUP BY wallet_id
      ) req
      WHERE w.id = req.id
    ),
    upd_l AS (
      UPDATE public.wallet_lots AS l SET
        reserved_atoms = l.reserved_atoms - req.spent,
        spent_atoms = l.spent_atoms + req.spent
      FROM (
        SELECT lot_id AS id, sum(customer_atoms) AS spent
        FROM items
        WHERE customer_atoms > 0
        GROUP BY lot_id
      ) req
      WHERE l.id = req.id
    ),
    upd_c AS (
      UPDATE public.supplier_cells AS c SET
        reserved_atoms = c.reserved_atoms - req.spent,
        balance_atoms = c.balance_atoms - req.spent,
        spent_atoms = c.spent_atoms + req.spent,
        revision = c.revision + 1
      FROM (
        SELECT supplier_account_id AS account_id, sl_currency AS currency, supplier_cell AS cell, sum(supplier_atoms) AS spent
        FROM items
        WHERE supplier_atoms > 0
        GROUP BY supplier_account_id, sl_currency, supplier_cell
      ) req
      WHERE c.account_id = req.account_id AND c.currency = req.currency AND c.cell = req.cell
    ),
    upd_h AS (
      UPDATE public.billing_holds AS h
      SET state = 'SETTLED', closed_at = now()
      WHERE h.request_id = ANY(v_req_ids)
    ),
    ins_ch AS (
      INSERT INTO public.billing_charges (
        id, request_id, project_id, wallet_id, selected_attempt_id, channel_id,
        model, operation, currency, billing_price_id, cost_version_id, usage, usage_digest,
        customer_atoms, funded_atoms, debt_atoms, supplier_cost_atoms, supplier_currency,
        lot_allocations, plugin_audit, charge_policy, outcome, customer_journal_id, supplier_journal_id
      )
      SELECT
        it.charge_id, it.request_id, it.project_id, it.wallet_id, it.selected_attempt_id, it.channel_id,
        it.model, it.operation, it.currency, it.billing_price_id, it.cost_version_id, it.usage, it.usage_digest,
        it.customer_atoms, it.customer_atoms, 0, it.supplier_atoms, it.supplier_currency,
        CASE
          WHEN it.customer_atoms > 0 THEN jsonb_build_array(
            jsonb_build_object(
              'lot_id', it.lot_id::text,
              'amount_atoms', it.customer_atoms::text,
              'refunded_atoms', '0'
            )
          )
          ELSE '[]'::jsonb
        END,
        it.plugin_audit,
        it.charge_policy,
        'SETTLED',
        CASE WHEN it.customer_atoms > 0 THEN cg.journal_id ELSE NULL END,
        CASE WHEN it.supplier_atoms > 0 THEN sg.journal_id ELSE NULL END
      FROM items it
      LEFT JOIN cust_groups cg ON cg.wallet_id = it.wallet_id AND cg.currency = it.currency
      LEFT JOIN sup_groups sg ON sg.account_id = it.supplier_account_id AND sg.currency = it.sl_currency
    ),
    ins_le AS (
      INSERT INTO public.ledger_entries (
        journal_id, ordinal, side, entity_id, request_id, reference, kind, currency, account, atoms
      )
      SELECT journal_id, ordinal, side, entity_id, request_id, reference, kind, currency, account, atoms
      FROM (
        SELECT
          cg.journal_id,
          0::smallint AS ordinal,
          'CUSTOMER'::text AS side,
          cg.wallet_id AS entity_id,
          cg.single_req_id AS request_id,
          CASE WHEN cg.single_req_id IS NOT NULL THEN 'settle:' || cg.single_req_id::text ELSE 'settle_batch:' || cg.journal_id::text END AS reference,
          'SETTLE'::text AS kind,
          cg.currency,
          'lot:' || cg.lot_id::text || ':available' AS account,
          -cg.total_atoms AS atoms
        FROM cust_groups cg
        UNION ALL
        SELECT
          cg.journal_id,
          1::smallint,
          'CUSTOMER',
          cg.wallet_id,
          cg.single_req_id,
          CASE WHEN cg.single_req_id IS NOT NULL THEN 'settle:' || cg.single_req_id::text ELSE 'settle_batch:' || cg.journal_id::text END,
          'SETTLE',
          cg.currency,
          'system:revenue',
          cg.total_atoms
        FROM cust_groups cg
        UNION ALL
        SELECT
          sg.journal_id,
          0::smallint,
          'SUPPLIER',
          sg.account_id,
          sg.single_req_id,
          CASE WHEN sg.single_req_id IS NOT NULL THEN 'supplier_cost:' || sg.single_attempt_id::text ELSE 'supplier_cost_batch:' || sg.journal_id::text END,
          'SUPPLIER_COST',
          sg.currency,
          'upstream:available',
          -sg.total_atoms
        FROM sup_groups sg
        UNION ALL
        SELECT
          sg.journal_id,
          1::smallint,
          'SUPPLIER',
          sg.account_id,
          sg.single_req_id,
          CASE WHEN sg.single_req_id IS NOT NULL THEN 'supplier_cost:' || sg.single_attempt_id::text ELSE 'supplier_cost_batch:' || sg.journal_id::text END,
          'SUPPLIER_COST',
          sg.currency,
          'upstream:expense',
          sg.total_atoms
        FROM sup_groups sg
      ) le
      ORDER BY le.journal_id, le.ordinal
    )
  SELECT
    COALESCE((SELECT jsonb_object_agg(id::text, expires_ms) FROM upd_wl), '{}'::jsonb),
    COALESCE((SELECT jsonb_object_agg(id::text, expires_ms) FROM upd_sl), '{}'::jsonb)
  INTO v_wl_exp, v_sl_exp;

  PERFORM pg_advisory_xact_lock(81442591, 100 + s::integer)
  FROM unnest(v_shards) AS s;

  INSERT INTO public.outbox_events (event_id, kind, request_id, project_id, payload)
  SELECT
    it.event_id,
    'billing.settled',
    it.request_id,
    it.project_id,
    jsonb_build_object(
      'charge_id', it.charge_id,
      'request_id', it.request_id,
      'attempt_id', it.selected_attempt_id,
      'channel_id', it.channel_id,
      'model', it.model,
      'operation', it.operation,
      'currency', it.currency,
      'usage', it.usage,
      'customer_atoms', it.customer_atoms::text,
      'funded_atoms', it.customer_atoms::text,
      'debt_atoms', '0',
      'supplier_cost_atoms', it.supplier_atoms::text,
      'outcome', 'SETTLED',
      'wallet_available_atoms', COALESCE(w.available_atoms, 0)::text
    )
  FROM jsonb_to_recordset(p_items) AS it(
    charge_id UUID,
    event_id UUID,
    request_id UUID,
    project_id UUID,
    wallet_id UUID,
    selected_attempt_id UUID,
    channel_id UUID,
    model TEXT,
    operation TEXT,
    currency TEXT,
    usage JSONB,
    customer_atoms NUMERIC,
    supplier_atoms NUMERIC
  )
  LEFT JOIN public.wallets w ON w.id = it.wallet_id
  ORDER BY public.outbox_shard(it.project_id) ASC, it.request_id ASC;

  RETURN jsonb_build_object('fast', true, 'wl_expiries', v_wl_exp, 'sl_expiries', v_sl_exp);
END;
$$;

REVOKE ALL ON FUNCTION public.commit_leased_batch(JSONB, TIMESTAMPTZ, TIMESTAMPTZ, TIMESTAMPTZ) FROM PUBLIC;
