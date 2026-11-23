ALTER TABLE public.api_keys
  ADD COLUMN IF NOT EXISTS spend_limit_atoms NUMERIC(38, 0) CHECK (spend_limit_atoms IS NULL OR spend_limit_atoms >= 0),
  ADD COLUMN IF NOT EXISTS spend_limit_window TEXT NOT NULL DEFAULT 'LIFETIME' CHECK (spend_limit_window IN ('LIFETIME', 'DAILY', 'MONTHLY')),
  ADD COLUMN IF NOT EXISTS spend_window_start TIMESTAMPTZ NOT NULL DEFAULT now(),
  ADD COLUMN IF NOT EXISTS spent_atoms NUMERIC(38, 0) NOT NULL DEFAULT 0 CHECK (spent_atoms >= 0),
  ADD COLUMN IF NOT EXISTS reserved_atoms NUMERIC(38, 0) NOT NULL DEFAULT 0 CHECK (reserved_atoms >= 0);

ALTER TABLE public.billing_holds
  ADD COLUMN IF NOT EXISTS api_key_id UUID,
  ADD COLUMN IF NOT EXISTS key_reserved_atoms NUMERIC(38, 0) NOT NULL DEFAULT 0 CHECK (key_reserved_atoms >= 0),
  ADD COLUMN IF NOT EXISTS pricing_override_id UUID,
  ADD COLUMN IF NOT EXISTS discount_numerator NUMERIC(38, 0),
  ADD COLUMN IF NOT EXISTS discount_denominator NUMERIC(38, 0);

ALTER TABLE public.billing_charges
  ADD COLUMN IF NOT EXISTS api_key_id UUID,
  ADD COLUMN IF NOT EXISTS pricing_override_id UUID,
  ADD COLUMN IF NOT EXISTS discount_numerator NUMERIC(38, 0),
  ADD COLUMN IF NOT EXISTS discount_denominator NUMERIC(38, 0);

CREATE INDEX IF NOT EXISTS billing_charges_api_key_created_idx
  ON public.billing_charges (api_key_id, created_at)
  WHERE api_key_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS public.project_pricing_overrides (
  id UUID PRIMARY KEY,
  project_id UUID NOT NULL REFERENCES public.projects(id),
  model_name TEXT NOT NULL,
  operation TEXT NOT NULL DEFAULT '*',
  override_kind TEXT NOT NULL CHECK (override_kind IN ('EXACT_PRICE', 'MULTIPLIER')),
  billing_price_id UUID REFERENCES public.price_versions(id),
  multiplier_numerator NUMERIC(38, 0),
  multiplier_denominator NUMERIC(38, 0),
  enabled BOOLEAN NOT NULL DEFAULT true,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT project_pricing_overrides_scope_uniq UNIQUE (project_id, model_name, operation),
  CONSTRAINT project_pricing_overrides_shape_chk CHECK (
    (override_kind = 'EXACT_PRICE' AND billing_price_id IS NOT NULL AND multiplier_numerator IS NULL AND multiplier_denominator IS NULL)
    OR
    (override_kind = 'MULTIPLIER' AND billing_price_id IS NULL AND multiplier_numerator > 0 AND multiplier_denominator > 0)
  )
);

CREATE INDEX IF NOT EXISTS project_pricing_overrides_project_idx
  ON public.project_pricing_overrides (project_id);

CREATE TABLE IF NOT EXISTS public.async_tasks (
  id TEXT PRIMARY KEY,
  request_id UUID NOT NULL UNIQUE,
  charge_id UUID REFERENCES public.billing_charges(id),
  project_id UUID NOT NULL REFERENCES public.projects(id),
  api_key_id UUID,
  channel_id UUID NOT NULL REFERENCES public.channels(id),
  model TEXT NOT NULL,
  operation TEXT NOT NULL,
  upstream_task_id TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('QUEUED', 'PROCESSING', 'SUCCEEDED', 'FAILED', 'CANCELLED')),
  user_cancelled BOOLEAN NOT NULL DEFAULT false,
  upstream_billed BOOLEAN NOT NULL DEFAULT false,
  failure_category TEXT CHECK (
    failure_category IS NULL OR failure_category IN (
      'UPSTREAM_SYSTEM', 'CONTENT_POLICY', 'INVALID_INPUT', 'USER_CANCELLED', 'UPSTREAM_BILLED', 'TIMEOUT', 'UNKNOWN'
    )
  ),
  refund_eligibility TEXT NOT NULL DEFAULT 'PENDING' CHECK (
    refund_eligibility IN (
      'PENDING', 'AUTO_REFUNDED', 'DENIED_USER_CANCELLED', 'DENIED_POLICY_OR_CLIENT',
      'DENIED_UPSTREAM_BILLED', 'DENIED_ABUSE_LIMIT', 'MANUAL_REVIEW', 'NOT_APPLICABLE'
    )
  ),
  refund_id UUID,
  result_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  completed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS async_tasks_project_status_idx
  ON public.async_tasks (project_id, status);

CREATE INDEX IF NOT EXISTS async_tasks_active_idx
  ON public.async_tasks (status, updated_at)
  WHERE status IN ('QUEUED', 'PROCESSING');

CREATE INDEX IF NOT EXISTS async_tasks_auto_refund_idx
  ON public.async_tasks (project_id, refund_eligibility, completed_at)
  WHERE refund_eligibility = 'AUTO_REFUNDED';

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
  v_req_wl_ids UUID[];
  v_req_sl_ids UUID[];
  v_wl_ids UUID[];
  v_wl_needs NUMERIC[];
  v_sl_ids UUID[];
  v_sl_needs NUMERIC[];
  v_w_ids UUID[];
  v_shards SMALLINT[];
  v_w_spend_ids UUID[];
  v_w_spend_atoms NUMERIC[];
  v_lot_spend_ids UUID[];
  v_lot_spend_atoms NUMERIC[];
  v_cell_spend_accts UUID[];
  v_cell_spend_curs TEXT[];
  v_cell_spend_nums SMALLINT[];
  v_cell_spend_atoms NUMERIC[];
  v_ob_event_ids UUID[];
  v_ob_req_ids UUID[];
  v_ob_proj_ids UUID[];
  v_ob_payloads JSONB[];
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
      x.supplier_lease_id,
      x.customer_atoms,
      x.supplier_atoms
    FROM jsonb_to_recordset(p_items) AS x(
      request_id UUID,
      project_id UUID,
      wallet_id UUID,
      wallet_lease_id UUID,
      supplier_lease_id UUID,
      customer_atoms NUMERIC,
      supplier_atoms NUMERIC
    )
  ),
  ord_req AS (
    SELECT request_id, wallet_lease_id, supplier_lease_id
    FROM it
    ORDER BY request_id
  ),
  wl_agg AS (
    SELECT wallet_lease_id AS id, sum(customer_atoms) AS need
    FROM it
    GROUP BY wallet_lease_id
    ORDER BY wallet_lease_id
  ),
  sl_agg AS (
    SELECT supplier_lease_id AS id, sum(supplier_atoms) AS need
    FROM it
    GROUP BY supplier_lease_id
    ORDER BY supplier_lease_id
  )
  SELECT
    (SELECT array_agg(request_id) FROM ord_req),
    (SELECT array_agg(wallet_lease_id) FROM ord_req),
    (SELECT array_agg(supplier_lease_id) FROM ord_req),
    (SELECT array_agg(id) FROM wl_agg),
    (SELECT array_agg(need) FROM wl_agg),
    (SELECT array_agg(id) FROM sl_agg),
    (SELECT array_agg(need) FROM sl_agg),
    array_agg(DISTINCT it.wallet_id ORDER BY it.wallet_id),
    array_agg(DISTINCT public.outbox_shard(it.project_id) ORDER BY public.outbox_shard(it.project_id)),
    EXISTS (
      SELECT 1
      FROM (SELECT wallet_id, count(DISTINCT wallet_lease_id) AS n FROM it GROUP BY wallet_id) wm
      WHERE wm.n > 1
    ) OR EXISTS (
      SELECT 1
      FROM (SELECT request_id FROM it GROUP BY request_id HAVING count(*) > 1) dup
    )
  INTO
    v_req_ids,
    v_req_wl_ids,
    v_req_sl_ids,
    v_wl_ids,
    v_wl_needs,
    v_sl_ids,
    v_sl_needs,
    v_w_ids,
    v_shards,
    v_needs_slow
  FROM it;

  IF v_needs_slow THEN
    RETURN jsonb_build_object('fast', false);
  END IF;

  WITH locked AS MATERIALIZED (
    SELECT h.request_id, h.state, h.wallet_lease_id, h.supplier_lease_id, h.key_reserved_atoms
    FROM public.billing_holds h
    WHERE h.request_id = ANY(v_req_ids)
    ORDER BY h.request_id
    FOR NO KEY UPDATE
  )
  SELECT count(*) FILTER (
    WHERE l.state = 'HELD'
      AND l.key_reserved_atoms = 0
      AND l.wallet_lease_id IS NOT DISTINCT FROM u.wallet_lease_id
      AND l.supplier_lease_id IS NOT DISTINCT FROM u.supplier_lease_id
  )
  INTO v_ok_holds
  FROM unnest(v_req_ids, v_req_wl_ids, v_req_sl_ids) AS u(
    request_id,
    wallet_lease_id,
    supplier_lease_id
  )
  JOIN locked l ON l.request_id = u.request_id;

  IF v_ok_holds <> v_item_count THEN
    RETURN jsonb_build_object('fast', false);
  END IF;

  WITH locked AS MATERIALIZED (
    SELECT wl.id, wl.state, wl.earliest_lot_expires_at, wl.remaining_atoms, wl.lot_allocations
    FROM public.wallet_leases wl
    WHERE wl.id = ANY(v_wl_ids)
    ORDER BY wl.id
    FOR NO KEY UPDATE
  )
  SELECT EXISTS (
    SELECT 1
    FROM unnest(v_wl_ids, v_wl_needs) AS req(lid, need)
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
  )
  INTO v_needs_slow;

  IF v_needs_slow THEN
    RETURN jsonb_build_object('fast', false);
  END IF;

  WITH locked AS MATERIALIZED (
    SELECT sl.id, sl.state, sl.remaining_atoms
    FROM public.supplier_leases sl
    WHERE sl.id = ANY(v_sl_ids)
    ORDER BY sl.id
    FOR NO KEY UPDATE
  )
  SELECT EXISTS (
    SELECT 1
    FROM unnest(v_sl_ids, v_sl_needs) AS req(lid, need)
    LEFT JOIN locked sl ON sl.id = req.lid
    WHERE sl.id IS NULL
       OR sl.state <> 'ACTIVE'
       OR req.need > sl.remaining_atoms
  )
  INTO v_needs_slow;

  IF v_needs_slow THEN
    RETURN jsonb_build_object('fast', false);
  END IF;

  WITH
    items AS MATERIALIZED (
      SELECT
        it.charge_id,
        it.event_id,
        it.request_id,
        it.project_id,
        it.wallet_id,
        it.api_key_id,
        it.pricing_override_id,
        it.discount_numerator,
        it.discount_denominator,
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
        sl.cell AS supplier_cell,
        COALESCE(w.available_atoms, 0) AS wallet_available_atoms,
        public.outbox_shard(it.project_id) AS outbox_shard_id
      FROM jsonb_to_recordset(p_items) AS it(
        charge_id UUID,
        event_id UUID,
        request_id UUID,
        project_id UUID,
        wallet_id UUID,
        api_key_id UUID,
        pricing_override_id UUID,
        discount_numerator NUMERIC,
        discount_denominator NUMERIC,
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
      LEFT JOIN public.wallets w ON w.id = it.wallet_id
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
    w_spend AS (
      SELECT wallet_id AS id, sum(customer_atoms) AS spent
      FROM items
      WHERE customer_atoms > 0
      GROUP BY wallet_id
      ORDER BY wallet_id
    ),
    l_spend AS (
      SELECT lot_id AS id, sum(customer_atoms) AS spent
      FROM items
      WHERE customer_atoms > 0
      GROUP BY lot_id
      ORDER BY lot_id
    ),
    c_spend AS (
      SELECT supplier_account_id AS account_id, sl_currency AS currency, supplier_cell AS cell, sum(supplier_atoms) AS spent
      FROM items
      WHERE supplier_atoms > 0
      GROUP BY supplier_account_id, sl_currency, supplier_cell
      ORDER BY supplier_account_id, sl_currency, supplier_cell
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
      FROM unnest(v_wl_ids, v_wl_needs) AS req(id, need)
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
      FROM unnest(v_sl_ids, v_sl_needs) AS req(id, need)
      WHERE l.id = req.id AND (req.need > 0 OR l.expires_at < p_renew_threshold)
      RETURNING l.id, (extract(epoch FROM l.expires_at) * 1000)::bigint AS expires_ms
    ),
    upd_h AS (
      UPDATE public.billing_holds AS h
      SET state = 'SETTLED', closed_at = now()
      WHERE h.request_id = ANY(v_req_ids)
    ),
    ins_ch AS (
      INSERT INTO public.billing_charges (
        id, request_id, project_id, wallet_id, api_key_id, selected_attempt_id, channel_id,
        model, operation, currency, billing_price_id, cost_version_id,
        pricing_override_id, discount_numerator, discount_denominator,
        usage, usage_digest,
        customer_atoms, funded_atoms, debt_atoms, supplier_cost_atoms, supplier_currency,
        lot_allocations, plugin_audit, charge_policy, outcome, customer_journal_id, supplier_journal_id
      )
      SELECT
        it.charge_id, it.request_id, it.project_id, it.wallet_id, it.api_key_id, it.selected_attempt_id, it.channel_id,
        it.model, it.operation, it.currency, it.billing_price_id, it.cost_version_id,
        it.pricing_override_id, it.discount_numerator, it.discount_denominator,
        it.usage, it.usage_digest,
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
    ),
    ob_ord AS MATERIALIZED (
      SELECT
        it.event_id,
        it.request_id,
        it.project_id,
        jsonb_build_object(
          'charge_id', it.charge_id,
          'request_id', it.request_id,
          'api_key_id', it.api_key_id,
          'attempt_id', it.selected_attempt_id,
          'channel_id', it.channel_id,
          'model', it.model,
          'operation', it.operation,
          'currency', it.currency,
          'pricing_override_id', it.pricing_override_id,
          'usage', it.usage,
          'customer_atoms', it.customer_atoms::text,
          'funded_atoms', it.customer_atoms::text,
          'debt_atoms', '0',
          'supplier_cost_atoms', it.supplier_atoms::text,
          'outcome', 'SETTLED',
          'wallet_available_atoms', it.wallet_available_atoms::text
        ) AS payload
      FROM items it
      ORDER BY it.outbox_shard_id ASC, it.request_id ASC
    )
  SELECT
    COALESCE((SELECT jsonb_object_agg(id::text, expires_ms) FROM upd_wl), '{}'::jsonb),
    COALESCE((SELECT jsonb_object_agg(id::text, expires_ms) FROM upd_sl), '{}'::jsonb),
    COALESCE(ws.ids, '{}'::uuid[]),
    COALESCE(ws.spents, '{}'::numeric[]),
    COALESCE(ls.ids, '{}'::uuid[]),
    COALESCE(ls.spents, '{}'::numeric[]),
    COALESCE(cs.accts, '{}'::uuid[]),
    COALESCE(cs.curs, '{}'::text[]),
    COALESCE(cs.cells, '{}'::smallint[]),
    COALESCE(cs.spents, '{}'::numeric[]),
    COALESCE(ob.event_ids, '{}'::uuid[]),
    COALESCE(ob.req_ids, '{}'::uuid[]),
    COALESCE(ob.proj_ids, '{}'::uuid[]),
    COALESCE(ob.payloads, '{}'::jsonb[])
  INTO
    v_wl_exp,
    v_sl_exp,
    v_w_spend_ids,
    v_w_spend_atoms,
    v_lot_spend_ids,
    v_lot_spend_atoms,
    v_cell_spend_accts,
    v_cell_spend_curs,
    v_cell_spend_nums,
    v_cell_spend_atoms,
    v_ob_event_ids,
    v_ob_req_ids,
    v_ob_proj_ids,
    v_ob_payloads
  FROM (SELECT array_agg(id) AS ids, array_agg(spent) AS spents FROM w_spend) ws
  CROSS JOIN (SELECT array_agg(id) AS ids, array_agg(spent) AS spents FROM l_spend) ls
  CROSS JOIN (
    SELECT
      array_agg(account_id) AS accts,
      array_agg(currency) AS curs,
      array_agg(cell) AS cells,
      array_agg(spent) AS spents
    FROM c_spend
  ) cs
  CROSS JOIN (
    SELECT
      array_agg(event_id) AS event_ids,
      array_agg(request_id) AS req_ids,
      array_agg(project_id) AS proj_ids,
      array_agg(payload) AS payloads
    FROM ob_ord
  ) ob;

  IF cardinality(v_w_spend_ids) > 0 THEN
    WITH locked_w AS MATERIALIZED (
      SELECT w.id
      FROM public.wallets w
      WHERE w.id = ANY(v_w_spend_ids)
      ORDER BY w.id
      FOR NO KEY UPDATE
    )
    UPDATE public.wallets AS w SET
      reserved_atoms = w.reserved_atoms - req.spent,
      spent_atoms = w.spent_atoms + req.spent
    FROM unnest(v_w_spend_ids, v_w_spend_atoms) AS req(id, spent)
    WHERE w.id = req.id AND EXISTS (SELECT 1 FROM locked_w lw WHERE lw.id = w.id);
  END IF;

  IF cardinality(v_lot_spend_ids) > 0 THEN
    WITH locked_l AS MATERIALIZED (
      SELECT l.id
      FROM public.wallet_lots l
      WHERE l.id = ANY(v_lot_spend_ids)
      ORDER BY l.id
      FOR NO KEY UPDATE
    )
    UPDATE public.wallet_lots AS l SET
      reserved_atoms = l.reserved_atoms - req.spent,
      spent_atoms = l.spent_atoms + req.spent
    FROM unnest(v_lot_spend_ids, v_lot_spend_atoms) AS req(id, spent)
    WHERE l.id = req.id AND EXISTS (SELECT 1 FROM locked_l ll WHERE ll.id = l.id);
  END IF;

  IF cardinality(v_cell_spend_accts) > 0 THEN
    WITH locked_c AS MATERIALIZED (
      SELECT c.account_id, c.currency, c.cell
      FROM public.supplier_cells c
      JOIN unnest(v_cell_spend_accts, v_cell_spend_curs, v_cell_spend_nums) AS k(account_id, currency, cell)
        ON c.account_id = k.account_id AND c.currency = k.currency AND c.cell = k.cell
      ORDER BY c.account_id, c.currency, c.cell
      FOR NO KEY UPDATE OF c
    )
    UPDATE public.supplier_cells AS c SET
      reserved_atoms = c.reserved_atoms - req.spent,
      balance_atoms = c.balance_atoms - req.spent,
      spent_atoms = c.spent_atoms + req.spent,
      revision = c.revision + 1
    FROM unnest(v_cell_spend_accts, v_cell_spend_curs, v_cell_spend_nums, v_cell_spend_atoms)
      AS req(account_id, currency, cell, spent)
    WHERE c.account_id = req.account_id
      AND c.currency = req.currency
      AND c.cell = req.cell
      AND EXISTS (
        SELECT 1 FROM locked_c lc
        WHERE lc.account_id = c.account_id AND lc.currency = c.currency AND lc.cell = c.cell
      );
  END IF;

  PERFORM pg_advisory_xact_lock(81442591, 100 + s::integer)
  FROM unnest(v_shards) AS s;

  INSERT INTO public.outbox_events (event_id, kind, request_id, project_id, payload)
  SELECT
    o.event_id,
    'billing.settled',
    o.request_id,
    o.project_id,
    o.payload
  FROM unnest(v_ob_event_ids, v_ob_req_ids, v_ob_proj_ids, v_ob_payloads) AS o(
    event_id,
    request_id,
    project_id,
    payload
  );

  RETURN jsonb_build_object('fast', true, 'wl_expiries', v_wl_exp, 'sl_expiries', v_sl_exp);
END;
$$;
