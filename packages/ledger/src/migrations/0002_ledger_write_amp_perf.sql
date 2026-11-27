ALTER TABLE public.wallet_leases
  ADD COLUMN spent_atoms numeric(38,0) NOT NULL DEFAULT 0,
  ADD COLUMN spent_lot_id uuid NULL,
  ADD CONSTRAINT wallet_leases_spent_check CHECK (
    spent_atoms >= 0
    AND (
      (spent_atoms = 0 AND spent_lot_id IS NULL)
      OR (spent_atoms > 0 AND spent_lot_id IS NOT NULL)
    )
    AND (state = 'ACTIVE' OR (remaining_atoms = 0 AND spent_atoms = 0))
  );

ALTER TABLE public.supplier_leases
  ADD COLUMN spent_atoms numeric(38,0) NOT NULL DEFAULT 0,
  ADD CONSTRAINT supplier_leases_spent_check CHECK (
    spent_atoms >= 0
    AND (state = 'ACTIVE' OR (remaining_atoms = 0 AND spent_atoms = 0))
  );

CREATE INDEX supplier_leases_account_active_idx
  ON public.supplier_leases USING btree (account_id, currency, cell)
  WHERE (state = 'ACTIVE'::text);

DROP INDEX IF EXISTS public.outbox_events_billing_account_idx;
DROP INDEX IF EXISTS public.billing_holds_wallet_lease_idx;
DROP INDEX IF EXISTS public.billing_holds_supplier_lease_idx;
DROP INDEX IF EXISTS public.billing_holds_open_idx;

CREATE INDEX billing_holds_open_idx
  ON public.billing_holds USING btree (expires_at)
  INCLUDE (wallet_lease_id, supplier_lease_id, request_id)
  WHERE (state = ANY (ARRAY['HELD'::text, 'UNKNOWN'::text]));

ALTER TABLE public.billing_holds SET (
  autovacuum_vacuum_scale_factor = 0.01,
  autovacuum_vacuum_threshold = 2000,
  autovacuum_vacuum_cost_limit = 2000,
  autovacuum_vacuum_cost_delay = 2
);

ALTER TABLE public.wallets SET (
  autovacuum_vacuum_scale_factor = 0.0,
  autovacuum_vacuum_threshold = 50,
  autovacuum_vacuum_cost_delay = 0
);

ALTER TABLE public.wallet_lots SET (
  autovacuum_vacuum_scale_factor = 0.0,
  autovacuum_vacuum_threshold = 50,
  autovacuum_vacuum_cost_delay = 0
);

ALTER TABLE public.supplier_cells SET (
  autovacuum_vacuum_scale_factor = 0.0,
  autovacuum_vacuum_threshold = 50,
  autovacuum_vacuum_cost_delay = 0
);

ALTER TABLE public.wallet_leases SET (
  autovacuum_vacuum_scale_factor = 0.0,
  autovacuum_vacuum_threshold = 50,
  autovacuum_vacuum_cost_delay = 0
);

ALTER TABLE public.supplier_leases SET (
  autovacuum_vacuum_scale_factor = 0.0,
  autovacuum_vacuum_threshold = 50,
  autovacuum_vacuum_cost_delay = 0
);

ALTER TABLE public.wallets
  DROP CONSTRAINT wallets_currency_check,
  ADD CONSTRAINT wallets_currency_check CHECK (
    char_length(currency) >= 3 AND char_length(currency) <= 8
    AND ltrim(currency, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ') = ''
  );

ALTER TABLE public.wallet_leases
  DROP CONSTRAINT wallet_leases_currency_check,
  ADD CONSTRAINT wallet_leases_currency_check CHECK (
    char_length(currency) >= 3 AND char_length(currency) <= 8
    AND ltrim(currency, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ') = ''
  );

ALTER TABLE public.supplier_leases
  DROP CONSTRAINT supplier_leases_currency_check,
  ADD CONSTRAINT supplier_leases_currency_check CHECK (
    char_length(currency) >= 3 AND char_length(currency) <= 8
    AND ltrim(currency, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ') = ''
  );

CREATE OR REPLACE FUNCTION public.publish_outbox_batch(
  p_event_ids uuid[],
  p_kinds text[],
  p_request_ids uuid[],
  p_billing_account_ids uuid[],
  p_payloads jsonb[]
) RETURNS void
    LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE
  v_len INTEGER;
  v_acct_ids UUID[];
  v_acct_shards SMALLINT[];
  v_shards SMALLINT[];
  v_shard SMALLINT;
BEGIN
  IF p_event_ids IS NULL THEN
    RETURN;
  END IF;
  v_len := cardinality(p_event_ids);
  IF v_len = 0 THEN
    RETURN;
  END IF;
  IF cardinality(p_kinds) IS DISTINCT FROM v_len
    OR cardinality(p_request_ids) IS DISTINCT FROM v_len
    OR cardinality(p_billing_account_ids) IS DISTINCT FROM v_len
    OR cardinality(p_payloads) IS DISTINCT FROM v_len
  THEN
    RAISE EXCEPTION 'publish_outbox_batch array length mismatch' USING ERRCODE = '2202E';
  END IF;

  SELECT
    array_agg(d.id),
    array_agg(d.shard),
    array_agg(DISTINCT d.shard ORDER BY d.shard)
  INTO v_acct_ids, v_acct_shards, v_shards
  FROM (
    SELECT DISTINCT
      u.billing_account_id AS id,
      public.outbox_shard(u.billing_account_id) AS shard
    FROM unnest(p_billing_account_ids) AS u(billing_account_id)
  ) d;

  FOREACH v_shard IN ARRAY v_shards LOOP
    PERFORM pg_advisory_xact_lock(81442591, 100 + v_shard::integer);
  END LOOP;

  WITH ordered AS MATERIALIZED (
    SELECT
      u.event_id,
      u.kind,
      u.request_id,
      u.billing_account_id,
      sm.shard,
      u.payload,
      u.ord
    FROM unnest(p_event_ids, p_kinds, p_request_ids, p_billing_account_ids, p_payloads)
      WITH ORDINALITY AS u(event_id, kind, request_id, billing_account_id, payload, ord)
    JOIN unnest(v_acct_ids, v_acct_shards) AS sm(id, shard)
      ON sm.id IS NOT DISTINCT FROM u.billing_account_id
    ORDER BY sm.shard ASC, u.request_id ASC, u.ord ASC
  )
  INSERT INTO public.outbox_events (seq, shard, event_id, kind, request_id, billing_account_id, payload)
  SELECT
    nextval('public.outbox_events_seq_seq'),
    ordered.shard,
    ordered.event_id,
    ordered.kind,
    ordered.request_id,
    ordered.billing_account_id,
    ordered.payload
  FROM ordered;
END;
$$;

REVOKE ALL ON FUNCTION public.publish_outbox_batch(uuid[], text[], uuid[], uuid[], jsonb[]) FROM PUBLIC;

DROP TRIGGER order_outbox_publish ON public.outbox_events;

CREATE TRIGGER order_outbox_publish
  BEFORE INSERT ON public.outbox_events
  FOR EACH ROW
  WHEN (NEW.seq IS NULL OR NEW.seq <= 0)
  EXECUTE FUNCTION public.order_outbox_publish();

DROP FUNCTION IF EXISTS public.commit_leased_batch(jsonb, timestamp with time zone, timestamp with time zone, timestamp with time zone);

CREATE OR REPLACE FUNCTION public.commit_leased_batch(
  p_charge_ids uuid[],
  p_event_ids uuid[],
  p_request_ids uuid[],
  p_billing_account_ids uuid[],
  p_wallet_ids uuid[],
  p_api_key_ids uuid[],
  p_pricing_override_ids uuid[],
  p_discount_numerators numeric[],
  p_discount_denominators numeric[],
  p_wallet_lease_ids uuid[],
  p_supplier_lease_ids uuid[],
  p_selected_attempt_ids uuid[],
  p_channel_ids uuid[],
  p_models text[],
  p_operations text[],
  p_currencies text[],
  p_billing_price_ids uuid[],
  p_cost_version_ids uuid[],
  p_usages text[],
  p_usage_digests text[],
  p_customer_atoms numeric[],
  p_supplier_atoms numeric[],
  p_supplier_currencies text[],
  p_plugin_audits text[],
  p_charge_policies text[],
  p_next_expires_at timestamp with time zone,
  p_renew_threshold timestamp with time zone,
  p_now timestamp with time zone
) RETURNS jsonb
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE
  v_item_count INTEGER;
  v_ok_holds INTEGER;
  v_needs_slow BOOLEAN;
  v_wl_exp JSONB;
  v_sl_exp JSONB;
  v_ts_bytes BYTEA;
  v_hold_ctids TID[];
  v_wl_ids UUID[];
  v_wl_needs NUMERIC[];
  v_wl_ctids TID[];
  v_wl_lot_ids UUID[];
  v_wl_lot_amounts NUMERIC[];
  v_wl_lot_exp_ms JSONB[];
  v_sl_ids UUID[];
  v_sl_needs NUMERIC[];
  v_sl_ctids TID[];
  v_sl_accts UUID[];
  v_sl_curs TEXT[];
  v_w_ids UUID[];
  v_w_avail_ids UUID[];
  v_w_avail_atoms NUMERIC[];
  v_ob_payloads JSONB[];
BEGIN
  IF p_request_ids IS NULL THEN
    RETURN jsonb_build_object('fast', true, 'wl_expiries', '{}'::jsonb, 'sl_expiries', '{}'::jsonb);
  END IF;
  v_item_count := cardinality(p_request_ids);
  IF v_item_count = 0 THEN
    RETURN jsonb_build_object('fast', true, 'wl_expiries', '{}'::jsonb, 'sl_expiries', '{}'::jsonb);
  END IF;

  WITH base AS MATERIALIZED (
    SELECT
      x.request_id,
      x.wallet_id,
      x.wallet_lease_id,
      x.supplier_lease_id,
      x.customer_atoms,
      x.supplier_atoms
    FROM unnest(
      p_request_ids,
      p_wallet_ids,
      p_wallet_lease_ids,
      p_supplier_lease_ids,
      p_customer_atoms,
      p_supplier_atoms
    ) AS x(request_id, wallet_id, wallet_lease_id, supplier_lease_id, customer_atoms, supplier_atoms)
  ),
  wl_agg AS (
    SELECT wallet_lease_id AS id, sum(customer_atoms) AS need
    FROM base
    GROUP BY wallet_lease_id
    ORDER BY wallet_lease_id
  ),
  sl_agg AS (
    SELECT supplier_lease_id AS id, sum(supplier_atoms) AS need
    FROM base
    GROUP BY supplier_lease_id
    ORDER BY supplier_lease_id
  )
  SELECT
    wl.ids,
    wl.needs,
    sl.ids,
    sl.needs,
    s.w_ids,
    s.needs_slow
  INTO
    v_wl_ids,
    v_wl_needs,
    v_sl_ids,
    v_sl_needs,
    v_w_ids,
    v_needs_slow
  FROM (SELECT array_agg(id) AS ids, array_agg(need) AS needs FROM wl_agg) wl
  CROSS JOIN (SELECT array_agg(id) AS ids, array_agg(need) AS needs FROM sl_agg) sl
  CROSS JOIN (
    SELECT
      array_agg(DISTINCT b.wallet_id ORDER BY b.wallet_id) AS w_ids,
      EXISTS (
        SELECT 1
        FROM (SELECT wallet_id FROM base GROUP BY wallet_id HAVING count(DISTINCT wallet_lease_id) > 1) wm
      ) OR (count(DISTINCT b.request_id) <> v_item_count) AS needs_slow
    FROM base b
  ) s;

  IF v_needs_slow THEN
    RETURN jsonb_build_object('fast', false);
  END IF;

  WITH locked AS MATERIALIZED (
    SELECT h.ctid, h.request_id, h.state, h.wallet_lease_id, h.supplier_lease_id, h.key_reserved_atoms, h.supplier_holds
    FROM public.billing_holds h
    WHERE h.request_id = ANY(p_request_ids)
    ORDER BY h.request_id
    FOR NO KEY UPDATE
  )
  SELECT
    count(*) FILTER (
      WHERE l.state = 'HELD'
        AND l.key_reserved_atoms = 0
        AND l.wallet_lease_id IS NOT DISTINCT FROM u.wallet_lease_id
        AND l.supplier_lease_id IS NOT DISTINCT FROM u.supplier_lease_id
        AND (
          CASE
            WHEN jsonb_array_length(l.supplier_holds) = 1 THEN
              (l.supplier_holds->0->>'state') = 'HELD'
              AND (l.supplier_holds->0->>'attempt_id') = u.selected_attempt_id::text
              AND (l.supplier_holds->0->>'channel_id') = u.channel_id::text
              AND (l.supplier_holds->0->>'currency') = u.supplier_currency
            ELSE
              l.supplier_holds @> jsonb_build_array(jsonb_build_object(
                'attempt_id', u.selected_attempt_id,
                'channel_id', u.channel_id,
                'currency', u.supplier_currency,
                'state', 'HELD'
              ))
          END
        )
    ),
    array_agg(l.ctid)
  INTO v_ok_holds, v_hold_ctids
  FROM unnest(
    p_request_ids,
    p_wallet_lease_ids,
    p_supplier_lease_ids,
    p_selected_attempt_ids,
    p_channel_ids,
    p_supplier_currencies
  ) AS u(request_id, wallet_lease_id, supplier_lease_id, selected_attempt_id, channel_id, supplier_currency)
  JOIN locked l ON l.request_id = u.request_id;

  IF v_ok_holds <> v_item_count THEN
    RETURN jsonb_build_object('fast', false);
  END IF;

  WITH locked AS MATERIALIZED (
    SELECT
      wl.ctid,
      wl.id,
      wl.state,
      wl.earliest_lot_expires_at,
      wl.remaining_atoms,
      wl.spent_lot_id,
      jsonb_array_length(wl.lot_allocations) AS lot_len,
      (wl.lot_allocations->0->>'lot_id')::uuid AS lot_id,
      COALESCE((wl.lot_allocations->0->>'amount_atoms')::numeric, 0) AS lot_amount,
      wl.lot_allocations->0->'expires_at_ms' AS lot_exp_ms
    FROM public.wallet_leases wl
    WHERE wl.id = ANY(v_wl_ids)
    ORDER BY wl.id
    FOR NO KEY UPDATE
  )
  SELECT
    bool_or(
      wl.id IS NULL
      OR wl.state <> 'ACTIVE'
      OR (wl.earliest_lot_expires_at IS NOT NULL AND wl.earliest_lot_expires_at <= p_now)
      OR req.need > wl.remaining_atoms
      OR (
        req.need > 0 AND (
          wl.lot_len <> 1
          OR wl.lot_id IS NULL
          OR req.need > wl.lot_amount
          OR (wl.spent_lot_id IS NOT NULL AND wl.spent_lot_id <> wl.lot_id)
        )
      )
    ),
    array_agg(wl.ctid ORDER BY req.ord),
    array_agg(wl.lot_id ORDER BY req.ord),
    array_agg(wl.lot_amount ORDER BY req.ord),
    array_agg(wl.lot_exp_ms ORDER BY req.ord)
  INTO v_needs_slow, v_wl_ctids, v_wl_lot_ids, v_wl_lot_amounts, v_wl_lot_exp_ms
  FROM unnest(v_wl_ids, v_wl_needs) WITH ORDINALITY AS req(lid, need, ord)
  LEFT JOIN locked wl ON wl.id = req.lid;

  IF COALESCE(v_needs_slow, true) THEN
    RETURN jsonb_build_object('fast', false);
  END IF;

  WITH locked AS MATERIALIZED (
    SELECT sl.ctid, sl.id, sl.state, sl.remaining_atoms, sl.account_id, sl.currency
    FROM public.supplier_leases sl
    WHERE sl.id = ANY(v_sl_ids)
    ORDER BY sl.id
    FOR NO KEY UPDATE
  )
  SELECT
    bool_or(
      sl.id IS NULL
      OR sl.state <> 'ACTIVE'
      OR req.need > sl.remaining_atoms
    ),
    array_agg(sl.ctid ORDER BY req.ord),
    array_agg(sl.account_id ORDER BY req.ord),
    array_agg(sl.currency ORDER BY req.ord)
  INTO v_needs_slow, v_sl_ctids, v_sl_accts, v_sl_curs
  FROM unnest(v_sl_ids, v_sl_needs) WITH ORDINALITY AS req(lid, need, ord)
  LEFT JOIN locked sl ON sl.id = req.lid;

  IF COALESCE(v_needs_slow, true) THEN
    RETURN jsonb_build_object('fast', false);
  END IF;

  SELECT
    COALESCE(bool_or(w.debt_atoms > 0), false),
    COALESCE(array_agg(w.id), '{}'::uuid[]),
    COALESCE(array_agg(w.available_atoms), '{}'::numeric[])
  INTO v_needs_slow, v_w_avail_ids, v_w_avail_atoms
  FROM public.wallets w
  WHERE w.id = ANY(v_w_ids);

  IF v_needs_slow THEN
    RETURN jsonb_build_object('fast', false);
  END IF;

  v_ts_bytes := substring(int8send(floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint) FROM 3 FOR 6);

  WITH
    items AS MATERIALIZED (
      SELECT
        u.charge_id,
        u.request_id,
        u.billing_account_id,
        u.wallet_id,
        u.api_key_id,
        u.pricing_override_id,
        u.discount_numerator,
        u.discount_denominator,
        u.selected_attempt_id,
        u.channel_id,
        u.model,
        u.operation,
        u.currency,
        u.billing_price_id,
        u.cost_version_id,
        u.usage::jsonb AS usage,
        u.usage_digest,
        u.customer_atoms,
        u.supplier_atoms,
        u.supplier_currency,
        u.plugin_audit::jsonb AS plugin_audit,
        u.charge_policy,
        u.ord,
        wl.lot_id,
        sl.account_id AS supplier_account_id,
        sl.currency AS sl_currency
      FROM unnest(
        p_charge_ids,
        p_request_ids,
        p_billing_account_ids,
        p_wallet_ids,
        p_api_key_ids,
        p_pricing_override_ids,
        p_discount_numerators,
        p_discount_denominators,
        p_wallet_lease_ids,
        p_supplier_lease_ids,
        p_selected_attempt_ids,
        p_channel_ids,
        p_models,
        p_operations,
        p_currencies,
        p_billing_price_ids,
        p_cost_version_ids,
        p_usages,
        p_usage_digests,
        p_customer_atoms,
        p_supplier_atoms,
        p_supplier_currencies,
        p_plugin_audits,
        p_charge_policies
      ) WITH ORDINALITY AS u(
        charge_id,
        request_id,
        billing_account_id,
        wallet_id,
        api_key_id,
        pricing_override_id,
        discount_numerator,
        discount_denominator,
        wallet_lease_id,
        supplier_lease_id,
        selected_attempt_id,
        channel_id,
        model,
        operation,
        currency,
        billing_price_id,
        cost_version_id,
        usage,
        usage_digest,
        customer_atoms,
        supplier_atoms,
        supplier_currency,
        plugin_audit,
        charge_policy,
        ord
      )
      JOIN unnest(v_wl_ids, v_wl_lot_ids) AS wl(id, lot_id) ON wl.id = u.wallet_lease_id
      JOIN unnest(v_sl_ids, v_sl_accts, v_sl_curs) AS sl(id, account_id, currency) ON sl.id = u.supplier_lease_id
    ),
    cust_groups AS MATERIALIZED (
      SELECT
        g.wallet_id,
        g.currency,
        encode(
          v_ts_bytes || set_byte(substring(g.rnd FROM 7 FOR 10), 0, (get_byte(g.rnd, 6) & 15) | 112),
          'hex'
        )::uuid AS journal_id,
        g.single_req_id,
        g.total_atoms,
        g.lot_id
      FROM (
        SELECT
          wallet_id,
          currency,
          uuid_send(gen_random_uuid()) AS rnd,
          CASE WHEN count(*) = 1 THEN min(request_id::text)::uuid ELSE NULL END AS single_req_id,
          sum(customer_atoms) AS total_atoms,
          min(lot_id::text)::uuid AS lot_id
        FROM items
        WHERE customer_atoms > 0
        GROUP BY wallet_id, currency
      ) g
    ),
    sup_groups AS MATERIALIZED (
      SELECT
        g.account_id,
        g.currency,
        encode(
          v_ts_bytes || set_byte(substring(g.rnd FROM 7 FOR 10), 0, (get_byte(g.rnd, 6) & 15) | 112),
          'hex'
        )::uuid AS journal_id,
        g.single_req_id,
        g.single_attempt_id,
        g.total_atoms
      FROM (
        SELECT
          supplier_account_id AS account_id,
          sl_currency AS currency,
          uuid_send(gen_random_uuid()) AS rnd,
          CASE WHEN count(*) = 1 THEN min(request_id::text)::uuid ELSE NULL END AS single_req_id,
          CASE WHEN count(*) = 1 THEN min(selected_attempt_id::text)::uuid ELSE NULL END AS single_attempt_id,
          sum(supplier_atoms) AS total_atoms
        FROM items
        WHERE supplier_atoms > 0
        GROUP BY supplier_account_id, sl_currency
      ) g
    ),
    upd_wl AS (
      UPDATE public.wallet_leases AS l SET
        lot_allocations = CASE
          WHEN req.need = 0 THEN l.lot_allocations
          WHEN req.lot_amount - req.need > 0 THEN jsonb_build_array(
            jsonb_build_object(
              'lot_id', req.lot_id::text,
              'amount_atoms', (req.lot_amount - req.need)::text,
              'expires_at_ms', req.lot_exp_ms
            )
          )
          ELSE '[]'::jsonb
        END,
        remaining_atoms = l.remaining_atoms - req.need,
        spent_atoms = l.spent_atoms + req.need,
        spent_lot_id = CASE
          WHEN req.need > 0 THEN req.lot_id
          ELSE l.spent_lot_id
        END,
        earliest_lot_expires_at = CASE
          WHEN req.need = 0 THEN l.earliest_lot_expires_at
          WHEN req.lot_amount - req.need > 0 THEN l.earliest_lot_expires_at
          ELSE NULL
        END,
        expires_at = CASE
          WHEN l.expires_at < p_renew_threshold THEN GREATEST(l.expires_at, p_next_expires_at)
          ELSE l.expires_at
        END,
        updated_at = now()
      FROM unnest(v_wl_ctids, v_wl_needs, v_wl_lot_ids, v_wl_lot_amounts, v_wl_lot_exp_ms)
        AS req(ctid, need, lot_id, lot_amount, lot_exp_ms)
      WHERE l.ctid = req.ctid AND (req.need > 0 OR l.expires_at < p_renew_threshold)
      RETURNING l.id, (extract(epoch FROM l.expires_at) * 1000)::bigint AS expires_ms
    ),
    upd_sl AS (
      UPDATE public.supplier_leases AS l SET
        remaining_atoms = l.remaining_atoms - req.need,
        spent_atoms = l.spent_atoms + req.need,
        expires_at = CASE
          WHEN l.expires_at < p_renew_threshold THEN GREATEST(l.expires_at, p_next_expires_at)
          ELSE l.expires_at
        END,
        updated_at = now()
      FROM unnest(v_sl_ctids, v_sl_needs) AS req(ctid, need)
      WHERE l.ctid = req.ctid AND (req.need > 0 OR l.expires_at < p_renew_threshold)
      RETURNING l.id, (extract(epoch FROM l.expires_at) * 1000)::bigint AS expires_ms
    ),
    upd_h AS (
      UPDATE public.billing_holds AS h
      SET state = 'SETTLED', closed_at = now()
      WHERE h.ctid = ANY(v_hold_ctids)
    ),
    ins_ch AS (
      INSERT INTO public.billing_charges (
        id, request_id, billing_account_id, wallet_id, api_key_id, selected_attempt_id, channel_id,
        model, operation, currency, billing_price_id, cost_version_id,
        pricing_override_id, discount_numerator, discount_denominator,
        usage, usage_digest,
        customer_atoms, funded_atoms, debt_atoms, supplier_cost_atoms, supplier_currency,
        lot_allocations, plugin_audit, charge_policy, outcome, customer_journal_id, supplier_journal_id
      )
      SELECT
        it.charge_id, it.request_id, it.billing_account_id, it.wallet_id, it.api_key_id, it.selected_attempt_id, it.channel_id,
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
    )
  SELECT
    COALESCE((SELECT jsonb_object_agg(id::text, expires_ms) FROM upd_wl), '{}'::jsonb),
    COALESCE((SELECT jsonb_object_agg(id::text, expires_ms) FROM upd_sl), '{}'::jsonb),
    (
      SELECT array_agg(
        jsonb_build_object(
          'charge_id', o.charge_id,
          'request_id', o.request_id,
          'api_key_id', o.api_key_id,
          'attempt_id', o.selected_attempt_id,
          'channel_id', o.channel_id,
          'model', o.model,
          'operation', o.operation,
          'currency', o.currency,
          'pricing_override_id', o.pricing_override_id,
          'usage', o.usage,
          'customer_atoms', o.customer_atoms::text,
          'funded_atoms', o.customer_atoms::text,
          'debt_atoms', '0',
          'supplier_cost_atoms', o.supplier_atoms::text,
          'outcome', 'SETTLED',
          'wallet_available_atoms', COALESCE(wa.available_atoms, 0)::text
        )
        ORDER BY o.ord
      )
      FROM items o
      LEFT JOIN unnest(v_w_avail_ids, v_w_avail_atoms) AS wa(id, available_atoms) ON wa.id = o.wallet_id
    )
  INTO v_wl_exp, v_sl_exp, v_ob_payloads;

  PERFORM public.publish_outbox_batch(
    p_event_ids,
    array_fill('billing.settled'::text, ARRAY[v_item_count]),
    p_request_ids,
    p_billing_account_ids,
    v_ob_payloads
  );

  RETURN jsonb_build_object('fast', true, 'wl_expiries', v_wl_exp, 'sl_expiries', v_sl_exp);
END;
$$;

DROP FUNCTION IF EXISTS public.acquire_leased_holds(text, uuid[], uuid[], jsonb, boolean);

CREATE OR REPLACE FUNCTION public.acquire_leased_holds(
  p_node_id text,
  p_wallet_lease_ids uuid[],
  p_supplier_lease_ids uuid[],
  p_check_debt_grants boolean,
  p_request_ids uuid[],
  p_billing_account_ids uuid[],
  p_wallet_ids uuid[],
  p_api_key_ids uuid[],
  p_pricing_override_ids uuid[],
  p_discount_numerators numeric[],
  p_discount_denominators numeric[],
  p_operations text[],
  p_models text[],
  p_fingerprints text[],
  p_held_atoms numeric[],
  p_supplier_holds text[],
  p_item_wallet_lease_ids uuid[],
  p_item_supplier_lease_ids uuid[],
  p_expires_ats timestamp with time zone[]
) RETURNS uuid[]
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE
  v_locked_wl_ids uuid[];
  v_locked_sl_ids uuid[];
  v_now timestamp with time zone;
  v_admitted uuid[];
BEGIN
  IF p_request_ids IS NULL OR cardinality(p_request_ids) = 0 THEN
    RETURN '{}'::uuid[];
  END IF;

  WITH local_wallet_leases AS MATERIALIZED (
    SELECT l.id, l.wallet_id
    FROM public.wallet_leases l
    WHERE l.id = ANY(p_wallet_lease_ids)
      AND l.node_id = p_node_id
      AND l.state = 'ACTIVE' AND l.expires_at > clock_timestamp()
      AND (l.earliest_lot_expires_at IS NULL OR l.earliest_lot_expires_at > clock_timestamp())
    ORDER BY l.id FOR KEY SHARE OF l
  ), locked_supplier_leases AS MATERIALIZED (
    SELECT l.id
    FROM public.supplier_leases l
    WHERE EXISTS (SELECT 1 FROM local_wallet_leases)
      AND l.id = ANY(p_supplier_lease_ids)
      AND l.node_id = p_node_id
      AND l.state = 'ACTIVE' AND l.expires_at > clock_timestamp()
    ORDER BY l.id FOR KEY SHARE OF l
  ), locked_wallet_leases AS MATERIALIZED (
    SELECT l.id
    FROM local_wallet_leases l
    JOIN public.wallets w ON w.id = l.wallet_id
    WHERE EXISTS (SELECT 1 FROM locked_supplier_leases)
    ORDER BY w.id, l.id FOR SHARE OF w
  )
  SELECT
    COALESCE(ARRAY(SELECT id FROM locked_wallet_leases), '{}'::uuid[]),
    COALESCE(ARRAY(SELECT id FROM locked_supplier_leases), '{}'::uuid[])
  INTO v_locked_wl_ids, v_locked_sl_ids;

  IF cardinality(v_locked_wl_ids) = 0 OR cardinality(v_locked_sl_ids) = 0 THEN
    RETURN '{}'::uuid[];
  END IF;

  v_now := clock_timestamp();

  WITH active_wallet_leases AS MATERIALIZED (
    SELECT l.id
    FROM unnest(v_locked_wl_ids) AS acquired(id)
    CROSS JOIN LATERAL (
      SELECT wl.id, wl.wallet_id, wl.state, wl.expires_at, wl.earliest_lot_expires_at
      FROM public.wallet_leases wl
      WHERE wl.id = acquired.id
      LIMIT 1
    ) l
    JOIN public.wallets w ON w.id = l.wallet_id
    CROSS JOIN LATERAL (
      SELECT CASE
        WHEN w.debt_atoms = 0 OR w.freeze_reason IS NOT NULL THEN 0::numeric
        ELSE (
          SELECT COALESCE(SUM(wl.spent_atoms), 0)
          FROM public.wallet_leases wl
          WHERE wl.wallet_id = w.id AND wl.state = 'ACTIVE'
        )
      END AS spent
    ) ds
    WHERE l.state = 'ACTIVE' AND l.expires_at > v_now
      AND (l.earliest_lot_expires_at IS NULL OR l.earliest_lot_expires_at > v_now)
      AND w.freeze_reason IS NULL
      AND CASE
        WHEN w.debt_atoms = 0 THEN true
        WHEN w.reserved_atoms - ds.spent >= w.debt_atoms THEN true
        WHEN p_check_debt_grants THEN w.available_atoms + (w.reserved_atoms - ds.spent) - w.debt_atoms >= (
          SELECT COALESCE(SUM(lot.available_atoms), 0)
          FROM public.wallet_lots lot
          WHERE lot.wallet_id = w.id AND lot.available_atoms > 0
            AND lot.expires_at <= v_now
        )
        ELSE false
      END
  ), active_supplier_leases AS MATERIALIZED (
    SELECT l.id
    FROM public.supplier_leases l
    JOIN public.upstream_accounts a ON a.id = l.account_id
    WHERE l.id = ANY(v_locked_sl_ids)
      AND l.state = 'ACTIVE' AND l.expires_at > v_now AND a.state = 'ACTIVE'
  ), ins AS (
    INSERT INTO public.billing_holds (
      request_id, billing_account_id, wallet_id, api_key_id, key_reserved_atoms,
      pricing_override_id, discount_numerator, discount_denominator,
      operation, model, idempotency_key, fingerprint,
      held_atoms, credit_atoms, lot_allocations, supplier_holds, wallet_lease_id, supplier_lease_id, state, expires_at
    )
    SELECT
      u.request_id, u.billing_account_id, u.wallet_id, u.api_key_id, 0,
      u.pricing_override_id, u.discount_numerator, u.discount_denominator,
      u.operation, u.model, NULL, u.fingerprint,
      u.held_atoms, 0, '[]'::jsonb, u.supplier_holds::jsonb, u.wallet_lease_id, u.supplier_lease_id, 'HELD', u.expires_at
    FROM unnest(
      p_request_ids,
      p_billing_account_ids,
      p_wallet_ids,
      p_api_key_ids,
      p_pricing_override_ids,
      p_discount_numerators,
      p_discount_denominators,
      p_operations,
      p_models,
      p_fingerprints,
      p_held_atoms,
      p_supplier_holds,
      p_item_wallet_lease_ids,
      p_item_supplier_lease_ids,
      p_expires_ats
    ) AS u(
      request_id,
      billing_account_id,
      wallet_id,
      api_key_id,
      pricing_override_id,
      discount_numerator,
      discount_denominator,
      operation,
      model,
      fingerprint,
      held_atoms,
      supplier_holds,
      wallet_lease_id,
      supplier_lease_id,
      expires_at
    )
    JOIN active_wallet_leases awl ON awl.id = u.wallet_lease_id
    JOIN active_supplier_leases asl ON asl.id = u.supplier_lease_id
    RETURNING request_id
  )
  SELECT COALESCE(array_agg(request_id), '{}'::uuid[])
  INTO v_admitted
  FROM ins;

  RETURN COALESCE(v_admitted, '{}'::uuid[]);
END;
$$;
