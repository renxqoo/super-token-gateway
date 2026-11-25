SET LOCAL check_function_bodies = false;

CREATE FUNCTION public.check_ledger_journal_balance() RETURNS trigger
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

CREATE FUNCTION public.commit_leased_batch(p_items jsonb, p_next_expires_at timestamp with time zone, p_renew_threshold timestamp with time zone, p_now timestamp with time zone) RETURNS jsonb
    LANGUAGE plpgsql
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
  v_ob_account_ids UUID[];
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
      x.billing_account_id,
      x.wallet_id,
      x.wallet_lease_id,
      x.supplier_lease_id,
      x.customer_atoms,
      x.supplier_atoms
    FROM jsonb_to_recordset(p_items) AS x(
      request_id UUID,
      billing_account_id UUID,
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
    array_agg(DISTINCT public.outbox_shard(it.billing_account_id) ORDER BY public.outbox_shard(it.billing_account_id)),
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
        it.billing_account_id,
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
        public.outbox_shard(it.billing_account_id) AS outbox_shard_id
      FROM jsonb_to_recordset(p_items) AS it(
        charge_id UUID,
        event_id UUID,
        request_id UUID,
        billing_account_id UUID,
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
    ),
    ob_ord AS MATERIALIZED (
      SELECT
        it.event_id,
        it.request_id,
        it.billing_account_id,
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
    v_ob_account_ids,
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
      array_agg(billing_account_id) AS proj_ids,
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

  INSERT INTO public.outbox_events (event_id, kind, request_id, billing_account_id, payload)
  SELECT
    o.event_id,
    'billing.settled',
    o.request_id,
    o.billing_account_id,
    o.payload
  FROM unnest(v_ob_event_ids, v_ob_req_ids, v_ob_account_ids, v_ob_payloads) AS o(
    event_id,
    request_id,
    billing_account_id,
    payload
  );

  RETURN jsonb_build_object('fast', true, 'wl_expiries', v_wl_exp, 'sl_expiries', v_sl_exp);
END;
$$;

CREATE FUNCTION public.immutable_record() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
BEGIN
  RAISE EXCEPTION 'immutable record' USING ERRCODE = 'P0403';
END $$;

CREATE FUNCTION public.keep_plugin_revision() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public'
    AS $$
BEGIN
  IF (to_jsonb(NEW) - 'enabled') IS DISTINCT FROM (to_jsonb(OLD) - 'enabled') THEN
    RAISE EXCEPTION 'Plugin revision fields are immutable' USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END;
$$;

CREATE FUNCTION public.operations_health(p_consumer text) RETURNS TABLE(expired_hold double precision, customer_unknown double precision, supplier_unknown double precision, expired_wallet_lease double precision, expired_supplier_lease double precision, outbox_head double precision, admin_audit_pending double precision)
    LANGUAGE sql STABLE SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'pg_temp'
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

CREATE FUNCTION public.order_outbox_publish() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE
  v_bytes BYTEA;
BEGIN
  IF NEW.billing_account_id IS NULL THEN
    NEW.shard := 0::smallint;
  ELSE
    v_bytes := uuid_send(NEW.billing_account_id);
    NEW.shard := (((get_byte(v_bytes, 0) >> 4) # (get_byte(v_bytes, 15) & 15)))::smallint;
  END IF;
  PERFORM pg_advisory_xact_lock(81442591, 100 + NEW.shard::integer);
  IF NEW.seq IS NULL OR NEW.seq <= 0 THEN
    NEW.seq := nextval('public.outbox_events_seq_seq');
  END IF;
  RETURN NEW;
END;
$$;

CREATE FUNCTION public.outbox_shard(p_billing_account_id uuid) RETURNS smallint
    LANGUAGE sql IMMUTABLE
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
  SELECT CASE
    WHEN p_billing_account_id IS NULL THEN 0::smallint
    ELSE (((get_byte(uuid_send(p_billing_account_id), 0) >> 4) # (get_byte(uuid_send(p_billing_account_id), 15) & 15)))::smallint
  END;
$$;

CREATE FUNCTION public.prepare_ledger_month(p_month date) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'pg_temp'
    AS $_$
DECLARE
  wanted_month DATE := date_trunc('month', clock_timestamp() AT TIME ZONE 'UTC')::DATE;
  part_name TEXT;
  first_day TIMESTAMPTZ;
  next_day TIMESTAMPTZ;
BEGIN
  IF p_month IS NULL OR (p_month <> wanted_month AND p_month <> (wanted_month + INTERVAL '1 month')::DATE) THEN
    RAISE EXCEPTION 'only the current and next UTC month may be prepared' USING ERRCODE = 'P0422';
  END IF;
  part_name := 'ledger_entries_' || to_char(p_month, 'YYYYMM');
  first_day := p_month::TIMESTAMP AT TIME ZONE 'UTC';
  next_day := (p_month + INTERVAL '1 month')::TIMESTAMP AT TIME ZONE 'UTC';
  IF EXISTS (SELECT 1 FROM pg_inherits WHERE inhrelid = to_regclass('public.' || part_name)
    AND inhparent = 'public.ledger_entries'::regclass) THEN
    RETURN;
  END IF;
  LOCK TABLE public.ledger_entries IN ACCESS EXCLUSIVE MODE;
  IF EXISTS (SELECT 1 FROM pg_inherits WHERE inhrelid = to_regclass('public.' || part_name)
    AND inhparent = 'public.ledger_entries'::regclass) THEN
    RETURN;
  END IF;
  EXECUTE format('CREATE TABLE public.%I (LIKE public.ledger_entries INCLUDING DEFAULTS INCLUDING CONSTRAINTS)', part_name);
  ALTER TABLE public.ledger_entries_default DISABLE TRIGGER immutable_ledger_entries;
  EXECUTE format(
    'WITH moved AS (DELETE FROM public.ledger_entries_default WHERE created_at >= $1 AND created_at < $2 RETURNING *) INSERT INTO public.%I SELECT * FROM moved',
    part_name
  ) USING first_day, next_day;
  ALTER TABLE public.ledger_entries_default ENABLE TRIGGER immutable_ledger_entries;
  EXECUTE format('ALTER TABLE public.ledger_entries ATTACH PARTITION public.%I FOR VALUES FROM (%L) TO (%L)', part_name, first_day, next_day);
END;
$_$;

CREATE FUNCTION public.refresh_config_epoch_lineage() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public'
    AS $$
BEGIN
  IF NEW.version IS DISTINCT FROM OLD.version AND NEW.lineage_token IS NOT DISTINCT FROM OLD.lineage_token THEN
    NEW.lineage_token := gen_random_uuid();
  END IF;
  RETURN NEW;
END;
$$;

CREATE FUNCTION public.reject_revoked_plugin_activation() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public'
    AS $$
BEGIN
  IF NEW.enabled AND EXISTS (SELECT 1 FROM public.plugin_revocations WHERE plugin_id = NEW.id) THEN
    RAISE EXCEPTION 'Revoked plugin revisions cannot be enabled' USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TABLE public.admin_audit_events (
    request_id uuid NOT NULL,
    phase text NOT NULL,
    actor text NOT NULL,
    action text NOT NULL,
    target text NOT NULL,
    status smallint,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT admin_audit_events_check CHECK (((phase = 'STARTED'::text) = (status IS NULL))),
    CONSTRAINT admin_audit_events_phase_check CHECK ((phase = ANY (ARRAY['STARTED'::text, 'COMPLETED'::text]))),
    CONSTRAINT admin_audit_events_status_check CHECK (((status >= 100) AND (status <= 599)))
);

CREATE TABLE public.admin_audit_pending (
    request_id uuid NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL
);

CREATE TABLE public.api_keys (
    id uuid NOT NULL,
    billing_account_id uuid NOT NULL,
    name text NOT NULL,
    verifier text NOT NULL,
    prefix text NOT NULL,
    role text DEFAULT 'INVOKER'::text NOT NULL,
    allowed_cidrs text[] DEFAULT '{}'::text[] NOT NULL,
    models text[] DEFAULT '{}'::text[] NOT NULL,
    operations text[] DEFAULT '{}'::text[] NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    expires_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    spend_limit_atoms numeric(38,0),
    spend_limit_window text DEFAULT 'LIFETIME'::text NOT NULL,
    spend_window_start timestamp with time zone DEFAULT now() NOT NULL,
    spent_atoms numeric(38,0) DEFAULT 0 NOT NULL,
    reserved_atoms numeric(38,0) DEFAULT 0 NOT NULL,
    source_revision integer DEFAULT 0 NOT NULL,
    resource_scope_id uuid,
    CONSTRAINT api_keys_reserved_atoms_check CHECK ((reserved_atoms >= (0)::numeric)),
    CONSTRAINT api_keys_role_check CHECK ((role = ANY (ARRAY['ADMIN'::text, 'BILLING'::text, 'INVOKER'::text, 'VIEWER'::text]))),
    CONSTRAINT api_keys_source_revision_check CHECK ((source_revision >= 0)),
    CONSTRAINT api_keys_spend_limit_atoms_check CHECK (((spend_limit_atoms IS NULL) OR (spend_limit_atoms >= (0)::numeric))),
    CONSTRAINT api_keys_spend_limit_window_check CHECK ((spend_limit_window = ANY (ARRAY['LIFETIME'::text, 'DAILY'::text, 'MONTHLY'::text]))),
    CONSTRAINT api_keys_spent_atoms_check CHECK ((spent_atoms >= (0)::numeric))
);

CREATE TABLE public.async_tasks (
    id text NOT NULL,
    request_id uuid NOT NULL,
    charge_id uuid,
    billing_account_id uuid NOT NULL,
    api_key_id uuid,
    channel_id uuid NOT NULL,
    model text NOT NULL,
    operation text NOT NULL,
    upstream_task_id text NOT NULL,
    status text NOT NULL,
    user_cancelled boolean DEFAULT false NOT NULL,
    upstream_billed boolean DEFAULT false NOT NULL,
    failure_category text,
    refund_eligibility text DEFAULT 'PENDING'::text NOT NULL,
    refund_id uuid,
    result_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    completed_at timestamp with time zone,
    CONSTRAINT async_tasks_failure_category_check CHECK (((failure_category IS NULL) OR (failure_category = ANY (ARRAY['UPSTREAM_SYSTEM'::text, 'CONTENT_POLICY'::text, 'INVALID_INPUT'::text, 'USER_CANCELLED'::text, 'UPSTREAM_BILLED'::text, 'TIMEOUT'::text, 'UNKNOWN'::text])))),
    CONSTRAINT async_tasks_refund_eligibility_check CHECK ((refund_eligibility = ANY (ARRAY['PENDING'::text, 'AUTO_REFUNDED'::text, 'DENIED_USER_CANCELLED'::text, 'DENIED_POLICY_OR_CLIENT'::text, 'DENIED_UPSTREAM_BILLED'::text, 'DENIED_ABUSE_LIMIT'::text, 'MANUAL_REVIEW'::text, 'NOT_APPLICABLE'::text]))),
    CONSTRAINT async_tasks_status_check CHECK ((status = ANY (ARRAY['QUEUED'::text, 'PROCESSING'::text, 'SUCCEEDED'::text, 'FAILED'::text, 'CANCELLED'::text])))
);

CREATE TABLE public.billing_account_pricing_overrides (
    id uuid NOT NULL,
    billing_account_id uuid NOT NULL,
    model_name text NOT NULL,
    operation text DEFAULT '*'::text NOT NULL,
    override_kind text NOT NULL,
    billing_price_id uuid,
    multiplier_numerator numeric(38,0),
    multiplier_denominator numeric(38,0),
    enabled boolean DEFAULT true NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    source_revision integer DEFAULT 0 NOT NULL,
    CONSTRAINT billing_account_pricing_overrides_override_kind_check CHECK ((override_kind = ANY (ARRAY['EXACT_PRICE'::text, 'MULTIPLIER'::text]))),
    CONSTRAINT billing_account_pricing_overrides_shape_chk CHECK ((((override_kind = 'EXACT_PRICE'::text) AND (billing_price_id IS NOT NULL) AND (multiplier_numerator IS NULL) AND (multiplier_denominator IS NULL)) OR ((override_kind = 'MULTIPLIER'::text) AND (billing_price_id IS NULL) AND (multiplier_numerator > (0)::numeric) AND (multiplier_denominator > (0)::numeric)))),
    CONSTRAINT billing_account_pricing_overrides_source_revision_check CHECK ((source_revision >= 0))
);

CREATE TABLE public.billing_accounts (
    id uuid NOT NULL,
    external_ref text,
    name text NOT NULL,
    currency text DEFAULT 'USD'::text NOT NULL,
    rpm integer NOT NULL,
    tpm bigint NOT NULL,
    concurrency integer NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    source_revision integer DEFAULT 0 NOT NULL,
    CONSTRAINT billing_accounts_concurrency_check CHECK ((concurrency > 0)),
    CONSTRAINT billing_accounts_currency_check CHECK ((currency ~ '^[A-Z]{3,8}$'::text)),
    CONSTRAINT billing_accounts_rpm_check CHECK ((rpm > 0)),
    CONSTRAINT billing_accounts_source_revision_check CHECK ((source_revision >= 0)),
    CONSTRAINT billing_accounts_tpm_check CHECK (((tpm > 0) AND (tpm <= '9007199254740991'::bigint)))
);

CREATE TABLE public.billing_charges (
    id uuid NOT NULL,
    request_id uuid NOT NULL,
    billing_account_id uuid NOT NULL,
    wallet_id uuid NOT NULL,
    selected_attempt_id uuid,
    channel_id uuid,
    model text NOT NULL,
    operation text NOT NULL,
    currency text NOT NULL,
    billing_price_id uuid,
    cost_version_id uuid,
    usage jsonb DEFAULT '{}'::jsonb NOT NULL,
    usage_digest text NOT NULL,
    customer_atoms numeric(38,0) NOT NULL,
    funded_atoms numeric(38,0) NOT NULL,
    debt_atoms numeric(38,0) DEFAULT 0 NOT NULL,
    refunded_atoms numeric(38,0) DEFAULT 0 NOT NULL,
    debt_refunded_atoms numeric(38,0) DEFAULT 0 NOT NULL,
    supplier_cost_atoms numeric(38,0) DEFAULT 0 NOT NULL,
    supplier_currency text,
    lot_allocations jsonb DEFAULT '[]'::jsonb NOT NULL,
    plugin_audit jsonb DEFAULT '[]'::jsonb NOT NULL,
    charge_policy text DEFAULT 'CHARGE_CUSTOMER'::text NOT NULL,
    outcome text NOT NULL,
    customer_journal_id uuid,
    supplier_journal_id uuid,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    api_key_id uuid,
    pricing_override_id uuid,
    discount_numerator numeric(38,0),
    discount_denominator numeric(38,0),
    CONSTRAINT billing_charges_charge_policy_check CHECK ((charge_policy = ANY (ARRAY['CHARGE_CUSTOMER'::text, 'PLATFORM_ABSORB'::text]))),
    CONSTRAINT billing_charges_check CHECK (((refunded_atoms >= (0)::numeric) AND (refunded_atoms <= customer_atoms))),
    CONSTRAINT billing_charges_currency_check CHECK ((currency ~ '^[A-Z]{3,8}$'::text)),
    CONSTRAINT billing_charges_customer_atoms_check CHECK ((customer_atoms >= (0)::numeric)),
    CONSTRAINT billing_charges_debt_atoms_check CHECK ((debt_atoms >= (0)::numeric)),
    CONSTRAINT billing_charges_debt_refunded_atoms_check CHECK ((debt_refunded_atoms >= (0)::numeric)),
    CONSTRAINT billing_charges_funded_atoms_check CHECK ((funded_atoms >= (0)::numeric)),
    CONSTRAINT billing_charges_lot_allocations_check CHECK ((jsonb_typeof(lot_allocations) = 'array'::text)),
    CONSTRAINT billing_charges_outcome_check CHECK ((outcome = ANY (ARRAY['SETTLED'::text, 'RELEASED'::text, 'DEBT'::text, 'MODERATION_BLOCKED'::text, 'UNKNOWN_RESOLVED'::text]))),
    CONSTRAINT billing_charges_plugin_audit_check CHECK ((jsonb_typeof(plugin_audit) = 'array'::text)),
    CONSTRAINT billing_charges_supplier_cost_atoms_check CHECK ((supplier_cost_atoms >= (0)::numeric)),
    CONSTRAINT billing_charges_supplier_currency_check CHECK (((supplier_currency IS NULL) OR (supplier_currency ~ '^[A-Z]{3,8}$'::text))),
    CONSTRAINT billing_charges_usage_check CHECK ((jsonb_typeof(usage) = 'object'::text)),
    CONSTRAINT billing_charges_usage_digest_check CHECK ((usage_digest ~ '^[a-f0-9]{64}$'::text))
);

CREATE TABLE public.billing_holds (
    request_id uuid NOT NULL,
    billing_account_id uuid NOT NULL,
    wallet_id uuid NOT NULL,
    operation text NOT NULL,
    model text NOT NULL,
    idempotency_key text,
    fingerprint text NOT NULL,
    held_atoms numeric(38,0) NOT NULL,
    credit_atoms numeric(38,0) DEFAULT 0 NOT NULL,
    lot_allocations jsonb DEFAULT '[]'::jsonb NOT NULL,
    supplier_holds jsonb DEFAULT '[]'::jsonb NOT NULL,
    wallet_lease_id uuid,
    supplier_lease_id uuid,
    state text DEFAULT 'HELD'::text NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    closed_at timestamp with time zone,
    api_key_id uuid,
    key_reserved_atoms numeric(38,0) DEFAULT 0 NOT NULL,
    pricing_override_id uuid,
    discount_numerator numeric(38,0),
    discount_denominator numeric(38,0),
    reconciliation_started_at timestamp with time zone,
    CONSTRAINT billing_holds_credit_atoms_check CHECK ((credit_atoms >= (0)::numeric)),
    CONSTRAINT billing_holds_held_atoms_check CHECK ((held_atoms >= (0)::numeric)),
    CONSTRAINT billing_holds_key_reserved_atoms_check CHECK ((key_reserved_atoms >= (0)::numeric)),
    CONSTRAINT billing_holds_lot_allocations_check CHECK ((jsonb_typeof(lot_allocations) = 'array'::text)),
    CONSTRAINT billing_holds_state_check CHECK ((state = ANY (ARRAY['HELD'::text, 'SETTLED'::text, 'RELEASED'::text, 'UNKNOWN'::text, 'CONFLICT'::text]))),
    CONSTRAINT billing_holds_supplier_holds_check CHECK ((jsonb_typeof(supplier_holds) = 'array'::text))
);

CREATE TABLE public.billing_reconciliation_budgets (
    currency text NOT NULL,
    limit_atoms numeric(38,0) NOT NULL,
    used_atoms numeric(38,0) DEFAULT 0 NOT NULL,
    CONSTRAINT billing_reconciliation_budgets_check CHECK (((used_atoms >= (0)::numeric) AND (used_atoms <= limit_atoms))),
    CONSTRAINT billing_reconciliation_budgets_currency_check CHECK ((currency ~ '^[A-Z]{3,8}$'::text)),
    CONSTRAINT billing_reconciliation_budgets_limit_atoms_check CHECK ((limit_atoms >= (0)::numeric))
);

CREATE TABLE public.billing_reconciliation_policies (
    billing_account_id uuid NOT NULL,
    channel_id uuid NOT NULL,
    supplier_currency text NOT NULL,
    enabled boolean NOT NULL,
    include_historical boolean NOT NULL,
    eligible_since timestamp with time zone DEFAULT now() NOT NULL,
    age_seconds integer NOT NULL,
    max_customer_atoms numeric(38,0) NOT NULL,
    limit_atoms numeric(38,0) NOT NULL,
    used_atoms numeric(38,0) DEFAULT 0 NOT NULL,
    CONSTRAINT billing_reconciliation_policies_age_seconds_check CHECK ((age_seconds > 0)),
    CONSTRAINT billing_reconciliation_policies_check CHECK (((used_atoms >= (0)::numeric) AND (used_atoms <= limit_atoms))),
    CONSTRAINT billing_reconciliation_policies_limit_atoms_check CHECK ((limit_atoms > (0)::numeric)),
    CONSTRAINT billing_reconciliation_policies_max_customer_atoms_check CHECK ((max_customer_atoms > (0)::numeric))
);

CREATE TABLE public.billing_reconciliation_polls (
    attempt_id uuid NOT NULL,
    request_id uuid NOT NULL,
    poll_count integer DEFAULT 1 NOT NULL,
    next_at timestamp with time zone NOT NULL,
    CONSTRAINT billing_reconciliation_polls_poll_count_check CHECK ((poll_count > 0))
);

CREATE TABLE public.billing_reconciliation_receipts (
    attempt_id uuid NOT NULL,
    request_id uuid NOT NULL,
    account_id uuid NOT NULL,
    channel_id uuid NOT NULL,
    currency text NOT NULL,
    provider_request_id text NOT NULL,
    evidence_ref text NOT NULL,
    match_reason text,
    actor text NOT NULL,
    verdict text NOT NULL,
    supplier_cost_atoms numeric(38,0),
    customer_usage jsonb,
    customer_actor text,
    state text DEFAULT 'PENDING'::text NOT NULL,
    result text,
    review_history jsonb DEFAULT '[]'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    reviewed_at timestamp with time zone,
    CONSTRAINT billing_reconciliation_receipts_check CHECK ((((verdict = 'NOT_EXECUTED'::text) AND (supplier_cost_atoms IS NULL) AND (customer_usage IS NULL)) OR ((verdict = 'FINAL'::text) AND (supplier_cost_atoms IS NOT NULL) AND (supplier_cost_atoms >= (0)::numeric)))),
    CONSTRAINT billing_reconciliation_receipts_check1 CHECK (((customer_usage IS NULL) = (customer_actor IS NULL))),
    CONSTRAINT billing_reconciliation_receipts_review_history_check CHECK ((jsonb_typeof(review_history) = 'array'::text)),
    CONSTRAINT billing_reconciliation_receipts_state_check CHECK ((state = ANY (ARRAY['PENDING'::text, 'APPLIED'::text, 'REVIEW'::text, 'DISMISSED'::text]))),
    CONSTRAINT billing_reconciliation_receipts_verdict_check CHECK ((verdict = ANY (ARRAY['NOT_EXECUTED'::text, 'FINAL'::text])))
);

CREATE TABLE public.billing_reconciliation_waivers (
    request_id uuid NOT NULL,
    attempt_id uuid NOT NULL,
    billing_account_id uuid NOT NULL,
    channel_id uuid NOT NULL,
    supplier_currency text NOT NULL,
    supplier_risk_atoms numeric(38,0) NOT NULL,
    customer_released_atoms numeric(38,0) NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT billing_reconciliation_waivers_customer_released_atoms_check CHECK ((customer_released_atoms >= (0)::numeric)),
    CONSTRAINT billing_reconciliation_waivers_supplier_risk_atoms_check CHECK ((supplier_risk_atoms >= (0)::numeric))
);

CREATE TABLE public.billing_resolutions (
    id uuid NOT NULL,
    request_id uuid NOT NULL,
    action text NOT NULL,
    billing_price_id uuid,
    usage jsonb DEFAULT '{}'::jsonb NOT NULL,
    reason text NOT NULL,
    actor text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT billing_resolutions_action_check CHECK ((action = ANY (ARRAY['SETTLE'::text, 'RELEASE'::text])))
);

CREATE TABLE public.channels (
    id uuid NOT NULL,
    account_id uuid NOT NULL,
    name text NOT NULL,
    kind text NOT NULL,
    provider_plugin_id uuid,
    base_url text NOT NULL,
    secret_ciphertext text NOT NULL,
    rpm integer NOT NULL,
    concurrency integer NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    routing_policy jsonb DEFAULT '{}'::jsonb NOT NULL,
    adapter_config jsonb DEFAULT '{}'::jsonb NOT NULL,
    config_revision bigint DEFAULT 1 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    source_revision integer DEFAULT 0 NOT NULL,
    CONSTRAINT channels_adapter_config_check CHECK ((jsonb_typeof(adapter_config) = 'object'::text)),
    CONSTRAINT channels_concurrency_check CHECK ((concurrency > 0)),
    CONSTRAINT channels_enabled_provider_binding CHECK (((NOT enabled) OR (provider_plugin_id IS NOT NULL))),
    CONSTRAINT channels_kind_check CHECK ((kind ~ '^[a-z0-9][a-z0-9_-]{1,63}$'::text)),
    CONSTRAINT channels_routing_policy_check CHECK ((jsonb_typeof(routing_policy) = 'object'::text)),
    CONSTRAINT channels_rpm_check CHECK ((rpm > 0)),
    CONSTRAINT channels_source_revision_check CHECK ((source_revision >= 0))
);

CREATE TABLE public.config_epoch (
    id smallint NOT NULL,
    version bigint DEFAULT 1 NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    lineage_token uuid DEFAULT gen_random_uuid() NOT NULL,
    CONSTRAINT config_epoch_id_check CHECK ((id = 1))
);

CREATE TABLE public.console_organization_admission (
    organization_id uuid NOT NULL,
    suspended boolean NOT NULL,
    source_revision integer NOT NULL,
    CONSTRAINT console_organization_admission_source_revision_check CHECK ((source_revision > 0))
);

CREATE TABLE public.funding_operations (
    id uuid NOT NULL,
    side text NOT NULL,
    entity_id uuid NOT NULL,
    kind text NOT NULL,
    external_ref text NOT NULL,
    fingerprint text NOT NULL,
    amount_atoms numeric(38,0) NOT NULL,
    currency text NOT NULL,
    reason text DEFAULT ''::text NOT NULL,
    payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    receipt jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT funding_operations_currency_check CHECK ((currency ~ '^[A-Z]{3,8}$'::text)),
    CONSTRAINT funding_operations_external_ref_check CHECK (((length(external_ref) >= 1) AND (length(external_ref) <= 200))),
    CONSTRAINT funding_operations_fingerprint_check CHECK ((fingerprint ~ '^[a-f0-9]{64}$'::text)),
    CONSTRAINT funding_operations_kind_check CHECK ((kind = ANY (ARRAY['CREDIT'::text, 'REFUND'::text, 'ADJUST'::text, 'SUPPLIER_RECHARGE'::text, 'SUPPLIER_ADJUST'::text]))),
    CONSTRAINT funding_operations_side_check CHECK ((side = ANY (ARRAY['CUSTOMER'::text, 'SUPPLIER'::text])))
);

CREATE TABLE public.gateway_config_nodes (
    id uuid NOT NULL,
    applied_epoch bigint NOT NULL,
    last_seen_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    stopped_at timestamp with time zone
);

CREATE TABLE public.ledger_entries (
    journal_id uuid NOT NULL,
    ordinal smallint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    side text NOT NULL,
    entity_id uuid NOT NULL,
    request_id uuid,
    reference text NOT NULL,
    kind text NOT NULL,
    currency text NOT NULL,
    account text NOT NULL,
    atoms numeric(38,0) NOT NULL,
    CONSTRAINT ledger_entries_account_check CHECK ((account ~ '^(lot:[a-f0-9-]{36}:(available|spent|expired)|system:(funding|revenue|receivable|moderation_loss|adjustment)|upstream:(available|expense|funding|adjustment))$'::text)),
    CONSTRAINT ledger_entries_currency_check CHECK ((currency ~ '^[A-Z]{3,8}$'::text)),
    CONSTRAINT ledger_entries_kind_check CHECK ((kind = ANY (ARRAY['CREDIT'::text, 'SETTLE'::text, 'RELEASE'::text, 'REFUND'::text, 'ADJUST'::text, 'EXPIRE'::text, 'DEBT_COLLECT'::text, 'MODERATION_ABSORB'::text, 'SUPPLIER_RECHARGE'::text, 'SUPPLIER_COST'::text, 'SUPPLIER_ADJUST'::text]))),
    CONSTRAINT ledger_entries_ordinal_check CHECK ((ordinal >= 0)),
    CONSTRAINT ledger_entries_side_check CHECK ((side = ANY (ARRAY['CUSTOMER'::text, 'SUPPLIER'::text])))
)
PARTITION BY RANGE (created_at);

CREATE TABLE public.ledger_entries_default (
    journal_id uuid NOT NULL,
    ordinal smallint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    side text NOT NULL,
    entity_id uuid NOT NULL,
    request_id uuid,
    reference text NOT NULL,
    kind text NOT NULL,
    currency text NOT NULL,
    account text NOT NULL,
    atoms numeric(38,0) NOT NULL,
    CONSTRAINT ledger_entries_account_check CHECK ((account ~ '^(lot:[a-f0-9-]{36}:(available|spent|expired)|system:(funding|revenue|receivable|moderation_loss|adjustment)|upstream:(available|expense|funding|adjustment))$'::text)),
    CONSTRAINT ledger_entries_currency_check CHECK ((currency ~ '^[A-Z]{3,8}$'::text)),
    CONSTRAINT ledger_entries_kind_check CHECK ((kind = ANY (ARRAY['CREDIT'::text, 'SETTLE'::text, 'RELEASE'::text, 'REFUND'::text, 'ADJUST'::text, 'EXPIRE'::text, 'DEBT_COLLECT'::text, 'MODERATION_ABSORB'::text, 'SUPPLIER_RECHARGE'::text, 'SUPPLIER_COST'::text, 'SUPPLIER_ADJUST'::text]))),
    CONSTRAINT ledger_entries_ordinal_check CHECK ((ordinal >= 0)),
    CONSTRAINT ledger_entries_side_check CHECK ((side = ANY (ARRAY['CUSTOMER'::text, 'SUPPLIER'::text])))
);

CREATE TABLE public.models (
    name text NOT NULL,
    operations text[] NOT NULL,
    routing_strategy text DEFAULT 'WEIGHTED'::text NOT NULL,
    max_output_tokens integer NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    source_revision integer DEFAULT 0 NOT NULL,
    CONSTRAINT models_max_output_tokens_check CHECK ((max_output_tokens > 0)),
    CONSTRAINT models_routing_strategy_check CHECK ((routing_strategy = ANY (ARRAY['WEIGHTED'::text, 'ROUND_ROBIN'::text, 'LEAST_LATENCY'::text]))),
    CONSTRAINT models_source_revision_check CHECK ((source_revision >= 0))
);

CREATE TABLE public.outbox_consumer_shards (
    name text NOT NULL,
    shard smallint NOT NULL,
    acked_seq bigint DEFAULT 0 NOT NULL,
    lease_token uuid,
    leased_through_seq bigint,
    leased_until timestamp with time zone,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT outbox_consumer_shards_acked_seq_check CHECK ((acked_seq >= 0)),
    CONSTRAINT outbox_consumer_shards_leased_through_seq_check CHECK ((leased_through_seq >= 0)),
    CONSTRAINT outbox_consumer_shards_shard_check CHECK (((shard >= 0) AND (shard <= 15)))
);

CREATE TABLE public.outbox_consumers (
    name text NOT NULL
);

CREATE TABLE public.outbox_events (
    seq bigint DEFAULT 0 NOT NULL,
    event_id uuid NOT NULL,
    kind text NOT NULL,
    request_id uuid,
    billing_account_id uuid,
    payload jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    shard smallint DEFAULT 0 NOT NULL,
    CONSTRAINT outbox_events_shard_check CHECK (((shard >= 0) AND (shard <= 15)))
);

CREATE SEQUENCE public.outbox_events_seq_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE public.outbox_events_seq_seq OWNED BY public.outbox_events.seq;

CREATE TABLE public.plugin_release_acks (
    config_epoch bigint NOT NULL,
    node_id uuid NOT NULL,
    applied_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL
);

CREATE TABLE public.plugin_releases (
    id uuid NOT NULL,
    request_digest text NOT NULL,
    actor text NOT NULL,
    config_epoch bigint NOT NULL,
    changes jsonb NOT NULL,
    rollback_of uuid,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    expected_nodes uuid[] DEFAULT '{}'::uuid[] NOT NULL,
    CONSTRAINT plugin_releases_changes_check CHECK ((jsonb_typeof(changes) = 'array'::text)),
    CONSTRAINT plugin_releases_request_digest_check CHECK ((request_digest ~ '^[a-f0-9]{64}$'::text))
);

CREATE TABLE public.plugin_revocations (
    plugin_id uuid NOT NULL,
    actor text NOT NULL,
    reason text NOT NULL,
    config_epoch bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE public.plugins (
    id uuid NOT NULL,
    name text NOT NULL,
    version text NOT NULL,
    capability text DEFAULT 'CONTENT'::text NOT NULL,
    kind text NOT NULL,
    digest text NOT NULL,
    wasm_bytes bytea,
    webhook_config jsonb,
    provider_config jsonb,
    signature text,
    signing_key_id text,
    manifest jsonb DEFAULT '{}'::jsonb NOT NULL,
    enabled boolean DEFAULT false NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    builtin boolean DEFAULT false NOT NULL,
    CONSTRAINT plugins_capability_check CHECK ((capability = ANY (ARRAY['CONTENT'::text, 'PROVIDER'::text]))),
    CONSTRAINT plugins_check CHECK ((((capability = 'CONTENT'::text) AND (kind = ANY (ARRAY['WASM'::text, 'WEBHOOK'::text]))) OR ((capability = 'PROVIDER'::text) AND (kind = ANY (ARRAY['DECLARATIVE'::text, 'WASM'::text]))))),
    CONSTRAINT plugins_check1 CHECK ((((kind = 'WASM'::text) AND (wasm_bytes IS NOT NULL)) OR ((kind = 'WEBHOOK'::text) AND (webhook_config IS NOT NULL)) OR ((kind = 'DECLARATIVE'::text) AND (provider_config IS NOT NULL)))),
    CONSTRAINT plugins_digest_check CHECK ((digest ~ '^[a-f0-9]{64}$'::text)),
    CONSTRAINT plugins_kind_check CHECK ((kind = ANY (ARRAY['WASM'::text, 'WEBHOOK'::text, 'DECLARATIVE'::text]))),
    CONSTRAINT plugins_manifest_check CHECK ((jsonb_typeof(manifest) = 'object'::text)),
    CONSTRAINT plugins_name_check CHECK ((name ~ '^[a-z0-9][a-z0-9_-]{1,63}$'::text)),
    CONSTRAINT plugins_provider_config_check CHECK (((provider_config IS NULL) OR (jsonb_typeof(provider_config) = 'object'::text))),
    CONSTRAINT plugins_wasm_bytes_check CHECK (((wasm_bytes IS NULL) OR ((octet_length(wasm_bytes) >= 1) AND (octet_length(wasm_bytes) <= 16777216)))),
    CONSTRAINT plugins_webhook_config_check CHECK (((webhook_config IS NULL) OR (jsonb_typeof(webhook_config) = 'object'::text)))
);

CREATE TABLE public.price_versions (
    id uuid NOT NULL,
    side text NOT NULL,
    currency text NOT NULL,
    basis text DEFAULT 'PROVIDER_USAGE'::text NOT NULL,
    rates jsonb NOT NULL,
    ceilings jsonb DEFAULT '{}'::jsonb NOT NULL,
    source_digest text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT price_versions_basis_check CHECK ((basis = ANY (ARRAY['PROVIDER_USAGE'::text, 'REQUEST_COMPLETION'::text, 'INPUT_CHARACTERS'::text]))),
    CONSTRAINT price_versions_ceilings_check CHECK ((jsonb_typeof(ceilings) = 'object'::text)),
    CONSTRAINT price_versions_currency_check CHECK ((currency ~ '^[A-Z]{3,8}$'::text)),
    CONSTRAINT price_versions_rates_check CHECK ((jsonb_typeof(rates) = 'object'::text)),
    CONSTRAINT price_versions_side_check CHECK ((side = ANY (ARRAY['CUSTOMER'::text, 'SUPPLIER'::text]))),
    CONSTRAINT price_versions_source_digest_check CHECK ((source_digest ~ '^[a-f0-9]{64}$'::text))
);

CREATE TABLE public.routes (
    id uuid NOT NULL,
    model_name text NOT NULL,
    channel_id uuid NOT NULL,
    upstream_model text NOT NULL,
    operations text[] NOT NULL,
    priority integer DEFAULT 0 NOT NULL,
    weight double precision DEFAULT 1 NOT NULL,
    billing_price_id uuid NOT NULL,
    cost_version_id uuid NOT NULL,
    request_policy jsonb DEFAULT '{}'::jsonb NOT NULL,
    adapter_overrides jsonb DEFAULT '{}'::jsonb NOT NULL,
    plugin_pipeline jsonb DEFAULT '[]'::jsonb NOT NULL,
    retry_policy jsonb DEFAULT '{}'::jsonb NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    source_revision integer DEFAULT 0 NOT NULL,
    CONSTRAINT routes_adapter_overrides_check CHECK ((jsonb_typeof(adapter_overrides) = 'object'::text)),
    CONSTRAINT routes_plugin_pipeline_check CHECK ((jsonb_typeof(plugin_pipeline) = 'array'::text)),
    CONSTRAINT routes_request_policy_check CHECK ((jsonb_typeof(request_policy) = 'object'::text)),
    CONSTRAINT routes_retry_policy_check CHECK ((jsonb_typeof(retry_policy) = 'object'::text)),
    CONSTRAINT routes_source_revision_check CHECK ((source_revision >= 0)),
    CONSTRAINT routes_weight_check CHECK ((weight > (0)::double precision))
);

CREATE TABLE public.schema_migrations (
    version integer NOT NULL,
    checksum text NOT NULL,
    manifest jsonb NOT NULL,
    applied_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT schema_migrations_checksum_check CHECK ((checksum ~ '^[a-f0-9]{64}$'::text))
);

CREATE TABLE public.supplier_cells (
    account_id uuid NOT NULL,
    currency text NOT NULL,
    cell smallint NOT NULL,
    balance_atoms numeric(38,0) DEFAULT 0 NOT NULL,
    reserved_atoms numeric(38,0) DEFAULT 0 NOT NULL,
    spent_atoms numeric(38,0) DEFAULT 0 NOT NULL,
    revision numeric(38,0) DEFAULT 0 NOT NULL,
    CONSTRAINT supplier_cells_cell_check CHECK (((cell >= 0) AND (cell <= 15))),
    CONSTRAINT supplier_cells_reserved_atoms_check CHECK ((reserved_atoms >= (0)::numeric)),
    CONSTRAINT supplier_cells_revision_check CHECK ((revision >= (0)::numeric)),
    CONSTRAINT supplier_cells_spent_atoms_check CHECK ((spent_atoms >= (0)::numeric))
)
WITH (fillfactor='70');

CREATE TABLE public.supplier_leases (
    id uuid NOT NULL,
    node_id text NOT NULL,
    account_id uuid NOT NULL,
    currency text NOT NULL,
    cell smallint NOT NULL,
    remaining_atoms numeric(38,0) NOT NULL,
    state text DEFAULT 'ACTIVE'::text NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT supplier_leases_cell_check CHECK (((cell >= 0) AND (cell <= 15))),
    CONSTRAINT supplier_leases_currency_check CHECK ((currency ~ '^[A-Z]{3,8}$'::text)),
    CONSTRAINT supplier_leases_remaining_atoms_check CHECK ((remaining_atoms >= (0)::numeric)),
    CONSTRAINT supplier_leases_state_check CHECK ((state = ANY (ARRAY['ACTIVE'::text, 'CLOSED'::text])))
)
WITH (fillfactor='60');

CREATE TABLE public.supplier_root_balances (
    account_id uuid NOT NULL,
    currency text NOT NULL,
    balance_atoms numeric(38,0) DEFAULT 0 NOT NULL,
    spent_atoms numeric(38,0) DEFAULT 0 NOT NULL,
    revision numeric(38,0) DEFAULT 0 NOT NULL,
    CONSTRAINT supplier_root_balances_currency_check CHECK ((currency ~ '^[A-Z]{3,8}$'::text)),
    CONSTRAINT supplier_root_balances_revision_check CHECK ((revision >= (0)::numeric)),
    CONSTRAINT supplier_root_balances_spent_atoms_check CHECK ((spent_atoms >= (0)::numeric))
);

CREATE TABLE public.upstream_accounts (
    id uuid NOT NULL,
    name text NOT NULL,
    state text DEFAULT 'ACTIVE'::text NOT NULL,
    revision numeric(38,0) DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    source_revision integer DEFAULT 0 NOT NULL,
    CONSTRAINT upstream_accounts_revision_check CHECK ((revision >= (0)::numeric)),
    CONSTRAINT upstream_accounts_source_revision_check CHECK ((source_revision >= 0)),
    CONSTRAINT upstream_accounts_state_check CHECK ((state = ANY (ARRAY['ACTIVE'::text, 'DISABLED'::text, 'RECONCILE_REQUIRED'::text])))
);

CREATE TABLE public.wallet_leases (
    id uuid NOT NULL,
    node_id text NOT NULL,
    wallet_id uuid NOT NULL,
    currency text NOT NULL,
    lot_allocations jsonb DEFAULT '[]'::jsonb NOT NULL,
    remaining_atoms numeric(38,0) NOT NULL,
    earliest_lot_expires_at timestamp with time zone,
    state text DEFAULT 'ACTIVE'::text NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT wallet_leases_currency_check CHECK ((currency ~ '^[A-Z]{3,8}$'::text)),
    CONSTRAINT wallet_leases_lot_allocations_check CHECK ((jsonb_typeof(lot_allocations) = 'array'::text)),
    CONSTRAINT wallet_leases_remaining_atoms_check CHECK ((remaining_atoms >= (0)::numeric)),
    CONSTRAINT wallet_leases_state_check CHECK ((state = ANY (ARRAY['ACTIVE'::text, 'CLOSED'::text])))
)
WITH (fillfactor='60');

CREATE TABLE public.wallet_lots (
    id uuid NOT NULL,
    wallet_id uuid NOT NULL,
    external_ref text NOT NULL,
    kind text NOT NULL,
    priority integer DEFAULT 100 NOT NULL,
    models text[] DEFAULT '{}'::text[] NOT NULL,
    operations text[] DEFAULT '{}'::text[] NOT NULL,
    expires_at timestamp with time zone,
    issued_atoms numeric(38,0) NOT NULL,
    available_atoms numeric(38,0) NOT NULL,
    reserved_atoms numeric(38,0) DEFAULT 0 NOT NULL,
    spent_atoms numeric(38,0) DEFAULT 0 NOT NULL,
    expired_atoms numeric(38,0) DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT wallet_lots_available_atoms_check CHECK ((available_atoms >= (0)::numeric)),
    CONSTRAINT wallet_lots_check CHECK ((issued_atoms = (((available_atoms + reserved_atoms) + spent_atoms) + expired_atoms))),
    CONSTRAINT wallet_lots_expired_atoms_check CHECK ((expired_atoms >= (0)::numeric)),
    CONSTRAINT wallet_lots_issued_atoms_check CHECK ((issued_atoms > (0)::numeric)),
    CONSTRAINT wallet_lots_kind_check CHECK ((kind = ANY (ARRAY['CASH'::text, 'GRANT'::text]))),
    CONSTRAINT wallet_lots_reserved_atoms_check CHECK ((reserved_atoms >= (0)::numeric)),
    CONSTRAINT wallet_lots_spent_atoms_check CHECK ((spent_atoms >= (0)::numeric))
)
WITH (fillfactor='70');

CREATE TABLE public.wallets (
    id uuid NOT NULL,
    billing_account_id uuid NOT NULL,
    currency text NOT NULL,
    policy text DEFAULT 'GIFT_FIRST'::text NOT NULL,
    available_atoms numeric(38,0) DEFAULT 0 NOT NULL,
    reserved_atoms numeric(38,0) DEFAULT 0 NOT NULL,
    spent_atoms numeric(38,0) DEFAULT 0 NOT NULL,
    expired_atoms numeric(38,0) DEFAULT 0 NOT NULL,
    debt_atoms numeric(38,0) DEFAULT 0 NOT NULL,
    credit_limit_atoms numeric(38,0) DEFAULT 0 NOT NULL,
    credit_reserved_atoms numeric(38,0) DEFAULT 0 NOT NULL,
    credit_policy_version numeric(38,0) DEFAULT 0 NOT NULL,
    manual_frozen boolean DEFAULT false NOT NULL,
    evidence_disputed boolean DEFAULT false NOT NULL,
    freeze_reason text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT wallets_available_atoms_check CHECK ((available_atoms >= (0)::numeric)),
    CONSTRAINT wallets_credit_limit_atoms_check CHECK ((credit_limit_atoms >= (0)::numeric)),
    CONSTRAINT wallets_credit_policy_version_check CHECK ((credit_policy_version >= (0)::numeric)),
    CONSTRAINT wallets_credit_reserved_atoms_check CHECK ((credit_reserved_atoms >= (0)::numeric)),
    CONSTRAINT wallets_currency_check CHECK ((currency ~ '^[A-Z]{3,8}$'::text)),
    CONSTRAINT wallets_debt_atoms_check CHECK ((debt_atoms >= (0)::numeric)),
    CONSTRAINT wallets_expired_atoms_check CHECK ((expired_atoms >= (0)::numeric)),
    CONSTRAINT wallets_policy_check CHECK ((policy = ANY (ARRAY['GIFT_FIRST'::text, 'CASH_FIRST'::text, 'EARLIEST_EXPIRY'::text]))),
    CONSTRAINT wallets_reserved_atoms_check CHECK ((reserved_atoms >= (0)::numeric)),
    CONSTRAINT wallets_spent_atoms_check CHECK ((spent_atoms >= (0)::numeric))
)
WITH (fillfactor='70');

ALTER TABLE ONLY public.ledger_entries ATTACH PARTITION public.ledger_entries_default DEFAULT;

ALTER TABLE ONLY public.admin_audit_events
    ADD CONSTRAINT admin_audit_events_pkey PRIMARY KEY (request_id, phase);

ALTER TABLE ONLY public.admin_audit_pending
    ADD CONSTRAINT admin_audit_pending_pkey PRIMARY KEY (request_id);

ALTER TABLE ONLY public.api_keys
    ADD CONSTRAINT api_keys_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.api_keys
    ADD CONSTRAINT api_keys_verifier_key UNIQUE (verifier);

ALTER TABLE ONLY public.async_tasks
    ADD CONSTRAINT async_tasks_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.async_tasks
    ADD CONSTRAINT async_tasks_request_id_key UNIQUE (request_id);

ALTER TABLE ONLY public.billing_account_pricing_overrides
    ADD CONSTRAINT billing_account_pricing_overrides_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.billing_account_pricing_overrides
    ADD CONSTRAINT billing_account_pricing_overrides_scope_uniq UNIQUE (billing_account_id, model_name, operation);

ALTER TABLE ONLY public.billing_accounts
    ADD CONSTRAINT billing_accounts_external_ref_key UNIQUE (external_ref);

ALTER TABLE ONLY public.billing_accounts
    ADD CONSTRAINT billing_accounts_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.billing_charges
    ADD CONSTRAINT billing_charges_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.billing_charges
    ADD CONSTRAINT billing_charges_request_id_key UNIQUE (request_id);

ALTER TABLE ONLY public.billing_holds
    ADD CONSTRAINT billing_holds_pkey PRIMARY KEY (request_id);

ALTER TABLE ONLY public.billing_reconciliation_budgets
    ADD CONSTRAINT billing_reconciliation_budgets_pkey PRIMARY KEY (currency);

ALTER TABLE ONLY public.billing_reconciliation_policies
    ADD CONSTRAINT billing_reconciliation_policies_pkey PRIMARY KEY (billing_account_id, channel_id, supplier_currency);

ALTER TABLE ONLY public.billing_reconciliation_polls
    ADD CONSTRAINT billing_reconciliation_polls_pkey PRIMARY KEY (attempt_id);

ALTER TABLE ONLY public.billing_reconciliation_receipts
    ADD CONSTRAINT billing_reconciliation_receip_account_id_provider_request_i_key UNIQUE (account_id, provider_request_id);

ALTER TABLE ONLY public.billing_reconciliation_receipts
    ADD CONSTRAINT billing_reconciliation_receipts_account_id_evidence_ref_key UNIQUE (account_id, evidence_ref);

ALTER TABLE ONLY public.billing_reconciliation_receipts
    ADD CONSTRAINT billing_reconciliation_receipts_pkey PRIMARY KEY (attempt_id);

ALTER TABLE ONLY public.billing_reconciliation_waivers
    ADD CONSTRAINT billing_reconciliation_waivers_pkey PRIMARY KEY (request_id);

ALTER TABLE ONLY public.billing_resolutions
    ADD CONSTRAINT billing_resolutions_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.billing_resolutions
    ADD CONSTRAINT billing_resolutions_request_id_key UNIQUE (request_id);

ALTER TABLE ONLY public.channels
    ADD CONSTRAINT channels_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.config_epoch
    ADD CONSTRAINT config_epoch_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.console_organization_admission
    ADD CONSTRAINT console_organization_admission_pkey PRIMARY KEY (organization_id);

ALTER TABLE ONLY public.funding_operations
    ADD CONSTRAINT funding_operations_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.funding_operations
    ADD CONSTRAINT funding_operations_side_entity_id_kind_external_ref_key UNIQUE (side, entity_id, kind, external_ref);

ALTER TABLE ONLY public.gateway_config_nodes
    ADD CONSTRAINT gateway_config_nodes_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.ledger_entries
    ADD CONSTRAINT ledger_entries_pkey PRIMARY KEY (journal_id, ordinal, created_at);

ALTER TABLE ONLY public.ledger_entries_default
    ADD CONSTRAINT ledger_entries_default_pkey PRIMARY KEY (journal_id, ordinal, created_at);

ALTER TABLE ONLY public.models
    ADD CONSTRAINT models_pkey PRIMARY KEY (name);

ALTER TABLE ONLY public.outbox_consumer_shards
    ADD CONSTRAINT outbox_consumer_shards_pkey PRIMARY KEY (name, shard);

ALTER TABLE ONLY public.outbox_consumers
    ADD CONSTRAINT outbox_consumers_pkey PRIMARY KEY (name);

ALTER TABLE ONLY public.outbox_events
    ADD CONSTRAINT outbox_events_event_id_key UNIQUE (event_id);

ALTER TABLE ONLY public.outbox_events
    ADD CONSTRAINT outbox_events_pkey PRIMARY KEY (seq);

ALTER TABLE ONLY public.plugin_release_acks
    ADD CONSTRAINT plugin_release_acks_pkey PRIMARY KEY (config_epoch, node_id);

ALTER TABLE ONLY public.plugin_releases
    ADD CONSTRAINT plugin_releases_config_epoch_key UNIQUE (config_epoch);

ALTER TABLE ONLY public.plugin_releases
    ADD CONSTRAINT plugin_releases_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.plugin_revocations
    ADD CONSTRAINT plugin_revocations_pkey PRIMARY KEY (plugin_id);

ALTER TABLE ONLY public.plugins
    ADD CONSTRAINT plugins_name_version_key UNIQUE (name, version);

ALTER TABLE ONLY public.plugins
    ADD CONSTRAINT plugins_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.price_versions
    ADD CONSTRAINT price_versions_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.routes
    ADD CONSTRAINT routes_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.schema_migrations
    ADD CONSTRAINT schema_migrations_pkey PRIMARY KEY (version);

ALTER TABLE ONLY public.supplier_cells
    ADD CONSTRAINT supplier_cells_pkey PRIMARY KEY (account_id, currency, cell);

ALTER TABLE ONLY public.supplier_leases
    ADD CONSTRAINT supplier_leases_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.supplier_root_balances
    ADD CONSTRAINT supplier_root_balances_pkey PRIMARY KEY (account_id, currency);

ALTER TABLE ONLY public.upstream_accounts
    ADD CONSTRAINT upstream_accounts_name_key UNIQUE (name);

ALTER TABLE ONLY public.upstream_accounts
    ADD CONSTRAINT upstream_accounts_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.wallet_leases
    ADD CONSTRAINT wallet_leases_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.wallet_lots
    ADD CONSTRAINT wallet_lots_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.wallet_lots
    ADD CONSTRAINT wallet_lots_wallet_id_external_ref_key UNIQUE (wallet_id, external_ref);

ALTER TABLE ONLY public.wallets
    ADD CONSTRAINT wallets_billing_account_id_currency_key UNIQUE (billing_account_id, currency);

ALTER TABLE ONLY public.wallets
    ADD CONSTRAINT wallets_pkey PRIMARY KEY (id);

CREATE INDEX admin_audit_pending_created_idx ON public.admin_audit_pending USING btree (created_at);

CREATE INDEX api_keys_billing_account_idx ON public.api_keys USING btree (billing_account_id);

CREATE INDEX async_tasks_active_idx ON public.async_tasks USING btree (status, updated_at) WHERE (status = ANY (ARRAY['QUEUED'::text, 'PROCESSING'::text]));

CREATE INDEX async_tasks_auto_refund_idx ON public.async_tasks USING btree (billing_account_id, refund_eligibility, completed_at) WHERE (refund_eligibility = 'AUTO_REFUNDED'::text);

CREATE INDEX async_tasks_billing_account_status_idx ON public.async_tasks USING btree (billing_account_id, status);

CREATE INDEX billing_account_pricing_overrides_billing_account_idx ON public.billing_account_pricing_overrides USING btree (billing_account_id);

CREATE INDEX billing_charges_api_key_created_idx ON public.billing_charges USING btree (api_key_id, created_at) WHERE (api_key_id IS NOT NULL);

CREATE INDEX billing_charges_billing_account_created_idx ON public.billing_charges USING btree (billing_account_id, created_at DESC);

CREATE INDEX billing_charges_wallet_debt_idx ON public.billing_charges USING btree (wallet_id, created_at) WHERE (debt_atoms > (0)::numeric);

CREATE UNIQUE INDEX billing_holds_idempotency_idx ON public.billing_holds USING btree (billing_account_id, operation, idempotency_key) WHERE (idempotency_key IS NOT NULL);

CREATE INDEX billing_holds_open_idx ON public.billing_holds USING btree (expires_at) WHERE (state = ANY (ARRAY['HELD'::text, 'UNKNOWN'::text]));

CREATE INDEX billing_holds_supplier_lease_idx ON public.billing_holds USING btree (supplier_lease_id) WHERE ((state = ANY (ARRAY['HELD'::text, 'UNKNOWN'::text])) AND (supplier_lease_id IS NOT NULL));

CREATE INDEX billing_holds_supplier_unknown_idx ON public.billing_holds USING btree (expires_at) WHERE (supplier_holds @> '[{"state": "UNKNOWN"}]'::jsonb);

CREATE INDEX billing_holds_wallet_lease_idx ON public.billing_holds USING btree (wallet_lease_id) WHERE ((state = ANY (ARRAY['HELD'::text, 'UNKNOWN'::text])) AND (wallet_lease_id IS NOT NULL));

CREATE INDEX billing_reconciliation_receipts_due_idx ON public.billing_reconciliation_receipts USING btree (created_at, attempt_id) WHERE (state = 'PENDING'::text);

CREATE INDEX billing_reconciliation_receipts_request_idx ON public.billing_reconciliation_receipts USING btree (request_id, created_at, attempt_id);

CREATE INDEX billing_reconciliation_supplier_pending_idx ON public.billing_holds USING btree (created_at, request_id) WHERE ((state = ANY (ARRAY['SETTLED'::text, 'RELEASED'::text])) AND (supplier_holds @> '[{"state": "UNKNOWN"}]'::jsonb));

CREATE INDEX billing_reconciliation_unknown_idx ON public.billing_holds USING btree (created_at, request_id) WHERE (state = 'UNKNOWN'::text);

CREATE INDEX channels_account_idx ON public.channels USING btree (account_id);

CREATE INDEX channels_provider_plugin_idx ON public.channels USING btree (provider_plugin_id) WHERE (provider_plugin_id IS NOT NULL);

CREATE INDEX ledger_entries_request_idx ON ONLY public.ledger_entries USING btree (request_id) WHERE (request_id IS NOT NULL);

CREATE INDEX ledger_entries_default_request_id_idx ON public.ledger_entries_default USING btree (request_id) WHERE (request_id IS NOT NULL);

CREATE INDEX ledger_entries_entity_idx ON ONLY public.ledger_entries USING btree (side, entity_id, created_at DESC);

CREATE INDEX ledger_entries_default_side_entity_id_created_at_idx ON public.ledger_entries_default USING btree (side, entity_id, created_at DESC);

CREATE INDEX outbox_events_billing_account_idx ON public.outbox_events USING btree (billing_account_id, seq);

CREATE INDEX outbox_events_shard_seq_idx ON public.outbox_events USING btree (shard, seq);

CREATE INDEX plugins_provider_name_idx ON public.plugins USING btree (name, created_at DESC) WHERE ((capability = 'PROVIDER'::text) AND enabled);

CREATE INDEX routes_model_lookup_idx ON public.routes USING btree (model_name, priority DESC) WHERE enabled;

CREATE INDEX supplier_leases_active_idx ON public.supplier_leases USING btree (expires_at) WHERE (state = 'ACTIVE'::text);

CREATE INDEX wallet_leases_active_idx ON public.wallet_leases USING btree (expires_at) WHERE (state = 'ACTIVE'::text);

CREATE INDEX wallet_leases_wallet_idx ON public.wallet_leases USING btree (wallet_id) WHERE (state = 'ACTIVE'::text);

CREATE INDEX wallet_lots_avail_idx ON public.wallet_lots USING btree (wallet_id, priority, expires_at, created_at) WHERE (available_atoms > (0)::numeric);

CREATE INDEX wallet_lots_expiry_idx ON public.wallet_lots USING btree (expires_at) WHERE ((available_atoms > (0)::numeric) AND (expires_at IS NOT NULL));

ALTER INDEX public.ledger_entries_pkey ATTACH PARTITION public.ledger_entries_default_pkey;

ALTER INDEX public.ledger_entries_request_idx ATTACH PARTITION public.ledger_entries_default_request_id_idx;

ALTER INDEX public.ledger_entries_entity_idx ATTACH PARTITION public.ledger_entries_default_side_entity_id_created_at_idx;

CREATE CONSTRAINT TRIGGER balanced_ledger_journal AFTER INSERT ON public.ledger_entries DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.check_ledger_journal_balance();

CREATE TRIGGER config_epoch_refresh_lineage BEFORE UPDATE ON public.config_epoch FOR EACH ROW EXECUTE FUNCTION public.refresh_config_epoch_lineage();

CREATE TRIGGER immutable_admin_audit_events BEFORE DELETE OR UPDATE ON public.admin_audit_events FOR EACH ROW EXECUTE FUNCTION public.immutable_record();

CREATE TRIGGER immutable_ledger_entries BEFORE DELETE OR UPDATE ON public.ledger_entries FOR EACH ROW EXECUTE FUNCTION public.immutable_record();

CREATE TRIGGER immutable_price_versions BEFORE DELETE OR UPDATE ON public.price_versions FOR EACH ROW EXECUTE FUNCTION public.immutable_record();

CREATE TRIGGER order_outbox_publish BEFORE INSERT ON public.outbox_events FOR EACH ROW EXECUTE FUNCTION public.order_outbox_publish();

CREATE TRIGGER plugins_immutable_revision BEFORE UPDATE ON public.plugins FOR EACH ROW EXECUTE FUNCTION public.keep_plugin_revision();

CREATE TRIGGER plugins_reject_revoked_activation BEFORE UPDATE OF enabled ON public.plugins FOR EACH ROW EXECUTE FUNCTION public.reject_revoked_plugin_activation();

ALTER TABLE ONLY public.api_keys
    ADD CONSTRAINT api_keys_billing_account_id_fkey FOREIGN KEY (billing_account_id) REFERENCES public.billing_accounts(id);

ALTER TABLE ONLY public.async_tasks
    ADD CONSTRAINT async_tasks_billing_account_id_fkey FOREIGN KEY (billing_account_id) REFERENCES public.billing_accounts(id);

ALTER TABLE ONLY public.async_tasks
    ADD CONSTRAINT async_tasks_channel_id_fkey FOREIGN KEY (channel_id) REFERENCES public.channels(id);

ALTER TABLE ONLY public.async_tasks
    ADD CONSTRAINT async_tasks_charge_id_fkey FOREIGN KEY (charge_id) REFERENCES public.billing_charges(id);

ALTER TABLE ONLY public.billing_account_pricing_overrides
    ADD CONSTRAINT billing_account_pricing_overrides_billing_account_id_fkey FOREIGN KEY (billing_account_id) REFERENCES public.billing_accounts(id);

ALTER TABLE ONLY public.billing_account_pricing_overrides
    ADD CONSTRAINT billing_account_pricing_overrides_billing_price_id_fkey FOREIGN KEY (billing_price_id) REFERENCES public.price_versions(id);

ALTER TABLE ONLY public.billing_reconciliation_policies
    ADD CONSTRAINT billing_reconciliation_policies_billing_account_id_fkey FOREIGN KEY (billing_account_id) REFERENCES public.billing_accounts(id);

ALTER TABLE ONLY public.billing_reconciliation_policies
    ADD CONSTRAINT billing_reconciliation_policies_channel_id_fkey FOREIGN KEY (channel_id) REFERENCES public.channels(id);

ALTER TABLE ONLY public.billing_reconciliation_policies
    ADD CONSTRAINT billing_reconciliation_policies_supplier_currency_fkey FOREIGN KEY (supplier_currency) REFERENCES public.billing_reconciliation_budgets(currency);

ALTER TABLE ONLY public.billing_reconciliation_polls
    ADD CONSTRAINT billing_reconciliation_polls_request_id_fkey FOREIGN KEY (request_id) REFERENCES public.billing_holds(request_id);

ALTER TABLE ONLY public.billing_reconciliation_receipts
    ADD CONSTRAINT billing_reconciliation_receipts_account_id_fkey FOREIGN KEY (account_id) REFERENCES public.upstream_accounts(id);

ALTER TABLE ONLY public.billing_reconciliation_receipts
    ADD CONSTRAINT billing_reconciliation_receipts_channel_id_fkey FOREIGN KEY (channel_id) REFERENCES public.channels(id);

ALTER TABLE ONLY public.billing_reconciliation_receipts
    ADD CONSTRAINT billing_reconciliation_receipts_request_id_fkey FOREIGN KEY (request_id) REFERENCES public.billing_holds(request_id);

ALTER TABLE ONLY public.billing_reconciliation_waivers
    ADD CONSTRAINT billing_reconciliation_waiver_policy_fkey FOREIGN KEY (billing_account_id, channel_id, supplier_currency) REFERENCES public.billing_reconciliation_policies(billing_account_id, channel_id, supplier_currency);

ALTER TABLE ONLY public.billing_reconciliation_waivers
    ADD CONSTRAINT billing_reconciliation_waivers_request_id_fkey FOREIGN KEY (request_id) REFERENCES public.billing_holds(request_id);

ALTER TABLE ONLY public.billing_resolutions
    ADD CONSTRAINT billing_resolutions_billing_price_id_fkey FOREIGN KEY (billing_price_id) REFERENCES public.price_versions(id);

ALTER TABLE ONLY public.billing_resolutions
    ADD CONSTRAINT billing_resolutions_request_id_fkey FOREIGN KEY (request_id) REFERENCES public.billing_holds(request_id);

ALTER TABLE ONLY public.channels
    ADD CONSTRAINT channels_account_id_fkey FOREIGN KEY (account_id) REFERENCES public.upstream_accounts(id);

ALTER TABLE ONLY public.channels
    ADD CONSTRAINT channels_provider_plugin_id_fkey FOREIGN KEY (provider_plugin_id) REFERENCES public.plugins(id);

ALTER TABLE ONLY public.plugin_release_acks
    ADD CONSTRAINT plugin_release_acks_config_epoch_fkey FOREIGN KEY (config_epoch) REFERENCES public.plugin_releases(config_epoch);

ALTER TABLE ONLY public.plugin_release_acks
    ADD CONSTRAINT plugin_release_acks_node_id_fkey FOREIGN KEY (node_id) REFERENCES public.gateway_config_nodes(id);

ALTER TABLE ONLY public.plugin_releases
    ADD CONSTRAINT plugin_releases_rollback_of_fkey FOREIGN KEY (rollback_of) REFERENCES public.plugin_releases(id);

ALTER TABLE ONLY public.plugin_revocations
    ADD CONSTRAINT plugin_revocations_plugin_id_fkey FOREIGN KEY (plugin_id) REFERENCES public.plugins(id);

ALTER TABLE ONLY public.routes
    ADD CONSTRAINT routes_billing_price_id_fkey FOREIGN KEY (billing_price_id) REFERENCES public.price_versions(id);

ALTER TABLE ONLY public.routes
    ADD CONSTRAINT routes_channel_id_fkey FOREIGN KEY (channel_id) REFERENCES public.channels(id);

ALTER TABLE ONLY public.routes
    ADD CONSTRAINT routes_cost_version_id_fkey FOREIGN KEY (cost_version_id) REFERENCES public.price_versions(id);

ALTER TABLE ONLY public.routes
    ADD CONSTRAINT routes_model_name_fkey FOREIGN KEY (model_name) REFERENCES public.models(name);

ALTER TABLE ONLY public.supplier_cells
    ADD CONSTRAINT supplier_cells_account_id_currency_fkey FOREIGN KEY (account_id, currency) REFERENCES public.supplier_root_balances(account_id, currency);

ALTER TABLE ONLY public.supplier_leases
    ADD CONSTRAINT supplier_leases_account_id_fkey FOREIGN KEY (account_id) REFERENCES public.upstream_accounts(id);

ALTER TABLE ONLY public.supplier_root_balances
    ADD CONSTRAINT supplier_root_balances_account_id_fkey FOREIGN KEY (account_id) REFERENCES public.upstream_accounts(id);

ALTER TABLE ONLY public.wallet_leases
    ADD CONSTRAINT wallet_leases_wallet_id_fkey FOREIGN KEY (wallet_id) REFERENCES public.wallets(id);

ALTER TABLE ONLY public.wallet_lots
    ADD CONSTRAINT wallet_lots_wallet_id_fkey FOREIGN KEY (wallet_id) REFERENCES public.wallets(id);

ALTER TABLE ONLY public.wallets
    ADD CONSTRAINT wallets_billing_account_id_fkey FOREIGN KEY (billing_account_id) REFERENCES public.billing_accounts(id);

INSERT INTO public.config_epoch (id, version) VALUES (1, 1);

INSERT INTO public.plugins (id, name, version, digest, provider_config, capability, kind, builtin, enabled, created_at)
VALUES
  ('4f08eb09-7967-5719-8323-10e3d951253b', 'openai', 'builtin:1.0.0', '4f08eb0979677719f32310e3d951253b7958379ae928be35398491f0d7634e77', '{"base_dialect":"openai_chat","usage_dialect":"openai","stream_framing":"sse","auth":{"scheme":"bearer"},"endpoints":{"chat":{"path":"/v1/chat/completions"},"embeddings":{"path":"/v1/embeddings"},"moderations":{"path":"/v1/moderations"},"completions":{"path":"/v1/completions"},"images":{"path":"/v1/images/generations"},"image_edits":{"path":"/v1/images/edits"},"speech":{"path":"/v1/audio/speech"},"transcription":{"path":"/v1/audio/transcriptions"},"translation":{"path":"/v1/audio/translations"},"default":{"path":"/v1/chat/completions"},"responses":{"path":"/v1/responses"}},"include_stream_usage_option":true}', 'PROVIDER', 'DECLARATIVE', true, true, 'epoch'),
  ('3600d2ef-af02-5c5a-82ba-510820ee5906', 'openai_compatible', 'builtin:1.0.0', '3600d2efaf028c5ad2ba510820ee59068e78ccad23a4e73398a1720b8ce6b430', '{"base_dialect":"openai_chat","usage_dialect":"openai","stream_framing":"sse","auth":{"scheme":"bearer"},"endpoints":{"chat":{"path":"/v1/chat/completions"},"embeddings":{"path":"/v1/embeddings"},"moderations":{"path":"/v1/moderations"},"completions":{"path":"/v1/completions"},"images":{"path":"/v1/images/generations"},"image_edits":{"path":"/v1/images/edits"},"speech":{"path":"/v1/audio/speech"},"transcription":{"path":"/v1/audio/transcriptions"},"translation":{"path":"/v1/audio/translations"},"default":{"path":"/v1/chat/completions"}},"include_stream_usage_option":true}', 'PROVIDER', 'DECLARATIVE', true, true, 'epoch'),
  ('6ca156f2-6757-5490-8efd-90aa65c438fe', 'anthropic', 'builtin:1.0.0', '6ca156f267572490defd90aa65c438fe1b77cd9ea5a9e6c039762c401723a9e5', '{"base_dialect":"anthropic_messages","usage_dialect":"anthropic","stream_framing":"sse","auth":{"scheme":"header","header_name":"x-api-key"},"extra_headers":{"anthropic-version":"2023-06-01"},"endpoints":{"messages":{"path":"/v1/messages"},"chat":{"path":"/v1/messages"},"responses":{"path":"/v1/messages"},"gemini":{"path":"/v1/messages"},"default":{"path":"/v1/messages"}}}', 'PROVIDER', 'DECLARATIVE', true, true, 'epoch'),
  ('db2b9ec8-ec60-5fca-8a06-5890e0070e8d', 'gemini', 'builtin:1.0.0', 'db2b9ec8ec60efca5a065890e0070e8deaf4d954aa4654bd156e1f3409e095be', '{"base_dialect":"gemini","usage_dialect":"gemini","stream_framing":"sse","auth":{"scheme":"header","header_name":"x-goog-api-key"},"model_field":null,"stream_field":null,"endpoints":{"default":{"path":"/v1beta/models/${encoded_model}:generateContent","stream_path":"/v1beta/models/${encoded_model}:streamGenerateContent","stream_query":{"alt":"sse"}}}}', 'PROVIDER', 'DECLARATIVE', true, true, 'epoch'),
  ('6e4afa95-2559-5721-8ef7-069bb063c5f8', 'azure', 'builtin:1.0.0', '6e4afa952559b7217ef7069bb063c5f85cb6881590fd59db013d908054689e55', '{"base_dialect":"openai_chat","usage_dialect":"openai","stream_framing":"sse","auth":{"scheme":"azure_key","default_api_version":"2024-10-21"},"include_stream_usage_option":true,"endpoints":{"embeddings":{"path":"/openai/deployments/${encoded_model}/embeddings"},"completions":{"path":"/openai/deployments/${encoded_model}/completions"},"default":{"path":"/openai/deployments/${encoded_model}/chat/completions"}}}', 'PROVIDER', 'DECLARATIVE', true, true, 'epoch'),
  ('a3786dc7-28da-570d-8fa3-6ee1857473a2', 'bedrock', 'builtin:1.0.0', 'a3786dc728da370d8fa36ee1857473a22b37c543925495e0ef68440137571598', '{"base_dialect":"anthropic_messages","usage_dialect":"anthropic","stream_framing":"bedrock_eventstream","auth":{"scheme":"aws_sigv4","service":"bedrock"},"model_field":null,"stream_field":null,"request_transforms":[{"op":"drop","path":"model"},{"op":"drop","path":"stream"},{"op":"force_set","path":"anthropic_version","value":"bedrock-2023-05-31"}],"endpoints":{"default":{"path":"/model/${encoded_model}/invoke","stream_path":"/model/${encoded_model}/invoke-with-response-stream"}}}', 'PROVIDER', 'DECLARATIVE', true, true, 'epoch'),
  ('159bb4c4-6295-5883-8919-ca9409afc47b', 'vertex', 'builtin:1.0.0', '159bb4c46295d8837919ca9409afc47befb2277f0772f26141a25d857d232645', '{"base_dialect":"gemini","usage_dialect":"gemini","stream_framing":"sse","auth":{"scheme":"gcp_vertex_oauth"},"model_field":null,"stream_field":null,"endpoints":{"default":{"path":"/models/${encoded_model}:generateContent","stream_path":"/models/${encoded_model}:streamGenerateContent","stream_query":{"alt":"sse"}}}}', 'PROVIDER', 'DECLARATIVE', true, true, 'epoch'),
  ('3a663e13-dc03-5610-84cf-f3183e22f0c3', 'deepseek', 'builtin:1.0.0', '3a663e13dc032610b4cff3183e22f0c3226dd61f06088167d70e1f9218d5606c', '{"base_dialect":"openai_chat","usage_dialect":"openai","stream_framing":"sse","auth":{"scheme":"bearer"},"include_stream_usage_option":true,"reasoning_field":"reasoning_content","request_transforms":[{"op":"rename","from":"max_completion_tokens","to":"max_tokens","overwrite":false},{"op":"drop","path":"max_completion_tokens"}],"usage_aliases":[{"target":"cached_input_tokens","source_path":"usage.prompt_cache_hit_tokens"}],"endpoints":{"chat":{"path":"/v1/chat/completions"},"embeddings":{"path":"/v1/embeddings"},"moderations":{"path":"/v1/moderations"},"completions":{"path":"/v1/completions"},"images":{"path":"/v1/images/generations"},"image_edits":{"path":"/v1/images/edits"},"speech":{"path":"/v1/audio/speech"},"transcription":{"path":"/v1/audio/transcriptions"},"translation":{"path":"/v1/audio/translations"},"default":{"path":"/v1/chat/completions"}}}', 'PROVIDER', 'DECLARATIVE', true, true, 'epoch'),
  ('97b8f143-a5a1-5de1-88a5-c2e4c05b3401', 'ollama', 'builtin:1.0.0', '97b8f143a5a1ade108a5c2e4c05b3401125bf7654cab554d59e2a86f94a250e2', '{"base_dialect":"openai_chat","usage_dialect":"openai","stream_framing":"sse","auth":{"scheme":"bearer"},"include_stream_usage_option":true,"request_transforms":[{"op":"rename","from":"max_completion_tokens","to":"max_tokens","overwrite":false},{"op":"drop","path":"max_completion_tokens"}],"endpoints":{"chat":{"path":"/v1/chat/completions"},"embeddings":{"path":"/v1/embeddings"},"moderations":{"path":"/v1/moderations"},"completions":{"path":"/v1/completions"},"images":{"path":"/v1/images/generations"},"image_edits":{"path":"/v1/images/edits"},"speech":{"path":"/v1/audio/speech"},"transcription":{"path":"/v1/audio/transcriptions"},"translation":{"path":"/v1/audio/translations"},"default":{"path":"/v1/chat/completions"},"responses":{"path":"/v1/responses"}}}', 'PROVIDER', 'DECLARATIVE', true, true, 'epoch'),
  ('2b9f2f42-23dc-50c1-885c-6bfb109e5106', 'qwen', 'builtin:1.0.0', '2b9f2f4223dcc0c1d85c6bfb109e510661963579254a8e54e3ed7147e17a1e6d', '{"base_dialect":"openai_chat","usage_dialect":"openai","stream_framing":"sse","auth":{"scheme":"bearer"},"include_stream_usage_option":true,"reasoning_field":"reasoning_content","request_transforms":[{"op":"rename","from":"max_completion_tokens","to":"max_tokens","overwrite":false},{"op":"drop","path":"max_completion_tokens"}],"error_rules":[{"when_http_status":[200],"path":"code","in":["Throttling","Throttling.RateQuota","Throttling.AllocationQuota"],"retry_kind":"rate_limit","message_path":"message"},{"when_http_status":[200],"path":"code","in":["InvalidApiKey","Arrearage"],"retry_kind":"auth_error","message_path":"message"}],"endpoints":{"chat":{"path":"/compatible-mode/v1/chat/completions"},"embeddings":{"path":"/compatible-mode/v1/embeddings"},"completions":{"path":"/compatible-mode/v1/completions"},"default":{"path":"/compatible-mode/v1/chat/completions"}}}', 'PROVIDER', 'DECLARATIVE', true, true, 'epoch'),
  ('896fcbc6-7f9d-5701-8064-495eb9bed6d0', 'volcengine', 'builtin:1.0.0', '896fcbc67f9db7015064495eb9bed6d0d822d17d1ee36d2924413120ce7ff167', '{"base_dialect":"openai_chat","usage_dialect":"openai","stream_framing":"sse","auth":{"scheme":"bearer"},"include_stream_usage_option":true,"reasoning_field":"reasoning_content","request_transforms":[{"op":"rename","from":"max_completion_tokens","to":"max_tokens","overwrite":false},{"op":"drop","path":"max_completion_tokens"}],"endpoints":{"chat":{"path":"/api/v3/chat/completions"},"embeddings":{"path":"/api/v3/embeddings"},"default":{"path":"/api/v3/chat/completions"}}}', 'PROVIDER', 'DECLARATIVE', true, true, 'epoch'),
  ('d083aa1d-f6fa-5ee3-821d-6223d582a4ac', 'zhipu', 'builtin:1.0.0', 'd083aa1df6fa8ee3021d6223d582a4acc1d8ab101d60d10c4402a19402997030', '{"base_dialect":"openai_chat","usage_dialect":"openai","stream_framing":"sse","auth":{"scheme":"bearer"},"include_stream_usage_option":false,"request_transforms":[{"op":"rename","from":"max_completion_tokens","to":"max_tokens","overwrite":false},{"op":"drop","path":"max_completion_tokens"},{"op":"clamp","path":"temperature","min":0.01,"max":0.99}],"error_rules":[{"when_http_status":[200],"path":"error.code","in":["1302","1303","1305"],"retry_kind":"rate_limit","message_path":"error.message"},{"when_http_status":[200],"path":"error.code","in":["1001","1002","1003","1004","1113"],"retry_kind":"auth_error","message_path":"error.message"}],"endpoints":{"chat":{"path":"/api/paas/v4/chat/completions"},"embeddings":{"path":"/api/paas/v4/embeddings"},"default":{"path":"/api/paas/v4/chat/completions"}}}', 'PROVIDER', 'DECLARATIVE', true, true, 'epoch'),
  ('138c925a-81a8-561e-8270-ac4d40704b96', 'moonshot', 'builtin:1.0.0', '138c925a81a8661e1270ac4d40704b96e4bb36fd607eeae06b191362daf2708f', '{"base_dialect":"openai_chat","usage_dialect":"openai","stream_framing":"sse","auth":{"scheme":"bearer"},"include_stream_usage_option":true,"request_transforms":[{"op":"rename","from":"max_completion_tokens","to":"max_tokens","overwrite":false},{"op":"drop","path":"max_completion_tokens"},{"op":"clamp","path":"temperature","min":0,"max":1}],"endpoints":{"chat":{"path":"/v1/chat/completions"},"embeddings":{"path":"/v1/embeddings"},"moderations":{"path":"/v1/moderations"},"completions":{"path":"/v1/completions"},"images":{"path":"/v1/images/generations"},"image_edits":{"path":"/v1/images/edits"},"speech":{"path":"/v1/audio/speech"},"transcription":{"path":"/v1/audio/transcriptions"},"translation":{"path":"/v1/audio/translations"},"default":{"path":"/v1/chat/completions"}}}', 'PROVIDER', 'DECLARATIVE', true, true, 'epoch'),
  ('1f6add9d-08fb-5ed2-8da3-fa160682728c', 'minimax', 'builtin:1.0.0', '1f6add9d08fbfed2ada3fa160682728ca4003f5495a2a46d0ba2a9a35af970dd', '{"base_dialect":"openai_chat","usage_dialect":"openai","stream_framing":"sse","auth":{"scheme":"bearer"},"include_stream_usage_option":false,"request_transforms":[{"op":"rename","from":"max_completion_tokens","to":"max_tokens","overwrite":false},{"op":"drop","path":"max_completion_tokens"},{"op":"clamp","path":"temperature","min":0.01,"max":1}],"error_rules":[{"when_http_status":[200],"path":"base_resp.status_code","equals":1002,"retry_kind":"rate_limit","message_path":"base_resp.status_msg"},{"when_http_status":[200],"path":"base_resp.status_code","in":[1004,1008],"retry_kind":"auth_error","message_path":"base_resp.status_msg"},{"when_http_status":[200],"path":"base_resp.status_code","in":[1000,1001,2013],"retry_kind":"retryable","message_path":"base_resp.status_msg"}],"endpoints":{"chat":{"path":"/v1/text/chatcompletion_v2"},"embeddings":{"path":"/v1/embeddings"},"default":{"path":"/v1/text/chatcompletion_v2"}}}', 'PROVIDER', 'DECLARATIVE', true, true, 'epoch'),
  ('45a233d2-b479-5d01-8ae9-a5c155b49c50', 'openrouter', 'builtin:1.0.0', '45a233d2b4792d01fae9a5c155b49c505a7cdd9ff7b048124973aa1d454bf8a4', '{"base_dialect":"openai_chat","usage_dialect":"openai","stream_framing":"sse","auth":{"scheme":"bearer"},"include_stream_usage_option":true,"reasoning_field":"reasoning","endpoints":{"chat":{"path":"/api/v1/chat/completions"},"embeddings":{"path":"/api/v1/embeddings"},"completions":{"path":"/api/v1/completions"},"default":{"path":"/api/v1/chat/completions"}}}', 'PROVIDER', 'DECLARATIVE', true, true, 'epoch');
