CREATE OR REPLACE FUNCTION public.commit_leased_batch(p_items jsonb, p_next_expires_at timestamp with time zone, p_renew_threshold timestamp with time zone, p_now timestamp with time zone) RETURNS jsonb
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
  v_charge_ids UUID[];
  v_event_ids UUID[];
  v_req_ids UUID[];
  v_account_ids UUID[];
  v_wallet_ids UUID[];
  v_api_key_ids UUID[];
  v_override_ids UUID[];
  v_disc_nums NUMERIC[];
  v_disc_dens NUMERIC[];
  v_req_wl_ids UUID[];
  v_req_sl_ids UUID[];
  v_sel_attempt_ids UUID[];
  v_channel_ids UUID[];
  v_models TEXT[];
  v_operations TEXT[];
  v_currencies TEXT[];
  v_price_ids UUID[];
  v_cost_ids UUID[];
  v_usages JSONB[];
  v_usage_digests TEXT[];
  v_cust_atoms NUMERIC[];
  v_sup_atoms NUMERIC[];
  v_sup_currencies TEXT[];
  v_plugin_audits JSONB[];
  v_charge_policies TEXT[];
  v_item_shards SMALLINT[];
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
  v_sl_cells SMALLINT[];
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
  v_w_avail_ids UUID[];
  v_w_avail_atoms NUMERIC[];
  v_extra_w_ids UUID[];
  v_extra_w_atoms NUMERIC[];
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
      x.charge_id,
      x.event_id,
      x.request_id,
      x.billing_account_id,
      x.wallet_id,
      x.api_key_id,
      x.pricing_override_id,
      x.discount_numerator,
      x.discount_denominator,
      x.wallet_lease_id,
      x.supplier_lease_id,
      x.selected_attempt_id,
      x.channel_id,
      x.model,
      x.operation,
      x.currency,
      x.billing_price_id,
      x.cost_version_id,
      x.usage,
      x.usage_digest,
      x.customer_atoms,
      x.supplier_atoms,
      x.supplier_currency,
      x.plugin_audit,
      x.charge_policy,
      CASE
        WHEN x.billing_account_id IS NULL THEN 0::smallint
        ELSE (((get_byte(b.raw, 0) >> 4) # (get_byte(b.raw, 15) & 15)))::smallint
      END AS outbox_shard_id
    FROM jsonb_to_recordset(p_items) AS x(
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
    CROSS JOIN LATERAL (
      SELECT CASE WHEN x.billing_account_id IS NULL THEN NULL::bytea ELSE uuid_send(x.billing_account_id) END AS raw
    ) b
  ),
  ord AS MATERIALIZED (
    SELECT * FROM it ORDER BY request_id
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
    o.charge_ids,
    o.event_ids,
    o.req_ids,
    o.account_ids,
    o.wallet_ids,
    o.api_key_ids,
    o.override_ids,
    o.disc_nums,
    o.disc_dens,
    o.req_wl_ids,
    o.req_sl_ids,
    o.sel_attempt_ids,
    o.channel_ids,
    o.models,
    o.operations,
    o.currencies,
    o.price_ids,
    o.cost_ids,
    o.usages,
    o.usage_digests,
    o.cust_atoms,
    o.sup_atoms,
    o.sup_currencies,
    o.plugin_audits,
    o.charge_policies,
    o.item_shards,
    wl.ids,
    wl.needs,
    sl.ids,
    sl.needs,
    s.w_ids,
    s.shards,
    s.needs_slow
  INTO
    v_charge_ids,
    v_event_ids,
    v_req_ids,
    v_account_ids,
    v_wallet_ids,
    v_api_key_ids,
    v_override_ids,
    v_disc_nums,
    v_disc_dens,
    v_req_wl_ids,
    v_req_sl_ids,
    v_sel_attempt_ids,
    v_channel_ids,
    v_models,
    v_operations,
    v_currencies,
    v_price_ids,
    v_cost_ids,
    v_usages,
    v_usage_digests,
    v_cust_atoms,
    v_sup_atoms,
    v_sup_currencies,
    v_plugin_audits,
    v_charge_policies,
    v_item_shards,
    v_wl_ids,
    v_wl_needs,
    v_sl_ids,
    v_sl_needs,
    v_w_ids,
    v_shards,
    v_needs_slow
  FROM (
    SELECT
      array_agg(charge_id) AS charge_ids,
      array_agg(event_id) AS event_ids,
      array_agg(request_id) AS req_ids,
      array_agg(billing_account_id) AS account_ids,
      array_agg(wallet_id) AS wallet_ids,
      array_agg(api_key_id) AS api_key_ids,
      array_agg(pricing_override_id) AS override_ids,
      array_agg(discount_numerator) AS disc_nums,
      array_agg(discount_denominator) AS disc_dens,
      array_agg(wallet_lease_id) AS req_wl_ids,
      array_agg(supplier_lease_id) AS req_sl_ids,
      array_agg(selected_attempt_id) AS sel_attempt_ids,
      array_agg(channel_id) AS channel_ids,
      array_agg(model) AS models,
      array_agg(operation) AS operations,
      array_agg(currency) AS currencies,
      array_agg(billing_price_id) AS price_ids,
      array_agg(cost_version_id) AS cost_ids,
      array_agg(usage) AS usages,
      array_agg(usage_digest) AS usage_digests,
      array_agg(customer_atoms) AS cust_atoms,
      array_agg(supplier_atoms) AS sup_atoms,
      array_agg(supplier_currency) AS sup_currencies,
      array_agg(plugin_audit) AS plugin_audits,
      array_agg(charge_policy) AS charge_policies,
      array_agg(outbox_shard_id) AS item_shards
    FROM ord
  ) o
  CROSS JOIN (SELECT array_agg(id) AS ids, array_agg(need) AS needs FROM wl_agg) wl
  CROSS JOIN (SELECT array_agg(id) AS ids, array_agg(need) AS needs FROM sl_agg) sl
  CROSS JOIN (
    SELECT
      array_agg(DISTINCT it.wallet_id ORDER BY it.wallet_id) AS w_ids,
      array_agg(DISTINCT it.outbox_shard_id ORDER BY it.outbox_shard_id) AS shards,
      EXISTS (
        SELECT 1
        FROM (SELECT wallet_id FROM it GROUP BY wallet_id HAVING count(DISTINCT wallet_lease_id) > 1) wm
      ) OR (count(DISTINCT it.request_id) <> v_item_count) AS needs_slow
    FROM it
  ) s;

  IF v_needs_slow THEN
    RETURN jsonb_build_object('fast', false);
  END IF;

  WITH locked AS MATERIALIZED (
    SELECT h.ctid, h.request_id, h.state, h.wallet_lease_id, h.supplier_lease_id, h.key_reserved_atoms, h.supplier_holds
    FROM public.billing_holds h
    WHERE h.request_id = ANY(v_req_ids)
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
  FROM unnest(v_req_ids, v_req_wl_ids, v_req_sl_ids, v_sel_attempt_ids, v_channel_ids, v_sup_currencies)
    AS u(request_id, wallet_lease_id, supplier_lease_id, selected_attempt_id, channel_id, supplier_currency)
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
      OR (req.need > 0 AND (wl.lot_len <> 1 OR req.need > wl.lot_amount))
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
    SELECT sl.ctid, sl.id, sl.state, sl.remaining_atoms, sl.account_id, sl.currency, sl.cell
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
    array_agg(sl.currency ORDER BY req.ord),
    array_agg(sl.cell ORDER BY req.ord)
  INTO v_needs_slow, v_sl_ctids, v_sl_accts, v_sl_curs, v_sl_cells
  FROM unnest(v_sl_ids, v_sl_needs) WITH ORDINALITY AS req(lid, need, ord)
  LEFT JOIN locked sl ON sl.id = req.lid;

  IF COALESCE(v_needs_slow, true) THEN
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
        u.usage,
        u.usage_digest,
        u.customer_atoms,
        u.supplier_atoms,
        u.supplier_currency,
        u.plugin_audit,
        u.charge_policy,
        wl.lot_id,
        sl.account_id AS supplier_account_id,
        sl.currency AS sl_currency,
        sl.cell AS supplier_cell
      FROM unnest(
        v_charge_ids,
        v_req_ids,
        v_account_ids,
        v_wallet_ids,
        v_api_key_ids,
        v_override_ids,
        v_disc_nums,
        v_disc_dens,
        v_req_wl_ids,
        v_req_sl_ids,
        v_sel_attempt_ids,
        v_channel_ids,
        v_models,
        v_operations,
        v_currencies,
        v_price_ids,
        v_cost_ids,
        v_usages,
        v_usage_digests,
        v_cust_atoms,
        v_sup_atoms,
        v_sup_currencies,
        v_plugin_audits,
        v_charge_policies
      ) AS u(
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
        charge_policy
      )
      JOIN unnest(v_wl_ids, v_wl_lot_ids) AS wl(id, lot_id) ON wl.id = u.wallet_lease_id
      JOIN unnest(v_sl_ids, v_sl_accts, v_sl_curs, v_sl_cells) AS sl(id, account_id, currency, cell) ON sl.id = u.supplier_lease_id
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
    COALESCE(ws.ids, '{}'::uuid[]),
    COALESCE(ws.spents, '{}'::numeric[]),
    COALESCE(ls.ids, '{}'::uuid[]),
    COALESCE(ls.spents, '{}'::numeric[]),
    COALESCE(cs.accts, '{}'::uuid[]),
    COALESCE(cs.curs, '{}'::text[]),
    COALESCE(cs.cells, '{}'::smallint[]),
    COALESCE(cs.spents, '{}'::numeric[])
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
    v_cell_spend_atoms
  FROM (SELECT array_agg(id) AS ids, array_agg(spent) AS spents FROM w_spend) ws
  CROSS JOIN (SELECT array_agg(id) AS ids, array_agg(spent) AS spents FROM l_spend) ls
  CROSS JOIN (
    SELECT
      array_agg(account_id) AS accts,
      array_agg(currency) AS curs,
      array_agg(cell) AS cells,
      array_agg(spent) AS spents
    FROM c_spend
  ) cs;

  IF cardinality(v_w_spend_ids) > 0 OR cardinality(v_lot_spend_ids) > 0 OR cardinality(v_cell_spend_accts) > 0 THEN
    WITH
      locked_w AS MATERIALIZED (
        SELECT w.id
        FROM public.wallets w
        WHERE w.id = ANY(v_w_spend_ids)
        ORDER BY w.id
        FOR NO KEY UPDATE
      ),
      locked_l AS MATERIALIZED (
        SELECT l.id
        FROM public.wallet_lots l
        WHERE (SELECT count(*) FROM locked_w) >= 0
          AND l.id = ANY(v_lot_spend_ids)
        ORDER BY l.id
        FOR NO KEY UPDATE
      ),
      locked_c AS MATERIALIZED (
        SELECT c.account_id, c.currency, c.cell
        FROM public.supplier_cells c
        JOIN unnest(v_cell_spend_accts, v_cell_spend_curs, v_cell_spend_nums) AS k(account_id, currency, cell)
          ON c.account_id = k.account_id AND c.currency = k.currency AND c.cell = k.cell
        WHERE (SELECT count(*) FROM locked_l) >= 0
        ORDER BY c.account_id, c.currency, c.cell
        FOR NO KEY UPDATE OF c
      ),
      upd_w AS (
        UPDATE public.wallets AS w SET
          reserved_atoms = w.reserved_atoms - req.spent,
          spent_atoms = w.spent_atoms + req.spent
        FROM locked_w lw
        JOIN unnest(v_w_spend_ids, v_w_spend_atoms) AS req(id, spent) ON req.id = lw.id
        WHERE w.id = lw.id
        RETURNING w.id, w.available_atoms
      ),
      upd_l AS (
        UPDATE public.wallet_lots AS l SET
          reserved_atoms = l.reserved_atoms - req.spent,
          spent_atoms = l.spent_atoms + req.spent
        FROM locked_l ll
        JOIN unnest(v_lot_spend_ids, v_lot_spend_atoms) AS req(id, spent) ON req.id = ll.id
        WHERE l.id = ll.id
      ),
      upd_c AS (
        UPDATE public.supplier_cells AS c SET
          reserved_atoms = c.reserved_atoms - req.spent,
          balance_atoms = c.balance_atoms - req.spent,
          spent_atoms = c.spent_atoms + req.spent,
          revision = c.revision + 1
        FROM locked_c lc
        JOIN unnest(v_cell_spend_accts, v_cell_spend_curs, v_cell_spend_nums, v_cell_spend_atoms)
          AS req(account_id, currency, cell, spent)
          ON req.account_id = lc.account_id AND req.currency = lc.currency AND req.cell = lc.cell
        WHERE c.account_id = lc.account_id
          AND c.currency = lc.currency
          AND c.cell = lc.cell
      )
    SELECT
      COALESCE(array_agg(id), '{}'::uuid[]),
      COALESCE(array_agg(available_atoms), '{}'::numeric[])
    INTO v_w_avail_ids, v_w_avail_atoms
    FROM upd_w;
  ELSE
    v_w_avail_ids := '{}'::uuid[];
    v_w_avail_atoms := '{}'::numeric[];
  END IF;

  IF cardinality(v_w_avail_ids) < cardinality(v_w_ids) THEN
    SELECT
      COALESCE(array_agg(w.id), '{}'::uuid[]),
      COALESCE(array_agg(w.available_atoms), '{}'::numeric[])
    INTO v_extra_w_ids, v_extra_w_atoms
    FROM public.wallets w
    WHERE w.id = ANY(v_w_ids) AND NOT (w.id = ANY(v_w_avail_ids));
    v_w_avail_ids := v_w_avail_ids || v_extra_w_ids;
    v_w_avail_atoms := v_w_avail_atoms || v_extra_w_atoms;
  END IF;

  WITH adv AS MATERIALIZED (
    SELECT pg_advisory_xact_lock(81442591, 100 + s::integer)
    FROM unnest(v_shards) AS s
  )
  INSERT INTO public.outbox_events (event_id, kind, request_id, billing_account_id, payload)
  SELECT
    o.event_id,
    'billing.settled',
    o.request_id,
    o.billing_account_id,
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
  FROM unnest(
    v_event_ids,
    v_charge_ids,
    v_req_ids,
    v_account_ids,
    v_wallet_ids,
    v_api_key_ids,
    v_sel_attempt_ids,
    v_channel_ids,
    v_models,
    v_operations,
    v_currencies,
    v_override_ids,
    v_usages,
    v_cust_atoms,
    v_sup_atoms,
    v_item_shards
  ) AS o(
    event_id,
    charge_id,
    request_id,
    billing_account_id,
    wallet_id,
    api_key_id,
    selected_attempt_id,
    channel_id,
    model,
    operation,
    currency,
    pricing_override_id,
    usage,
    customer_atoms,
    supplier_atoms,
    outbox_shard_id
  )
  LEFT JOIN unnest(v_w_avail_ids, v_w_avail_atoms) AS wa(id, available_atoms) ON wa.id = o.wallet_id
  WHERE (SELECT count(*) FROM adv) >= 0
  ORDER BY o.outbox_shard_id ASC, o.request_id ASC;

  RETURN jsonb_build_object('fast', true, 'wl_expiries', v_wl_exp, 'sl_expiries', v_sl_exp);
END;
$$;

ALTER TABLE public.billing_charges
  DROP CONSTRAINT billing_charges_currency_check,
  DROP CONSTRAINT billing_charges_supplier_currency_check,
  DROP CONSTRAINT billing_charges_usage_digest_check,
  ADD CONSTRAINT billing_charges_currency_check CHECK (
    char_length(currency) >= 3 AND char_length(currency) <= 8
    AND ltrim(currency, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ') = ''
  ),
  ADD CONSTRAINT billing_charges_supplier_currency_check CHECK (
    supplier_currency IS NULL OR (
      char_length(supplier_currency) >= 3 AND char_length(supplier_currency) <= 8
      AND ltrim(supplier_currency, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ') = ''
    )
  ),
  ADD CONSTRAINT billing_charges_usage_digest_check CHECK (
    char_length(usage_digest) = 64 AND ltrim(usage_digest, '0123456789abcdef') = ''
  );

ALTER TABLE public.ledger_entries
  DROP CONSTRAINT ledger_entries_currency_check,
  DROP CONSTRAINT ledger_entries_account_check,
  ADD CONSTRAINT ledger_entries_currency_check CHECK (
    char_length(currency) >= 3 AND char_length(currency) <= 8
    AND ltrim(currency, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ') = ''
  ),
  ADD CONSTRAINT ledger_entries_account_check CHECK (
    account = ANY (ARRAY[
      'system:funding'::text,
      'system:revenue'::text,
      'system:receivable'::text,
      'system:moderation_loss'::text,
      'system:adjustment'::text,
      'upstream:available'::text,
      'upstream:expense'::text,
      'upstream:funding'::text,
      'upstream:adjustment'::text
    ]) OR (
      left(account, 4) = 'lot:'
      AND substring(account FROM 41) = ANY (ARRAY[':available'::text, ':spent'::text, ':expired'::text])
      AND char_length(substring(account FROM 5 FOR 36)) = 36
      AND ltrim(substring(account FROM 5 FOR 36), '0123456789abcdef-') = ''
    )
  );

CREATE OR REPLACE FUNCTION public.acquire_leased_holds(
  p_node_id text,
  p_wallet_lease_ids uuid[],
  p_supplier_lease_ids uuid[],
  p_items jsonb,
  p_check_debt_grants boolean
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
  IF p_items IS NULL OR jsonb_array_length(p_items) = 0 THEN
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
    WHERE l.state = 'ACTIVE' AND l.expires_at > v_now
      AND (l.earliest_lot_expires_at IS NULL OR l.earliest_lot_expires_at > v_now)
      AND w.freeze_reason IS NULL
      AND CASE WHEN w.reserved_atoms >= w.debt_atoms THEN true
        WHEN p_check_debt_grants THEN w.available_atoms + w.reserved_atoms - w.debt_atoms >= (
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
      u.held_atoms, 0, '[]'::jsonb, u.supplier_holds, u.wallet_lease_id, u.supplier_lease_id, 'HELD', u.expires_at
    FROM jsonb_to_recordset(p_items) AS u(
      request_id uuid,
      billing_account_id uuid,
      wallet_id uuid,
      api_key_id uuid,
      pricing_override_id uuid,
      discount_numerator numeric,
      discount_denominator numeric,
      operation text,
      model text,
      fingerprint text,
      held_atoms numeric,
      supplier_holds jsonb,
      wallet_lease_id uuid,
      supplier_lease_id uuid,
      expires_at timestamptz
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
