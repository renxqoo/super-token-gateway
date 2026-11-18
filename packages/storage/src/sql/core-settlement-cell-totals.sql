CREATE OR REPLACE FUNCTION core_funding.flush_first_final_costs_v1(items core_funding.first_final_cost_v1[],pending jsonb)
RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE
 delta record;
 cell_state core_funding.balance_cells%ROWTYPE;
 cell_key text;
 base jsonb;
 next_state jsonb;
 cells jsonb=jsonb_build_object('cells',pending->'cells');
 journals jsonb;
 entries jsonb;
 changed integer;
BEGIN
 IF EXISTS(
  SELECT 1 FROM (SELECT DISTINCT (s.hold).account_id AS id FROM unnest(items) s WHERE s.journal_id IS NOT NULL) a
  WHERE NOT core_funding.held_v1(a.id)
 ) THEN RAISE EXCEPTION 'funding_fence_required' USING ERRCODE='P0403';END IF;
 FOR delta IN
  SELECT (s.hold).account_id AS account_id,(s.hold).currency AS currency,(s.hold).funding_cell AS cell,
   sum((s.hold).current_reserved_atoms) AS reserved,sum(s.cost_atoms) AS spent,count(*) AS revisions,
   bool_and(s.cost_atoms IS NOT NULL AND s.cost_atoms>=0 AND s.cost_atoms=trunc(s.cost_atoms)
    AND s.cost_atoms<=(s.hold).current_reserved_atoms) AS covered
  FROM unnest(items) s WHERE s.journal_id IS NOT NULL
  GROUP BY (s.hold).account_id,(s.hold).currency,(s.hold).funding_cell
  ORDER BY (s.hold).account_id,(s.hold).currency,(s.hold).funding_cell
 LOOP
  IF delta.covered IS DISTINCT FROM true THEN RAISE EXCEPTION 'funding_delta_invalid' USING ERRCODE='P0400';END IF;
  cell_key=delta.account_id::text||':'||delta.currency||':'||delta.cell::text;
  base=cells->'cells'->cell_key->'base';
  IF base IS NULL THEN
   SELECT * INTO STRICT cell_state FROM core_funding.balance_cells c
   WHERE (c.account_id,c.currency,c.cell)=(delta.account_id,delta.currency,delta.cell) FOR NO KEY UPDATE NOWAIT;
   base=to_jsonb(cell_state);
  ELSE
   cell_state=jsonb_populate_record(NULL::core_funding.balance_cells,cells->'cells'->cell_key->'next');
  END IF;
  IF cell_state.reserved_atoms<delta.reserved THEN RAISE EXCEPTION 'funding_counter_overflow' USING ERRCODE='22003';END IF;
  IF cell_state.spent_atoms+delta.spent>cell_state.spent_limit OR cell_state.revision+delta.revisions>cell_state.revision_limit
   OR cell_state.balance_atoms<cell_state.reserved_atoms THEN RAISE EXCEPTION 'funding_outer_transaction_required' USING ERRCODE='P0F01';END IF;
  next_state=to_jsonb(cell_state)||jsonb_build_object('balance_atoms',cell_state.balance_atoms-delta.spent,
   'reserved_atoms',cell_state.reserved_atoms-delta.reserved,'spent_atoms',cell_state.spent_atoms+delta.spent,'revision',cell_state.revision+delta.revisions);
  cells=jsonb_set(cells,ARRAY['cells',cell_key],jsonb_build_object('base',base,'next',next_state));
 END LOOP;

 INSERT INTO public.upstream_costs(attempt_id,revision,currency,cost_atoms,complete,disputed,evidence_digest,evidence_id,cost_version_id)
 SELECT (s.hold).attempt_id,1,(s.hold).currency,s.cost_atoms,true,false,
  public.core_digest_v1(jsonb_build_object('snapshot',s.snapshot_digest,'final_dimensions',to_jsonb(s.final_dimensions),'cost_version_id',(s.hold).cost_version_id,'estimated_quantities_digest',(s.hold).estimated_quantities_digest,'cost_atoms',s.cost_atoms::text)),
  s.evidence_id,(s.hold).cost_version_id
 FROM unnest(items) s;
 INSERT INTO public.upstream_cost_heads(attempt_id,revision)
 SELECT (s.hold).attempt_id,1 FROM unnest(items) s;
 UPDATE public.upstream_holds h SET state='SETTLED',current_reserved_atoms=0,final_cost_atoms=s.cost_atoms
 FROM unnest(items) s WHERE h.attempt_id=(s.hold).attempt_id AND h.state='HELD'
 AND h.current_reserved_atoms=(s.hold).current_reserved_atoms;
 GET DIAGNOSTICS changed=ROW_COUNT;
 IF changed<>cardinality(items) THEN RAISE EXCEPTION 'evidence_hold_base_changed' USING ERRCODE='P0500';END IF;

 SELECT coalesce(jsonb_agg(jsonb_build_object('id',s.journal_id,'account_id',(s.hold).account_id,'channel_id',(s.hold).channel_id,
  'attempt_id',(s.hold).attempt_id,'currency',(s.hold).currency,'kind',CASE WHEN s.cost_atoms<>0 THEN 'COST' ELSE 'RISK' END,
  'reference','cost:'||(s.hold).attempt_id||':'||s.snapshot_revision)),'[]')
 INTO journals FROM unnest(items) s WHERE s.journal_id IS NOT NULL;
 SELECT coalesce(jsonb_agg(to_jsonb(leg)),'[]') INTO entries FROM (
  SELECT s.journal_id,row_number() OVER (PARTITION BY s.journal_id ORDER BY v.n)-1 AS ordinal,(s.hold).currency,v.account,v.atoms
  FROM unnest(items) s CROSS JOIN LATERAL (VALUES
   (0,'upstream:available',(s.hold).current_reserved_atoms-s.cost_atoms),
   (1,'upstream:reserved',-(s.hold).current_reserved_atoms),
   (2,'upstream:expense',s.cost_atoms)
  ) v(n,account,atoms) WHERE s.journal_id IS NOT NULL AND v.atoms<>0
 ) leg;
 RETURN pending||cells||jsonb_build_object('journals',(pending->'journals')||journals,'entries',(pending->'entries')||entries);
END $$;
