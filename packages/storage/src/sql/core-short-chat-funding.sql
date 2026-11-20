CREATE TABLE core_funding.writer_groups (
 cell smallint PRIMARY KEY CHECK (cell BETWEEN 0 AND 15),
 epoch bigint NOT NULL CHECK (epoch > 0)
);
INSERT INTO core_funding.writer_groups(cell,epoch) SELECT value,1 FROM generate_series(0,15) value;

CREATE FUNCTION public.core_short_chat_funding_v1(group_id smallint,expected_epoch bigint)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
 PERFORM 1 FROM core_funding.writer_groups WHERE cell=group_id AND epoch=expected_epoch FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'short_chat_funding_owner_changed' USING ERRCODE='P0403';END IF;
 PERFORM set_config('stg.short_chat_funding_cell',group_id::text,true);
END $$;
REVOKE ALL ON FUNCTION public.core_short_chat_funding_v1(smallint,bigint) FROM PUBLIC;

CREATE OR REPLACE FUNCTION core_funding.reserve_batch_v1(account uuid,cur text,attempt uuid,amount numeric,pending jsonb) RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE c core_funding.balance_cells%ROWTYPE;entry jsonb;k text;owned smallint=NULLIF(current_setting('stg.short_chat_funding_cell',true),'')::smallint;preferred integer=coalesce(owned,get_byte(uuid_send(attempt),15)%16);BEGIN
 IF NOT core_funding.held_v1(account) THEN RAISE EXCEPTION 'funding_fence_required' USING ERRCODE='P0403';END IF;
 IF jsonb_typeof(pending->'cells') IS DISTINCT FROM 'object' OR amount IS NULL OR amount<0 OR amount<>trunc(amount) OR amount>99999999999999999999999999999999999999 THEN RAISE EXCEPTION 'funding_budget_invalid' USING ERRCODE='P0400';END IF;
 IF NOT EXISTS(SELECT 1 FROM public.upstream_accounts WHERE id=account AND state='ACTIVE' AND mode='PREPAID') THEN RAISE EXCEPTION 'upstream_funds_unavailable' USING ERRCODE='P0402';END IF;
 SELECT state.* INTO c FROM core_funding.balance_cells state
 WHERE state.account_id=account AND state.currency=cur AND (owned IS NULL OR state.cell=owned)
  AND state.balance_atoms-coalesce((pending->'cells'->(state.account_id::text||':'||state.currency||':'||state.cell::text)->'next'->>'reserved_atoms')::numeric,state.reserved_atoms)>=amount
  AND coalesce((pending->'cells'->(state.account_id::text||':'||state.currency||':'||state.cell::text)->'next'->>'revision')::numeric,state.revision)<state.revision_limit
 ORDER BY (state.cell-preferred+16)%16 LIMIT 1 FOR NO KEY UPDATE OF state SKIP LOCKED;
 IF NOT FOUND THEN
  IF EXISTS(SELECT 1 FROM core_funding.balance_cells state
   WHERE state.account_id=account AND state.currency=cur AND (owned IS NULL OR state.cell=owned)
    AND state.balance_atoms-coalesce((pending->'cells'->(state.account_id::text||':'||state.currency||':'||state.cell::text)->'next'->>'reserved_atoms')::numeric,state.reserved_atoms)>=amount
    AND coalesce((pending->'cells'->(state.account_id::text||':'||state.currency||':'||state.cell::text)->'next'->>'revision')::numeric,state.revision)<state.revision_limit) THEN RAISE EXCEPTION 'core_busy' USING ERRCODE='P0503';END IF;
  RAISE EXCEPTION 'funding_outer_transaction_required' USING ERRCODE='P0F01';
 END IF;
 k=account::text||':'||cur||':'||c.cell::text;entry=pending->'cells'->k;
 IF entry IS NULL THEN entry=jsonb_build_object('base',to_jsonb(c),'next',to_jsonb(c));ELSE c=jsonb_populate_record(NULL::core_funding.balance_cells,entry->'next');END IF;
 pending=jsonb_set(pending,ARRAY['cells',k],jsonb_build_object('base',entry->'base','next',(entry->'next')||jsonb_build_object('reserved_atoms',c.reserved_atoms+amount,'revision',c.revision+1)));
 RETURN jsonb_build_object('cell',c.cell,'pending',pending);
END $$;

CREATE OR REPLACE FUNCTION core_funding.reserve_v1(account uuid,cur text,attempt uuid,amount numeric) RETURNS smallint LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE target smallint;owned smallint=NULLIF(current_setting('stg.short_chat_funding_cell',true),'')::smallint;preferred integer=coalesce(owned,get_byte(uuid_send(attempt),15)%16);c core_funding.balance_cells%ROWTYPE;free numeric;root numeric;budget_grant numeric;seed record;slots integer=16;actual_spent numeric;BEGIN
 IF NOT core_funding.held_v1(account) THEN RAISE EXCEPTION 'funding_fence_required' USING ERRCODE='P0403';END IF;
 IF amount IS NULL OR amount<0 OR amount<>trunc(amount) OR amount>99999999999999999999999999999999999999 THEN RAISE EXCEPTION 'funding_budget_invalid' USING ERRCODE='P0400';END IF;
 IF NOT EXISTS(SELECT 1 FROM public.upstream_accounts WHERE id=account AND state='ACTIVE' AND mode='PREPAID') THEN RAISE EXCEPTION 'upstream_funds_unavailable' USING ERRCODE='P0402';END IF;
 SELECT * INTO c FROM core_funding.balance_cells WHERE account_id=account AND currency=cur AND (owned IS NULL OR cell=owned) AND balance_atoms-reserved_atoms>=amount AND revision<revision_limit
 ORDER BY (cell-preferred+16)%16 LIMIT 1 FOR NO KEY UPDATE SKIP LOCKED;
 IF NOT FOUND THEN
  IF NOT core_funding.held_v1(account,true) THEN
   IF EXISTS(SELECT 1 FROM core_funding.balance_cells WHERE account_id=account AND currency=cur AND (owned IS NULL OR cell=owned) AND balance_atoms-reserved_atoms>=amount AND revision<revision_limit) THEN RAISE EXCEPTION 'core_busy' USING ERRCODE='P0503';END IF;
   RAISE EXCEPTION 'funding_outer_transaction_required' USING ERRCODE='P0F01';
  END IF;
  SELECT balance_atoms-reserved_atoms INTO free FROM public.core_upstream_balances_v1 WHERE account_id=account AND currency=cur;
  IF free IS NULL OR free<amount THEN RAISE EXCEPTION 'upstream_funds_unavailable' USING ERRCODE='P0402';END IF;
  INSERT INTO core_funding.balance_cells(account_id,currency,cell,spent_limit,revision_limit) SELECT account,cur,i,0,0 FROM generate_series(0,15) i ON CONFLICT DO NOTHING;
  IF NOT EXISTS(SELECT 1 FROM core_funding.balance_cells WHERE account_id=account AND currency=cur AND revision<revision_limit) THEN
   SELECT spent_atoms INTO actual_spent FROM public.core_upstream_balances_v1 WHERE account_id=account AND currency=cur;
   FOR seed IN SELECT cell,balance_atoms,reserved_atoms,spent_atoms,revision FROM core_funding.balance_cells WHERE account_id=account AND currency=cur ORDER BY (cell-preferred+16)%16 FOR NO KEY UPDATE NOWAIT LOOP
    IF seed.cell=preferred THEN PERFORM core_funding.collect_v1(account,cur,greatest(amount-(seed.balance_atoms-seed.reserved_atoms),0),preferred);END IF;
    SELECT balance_atoms INTO STRICT root FROM public.upstream_account_balances WHERE account_id=account AND currency=cur;
    budget_grant=least(root,greatest(CASE WHEN seed.cell=preferred THEN amount-(seed.balance_atoms-seed.reserved_atoms) ELSE 0 END,div(root,slots)));
    IF budget_grant>0 THEN PERFORM core_funding.transfer_v1(account,cur,seed.cell,budget_grant,0,0,'initialize:'||attempt||':'||seed.cell);END IF;
    PERFORM core_funding.headroom_v1(account,cur,seed.cell,seed.spent_atoms+CASE WHEN actual_spent<99999999999999999999999999999999999999 THEN 1 ELSE 0 END,seed.revision+1);
    slots=slots-1;
   END LOOP;
  END IF;
  SELECT * INTO STRICT c FROM core_funding.balance_cells WHERE account_id=account AND currency=cur AND cell=preferred FOR NO KEY UPDATE NOWAIT;
  IF c.balance_atoms-c.reserved_atoms<amount THEN
   PERFORM core_funding.collect_v1(account,cur,amount-(c.balance_atoms-c.reserved_atoms),preferred);
   SELECT balance_atoms INTO STRICT root FROM public.upstream_account_balances WHERE account_id=account AND currency=cur;
   budget_grant=least(root,greatest(amount-(c.balance_atoms-c.reserved_atoms),div(free,16)));
   IF budget_grant>0 THEN PERFORM core_funding.transfer_v1(account,cur,preferred,budget_grant,0,0,'reserve:'||attempt);END IF;
  END IF;
  PERFORM core_funding.headroom_v1(account,cur,preferred,c.spent_atoms,c.revision+1);
  SELECT * INTO STRICT c FROM core_funding.balance_cells WHERE account_id=account AND currency=cur AND cell=preferred;
 END IF;
 target=c.cell;
 IF c.balance_atoms-c.reserved_atoms<amount OR c.revision>=c.revision_limit THEN RAISE EXCEPTION 'funding_allocation_inconsistent' USING ERRCODE='P0500';END IF;
 UPDATE core_funding.balance_cells SET reserved_atoms=reserved_atoms+amount,revision=revision+1 WHERE account_id=account AND currency=cur AND cell=target;
 RETURN target;
END $$;
