CREATE FUNCTION core_funding.cost_v1(price uuid,usage jsonb) RETURNS numeric LANGUAGE plpgsql STABLE SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE p core_funding.cost_prices%ROWTYPE;BEGIN
 SELECT * INTO p FROM core_funding.cost_prices WHERE price_id=price;
 IF NOT FOUND THEN RAISE EXCEPTION 'supplier_price_missing' USING ERRCODE='P0422';END IF;
 RETURN core_funding.price_v1(usage,p.dimensions,p.weights,p.denominator);
END $$;

CREATE FUNCTION core_funding.collect_v1(account uuid,cur text,needed numeric,exclude_cell integer DEFAULT NULL) RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE root numeric;c record;take numeric;BEGIN
 IF NOT core_funding.held_v1(account,true) THEN RAISE EXCEPTION 'funding_outer_transaction_required' USING ERRCODE='P0F01';END IF;
 IF needed IS NULL OR needed<0 OR needed>99999999999999999999999999999999999999 OR needed<>trunc(needed) THEN RAISE EXCEPTION 'funding_budget_invalid' USING ERRCODE='P0400';END IF;
 SELECT balance_atoms INTO STRICT root FROM public.upstream_account_balances WHERE account_id=account AND currency=cur FOR NO KEY UPDATE NOWAIT;
 FOR c IN SELECT cell,balance_atoms-reserved_atoms free FROM core_funding.balance_cells WHERE account_id=account AND currency=cur AND balance_atoms>reserved_atoms AND (exclude_cell IS NULL OR cell<>exclude_cell) ORDER BY cell FOR NO KEY UPDATE NOWAIT LOOP
  EXIT WHEN root>=needed;
  take=least(c.free,needed-root,99999999999999999999999999999999999999-root);
  IF take>0 THEN PERFORM core_funding.transfer_v1(account,cur,c.cell,-take,0,0,'collect:'||gen_random_uuid());root=root+take;END IF;
 END LOOP;
END $$;

CREATE FUNCTION core_funding.normalize_v1(account uuid,cur text) RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE c record;root numeric;deficit numeric;take numeric;BEGIN
 IF NOT core_funding.held_v1(account,true) THEN RAISE EXCEPTION 'funding_outer_transaction_required' USING ERRCODE='P0F01';END IF;
 PERFORM core_funding.collect_v1(account,cur,0);
 FOR c IN SELECT cell,reserved_atoms-balance_atoms deficit FROM core_funding.balance_cells WHERE account_id=account AND currency=cur AND balance_atoms<reserved_atoms ORDER BY cell FOR NO KEY UPDATE NOWAIT LOOP
  deficit=c.deficit;
  WHILE deficit>0 LOOP
   PERFORM core_funding.collect_v1(account,cur,least(deficit,99999999999999999999999999999999999999));
   SELECT balance_atoms INTO STRICT root FROM public.upstream_account_balances WHERE account_id=account AND currency=cur;
   EXIT WHEN root<=0;
   take=least(root,deficit);
   PERFORM core_funding.transfer_v1(account,cur,c.cell,take,0,0,'normalize:'||gen_random_uuid());
   deficit=deficit-take;
  END LOOP;
 END LOOP;
END $$;

CREATE FUNCTION core_funding.headroom_v1(account uuid,cur text,target integer,needed_spent numeric,needed_revision numeric) RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE r public.upstream_account_balances%ROWTYPE;t core_funding.balance_cells%ROWTYPE;c record;free_spent numeric;free_revision numeric;actual_spent numeric;actual_revision numeric;take_spent numeric;take_revision numeric;need_spent numeric;need_revision numeric;grant_spent numeric;grant_revision numeric;BEGIN
 IF NOT core_funding.held_v1(account,true) THEN RAISE EXCEPTION 'funding_outer_transaction_required' USING ERRCODE='P0F01';END IF;
 SELECT * INTO STRICT r FROM public.upstream_account_balances WHERE account_id=account AND currency=cur FOR NO KEY UPDATE NOWAIT;
 SELECT * INTO STRICT t FROM core_funding.balance_cells WHERE account_id=account AND currency=cur AND cell=target FOR NO KEY UPDATE NOWAIT;
 SELECT r.spent_atoms+coalesce(sum(spent_atoms),0),r.revision+coalesce(sum(revision),0),99999999999999999999999999999999999999-r.spent_atoms-coalesce(sum(spent_limit),0),99999999999999999999999999999999999999-r.revision-coalesce(sum(revision_limit),0)
 INTO actual_spent,actual_revision,free_spent,free_revision FROM core_funding.balance_cells WHERE account_id=account AND currency=cur;
 IF needed_spent<t.spent_atoms OR needed_revision<t.revision OR actual_spent+needed_spent-t.spent_atoms>99999999999999999999999999999999999999 OR actual_revision+needed_revision-t.revision>99999999999999999999999999999999999999 THEN RAISE EXCEPTION 'funding_counter_overflow' USING ERRCODE='22003';END IF;
 need_spent=greatest(needed_spent-t.spent_limit,0);need_revision=greatest(needed_revision-t.revision_limit,0);
 FOR c IN SELECT cell,spent_limit-spent_atoms unused_spent,revision_limit-revision unused_revision FROM core_funding.balance_cells WHERE account_id=account AND currency=cur AND cell<>target ORDER BY cell FOR NO KEY UPDATE NOWAIT LOOP
  EXIT WHEN free_spent>=need_spent AND free_revision>=need_revision;
  take_spent=least(c.unused_spent,greatest(need_spent-free_spent,0));take_revision=least(c.unused_revision,greatest(need_revision-free_revision,0));
  IF take_spent>0 OR take_revision>0 THEN PERFORM core_funding.transfer_v1(account,cur,c.cell,0,-take_spent,-take_revision,'headroom:return:'||gen_random_uuid());free_spent=free_spent+take_spent;free_revision=free_revision+take_revision;END IF;
 END LOOP;
 IF free_spent<need_spent OR free_revision<need_revision THEN RAISE EXCEPTION 'funding_counter_overflow' USING ERRCODE='22003';END IF;
 grant_spent=CASE WHEN need_spent>0 THEN greatest(need_spent,least(free_spent,div(99999999999999999999999999999999999999-actual_spent,32))) ELSE 0 END;
 grant_revision=CASE WHEN need_revision>0 THEN greatest(need_revision,least(free_revision,div(99999999999999999999999999999999999999-actual_revision,32))) ELSE 0 END;
 IF grant_spent>0 OR grant_revision>0 THEN PERFORM core_funding.transfer_v1(account,cur,target,0,grant_spent,grant_revision,'headroom:grant:'||gen_random_uuid());END IF;
END $$;

CREATE FUNCTION core_funding.reserve_v1(account uuid,cur text,attempt uuid,amount numeric) RETURNS smallint LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE target smallint;preferred integer=get_byte(uuid_send(attempt),15)%16;c core_funding.balance_cells%ROWTYPE;free numeric;root numeric;budget_grant numeric;seed record;slots integer=16;actual_spent numeric;BEGIN
 IF NOT core_funding.held_v1(account) THEN RAISE EXCEPTION 'funding_fence_required' USING ERRCODE='P0403';END IF;
 IF amount IS NULL OR amount<0 OR amount<>trunc(amount) OR amount>99999999999999999999999999999999999999 THEN RAISE EXCEPTION 'funding_budget_invalid' USING ERRCODE='P0400';END IF;
 IF NOT EXISTS(SELECT 1 FROM public.upstream_accounts WHERE id=account AND state='ACTIVE' AND mode='PREPAID') THEN RAISE EXCEPTION 'upstream_funds_unavailable' USING ERRCODE='P0402';END IF;
 SELECT * INTO c FROM core_funding.balance_cells WHERE account_id=account AND currency=cur AND balance_atoms-reserved_atoms>=amount AND revision<revision_limit
 ORDER BY (cell-preferred+16)%16 LIMIT 1 FOR NO KEY UPDATE SKIP LOCKED;
 IF NOT FOUND THEN
  IF NOT core_funding.held_v1(account,true) THEN
   IF EXISTS(SELECT 1 FROM core_funding.balance_cells WHERE account_id=account AND currency=cur AND balance_atoms-reserved_atoms>=amount AND revision<revision_limit) THEN RAISE EXCEPTION 'core_busy' USING ERRCODE='P0503';END IF;
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

CREATE FUNCTION core_funding.apply_v1(attempt uuid,cost_delta numeric,new_reserve numeric,advance boolean) RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE h public.upstream_holds%ROWTYPE;c core_funding.balance_cells%ROWTYPE;BEGIN
 SELECT * INTO STRICT h FROM public.upstream_holds WHERE attempt_id=attempt FOR NO KEY UPDATE NOWAIT;
 IF NOT core_funding.held_v1(h.account_id) THEN RAISE EXCEPTION 'funding_fence_required' USING ERRCODE='P0403';END IF;
 IF cost_delta IS NULL OR new_reserve IS NULL OR advance IS NULL OR cost_delta<0 OR new_reserve<0 OR cost_delta<>trunc(cost_delta) OR new_reserve<>trunc(new_reserve) THEN RAISE EXCEPTION 'funding_delta_invalid' USING ERRCODE='P0400';END IF;
 SELECT * INTO STRICT c FROM core_funding.balance_cells WHERE account_id=h.account_id AND currency=h.currency AND cell=h.funding_cell FOR NO KEY UPDATE NOWAIT;
 IF c.spent_atoms+cost_delta>c.spent_limit OR c.revision+advance::integer>c.revision_limit OR c.balance_atoms-cost_delta<c.reserved_atoms+new_reserve-h.current_reserved_atoms OR NOT EXISTS(SELECT 1 FROM public.upstream_accounts WHERE id=h.account_id AND state='ACTIVE') THEN
  IF NOT core_funding.held_v1(h.account_id,true) THEN RAISE EXCEPTION 'funding_outer_transaction_required' USING ERRCODE='P0F01';END IF;
  PERFORM core_funding.headroom_v1(h.account_id,h.currency,h.funding_cell,c.spent_atoms+cost_delta,c.revision+advance::integer);
 END IF;
 UPDATE core_funding.balance_cells SET balance_atoms=balance_atoms-cost_delta,spent_atoms=spent_atoms+cost_delta,reserved_atoms=reserved_atoms+new_reserve-h.current_reserved_atoms,revision=revision+advance::integer WHERE account_id=h.account_id AND currency=h.currency AND cell=h.funding_cell;
 UPDATE public.upstream_holds SET current_reserved_atoms=new_reserve WHERE attempt_id=attempt;
END $$;

CREATE FUNCTION public.core_funding_control_v1(account uuid,catalog boolean DEFAULT true) RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
 IF account IS NULL OR catalog IS NULL THEN RAISE EXCEPTION 'funding_accounts_invalid' USING ERRCODE='P0400';END IF;
 PERFORM core_funding.exclusive_v1(ARRAY[account]);
 IF catalog THEN PERFORM public.core_catalog_write_v1();END IF;
END $$;
REVOKE ALL ON FUNCTION public.core_funding_control_v1(uuid,boolean) FROM PUBLIC;

CREATE FUNCTION core_funding.offset_debt_v1(account uuid,cur text,needed numeric) RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE root numeric;c record;take numeric;BEGIN
 IF NOT core_funding.held_v1(account,true) OR EXISTS(SELECT 1 FROM public.upstream_accounts WHERE id=account AND state='ACTIVE') THEN RAISE EXCEPTION 'funding_outer_transaction_required' USING ERRCODE='P0F01';END IF;
 SELECT balance_atoms INTO STRICT root FROM public.upstream_account_balances WHERE account_id=account AND currency=cur FOR NO KEY UPDATE NOWAIT;
 IF needed IS NULL OR needed<=0 OR needed>99999999999999999999999999999999999999 OR needed<>trunc(needed) OR root>=0 OR needed> -root THEN RAISE EXCEPTION 'funding_debt_offset_invalid' USING ERRCODE='P0400';END IF;
 FOR c IN SELECT cell,balance_atoms FROM core_funding.balance_cells WHERE account_id=account AND currency=cur AND balance_atoms>0 ORDER BY cell FOR NO KEY UPDATE NOWAIT LOOP
  EXIT WHEN needed=0;
  take=least(c.balance_atoms,needed);
  UPDATE public.upstream_account_balances SET balance_atoms=balance_atoms+take WHERE account_id=account AND currency=cur;
  UPDATE core_funding.balance_cells SET balance_atoms=balance_atoms-take WHERE account_id=account AND currency=cur AND cell=c.cell;
  INSERT INTO core_funding.budget_transfers(id,account_id,currency,cell,kind,reference,balance_delta) VALUES(gen_random_uuid(),account,cur,c.cell,'REBASE','debt-offset:'||gen_random_uuid(),-take);
  needed=needed-take;
 END LOOP;
 IF needed<>0 THEN RAISE EXCEPTION 'funding_debt_offset_inconsistent' USING ERRCODE='P0500';END IF;
END $$;

CREATE FUNCTION public.core_upstream_fund_v1(account uuid,cur text,delta numeric,adjustment boolean) RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE root public.upstream_account_balances%ROWTYPE;b record;c record;needed numeric;take numeric;BEGIN
 IF NOT core_funding.held_v1(account,true) THEN RAISE EXCEPTION 'funding_outer_transaction_required' USING ERRCODE='P0F01';END IF;
 IF delta IS NULL OR adjustment IS NULL OR delta<>trunc(delta) OR abs(delta)>99999999999999999999999999999999999999 OR NOT adjustment AND delta<=0 THEN RAISE EXCEPTION 'funding_delta_invalid' USING ERRCODE='P0400';END IF;
 INSERT INTO public.upstream_account_balances(account_id,currency) VALUES(account,cur) ON CONFLICT DO NOTHING;
 SELECT * INTO STRICT b FROM public.core_upstream_balances_v1 WHERE account_id=account AND currency=cur;
 IF abs(b.balance_atoms+delta)>99999999999999999999999999999999999999 OR b.revision>=99999999999999999999999999999999999999 OR adjustment AND abs(b.adjustment_atoms+delta)>99999999999999999999999999999999999999 THEN RAISE EXCEPTION 'funding_counter_overflow' USING ERRCODE='22003';END IF;
 IF delta>0 THEN PERFORM core_funding.normalize_v1(account,cur);
 ELSE PERFORM core_funding.collect_v1(account,cur,least(-delta,greatest(b.balance_atoms-b.reserved_atoms,0)));END IF;
 SELECT * INTO STRICT root FROM public.upstream_account_balances WHERE account_id=account AND currency=cur FOR NO KEY UPDATE NOWAIT;
 IF root.balance_atoms+delta< -99999999999999999999999999999999999999 THEN PERFORM core_funding.offset_debt_v1(account,cur,-99999999999999999999999999999999999999-root.balance_atoms-delta);END IF;
 SELECT greatest(root.revision+1+coalesce(sum(revision_limit),0)-99999999999999999999999999999999999999,0) INTO needed FROM core_funding.balance_cells WHERE account_id=account AND currency=cur;
 FOR c IN SELECT cell,revision_limit-revision unused FROM core_funding.balance_cells WHERE account_id=account AND currency=cur ORDER BY cell FOR NO KEY UPDATE NOWAIT LOOP
  EXIT WHEN needed=0;take=least(c.unused,needed);
  IF take>0 THEN PERFORM core_funding.transfer_v1(account,cur,c.cell,0,0,-take,'root:revision:'||gen_random_uuid());needed=needed-take;END IF;
 END LOOP;
 IF needed<>0 THEN RAISE EXCEPTION 'funding_counter_overflow' USING ERRCODE='22003';END IF;
 UPDATE public.upstream_account_balances SET balance_atoms=balance_atoms+delta,adjustment_atoms=adjustment_atoms+CASE WHEN adjustment THEN delta ELSE 0 END,revision=revision+1 WHERE account_id=account AND currency=cur;
END $$;
REVOKE ALL ON FUNCTION public.core_upstream_fund_v1(uuid,text,numeric,boolean) FROM PUBLIC;

CREATE FUNCTION public.core_upstream_normalize_v1(account uuid) RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE cur text;BEGIN
 IF NOT core_funding.held_v1(account,true) THEN RAISE EXCEPTION 'funding_outer_transaction_required' USING ERRCODE='P0F01';END IF;
 FOR cur IN SELECT currency FROM public.upstream_account_balances WHERE account_id=account ORDER BY currency LOOP PERFORM core_funding.normalize_v1(account,cur);END LOOP;
END $$;
REVOKE ALL ON FUNCTION public.core_upstream_normalize_v1(uuid) FROM PUBLIC;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA core_funding FROM PUBLIC;
