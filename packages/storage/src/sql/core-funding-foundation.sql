REVOKE ALL ON SCHEMA core_funding FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA core_funding FROM PUBLIC;

CREATE FUNCTION core_funding.account_key_v1(account uuid) RETURNS bigint LANGUAGE sql IMMUTABLE SET search_path=pg_catalog,public,pg_temp AS $$
 SELECT hashtextextended('stg.funding.account.v1:'||account::text,0)
$$;

CREATE FUNCTION core_funding.held_v1(account uuid,exclusive boolean DEFAULT false) RETURNS boolean LANGUAGE sql STABLE SET search_path=pg_catalog,public,pg_temp AS $$
 SELECT EXISTS(
  SELECT 1 FROM pg_catalog.pg_locks l
  WHERE l.locktype='advisory' AND l.pid=pg_backend_pid() AND l.database=(SELECT oid FROM pg_catalog.pg_database WHERE datname=current_database())
   AND l.classid::bigint=((core_funding.account_key_v1(account)>>32)&4294967295)
   AND l.objid::bigint=(core_funding.account_key_v1(account)&4294967295) AND l.objsubid=1 AND l.granted
   AND (l.mode='ExclusiveLock' OR NOT exclusive AND l.mode='ShareLock')
 )
$$;

CREATE FUNCTION core_funding.share_v1(accounts uuid[]) RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE account uuid;BEGIN
 IF accounts IS NULL OR cardinality(accounts)>128 OR array_position(accounts,NULL) IS NOT NULL THEN RAISE EXCEPTION 'funding_accounts_invalid' USING ERRCODE='P0400';END IF;
 FOR account IN SELECT DISTINCT id FROM unnest(accounts) id ORDER BY id LOOP
  IF NOT pg_try_advisory_xact_lock_shared(core_funding.account_key_v1(account)) THEN RAISE EXCEPTION 'core_busy' USING ERRCODE='P0503';END IF;
 END LOOP;
END $$;

CREATE FUNCTION core_funding.exclusive_v1(accounts uuid[]) RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE account uuid;BEGIN
 IF accounts IS NULL OR cardinality(accounts)>128 OR array_position(accounts,NULL) IS NOT NULL THEN RAISE EXCEPTION 'funding_accounts_invalid' USING ERRCODE='P0400';END IF;
 FOR account IN SELECT DISTINCT id FROM unnest(accounts) id ORDER BY id LOOP
  IF core_funding.held_v1(account) AND NOT core_funding.held_v1(account,true) THEN RAISE EXCEPTION 'funding_outer_transaction_required' USING ERRCODE='P0F01';END IF;
  PERFORM pg_advisory_xact_lock(core_funding.account_key_v1(account));
 END LOOP;
END $$;

CREATE FUNCTION core_funding.guard_cell_v1() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE exclusive boolean;old_balance numeric=0;old_reserved numeric=0;old_spent_limit numeric=0;old_revision_limit numeric=0;total_balance numeric;total_reserved numeric;total_spent_limit numeric;total_revision_limit numeric;BEGIN
 IF TG_OP='DELETE' THEN RAISE EXCEPTION 'funding_cell_immutable' USING ERRCODE='P0403';END IF;
 exclusive=core_funding.held_v1(NEW.account_id,true);
 IF TG_OP='UPDATE' THEN
  IF (NEW.account_id,NEW.currency,NEW.cell) IS DISTINCT FROM (OLD.account_id,OLD.currency,OLD.cell) OR NEW.spent_atoms<OLD.spent_atoms OR NEW.revision<OLD.revision THEN RAISE EXCEPTION 'funding_cell_immutable' USING ERRCODE='P0403';END IF;
  old_balance=OLD.balance_atoms;old_reserved=OLD.reserved_atoms;old_spent_limit=OLD.spent_limit;old_revision_limit=OLD.revision_limit;
  IF NOT exclusive THEN
   IF NOT core_funding.held_v1(NEW.account_id) THEN RAISE EXCEPTION 'funding_fence_required' USING ERRCODE='P0403';END IF;
   IF NEW.spent_limit IS DISTINCT FROM OLD.spent_limit OR NEW.revision_limit IS DISTINCT FROM OLD.revision_limit OR NEW.balance_atoms-OLD.balance_atoms IS DISTINCT FROM -(NEW.spent_atoms-OLD.spent_atoms) THEN RAISE EXCEPTION 'funding_outer_transaction_required' USING ERRCODE='P0F01';END IF;
   IF NEW.balance_atoms<NEW.reserved_atoms OR NOT EXISTS(SELECT 1 FROM public.upstream_accounts WHERE id=NEW.account_id AND state='ACTIVE') THEN RAISE EXCEPTION 'funding_outer_transaction_required' USING ERRCODE='P0F01';END IF;
   RETURN NEW;
  END IF;
 ELSIF NOT exclusive THEN RAISE EXCEPTION 'funding_fence_required' USING ERRCODE='P0403';END IF;
 SELECT b.balance_atoms+coalesce((SELECT sum(c.balance_atoms) FROM core_funding.balance_cells c WHERE c.account_id=NEW.account_id AND c.currency=NEW.currency),0)+NEW.balance_atoms-old_balance,
  coalesce((SELECT sum(c.reserved_atoms) FROM core_funding.balance_cells c WHERE c.account_id=NEW.account_id AND c.currency=NEW.currency),0)+NEW.reserved_atoms-old_reserved,
  b.spent_atoms+coalesce((SELECT sum(c.spent_limit) FROM core_funding.balance_cells c WHERE c.account_id=NEW.account_id AND c.currency=NEW.currency),0)+NEW.spent_limit-old_spent_limit,
  b.revision+coalesce((SELECT sum(c.revision_limit) FROM core_funding.balance_cells c WHERE c.account_id=NEW.account_id AND c.currency=NEW.currency),0)+NEW.revision_limit-old_revision_limit
 INTO total_balance,total_reserved,total_spent_limit,total_revision_limit FROM public.upstream_account_balances b WHERE b.account_id=NEW.account_id AND b.currency=NEW.currency;
 IF total_balance IS NULL OR abs(total_balance)>99999999999999999999999999999999999999 OR total_reserved>99999999999999999999999999999999999999 OR total_spent_limit>99999999999999999999999999999999999999 OR total_revision_limit>99999999999999999999999999999999999999 THEN RAISE EXCEPTION 'funding_counter_overflow' USING ERRCODE='22003';END IF;
 RETURN NEW;
END $$;

CREATE FUNCTION core_funding.check_coverage_v1() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
 IF EXISTS(SELECT 1 FROM public.upstream_accounts a WHERE a.id=NEW.account_id AND a.state='ACTIVE')
  AND (EXISTS(SELECT 1 FROM public.upstream_account_balances b WHERE b.account_id=NEW.account_id AND b.balance_atoms<0)
   OR EXISTS(SELECT 1 FROM core_funding.balance_cells c WHERE c.account_id=NEW.account_id AND c.balance_atoms<c.reserved_atoms)) THEN RAISE EXCEPTION 'funding_coverage_invalid' USING ERRCODE='P0403';END IF;
 RETURN NULL;
END $$;

REVOKE ALL ON ALL FUNCTIONS IN SCHEMA core_funding FROM PUBLIC;
CREATE TRIGGER funding_cell_guard BEFORE INSERT OR UPDATE OR DELETE ON core_funding.balance_cells FOR EACH ROW EXECUTE FUNCTION core_funding.guard_cell_v1();
CREATE CONSTRAINT TRIGGER funding_cell_coverage AFTER INSERT OR UPDATE ON core_funding.balance_cells DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION core_funding.check_coverage_v1();
CREATE TRIGGER immutable_budget_transfers BEFORE UPDATE OR DELETE ON core_funding.budget_transfers FOR EACH ROW EXECUTE FUNCTION public.immutable_row();

CREATE FUNCTION core_funding.transfer_v1(account uuid,cur text,target_cell integer,balance_delta numeric,spent_delta numeric,revision_delta numeric,ref text) RETURNS uuid LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE root public.upstream_account_balances%ROWTYPE;c core_funding.balance_cells%ROWTYPE;prior core_funding.budget_transfers%ROWTYPE;transfer uuid;transfer_kind text;BEGIN
 IF account IS NULL OR cur IS NULL OR target_cell IS NULL OR target_cell NOT BETWEEN 0 AND 15 OR ref IS NULL OR length(ref) NOT BETWEEN 1 AND 200
  OR balance_delta IS NULL OR spent_delta IS NULL OR revision_delta IS NULL
  OR EXISTS(SELECT 1 FROM unnest(ARRAY[balance_delta,spent_delta,revision_delta]) d WHERE abs(d)>99999999999999999999999999999999999999 OR d<>trunc(d))
  OR (balance_delta,spent_delta,revision_delta)=(0,0,0) THEN RAISE EXCEPTION 'funding_transfer_invalid' USING ERRCODE='P0400';END IF;
 IF NOT core_funding.held_v1(account,true) THEN RAISE EXCEPTION 'funding_outer_transaction_required' USING ERRCODE='P0F01';END IF;
 transfer_kind=CASE WHEN balance_delta>0 THEN 'GRANT' WHEN balance_delta<0 THEN 'RECLAIM' ELSE 'HEADROOM' END;
 SELECT * INTO prior FROM core_funding.budget_transfers t WHERE t.account_id=account AND t.currency=cur AND t.reference=ref;
 IF FOUND THEN
  IF (prior.cell,prior.kind,prior.balance_delta,prior.spent_allocation_atoms,prior.spent_limit_delta,prior.revision_limit_delta) IS DISTINCT FROM (target_cell,transfer_kind,balance_delta,0::numeric,spent_delta,revision_delta) THEN RAISE EXCEPTION 'funding_transfer_conflict' USING ERRCODE='P0409';END IF;
  RETURN prior.id;
 END IF;
 PERFORM 1 FROM public.upstream_accounts WHERE id=account FOR SHARE NOWAIT;
 SELECT * INTO root FROM public.upstream_account_balances b WHERE b.account_id=account AND b.currency=cur FOR NO KEY UPDATE NOWAIT;
 IF NOT FOUND THEN RAISE EXCEPTION 'funding_balance_missing' USING ERRCODE='P0404';END IF;
 IF root.reserved_atoms<>0 THEN RAISE EXCEPTION 'funding_root_not_migrated' USING ERRCODE='P0422';END IF;
 INSERT INTO core_funding.balance_cells(account_id,currency,cell,spent_limit,revision_limit) VALUES(account,cur,target_cell,0,0) ON CONFLICT DO NOTHING;
 SELECT * INTO c FROM core_funding.balance_cells b WHERE b.account_id=account AND b.currency=cur AND b.cell=target_cell FOR NO KEY UPDATE NOWAIT;
 IF balance_delta>0 AND balance_delta>root.balance_atoms OR balance_delta<0 AND -balance_delta>greatest(c.balance_atoms-c.reserved_atoms,0) THEN RAISE EXCEPTION 'funding_budget_unavailable' USING ERRCODE='P0422';END IF;
 IF c.spent_limit+spent_delta<c.spent_atoms OR c.revision_limit+revision_delta<c.revision THEN RAISE EXCEPTION 'funding_headroom_unavailable' USING ERRCODE='P0422';END IF;
 IF EXISTS(SELECT 1 FROM unnest(ARRAY[root.balance_atoms-balance_delta,c.balance_atoms+balance_delta,c.spent_limit+spent_delta,c.revision_limit+revision_delta]) d WHERE abs(d)>99999999999999999999999999999999999999) THEN RAISE EXCEPTION 'funding_counter_overflow' USING ERRCODE='22003';END IF;
 UPDATE public.upstream_account_balances SET balance_atoms=balance_atoms-balance_delta WHERE account_id=account AND currency=cur;
 UPDATE core_funding.balance_cells SET balance_atoms=balance_atoms+balance_delta,spent_limit=spent_limit+spent_delta,revision_limit=revision_limit+revision_delta WHERE account_id=account AND currency=cur AND cell=target_cell;
 transfer=gen_random_uuid();
 INSERT INTO core_funding.budget_transfers(id,account_id,currency,cell,kind,reference,balance_delta,spent_limit_delta,revision_limit_delta) VALUES(transfer,account,cur,target_cell,transfer_kind,ref,balance_delta,spent_delta,revision_delta);
 RETURN transfer;
END $$;

CREATE FUNCTION core_funding.audit_v1(account uuid,cur text) RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE result jsonb;BEGIN
 WITH cells AS (
  SELECT c.*,coalesce(t.balance,0) budget,coalesce(t.spent_allocation,0) history,coalesce(t.spent_limit,0) allocated_spent_limit,coalesce(t.revision_limit,0) allocated_revision_limit
  FROM core_funding.balance_cells c LEFT JOIN LATERAL (
   SELECT sum(t.balance_delta) balance,sum(t.spent_allocation_atoms) spent_allocation,sum(t.spent_limit_delta) spent_limit,sum(t.revision_limit_delta) revision_limit
   FROM core_funding.budget_transfers t WHERE t.account_id=c.account_id AND t.currency=c.currency AND t.cell=c.cell
  ) t ON true WHERE c.account_id=account AND c.currency=cur
 ), totals AS (
  SELECT coalesce(sum(balance_atoms),0) balance,coalesce(sum(reserved_atoms),0) reserved,coalesce(sum(spent_atoms),0) spent,coalesce(sum(revision),0) revision,
   coalesce(bool_and(balance_atoms+spent_atoms=budget+history AND spent_limit=allocated_spent_limit AND revision_limit=allocated_revision_limit),true) conserved
  FROM cells
 ) SELECT jsonb_build_object('balance_atoms',(b.balance_atoms+t.balance)::text,'reserved_atoms',t.reserved::text,'spent_atoms',(b.spent_atoms+t.spent)::text,'revision',(b.revision+t.revision)::text,
  'available_atoms',(b.balance_atoms+t.balance-t.reserved)::text,'adjustment_atoms',b.adjustment_atoms::text,'root_reserved_atoms',b.reserved_atoms::text,'cells_conserved',t.conserved)
 INTO result FROM public.upstream_account_balances b CROSS JOIN totals t WHERE b.account_id=account AND b.currency=cur;
 IF result->>'root_reserved_atoms'<>'0' THEN RAISE EXCEPTION 'funding_root_not_migrated' USING ERRCODE='P0422';END IF;
 RETURN result;
END $$;

REVOKE ALL ON ALL FUNCTIONS IN SCHEMA core_funding FROM PUBLIC;
