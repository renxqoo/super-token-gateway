LOCK TABLE public.upstream_accounts,public.upstream_account_balances,public.upstream_holds,public.upstream_journals,public.upstream_entries,core_funding.balance_cells,core_funding.budget_transfers IN ACCESS EXCLUSIVE MODE;
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM core_funding.balance_cells) OR EXISTS(SELECT 1 FROM core_funding.budget_transfers) THEN RAISE EXCEPTION 'funding_cells_not_empty';END IF;
END $$;

CREATE TEMP TABLE funding_hold_history ON COMMIT DROP AS
 SELECT h.attempt_id,h.account_id,h.currency,get_byte(uuid_send(h.attempt_id),15)%16 cell,
  coalesce(sum(e.atoms) FILTER(WHERE e.account='upstream:reserved'),0) reserved,
  coalesce(sum(e.atoms) FILTER(WHERE e.account='upstream:expense'),0) spent
 FROM public.upstream_holds h LEFT JOIN public.upstream_journals j ON j.attempt_id=h.attempt_id
 LEFT JOIN public.upstream_entries e ON e.journal_id=j.id
 GROUP BY h.attempt_id,h.account_id,h.currency;
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM funding_hold_history WHERE reserved<0 OR spent<0 OR greatest(reserved,spent)>99999999999999999999999999999999999999)
  OR EXISTS(SELECT 1 FROM funding_hold_history x JOIN public.upstream_holds h ON h.attempt_id=x.attempt_id WHERE h.state='RELEASED' AND (x.reserved<>0 OR x.spent<>0))
  OR EXISTS(SELECT 1 FROM public.upstream_account_balances b LEFT JOIN LATERAL(SELECT coalesce(sum(x.reserved),0) reserved,coalesce(sum(x.spent),0) spent FROM funding_hold_history x WHERE x.account_id=b.account_id AND x.currency=b.currency)x ON true WHERE x.reserved<>b.reserved_atoms OR x.spent>b.spent_atoms)
  OR EXISTS(SELECT 1 FROM public.upstream_account_balances b JOIN public.upstream_accounts a ON a.id=b.account_id WHERE a.state='ACTIVE' AND b.balance_atoms<b.reserved_atoms) THEN RAISE EXCEPTION 'funding_history_inconsistent';END IF;
END $$;

DROP TRIGGER funding_cell_guard ON core_funding.balance_cells;
DROP TRIGGER funding_cell_coverage ON core_funding.balance_cells;
CREATE TEMP TABLE funding_balance_history ON COMMIT DROP AS SELECT b.*,a.state,b.balance_atoms>=b.reserved_atoms backed FROM public.upstream_account_balances b JOIN public.upstream_accounts a ON a.id=b.account_id;
UPDATE public.upstream_account_balances b SET balance_atoms=CASE WHEN x.backed THEN x.balance_atoms-x.reserved_atoms ELSE x.balance_atoms END,reserved_atoms=0,
 spent_atoms=x.spent_atoms-coalesce((SELECT sum(h.spent) FROM funding_hold_history h WHERE h.account_id=b.account_id AND h.currency=b.currency),0)
 FROM funding_balance_history x WHERE (b.account_id,b.currency)=(x.account_id,x.currency);
INSERT INTO core_funding.balance_cells(account_id,currency,cell,balance_atoms,reserved_atoms,spent_atoms,spent_limit,revision_limit)
 SELECT b.account_id,b.currency,c.cell,CASE WHEN b.backed THEN coalesce(h.reserved,0) ELSE 0 END,coalesce(h.reserved,0),coalesce(h.spent,0),coalesce(h.spent,0),0
 FROM funding_balance_history b CROSS JOIN generate_series(0,15) c(cell)
 LEFT JOIN LATERAL(SELECT sum(h.reserved) reserved,sum(h.spent) spent FROM funding_hold_history h WHERE h.account_id=b.account_id AND h.currency=b.currency AND h.cell=c.cell)h ON true;
INSERT INTO core_funding.budget_transfers(id,account_id,currency,cell,kind,reference,balance_delta,spent_allocation_atoms,spent_limit_delta)
 SELECT gen_random_uuid(),account_id,currency,cell,'MIGRATION','migration:2026111804:'||cell,balance_atoms,spent_atoms,spent_limit FROM core_funding.balance_cells;
DROP TRIGGER core_hold_guard ON public.upstream_holds;
UPDATE public.upstream_holds h SET funding_cell=x.cell,current_reserved_atoms=x.reserved FROM funding_hold_history x WHERE x.attempt_id=h.attempt_id;
ALTER TABLE public.upstream_holds ALTER COLUMN funding_cell SET NOT NULL,ALTER COLUMN current_reserved_atoms SET NOT NULL;

CREATE VIEW public.core_upstream_balances_v1 AS
 SELECT b.account_id,b.currency,(b.balance_atoms+coalesce(c.balance,0))::numeric(38,0) balance_atoms,
  coalesce(c.reserved,0)::numeric(38,0) reserved_atoms,(b.spent_atoms+coalesce(c.spent,0))::numeric(38,0) spent_atoms,
  b.adjustment_atoms,(b.revision+coalesce(c.revision,0))::numeric(38,0) revision
 FROM public.upstream_account_balances b LEFT JOIN LATERAL(
  SELECT sum(c.balance_atoms) balance,sum(c.reserved_atoms) reserved,sum(c.spent_atoms) spent,sum(c.revision) revision
  FROM core_funding.balance_cells c WHERE c.account_id=b.account_id AND c.currency=b.currency
 ) c ON true;
REVOKE ALL ON public.core_upstream_balances_v1 FROM PUBLIC;

CREATE FUNCTION core_funding.catalog_held_v1() RETURNS boolean LANGUAGE sql STABLE SET search_path=pg_catalog,public,pg_temp AS $$
 SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_locks l WHERE l.locktype='advisory' AND l.pid=pg_backend_pid() AND l.database=(SELECT oid FROM pg_catalog.pg_database WHERE datname=current_database())
  AND l.classid::bigint=((hashtextextended('stg.catalog.control.v1',0)>>32)&4294967295) AND l.objid::bigint=(hashtextextended('stg.catalog.control.v1',0)&4294967295) AND l.objsubid=1 AND l.granted AND l.mode='ExclusiveLock')
$$;

CREATE FUNCTION core_funding.guard_root_v1() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
 IF TG_OP='DELETE' THEN RAISE EXCEPTION 'funding_root_immutable' USING ERRCODE='P0403';END IF;
 IF TG_OP='UPDATE' AND ((NEW.account_id,NEW.currency) IS DISTINCT FROM (OLD.account_id,OLD.currency) OR NEW.spent_atoms<OLD.spent_atoms OR NEW.revision<OLD.revision) THEN RAISE EXCEPTION 'funding_root_immutable' USING ERRCODE='P0403';END IF;
 IF NEW.reserved_atoms<>0 THEN RAISE EXCEPTION 'funding_root_reservation_forbidden' USING ERRCODE='P0403';END IF;
 IF NOT core_funding.held_v1(NEW.account_id,true) AND NOT(TG_OP='INSERT' AND (NEW.balance_atoms,NEW.reserved_atoms,NEW.spent_atoms,NEW.adjustment_atoms,NEW.revision)=(0,0,0,0,0)) THEN RAISE EXCEPTION 'funding_outer_transaction_required' USING ERRCODE='P0F01';END IF;
 RETURN NEW;
END $$;

CREATE FUNCTION core_funding.check_root_v1() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE b record;BEGIN
 SELECT r.balance_atoms+coalesce(c.balance,0) balance,r.spent_atoms+coalesce(c.spent_limit,0) spent_limit,r.revision+coalesce(c.revision_limit,0) revision_limit,coalesce(c.reserved,0) reserved
 INTO b FROM public.upstream_account_balances r LEFT JOIN LATERAL(
  SELECT sum(c.balance_atoms) balance,sum(c.reserved_atoms) reserved,sum(c.spent_limit) spent_limit,sum(c.revision_limit) revision_limit FROM core_funding.balance_cells c WHERE c.account_id=r.account_id AND c.currency=r.currency
 )c ON true WHERE r.account_id=NEW.account_id AND r.currency=NEW.currency;
 IF abs(b.balance)>99999999999999999999999999999999999999 OR greatest(b.spent_limit,b.revision_limit,b.reserved)>99999999999999999999999999999999999999 THEN RAISE EXCEPTION 'funding_counter_overflow' USING ERRCODE='22003';END IF;
 PERFORM core_funding.check_account_v1(NEW.account_id);
 RETURN NULL;
END $$;

CREATE FUNCTION core_funding.check_account_v1(account uuid) RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
 IF EXISTS(SELECT 1 FROM public.upstream_accounts WHERE id=account AND state='ACTIVE') AND
  (EXISTS(SELECT 1 FROM public.upstream_account_balances WHERE account_id=account AND balance_atoms<0) OR EXISTS(SELECT 1 FROM core_funding.balance_cells WHERE account_id=account AND balance_atoms<reserved_atoms)) THEN RAISE EXCEPTION 'funding_coverage_invalid' USING ERRCODE='P0403';END IF;
END $$;

CREATE FUNCTION core_funding.guard_account_v1() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
 IF TG_OP='DELETE' OR (NEW.id,NEW.state,NEW.mode) IS DISTINCT FROM (OLD.id,OLD.state,OLD.mode) THEN
  IF TG_OP='DELETE' OR NEW.id IS DISTINCT FROM OLD.id THEN RAISE EXCEPTION 'funding_account_immutable' USING ERRCODE='P0403';END IF;
  IF NOT core_funding.held_v1(NEW.id,true) OR NOT core_funding.catalog_held_v1() THEN RAISE EXCEPTION 'funding_outer_transaction_required' USING ERRCODE='P0F01';END IF;
 END IF;
 RETURN NEW;
END $$;

CREATE FUNCTION core_funding.check_account_trigger_v1() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN PERFORM core_funding.check_account_v1(NEW.id);RETURN NULL;END $$;

CREATE OR REPLACE FUNCTION public.core_hold_guard_v1() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
 IF TG_OP='DELETE' OR TG_OP='UPDATE' AND (to_jsonb(NEW)-ARRAY['state','final_cost_atoms','current_reserved_atoms']) IS DISTINCT FROM (to_jsonb(OLD)-ARRAY['state','final_cost_atoms','current_reserved_atoms']) THEN RAISE EXCEPTION 'hold_snapshot_immutable' USING ERRCODE='P0403';END IF;
 IF (TG_OP='INSERT' OR NEW.current_reserved_atoms IS DISTINCT FROM OLD.current_reserved_atoms) AND NOT core_funding.held_v1(NEW.account_id) THEN RAISE EXCEPTION 'funding_fence_required' USING ERRCODE='P0403';END IF;
 RETURN NEW;
END $$;

CREATE TRIGGER core_hold_guard BEFORE INSERT OR UPDATE OR DELETE ON public.upstream_holds FOR EACH ROW EXECUTE FUNCTION public.core_hold_guard_v1();
CREATE TRIGGER funding_cell_guard BEFORE INSERT OR UPDATE OR DELETE ON core_funding.balance_cells FOR EACH ROW EXECUTE FUNCTION core_funding.guard_cell_v1();
CREATE CONSTRAINT TRIGGER funding_cell_coverage AFTER INSERT OR UPDATE ON core_funding.balance_cells DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION core_funding.check_coverage_v1();
CREATE TRIGGER funding_root_guard BEFORE INSERT OR UPDATE OR DELETE ON public.upstream_account_balances FOR EACH ROW EXECUTE FUNCTION core_funding.guard_root_v1();
CREATE CONSTRAINT TRIGGER funding_root_consistent AFTER INSERT OR UPDATE ON public.upstream_account_balances DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION core_funding.check_root_v1();
CREATE TRIGGER funding_account_guard BEFORE UPDATE OR DELETE ON public.upstream_accounts FOR EACH ROW EXECUTE FUNCTION core_funding.guard_account_v1();
CREATE CONSTRAINT TRIGGER funding_account_coverage AFTER UPDATE ON public.upstream_accounts DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION core_funding.check_account_trigger_v1();
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA core_funding FROM PUBLIC;
