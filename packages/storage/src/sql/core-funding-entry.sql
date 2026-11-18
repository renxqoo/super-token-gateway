CREATE FUNCTION public.core_execution_slow_v1(item jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE p jsonb=item->'projection';account uuid;current_account uuid;BEGIN
 PERFORM public.core_caller_v1(p);
 IF p->>'command_type' IN('execution_admit_v1','execution_next_v1') THEN SELECT account_id INTO account FROM public.channels WHERE id=(item->'data'->'dispatch_projection'->'candidate_ref'->>'channel_id')::uuid;
 ELSIF p->>'command_type' IN('execution_begin_v1','execution_abort_v1') THEN SELECT account_id INTO account FROM public.upstream_holds WHERE attempt_id=(item->'data'->>'attempt_id')::uuid;
 ELSE RAISE EXCEPTION 'internal_peer_forbidden' USING ERRCODE='P0403';END IF;
 IF account IS NULL THEN RETURN public.core_execution_batch_v1(jsonb_build_array(item))->0;END IF;
 PERFORM core_funding.exclusive_v1(ARRAY[account]);
 PERFORM public.core_catalog_write_v1();
 IF p->>'command_type' IN('execution_admit_v1','execution_next_v1') THEN
  SELECT account_id INTO current_account FROM public.channels WHERE id=(item->'data'->'dispatch_projection'->'candidate_ref'->>'channel_id')::uuid;
  IF current_account IS DISTINCT FROM account THEN RAISE EXCEPTION 'core_busy' USING ERRCODE='P0503';END IF;
 END IF;
 RETURN public.core_execution_batch_v1(jsonb_build_array(item))->0;
END $$;
REVOKE ALL ON FUNCTION public.core_execution_slow_v1(jsonb) FROM PUBLIC;

CREATE FUNCTION public.core_abort_recovery_slow_v1(p jsonb,e jsonb,d jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE account uuid;BEGIN
 PERFORM public.core_caller_v1(p);
 IF p->>'command_type' IS DISTINCT FROM 'execution_abort_recovery_v1' OR p->>'service' NOT IN('core-api','core-worker') THEN RAISE EXCEPTION 'internal_peer_forbidden' USING ERRCODE='P0403';END IF;
 SELECT account_id INTO account FROM public.upstream_holds WHERE attempt_id=(d->>'attempt_id')::uuid;
 IF account IS NULL THEN RETURN public.core_abort_recovery_v1(p,e,d);END IF;
 PERFORM core_funding.exclusive_v1(ARRAY[account]);
 PERFORM public.core_catalog_write_v1();
 RETURN public.core_abort_recovery_v1(p,e,d);
END $$;
REVOKE ALL ON FUNCTION public.core_abort_recovery_slow_v1(jsonb,jsonb,jsonb) FROM PUBLIC;

CREATE FUNCTION public.persist_core_evidence_slow_v1(g jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE accounts uuid[];BEGIN
 IF jsonb_typeof(g) IS DISTINCT FROM 'object' OR jsonb_typeof(g->'events') IS DISTINCT FROM 'array' OR jsonb_array_length(g->'events') NOT BETWEEN 1 AND 64 THEN RAISE EXCEPTION 'evidence_group_invalid' USING ERRCODE='P0400';END IF;
 SELECT ARRAY(SELECT DISTINCT h.account_id FROM public.upstream_holds h WHERE h.attempt_id IN(SELECT (e->>'attempt_id')::uuid FROM jsonb_array_elements(g->'events') e) ORDER BY h.account_id) INTO accounts;
 PERFORM core_funding.exclusive_v1(accounts);
 PERFORM public.core_catalog_write_v1();
 RETURN public.persist_core_evidence_groups_v1(jsonb_build_array(g))->0;
END $$;
REVOKE ALL ON FUNCTION public.persist_core_evidence_slow_v1(jsonb) FROM PUBLIC;

DROP FUNCTION public.core_supplier_exposure_v1(jsonb,jsonb,jsonb,text[],jsonb);

DO $$ DECLARE owner name;role name;column_name text;BEGIN
 FOR owner IN SELECT DISTINCT pg_get_userbyid(proowner) FROM pg_proc WHERE oid IN('public.core_execution_item_v2(jsonb,jsonb,jsonb,jsonb,jsonb)'::regprocedure,'public.persist_core_evidence_groups_v1(jsonb)'::regprocedure,'public.core_abort_recovery_v1(jsonb,jsonb,jsonb)'::regprocedure) LOOP
  EXECUTE format('GRANT USAGE ON SCHEMA core_funding TO %I',owner);
  EXECUTE format('GRANT SELECT,INSERT,UPDATE ON core_funding.balance_cells TO %I',owner);
  EXECUTE format('GRANT SELECT,INSERT ON core_funding.budget_transfers TO %I',owner);
  EXECUTE format('GRANT SELECT ON core_funding.cost_prices,core_funding.billing_prices TO %I',owner);
  EXECUTE format('GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA core_funding TO %I',owner);
 END LOOP;
 FOR role IN SELECT rolname FROM pg_roles LOOP
  IF has_function_privilege(role,'public.core_execution_batch_v1(jsonb)','EXECUTE') THEN EXECUTE format('GRANT EXECUTE ON FUNCTION public.core_execution_slow_v1(jsonb) TO %I',role);END IF;
  IF has_function_privilege(role,'public.core_abort_recovery_v1(jsonb,jsonb,jsonb)','EXECUTE') THEN EXECUTE format('GRANT EXECUTE ON FUNCTION public.core_abort_recovery_slow_v1(jsonb,jsonb,jsonb) TO %I',role);END IF;
  IF has_function_privilege(role,'public.persist_core_evidence_groups_v1(jsonb)','EXECUTE') THEN EXECUTE format('GRANT EXECUTE ON FUNCTION public.persist_core_evidence_slow_v1(jsonb) TO %I',role);END IF;
  IF has_table_privilege(role,'public.upstream_account_balances','SELECT') THEN EXECUTE format('GRANT SELECT ON public.core_upstream_balances_v1 TO %I',role);
  ELSE
   FOREACH column_name IN ARRAY ARRAY['account_id','currency','balance_atoms','reserved_atoms','spent_atoms','adjustment_atoms','revision'] LOOP
    IF has_column_privilege(role,'public.upstream_account_balances',column_name,'SELECT') THEN EXECUTE format('GRANT SELECT(%I) ON public.core_upstream_balances_v1 TO %I',column_name,role);END IF;
   END LOOP;
  END IF;
  IF has_table_privilege(role,'public.upstream_account_balances','SELECT') AND has_table_privilege(role,'public.upstream_account_balances','INSERT') AND has_table_privilege(role,'public.upstream_account_balances','UPDATE') THEN
   EXECUTE format('GRANT EXECUTE ON FUNCTION public.core_funding_control_v1(uuid,boolean),public.core_upstream_fund_v1(uuid,text,numeric,boolean),public.core_upstream_normalize_v1(uuid) TO %I',role);
  END IF;
 END LOOP;
 FOR role IN SELECT rolname FROM pg_roles WHERE rolname IS DISTINCT FROM pg_get_userbyid((SELECT relowner FROM pg_class WHERE oid='public.upstream_account_balances'::regclass)) AND has_table_privilege(rolname,'public.upstream_account_balances','SELECT') AND has_table_privilege(rolname,'public.upstream_account_balances','INSERT') AND has_table_privilege(rolname,'public.upstream_account_balances','UPDATE') AND has_function_privilege(rolname,'public.core_platform_authorize_v1(jsonb,text)','EXECUTE') LOOP
  EXECUTE format('REVOKE ALL ON public.upstream_account_balances FROM %I',role);
 END LOOP;
 IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='core_management_api') THEN REVOKE ALL ON public.upstream_account_balances FROM core_management_api;END IF;
END $$;

DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM public.core_upstream_balances_v1 b JOIN funding_balance_history h USING(account_id,currency) WHERE (b.balance_atoms,b.reserved_atoms,b.spent_atoms,b.revision,b.adjustment_atoms) IS DISTINCT FROM (h.balance_atoms,h.reserved_atoms,h.spent_atoms,h.revision,h.adjustment_atoms))
  OR EXISTS(SELECT 1 FROM core_funding.balance_cells c WHERE c.reserved_atoms IS DISTINCT FROM (SELECT coalesce(sum(h.current_reserved_atoms),0) FROM public.upstream_holds h WHERE h.account_id=c.account_id AND h.currency=c.currency AND h.funding_cell=c.cell)) THEN RAISE EXCEPTION 'funding_migration_conservation_failed';END IF;
END $$;
