CREATE FUNCTION public.core_reconcile_holds_v1(batch integer) RETURNS integer LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE token uuid=gen_random_uuid();requests uuid[];result jsonb;BEGIN
 IF batch IS NULL OR batch NOT BETWEEN 1 AND 256 THEN RAISE EXCEPTION 'reconcile_batch_invalid' USING ERRCODE='P0422';END IF;
 WITH candidates AS (
  SELECT h.id,h.next_reconcile_at,1 AS priority FROM public.billing_holds h
  WHERE h.state IN ('HELD','RECONCILING') AND h.next_reconcile_at<=now() AND (h.reconcile_until IS NULL OR h.reconcile_until<now())
  AND (EXISTS(SELECT 1 FROM public.attempts a WHERE a.request_id=h.request_id AND a.evidence_id IS NOT NULL) OR EXISTS(SELECT 1 FROM public.billing_resolutions r WHERE r.request_id=h.request_id))
  UNION ALL
  SELECT h.id,h.next_reconcile_at,0 AS priority FROM public.billing_charges c JOIN public.billing_holds h ON h.id=c.hold_id
  WHERE c.debt_atoms>0 AND h.state NOT IN ('HELD','RECONCILING') AND h.next_reconcile_at<=now() AND (h.reconcile_until IS NULL OR h.reconcile_until<now())
 ),due AS (
  SELECT h.id FROM candidates c JOIN public.billing_holds h ON h.id=c.id WHERE h.reconcile_until IS NULL OR h.reconcile_until<now()
  ORDER BY c.priority,c.next_reconcile_at,h.id LIMIT batch FOR UPDATE OF h SKIP LOCKED
 ),leased AS (
  UPDATE public.billing_holds h SET reconcile_token=token,reconcile_until=now()+interval '30 seconds' FROM due WHERE h.id=due.id RETURNING h.request_id
 ) SELECT array_agg(request_id) INTO requests FROM leased;
 IF requests IS NULL THEN RETURN 0;END IF;
 result=public.settle_requests(requests,token);
 RETURN (SELECT count(*)::integer FROM jsonb_each(result) e WHERE e.value->>'state' IS DISTINCT FROM 'STALE_LEASE' AND e.value->>'state' IS DISTINCT FROM 'ERROR');
END $$;
REVOKE ALL ON FUNCTION public.core_reconcile_holds_v1(integer) FROM PUBLIC;
