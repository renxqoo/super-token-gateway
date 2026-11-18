CREATE OR REPLACE FUNCTION public.core_preflight_v1(p jsonb,context_id uuid,attempt_id uuid) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE ctx public.execution_contexts%ROWTYPE;permit public.execution_permits%ROWTYPE;channel public.channels%ROWTYPE;auth jsonb;leases jsonb='[]';requested_request uuid;execution_closed boolean;BEGIN
 PERFORM public.core_caller_v1(p);
 IF attempt_id IS NOT NULL THEN
  EXECUTE 'SELECT * FROM public.execution_permits ep WHERE ep.attempt_id=$1' INTO permit USING core_preflight_v1.attempt_id;
  IF permit.owner_session_id IS DISTINCT FROM (p->>'session_id')::uuid OR permit.incarnation_id IS DISTINCT FROM (p->>'incarnation_id')::uuid OR permit.owner_key_sha256 IS DISTINCT FROM p->>'key_sha256' OR permit.gateway_instance_id IS DISTINCT FROM p->>'instance_id' OR permit.owner_service IS DISTINCT FROM p->>'service' OR permit.certificate_sha256 IS DISTINCT FROM p->>'certificate_sha256' THEN RAISE EXCEPTION 'internal_owner_mismatch' USING ERRCODE='P0403';END IF;
  context_id=permit.context_id;
 END IF;
 EXECUTE 'SELECT * FROM public.execution_contexts ec WHERE ec.id=$1' INTO ctx USING context_id;
 IF ctx.owner_session_id IS DISTINCT FROM (p->>'session_id')::uuid THEN RAISE EXCEPTION 'context_invalid' USING ERRCODE='P0403';END IF;
 IF p->>'command_type'<>'execution_quota_v1' THEN auth=public.core_auth_v1(ctx.key_id,ctx.ip,ctx.operation,ctx.model);END IF;
 IF attempt_id IS NOT NULL THEN
  EXECUTE 'SELECT request_id FROM public.attempts WHERE id=$1' INTO requested_request USING core_preflight_v1.attempt_id;
  EXECUTE 'SELECT coalesce(jsonb_agg(jsonb_build_object(''state'',l.state,''attempt_id'',l.attempt_id,''permit_epoch'',ep.permit_epoch::text,''descriptor'',l.descriptor) ORDER BY l.lease_id),''[]''::jsonb)
   FROM public.execution_quota_leases l JOIN LATERAL(SELECT ep.permit_epoch FROM public.execution_permits ep WHERE ep.attempt_id=l.attempt_id OFFSET 0) ep ON true WHERE l.request_id=$1' INTO leases USING requested_request;
  EXECUTE 'SELECT execution_closed_at IS NOT NULL FROM public.gateway_requests WHERE id=$1' INTO execution_closed USING requested_request;
 END IF;
 RETURN ctx.snapshot||jsonb_build_object('leases',leases,'permit_state',permit.state,'permit_epoch',permit.permit_epoch::text,'execution_closed',execution_closed);
END $$;
