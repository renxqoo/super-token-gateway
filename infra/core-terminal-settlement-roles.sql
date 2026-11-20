DO $$ BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='core_terminal_writer') THEN
  CREATE ROLE core_terminal_writer NOLOGIN;
 END IF;
 IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='core_terminal_confirmation') THEN
  CREATE ROLE core_terminal_confirmation NOLOGIN;
 END IF;
END $$;
GRANT USAGE ON SCHEMA public TO core_terminal_writer;
GRANT EXECUTE ON FUNCTION public.core_short_chat_funding_v1(smallint,bigint) TO core_terminal_writer;
GRANT SELECT(id,project_id) ON public.gateway_requests TO core_terminal_writer;
GRANT SELECT(id,request_id) ON public.attempts TO core_terminal_writer;
GRANT SELECT(session_id,public_key_spki,public_key_sha256)
 ON public.internal_startup_sessions TO core_terminal_writer;
GRANT SELECT(attempt_id,manifest) ON public.execution_permits TO core_terminal_writer;
CREATE OR REPLACE VIEW public.core_terminal_stream_grants WITH (security_barrier=true) AS
 SELECT p.attempt_id,p.permit_epoch,p.owner_session_id,p.incarnation_id,
   ARRAY(SELECT jsonb_object_keys(v.rates)) AS priced_dimensions
 FROM public.execution_permits p JOIN public.execution_commands c ON c.id=p.command_id
 JOIN public.attempts a ON a.id=p.attempt_id
 JOIN public.upstream_holds h ON h.attempt_id=p.attempt_id
 JOIN public.cost_versions v ON v.id=h.cost_version_id
 JOIN public.billing_prices bp ON bp.id=a.billing_pricing_id AND bp.basis='REQUEST_COMPLETION'
 WHERE p.manifest->>'operation'='chat' AND p.manifest->'stream'='true'::jsonb
   AND p.manifest->'resource_ids'='[]'::jsonb AND c.kind='execution_admit_v1'
   AND c.receipt->>'state'='SEND_GRANTED' AND v.rates<>'{}'::jsonb;
REVOKE ALL ON public.core_terminal_stream_grants FROM PUBLIC;
GRANT SELECT ON public.core_terminal_stream_grants TO core_terminal_writer;
GRANT EXECUTE ON FUNCTION public.persist_core_evidence_groups_v1(jsonb) TO core_terminal_writer;
GRANT EXECUTE ON FUNCTION public.persist_core_evidence_slow_v1(jsonb) TO core_terminal_writer;
GRANT USAGE ON SCHEMA public TO core_terminal_confirmation;
GRANT SELECT(id) ON public.usage_event_index TO core_terminal_confirmation;
GRANT SELECT(id,command_id,kind,caller_identity,owner_session_id,caller_incarnation,body_digest,state,attempt_id)
 ON public.execution_commands TO core_terminal_confirmation;
GRANT SELECT(attempt_id,state,permit_epoch,authorization_plan_digest,owner_session_id,incarnation_id)
 ON public.execution_permits TO core_terminal_confirmation;
GRANT SELECT(id,request_id,state) ON public.attempts TO core_terminal_confirmation;
GRANT SELECT(id,selected_attempt_id,operation,state,billing_outcome,execution_closed_at)
 ON public.gateway_requests TO core_terminal_confirmation;
GRANT SELECT(request_id,evidence_id) ON public.billing_charges TO core_terminal_confirmation;
GRANT SELECT(attempt_id,state,final_cost_atoms) ON public.upstream_holds TO core_terminal_confirmation;
GRANT SELECT(attempt_id,evidence_id,complete,disputed)
 ON public.upstream_costs TO core_terminal_confirmation;
GRANT SELECT(id,event_id,attempt_id,origin_identity,origin_session_id,origin_incarnation,
 outcome,signature,signature_payload_digest,canonical_evidence_id)
 ON public.core_evidence_receipts TO core_terminal_confirmation;
