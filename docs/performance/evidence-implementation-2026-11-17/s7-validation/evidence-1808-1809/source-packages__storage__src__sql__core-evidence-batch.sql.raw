CREATE FUNCTION core_funding.fact_frame_v1(ids uuid[]) RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE frame jsonb;BEGIN
 SELECT jsonb_build_object('attempts',jsonb_object_agg(t.id::text,jsonb_build_object('attempt',jsonb_build_object('id',t.id,'request_id',t.request_id,'state',t.state,'finished_at',t.finished_at,'error_code',t.error_code,'provider_request_id',t.provider_request_id),'hold',to_jsonb(h),'rates',v.rates,'prices',coalesce(bp.rates,'{}'),'basis',bp.basis,'project_id',q.project_id,'request_id',q.id,'currency',v.currency,'revision',coalesce(rev.revision,0),'canonical',to_jsonb(ci),'usage',coalesce(cp.usage,'{}'),'seen_final',EXISTS(SELECT 1 FROM public.core_evidence_receipts r WHERE r.attempt_id=t.id AND r.outcome='ACCEPTED' AND r.finality IN('FINAL','NOT_EXECUTED')),'sources',coalesce(src.sources,'{}'))),'requests',jsonb_object_agg(q.id::text,jsonb_build_object('id',q.id,'selected_attempt_id',q.selected_attempt_id,'state',q.state,'billing_outcome',q.billing_outcome)),'snapshots','[]'::jsonb,'receipts','[]'::jsonb,'dirty_attempts','{}'::jsonb,'dirty_requests','{}'::jsonb)
 INTO frame FROM public.attempts t JOIN public.gateway_requests q ON q.id=t.request_id JOIN public.upstream_holds h ON h.attempt_id=t.id JOIN public.cost_versions v ON v.id=h.cost_version_id
 LEFT JOIN public.billing_prices bp ON bp.id=t.billing_pricing_id
 LEFT JOIN LATERAL(SELECT max(i.snapshot_revision) revision FROM public.usage_event_index i WHERE i.attempt_id=t.id AND i.snapshot_revision IS NOT NULL) rev ON true
 LEFT JOIN LATERAL(SELECT i.* FROM public.usage_event_index i WHERE i.attempt_id=t.id AND i.record_kind='CANONICAL' ORDER BY i.snapshot_revision DESC LIMIT 1) ci ON true
 LEFT JOIN public.usage_payloads cp ON cp.id=ci.id AND cp.created_at=ci.created_at
 LEFT JOIN LATERAL(SELECT jsonb_object_agg(jsonb_build_array(s.origin_identity,s.source)::text,s.revision) sources FROM(SELECT r.origin_identity,r.source,max(r.source_revision) revision FROM public.core_evidence_receipts r WHERE r.attempt_id=t.id AND r.outcome='ACCEPTED' GROUP BY r.origin_identity,r.source) s) src ON true
 WHERE t.id=ANY(ids);
 RETURN frame;
END $$;

CREATE FUNCTION core_funding.stage_snapshot_v1(frame jsonb,attempt uuid,kind text,source text,finality text,usage jsonb,finals text[],parent uuid,input uuid,cost numeric,complete boolean) RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE sid uuid=gen_random_uuid();meta jsonb=frame->'attempts'->attempt::text;rev numeric=(meta->>'revision')::numeric+1;row jsonb;BEGIN
 IF meta IS NULL THEN RAISE EXCEPTION 'evidence_frame_missing' USING ERRCODE='P0500';END IF;
 row=jsonb_build_object('id',sid,'created_at',transaction_timestamp(),'project_id',meta->>'project_id','request_id',meta->>'request_id','attempt_id',attempt,'source',source,'finality',finality,'cost_atoms',cost,'currency',meta->>'currency','cost_complete',coalesce(complete,false),'record_kind',kind,'snapshot_revision',rev,'snapshot_digest',public.core_digest_v1(jsonb_build_object('attempt_id',attempt,'record_kind',kind,'snapshot_revision',rev::text,'source',source,'finality',finality,'usage',usage,'final_dimensions',to_jsonb(finals),'parent',parent,'input',input,'cost_atoms',cost::text)),'final_dimensions',to_jsonb(finals),'parent_evidence_id',parent,'input_fact_id',input);
 frame=jsonb_set(frame,ARRAY['snapshots'],(frame->'snapshots')||jsonb_build_array(jsonb_build_object('index',row,'usage',usage)));
 frame=jsonb_set(frame,ARRAY['attempts',attempt::text,'revision'],to_jsonb(rev));
 IF kind='CANONICAL' THEN
  frame=jsonb_set(frame,ARRAY['attempts',attempt::text,'canonical'],row);
  frame=jsonb_set(frame,ARRAY['attempts',attempt::text,'usage'],usage);
 END IF;
 RETURN jsonb_build_object('snapshot',jsonb_build_object('id',sid,'revision',rev::text),'frame',frame);
END $$;

CREATE FUNCTION core_funding.flush_facts_v1(frame jsonb) RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE updated integer;BEGIN
 IF frame IS NULL THEN RETURN;END IF;
 INSERT INTO public.usage_event_index(id,project_id,request_id,attempt_id,source,finality,cost_atoms,currency,cost_complete,record_kind,snapshot_revision,snapshot_digest,final_dimensions,parent_evidence_id,input_fact_id)
  SELECT i.id,i.project_id,i.request_id,i.attempt_id,i.source,i.finality,i.cost_atoms,i.currency,i.cost_complete,i.record_kind,i.snapshot_revision,i.snapshot_digest,i.final_dimensions,i.parent_evidence_id,i.input_fact_id
  FROM jsonb_array_elements(frame->'snapshots') s CROSS JOIN LATERAL jsonb_populate_record(NULL::public.usage_event_index,s->'index') i ORDER BY i.attempt_id,i.snapshot_revision;
 INSERT INTO public.usage_payloads(id,created_at,usage)
  SELECT (s->'index'->>'id')::uuid,(s->'index'->>'created_at')::timestamptz,s->'usage' FROM jsonb_array_elements(frame->'snapshots') s;
 INSERT INTO public.event_deliveries(consumer_id,event_id)
  SELECT c.id,(s->'index'->>'id')::uuid FROM jsonb_array_elements(frame->'snapshots') s CROSS JOIN public.event_consumers c WHERE s->'index'->>'record_kind'<>'RAW';
 INSERT INTO public.core_evidence_receipts(id,origin_identity,origin_incarnation,origin_session_id,event_id,attempt_id,source,source_revision,body_digest,event_claim,revision_claim,outcome,finality,attempt_state,request_state,error_code,provider_request_id,raw_evidence_id,canonical_evidence_id,disputed_evidence_id,conflict_id,risk_dimensions,numeric_fault,authentication_disposition,authentication_reason,producer_key_sha256,signature_version,signature,signature_payload_digest)
  SELECT r.id,r.origin_identity,r.origin_incarnation,r.origin_session_id,r.event_id,r.attempt_id,r.source,r.source_revision,r.body_digest,r.event_claim,r.revision_claim,r.outcome,r.finality,r.attempt_state,r.request_state,r.error_code,r.provider_request_id,r.raw_evidence_id,r.canonical_evidence_id,r.disputed_evidence_id,r.conflict_id,r.risk_dimensions,r.numeric_fault,r.authentication_disposition,r.authentication_reason,r.producer_key_sha256,r.signature_version,r.signature,r.signature_payload_digest
  FROM jsonb_array_elements(frame->'receipts') x CROSS JOIN LATERAL jsonb_populate_record(NULL::public.core_evidence_receipts,x) r;
 UPDATE public.attempts a SET evidence_id=n.evidence_id,state=n.state,finished_at=n.finished_at,error_code=n.error_code,provider_request_id=n.provider_request_id
  FROM jsonb_each(frame->'dirty_attempts') d CROSS JOIN LATERAL jsonb_populate_record(NULL::public.attempts,frame->'attempts'->d.key->'attempt') n WHERE a.id=n.id;
 GET DIAGNOSTICS updated=ROW_COUNT;
 IF updated<>(SELECT count(*) FROM jsonb_each(frame->'dirty_attempts')) THEN RAISE EXCEPTION 'evidence_attempt_missing' USING ERRCODE='P0500';END IF;
 UPDATE public.gateway_requests q SET selected_attempt_id=n.selected_attempt_id,state=n.state,billing_outcome=n.billing_outcome,updated_at=n.updated_at
  FROM jsonb_each(frame->'dirty_requests') d CROSS JOIN LATERAL jsonb_populate_record(NULL::public.gateway_requests,frame->'requests'->d.key) n WHERE q.id=n.id;
 GET DIAGNOSTICS updated=ROW_COUNT;
 IF updated<>(SELECT count(*) FROM jsonb_each(frame->'dirty_requests')) THEN RAISE EXCEPTION 'evidence_request_missing' USING ERRCODE='P0500';END IF;
END $$;

CREATE FUNCTION core_funding.flush_costs_v1(pending jsonb) RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE updated integer;BEGIN
 IF pending IS NULL THEN RETURN NULL;END IF;
 IF jsonb_array_length(coalesce(pending->'costs','[]'))>0 THEN
  INSERT INTO public.upstream_costs(attempt_id,revision,currency,cost_atoms,complete,disputed,evidence_digest,evidence_id,cost_version_id)
   SELECT c.attempt_id,c.revision,c.currency,c.cost_atoms,c.complete,c.disputed,c.evidence_digest,c.evidence_id,c.cost_version_id FROM jsonb_array_elements(pending->'costs') x CROSS JOIN LATERAL jsonb_populate_record(NULL::public.upstream_costs,x) c;
  INSERT INTO public.upstream_cost_heads(attempt_id,revision)
   SELECT (c->>'attempt_id')::uuid,(c->>'revision')::numeric FROM jsonb_array_elements(pending->'costs') c ON CONFLICT(attempt_id) DO UPDATE SET revision=excluded.revision;
 END IF;
 IF coalesce(pending->'holds','{}')<>'{}'::jsonb THEN
  UPDATE public.upstream_holds h SET current_reserved_atoms=(d.value->>'current_reserved_atoms')::numeric,state=d.value->>'state',final_cost_atoms=(d.value->>'final_cost_atoms')::numeric
   FROM jsonb_each(pending->'holds') d WHERE h.attempt_id=d.key::uuid AND h.current_reserved_atoms=(d.value->>'base_reserved_atoms')::numeric;
  GET DIAGNOSTICS updated=ROW_COUNT;
  IF updated<>(SELECT count(*) FROM jsonb_each(pending->'holds')) THEN RAISE EXCEPTION 'evidence_hold_base_changed' USING ERRCODE='P0500';END IF;
 END IF;
 RETURN pending-ARRAY['costs','holds'];
END $$;

CREATE FUNCTION core_funding.coalesce_groups_v1(groups jsonb,pending jsonb) RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE events jsonb;attempt_owners jsonb;event_owners jsonb;request_owners jsonb;applied jsonb;result jsonb;results jsonb='[]';i integer;BEGIN
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(groups) g WHERE jsonb_typeof(g) IS DISTINCT FROM 'object' OR g-'events'<>'{}'::jsonb OR jsonb_typeof(g->'events') IS DISTINCT FROM 'array' OR jsonb_array_length(g->'events') NOT BETWEEN 1 AND 64) THEN RETURN NULL;END IF;
 SELECT jsonb_agg(e.value ORDER BY g.n,e.n),jsonb_object_agg(((e.value->>'attempt_id')::uuid)::text,g.n),jsonb_object_agg(((e.value->>'event_id')::uuid)::text,g.n)
 INTO events,attempt_owners,event_owners FROM jsonb_array_elements(groups) WITH ORDINALITY g(value,n) CROSS JOIN LATERAL jsonb_array_elements(g.value->'events') WITH ORDINALITY e(value,n);
 IF jsonb_array_length(events)>64 THEN RETURN NULL;END IF;
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(groups) WITH ORDINALITY g(value,n) CROSS JOIN LATERAL jsonb_array_elements(g.value->'events') e GROUP BY (e->>'attempt_id')::uuid HAVING count(DISTINCT g.n)>1)
  OR EXISTS(SELECT 1 FROM jsonb_array_elements(groups) WITH ORDINALITY g(value,n) CROSS JOIN LATERAL jsonb_array_elements(g.value->'events') e GROUP BY (e->>'event_id')::uuid HAVING count(DISTINCT g.n)>1) THEN RETURN NULL;END IF;
 IF EXISTS(SELECT 1 FROM public.attempts t JOIN public.billing_holds h ON h.request_id=t.request_id JOIN public.wallets w ON w.id=h.wallet_id WHERE attempt_owners?t.id::text GROUP BY h.wallet_id HAVING count(DISTINCT attempt_owners->>t.id::text)>1 OR bool_or(w.has_expiring_lots)) THEN RETURN NULL;END IF;
 applied=core_funding.evidence_group_v1(jsonb_build_object('events',events),pending||jsonb_build_object('partition',attempt_owners));
 result=applied->'result';
 SELECT jsonb_object_agg(t.request_id::text,attempt_owners->t.id::text) INTO request_owners FROM public.attempts t WHERE attempt_owners?t.id::text;
 FOR i IN 1..jsonb_array_length(groups) LOOP
  results=results||jsonb_build_array(jsonb_build_object('status','COMMITTED','receipts',coalesce((SELECT jsonb_agg(r.value ORDER BY r.n) FROM jsonb_array_elements(result->'receipts') WITH ORDINALITY r(value,n) WHERE (event_owners->>(((r.value->>'event_id')::uuid)::text))::integer=i),'[]'),'billing',coalesce((SELECT jsonb_object_agg(b.key,b.value) FROM jsonb_each(result->'billing') b WHERE (request_owners->>b.key)::integer=i),'{}')));
 END LOOP;
 RETURN jsonb_build_object('results',results,'pending',(applied->'pending')-'partition');
END $$;

CREATE OR REPLACE FUNCTION core_funding.supplier_settle_v1(attempt uuid,snapshot uuid,reference text,pending jsonb) RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE h public.upstream_holds%ROWTYPE;acct public.upstream_accounts%ROWTYPE;head public.upstream_costs%ROWTYPE;s public.usage_event_index%ROWTYPE;rates jsonb;usage jsonb;finals text[];x jsonb;
 old_cost numeric;old_reserve numeric;new_cost numeric;new_reserve numeric;cost_delta numeric;reserve_delta numeric;cost_rev numeric;journal uuid;frozen boolean;disputed boolean;faulted boolean;reserve_floor numeric=0;d record;dx jsonb;BEGIN
 SELECT * INTO h FROM public.upstream_holds WHERE attempt_id=attempt;
 IF h.attempt_id IS NULL OR h.estimated_quantities IS NULL OR h.state='RELEASED' OR reference !~ '^(cost|risk):[0-9a-f-]{36}:[0-9]{1,38}$' THEN RAISE EXCEPTION 'supplier_settlement_invalid' USING ERRCODE='P0409';END IF;
 PERFORM core_funding.share_v1(ARRAY[h.account_id]);
 SELECT * INTO acct FROM public.upstream_accounts WHERE id=h.account_id FOR SHARE NOWAIT;
 SELECT * INTO h FROM public.upstream_holds WHERE attempt_id=attempt FOR UPDATE;
 SELECT c.* INTO head FROM public.upstream_cost_heads hd JOIN public.upstream_costs c ON c.attempt_id=hd.attempt_id AND c.revision=hd.revision WHERE hd.attempt_id=attempt;
 faulted=EXISTS(SELECT 1 FROM public.core_evidence_receipts r WHERE r.attempt_id=attempt AND r.numeric_fault IS NOT NULL);
 disputed=EXISTS(SELECT 1 FROM public.usage_event_index i WHERE i.attempt_id=attempt AND i.record_kind='DISPUTED');
 IF snapshot IS NULL OR disputed OR faulted THEN
  IF head.attempt_id IS NULL THEN usage='{}';finals='{}';ELSE SELECT p.usage,i.final_dimensions INTO usage,finals FROM public.usage_event_index i JOIN public.usage_payloads p ON p.id=i.id AND p.created_at=i.created_at WHERE i.id=head.evidence_id;END IF;
 ELSE
  SELECT * INTO s FROM public.usage_event_index WHERE id=snapshot AND attempt_id=attempt;
  IF s.id IS NULL OR s.record_kind<>'CANONICAL' THEN RAISE EXCEPTION 'supplier_settlement_invalid' USING ERRCODE='P0409';END IF;
  SELECT p.usage INTO usage FROM public.usage_payloads p WHERE p.id=s.id AND p.created_at=s.created_at;finals=s.final_dimensions;
 END IF;
 x=core_funding.exposure_v1(h.cost_version_id,h.estimated_quantities,usage,finals,public.core_supplier_risk_v1(attempt));
 old_cost=coalesce(head.cost_atoms,0);
 FOR d IN SELECT i.final_dimensions,p.usage FROM public.usage_event_index i JOIN public.usage_payloads p ON p.id=i.id AND p.created_at=i.created_at WHERE i.attempt_id=attempt AND i.record_kind='DISPUTED' AND i.cost_atoms IS NOT NULL LOOP
  dx=core_funding.exposure_v1(h.cost_version_id,h.estimated_quantities,d.usage,d.final_dimensions,'{}'::jsonb);
  reserve_floor=greatest(reserve_floor,(dx->>'cost_atoms')::numeric+(dx->>'reserve_atoms')::numeric-old_cost);
 END LOOP;
 old_reserve=h.current_reserved_atoms;
 new_cost=(x->>'cost_atoms')::numeric;new_reserve=greatest((x->>'reserve_atoms')::numeric,reserve_floor);
 cost_delta=new_cost-old_cost;reserve_delta=new_reserve-old_reserve;
 IF cost_delta<0 THEN RAISE EXCEPTION 'supplier_cost_regression' USING ERRCODE='P0409';END IF;
 frozen=(x->>'overrun')::boolean OR jsonb_array_length(x->'risk_dimensions')>0 OR disputed OR faulted;
 IF (frozen OR acct.state<>'ACTIVE') AND (NOT core_funding.held_v1(h.account_id,true) OR NOT core_funding.catalog_held_v1()) THEN RAISE EXCEPTION 'funding_outer_transaction_required' USING ERRCODE='P0F01';END IF;
 IF snapshot IS NOT NULL AND NOT disputed AND NOT faulted THEN
  SELECT coalesce(max(c.revision),0)+1 INTO cost_rev FROM public.upstream_costs c WHERE c.attempt_id=attempt;
  IF pending IS NULL THEN
  INSERT INTO public.upstream_costs(attempt_id,revision,currency,cost_atoms,complete,disputed,evidence_digest,evidence_id,cost_version_id) VALUES(attempt,cost_rev,h.currency,new_cost,(x->>'complete')::boolean,false,public.core_digest_v1(jsonb_build_object('snapshot',s.snapshot_digest,'final_dimensions',to_jsonb(finals),'cost_version_id',h.cost_version_id,'estimated_quantities_digest',h.estimated_quantities_digest,'cost_atoms',new_cost::text)),snapshot,h.cost_version_id);
  INSERT INTO public.upstream_cost_heads(attempt_id,revision) VALUES(attempt,cost_rev) ON CONFLICT(attempt_id) DO UPDATE SET revision=excluded.revision;
  ELSE pending=jsonb_set(pending,ARRAY['costs'],coalesce(pending->'costs','[]')||jsonb_build_array(jsonb_build_object('attempt_id',attempt,'revision',cost_rev,'currency',h.currency,'cost_atoms',new_cost,'complete',(x->>'complete')::boolean,'disputed',false,'evidence_digest',public.core_digest_v1(jsonb_build_object('snapshot',s.snapshot_digest,'final_dimensions',to_jsonb(finals),'cost_version_id',h.cost_version_id,'estimated_quantities_digest',h.estimated_quantities_digest,'cost_atoms',new_cost::text)),'evidence_id',snapshot,'cost_version_id',h.cost_version_id)));END IF;
 END IF;
 IF cost_delta<>0 OR reserve_delta<>0 THEN
  journal=gen_random_uuid();
  IF pending IS NULL THEN
   INSERT INTO public.upstream_journals(id,account_id,channel_id,attempt_id,currency,kind,reference) VALUES(journal,h.account_id,h.channel_id,attempt,h.currency,CASE WHEN cost_delta<>0 THEN 'COST' ELSE 'RISK' END,reference);
   INSERT INTO public.upstream_entries(journal_id,ordinal,currency,account,atoms)
    SELECT journal,(row_number() OVER (ORDER BY ord))-1,h.currency,account,atoms FROM (VALUES(0,'upstream:available',-(cost_delta+reserve_delta)),(1,'upstream:reserved',reserve_delta),(2,'upstream:expense',cost_delta)) leg(ord,account,atoms) WHERE atoms<>0;
   SET CONSTRAINTS balanced_upstream_journal,balanced_upstream_entries IMMEDIATE;
   SET CONSTRAINTS balanced_upstream_journal,balanced_upstream_entries DEFERRED;
   PERFORM core_funding.apply_v1(attempt,cost_delta,new_reserve,true);
  ELSE
   pending=jsonb_set(pending,ARRAY['journals'],(pending->'journals')||jsonb_build_array(jsonb_build_object('id',journal,'account_id',h.account_id,'channel_id',h.channel_id,'attempt_id',attempt,'currency',h.currency,'kind',CASE WHEN cost_delta<>0 THEN 'COST' ELSE 'RISK' END,'reference',reference)));
   pending=jsonb_set(pending,ARRAY['entries'],(pending->'entries')||(SELECT jsonb_agg(jsonb_build_object('journal_id',journal,'ordinal',ordinal,'currency',h.currency,'account',account,'atoms',atoms) ORDER BY ordinal) FROM (SELECT (row_number() OVER (ORDER BY ord))-1 ordinal,account,atoms FROM (VALUES(0,'upstream:available',-(cost_delta+reserve_delta)),(1,'upstream:reserved',reserve_delta),(2,'upstream:expense',cost_delta)) leg(ord,account,atoms) WHERE atoms<>0) entries));
   pending=core_funding.project_v1(h,cost_delta,new_reserve,pending);
  END IF;
 END IF;
 IF pending IS NULL THEN
 UPDATE public.upstream_holds SET current_reserved_atoms=new_reserve,state=CASE WHEN (x->>'complete')::boolean AND new_reserve=0 AND NOT disputed AND NOT faulted THEN 'SETTLED' ELSE 'PARTIAL' END,final_cost_atoms=CASE WHEN (x->>'complete')::boolean AND new_reserve=0 AND NOT disputed AND NOT faulted THEN new_cost END WHERE attempt_id=attempt;
 ELSE pending=jsonb_set(pending,ARRAY['holds'],coalesce(pending->'holds','{}')||jsonb_build_object(attempt::text,jsonb_build_object('base_reserved_atoms',h.current_reserved_atoms,'current_reserved_atoms',new_reserve,'state',CASE WHEN (x->>'complete')::boolean AND new_reserve=0 AND NOT disputed AND NOT faulted THEN 'SETTLED' ELSE 'PARTIAL' END,'final_cost_atoms',CASE WHEN (x->>'complete')::boolean AND new_reserve=0 AND NOT disputed AND NOT faulted THEN new_cost END)));END IF;
 frozen=(x->>'overrun')::boolean OR jsonb_array_length(x->'risk_dimensions')>0 OR disputed OR faulted;
 IF frozen THEN
  UPDATE public.upstream_accounts SET state='RECONCILE_REQUIRED',revision=revision+1 WHERE id=h.account_id AND state='ACTIVE';
  INSERT INTO public.upstream_audit_events(id,account_id,channel_id,actor,action,new_digest,reason) VALUES(gen_random_uuid(),h.account_id,h.channel_id,'core-settlement','RECONCILE_REQUIRED',public.core_digest_v1(jsonb_build_object('attempt',attempt,'reference',reference,'exposure',x)),CASE WHEN (x->>'overrun')::boolean THEN 'supplier_cost_overrun' WHEN jsonb_array_length(x->'risk_dimensions')>0 THEN 'supplier_partial_risk' WHEN faulted THEN 'supplier_cost_overflow' ELSE 'supplier_evidence_disputed' END);
 END IF;
 RETURN x||jsonb_build_object('cost_delta',cost_delta::text,'reserve_delta',reserve_delta::text,'reserve_atoms',new_reserve::text,'revision',cost_rev::text,'frozen',frozen,'pending',pending);
END $$;


CREATE OR REPLACE FUNCTION core_funding.evidence_group_v1(g jsonb,pending jsonb) RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE ev jsonb;events jsonb[]='{}';n integer;i integer;pass integer;k text;attempt_ids uuid[];request_ids uuid[];permit public.execution_permits%ROWTYPE;h public.upstream_holds%ROWTYPE;
 prior public.core_evidence_receipts%ROWTYPE;rev_prior public.core_evidence_receipts%ROWTYPE;head record;cur public.usage_event_index%ROWTYPE;
 mode text[];contested boolean[];claims jsonb='{}';claim jsonb;d text;q text;owner integer;
 rates jsonb;prices jsonb;usage jsonb;finals text[];merged jsonb;merged_finals text[];raw jsonb;snap jsonb;x jsonb;outcome text;fault text;canonical uuid;disputed uuid;conflict uuid;receipt uuid;risk text[];stale boolean;event_claimed boolean;existing uuid;finality text;
 touched jsonb='{}';receipts jsonb='[]';billing jsonb='{}';settled jsonb;a uuid;row record;auth text[];sess public.internal_startup_sessions%ROWTYPE;revoked boolean;basis text;seen_final boolean;first_copy integer;frame jsonb;step jsonb;meta jsonb;attempt_row public.attempts%ROWTYPE;request_row public.gateway_requests%ROWTYPE;BEGIN
 IF jsonb_typeof(g) IS DISTINCT FROM 'object' OR g-'events'<>'{}'::jsonb OR jsonb_typeof(g->'events') IS DISTINCT FROM 'array' OR jsonb_array_length(g->'events') NOT BETWEEN 1 AND 64 THEN RAISE EXCEPTION 'evidence_group_invalid' USING ERRCODE='P0400';END IF;
 FOR ev IN SELECT value FROM jsonb_array_elements(g->'events') e(value) ORDER BY value->>'attempt_id',value->>'origin_identity',value->>'source',lpad(value->>'source_revision',38,'0'),value->>'event_id' LOOP
  IF jsonb_typeof(ev) IS DISTINCT FROM 'object' OR jsonb_typeof(ev->'signature') IS DISTINCT FROM 'string' OR ev->>'signature' !~ '^[a-f0-9]{128}$'
   OR jsonb_typeof(ev->'signature_payload_digest') IS DISTINCT FROM 'string' OR ev->>'signature_payload_digest' !~ '^[a-f0-9]{64}$'
   OR jsonb_typeof(ev->'producer_key_sha256') IS DISTINCT FROM 'string' OR ev->>'producer_key_sha256' !~ '^[a-f0-9]{64}$' THEN RAISE EXCEPTION 'evidence_event_invalid' USING ERRCODE='P0400';END IF;
  PERFORM public.core_evidence_validate_v1(ev-ARRAY['signature','signature_payload_digest','producer_key_sha256']);
  events=events||(ev||jsonb_build_object('body_digest',public.core_digest_v1(ev-ARRAY['event_id','signature','signature_payload_digest','producer_key_sha256'])));
 END LOOP;
 n=cardinality(events);
 FOR k IN SELECT DISTINCT key FROM unnest(events) e,LATERAL (VALUES('core-event:'||(e->>'origin_identity')||':'||(e->>'event_id')),('core-revision:'||(e->>'attempt_id')||':'||(e->>'origin_identity')||':'||(e->>'source')||':'||(e->>'source_revision'))) l(key) ORDER BY key LOOP
  IF NOT pg_try_advisory_xact_lock(hashtextextended(k,81442607)) THEN RAISE EXCEPTION 'core_busy' USING ERRCODE='P0503';END IF;
 END LOOP;
 IF EXISTS(SELECT 1 FROM unnest(events) e WHERE NOT EXISTS(SELECT 1 FROM public.internal_startup_sessions s WHERE s.session_id=(e->>'origin_session_id')::uuid)) THEN RAISE EXCEPTION 'evidence_subject_invalid' USING ERRCODE='P0403';END IF;
 PERFORM 1 FROM public.internal_service_instances WHERE (service,instance_id) IN(SELECT s.service,s.instance_id FROM public.internal_startup_sessions s WHERE s.session_id IN(SELECT (e->>'origin_session_id')::uuid FROM unnest(events) e)) ORDER BY service COLLATE "C",instance_id COLLATE "C" FOR SHARE NOWAIT;
 PERFORM 1 FROM public.internal_service_certificates WHERE certificate_sha256 IN(SELECT s.certificate_sha256 FROM public.internal_startup_sessions s WHERE s.session_id IN(SELECT (e->>'origin_session_id')::uuid FROM unnest(events) e)) ORDER BY certificate_sha256 COLLATE "C" FOR SHARE NOWAIT;
 PERFORM 1 FROM public.internal_startup_sessions WHERE session_id IN(SELECT (e->>'origin_session_id')::uuid FROM unnest(events) e) ORDER BY session_id FOR SHARE NOWAIT;
 attempt_ids=ARRAY(SELECT DISTINCT (e->>'attempt_id')::uuid FROM unnest(events) e ORDER BY 1);
 request_ids=ARRAY(SELECT DISTINCT t.request_id FROM public.attempts t WHERE t.id=ANY(attempt_ids) ORDER BY 1);
 PERFORM core_funding.share_v1(ARRAY(SELECT DISTINCT account_id FROM public.upstream_holds WHERE attempt_id=ANY(attempt_ids) ORDER BY account_id));
 PERFORM 1 FROM public.upstream_accounts WHERE id IN(SELECT account_id FROM public.upstream_holds WHERE attempt_id=ANY(attempt_ids)) ORDER BY id FOR SHARE NOWAIT;
 PERFORM 1 FROM public.wallets WHERE id IN(SELECT wallet_id FROM public.billing_holds WHERE request_id=ANY(request_ids)) ORDER BY id FOR NO KEY UPDATE NOWAIT;
 PERFORM 1 FROM public.gateway_requests WHERE id=ANY(request_ids) ORDER BY id FOR UPDATE NOWAIT;
 PERFORM 1 FROM public.billing_holds WHERE request_id=ANY(request_ids) ORDER BY id FOR UPDATE NOWAIT;
 PERFORM 1 FROM public.attempts WHERE id=ANY(attempt_ids) ORDER BY request_id,ordinal,id FOR NO KEY UPDATE NOWAIT;
 PERFORM 1 FROM public.execution_permits WHERE attempt_id=ANY(attempt_ids) ORDER BY attempt_id FOR NO KEY UPDATE NOWAIT;
 PERFORM 1 FROM public.upstream_holds WHERE attempt_id=ANY(attempt_ids) ORDER BY attempt_id FOR UPDATE NOWAIT;
 mode=array_fill('NEW'::text,ARRAY[n]);contested=array_fill(false,ARRAY[n]);auth=array_fill('NORMAL'::text,ARRAY[n]);
 FOR i IN 1..n LOOP
  ev=events[i];
  SELECT * INTO permit FROM public.execution_permits WHERE attempt_id=(ev->>'attempt_id')::uuid;
  SELECT * INTO sess FROM public.internal_startup_sessions WHERE session_id=(ev->>'origin_session_id')::uuid;
  IF permit.attempt_id IS NULL OR NOT (permit.state IN ('SEND_GRANTED','UNKNOWN','TERMINAL') OR (permit.state IN ('RESERVED','ABORTED') AND permit.granted_at IS NULL))
   OR sess.service||':'||sess.instance_id IS DISTINCT FROM ev->>'origin_identity' OR sess.incarnation_id IS DISTINCT FROM (ev->>'origin_incarnation')::uuid OR sess.public_key_sha256 IS DISTINCT FROM ev->>'producer_key_sha256'
   OR permit.permit_epoch IS DISTINCT FROM (ev->>'permit_epoch')::numeric
   OR ((permit.owner_service||':'||permit.gateway_instance_id IS DISTINCT FROM ev->>'origin_identity' OR permit.owner_session_id IS DISTINCT FROM sess.session_id OR permit.incarnation_id IS DISTINCT FROM sess.incarnation_id
     OR (permit.owner_key_sha256 IS NOT NULL AND permit.owner_key_sha256<>sess.public_key_sha256) OR (permit.certificate_sha256 IS NOT NULL AND permit.certificate_sha256<>sess.certificate_sha256))
    AND NOT (ev->>'source' IN ('async_partial','async_final','batch_completed','batch_cancelled','batch_failed') AND EXISTS(SELECT 1 FROM public.resources rs WHERE rs.attempt_id=permit.attempt_id AND rs.lease_session_id=sess.session_id AND rs.lease_epoch=(ev->>'source_revision')::numeric)))
   OR NOT EXISTS(SELECT 1 FROM public.upstream_holds WHERE attempt_id=permit.attempt_id AND estimated_quantities IS NOT NULL)
   OR EXISTS(SELECT 1 FROM public.core_evidence_receipts r WHERE r.origin_identity=ev->>'origin_identity' AND r.event_id=(ev->>'event_id')::uuid AND r.attempt_id<>permit.attempt_id) THEN RAISE EXCEPTION 'evidence_subject_invalid' USING ERRCODE='P0403';END IF;
  revoked=sess.security_revoked_at IS NOT NULL OR EXISTS(SELECT 1 FROM public.internal_service_instances x WHERE x.service=sess.service AND x.instance_id=sess.instance_id AND x.security_revoked_at IS NOT NULL) OR EXISTS(SELECT 1 FROM public.internal_service_certificates c WHERE c.certificate_sha256=sess.certificate_sha256 AND c.security_revoked_at IS NOT NULL);
  auth[i]=CASE WHEN revoked THEN 'SECURITY_REVOKED' WHEN permit.state IN ('RESERVED','ABORTED') THEN 'UNGRANTED' ELSE 'NORMAL' END;
 END LOOP;
 FOR i IN 1..n LOOP
  ev=events[i];
  IF ev->>'finality'='NOT_EXECUTED' THEN
   SELECT v.rates INTO rates FROM public.upstream_holds hh JOIN public.cost_versions v ON v.id=hh.cost_version_id WHERE hh.attempt_id=(ev->>'attempt_id')::uuid;
   SELECT coalesce(p.rates,'{}') INTO prices FROM public.attempts t LEFT JOIN public.billing_prices p ON p.id=t.billing_pricing_id WHERE t.id=(ev->>'attempt_id')::uuid;
   SELECT coalesce(jsonb_object_agg(key,'0'),'{}') INTO usage FROM (SELECT key FROM jsonb_each(rates) UNION SELECT key FROM jsonb_each(prices)) dims;
   events[i]=ev||jsonb_build_object('normalized_usage',usage,'normalized_finals',(SELECT coalesce(jsonb_agg(key ORDER BY key),'[]') FROM jsonb_each(usage)));
  ELSE
   events[i]=ev||jsonb_build_object('normalized_usage',ev->'usage','normalized_finals',ev->'final_dimensions');
  END IF;
 END LOOP;
 FOR i IN 1..n LOOP
  ev=events[i];
  IF EXISTS(SELECT 1 FROM public.core_evidence_receipts r WHERE r.origin_identity=ev->>'origin_identity' AND r.event_id=(ev->>'event_id')::uuid AND r.body_digest=ev->>'body_digest') OR EXISTS(SELECT 1 FROM generate_series(1,i-1) j WHERE events[j]->>'origin_identity'=ev->>'origin_identity' AND events[j]->>'event_id'=ev->>'event_id' AND events[j]->>'body_digest'=ev->>'body_digest') THEN mode[i]='DUPLICATE';CONTINUE;END IF;
  IF auth[i]<>'NORMAL' THEN mode[i]='QUARANTINED';CONTINUE;END IF;
  SELECT * INTO rev_prior FROM public.core_evidence_receipts r WHERE r.attempt_id=(ev->>'attempt_id')::uuid AND r.origin_identity=ev->>'origin_identity' AND r.source=ev->>'source' AND r.source_revision=(ev->>'source_revision')::numeric AND r.revision_claim;
  IF EXISTS(SELECT 1 FROM public.core_evidence_receipts r WHERE r.origin_identity=ev->>'origin_identity' AND r.event_id=(ev->>'event_id')::uuid AND r.event_claim) OR (rev_prior.id IS NOT NULL AND rev_prior.body_digest<>ev->>'body_digest')
   OR EXISTS(SELECT 1 FROM generate_series(1,i-1) j WHERE events[j]->>'origin_identity'=ev->>'origin_identity' AND ((events[j]->>'event_id'=ev->>'event_id' AND events[j]->>'body_digest'<>ev->>'body_digest') OR (events[j]->>'attempt_id'=ev->>'attempt_id' AND events[j]->>'source'=ev->>'source' AND events[j]->>'source_revision'=ev->>'source_revision' AND events[j]->>'body_digest'<>ev->>'body_digest')))
  THEN mode[i]='CONFLICT';contested[i]=true;CONTINUE;END IF;
  IF rev_prior.id IS NOT NULL OR EXISTS(SELECT 1 FROM generate_series(1,i-1) j WHERE events[j]->>'origin_identity'=ev->>'origin_identity' AND events[j]->>'attempt_id'=ev->>'attempt_id' AND events[j]->>'source'=ev->>'source' AND events[j]->>'source_revision'=ev->>'source_revision' AND mode[j] IN ('NEW','REPLAY')) THEN mode[i]='REPLAY';CONTINUE;END IF;
  a=(ev->>'attempt_id')::uuid;
  IF NOT claims?a::text THEN
   SELECT i2.final_dimensions,p.usage INTO head FROM public.usage_event_index i2 JOIN public.usage_payloads p ON p.id=i2.id AND p.created_at=i2.created_at WHERE i2.attempt_id=a AND i2.record_kind='CANONICAL' ORDER BY i2.snapshot_revision DESC LIMIT 1;
   claims=claims||jsonb_build_object(a::text,coalesce((SELECT jsonb_object_agg(f,jsonb_build_object('value',head.usage->>f,'owner',0)) FROM unnest(head.final_dimensions) f),'{}'));
  END IF;
  FOR d,q IN SELECT f,ev->'normalized_usage'->>f FROM jsonb_array_elements_text(ev->'normalized_finals') f LOOP
   claim=claims->a::text->d;
   IF claim IS NULL THEN claims=jsonb_set(claims,ARRAY[a::text,d],jsonb_build_object('value',q,'owner',i));
   ELSIF (claim->>'value')::numeric<>q::numeric OR coalesce((claim->>'contested')::boolean,false) THEN
    contested[i]=true;owner=(claim->>'owner')::integer;
    IF owner>0 THEN contested[owner]=true;claims=jsonb_set(claims,ARRAY[a::text,d,'contested'],'true');END IF;
   END IF;
  END LOOP;
 END LOOP;
 FOR i IN 1..n LOOP
  CONTINUE WHEN mode[i]<>'REPLAY';
  SELECT min(j2) INTO first_copy FROM generate_series(1,i-1) j2 WHERE events[j2]->>'origin_identity'=events[i]->>'origin_identity' AND events[j2]->>'attempt_id'=events[i]->>'attempt_id' AND events[j2]->>'source'=events[i]->>'source' AND events[j2]->>'source_revision'=events[i]->>'source_revision' AND mode[j2] IN ('NEW','REPLAY');
  IF first_copy IS NOT NULL AND contested[first_copy] THEN contested[i]=true;mode[i]='NEW';END IF;
 END LOOP;
 IF pending?'partition' THEN
  IF EXISTS(SELECT 1 FROM public.attempts t JOIN public.billing_holds bh ON bh.request_id=t.request_id JOIN public.wallets w ON w.id=bh.wallet_id WHERE t.id=ANY(attempt_ids) GROUP BY bh.wallet_id HAVING count(DISTINCT pending->'partition'->>t.id::text)>1 OR bool_or(w.has_expiring_lots))
   OR (SELECT count(*) FROM public.attempts t JOIN public.billing_holds bh ON bh.request_id=t.request_id WHERE t.id=ANY(attempt_ids))<>cardinality(attempt_ids)
   OR EXISTS(SELECT 1 FROM generate_series(1,n) j WHERE mode[j]<>'NEW' OR contested[j] OR auth[j]<>'NORMAL') THEN RAISE EXCEPTION 'evidence_batch_dependency' USING ERRCODE='P0E01';END IF;
 END IF;
 IF pending IS NOT NULL AND NOT EXISTS(SELECT 1 FROM generate_series(1,n) j WHERE mode[j]<>'NEW' OR contested[j] OR auth[j]<>'NORMAL') THEN frame=core_funding.fact_frame_v1(attempt_ids);END IF;
 FOR pass IN 1..2 LOOP
  FOR i IN 1..n LOOP
   ev=events[i];a=(ev->>'attempt_id')::uuid;
   CONTINUE WHEN (pass=1)=(contested[i] OR mode[i]='DUPLICATE');
   IF mode[i]='DUPLICATE' THEN
    SELECT * INTO prior FROM public.core_evidence_receipts r WHERE r.origin_identity=ev->>'origin_identity' AND r.event_id=(ev->>'event_id')::uuid AND r.body_digest=ev->>'body_digest';
    IF prior.signature_payload_digest<>ev->>'signature_payload_digest' OR prior.producer_key_sha256<>ev->>'producer_key_sha256' THEN RAISE EXCEPTION 'evidence_subject_invalid' USING ERRCODE='P0403';END IF;
    receipts=receipts||jsonb_build_array(jsonb_build_object('event_id',ev->>'event_id','receipt_id',prior.id,'outcome',prior.outcome,'duplicate',true));CONTINUE;
   END IF;
   IF frame IS NULL THEN
   SELECT * INTO h FROM public.upstream_holds WHERE attempt_id=a;
   SELECT v.rates INTO rates FROM public.cost_versions v WHERE v.id=h.cost_version_id;
   SELECT p.basis,coalesce(p.rates,'{}') INTO basis,prices FROM public.attempts t LEFT JOIN public.billing_prices p ON p.id=t.billing_pricing_id WHERE t.id=a;
   SELECT * INTO cur FROM public.usage_event_index WHERE attempt_id=a AND record_kind='CANONICAL' ORDER BY snapshot_revision DESC LIMIT 1;
   SELECT p.usage INTO merged FROM public.usage_payloads p WHERE p.id=cur.id AND p.created_at=cur.created_at;
   ELSE
    meta=frame->'attempts'->a::text;h=jsonb_populate_record(NULL::public.upstream_holds,meta->'hold');rates=meta->'rates';basis=meta->>'basis';prices=meta->'prices';
    cur=jsonb_populate_record(NULL::public.usage_event_index,NULLIF(meta->'canonical','null'::jsonb));merged=meta->'usage';
   END IF;
   merged=coalesce(merged,'{}');merged_finals=coalesce(cur.final_dimensions,'{}');
   usage=ev->'normalized_usage';finals=ARRAY(SELECT jsonb_array_elements_text(ev->'normalized_finals'));
   IF frame IS NULL THEN event_claimed=EXISTS(SELECT 1 FROM public.core_evidence_receipts r WHERE r.origin_identity=ev->>'origin_identity' AND r.event_id=(ev->>'event_id')::uuid AND r.event_claim);ELSE event_claimed=false;END IF;
   receipt=gen_random_uuid();canonical=cur.id;disputed=NULL;conflict=NULL;fault=NULL;risk='{}';
   IF mode[i]='REPLAY' THEN
    SELECT * INTO rev_prior FROM public.core_evidence_receipts r WHERE r.attempt_id=a AND r.origin_identity=ev->>'origin_identity' AND r.source=ev->>'source' AND r.source_revision=(ev->>'source_revision')::numeric AND r.revision_claim;
    IF rev_prior.id IS NULL OR rev_prior.body_digest<>ev->>'body_digest' THEN RAISE EXCEPTION 'evidence_replay_unanchored' USING ERRCODE='P0503';END IF;
    INSERT INTO public.core_evidence_receipts(id,origin_identity,origin_incarnation,origin_session_id,event_id,attempt_id,source,source_revision,body_digest,event_claim,revision_claim,outcome,finality,attempt_state,request_state,error_code,provider_request_id,raw_evidence_id,canonical_evidence_id,risk_dimensions,authentication_disposition,authentication_reason,producer_key_sha256,signature_version,signature,signature_payload_digest)
    VALUES(receipt,ev->>'origin_identity',(ev->>'origin_incarnation')::uuid,(ev->>'origin_session_id')::uuid,(ev->>'event_id')::uuid,a,ev->>'source',(ev->>'source_revision')::numeric,ev->>'body_digest',NOT event_claimed,false,'REVISION_REPLAY',ev->>'finality',ev->>'attempt_state',ev->>'request_state',ev->>'error_code',ev->>'provider_request_id',rev_prior.raw_evidence_id,rev_prior.canonical_evidence_id,'{}','VERIFIED','NORMAL',ev->>'producer_key_sha256',1,decode(ev->>'signature','hex'),ev->>'signature_payload_digest');
    receipts=receipts||jsonb_build_array(jsonb_build_object('event_id',ev->>'event_id','receipt_id',receipt,'outcome','REVISION_REPLAY','duplicate',false));CONTINUE;
   END IF;
   finality=CASE ev->>'finality' WHEN 'NOT_EXECUTED' THEN 'FINAL' ELSE ev->>'finality' END;
   IF frame IS NULL THEN raw=public.core_evidence_snapshot_v1(a,'RAW',ev->>'source',finality,usage,finals,NULL,NULL,NULL,false);ELSE step=core_funding.stage_snapshot_v1(frame,a,'RAW',ev->>'source',finality,usage,finals,NULL,NULL,NULL,false);raw=step->'snapshot';frame=step->'frame';END IF;
   IF mode[i]='QUARANTINED' THEN
    INSERT INTO public.core_evidence_receipts(id,origin_identity,origin_incarnation,origin_session_id,event_id,attempt_id,source,source_revision,body_digest,event_claim,revision_claim,outcome,finality,attempt_state,request_state,error_code,provider_request_id,raw_evidence_id,risk_dimensions,authentication_disposition,authentication_reason,producer_key_sha256,signature_version,signature,signature_payload_digest)
    VALUES(receipt,ev->>'origin_identity',(ev->>'origin_incarnation')::uuid,(ev->>'origin_session_id')::uuid,(ev->>'event_id')::uuid,a,ev->>'source',(ev->>'source_revision')::numeric,ev->>'body_digest',NOT event_claimed,false,'QUARANTINED',ev->>'finality',ev->>'attempt_state',ev->>'request_state',ev->>'error_code',ev->>'provider_request_id',(raw->>'id')::uuid,'{}','QUARANTINED',auth[i],ev->>'producer_key_sha256',1,decode(ev->>'signature','hex'),ev->>'signature_payload_digest');
    receipts=receipts||jsonb_build_array(jsonb_build_object('event_id',ev->>'event_id','receipt_id',receipt,'outcome','QUARANTINED','duplicate',false));CONTINUE;
   END IF;
   IF ev->>'finality'='PARTIAL' THEN
    risk=ARRAY(SELECT d2 FROM jsonb_object_keys(rates) d2 WHERE usage?d2 AND NOT d2=ANY(finals) AND NOT d2=ANY(merged_finals) AND (usage->>d2)::numeric>(h.estimated_quantities->>d2)::numeric ORDER BY d2);
   END IF;
   IF contested[i] THEN
    outcome='CONFLICT';merged=merged||usage;merged_finals=ARRAY(SELECT DISTINCT unnest(merged_finals||finals) ORDER BY 1);
    existing=coalesce(cur.id,(SELECT r.raw_evidence_id FROM public.core_evidence_receipts r WHERE r.attempt_id=a AND r.origin_identity=ev->>'origin_identity' AND (r.event_id=(ev->>'event_id')::uuid OR (r.source=ev->>'source' AND r.source_revision=(ev->>'source_revision')::numeric)) ORDER BY r.created_at LIMIT 1),
     (SELECT r.raw_evidence_id FROM public.core_evidence_receipts r WHERE r.attempt_id=a AND r.outcome='CONFLICT' ORDER BY r.created_at LIMIT 1),
     (SELECT i3.id FROM public.usage_event_index i3 WHERE i3.attempt_id=a AND i3.record_kind='RAW' AND i3.id<>(raw->>'id')::uuid ORDER BY i3.snapshot_revision DESC LIMIT 1),(raw->>'id')::uuid);
    BEGIN x=core_funding.exposure_v1(h.cost_version_id,h.estimated_quantities,merged,merged_finals,'{}');
    EXCEPTION WHEN numeric_value_out_of_range THEN x=NULL;fault='COST_OVERFLOW';
     WHEN raise_exception THEN IF SQLERRM IS DISTINCT FROM 'price exceeds amount range' THEN RAISE;END IF;x=NULL;fault='COST_OVERFLOW';END;
    IF frame IS NULL THEN snap=public.core_evidence_snapshot_v1(a,'DISPUTED',ev->>'source',finality,merged,merged_finals,cur.id,(raw->>'id')::uuid,(x->>'cost_atoms')::numeric,coalesce((x->>'complete')::boolean,false));ELSE step=core_funding.stage_snapshot_v1(frame,a,'DISPUTED',ev->>'source',finality,merged,merged_finals,cur.id,(raw->>'id')::uuid,(x->>'cost_atoms')::numeric,coalesce((x->>'complete')::boolean,false));snap=step->'snapshot';frame=step->'frame';END IF;
    disputed=(snap->>'id')::uuid;conflict=gen_random_uuid();
    INSERT INTO public.usage_conflicts(id,attempt_id,existing_event_id,source,finality,usage,fingerprint,incoming_evidence_id,disputed_evidence_id,incoming_receipt_id)
    VALUES(conflict,a,existing,ev->>'source',finality,usage,public.core_digest_v1(jsonb_build_object('attempt_id',a,'existing',existing,'incoming',raw->>'id','usage',usage,'final_dimensions',to_jsonb(finals))),(raw->>'id')::uuid,disputed,receipt);
    UPDATE public.attempts SET metering_conflict=true WHERE id=a;
   ELSIF ev->>'finality'='ESTIMATED' THEN outcome='ESTIMATED';risk='{}';
   ELSE
    IF frame IS NULL THEN stale=EXISTS(SELECT 1 FROM public.core_evidence_receipts r WHERE r.attempt_id=a AND r.origin_identity=ev->>'origin_identity' AND r.source=ev->>'source' AND r.outcome='ACCEPTED' AND r.source_revision>(ev->>'source_revision')::numeric);ELSE stale=coalesce((frame->'attempts'->a::text->'sources'->>jsonb_build_array(ev->>'origin_identity',ev->>'source')::text)::numeric,0)>(ev->>'source_revision')::numeric;END IF;
    IF stale THEN outcome='STALE';
    ELSE
     merged=merged||(usage-merged_finals);merged_finals=ARRAY(SELECT DISTINCT unnest(merged_finals||finals) ORDER BY 1);
     BEGIN x=core_funding.exposure_v1(h.cost_version_id,h.estimated_quantities,merged,merged_finals,'{}');
     EXCEPTION WHEN numeric_value_out_of_range THEN x=NULL;fault='COST_OVERFLOW';
      WHEN raise_exception THEN IF SQLERRM IS DISTINCT FROM 'price exceeds amount range' THEN RAISE;END IF;x=NULL;fault='COST_OVERFLOW';END;
     IF x IS NULL THEN
      outcome='OVERFLOW';IF frame IS NULL THEN snap=public.core_evidence_snapshot_v1(a,'DISPUTED',ev->>'source',finality,merged,merged_finals,cur.id,(raw->>'id')::uuid,NULL,false);ELSE step=core_funding.stage_snapshot_v1(frame,a,'DISPUTED',ev->>'source',finality,merged,merged_finals,cur.id,(raw->>'id')::uuid,NULL,false);snap=step->'snapshot';frame=step->'frame';END IF;disputed=(snap->>'id')::uuid;
     ELSE
      outcome='ACCEPTED';
      seen_final=finality='FINAL' OR CASE WHEN frame IS NULL THEN EXISTS(SELECT 1 FROM public.core_evidence_receipts r WHERE r.attempt_id=a AND r.outcome='ACCEPTED' AND r.finality IN ('FINAL','NOT_EXECUTED')) ELSE coalesce((frame->'attempts'->a::text->>'seen_final')::boolean,false) END;
      IF frame IS NULL THEN snap=public.core_evidence_snapshot_v1(a,'CANONICAL',ev->>'source',CASE WHEN seen_final AND (basis IS DISTINCT FROM 'PROVIDER_USAGE' OR NOT EXISTS(SELECT 1 FROM jsonb_object_keys(prices) p2 WHERE NOT p2=ANY(merged_finals))) THEN 'FINAL' WHEN cardinality(merged_finals)>0 OR merged<>'{}'::jsonb THEN 'PARTIAL' ELSE 'UNKNOWN' END,merged,merged_finals,cur.id,(raw->>'id')::uuid,(x->>'cost_atoms')::numeric,(x->>'complete')::boolean);ELSE step=core_funding.stage_snapshot_v1(frame,a,'CANONICAL',ev->>'source',CASE WHEN seen_final AND (basis IS DISTINCT FROM 'PROVIDER_USAGE' OR NOT EXISTS(SELECT 1 FROM jsonb_object_keys(prices) p2 WHERE NOT p2=ANY(merged_finals))) THEN 'FINAL' WHEN cardinality(merged_finals)>0 OR merged<>'{}'::jsonb THEN 'PARTIAL' ELSE 'UNKNOWN' END,merged,merged_finals,cur.id,(raw->>'id')::uuid,(x->>'cost_atoms')::numeric,(x->>'complete')::boolean);snap=step->'snapshot';frame=step->'frame';END IF;
      canonical=(snap->>'id')::uuid;
      IF frame IS NULL THEN
      UPDATE public.attempts SET evidence_id=canonical,state=CASE WHEN state IN ('SUCCEEDED','FAILED','NOT_EXECUTED') OR ev->>'attempt_state' IS NULL THEN state ELSE ev->>'attempt_state' END,
       finished_at=CASE WHEN ev->>'attempt_state' IN ('SUCCEEDED','FAILED','NOT_EXECUTED') THEN coalesce(finished_at,clock_timestamp()) ELSE finished_at END,error_code=coalesce(ev->>'error_code',error_code),provider_request_id=coalesce(ev->>'provider_request_id',provider_request_id) WHERE id=a;
      UPDATE public.gateway_requests SET selected_attempt_id=CASE WHEN ev->>'request_state' IN ('SUCCEEDED','CANCELLED') THEN a END,
       state=ev->>'request_state',billing_outcome=CASE ev->>'request_state' WHEN 'SUCCEEDED' THEN 'SUCCESS' ELSE ev->>'request_state' END,updated_at=clock_timestamp()
       WHERE id=(SELECT request_id FROM public.attempts WHERE id=a) AND ev->>'request_state' IN ('SUCCEEDED','CANCELLED','FAILED') AND selected_attempt_id IS NULL AND billing_outcome IN ('PENDING','UNKNOWN');
      ELSE
       attempt_row=jsonb_populate_record(NULL::public.attempts,frame->'attempts'->a::text->'attempt');
       attempt_row.evidence_id=canonical;
       IF attempt_row.state NOT IN('SUCCEEDED','FAILED','NOT_EXECUTED') AND ev->>'attempt_state' IS NOT NULL THEN attempt_row.state=ev->>'attempt_state';END IF;
       IF ev->>'attempt_state' IN('SUCCEEDED','FAILED','NOT_EXECUTED') THEN attempt_row.finished_at=coalesce(attempt_row.finished_at,clock_timestamp());END IF;
       attempt_row.error_code=coalesce(ev->>'error_code',attempt_row.error_code);attempt_row.provider_request_id=coalesce(ev->>'provider_request_id',attempt_row.provider_request_id);
       frame=jsonb_set(frame,ARRAY['attempts',a::text,'attempt'],to_jsonb(attempt_row));frame=jsonb_set(frame,ARRAY['dirty_attempts',a::text],'true');
       frame=jsonb_set(frame,ARRAY['attempts',a::text,'seen_final'],to_jsonb(seen_final));
       frame=jsonb_set(frame,ARRAY['attempts',a::text,'sources',jsonb_build_array(ev->>'origin_identity',ev->>'source')::text],to_jsonb((ev->>'source_revision')::numeric));
       request_row=jsonb_populate_record(NULL::public.gateway_requests,frame->'requests'->attempt_row.request_id::text);
       IF ev->>'request_state' IN('SUCCEEDED','CANCELLED','FAILED') AND request_row.selected_attempt_id IS NULL AND request_row.billing_outcome IN('PENDING','UNKNOWN') THEN
        request_row.selected_attempt_id=CASE WHEN ev->>'request_state' IN('SUCCEEDED','CANCELLED') THEN a END;request_row.state=ev->>'request_state';request_row.billing_outcome=CASE ev->>'request_state' WHEN 'SUCCEEDED' THEN 'SUCCESS' ELSE ev->>'request_state' END;request_row.updated_at=clock_timestamp();
        frame=jsonb_set(frame,ARRAY['requests',request_row.id::text],to_jsonb(request_row));frame=jsonb_set(frame,ARRAY['dirty_requests',request_row.id::text],'true');
       END IF;
      END IF;
     END IF;
    END IF;
   END IF;
   IF frame IS NULL THEN
   INSERT INTO public.core_evidence_receipts(id,origin_identity,origin_incarnation,origin_session_id,event_id,attempt_id,source,source_revision,body_digest,event_claim,revision_claim,outcome,finality,attempt_state,request_state,error_code,provider_request_id,raw_evidence_id,canonical_evidence_id,disputed_evidence_id,conflict_id,risk_dimensions,numeric_fault,authentication_disposition,authentication_reason,producer_key_sha256,signature_version,signature,signature_payload_digest)
   VALUES(receipt,ev->>'origin_identity',(ev->>'origin_incarnation')::uuid,(ev->>'origin_session_id')::uuid,(ev->>'event_id')::uuid,a,ev->>'source',(ev->>'source_revision')::numeric,ev->>'body_digest',NOT event_claimed,outcome<>'CONFLICT',outcome,ev->>'finality',ev->>'attempt_state',ev->>'request_state',ev->>'error_code',ev->>'provider_request_id',(raw->>'id')::uuid,canonical,disputed,conflict,risk,fault,'VERIFIED','NORMAL',ev->>'producer_key_sha256',1,decode(ev->>'signature','hex'),ev->>'signature_payload_digest');
   ELSE frame=jsonb_set(frame,ARRAY['receipts'],(frame->'receipts')||jsonb_build_array(jsonb_build_object('id',receipt,'origin_identity',ev->>'origin_identity','origin_incarnation',(ev->>'origin_incarnation')::uuid,'origin_session_id',(ev->>'origin_session_id')::uuid,'event_id',(ev->>'event_id')::uuid,'attempt_id',a,'source',ev->>'source','source_revision',(ev->>'source_revision')::numeric,'body_digest',ev->>'body_digest','event_claim',NOT event_claimed,'revision_claim',outcome<>'CONFLICT','outcome',outcome,'finality',ev->>'finality','attempt_state',ev->>'attempt_state','request_state',ev->>'request_state','error_code',ev->>'error_code','provider_request_id',ev->>'provider_request_id','raw_evidence_id',(raw->>'id')::uuid,'canonical_evidence_id',canonical,'disputed_evidence_id',disputed,'conflict_id',conflict,'risk_dimensions',risk,'numeric_fault',fault,'authentication_disposition','VERIFIED','authentication_reason','NORMAL','producer_key_sha256',ev->>'producer_key_sha256','signature_version',1,'signature',decode(ev->>'signature','hex'),'signature_payload_digest',ev->>'signature_payload_digest')));END IF;
   touched=jsonb_set(touched,ARRAY[a::text],jsonb_build_object('canonical',CASE WHEN outcome='ACCEPTED' THEN to_jsonb(canonical) ELSE coalesce(touched->a::text->'canonical','null'::jsonb) END,'revision',CASE WHEN outcome='ACCEPTED' THEN snap->>'revision' ELSE coalesce(touched->a::text->>'revision',raw->>'revision') END,'money',coalesce((touched->a::text->>'money')::boolean,false) OR outcome IN ('ACCEPTED','CONFLICT','OVERFLOW') OR cardinality(risk)>0));
   receipts=receipts||jsonb_build_array(jsonb_build_object('event_id',ev->>'event_id','receipt_id',receipt,'outcome',outcome,'duplicate',false));
  END LOOP;
 END LOOP;
 PERFORM core_funding.flush_facts_v1(frame);
 WITH closed AS (
  UPDATE public.gateway_requests q SET execution_closed_at=clock_timestamp()
  WHERE q.id=ANY(request_ids) AND q.execution_closed_at IS NULL AND q.state='SUCCEEDED' AND q.billing_outcome='SUCCESS'
   AND EXISTS(SELECT 1 FROM public.attempts t WHERE t.id=q.selected_attempt_id AND t.request_id=q.id AND t.state='SUCCEEDED')
   AND EXISTS(SELECT 1 FROM public.core_evidence_receipts r WHERE r.attempt_id=q.selected_attempt_id AND r.outcome='ACCEPTED' AND r.attempt_state='SUCCEEDED' AND r.request_state='SUCCEEDED')
  RETURNING q.id
 ) UPDATE public.execution_quota_leases l SET state='RELEASE_PENDING' WHERE l.request_id IN(SELECT id FROM closed) AND l.state IN('ADOPTION_PENDING','ACTIVE');
 FOR row IN SELECT key::uuid attempt,value FROM jsonb_each(touched) WHERE (value->>'money')::boolean ORDER BY key LOOP
  settled=core_funding.supplier_settle_v1(row.attempt,(row.value->>'canonical')::uuid,CASE WHEN row.value->>'canonical' IS NULL THEN 'risk:' ELSE 'cost:' END||row.attempt||':'||(row.value->>'revision'),pending);
  IF pending IS NOT NULL THEN pending=settled->'pending';END IF;
 END LOOP;
 pending=core_funding.flush_costs_v1(pending);
 request_ids=ARRAY(SELECT q.id FROM public.gateway_requests q WHERE q.id=ANY(request_ids) AND q.billing_outcome IN ('SUCCESS','CANCELLED','FAILED') AND NOT EXISTS(SELECT 1 FROM public.billing_charges c WHERE c.request_id=q.id) ORDER BY q.id);
 IF cardinality(request_ids)>0 THEN
  billing=public.settle_requests(request_ids);
  IF EXISTS(SELECT 1 FROM jsonb_each(billing) b WHERE b.value->>'state'='ERROR') THEN RAISE EXCEPTION 'customer_settlement_failed' USING ERRCODE='P0500';END IF;
 END IF;
 SET CONSTRAINTS usage_index_payload,core_receipt_conflict,usage_conflict_incoming_receipt IMMEDIATE;
 SET CONSTRAINTS usage_index_payload,core_receipt_conflict,usage_conflict_incoming_receipt DEFERRED;
 RETURN jsonb_build_object('result',jsonb_build_object('status','COMMITTED','receipts',receipts,'billing',billing),'pending',pending);
END $$;

CREATE OR REPLACE FUNCTION core_funding.persist_v1(p_groups jsonb,bulk boolean) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE g jsonb;pending jsonb=CASE WHEN bulk THEN '{"cells":{},"journals":[],"entries":[]}'::jsonb END;applied jsonb;results jsonb='[]';failure text;message text;batch uuid=gen_random_uuid();ordinal integer=0;reason text;ev jsonb;BEGIN
 IF jsonb_typeof(p_groups) IS DISTINCT FROM 'array' OR jsonb_array_length(p_groups) NOT BETWEEN 1 AND 128 THEN RAISE EXCEPTION 'evidence_batch_invalid' USING ERRCODE='P0400';END IF;
 IF bulk AND jsonb_array_length(p_groups)>1 THEN
  BEGIN
   applied=core_funding.coalesce_groups_v1(p_groups,pending);
   IF applied IS NOT NULL THEN
    PERFORM core_funding.flush_v1(applied->'pending');RETURN applied->'results';
   END IF;
  EXCEPTION WHEN OTHERS THEN pending='{"cells":{},"journals":[],"entries":[]}'::jsonb;results='[]';END;
 END IF;
 BEGIN
 FOR g IN SELECT value FROM jsonb_array_elements(p_groups) LOOP
  reason=NULL;
  IF jsonb_typeof(g) IS NOT DISTINCT FROM 'object' AND g?'rejected' THEN
   IF g->>'rejected' IS DISTINCT FROM 'evidence_signature_invalid' THEN RAISE EXCEPTION 'evidence_batch_invalid' USING ERRCODE='P0400';END IF;
   reason=g->>'rejected';results=results||jsonb_build_array(jsonb_build_object('status','REJECTED','error',reason));
  ELSE
   BEGIN
    applied=core_funding.evidence_group_v1(g,pending);
    results=results||jsonb_build_array(applied->'result');
    IF bulk THEN pending=applied->'pending';END IF;
   EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS failure=RETURNED_SQLSTATE,message=MESSAGE_TEXT;
    IF failure IN ('P0400','P0403') THEN reason=message;results=results||jsonb_build_array(jsonb_build_object('status','REJECTED','error',message));
    ELSE results=results||jsonb_build_array(jsonb_build_object('status','ERROR','error',failure));END IF;
   END;
  END IF;
  IF reason IS NOT NULL THEN
   FOR ev IN SELECT value FROM jsonb_array_elements(g->'events') LOOP
    INSERT INTO public.core_evidence_rejections(id,batch_id,ordinal,reason,origin_identity,origin_session_id,event_id,attempt_id)
    VALUES(gen_random_uuid(),batch,ordinal,reason,ev->>'origin_identity',(ev->>'origin_session_id')::uuid,(ev->>'event_id')::uuid,(ev->>'attempt_id')::uuid);
    ordinal=ordinal+1;
   END LOOP;
  END IF;
 END LOOP;
 IF bulk THEN PERFORM core_funding.flush_v1(pending);END IF;
 EXCEPTION WHEN OTHERS THEN
  IF NOT bulk THEN RAISE;END IF;
  RETURN core_funding.persist_v1(p_groups,false);
 END;
 RETURN results;
END $$;


DO $$ DECLARE owner name;BEGIN
 SELECT pg_get_userbyid(proowner) INTO owner FROM pg_proc WHERE oid='core_funding.persist_v1(jsonb,boolean)'::regprocedure;
 EXECUTE format('GRANT EXECUTE ON FUNCTION core_funding.fact_frame_v1(uuid[]),core_funding.stage_snapshot_v1(jsonb,uuid,text,text,text,jsonb,text[],uuid,uuid,numeric,boolean),core_funding.flush_facts_v1(jsonb),core_funding.flush_costs_v1(jsonb),core_funding.coalesce_groups_v1(jsonb,jsonb) TO %I',owner);
END $$;
REVOKE ALL ON FUNCTION core_funding.fact_frame_v1(uuid[]),core_funding.stage_snapshot_v1(jsonb,uuid,text,text,text,jsonb,text[],uuid,uuid,numeric,boolean),core_funding.flush_facts_v1(jsonb),core_funding.flush_costs_v1(jsonb),core_funding.coalesce_groups_v1(jsonb,jsonb) FROM PUBLIC;
