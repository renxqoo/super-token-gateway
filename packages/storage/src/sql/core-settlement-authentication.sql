REVOKE ALL ON public.core_evidence_rejections FROM PUBLIC;
CREATE TRIGGER immutable_core_rejections BEFORE DELETE OR UPDATE ON public.core_evidence_rejections FOR EACH ROW EXECUTE FUNCTION public.immutable_row();
CREATE OR REPLACE FUNCTION public.core_supplier_settle_v1(attempt uuid,snapshot uuid,reference text) RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE h public.upstream_holds%ROWTYPE;acct public.upstream_accounts%ROWTYPE;head public.upstream_costs%ROWTYPE;s public.usage_event_index%ROWTYPE;rates jsonb;usage jsonb;finals text[];x jsonb;
 old_cost numeric;old_reserve numeric;new_cost numeric;new_reserve numeric;cost_delta numeric;reserve_delta numeric;cost_rev numeric;journal uuid;frozen boolean;disputed boolean;faulted boolean;reserve_floor numeric=0;d record;dx jsonb;BEGIN
 SELECT * INTO h FROM public.upstream_holds WHERE attempt_id=attempt;
 IF h.attempt_id IS NULL OR h.estimated_quantities IS NULL OR h.state='RELEASED' OR reference !~ '^(cost|risk):[0-9a-f-]{36}:[0-9]{1,38}$' THEN RAISE EXCEPTION 'supplier_settlement_invalid' USING ERRCODE='P0409';END IF;
 SELECT * INTO acct FROM public.upstream_accounts WHERE id=h.account_id FOR NO KEY UPDATE;
 PERFORM 1 FROM public.upstream_account_balances WHERE account_id=h.account_id AND currency=h.currency FOR NO KEY UPDATE;
 SELECT * INTO h FROM public.upstream_holds WHERE attempt_id=attempt FOR UPDATE;
 SELECT v.rates INTO rates FROM public.cost_versions v WHERE v.id=h.cost_version_id;
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
 x=public.core_supplier_exposure_v1(rates,h.estimated_quantities,usage,finals,public.core_supplier_risk_v1(attempt));
 old_cost=coalesce(head.cost_atoms,0);
 FOR d IN SELECT i.final_dimensions,p.usage FROM public.usage_event_index i JOIN public.usage_payloads p ON p.id=i.id AND p.created_at=i.created_at WHERE i.attempt_id=attempt AND i.record_kind='DISPUTED' AND i.cost_atoms IS NOT NULL LOOP
  dx=public.core_supplier_exposure_v1(rates,h.estimated_quantities,d.usage,d.final_dimensions,'{}'::jsonb);
  reserve_floor=greatest(reserve_floor,(dx->>'cost_atoms')::numeric+(dx->>'reserve_atoms')::numeric-old_cost);
 END LOOP;
 SELECT coalesce(sum(e.atoms),0) INTO old_reserve FROM public.upstream_journals j JOIN public.upstream_entries e ON e.journal_id=j.id WHERE j.attempt_id=attempt AND e.account='upstream:reserved';
 new_cost=(x->>'cost_atoms')::numeric;new_reserve=greatest((x->>'reserve_atoms')::numeric,reserve_floor);
 cost_delta=new_cost-old_cost;reserve_delta=new_reserve-old_reserve;
 IF cost_delta<0 THEN RAISE EXCEPTION 'supplier_cost_regression' USING ERRCODE='P0409';END IF;
 IF snapshot IS NOT NULL AND NOT disputed AND NOT faulted THEN
  SELECT coalesce(max(c.revision),0)+1 INTO cost_rev FROM public.upstream_costs c WHERE c.attempt_id=attempt;
  INSERT INTO public.upstream_costs(attempt_id,revision,currency,cost_atoms,complete,disputed,evidence_digest,evidence_id,cost_version_id) VALUES(attempt,cost_rev,h.currency,new_cost,(x->>'complete')::boolean,false,public.core_digest_v1(jsonb_build_object('snapshot',s.snapshot_digest,'final_dimensions',to_jsonb(finals),'cost_version_id',h.cost_version_id,'estimated_quantities_digest',h.estimated_quantities_digest,'cost_atoms',new_cost::text)),snapshot,h.cost_version_id);
  INSERT INTO public.upstream_cost_heads(attempt_id,revision) VALUES(attempt,cost_rev) ON CONFLICT(attempt_id) DO UPDATE SET revision=excluded.revision;
 END IF;
 IF cost_delta<>0 OR reserve_delta<>0 THEN
  journal=gen_random_uuid();
  INSERT INTO public.upstream_journals(id,account_id,channel_id,attempt_id,currency,kind,reference) VALUES(journal,h.account_id,h.channel_id,attempt,h.currency,CASE WHEN cost_delta<>0 THEN 'COST' ELSE 'RISK' END,reference);
  INSERT INTO public.upstream_entries(journal_id,ordinal,currency,account,atoms)
   SELECT journal,(row_number() OVER (ORDER BY ord))-1,h.currency,account,atoms FROM (VALUES(0,'upstream:available',-(cost_delta+reserve_delta)),(1,'upstream:reserved',reserve_delta),(2,'upstream:expense',cost_delta)) leg(ord,account,atoms) WHERE atoms<>0;
  SET CONSTRAINTS balanced_upstream_journal,balanced_upstream_entries IMMEDIATE;
  SET CONSTRAINTS balanced_upstream_journal,balanced_upstream_entries DEFERRED;
  UPDATE public.upstream_account_balances SET balance_atoms=balance_atoms-cost_delta,spent_atoms=spent_atoms+cost_delta,reserved_atoms=reserved_atoms+reserve_delta,revision=revision+1 WHERE account_id=h.account_id AND currency=h.currency;
 END IF;
 UPDATE public.upstream_holds SET state=CASE WHEN (x->>'complete')::boolean AND new_reserve=0 AND NOT disputed AND NOT faulted THEN 'SETTLED' ELSE 'PARTIAL' END,final_cost_atoms=CASE WHEN (x->>'complete')::boolean AND new_reserve=0 AND NOT disputed AND NOT faulted THEN new_cost END WHERE attempt_id=attempt;
 frozen=(x->>'overrun')::boolean OR jsonb_array_length(x->'risk_dimensions')>0 OR disputed OR faulted;
 IF frozen THEN
  UPDATE public.upstream_accounts SET state='RECONCILE_REQUIRED',revision=revision+1 WHERE id=h.account_id AND state='ACTIVE';
  INSERT INTO public.upstream_audit_events(id,account_id,channel_id,actor,action,new_digest,reason) VALUES(gen_random_uuid(),h.account_id,h.channel_id,'core-settlement','RECONCILE_REQUIRED',public.core_digest_v1(jsonb_build_object('attempt',attempt,'reference',reference,'exposure',x)),CASE WHEN (x->>'overrun')::boolean THEN 'supplier_cost_overrun' WHEN jsonb_array_length(x->'risk_dimensions')>0 THEN 'supplier_partial_risk' WHEN faulted THEN 'supplier_cost_overflow' ELSE 'supplier_evidence_disputed' END);
 END IF;
 RETURN x||jsonb_build_object('cost_delta',cost_delta::text,'reserve_delta',reserve_delta::text,'reserve_atoms',new_reserve::text,'revision',cost_rev::text,'frozen',frozen);
END $$;
CREATE OR REPLACE FUNCTION public.core_evidence_group_v1(g jsonb) RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE ev jsonb;events jsonb[]='{}';n integer;i integer;pass integer;k text;attempt_ids uuid[];request_ids uuid[];permit public.execution_permits%ROWTYPE;h public.upstream_holds%ROWTYPE;
 prior public.core_evidence_receipts%ROWTYPE;rev_prior public.core_evidence_receipts%ROWTYPE;head record;cur public.usage_event_index%ROWTYPE;
 mode text[];contested boolean[];claims jsonb='{}';claim jsonb;d text;q text;owner integer;
 rates jsonb;prices jsonb;usage jsonb;finals text[];merged jsonb;merged_finals text[];raw jsonb;snap jsonb;x jsonb;outcome text;fault text;canonical uuid;disputed uuid;conflict uuid;receipt uuid;risk text[];stale boolean;event_claimed boolean;existing uuid;finality text;
 touched jsonb='{}';receipts jsonb='[]';billing jsonb='{}';settled jsonb;a uuid;row record;auth text[];sess public.internal_startup_sessions%ROWTYPE;revoked boolean;basis text;seen_final boolean;first_copy integer;BEGIN
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
 PERFORM 1 FROM public.upstream_accounts WHERE id IN(SELECT account_id FROM public.upstream_holds WHERE attempt_id=ANY(attempt_ids)) ORDER BY id FOR NO KEY UPDATE NOWAIT;
 PERFORM 1 FROM public.wallets WHERE id IN(SELECT wallet_id FROM public.billing_holds WHERE request_id=ANY(request_ids)) ORDER BY id FOR NO KEY UPDATE NOWAIT;
 PERFORM 1 FROM public.upstream_account_balances WHERE (account_id,currency) IN(SELECT account_id,currency FROM public.upstream_holds WHERE attempt_id=ANY(attempt_ids)) ORDER BY account_id,currency COLLATE "C" FOR NO KEY UPDATE NOWAIT;
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
   OR permit.owner_service||':'||permit.gateway_instance_id IS DISTINCT FROM ev->>'origin_identity' OR permit.owner_session_id IS DISTINCT FROM sess.session_id OR permit.incarnation_id IS DISTINCT FROM sess.incarnation_id OR permit.permit_epoch IS DISTINCT FROM (ev->>'permit_epoch')::numeric
   OR (permit.owner_key_sha256 IS NOT NULL AND permit.owner_key_sha256<>sess.public_key_sha256) OR (permit.certificate_sha256 IS NOT NULL AND permit.certificate_sha256<>sess.certificate_sha256)
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
  IF rev_prior.id IS NOT NULL OR EXISTS(SELECT 1 FROM generate_series(1,i-1) j WHERE events[j]->>'origin_identity'=ev->>'origin_identity' AND events[j]->>'attempt_id'=ev->>'attempt_id' AND events[j]->>'source'=ev->>'source' AND events[j]->>'source_revision'=ev->>'source_revision') THEN mode[i]='REPLAY';CONTINUE;END IF;
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
  SELECT min(j2) INTO first_copy FROM generate_series(1,i-1) j2 WHERE events[j2]->>'origin_identity'=events[i]->>'origin_identity' AND events[j2]->>'attempt_id'=events[i]->>'attempt_id' AND events[j2]->>'source'=events[i]->>'source' AND events[j2]->>'source_revision'=events[i]->>'source_revision' AND mode[j2]<>'DUPLICATE';
  IF first_copy IS NOT NULL AND (contested[first_copy] OR mode[first_copy]='QUARANTINED') THEN contested[i]=contested[first_copy];mode[i]='NEW';END IF;
 END LOOP;
 FOR pass IN 1..2 LOOP
  FOR i IN 1..n LOOP
   ev=events[i];a=(ev->>'attempt_id')::uuid;
   CONTINUE WHEN (pass=1)=(contested[i] OR mode[i]='DUPLICATE');
   IF mode[i]='DUPLICATE' THEN
    SELECT * INTO prior FROM public.core_evidence_receipts r WHERE r.origin_identity=ev->>'origin_identity' AND r.event_id=(ev->>'event_id')::uuid AND r.body_digest=ev->>'body_digest';
    IF prior.signature_payload_digest<>ev->>'signature_payload_digest' OR prior.producer_key_sha256<>ev->>'producer_key_sha256' THEN RAISE EXCEPTION 'evidence_subject_invalid' USING ERRCODE='P0403';END IF;
    receipts=receipts||jsonb_build_array(jsonb_build_object('event_id',ev->>'event_id','receipt_id',prior.id,'outcome',prior.outcome,'duplicate',true));CONTINUE;
   END IF;
   SELECT * INTO h FROM public.upstream_holds WHERE attempt_id=a;
   SELECT v.rates INTO rates FROM public.cost_versions v WHERE v.id=h.cost_version_id;
   SELECT p.basis,coalesce(p.rates,'{}') INTO basis,prices FROM public.attempts t LEFT JOIN public.billing_prices p ON p.id=t.billing_pricing_id WHERE t.id=a;
   SELECT * INTO cur FROM public.usage_event_index WHERE attempt_id=a AND record_kind='CANONICAL' ORDER BY snapshot_revision DESC LIMIT 1;
   SELECT p.usage INTO merged FROM public.usage_payloads p WHERE p.id=cur.id AND p.created_at=cur.created_at;
   merged=coalesce(merged,'{}');merged_finals=coalesce(cur.final_dimensions,'{}');
   usage=ev->'normalized_usage';finals=ARRAY(SELECT jsonb_array_elements_text(ev->'normalized_finals'));
   event_claimed=EXISTS(SELECT 1 FROM public.core_evidence_receipts r WHERE r.origin_identity=ev->>'origin_identity' AND r.event_id=(ev->>'event_id')::uuid AND r.event_claim);
   receipt=gen_random_uuid();canonical=cur.id;disputed=NULL;conflict=NULL;fault=NULL;risk='{}';
   IF mode[i]='REPLAY' THEN
    SELECT * INTO rev_prior FROM public.core_evidence_receipts r WHERE r.attempt_id=a AND r.origin_identity=ev->>'origin_identity' AND r.source=ev->>'source' AND r.source_revision=(ev->>'source_revision')::numeric AND r.revision_claim;
    IF rev_prior.id IS NULL OR rev_prior.body_digest<>ev->>'body_digest' THEN RAISE EXCEPTION 'evidence_replay_unanchored' USING ERRCODE='P0503';END IF;
    INSERT INTO public.core_evidence_receipts(id,origin_identity,origin_incarnation,origin_session_id,event_id,attempt_id,source,source_revision,body_digest,event_claim,revision_claim,outcome,finality,attempt_state,request_state,error_code,provider_request_id,raw_evidence_id,canonical_evidence_id,risk_dimensions,authentication_disposition,authentication_reason,producer_key_sha256,signature_version,signature,signature_payload_digest)
    VALUES(receipt,ev->>'origin_identity',(ev->>'origin_incarnation')::uuid,(ev->>'origin_session_id')::uuid,(ev->>'event_id')::uuid,a,ev->>'source',(ev->>'source_revision')::numeric,ev->>'body_digest',NOT event_claimed,false,'REVISION_REPLAY',ev->>'finality',ev->>'attempt_state',ev->>'request_state',ev->>'error_code',ev->>'provider_request_id',rev_prior.raw_evidence_id,rev_prior.canonical_evidence_id,'{}','VERIFIED','NORMAL',ev->>'producer_key_sha256',1,decode(ev->>'signature','hex'),ev->>'signature_payload_digest');
    receipts=receipts||jsonb_build_array(jsonb_build_object('event_id',ev->>'event_id','receipt_id',receipt,'outcome','REVISION_REPLAY','duplicate',false));CONTINUE;
   END IF;
   finality=CASE ev->>'finality' WHEN 'NOT_EXECUTED' THEN 'FINAL' ELSE ev->>'finality' END;
   raw=public.core_evidence_snapshot_v1(a,'RAW',ev->>'source',finality,usage,finals,NULL,NULL,NULL,false);
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
    BEGIN x=public.core_supplier_exposure_v1(rates,h.estimated_quantities,merged,merged_finals,'{}');
     IF greatest((x->>'cost_atoms')::numeric,(x->>'cost_atoms')::numeric+(x->>'reserve_atoms')::numeric)>99999999999999999999999999999999999999 THEN x=NULL;fault='COST_OVERFLOW';END IF;
    EXCEPTION WHEN numeric_value_out_of_range THEN x=NULL;fault='COST_OVERFLOW';
     WHEN raise_exception THEN IF SQLERRM IS DISTINCT FROM 'price exceeds amount range' THEN RAISE;END IF;x=NULL;fault='COST_OVERFLOW';END;
    snap=public.core_evidence_snapshot_v1(a,'DISPUTED',ev->>'source',finality,merged,merged_finals,cur.id,(raw->>'id')::uuid,(x->>'cost_atoms')::numeric,coalesce((x->>'complete')::boolean,false));
    disputed=(snap->>'id')::uuid;conflict=gen_random_uuid();
    INSERT INTO public.usage_conflicts(id,attempt_id,existing_event_id,source,finality,usage,fingerprint,incoming_evidence_id,disputed_evidence_id,incoming_receipt_id)
    VALUES(conflict,a,existing,ev->>'source',finality,usage,public.core_digest_v1(jsonb_build_object('attempt_id',a,'existing',existing,'incoming',raw->>'id','usage',usage,'final_dimensions',to_jsonb(finals))),(raw->>'id')::uuid,disputed,receipt);
    UPDATE public.attempts SET metering_conflict=true WHERE id=a;
   ELSIF ev->>'finality'='ESTIMATED' THEN outcome='ESTIMATED';risk='{}';
   ELSE
    stale=EXISTS(SELECT 1 FROM public.core_evidence_receipts r WHERE r.attempt_id=a AND r.origin_identity=ev->>'origin_identity' AND r.source=ev->>'source' AND r.outcome='ACCEPTED' AND r.source_revision>(ev->>'source_revision')::numeric);
    IF stale THEN outcome='STALE';
    ELSE
     merged=merged||(usage-merged_finals);merged_finals=ARRAY(SELECT DISTINCT unnest(merged_finals||finals) ORDER BY 1);
     BEGIN x=public.core_supplier_exposure_v1(rates,h.estimated_quantities,merged,merged_finals,'{}');
      IF greatest((x->>'cost_atoms')::numeric,(x->>'cost_atoms')::numeric+(x->>'reserve_atoms')::numeric)>99999999999999999999999999999999999999 THEN x=NULL;fault='COST_OVERFLOW';END IF;
     EXCEPTION WHEN numeric_value_out_of_range THEN x=NULL;fault='COST_OVERFLOW';
      WHEN raise_exception THEN IF SQLERRM IS DISTINCT FROM 'price exceeds amount range' THEN RAISE;END IF;x=NULL;fault='COST_OVERFLOW';END;
     IF x IS NULL THEN
      outcome='OVERFLOW';snap=public.core_evidence_snapshot_v1(a,'DISPUTED',ev->>'source',finality,merged,merged_finals,cur.id,(raw->>'id')::uuid,NULL,false);disputed=(snap->>'id')::uuid;
     ELSE
      outcome='ACCEPTED';
      seen_final=finality='FINAL' OR EXISTS(SELECT 1 FROM public.core_evidence_receipts r WHERE r.attempt_id=a AND r.outcome='ACCEPTED' AND r.finality IN ('FINAL','NOT_EXECUTED'));
      snap=public.core_evidence_snapshot_v1(a,'CANONICAL',ev->>'source',CASE WHEN seen_final AND (basis IS DISTINCT FROM 'PROVIDER_USAGE' OR NOT EXISTS(SELECT 1 FROM jsonb_object_keys(prices) p2 WHERE NOT p2=ANY(merged_finals))) THEN 'FINAL' WHEN cardinality(merged_finals)>0 OR merged<>'{}'::jsonb THEN 'PARTIAL' ELSE 'UNKNOWN' END,merged,merged_finals,cur.id,(raw->>'id')::uuid,(x->>'cost_atoms')::numeric,(x->>'complete')::boolean);
      canonical=(snap->>'id')::uuid;
      UPDATE public.attempts SET evidence_id=canonical,state=CASE WHEN state IN ('SUCCEEDED','FAILED','NOT_EXECUTED') OR ev->>'attempt_state' IS NULL THEN state ELSE ev->>'attempt_state' END,
       finished_at=CASE WHEN ev->>'attempt_state' IN ('SUCCEEDED','FAILED','NOT_EXECUTED') THEN coalesce(finished_at,clock_timestamp()) ELSE finished_at END,error_code=coalesce(ev->>'error_code',error_code),provider_request_id=coalesce(ev->>'provider_request_id',provider_request_id) WHERE id=a;
      UPDATE public.gateway_requests SET selected_attempt_id=CASE WHEN ev->>'request_state' IN ('SUCCEEDED','CANCELLED') THEN a END,
       state=ev->>'request_state',billing_outcome=CASE ev->>'request_state' WHEN 'SUCCEEDED' THEN 'SUCCESS' ELSE ev->>'request_state' END,updated_at=clock_timestamp()
       WHERE id=(SELECT request_id FROM public.attempts WHERE id=a) AND ev->>'request_state' IN ('SUCCEEDED','CANCELLED','FAILED') AND selected_attempt_id IS NULL AND billing_outcome IN ('PENDING','UNKNOWN');
     END IF;
    END IF;
   END IF;
   INSERT INTO public.core_evidence_receipts(id,origin_identity,origin_incarnation,origin_session_id,event_id,attempt_id,source,source_revision,body_digest,event_claim,revision_claim,outcome,finality,attempt_state,request_state,error_code,provider_request_id,raw_evidence_id,canonical_evidence_id,disputed_evidence_id,conflict_id,risk_dimensions,numeric_fault,authentication_disposition,authentication_reason,producer_key_sha256,signature_version,signature,signature_payload_digest)
   VALUES(receipt,ev->>'origin_identity',(ev->>'origin_incarnation')::uuid,(ev->>'origin_session_id')::uuid,(ev->>'event_id')::uuid,a,ev->>'source',(ev->>'source_revision')::numeric,ev->>'body_digest',NOT event_claimed,outcome<>'CONFLICT',outcome,ev->>'finality',ev->>'attempt_state',ev->>'request_state',ev->>'error_code',ev->>'provider_request_id',(raw->>'id')::uuid,canonical,disputed,conflict,risk,fault,'VERIFIED','NORMAL',ev->>'producer_key_sha256',1,decode(ev->>'signature','hex'),ev->>'signature_payload_digest');
   touched=jsonb_set(touched,ARRAY[a::text],jsonb_build_object('canonical',CASE WHEN outcome='ACCEPTED' THEN to_jsonb(canonical) ELSE coalesce(touched->a::text->'canonical','null'::jsonb) END,'revision',CASE WHEN outcome='ACCEPTED' THEN snap->>'revision' ELSE coalesce(touched->a::text->>'revision',raw->>'revision') END,'money',coalesce((touched->a::text->>'money')::boolean,false) OR outcome IN ('ACCEPTED','CONFLICT','OVERFLOW') OR cardinality(risk)>0));
   receipts=receipts||jsonb_build_array(jsonb_build_object('event_id',ev->>'event_id','receipt_id',receipt,'outcome',outcome,'duplicate',false));
  END LOOP;
 END LOOP;
 FOR row IN SELECT key::uuid attempt,value FROM jsonb_each(touched) WHERE (value->>'money')::boolean ORDER BY key LOOP
  PERFORM public.core_supplier_settle_v1(row.attempt,(row.value->>'canonical')::uuid,CASE WHEN row.value->>'canonical' IS NULL THEN 'risk:' ELSE 'cost:' END||row.attempt||':'||(row.value->>'revision'));
 END LOOP;
 request_ids=ARRAY(SELECT q.id FROM public.gateway_requests q WHERE q.id=ANY(request_ids) AND q.billing_outcome IN ('SUCCESS','CANCELLED','FAILED') AND NOT EXISTS(SELECT 1 FROM public.billing_charges c WHERE c.request_id=q.id) ORDER BY q.id);
 IF cardinality(request_ids)>0 THEN
  billing=public.settle_requests(request_ids);
  IF EXISTS(SELECT 1 FROM jsonb_each(billing) b WHERE b.value->>'state'='ERROR') THEN RAISE EXCEPTION 'customer_settlement_failed' USING ERRCODE='P0500';END IF;
 END IF;
 SET CONSTRAINTS usage_index_payload,core_receipt_conflict,usage_conflict_incoming_receipt,balanced_upstream_journal,balanced_upstream_entries IMMEDIATE;
 SET CONSTRAINTS usage_index_payload,core_receipt_conflict,usage_conflict_incoming_receipt,balanced_upstream_journal,balanced_upstream_entries DEFERRED;
 RETURN jsonb_build_object('status','COMMITTED','receipts',receipts,'billing',billing);
END $$;
CREATE OR REPLACE FUNCTION public.persist_core_evidence_groups_v1(p_groups jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE g jsonb;results jsonb='[]';failure text;message text;batch uuid=gen_random_uuid();ordinal integer=0;reason text;ev jsonb;BEGIN
 IF jsonb_typeof(p_groups) IS DISTINCT FROM 'array' OR jsonb_array_length(p_groups) NOT BETWEEN 1 AND 128 THEN RAISE EXCEPTION 'evidence_batch_invalid' USING ERRCODE='P0400';END IF;
 FOR g IN SELECT value FROM jsonb_array_elements(p_groups) LOOP
  reason=NULL;
  IF jsonb_typeof(g) IS NOT DISTINCT FROM 'object' AND g?'rejected' THEN
   IF g->>'rejected' IS DISTINCT FROM 'evidence_signature_invalid' THEN RAISE EXCEPTION 'evidence_batch_invalid' USING ERRCODE='P0400';END IF;
   reason=g->>'rejected';results=results||jsonb_build_array(jsonb_build_object('status','REJECTED','error',reason));
  ELSE
   BEGIN
    results=results||jsonb_build_array(public.core_evidence_group_v1(g));
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
 RETURN results;
END $$;
