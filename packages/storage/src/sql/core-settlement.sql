ALTER TABLE public.usage_conflicts ADD CONSTRAINT usage_conflict_incoming_receipt FOREIGN KEY(incoming_receipt_id) REFERENCES public.core_evidence_receipts(id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE public.core_evidence_receipts ADD CONSTRAINT core_receipt_conflict FOREIGN KEY(conflict_id) REFERENCES public.usage_conflicts(id) DEFERRABLE INITIALLY DEFERRED;
CREATE TRIGGER immutable_core_receipts BEFORE DELETE OR UPDATE ON public.core_evidence_receipts FOR EACH ROW EXECUTE FUNCTION public.immutable_row();
CREATE FUNCTION public.validate_evidence_lineage() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE core boolean;parent public.usage_event_index%ROWTYPE;fact public.usage_event_index%ROWTYPE;BEGIN
 core=EXISTS(SELECT 1 FROM public.execution_permits WHERE attempt_id=NEW.attempt_id);
 IF NEW.record_kind='GATEWAY' THEN
  IF core THEN RAISE EXCEPTION 'core_attempt_requires_signed_evidence' USING ERRCODE='P0409';END IF;
  RETURN NEW;
 END IF;
 IF NOT core THEN RAISE EXCEPTION 'signed_evidence_requires_core_attempt' USING ERRCODE='P0409';END IF;
 IF NEW.source NOT IN ('execution','execution_outcome','recovery_interruption','async_partial','async_final','batch_completed','batch_cancelled','batch_failed','provider_invoice')
  OR cardinality(NEW.final_dimensions)>15 OR EXISTS(SELECT 1 FROM unnest(NEW.final_dimensions) d GROUP BY d HAVING count(*)>1)
  OR EXISTS(SELECT 1 FROM unnest(NEW.final_dimensions) d WHERE d IS NULL OR d NOT IN ('input_tokens','output_tokens','cached_input_tokens','cache_write_tokens','cache_write_5m_tokens','cache_write_1h_tokens','reasoning_tokens','requests','images','audio_ms','music_ms','video_ms','audio_seconds','video_seconds','characters'))
 THEN RAISE EXCEPTION 'evidence_snapshot_invalid' USING ERRCODE='P0400';END IF;
 IF NEW.record_kind='RAW' THEN RETURN NEW;END IF;
 SELECT * INTO fact FROM public.usage_event_index WHERE id=NEW.input_fact_id AND attempt_id=NEW.attempt_id;
 IF fact.id IS NULL OR fact.record_kind<>'RAW' OR fact.snapshot_revision>=NEW.snapshot_revision THEN RAISE EXCEPTION 'evidence_lineage_invalid' USING ERRCODE='P0409';END IF;
 IF NEW.parent_evidence_id IS NOT NULL THEN
  SELECT * INTO parent FROM public.usage_event_index WHERE id=NEW.parent_evidence_id AND attempt_id=NEW.attempt_id;
  IF parent.id IS NULL OR parent.record_kind<>'CANONICAL' OR parent.snapshot_revision>=NEW.snapshot_revision THEN RAISE EXCEPTION 'evidence_lineage_invalid' USING ERRCODE='P0409';END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER evidence_lineage BEFORE INSERT ON public.usage_event_index FOR EACH ROW EXECUTE FUNCTION public.validate_evidence_lineage();
REVOKE ALL ON FUNCTION public.validate_evidence_lineage() FROM PUBLIC;
REVOKE ALL ON public.core_evidence_receipts FROM PUBLIC;
CREATE OR REPLACE FUNCTION public.record_usage_evidence(p_attempt uuid, p_event uuid, p_source text, p_state text, p_finality text, p_usage jsonb, p_cost numeric, p_complete boolean, p_error text, p_provider_request text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog', 'public', 'pg_temp'
AS $function$
DECLARE a record; prior usage_events%ROWTYPE; original usage_events%ROWTYPE; conflicting boolean; chosen boolean;
BEGIN
 IF jsonb_typeof(p_usage)<>'object' OR EXISTS(SELECT 1 FROM jsonb_each(p_usage) j WHERE jsonb_typeof(j.value)<>'string' OR j.key NOT IN ('input_tokens','output_tokens','cached_input_tokens','cache_write_tokens','cache_write_5m_tokens','cache_write_1h_tokens','reasoning_tokens','requests','images','audio_ms','music_ms','video_ms','audio_seconds','video_seconds','characters') OR
 (j.key IN ('audio_seconds','video_seconds') AND j.value#>>'{}' !~ '^(0|[1-9][0-9]{0,37})(\.[0-9]{1,18})?$') OR
 (j.key NOT IN ('audio_seconds','video_seconds') AND j.value#>>'{}' !~ '^(0|[1-9][0-9]{0,37})$')) THEN RAISE EXCEPTION 'invalid usage evidence'; END IF;
 IF p_usage?&ARRAY['cache_write_tokens','cache_write_5m_tokens','cache_write_1h_tokens'] AND (p_usage->>'cache_write_tokens')::numeric<>(p_usage->>'cache_write_5m_tokens')::numeric+(p_usage->>'cache_write_1h_tokens')::numeric THEN RAISE EXCEPTION 'cache write partitions disagree';END IF;
 SELECT attempts.*,q.project_id,v.currency INTO a FROM attempts JOIN gateway_requests q ON q.id=attempts.request_id JOIN cost_versions v ON v.id=attempts.pricing_id WHERE attempts.id=p_attempt FOR NO KEY UPDATE OF attempts;
 IF NOT FOUND THEN RAISE EXCEPTION 'unknown attempt'; END IF;
 IF a.evidence_id IS NOT NULL THEN SELECT * INTO prior FROM usage_events WHERE id=a.evidence_id;ELSE prior=NULL;END IF;
 SELECT * INTO original FROM usage_events WHERE attempt_id=p_attempt AND source=p_source;
 IF original.id IS NOT NULL AND original.finality=p_finality AND NOT EXISTS(SELECT 1 FROM jsonb_each_text(p_usage)n LEFT JOIN jsonb_each_text(original.usage)o USING(key) WHERE o.key IS NULL OR n.value::numeric<>o.value::numeric) THEN
  RETURN jsonb_build_object('status','REPLAY','evidence_id',a.evidence_id);
 END IF;
 conflicting=prior.id IS NOT NULL AND ((prior.finality='FINAL' AND p_finality='FINAL') OR original.id IS NOT NULL) AND EXISTS(
 SELECT 1 FROM jsonb_each_text(p_usage)n JOIN jsonb_each_text(CASE WHEN original.id IS NOT NULL THEN original.usage ELSE prior.usage END)o USING(key) WHERE n.value::numeric<>o.value::numeric);
 IF conflicting THEN
  INSERT INTO usage_conflicts(id,attempt_id,existing_event_id,source,finality,usage,fingerprint) VALUES(p_event,p_attempt,coalesce(original.id,prior.id),p_source,p_finality,p_usage,encode(sha256(convert_to(p_usage::text,'UTF8')),'hex')) ON CONFLICT DO NOTHING;
  UPDATE attempts SET metering_conflict=true WHERE id=p_attempt;
  RETURN jsonb_build_object('status','CONFLICT');
 END IF;
 IF p_source='recovery_interruption' AND a.state<>'DISPATCHING' THEN RETURN jsonb_build_object('status','REPLAY'); END IF;
 chosen=prior.id IS NULL OR (p_finality='FINAL' AND prior.finality<>'FINAL') OR p_finality=prior.finality OR (p_finality='PARTIAL' AND prior.finality IN ('UNKNOWN','ESTIMATED'));
 IF chosen AND prior.id IS NOT NULL AND EXISTS(SELECT 1 FROM jsonb_object_keys(prior.usage) k WHERE NOT p_usage?k) AND NOT EXISTS(SELECT 1 FROM jsonb_each_text(p_usage)n JOIN jsonb_each_text(prior.usage)o USING(key) WHERE n.value::numeric<>o.value::numeric) THEN
  RETURN jsonb_build_object('status','MERGE_REQUIRED','usage',prior.usage||p_usage);
 END IF;
 IF original.id IS NOT NULL THEN p_source=p_source||'_'||substr(encode(sha256(convert_to(p_usage::text||p_finality,'UTF8')),'hex'),1,24); END IF;
 PERFORM pg_advisory_xact_lock_shared(81442592);
 INSERT INTO usage_event_index(id,project_id,request_id,attempt_id,source,finality,cost_atoms,currency,cost_complete)
 VALUES(p_event,a.project_id,a.request_id,p_attempt,p_source,p_finality,p_cost,a.currency,p_complete) ON CONFLICT(attempt_id,source) WHERE record_kind='GATEWAY' DO NOTHING;
 IF NOT FOUND THEN RETURN jsonb_build_object('status','REPLAY'); END IF;
 INSERT INTO usage_payloads(id,created_at,usage) SELECT id,created_at,p_usage FROM usage_event_index WHERE id=p_event;
 INSERT INTO event_deliveries(consumer_id,event_id) SELECT id,p_event FROM event_consumers;
 UPDATE attempts SET evidence_id=CASE WHEN chosen THEN p_event ELSE evidence_id END,
 state=CASE WHEN chosen THEN p_state ELSE state END,finished_at=now(),error_code=p_error,provider_request_id=coalesce(p_provider_request,provider_request_id) WHERE id=p_attempt;

 RETURN jsonb_build_object('status','APPLIED','evidence_id',CASE WHEN chosen THEN p_event ELSE a.evidence_id END);
END $function$;

CREATE FUNCTION public.core_supplier_exposure_v1(rates jsonb,estimated jsonb,usage jsonb,finals text[],risk jsonb) RETURNS jsonb LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE d text;known jsonb='{}';exposure jsonb='{}';base jsonb='{}';e numeric;q numeric;r numeric;cost numeric;total numeric;base_total numeric;ceiling numeric;complete boolean=true;overrun boolean=false;risky text[]='{}';BEGIN
 IF jsonb_typeof(rates) IS DISTINCT FROM 'object' OR jsonb_typeof(estimated) IS DISTINCT FROM 'object' OR jsonb_typeof(usage) IS DISTINCT FROM 'object' OR finals IS NULL OR jsonb_typeof(risk) IS DISTINCT FROM 'object' THEN RAISE EXCEPTION 'supplier_exposure_invalid' USING ERRCODE='P0422';END IF;
 FOR d IN SELECT key FROM jsonb_each(rates) ORDER BY key LOOP
  IF estimated->>d IS NULL THEN RAISE EXCEPTION 'supplier_estimate_missing' USING ERRCODE='P0422';END IF;
  e=(estimated->>d)::numeric;
  IF d=ANY(finals) THEN
   IF usage->>d IS NULL THEN RAISE EXCEPTION 'supplier_final_quantity_missing' USING ERRCODE='P0422';END IF;
   q=(usage->>d)::numeric;
   known=known||jsonb_build_object(d,usage->>d);exposure=exposure||jsonb_build_object(d,usage->>d);base=base||jsonb_build_object(d,usage->>d);
   IF q>e THEN overrun=true;END IF;
  ELSE
   complete=false;r=greatest(e,coalesce((risk->>d)::numeric,0));
   IF r>e THEN risky=risky||d;END IF;
   exposure=exposure||jsonb_build_object(d,CASE WHEN r>e THEN risk->>d ELSE estimated->>d END);base=base||jsonb_build_object(d,estimated->>d);
  END IF;
 END LOOP;
 cost=public.exact_billing_price(known,rates);total=public.exact_billing_price(exposure,rates);base_total=public.exact_billing_price(base,rates);ceiling=public.exact_billing_price(estimated,rates);
 IF base_total>ceiling THEN overrun=true;END IF;
 RETURN jsonb_build_object('cost_atoms',cost::text,'reserve_atoms',(total-cost)::text,'base_reserve_atoms',(base_total-cost)::text,'complete',complete,'overrun',overrun,'risk_dimensions',to_jsonb(risky));
END $$;
CREATE FUNCTION public.core_supplier_risk_v1(attempt uuid) RETURNS jsonb LANGUAGE sql STABLE SET search_path=pg_catalog,public,pg_temp AS $$
 SELECT coalesce(jsonb_object_agg(dimension,quantity::text),'{}'::jsonb) FROM (
  SELECT d dimension,max((p.usage->>d)::numeric) quantity FROM public.core_evidence_receipts r
  CROSS JOIN LATERAL unnest(r.risk_dimensions) d
  JOIN public.usage_event_index i ON i.id=r.raw_evidence_id
  JOIN public.usage_payloads p ON p.id=i.id AND p.created_at=i.created_at
  WHERE r.attempt_id=attempt AND p.usage ? d GROUP BY d) observed
$$;
CREATE FUNCTION public.core_supplier_settle_v1(attempt uuid,snapshot uuid,reference text) RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
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
 UPDATE public.upstream_holds SET state=CASE WHEN (x->>'complete')::boolean AND new_reserve=0 THEN 'SETTLED' ELSE 'PARTIAL' END,final_cost_atoms=CASE WHEN (x->>'complete')::boolean AND new_reserve=0 THEN new_cost END WHERE attempt_id=attempt;
 frozen=(x->>'overrun')::boolean OR jsonb_array_length(x->'risk_dimensions')>0 OR disputed OR faulted;
 IF frozen THEN
  UPDATE public.upstream_accounts SET state='RECONCILE_REQUIRED',revision=revision+1 WHERE id=h.account_id AND state='ACTIVE';
  INSERT INTO public.upstream_audit_events(id,account_id,channel_id,actor,action,new_digest,reason) VALUES(gen_random_uuid(),h.account_id,h.channel_id,'core-settlement','RECONCILE_REQUIRED',public.core_digest_v1(jsonb_build_object('attempt',attempt,'reference',reference,'exposure',x)),CASE WHEN (x->>'overrun')::boolean THEN 'supplier_cost_overrun' WHEN jsonb_array_length(x->'risk_dimensions')>0 THEN 'supplier_partial_risk' WHEN faulted THEN 'supplier_cost_overflow' ELSE 'supplier_evidence_disputed' END);
 END IF;
 RETURN x||jsonb_build_object('cost_delta',cost_delta::text,'reserve_delta',reserve_delta::text,'reserve_atoms',new_reserve::text,'revision',cost_rev::text,'frozen',frozen);
END $$;
REVOKE ALL ON FUNCTION public.core_supplier_exposure_v1(jsonb,jsonb,jsonb,text[],jsonb),public.core_supplier_risk_v1(uuid),public.core_supplier_settle_v1(uuid,uuid,text) FROM PUBLIC;
CREATE FUNCTION public.core_evidence_validate_v1(ev jsonb) RETURNS void LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE u constant text='^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';d text;f text;BEGIN
 IF jsonb_typeof(ev) IS DISTINCT FROM 'object' OR ev-ARRAY['event_id','attempt_id','origin_identity','origin_incarnation','origin_session_id','permit_epoch','source','source_revision','finality','usage','final_dimensions','attempt_state','request_state','error_code','provider_request_id']<>'{}'::jsonb
  OR ev->>'event_id' !~ u OR ev->>'attempt_id' !~ u OR ev->>'origin_incarnation' !~ u OR ev->>'origin_session_id' !~ u
  OR jsonb_typeof(ev->'origin_identity') IS DISTINCT FROM 'string' OR ev->>'origin_identity' !~ '^gateway:[A-Za-z0-9_-]{1,100}$'
  OR jsonb_typeof(ev->'permit_epoch') IS DISTINCT FROM 'string' OR ev->>'permit_epoch' !~ '^[1-9][0-9]{0,37}$'
  OR jsonb_typeof(ev->'source_revision') IS DISTINCT FROM 'string' OR ev->>'source_revision' !~ '^[1-9][0-9]{0,37}$'
  OR jsonb_typeof(ev->'source') IS DISTINCT FROM 'string' OR ev->>'source' NOT IN ('execution','execution_outcome','recovery_interruption','async_partial','async_final','batch_completed','batch_cancelled','batch_failed','provider_invoice')
  OR jsonb_typeof(ev->'finality') IS DISTINCT FROM 'string' OR ev->>'finality' NOT IN ('FINAL','PARTIAL','UNKNOWN','ESTIMATED','NOT_EXECUTED')
  OR jsonb_typeof(ev->'usage') IS DISTINCT FROM 'object' OR jsonb_typeof(ev->'final_dimensions') IS DISTINCT FROM 'array' OR jsonb_array_length(ev->'final_dimensions')>15
  OR (ev?'attempt_state' AND (jsonb_typeof(ev->'attempt_state') IS DISTINCT FROM 'string' OR ev->>'attempt_state' NOT IN ('SUCCEEDED','FAILED','UNKNOWN','PARTIAL','NOT_EXECUTED')))
  OR (ev?'request_state' AND (jsonb_typeof(ev->'request_state') IS DISTINCT FROM 'string' OR ev->>'request_state' NOT IN ('SUCCEEDED','FAILED','CANCELLED','UNKNOWN')))
  OR (ev?'error_code' AND (jsonb_typeof(ev->'error_code') IS DISTINCT FROM 'string' OR ev->>'error_code' !~ '^[a-z][a-z0-9_]{0,79}$'))
  OR (ev?'provider_request_id' AND (jsonb_typeof(ev->'provider_request_id') IS DISTINCT FROM 'string' OR ev->>'provider_request_id' !~ '^[A-Za-z0-9._:/-]{1,256}$'))
 THEN RAISE EXCEPTION 'evidence_event_invalid' USING ERRCODE='P0400';END IF;
 FOR d IN SELECT key FROM jsonb_each(ev->'usage') LOOP
  IF jsonb_typeof(ev->'usage'->d)<>'string' OR d NOT IN ('input_tokens','output_tokens','cached_input_tokens','cache_write_tokens','cache_write_5m_tokens','cache_write_1h_tokens','reasoning_tokens','requests','images','audio_ms','music_ms','video_ms','audio_seconds','video_seconds','characters')
   OR (d IN ('audio_seconds','video_seconds') AND ev->'usage'->>d !~ '^(0|[1-9][0-9]{0,37})(\.[0-9]{1,18})?$') OR (d NOT IN ('audio_seconds','video_seconds') AND ev->'usage'->>d !~ '^(0|[1-9][0-9]{0,37})$') THEN RAISE EXCEPTION 'evidence_event_invalid' USING ERRCODE='P0400';END IF;
 END LOOP;
 IF ev->'usage'?&ARRAY['cache_write_tokens','cache_write_5m_tokens','cache_write_1h_tokens'] AND (ev->'usage'->>'cache_write_tokens')::numeric<>(ev->'usage'->>'cache_write_5m_tokens')::numeric+(ev->'usage'->>'cache_write_1h_tokens')::numeric THEN RAISE EXCEPTION 'evidence_event_invalid' USING ERRCODE='P0400';END IF;
 FOR f IN SELECT value FROM jsonb_array_elements_text(ev->'final_dimensions') LOOP
  IF NOT ev->'usage'?f THEN RAISE EXCEPTION 'evidence_event_invalid' USING ERRCODE='P0400';END IF;
 END LOOP;
 IF (SELECT count(DISTINCT value)<>count(*) FROM jsonb_array_elements_text(ev->'final_dimensions'))
  OR (ev->>'finality' IN ('UNKNOWN','ESTIMATED') AND jsonb_array_length(ev->'final_dimensions')>0)
  OR (ev->>'finality'='NOT_EXECUTED' AND (ev->'usage'<>'{}'::jsonb OR jsonb_array_length(ev->'final_dimensions')>0 OR coalesce(ev->>'attempt_state','NOT_EXECUTED')<>'NOT_EXECUTED'))
 THEN RAISE EXCEPTION 'evidence_event_invalid' USING ERRCODE='P0400';END IF;
END $$;
CREATE FUNCTION public.core_evidence_snapshot_v1(attempt uuid,kind text,source text,finality text,usage jsonb,finals text[],parent uuid,input uuid,cost numeric,complete boolean) RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE sid uuid=gen_random_uuid();rev numeric;a record;BEGIN
 SELECT q.project_id,q.id request_id,v.currency INTO a FROM public.attempts t JOIN public.gateway_requests q ON q.id=t.request_id JOIN public.upstream_holds h ON h.attempt_id=t.id JOIN public.cost_versions v ON v.id=h.cost_version_id WHERE t.id=attempt;
 SELECT coalesce(max(snapshot_revision),0)+1 INTO rev FROM public.usage_event_index WHERE attempt_id=attempt AND snapshot_revision IS NOT NULL;
 INSERT INTO public.usage_event_index(id,project_id,request_id,attempt_id,source,finality,cost_atoms,currency,cost_complete,record_kind,snapshot_revision,snapshot_digest,final_dimensions,parent_evidence_id,input_fact_id)
 VALUES(sid,a.project_id,a.request_id,attempt,source,finality,cost,a.currency,coalesce(complete,false),kind,rev,public.core_digest_v1(jsonb_build_object('attempt_id',attempt,'record_kind',kind,'snapshot_revision',rev::text,'source',source,'finality',finality,'usage',usage,'final_dimensions',to_jsonb(finals),'parent',parent,'input',input,'cost_atoms',cost::text)),finals,parent,input);
 INSERT INTO public.usage_payloads(id,created_at,usage) SELECT i.id,i.created_at,usage FROM public.usage_event_index i WHERE i.id=sid;
 IF kind<>'RAW' THEN INSERT INTO public.event_deliveries(consumer_id,event_id) SELECT c.id,sid FROM public.event_consumers c;END IF;
 RETURN jsonb_build_object('id',sid,'revision',rev::text);
END $$;
CREATE FUNCTION public.core_evidence_group_v1(g jsonb) RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE ev jsonb;events jsonb[]='{}';n integer;i integer;pass integer;k text;attempt_ids uuid[];request_ids uuid[];permit public.execution_permits%ROWTYPE;h public.upstream_holds%ROWTYPE;
 prior public.core_evidence_receipts%ROWTYPE;rev_prior public.core_evidence_receipts%ROWTYPE;head record;cur public.usage_event_index%ROWTYPE;
 mode text[];contested boolean[];claims jsonb='{}';claim jsonb;d text;q text;owner integer;
 rates jsonb;prices jsonb;usage jsonb;finals text[];merged jsonb;merged_finals text[];raw jsonb;snap jsonb;x jsonb;outcome text;fault text;canonical uuid;disputed uuid;conflict uuid;receipt uuid;risk text[];stale boolean;event_claimed boolean;existing uuid;finality text;
 touched jsonb='{}';receipts jsonb='[]';billing jsonb='{}';settled jsonb;a uuid;row record;BEGIN
 IF jsonb_typeof(g) IS DISTINCT FROM 'object' OR g-'events'<>'{}'::jsonb OR jsonb_typeof(g->'events') IS DISTINCT FROM 'array' OR jsonb_array_length(g->'events') NOT BETWEEN 1 AND 64 THEN RAISE EXCEPTION 'evidence_group_invalid' USING ERRCODE='P0400';END IF;
 FOR ev IN SELECT value FROM jsonb_array_elements(g->'events') e(value) ORDER BY value->>'attempt_id',value->>'origin_identity',value->>'source',lpad(value->>'source_revision',38,'0'),value->>'event_id' LOOP
  PERFORM public.core_evidence_validate_v1(ev);
  events=events||(ev||jsonb_build_object('body_digest',public.core_digest_v1(ev-'event_id')));
 END LOOP;
 n=cardinality(events);
 FOR k IN SELECT DISTINCT key FROM unnest(events) e,LATERAL (VALUES('core-event:'||(e->>'origin_identity')||':'||(e->>'event_id')),('core-revision:'||(e->>'attempt_id')||':'||(e->>'origin_identity')||':'||(e->>'source')||':'||(e->>'source_revision'))) l(key) ORDER BY key LOOP
  IF NOT pg_try_advisory_xact_lock(hashtextextended(k,81442607)) THEN RAISE EXCEPTION 'core_busy' USING ERRCODE='P0503';END IF;
 END LOOP;
 attempt_ids=ARRAY(SELECT DISTINCT (e->>'attempt_id')::uuid FROM unnest(events) e ORDER BY 1);
 request_ids=ARRAY(SELECT DISTINCT t.request_id FROM public.attempts t WHERE t.id=ANY(attempt_ids) ORDER BY 1);
 PERFORM 1 FROM public.upstream_accounts WHERE id IN(SELECT account_id FROM public.upstream_holds WHERE attempt_id=ANY(attempt_ids)) ORDER BY id FOR NO KEY UPDATE;
 PERFORM 1 FROM public.wallets WHERE id IN(SELECT wallet_id FROM public.billing_holds WHERE request_id=ANY(request_ids)) ORDER BY id FOR NO KEY UPDATE;
 PERFORM 1 FROM public.upstream_account_balances WHERE (account_id,currency) IN(SELECT account_id,currency FROM public.upstream_holds WHERE attempt_id=ANY(attempt_ids)) ORDER BY account_id,currency FOR NO KEY UPDATE;
 PERFORM 1 FROM public.gateway_requests WHERE id=ANY(request_ids) ORDER BY id FOR UPDATE;
 PERFORM 1 FROM public.attempts WHERE id=ANY(attempt_ids) ORDER BY id FOR NO KEY UPDATE;
 PERFORM 1 FROM public.execution_permits WHERE attempt_id=ANY(attempt_ids) ORDER BY attempt_id FOR NO KEY UPDATE;
 PERFORM 1 FROM public.upstream_holds WHERE attempt_id=ANY(attempt_ids) ORDER BY attempt_id FOR UPDATE;
 PERFORM 1 FROM public.billing_holds WHERE request_id=ANY(request_ids) ORDER BY id FOR UPDATE;
 mode=array_fill('NEW'::text,ARRAY[n]);contested=array_fill(false,ARRAY[n]);
 FOR i IN 1..n LOOP
  ev=events[i];
  SELECT * INTO permit FROM public.execution_permits WHERE attempt_id=(ev->>'attempt_id')::uuid;
  IF permit.attempt_id IS NULL OR permit.state NOT IN ('SEND_GRANTED','UNKNOWN','TERMINAL') OR permit.owner_service||':'||permit.gateway_instance_id IS DISTINCT FROM ev->>'origin_identity' OR permit.owner_session_id IS DISTINCT FROM (ev->>'origin_session_id')::uuid OR permit.incarnation_id IS DISTINCT FROM (ev->>'origin_incarnation')::uuid OR permit.permit_epoch IS DISTINCT FROM (ev->>'permit_epoch')::numeric
   OR NOT EXISTS(SELECT 1 FROM public.upstream_holds WHERE attempt_id=permit.attempt_id AND estimated_quantities IS NOT NULL)
   OR EXISTS(SELECT 1 FROM public.core_evidence_receipts r WHERE r.origin_identity=ev->>'origin_identity' AND r.event_id=(ev->>'event_id')::uuid AND r.attempt_id<>permit.attempt_id) THEN RAISE EXCEPTION 'evidence_subject_invalid' USING ERRCODE='P0403';END IF;
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
   ELSIF (claim->>'value')::numeric<>q::numeric THEN contested[i]=true;owner=(claim->>'owner')::integer;IF owner>0 THEN contested[owner]=true;END IF;END IF;
  END LOOP;
 END LOOP;
 FOR pass IN 1..2 LOOP
  FOR i IN 1..n LOOP
   ev=events[i];a=(ev->>'attempt_id')::uuid;
   CONTINUE WHEN (pass=1)=contested[i];
   IF mode[i]='DUPLICATE' THEN
    SELECT * INTO prior FROM public.core_evidence_receipts r WHERE r.origin_identity=ev->>'origin_identity' AND r.event_id=(ev->>'event_id')::uuid AND r.body_digest=ev->>'body_digest';
    receipts=receipts||jsonb_build_array(jsonb_build_object('event_id',ev->>'event_id','receipt_id',prior.id,'outcome',prior.outcome,'duplicate',true));CONTINUE;
   END IF;
   SELECT * INTO h FROM public.upstream_holds WHERE attempt_id=a;
   SELECT v.rates INTO rates FROM public.cost_versions v WHERE v.id=h.cost_version_id;
   SELECT * INTO cur FROM public.usage_event_index WHERE attempt_id=a AND record_kind='CANONICAL' ORDER BY snapshot_revision DESC LIMIT 1;
   SELECT p.usage INTO merged FROM public.usage_payloads p WHERE p.id=cur.id AND p.created_at=cur.created_at;
   merged=coalesce(merged,'{}');merged_finals=coalesce(cur.final_dimensions,'{}');
   usage=ev->'normalized_usage';finals=ARRAY(SELECT jsonb_array_elements_text(ev->'normalized_finals'));
   event_claimed=EXISTS(SELECT 1 FROM public.core_evidence_receipts r WHERE r.origin_identity=ev->>'origin_identity' AND r.event_id=(ev->>'event_id')::uuid AND r.event_claim);
   receipt=gen_random_uuid();canonical=cur.id;disputed=NULL;conflict=NULL;fault=NULL;risk='{}';
   IF mode[i]='REPLAY' THEN
    SELECT * INTO rev_prior FROM public.core_evidence_receipts r WHERE r.attempt_id=a AND r.origin_identity=ev->>'origin_identity' AND r.source=ev->>'source' AND r.source_revision=(ev->>'source_revision')::numeric AND r.revision_claim;
    INSERT INTO public.core_evidence_receipts(id,origin_identity,origin_incarnation,origin_session_id,event_id,attempt_id,source,source_revision,body_digest,event_claim,revision_claim,outcome,finality,attempt_state,request_state,error_code,provider_request_id,raw_evidence_id,canonical_evidence_id,risk_dimensions)
    VALUES(receipt,ev->>'origin_identity',(ev->>'origin_incarnation')::uuid,(ev->>'origin_session_id')::uuid,(ev->>'event_id')::uuid,a,ev->>'source',(ev->>'source_revision')::numeric,ev->>'body_digest',NOT event_claimed,false,'REVISION_REPLAY',ev->>'finality',ev->>'attempt_state',ev->>'request_state',ev->>'error_code',ev->>'provider_request_id',rev_prior.raw_evidence_id,coalesce(rev_prior.canonical_evidence_id,cur.id),'{}');
    receipts=receipts||jsonb_build_array(jsonb_build_object('event_id',ev->>'event_id','receipt_id',receipt,'outcome','REVISION_REPLAY','duplicate',false));CONTINUE;
   END IF;
   finality=CASE ev->>'finality' WHEN 'NOT_EXECUTED' THEN 'FINAL' ELSE ev->>'finality' END;
   raw=public.core_evidence_snapshot_v1(a,'RAW',ev->>'source',finality,usage,finals,NULL,NULL,NULL,false);
   IF ev->>'finality'='PARTIAL' THEN
    risk=ARRAY(SELECT d2 FROM jsonb_object_keys(rates) d2 WHERE usage?d2 AND NOT d2=ANY(finals) AND NOT d2=ANY(merged_finals) AND (usage->>d2)::numeric>(h.estimated_quantities->>d2)::numeric ORDER BY d2);
   END IF;
   IF contested[i] THEN
    outcome='CONFLICT';merged=merged||usage;merged_finals=ARRAY(SELECT DISTINCT unnest(merged_finals||finals) ORDER BY 1);
    existing=coalesce(cur.id,(SELECT r.raw_evidence_id FROM public.core_evidence_receipts r WHERE r.attempt_id=a AND r.origin_identity=ev->>'origin_identity' AND (r.event_id=(ev->>'event_id')::uuid OR (r.source=ev->>'source' AND r.source_revision=(ev->>'source_revision')::numeric)) ORDER BY r.created_at LIMIT 1),
     (SELECT r.raw_evidence_id FROM public.core_evidence_receipts r WHERE r.attempt_id=a AND r.outcome='CONFLICT' ORDER BY r.created_at LIMIT 1),
     (SELECT i3.id FROM public.usage_event_index i3 WHERE i3.attempt_id=a AND i3.record_kind='RAW' AND i3.id<>(raw->>'id')::uuid ORDER BY i3.snapshot_revision DESC LIMIT 1),(raw->>'id')::uuid);
    BEGIN x=public.core_supplier_exposure_v1(rates,h.estimated_quantities,merged,merged_finals,'{}');
    EXCEPTION WHEN OTHERS THEN x=NULL;fault='COST_OVERFLOW';END;
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
     EXCEPTION WHEN OTHERS THEN x=NULL;fault='COST_OVERFLOW';END;
     IF x IS NULL THEN
      outcome='OVERFLOW';snap=public.core_evidence_snapshot_v1(a,'DISPUTED',ev->>'source',finality,merged,merged_finals,cur.id,(raw->>'id')::uuid,NULL,false);disputed=(snap->>'id')::uuid;
     ELSE
      outcome='ACCEPTED';
      snap=public.core_evidence_snapshot_v1(a,'CANONICAL',ev->>'source',CASE WHEN finality='FINAL' AND (x->>'complete')::boolean THEN 'FINAL' WHEN cardinality(merged_finals)>0 OR merged<>'{}'::jsonb THEN 'PARTIAL' ELSE 'UNKNOWN' END,merged,merged_finals,cur.id,(raw->>'id')::uuid,(x->>'cost_atoms')::numeric,(x->>'complete')::boolean);
      canonical=(snap->>'id')::uuid;
      UPDATE public.attempts SET evidence_id=canonical,state=CASE WHEN state IN ('SUCCEEDED','FAILED','NOT_EXECUTED') OR ev->>'attempt_state' IS NULL THEN state ELSE ev->>'attempt_state' END,
       finished_at=CASE WHEN ev->>'attempt_state' IN ('SUCCEEDED','FAILED','NOT_EXECUTED') THEN coalesce(finished_at,clock_timestamp()) ELSE finished_at END,error_code=coalesce(ev->>'error_code',error_code),provider_request_id=coalesce(ev->>'provider_request_id',provider_request_id) WHERE id=a;
      UPDATE public.gateway_requests SET selected_attempt_id=CASE WHEN ev->>'request_state' IN ('SUCCEEDED','CANCELLED') THEN a END,
       state=ev->>'request_state',billing_outcome=CASE ev->>'request_state' WHEN 'SUCCEEDED' THEN 'SUCCESS' ELSE ev->>'request_state' END,updated_at=clock_timestamp()
       WHERE id=(SELECT request_id FROM public.attempts WHERE id=a) AND ev->>'request_state' IN ('SUCCEEDED','CANCELLED','FAILED') AND selected_attempt_id IS NULL AND billing_outcome IN ('PENDING','UNKNOWN');
     END IF;
    END IF;
   END IF;
   INSERT INTO public.core_evidence_receipts(id,origin_identity,origin_incarnation,origin_session_id,event_id,attempt_id,source,source_revision,body_digest,event_claim,revision_claim,outcome,finality,attempt_state,request_state,error_code,provider_request_id,raw_evidence_id,canonical_evidence_id,disputed_evidence_id,conflict_id,risk_dimensions,numeric_fault)
   VALUES(receipt,ev->>'origin_identity',(ev->>'origin_incarnation')::uuid,(ev->>'origin_session_id')::uuid,(ev->>'event_id')::uuid,a,ev->>'source',(ev->>'source_revision')::numeric,ev->>'body_digest',NOT event_claimed,outcome<>'CONFLICT',outcome,ev->>'finality',ev->>'attempt_state',ev->>'request_state',ev->>'error_code',ev->>'provider_request_id',(raw->>'id')::uuid,canonical,disputed,conflict,risk,fault);
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
CREATE FUNCTION public.persist_core_evidence_groups_v1(p_groups jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE g jsonb;results jsonb='[]';failure text;message text;BEGIN
 IF jsonb_typeof(p_groups) IS DISTINCT FROM 'array' OR jsonb_array_length(p_groups) NOT BETWEEN 1 AND 128 THEN RAISE EXCEPTION 'evidence_batch_invalid' USING ERRCODE='P0400';END IF;
 FOR g IN SELECT value FROM jsonb_array_elements(p_groups) LOOP
  BEGIN
   results=results||jsonb_build_array(public.core_evidence_group_v1(g));
  EXCEPTION WHEN OTHERS THEN
   GET STACKED DIAGNOSTICS failure=RETURNED_SQLSTATE,message=MESSAGE_TEXT;
   results=results||jsonb_build_array(CASE WHEN failure IN ('P0400','P0403') THEN jsonb_build_object('status','REJECTED','error',message) ELSE jsonb_build_object('status','ERROR','error',failure) END);
  END;
 END LOOP;
 RETURN results;
END $$;
REVOKE ALL ON FUNCTION public.core_evidence_validate_v1(jsonb),public.core_evidence_snapshot_v1(uuid,text,text,text,jsonb,text[],uuid,uuid,numeric,boolean),public.core_evidence_group_v1(jsonb),public.persist_core_evidence_groups_v1(jsonb) FROM PUBLIC;
