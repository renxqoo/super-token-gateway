DROP FUNCTION public.admit_gateway_batch(jsonb);
DROP FUNCTION public.persist_usage_batch(jsonb);
DROP FUNCTION public.ingest_initial_usage(jsonb,uuid[]);
DROP FUNCTION public.commit_usage_evidence(uuid,uuid,text,text,text,jsonb,numeric,boolean,text,text,text);
DROP FUNCTION public.record_usage_evidence(uuid,uuid,text,text,text,jsonb,numeric,boolean,text,text);
DROP FUNCTION public.gateway_token_summary(uuid);
DO $$ DECLARE r text;BEGIN
 FOREACH r IN ARRAY ARRAY['gateway_runtime','gateway_worker'] LOOP
  IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname=r) THEN
   EXECUTE format('REVOKE ALL ON ALL TABLES IN SCHEMA public FROM %I',r);
   EXECUTE format('REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM %I',r);
   EXECUTE format('REVOKE ALL ON ALL FUNCTIONS IN SCHEMA public FROM %I',r);
   EXECUTE format('REVOKE ALL ON SCHEMA public FROM %I',r);
  END IF;
 END LOOP;
END $$;
