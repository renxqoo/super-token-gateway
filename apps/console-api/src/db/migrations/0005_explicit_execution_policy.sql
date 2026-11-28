UPDATE public.model_catalog
SET adaptive_routing = adaptive_routing || '{"execution_policy":"protected"}'::jsonb
WHERE jsonb_typeof(adaptive_routing) = 'object'
  AND NOT (adaptive_routing ? 'execution_policy');

ALTER TABLE public.model_catalog ADD CONSTRAINT model_catalog_adaptive_execution_policy_check
CHECK (
  adaptive_routing IS NULL
  OR adaptive_routing IN ('null'::jsonb, 'false'::jsonb)
  OR (
    jsonb_typeof(adaptive_routing) = 'object'
    AND COALESCE(adaptive_routing->>'execution_policy' IN ('route', 'protected'), false)
  )
);
