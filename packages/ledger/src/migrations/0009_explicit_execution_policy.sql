WITH normalized AS (
  UPDATE public.models
  SET adaptive_routing = adaptive_routing || '{"execution_policy":"protected"}'::jsonb
  WHERE jsonb_typeof(adaptive_routing) = 'object'
    AND NOT (adaptive_routing ? 'execution_policy')
  RETURNING name
)
UPDATE public.config_epoch
SET version = version + 1, updated_at = now()
WHERE id = 1 AND EXISTS (SELECT 1 FROM normalized);

ALTER TABLE public.models ADD CONSTRAINT models_adaptive_execution_policy_check
CHECK (
  adaptive_routing IS NULL
  OR adaptive_routing IN ('null'::jsonb, 'false'::jsonb)
  OR (
    jsonb_typeof(adaptive_routing) = 'object'
    AND COALESCE(adaptive_routing->>'execution_policy' IN ('route', 'protected'), false)
  )
);
