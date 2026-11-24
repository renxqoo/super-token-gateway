ALTER TABLE public.alert_eval_queue
  DROP COLUMN latest_wallet_available_atoms,
  DROP COLUMN currency,
  ADD COLUMN generation UUID NOT NULL DEFAULT gen_random_uuid(),
  ADD COLUMN next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now();

CREATE INDEX alert_eval_queue_next_attempt_idx
  ON public.alert_eval_queue (next_attempt_at, dirty_at, project_id);
