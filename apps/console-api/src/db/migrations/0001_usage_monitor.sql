CREATE INDEX usage_events_monitor_scope_time_idx
  ON public.usage_events (channel_id, model, operation, currency, occurred_at, event_id)
  WHERE kind = 'billing.settled' AND outcome IN ('SETTLED', 'DEBT')
    AND channel_id IS NOT NULL
    AND octet_length(model) <= 640 AND octet_length(operation) <= 32 AND octet_length(currency) <= 8;

CREATE TABLE public.usage_monitor_windows (
  policy_version text NOT NULL,
  window_start timestamptz NOT NULL,
  policy jsonb NOT NULL CHECK (jsonb_typeof(policy) = 'object'),
  phase text NOT NULL CHECK (phase IN ('INITIAL', 'FINAL')),
  final_at timestamptz NOT NULL,
  finalized_at timestamptz,
  next_attempt_at timestamptz,
  lease_token uuid,
  leased_until timestamptz,
  lease_superseded boolean NOT NULL DEFAULT false,
  replay_requested boolean NOT NULL DEFAULT false,
  trend_refresh boolean NOT NULL DEFAULT false,
  skipped_windows bigint NOT NULL DEFAULT 0 CHECK (skipped_windows >= 0),
  last_skipped_at timestamptz,
  failed_attempts integer NOT NULL DEFAULT 0 CHECK (failed_attempts >= 0),
  last_error text CHECK (last_error IN ('SCAN_LIMIT', 'EVALUATION_FAILED')),
  coverage jsonb,
  PRIMARY KEY (policy_version, window_start),
  CHECK ((lease_token IS NULL) = (leased_until IS NULL)),
  CHECK (NOT lease_superseded OR (lease_token IS NOT NULL AND replay_requested)),
  CHECK (next_attempt_at IS NOT NULL OR (phase = 'FINAL' AND lease_token IS NULL))
);

CREATE INDEX usage_monitor_windows_due_idx
  ON public.usage_monitor_windows (policy_version, next_attempt_at, window_start)
  WHERE next_attempt_at IS NOT NULL;

CREATE INDEX usage_monitor_windows_recheck_idx
  ON public.usage_monitor_windows (next_attempt_at, window_start)
  WHERE next_attempt_at IS NOT NULL AND (replay_requested OR trend_refresh);

CREATE TABLE public.usage_monitor_window_stats (
  policy_version text NOT NULL,
  window_start timestamptz NOT NULL,
  scope_key text NOT NULL,
  evaluated jsonb NOT NULL CHECK (jsonb_typeof(evaluated) = 'object'),
  PRIMARY KEY (policy_version, window_start, scope_key),
  FOREIGN KEY (policy_version, window_start)
    REFERENCES public.usage_monitor_windows (policy_version, window_start)
);

CREATE TABLE public.usage_monitor_findings (
  id uuid PRIMARY KEY,
  policy_version text NOT NULL,
  window_start timestamptz NOT NULL,
  scope_key text NOT NULL,
  rule_key text NOT NULL CHECK (rule_key IN ('MAX_INPUT', 'MAX_OUTPUT', 'MAX_CHARGE', 'TREND_INPUT', 'TREND_OUTPUT')),
  channel_id uuid NOT NULL,
  model text NOT NULL,
  operation text NOT NULL,
  currency text NOT NULL,
  evidence jsonb NOT NULL CHECK (jsonb_typeof(evidence) = 'object'),
  matched boolean NOT NULL DEFAULT true,
  disposition text NOT NULL DEFAULT 'OPEN' CHECK (disposition IN ('OPEN', 'ACKNOWLEDGED', 'RESOLVED')),
  revision integer NOT NULL DEFAULT 1 CHECK (revision > 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  observed_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (policy_version, window_start, scope_key, rule_key),
  FOREIGN KEY (policy_version, window_start)
    REFERENCES public.usage_monitor_windows (policy_version, window_start)
);

CREATE INDEX usage_monitor_findings_page_idx
  ON public.usage_monitor_findings (window_start DESC, id DESC);

CREATE INDEX usage_monitor_findings_open_idx
  ON public.usage_monitor_findings (policy_version, rule_key)
  WHERE disposition = 'OPEN' AND matched;
