DROP TABLE IF EXISTS public.user_identities;

ALTER TABLE public.sessions
  ADD COLUMN IF NOT EXISTS scope TEXT NOT NULL DEFAULT 'CLIENT'
  CHECK (scope IN ('CLIENT', 'ADMIN'));

ALTER TABLE public.projects
  ADD COLUMN IF NOT EXISTS developer_key_spend_limit_atoms NUMERIC(38, 0)
    CHECK (developer_key_spend_limit_atoms IS NULL OR developer_key_spend_limit_atoms >= 0),
  ADD COLUMN IF NOT EXISTS developer_max_active_keys INTEGER
    CHECK (developer_max_active_keys IS NULL OR developer_max_active_keys > 0);

ALTER TABLE public.pricing_templates
  ADD COLUMN IF NOT EXISTS pending_price_revision INTEGER;

ALTER TABLE public.api_key_records
  ADD COLUMN IF NOT EXISTS rotated_from_key_id UUID REFERENCES public.api_key_records(id) ON DELETE SET NULL;

CREATE TABLE IF NOT EXISTS public.alert_eval_queue (
  project_id UUID PRIMARY KEY REFERENCES public.projects(id) ON DELETE CASCADE,
  latest_wallet_available_atoms NUMERIC(38, 0),
  currency TEXT NOT NULL DEFAULT 'USD',
  dirty_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE public.alert_deliveries
  ADD COLUMN IF NOT EXISTS webhook_url TEXT,
  ADD COLUMN IF NOT EXISTS attempts INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  ADD COLUMN IF NOT EXISTS last_http_status INTEGER,
  ADD COLUMN IF NOT EXISTS last_error TEXT,
  ADD COLUMN IF NOT EXISTS delivered_at TIMESTAMPTZ;

CREATE UNIQUE INDEX IF NOT EXISTS idx_org_invitations_pending_email
  ON public.org_invitations (org_id, lower(email))
  WHERE status = 'PENDING';

CREATE INDEX IF NOT EXISTS idx_alert_rules_project_enabled
  ON public.alert_rules (project_id)
  WHERE enabled = true;

CREATE INDEX IF NOT EXISTS idx_alert_deliveries_project_time
  ON public.alert_deliveries (project_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_alert_deliveries_rule
  ON public.alert_deliveries (rule_id);

CREATE INDEX IF NOT EXISTS idx_alert_deliveries_pending
  ON public.alert_deliveries (next_attempt_at ASC)
  WHERE status = 'PENDING';

CREATE INDEX IF NOT EXISTS idx_projects_pending_sync
  ON public.projects (id)
  WHERE gateway_synced = true AND (sync_status <> 'SYNCED' OR synced_revision < desired_revision);

CREATE INDEX IF NOT EXISTS idx_api_keys_pending_sync
  ON public.api_key_records (id)
  WHERE synced_revision > 0 AND (sync_status <> 'SYNCED' OR synced_revision < desired_revision);

CREATE INDEX IF NOT EXISTS idx_channels_pending_sync
  ON public.channels (id)
  WHERE synced_revision > 0 AND (sync_status <> 'SYNCED' OR synced_revision < desired_revision);

CREATE INDEX IF NOT EXISTS idx_routes_pending_sync
  ON public.routes (id)
  WHERE sync_status <> 'SYNCED' OR synced_revision < desired_revision;

CREATE INDEX IF NOT EXISTS idx_overrides_pending_sync
  ON public.pricing_overrides (id)
  WHERE sync_status <> 'SYNCED' OR synced_revision < desired_revision;

CREATE INDEX IF NOT EXISTS idx_templates_pending_sync
  ON public.pricing_templates (id)
  WHERE sync_status <> 'SYNCED' OR synced_revision < desired_revision;

CREATE INDEX IF NOT EXISTS idx_models_pending_sync
  ON public.model_catalog (name)
  WHERE sync_status <> 'SYNCED' OR synced_revision < desired_revision;

CREATE INDEX IF NOT EXISTS idx_upstream_accounts_pending_sync
  ON public.upstream_accounts (id)
  WHERE sync_status <> 'SYNCED' OR synced_revision < desired_revision;

CREATE INDEX IF NOT EXISTS idx_sessions_cleanup
  ON public.sessions (refresh_expires_at)
  WHERE revoked_at IS NULL;
