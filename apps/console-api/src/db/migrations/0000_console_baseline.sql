CREATE TABLE IF NOT EXISTS public.console_schema_migrations (
  version INTEGER PRIMARY KEY,
  checksum TEXT NOT NULL,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.users (
  id UUID PRIMARY KEY,
  email TEXT NOT NULL UNIQUE,
  password_hash TEXT NOT NULL,
  display_name TEXT NOT NULL,
  is_platform_admin BOOLEAN NOT NULL DEFAULT false,
  status TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'SUSPENDED')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.user_identities (
  id UUID PRIMARY KEY,
  user_id UUID NOT NULL REFERENCES public.users(id) ON DELETE CASCADE,
  provider TEXT NOT NULL,
  provider_subject TEXT NOT NULL,
  email TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (provider, provider_subject)
);

CREATE TABLE IF NOT EXISTS public.sessions (
  id UUID PRIMARY KEY,
  user_id UUID NOT NULL REFERENCES public.users(id) ON DELETE CASCADE,
  token_hash TEXT NOT NULL UNIQUE,
  refresh_token_hash TEXT NOT NULL UNIQUE,
  expires_at TIMESTAMPTZ NOT NULL,
  refresh_expires_at TIMESTAMPTZ NOT NULL,
  revoked_at TIMESTAMPTZ,
  user_agent TEXT,
  ip_address TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_sessions_user ON public.sessions (user_id, created_at DESC);

CREATE TABLE IF NOT EXISTS public.organizations (
  id UUID PRIMARY KEY,
  slug TEXT NOT NULL UNIQUE,
  name TEXT NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('PERSONAL', 'ENTERPRISE')),
  owner_user_id UUID NOT NULL REFERENCES public.users(id),
  status TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'SUSPENDED')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.org_members (
  org_id UUID NOT NULL REFERENCES public.organizations(id) ON DELETE CASCADE,
  user_id UUID NOT NULL REFERENCES public.users(id) ON DELETE CASCADE,
  role TEXT NOT NULL CHECK (role IN ('OWNER', 'ADMIN', 'FINANCE', 'DEVELOPER', 'VIEWER')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (org_id, user_id)
);

CREATE INDEX IF NOT EXISTS idx_org_members_user ON public.org_members (user_id);

CREATE TABLE IF NOT EXISTS public.org_invitations (
  id UUID PRIMARY KEY,
  org_id UUID NOT NULL REFERENCES public.organizations(id) ON DELETE CASCADE,
  email TEXT NOT NULL,
  role TEXT NOT NULL CHECK (role IN ('ADMIN', 'FINANCE', 'DEVELOPER', 'VIEWER')),
  token_hash TEXT NOT NULL UNIQUE,
  invited_by UUID NOT NULL REFERENCES public.users(id),
  status TEXT NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'ACCEPTED', 'REVOKED', 'EXPIRED')),
  expires_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.projects (
  id UUID PRIMARY KEY,
  org_id UUID NOT NULL REFERENCES public.organizations(id) ON DELETE CASCADE,
  gateway_project_id UUID NOT NULL UNIQUE,
  name TEXT NOT NULL,
  currency TEXT NOT NULL DEFAULT 'USD',
  rpm INTEGER NOT NULL DEFAULT 6000 CHECK (rpm > 0),
  tpm BIGINT NOT NULL DEFAULT 10000000 CHECK (tpm > 0),
  concurrency INTEGER NOT NULL DEFAULT 1000 CHECK (concurrency > 0),
  enabled BOOLEAN NOT NULL DEFAULT true,
  gateway_synced BOOLEAN NOT NULL DEFAULT false,
  desired_revision INTEGER NOT NULL DEFAULT 1,
  synced_revision INTEGER NOT NULL DEFAULT 0,
  sync_status TEXT NOT NULL DEFAULT 'PENDING' CHECK (sync_status IN ('PENDING', 'SYNCED', 'FAILED')),
  last_sync_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_projects_org ON public.projects (org_id, created_at DESC);

CREATE TABLE IF NOT EXISTS public.api_key_records (
  id UUID PRIMARY KEY,
  project_id UUID NOT NULL REFERENCES public.projects(id) ON DELETE CASCADE,
  created_by UUID NOT NULL REFERENCES public.users(id),
  name TEXT NOT NULL,
  key_prefix TEXT NOT NULL,
  role TEXT NOT NULL DEFAULT 'INVOKER' CHECK (role IN ('ADMIN', 'BILLING', 'INVOKER', 'VIEWER')),
  allowed_cidrs TEXT[] NOT NULL DEFAULT '{}',
  models TEXT[] NOT NULL DEFAULT '{}',
  operations TEXT[] NOT NULL DEFAULT '{}',
  spend_limit_atoms NUMERIC(38,0) CHECK (spend_limit_atoms IS NULL OR spend_limit_atoms >= 0),
  spend_limit_window TEXT NOT NULL DEFAULT 'LIFETIME' CHECK (spend_limit_window IN ('LIFETIME', 'DAILY', 'MONTHLY')),
  enabled BOOLEAN NOT NULL DEFAULT true,
  expires_at TIMESTAMPTZ,
  revoked_at TIMESTAMPTZ,
  desired_revision INTEGER NOT NULL DEFAULT 1,
  synced_revision INTEGER NOT NULL DEFAULT 0,
  sync_status TEXT NOT NULL DEFAULT 'PENDING' CHECK (sync_status IN ('PENDING', 'SYNCED', 'FAILED')),
  last_sync_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_api_key_records_project ON public.api_key_records (project_id, created_at DESC);

CREATE TABLE IF NOT EXISTS public.upstream_accounts (
  id UUID PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  state TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (state IN ('ACTIVE', 'DISABLED', 'RECONCILE_REQUIRED')),
  desired_revision INTEGER NOT NULL DEFAULT 1,
  synced_revision INTEGER NOT NULL DEFAULT 0,
  sync_status TEXT NOT NULL DEFAULT 'PENDING' CHECK (sync_status IN ('PENDING', 'SYNCED', 'FAILED')),
  last_sync_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.channels (
  id UUID PRIMARY KEY,
  account_id UUID NOT NULL REFERENCES public.upstream_accounts(id),
  name TEXT NOT NULL,
  kind TEXT NOT NULL,
  provider_plugin_id UUID,
  base_url TEXT NOT NULL,
  key_hint TEXT NOT NULL DEFAULT '',
  rpm INTEGER NOT NULL DEFAULT 1000000 CHECK (rpm > 0),
  concurrency INTEGER NOT NULL DEFAULT 1000 CHECK (concurrency > 0),
  enabled BOOLEAN NOT NULL DEFAULT true,
  routing_policy JSONB NOT NULL DEFAULT '{}'::jsonb,
  adapter_config JSONB NOT NULL DEFAULT '{}'::jsonb,
  desired_revision INTEGER NOT NULL DEFAULT 1,
  synced_revision INTEGER NOT NULL DEFAULT 0,
  sync_status TEXT NOT NULL DEFAULT 'PENDING' CHECK (sync_status IN ('PENDING', 'SYNCED', 'FAILED')),
  last_sync_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.pricing_templates (
  id UUID PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  side TEXT NOT NULL CHECK (side IN ('CUSTOMER', 'SUPPLIER')),
  currency TEXT NOT NULL DEFAULT 'USD',
  basis TEXT NOT NULL DEFAULT 'PROVIDER_USAGE' CHECK (basis IN ('PROVIDER_USAGE', 'REQUEST_COMPLETION', 'INPUT_CHARACTERS')),
  rates JSONB NOT NULL,
  ceilings JSONB NOT NULL DEFAULT '{}'::jsonb,
  current_price_version_id UUID,
  pending_price_version_id UUID,
  desired_revision INTEGER NOT NULL DEFAULT 1,
  synced_revision INTEGER NOT NULL DEFAULT 0,
  sync_status TEXT NOT NULL DEFAULT 'PENDING' CHECK (sync_status IN ('PENDING', 'SYNCED', 'FAILED')),
  last_sync_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.price_version_snapshots (
  id UUID PRIMARY KEY,
  template_id UUID REFERENCES public.pricing_templates(id) ON DELETE SET NULL,
  side TEXT NOT NULL CHECK (side IN ('CUSTOMER', 'SUPPLIER')),
  currency TEXT NOT NULL DEFAULT 'USD',
  basis TEXT NOT NULL DEFAULT 'PROVIDER_USAGE' CHECK (basis IN ('PROVIDER_USAGE', 'REQUEST_COMPLETION', 'INPUT_CHARACTERS')),
  rates JSONB NOT NULL,
  ceilings JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.model_catalog (
  name TEXT PRIMARY KEY,
  display_name TEXT NOT NULL,
  provider_brand TEXT NOT NULL DEFAULT 'gateway',
  description TEXT NOT NULL DEFAULT '',
  category TEXT NOT NULL DEFAULT 'chat',
  context_length INTEGER NOT NULL DEFAULT 128000 CHECK (context_length > 0),
  max_output_tokens INTEGER NOT NULL DEFAULT 32768 CHECK (max_output_tokens > 0),
  operations TEXT[] NOT NULL,
  routing_strategy TEXT NOT NULL DEFAULT 'WEIGHTED' CHECK (routing_strategy IN ('WEIGHTED', 'ROUND_ROBIN', 'LEAST_LATENCY')),
  tags TEXT[] NOT NULL DEFAULT '{}',
  list_price_version_id UUID,
  list_rates JSONB NOT NULL DEFAULT '{}'::jsonb,
  currency TEXT NOT NULL DEFAULT 'USD',
  is_public BOOLEAN NOT NULL DEFAULT true,
  enabled BOOLEAN NOT NULL DEFAULT true,
  desired_revision INTEGER NOT NULL DEFAULT 1,
  synced_revision INTEGER NOT NULL DEFAULT 0,
  sync_status TEXT NOT NULL DEFAULT 'PENDING' CHECK (sync_status IN ('PENDING', 'SYNCED', 'FAILED')),
  last_sync_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.routes (
  id UUID PRIMARY KEY,
  model_name TEXT NOT NULL REFERENCES public.model_catalog(name) ON DELETE CASCADE,
  channel_id UUID NOT NULL REFERENCES public.channels(id),
  upstream_model TEXT NOT NULL,
  operations TEXT[] NOT NULL,
  priority INTEGER NOT NULL DEFAULT 0,
  weight DOUBLE PRECISION NOT NULL DEFAULT 1 CHECK (weight > 0),
  billing_price_id UUID NOT NULL,
  cost_version_id UUID NOT NULL,
  request_policy JSONB NOT NULL DEFAULT '{}'::jsonb,
  adapter_overrides JSONB NOT NULL DEFAULT '{}'::jsonb,
  retry_policy JSONB NOT NULL DEFAULT '{}'::jsonb,
  enabled BOOLEAN NOT NULL DEFAULT true,
  desired_revision INTEGER NOT NULL DEFAULT 1,
  synced_revision INTEGER NOT NULL DEFAULT 0,
  sync_status TEXT NOT NULL DEFAULT 'PENDING' CHECK (sync_status IN ('PENDING', 'SYNCED', 'FAILED')),
  last_sync_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_routes_model ON public.routes (model_name, priority DESC);

CREATE TABLE IF NOT EXISTS public.pricing_overrides (
  id UUID PRIMARY KEY,
  project_id UUID NOT NULL REFERENCES public.projects(id) ON DELETE CASCADE,
  model_name TEXT NOT NULL,
  operation TEXT NOT NULL DEFAULT '*',
  override_kind TEXT NOT NULL CHECK (override_kind IN ('EXACT_PRICE', 'MULTIPLIER')),
  billing_price_id UUID,
  multiplier_numerator NUMERIC(38,0),
  multiplier_denominator NUMERIC(38,0),
  enabled BOOLEAN NOT NULL DEFAULT true,
  desired_revision INTEGER NOT NULL DEFAULT 1,
  synced_revision INTEGER NOT NULL DEFAULT 0,
  sync_status TEXT NOT NULL DEFAULT 'PENDING' CHECK (sync_status IN ('PENDING', 'SYNCED', 'FAILED')),
  last_sync_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (project_id, model_name, operation),
  CHECK (
    (override_kind = 'EXACT_PRICE' AND model_name <> '*' AND billing_price_id IS NOT NULL AND multiplier_numerator IS NULL AND multiplier_denominator IS NULL)
    OR
    (override_kind = 'MULTIPLIER' AND billing_price_id IS NULL AND multiplier_numerator IS NOT NULL AND multiplier_denominator IS NOT NULL AND multiplier_numerator > 0 AND multiplier_denominator > 0)
  )
);

CREATE TABLE IF NOT EXISTS public.usage_events (
  event_id UUID PRIMARY KEY,
  seq BIGINT,
  kind TEXT NOT NULL,
  request_id UUID,
  charge_id UUID,
  project_id UUID,
  org_id UUID,
  api_key_id UUID,
  model TEXT,
  operation TEXT,
  channel_id UUID,
  outcome TEXT,
  currency TEXT NOT NULL DEFAULT 'USD',
  customer_atoms NUMERIC(38,0) NOT NULL DEFAULT 0,
  supplier_cost_atoms NUMERIC(38,0) NOT NULL DEFAULT 0,
  input_tokens BIGINT NOT NULL DEFAULT 0,
  output_tokens BIGINT NOT NULL DEFAULT 0,
  cached_input_tokens BIGINT NOT NULL DEFAULT 0,
  reasoning_tokens BIGINT NOT NULL DEFAULT 0,
  pricing_override_id UUID,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_usage_events_project_time ON public.usage_events (project_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_usage_events_key_time ON public.usage_events (api_key_id, occurred_at DESC);

CREATE TABLE IF NOT EXISTS public.usage_hourly_rollups (
  bucket_hour TIMESTAMPTZ NOT NULL,
  project_id UUID NOT NULL,
  api_key_id TEXT NOT NULL,
  model TEXT NOT NULL,
  operation TEXT NOT NULL,
  currency TEXT NOT NULL,
  request_count BIGINT NOT NULL DEFAULT 0,
  customer_atoms NUMERIC(38,0) NOT NULL DEFAULT 0,
  supplier_cost_atoms NUMERIC(38,0) NOT NULL DEFAULT 0,
  input_tokens BIGINT NOT NULL DEFAULT 0,
  output_tokens BIGINT NOT NULL DEFAULT 0,
  cached_input_tokens BIGINT NOT NULL DEFAULT 0,
  reasoning_tokens BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (bucket_hour, project_id, api_key_id, model, operation, currency)
);

CREATE INDEX IF NOT EXISTS idx_usage_hourly_project ON public.usage_hourly_rollups (project_id, bucket_hour DESC);

CREATE TABLE IF NOT EXISTS public.usage_daily_rollups (
  bucket_day DATE NOT NULL,
  project_id UUID NOT NULL,
  api_key_id TEXT NOT NULL,
  model TEXT NOT NULL,
  operation TEXT NOT NULL,
  currency TEXT NOT NULL,
  request_count BIGINT NOT NULL DEFAULT 0,
  customer_atoms NUMERIC(38,0) NOT NULL DEFAULT 0,
  supplier_cost_atoms NUMERIC(38,0) NOT NULL DEFAULT 0,
  input_tokens BIGINT NOT NULL DEFAULT 0,
  output_tokens BIGINT NOT NULL DEFAULT 0,
  cached_input_tokens BIGINT NOT NULL DEFAULT 0,
  reasoning_tokens BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (bucket_day, project_id, api_key_id, model, operation, currency)
);

CREATE INDEX IF NOT EXISTS idx_usage_daily_project ON public.usage_daily_rollups (project_id, bucket_day DESC);

CREATE TABLE IF NOT EXISTS public.alert_rules (
  id UUID PRIMARY KEY,
  project_id UUID NOT NULL REFERENCES public.projects(id) ON DELETE CASCADE,
  kind TEXT NOT NULL CHECK (kind IN ('LOW_BALANCE', 'DAILY_SPEND')),
  threshold_atoms NUMERIC(38,0) NOT NULL CHECK (threshold_atoms >= 0),
  webhook_url TEXT,
  enabled BOOLEAN NOT NULL DEFAULT true,
  last_triggered_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.alert_deliveries (
  id UUID PRIMARY KEY,
  rule_id UUID NOT NULL REFERENCES public.alert_rules(id) ON DELETE CASCADE,
  project_id UUID NOT NULL,
  status TEXT NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.console_audit_logs (
  id UUID PRIMARY KEY,
  actor_user_id UUID,
  actor_email TEXT,
  org_id UUID,
  action TEXT NOT NULL,
  target_type TEXT NOT NULL,
  target_id TEXT NOT NULL,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_console_audit_time ON public.console_audit_logs (created_at DESC);
