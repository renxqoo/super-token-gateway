CREATE TABLE public.alert_deliveries (
    id uuid NOT NULL,
    rule_id uuid NOT NULL,
    billing_account_id uuid NOT NULL,
    status text NOT NULL,
    payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    webhook_url text,
    attempts integer DEFAULT 0 NOT NULL,
    next_attempt_at timestamp with time zone DEFAULT now() NOT NULL,
    last_http_status integer,
    last_error text,
    delivered_at timestamp with time zone
);

CREATE TABLE public.alert_eval_queue (
    billing_account_id uuid NOT NULL,
    dirty_at timestamp with time zone DEFAULT now() NOT NULL,
    generation uuid DEFAULT gen_random_uuid() NOT NULL,
    next_attempt_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE public.alert_rules (
    id uuid NOT NULL,
    billing_account_id uuid NOT NULL,
    kind text NOT NULL,
    threshold_atoms numeric(38,0) NOT NULL,
    webhook_url text,
    enabled boolean DEFAULT true NOT NULL,
    last_triggered_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT alert_rules_kind_check CHECK ((kind = ANY (ARRAY['LOW_BALANCE'::text, 'DAILY_SPEND'::text]))),
    CONSTRAINT alert_rules_threshold_atoms_check CHECK ((threshold_atoms >= (0)::numeric))
);

CREATE TABLE public.api_key_records (
    id uuid NOT NULL,
    billing_account_id uuid NOT NULL,
    created_by uuid NOT NULL,
    name text NOT NULL,
    key_prefix text NOT NULL,
    role text DEFAULT 'INVOKER'::text NOT NULL,
    allowed_cidrs text[] DEFAULT '{}'::text[] NOT NULL,
    models text[] DEFAULT '{}'::text[] NOT NULL,
    operations text[] DEFAULT '{}'::text[] NOT NULL,
    spend_limit_atoms numeric(38,0),
    spend_limit_window text DEFAULT 'LIFETIME'::text NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    expires_at timestamp with time zone,
    revoked_at timestamp with time zone,
    desired_revision integer DEFAULT 1 NOT NULL,
    synced_revision integer DEFAULT 0 NOT NULL,
    sync_status text DEFAULT 'PENDING'::text NOT NULL,
    last_sync_error text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    rotated_from_key_id uuid,
    CONSTRAINT api_key_records_role_check CHECK ((role = ANY (ARRAY['ADMIN'::text, 'BILLING'::text, 'INVOKER'::text, 'VIEWER'::text]))),
    CONSTRAINT api_key_records_spend_limit_atoms_check CHECK (((spend_limit_atoms IS NULL) OR (spend_limit_atoms >= (0)::numeric))),
    CONSTRAINT api_key_records_spend_limit_window_check CHECK ((spend_limit_window = ANY (ARRAY['LIFETIME'::text, 'DAILY'::text, 'MONTHLY'::text]))),
    CONSTRAINT api_key_records_sync_status_check CHECK ((sync_status = ANY (ARRAY['PENDING'::text, 'SYNCED'::text, 'FAILED'::text])))
);

CREATE TABLE public.billing_accounts (
    id uuid NOT NULL,
    org_id uuid NOT NULL,
    gateway_billing_account_id uuid NOT NULL,
    name text NOT NULL,
    currency text DEFAULT 'USD'::text NOT NULL,
    rpm integer DEFAULT 6000 NOT NULL,
    tpm bigint DEFAULT 10000000 NOT NULL,
    concurrency integer DEFAULT 1000 NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    gateway_synced boolean DEFAULT false NOT NULL,
    desired_revision integer DEFAULT 1 NOT NULL,
    synced_revision integer DEFAULT 0 NOT NULL,
    sync_status text DEFAULT 'PENDING'::text NOT NULL,
    last_sync_error text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    developer_key_spend_limit_atoms numeric(38,0),
    developer_max_active_keys integer,
    CONSTRAINT billing_accounts_concurrency_check CHECK ((concurrency > 0)),
    CONSTRAINT billing_accounts_developer_key_spend_limit_atoms_check CHECK (((developer_key_spend_limit_atoms IS NULL) OR (developer_key_spend_limit_atoms >= (0)::numeric))),
    CONSTRAINT billing_accounts_developer_max_active_keys_check CHECK (((developer_max_active_keys IS NULL) OR (developer_max_active_keys > 0))),
    CONSTRAINT billing_accounts_rpm_check CHECK ((rpm > 0)),
    CONSTRAINT billing_accounts_sync_status_check CHECK ((sync_status = ANY (ARRAY['PENDING'::text, 'SYNCED'::text, 'FAILED'::text]))),
    CONSTRAINT billing_accounts_tpm_check CHECK ((tpm > 0))
);

CREATE TABLE public.channels (
    id uuid NOT NULL,
    account_id uuid NOT NULL,
    name text NOT NULL,
    kind text NOT NULL,
    provider_plugin_id uuid,
    base_url text NOT NULL,
    key_hint text DEFAULT ''::text NOT NULL,
    rpm integer DEFAULT 1000000 NOT NULL,
    concurrency integer DEFAULT 1000 NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    routing_policy jsonb DEFAULT '{}'::jsonb NOT NULL,
    adapter_config jsonb DEFAULT '{}'::jsonb NOT NULL,
    desired_revision integer DEFAULT 1 NOT NULL,
    synced_revision integer DEFAULT 0 NOT NULL,
    sync_status text DEFAULT 'PENDING'::text NOT NULL,
    last_sync_error text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    secret_sync_pending boolean DEFAULT false NOT NULL,
    CONSTRAINT channels_concurrency_check CHECK ((concurrency > 0)),
    CONSTRAINT channels_rpm_check CHECK ((rpm > 0)),
    CONSTRAINT channels_sync_status_check CHECK ((sync_status = ANY (ARRAY['PENDING'::text, 'SYNCED'::text, 'FAILED'::text])))
);

CREATE TABLE public.console_audit_logs (
    id uuid NOT NULL,
    actor_user_id uuid,
    actor_email text,
    org_id uuid,
    action text NOT NULL,
    target_type text NOT NULL,
    target_id text NOT NULL,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE public.model_catalog (
    name text NOT NULL,
    display_name text NOT NULL,
    provider_brand text DEFAULT 'gateway'::text NOT NULL,
    description text DEFAULT ''::text NOT NULL,
    category text DEFAULT 'chat'::text NOT NULL,
    context_length integer DEFAULT 128000 NOT NULL,
    max_output_tokens integer DEFAULT 32768 NOT NULL,
    operations text[] NOT NULL,
    routing_strategy text DEFAULT 'WEIGHTED'::text NOT NULL,
    tags text[] DEFAULT '{}'::text[] NOT NULL,
    list_price_version_id uuid,
    list_rates jsonb DEFAULT '{}'::jsonb NOT NULL,
    currency text DEFAULT 'USD'::text NOT NULL,
    is_public boolean DEFAULT true NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    desired_revision integer DEFAULT 1 NOT NULL,
    synced_revision integer DEFAULT 0 NOT NULL,
    sync_status text DEFAULT 'PENDING'::text NOT NULL,
    last_sync_error text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT model_catalog_context_length_check CHECK ((context_length > 0)),
    CONSTRAINT model_catalog_max_output_tokens_check CHECK ((max_output_tokens > 0)),
    CONSTRAINT model_catalog_routing_strategy_check CHECK ((routing_strategy = ANY (ARRAY['WEIGHTED'::text, 'ROUND_ROBIN'::text, 'LEAST_LATENCY'::text]))),
    CONSTRAINT model_catalog_sync_status_check CHECK ((sync_status = ANY (ARRAY['PENDING'::text, 'SYNCED'::text, 'FAILED'::text])))
);

CREATE TABLE public.org_invitations (
    id uuid NOT NULL,
    org_id uuid NOT NULL,
    email text NOT NULL,
    role text NOT NULL,
    token_hash text NOT NULL,
    invited_by uuid NOT NULL,
    status text DEFAULT 'PENDING'::text NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT org_invitations_role_check CHECK ((role = ANY (ARRAY['ADMIN'::text, 'FINANCE'::text, 'DEVELOPER'::text, 'VIEWER'::text]))),
    CONSTRAINT org_invitations_status_check CHECK ((status = ANY (ARRAY['PENDING'::text, 'ACCEPTED'::text, 'REVOKED'::text, 'EXPIRED'::text])))
);

CREATE TABLE public.org_members (
    org_id uuid NOT NULL,
    user_id uuid NOT NULL,
    role text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT org_members_role_check CHECK ((role = ANY (ARRAY['OWNER'::text, 'ADMIN'::text, 'FINANCE'::text, 'DEVELOPER'::text, 'VIEWER'::text])))
);

CREATE TABLE public.organizations (
    id uuid NOT NULL,
    slug text NOT NULL,
    name text NOT NULL,
    kind text NOT NULL,
    owner_user_id uuid NOT NULL,
    status text DEFAULT 'ACTIVE'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    inference_target_status text DEFAULT 'ACTIVE'::text NOT NULL,
    inference_revision integer DEFAULT 0 NOT NULL,
    inference_synced_revision integer DEFAULT 0 NOT NULL,
    CONSTRAINT organizations_check CHECK (((inference_synced_revision >= 0) AND (inference_synced_revision <= inference_revision))),
    CONSTRAINT organizations_inference_revision_check CHECK ((inference_revision >= 0)),
    CONSTRAINT organizations_inference_target_status_check CHECK ((inference_target_status = ANY (ARRAY['ACTIVE'::text, 'SUSPENDED'::text]))),
    CONSTRAINT organizations_kind_check CHECK ((kind = ANY (ARRAY['PERSONAL'::text, 'ENTERPRISE'::text]))),
    CONSTRAINT organizations_status_check CHECK ((status = ANY (ARRAY['ACTIVE'::text, 'SUSPENDED'::text])))
);

CREATE TABLE public.platform_credit_defaults (
    id smallint NOT NULL,
    default_credit_limit_atoms numeric(38,0) DEFAULT 0 NOT NULL,
    default_wallet_policy text DEFAULT 'GIFT_FIRST'::text NOT NULL,
    updated_by uuid,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT platform_credit_defaults_default_credit_limit_atoms_check CHECK ((default_credit_limit_atoms >= (0)::numeric)),
    CONSTRAINT platform_credit_defaults_default_wallet_policy_check CHECK ((default_wallet_policy = ANY (ARRAY['GIFT_FIRST'::text, 'CASH_FIRST'::text, 'EARLIEST_EXPIRY'::text]))),
    CONSTRAINT platform_credit_defaults_id_check CHECK ((id = 1))
);

CREATE TABLE public.price_version_snapshots (
    id uuid NOT NULL,
    template_id uuid,
    side text NOT NULL,
    currency text DEFAULT 'USD'::text NOT NULL,
    basis text DEFAULT 'PROVIDER_USAGE'::text NOT NULL,
    rates jsonb NOT NULL,
    ceilings jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT price_version_snapshots_basis_check CHECK ((basis = ANY (ARRAY['PROVIDER_USAGE'::text, 'REQUEST_COMPLETION'::text, 'INPUT_CHARACTERS'::text]))),
    CONSTRAINT price_version_snapshots_side_check CHECK ((side = ANY (ARRAY['CUSTOMER'::text, 'SUPPLIER'::text])))
);

CREATE TABLE public.pricing_overrides (
    id uuid NOT NULL,
    billing_account_id uuid NOT NULL,
    model_name text NOT NULL,
    operation text DEFAULT '*'::text NOT NULL,
    override_kind text NOT NULL,
    billing_price_id uuid,
    multiplier_numerator numeric(38,0),
    multiplier_denominator numeric(38,0),
    enabled boolean DEFAULT true NOT NULL,
    desired_revision integer DEFAULT 1 NOT NULL,
    synced_revision integer DEFAULT 0 NOT NULL,
    sync_status text DEFAULT 'PENDING'::text NOT NULL,
    last_sync_error text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT pricing_overrides_check CHECK ((((override_kind = 'EXACT_PRICE'::text) AND (model_name <> '*'::text) AND (billing_price_id IS NOT NULL) AND (multiplier_numerator IS NULL) AND (multiplier_denominator IS NULL)) OR ((override_kind = 'MULTIPLIER'::text) AND (billing_price_id IS NULL) AND (multiplier_numerator IS NOT NULL) AND (multiplier_denominator IS NOT NULL) AND (multiplier_numerator > (0)::numeric) AND (multiplier_denominator > (0)::numeric)))),
    CONSTRAINT pricing_overrides_override_kind_check CHECK ((override_kind = ANY (ARRAY['EXACT_PRICE'::text, 'MULTIPLIER'::text]))),
    CONSTRAINT pricing_overrides_sync_status_check CHECK ((sync_status = ANY (ARRAY['PENDING'::text, 'SYNCED'::text, 'FAILED'::text])))
);

CREATE TABLE public.pricing_templates (
    id uuid NOT NULL,
    name text NOT NULL,
    side text NOT NULL,
    currency text DEFAULT 'USD'::text NOT NULL,
    basis text DEFAULT 'PROVIDER_USAGE'::text NOT NULL,
    rates jsonb NOT NULL,
    ceilings jsonb DEFAULT '{}'::jsonb NOT NULL,
    current_price_version_id uuid,
    pending_price_version_id uuid,
    desired_revision integer DEFAULT 1 NOT NULL,
    synced_revision integer DEFAULT 0 NOT NULL,
    sync_status text DEFAULT 'PENDING'::text NOT NULL,
    last_sync_error text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    pending_price_revision integer,
    CONSTRAINT pricing_templates_basis_check CHECK ((basis = ANY (ARRAY['PROVIDER_USAGE'::text, 'REQUEST_COMPLETION'::text, 'INPUT_CHARACTERS'::text]))),
    CONSTRAINT pricing_templates_side_check CHECK ((side = ANY (ARRAY['CUSTOMER'::text, 'SUPPLIER'::text]))),
    CONSTRAINT pricing_templates_sync_status_check CHECK ((sync_status = ANY (ARRAY['PENDING'::text, 'SYNCED'::text, 'FAILED'::text])))
);

CREATE TABLE public.routes (
    id uuid NOT NULL,
    model_name text NOT NULL,
    channel_id uuid NOT NULL,
    upstream_model text NOT NULL,
    operations text[] NOT NULL,
    priority integer DEFAULT 0 NOT NULL,
    weight double precision DEFAULT 1 NOT NULL,
    billing_price_id uuid NOT NULL,
    cost_version_id uuid NOT NULL,
    request_policy jsonb DEFAULT '{}'::jsonb NOT NULL,
    adapter_overrides jsonb DEFAULT '{}'::jsonb NOT NULL,
    retry_policy jsonb DEFAULT '{}'::jsonb NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    desired_revision integer DEFAULT 1 NOT NULL,
    synced_revision integer DEFAULT 0 NOT NULL,
    sync_status text DEFAULT 'PENDING'::text NOT NULL,
    last_sync_error text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT routes_sync_status_check CHECK ((sync_status = ANY (ARRAY['PENDING'::text, 'SYNCED'::text, 'FAILED'::text]))),
    CONSTRAINT routes_weight_check CHECK ((weight > (0)::double precision))
);

CREATE TABLE public.sessions (
    id uuid NOT NULL,
    user_id uuid NOT NULL,
    token_hash text NOT NULL,
    refresh_token_hash text NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    refresh_expires_at timestamp with time zone NOT NULL,
    revoked_at timestamp with time zone,
    user_agent text,
    ip_address text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    scope text DEFAULT 'CLIENT'::text NOT NULL,
    CONSTRAINT sessions_scope_check CHECK ((scope = ANY (ARRAY['CLIENT'::text, 'ADMIN'::text])))
);

CREATE TABLE public.upstream_accounts (
    id uuid NOT NULL,
    name text NOT NULL,
    state text DEFAULT 'ACTIVE'::text NOT NULL,
    desired_revision integer DEFAULT 1 NOT NULL,
    synced_revision integer DEFAULT 0 NOT NULL,
    sync_status text DEFAULT 'PENDING'::text NOT NULL,
    last_sync_error text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    pending_state text,
    expected_state text,
    CONSTRAINT upstream_accounts_expected_state_check CHECK ((expected_state = ANY (ARRAY['ACTIVE'::text, 'DISABLED'::text, 'RECONCILE_REQUIRED'::text]))),
    CONSTRAINT upstream_accounts_pending_state_check CHECK ((pending_state = ANY (ARRAY['ACTIVE'::text, 'DISABLED'::text, 'RECONCILE_REQUIRED'::text]))),
    CONSTRAINT upstream_accounts_state_check CHECK ((state = ANY (ARRAY['ACTIVE'::text, 'DISABLED'::text, 'RECONCILE_REQUIRED'::text]))),
    CONSTRAINT upstream_accounts_sync_status_check CHECK ((sync_status = ANY (ARRAY['PENDING'::text, 'SYNCED'::text, 'FAILED'::text])))
);

CREATE TABLE public.usage_daily_rollups (
    bucket_day date NOT NULL,
    billing_account_id uuid NOT NULL,
    api_key_id text NOT NULL,
    model text NOT NULL,
    operation text NOT NULL,
    currency text NOT NULL,
    request_count bigint DEFAULT 0 NOT NULL,
    customer_atoms numeric(38,0) DEFAULT 0 NOT NULL,
    supplier_cost_atoms numeric(38,0) DEFAULT 0 NOT NULL,
    input_tokens bigint DEFAULT 0 NOT NULL,
    output_tokens bigint DEFAULT 0 NOT NULL,
    cached_input_tokens bigint DEFAULT 0 NOT NULL,
    reasoning_tokens bigint DEFAULT 0 NOT NULL
);

CREATE TABLE public.usage_events (
    event_id uuid NOT NULL,
    seq bigint,
    kind text NOT NULL,
    request_id uuid,
    charge_id uuid,
    billing_account_id uuid,
    org_id uuid,
    api_key_id uuid,
    model text,
    operation text,
    channel_id uuid,
    outcome text,
    currency text DEFAULT 'USD'::text NOT NULL,
    customer_atoms numeric(38,0) DEFAULT 0 NOT NULL,
    supplier_cost_atoms numeric(38,0) DEFAULT 0 NOT NULL,
    input_tokens bigint DEFAULT 0 NOT NULL,
    output_tokens bigint DEFAULT 0 NOT NULL,
    cached_input_tokens bigint DEFAULT 0 NOT NULL,
    reasoning_tokens bigint DEFAULT 0 NOT NULL,
    pricing_override_id uuid,
    payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    occurred_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE public.usage_hourly_rollups (
    bucket_hour timestamp with time zone NOT NULL,
    billing_account_id uuid NOT NULL,
    api_key_id text NOT NULL,
    model text NOT NULL,
    operation text NOT NULL,
    currency text NOT NULL,
    request_count bigint DEFAULT 0 NOT NULL,
    customer_atoms numeric(38,0) DEFAULT 0 NOT NULL,
    supplier_cost_atoms numeric(38,0) DEFAULT 0 NOT NULL,
    input_tokens bigint DEFAULT 0 NOT NULL,
    output_tokens bigint DEFAULT 0 NOT NULL,
    cached_input_tokens bigint DEFAULT 0 NOT NULL,
    reasoning_tokens bigint DEFAULT 0 NOT NULL
);

CREATE TABLE public.users (
    id uuid NOT NULL,
    email text NOT NULL,
    password_hash text NOT NULL,
    display_name text NOT NULL,
    is_platform_admin boolean DEFAULT false NOT NULL,
    status text DEFAULT 'ACTIVE'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT users_status_check CHECK ((status = ANY (ARRAY['ACTIVE'::text, 'SUSPENDED'::text])))
);

ALTER TABLE ONLY public.alert_deliveries
    ADD CONSTRAINT alert_deliveries_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.alert_eval_queue
    ADD CONSTRAINT alert_eval_queue_pkey PRIMARY KEY (billing_account_id);

ALTER TABLE ONLY public.alert_rules
    ADD CONSTRAINT alert_rules_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.api_key_records
    ADD CONSTRAINT api_key_records_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.channels
    ADD CONSTRAINT channels_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.console_audit_logs
    ADD CONSTRAINT console_audit_logs_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.model_catalog
    ADD CONSTRAINT model_catalog_pkey PRIMARY KEY (name);

ALTER TABLE ONLY public.org_invitations
    ADD CONSTRAINT org_invitations_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.org_invitations
    ADD CONSTRAINT org_invitations_token_hash_key UNIQUE (token_hash);

ALTER TABLE ONLY public.org_members
    ADD CONSTRAINT org_members_pkey PRIMARY KEY (org_id, user_id);

ALTER TABLE ONLY public.organizations
    ADD CONSTRAINT organizations_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.organizations
    ADD CONSTRAINT organizations_slug_key UNIQUE (slug);

ALTER TABLE ONLY public.platform_credit_defaults
    ADD CONSTRAINT platform_credit_defaults_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.price_version_snapshots
    ADD CONSTRAINT price_version_snapshots_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.pricing_overrides
    ADD CONSTRAINT pricing_overrides_billing_account_id_model_name_operation_key UNIQUE (billing_account_id, model_name, operation);

ALTER TABLE ONLY public.pricing_overrides
    ADD CONSTRAINT pricing_overrides_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.pricing_templates
    ADD CONSTRAINT pricing_templates_name_key UNIQUE (name);

ALTER TABLE ONLY public.pricing_templates
    ADD CONSTRAINT pricing_templates_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.billing_accounts
    ADD CONSTRAINT billing_accounts_gateway_billing_account_id_key UNIQUE (gateway_billing_account_id);

ALTER TABLE ONLY public.billing_accounts
    ADD CONSTRAINT billing_accounts_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.routes
    ADD CONSTRAINT routes_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.sessions
    ADD CONSTRAINT sessions_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.sessions
    ADD CONSTRAINT sessions_refresh_token_hash_key UNIQUE (refresh_token_hash);

ALTER TABLE ONLY public.sessions
    ADD CONSTRAINT sessions_token_hash_key UNIQUE (token_hash);

ALTER TABLE ONLY public.upstream_accounts
    ADD CONSTRAINT upstream_accounts_name_key UNIQUE (name);

ALTER TABLE ONLY public.upstream_accounts
    ADD CONSTRAINT upstream_accounts_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.usage_daily_rollups
    ADD CONSTRAINT usage_daily_rollups_pkey PRIMARY KEY (bucket_day, billing_account_id, api_key_id, model, operation, currency);

ALTER TABLE ONLY public.usage_events
    ADD CONSTRAINT usage_events_pkey PRIMARY KEY (event_id);

ALTER TABLE ONLY public.usage_hourly_rollups
    ADD CONSTRAINT usage_hourly_rollups_pkey PRIMARY KEY (bucket_hour, billing_account_id, api_key_id, model, operation, currency);

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_email_key UNIQUE (email);

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_pkey PRIMARY KEY (id);

CREATE INDEX alert_eval_queue_next_attempt_idx ON public.alert_eval_queue USING btree (next_attempt_at, dirty_at, billing_account_id);

CREATE INDEX idx_alert_deliveries_pending ON public.alert_deliveries USING btree (next_attempt_at) WHERE (status = 'PENDING'::text);

CREATE INDEX idx_alert_deliveries_billing_account_time ON public.alert_deliveries USING btree (billing_account_id, created_at DESC);

CREATE INDEX idx_alert_deliveries_rule ON public.alert_deliveries USING btree (rule_id);

CREATE INDEX idx_alert_rules_billing_account_enabled ON public.alert_rules USING btree (billing_account_id) WHERE (enabled = true);

CREATE INDEX idx_api_key_records_billing_account ON public.api_key_records USING btree (billing_account_id, created_at DESC);

CREATE INDEX idx_api_keys_pending_sync ON public.api_key_records USING btree (id) WHERE ((synced_revision > 0) AND ((sync_status <> 'SYNCED'::text) OR (synced_revision < desired_revision)));

CREATE INDEX idx_channels_pending_sync ON public.channels USING btree (id) WHERE ((synced_revision > 0) AND ((sync_status <> 'SYNCED'::text) OR (synced_revision < desired_revision)));

CREATE INDEX idx_console_audit_time ON public.console_audit_logs USING btree (created_at DESC);

CREATE INDEX idx_models_pending_sync ON public.model_catalog USING btree (name) WHERE ((sync_status <> 'SYNCED'::text) OR (synced_revision < desired_revision));

CREATE UNIQUE INDEX idx_org_invitations_pending_email ON public.org_invitations USING btree (org_id, lower(email)) WHERE (status = 'PENDING'::text);

CREATE INDEX idx_org_members_user ON public.org_members USING btree (user_id);

CREATE INDEX idx_overrides_pending_sync ON public.pricing_overrides USING btree (id) WHERE ((sync_status <> 'SYNCED'::text) OR (synced_revision < desired_revision));

CREATE INDEX idx_billing_accounts_org ON public.billing_accounts USING btree (org_id, created_at DESC);

CREATE INDEX idx_billing_accounts_pending_sync ON public.billing_accounts USING btree (id) WHERE ((gateway_synced = true) AND ((sync_status <> 'SYNCED'::text) OR (synced_revision < desired_revision)));

CREATE INDEX idx_routes_model ON public.routes USING btree (model_name, priority DESC);

CREATE INDEX idx_routes_pending_sync ON public.routes USING btree (id) WHERE ((sync_status <> 'SYNCED'::text) OR (synced_revision < desired_revision));

CREATE INDEX idx_sessions_cleanup ON public.sessions USING btree (refresh_expires_at) WHERE (revoked_at IS NULL);

CREATE INDEX idx_sessions_user ON public.sessions USING btree (user_id, created_at DESC);

CREATE INDEX idx_templates_pending_sync ON public.pricing_templates USING btree (id) WHERE ((sync_status <> 'SYNCED'::text) OR (synced_revision < desired_revision));

CREATE INDEX idx_upstream_accounts_pending_sync ON public.upstream_accounts USING btree (id) WHERE ((sync_status <> 'SYNCED'::text) OR (synced_revision < desired_revision));

CREATE INDEX idx_usage_daily_billing_account ON public.usage_daily_rollups USING btree (billing_account_id, bucket_day DESC);

CREATE INDEX idx_usage_events_key_time ON public.usage_events USING btree (api_key_id, occurred_at DESC);

CREATE INDEX idx_usage_events_billing_account_time ON public.usage_events USING btree (billing_account_id, occurred_at DESC);

CREATE INDEX idx_usage_hourly_billing_account ON public.usage_hourly_rollups USING btree (billing_account_id, bucket_hour DESC);

CREATE INDEX organizations_inference_pending_idx ON public.organizations USING btree (updated_at, id) WHERE (inference_synced_revision < inference_revision);

ALTER TABLE ONLY public.alert_deliveries
    ADD CONSTRAINT alert_deliveries_rule_id_fkey FOREIGN KEY (rule_id) REFERENCES public.alert_rules(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.alert_eval_queue
    ADD CONSTRAINT alert_eval_queue_billing_account_id_fkey FOREIGN KEY (billing_account_id) REFERENCES public.billing_accounts(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.alert_rules
    ADD CONSTRAINT alert_rules_billing_account_id_fkey FOREIGN KEY (billing_account_id) REFERENCES public.billing_accounts(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.api_key_records
    ADD CONSTRAINT api_key_records_created_by_fkey FOREIGN KEY (created_by) REFERENCES public.users(id);

ALTER TABLE ONLY public.api_key_records
    ADD CONSTRAINT api_key_records_billing_account_id_fkey FOREIGN KEY (billing_account_id) REFERENCES public.billing_accounts(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.api_key_records
    ADD CONSTRAINT api_key_records_rotated_from_key_id_fkey FOREIGN KEY (rotated_from_key_id) REFERENCES public.api_key_records(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.channels
    ADD CONSTRAINT channels_account_id_fkey FOREIGN KEY (account_id) REFERENCES public.upstream_accounts(id);

ALTER TABLE ONLY public.org_invitations
    ADD CONSTRAINT org_invitations_invited_by_fkey FOREIGN KEY (invited_by) REFERENCES public.users(id);

ALTER TABLE ONLY public.org_invitations
    ADD CONSTRAINT org_invitations_org_id_fkey FOREIGN KEY (org_id) REFERENCES public.organizations(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.org_members
    ADD CONSTRAINT org_members_org_id_fkey FOREIGN KEY (org_id) REFERENCES public.organizations(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.org_members
    ADD CONSTRAINT org_members_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.organizations
    ADD CONSTRAINT organizations_owner_user_id_fkey FOREIGN KEY (owner_user_id) REFERENCES public.users(id);

ALTER TABLE ONLY public.platform_credit_defaults
    ADD CONSTRAINT platform_credit_defaults_updated_by_fkey FOREIGN KEY (updated_by) REFERENCES public.users(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.price_version_snapshots
    ADD CONSTRAINT price_version_snapshots_template_id_fkey FOREIGN KEY (template_id) REFERENCES public.pricing_templates(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.pricing_overrides
    ADD CONSTRAINT pricing_overrides_billing_account_id_fkey FOREIGN KEY (billing_account_id) REFERENCES public.billing_accounts(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.billing_accounts
    ADD CONSTRAINT billing_accounts_org_id_fkey FOREIGN KEY (org_id) REFERENCES public.organizations(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.routes
    ADD CONSTRAINT routes_channel_id_fkey FOREIGN KEY (channel_id) REFERENCES public.channels(id);

ALTER TABLE ONLY public.routes
    ADD CONSTRAINT routes_model_name_fkey FOREIGN KEY (model_name) REFERENCES public.model_catalog(name) ON DELETE CASCADE;

ALTER TABLE ONLY public.sessions
    ADD CONSTRAINT sessions_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;

INSERT INTO public.platform_credit_defaults (id, default_credit_limit_atoms, default_wallet_policy)
VALUES (1, 0, 'GIFT_FIRST');
