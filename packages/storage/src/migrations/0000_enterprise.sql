CREATE TABLE "administrator_credentials" (
	"id" uuid PRIMARY KEY NOT NULL,
	"administrator_id" uuid NOT NULL,
	"verifier" text NOT NULL,
	"prefix" text NOT NULL,
	"enabled" boolean DEFAULT true NOT NULL,
	"expires_at" timestamp with time zone,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "administrator_credentials_verifier_unique" UNIQUE("verifier")
);

CREATE TABLE "channel_plugin_bindings" (
	"id" uuid PRIMARY KEY NOT NULL,
	"channel_id" uuid NOT NULL,
	"plugin_version_id" uuid NOT NULL,
	"config_revision" numeric(38, 0) NOT NULL,
	"config" jsonb NOT NULL,
	"config_digest" text NOT NULL,
	"actor" text NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "channel_plugin_bindings_channel_id_config_revision_unique" UNIQUE("channel_id","config_revision"),
	CONSTRAINT "binding_config" CHECK (jsonb_typeof(config)='object' AND config_revision>=0 AND config_digest ~ '^[a-f0-9]{64}$')
);

CREATE TABLE "channels" (
	"brand_id" uuid NOT NULL,
	"account_id" uuid NOT NULL,
	"binding_id" uuid,
	"config_revision" numeric(38, 0) DEFAULT '0' NOT NULL,
	"id" uuid PRIMARY KEY NOT NULL,
	"name" text NOT NULL,
	"kind" text NOT NULL,
	"base_url" text NOT NULL,
	"secret" text NOT NULL,
	"enabled" boolean DEFAULT true NOT NULL,
	"concurrency" integer NOT NULL,
	"rpm" integer NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "channels_name_key" UNIQUE("name"),
	CONSTRAINT "channels_concurrency_check" CHECK (concurrency > 0),
	CONSTRAINT "channels_rpm_check" CHECK (rpm > 0),
	CONSTRAINT "channels_kind_check" CHECK (kind = ANY (ARRAY['openai'::text, 'anthropic'::text, 'gemini'::text, 'deepseek'::text, 'ollama'::text, 'azure'::text, 'bedrock'::text, 'vertex'::text]))
);

CREATE TABLE "financial_vouchers" (
	"id" uuid PRIMARY KEY NOT NULL,
	"digest" text NOT NULL,
	"content_type" text NOT NULL,
	"content" "bytea" NOT NULL,
	"actor" text NOT NULL,
	"expires_at" timestamp with time zone NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "financial_vouchers_digest_unique" UNIQUE("digest"),
	CONSTRAINT "voucher_size" CHECK (octet_length(content)>0 AND octet_length(content)<=5242880),
	CONSTRAINT "voucher_type" CHECK (content_type IN ('image/png','image/jpeg','application/pdf')),
	CONSTRAINT "voucher_digest" CHECK (digest ~ '^[a-f0-9]{64}$')
);

CREATE TABLE "plugin_packages" (
	"digest" text PRIMARY KEY NOT NULL,
	"package_bytes" "bytea" NOT NULL,
	"manifest" jsonb NOT NULL,
	"signature" text NOT NULL,
	"signing_key_id" text NOT NULL,
	"uploaded_by" text NOT NULL,
	"uploaded_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "plugin_digest" CHECK (digest ~ '^[a-f0-9]{64}$'),
	CONSTRAINT "plugin_package_size" CHECK (octet_length(package_bytes)>0 AND octet_length(package_bytes)<=33554432),
	CONSTRAINT "plugin_manifest" CHECK (jsonb_typeof(manifest)='object')
);

CREATE TABLE "plugin_versions" (
	"id" uuid PRIMARY KEY NOT NULL,
	"name" text NOT NULL,
	"version" text NOT NULL,
	"package_digest" text NOT NULL,
	"abi_version" integer NOT NULL,
	"config_schema_digest" text NOT NULL,
	"state" text DEFAULT 'UPLOADED' NOT NULL,
	"quarantined" boolean DEFAULT false NOT NULL,
	"revision" numeric(38, 0) DEFAULT '0' NOT NULL,
	"validation_result" jsonb,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "plugin_versions_name_version_unique" UNIQUE("name","version"),
	CONSTRAINT "plugin_version_state" CHECK (state IN ('UPLOADED','VALIDATING','VALIDATED','PUBLISHED','REJECTED','DISABLED')),
	CONSTRAINT "plugin_version_revision" CHECK (abi_version>0 AND revision>=0),
	CONSTRAINT "plugin_config_digest" CHECK (config_schema_digest ~ '^[a-f0-9]{64}$')
);

CREATE TABLE "supplier_brands" (
	"id" uuid PRIMARY KEY NOT NULL,
	"name" text NOT NULL,
	"enabled" boolean DEFAULT true NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "supplier_brands_name_unique" UNIQUE("name")
);

CREATE TABLE "technical_administrators" (
	"id" uuid PRIMARY KEY NOT NULL,
	"name" text NOT NULL,
	"enabled" boolean DEFAULT true NOT NULL,
	"permissions" text[] NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE "upstream_account_balances" (
	"account_id" uuid NOT NULL,
	"currency" text NOT NULL,
	"balance_atoms" numeric(38, 0) DEFAULT '0' NOT NULL,
	"reserved_atoms" numeric(38, 0) DEFAULT '0' NOT NULL,
	"spent_atoms" numeric(38, 0) DEFAULT '0' NOT NULL,
	"adjustment_atoms" numeric(38, 0) DEFAULT '0' NOT NULL,
	"revision" numeric(38, 0) DEFAULT '0' NOT NULL,
	CONSTRAINT "upstream_account_balances_account_id_currency_pk" PRIMARY KEY("account_id","currency"),
	CONSTRAINT "upstream_balance_currency" CHECK (currency ~ '^[A-Z]{3,8}$'),
	CONSTRAINT "upstream_balance_nonnegative" CHECK (reserved_atoms>=0 AND spent_atoms>=0 AND revision>=0)
);

CREATE TABLE "upstream_accounts" (
	"id" uuid PRIMARY KEY NOT NULL,
	"brand_id" uuid NOT NULL,
	"name" text NOT NULL,
	"mode" text DEFAULT 'PREPAID' NOT NULL,
	"state" text DEFAULT 'ACTIVE' NOT NULL,
	"revision" numeric(38, 0) DEFAULT '0' NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "upstream_accounts_id_brand_id_unique" UNIQUE("id","brand_id"),
	CONSTRAINT "upstream_accounts_brand_id_name_unique" UNIQUE("brand_id","name"),
	CONSTRAINT "upstream_account_mode" CHECK (mode='PREPAID'),
	CONSTRAINT "upstream_account_state" CHECK (state IN ('ACTIVE','DISABLED','RECONCILE_REQUIRED','QUARANTINED')),
	CONSTRAINT "upstream_account_revision" CHECK (revision>=0)
);

CREATE TABLE "api_keys" (
	"id" uuid PRIMARY KEY NOT NULL,
	"project_id" uuid NOT NULL,
	"name" text NOT NULL,
	"verifier" text NOT NULL,
	"prefix" text NOT NULL,
	"models" text[] NOT NULL,
	"operations" text[] NOT NULL,
	"enabled" boolean DEFAULT true NOT NULL,
	"expires_at" timestamp with time zone,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"principal_id" uuid,
	"role" text DEFAULT 'INVOKER' NOT NULL,
	"allowed_cidrs" text[] DEFAULT '{}'::text[] NOT NULL,
	"rpm" integer,
	"tpm" bigint,
	"concurrency" integer,
	CONSTRAINT "api_keys_verifier_key" UNIQUE("verifier"),
	CONSTRAINT "api_keys_concurrency_check" CHECK (concurrency > 0),
	CONSTRAINT "api_keys_role_check" CHECK (role = ANY (ARRAY['ADMIN'::text, 'INVOKER'::text, 'BILLING'::text, 'VIEWER'::text])),
	CONSTRAINT "api_keys_rpm_check" CHECK (rpm > 0),
	CONSTRAINT "api_keys_tpm_check" CHECK ((tpm > 0) AND (tpm <= '9007199254740991'::bigint))
);

CREATE TABLE "attempts" (
	"binding_id" uuid,
	"id" uuid PRIMARY KEY NOT NULL,
	"request_id" uuid NOT NULL,
	"ordinal" integer NOT NULL,
	"provider_id" uuid NOT NULL,
	"upstream_model" text NOT NULL,
	"pricing_id" uuid NOT NULL,
	"state" text NOT NULL,
	"provider_request_id" text,
	"error_code" text,
	"started_at" timestamp with time zone DEFAULT now() NOT NULL,
	"finished_at" timestamp with time zone,
	"evidence_id" uuid,
	"metering_conflict" boolean DEFAULT false NOT NULL,
	"billing_pricing_id" uuid,
	CONSTRAINT "attempts_request_id_ordinal_key" UNIQUE("request_id","ordinal"),
	CONSTRAINT "attempts_error_code_check" CHECK (error_code ~ '^[a-z0-9_]{1,80}$'::text),
	CONSTRAINT "attempts_ordinal_check" CHECK (ordinal > 0),
	CONSTRAINT "attempts_state_check" CHECK (state = ANY (ARRAY['DISPATCHING'::text, 'SUCCEEDED'::text, 'NOT_EXECUTED'::text, 'UNKNOWN'::text, 'PARTIAL'::text, 'FAILED'::text]))
);

CREATE TABLE "audit_events" (
	"id" uuid PRIMARY KEY NOT NULL,
	"action" text NOT NULL,
	"resource_id" text NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE "billing_adjustments" (
	"id" uuid PRIMARY KEY NOT NULL,
	"charge_id" uuid NOT NULL,
	"external_ref" text NOT NULL,
	"fingerprint" text NOT NULL,
	"target_atoms" numeric(38, 0) NOT NULL,
	"delta_atoms" numeric(38, 0) NOT NULL,
	"reason" text NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "billing_adjustments_charge_id_external_ref_key" UNIQUE("charge_id","external_ref"),
	CONSTRAINT "billing_adjustments_reason_check" CHECK (reason = ANY (ARRAY['ADMIN_CORRECTION'::text, 'PROVIDER_RECONCILIATION'::text])),
	CONSTRAINT "billing_adjustments_target_atoms_check" CHECK (target_atoms >= (0)::numeric)
);

CREATE TABLE "billing_charges" (
	"id" uuid PRIMARY KEY NOT NULL,
	"request_id" uuid NOT NULL,
	"hold_id" uuid NOT NULL,
	"pricing_id" uuid,
	"evidence_id" uuid,
	"currency" text NOT NULL,
	"amount_atoms" numeric(38, 0) NOT NULL,
	"funded_atoms" numeric(38, 0) NOT NULL,
	"debt_atoms" numeric(38, 0) NOT NULL,
	"refunded_atoms" numeric(38, 0) DEFAULT '0' NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"debt_refunded_atoms" numeric(38, 0) DEFAULT '0' NOT NULL,
	CONSTRAINT "billing_charges_request_id_key" UNIQUE("request_id"),
	CONSTRAINT "billing_charges_hold_id_key" UNIQUE("hold_id"),
	CONSTRAINT "billing_charges_amount_atoms_check" CHECK (amount_atoms >= (0)::numeric),
	CONSTRAINT "billing_charges_check" CHECK ((refunded_atoms >= (0)::numeric) AND (refunded_atoms <= amount_atoms)),
	CONSTRAINT "billing_charges_debt_atoms_check" CHECK (debt_atoms >= (0)::numeric),
	CONSTRAINT "billing_charges_debt_refunded_atoms_check" CHECK (debt_refunded_atoms >= (0)::numeric),
	CONSTRAINT "billing_charges_funded_atoms_check" CHECK (funded_atoms >= (0)::numeric),
	CONSTRAINT "charge_value_conserved" CHECK ((amount_atoms = ((funded_atoms + debt_atoms) + debt_refunded_atoms)) AND (refunded_atoms >= debt_refunded_atoms) AND ((refunded_atoms - debt_refunded_atoms) <= funded_atoms))
);

CREATE TABLE "billing_holds" (
	"id" uuid PRIMARY KEY NOT NULL,
	"request_id" uuid NOT NULL,
	"wallet_id" uuid NOT NULL,
	"amount_atoms" numeric(38, 0) NOT NULL,
	"state" text DEFAULT 'HELD' NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"closed_at" timestamp with time zone,
	"next_reconcile_at" timestamp with time zone DEFAULT now() NOT NULL,
	"reconcile_until" timestamp with time zone,
	"reconcile_token" uuid,
	"reconcile_reason" text,
	"credit_atoms" numeric(38, 0) DEFAULT '0' NOT NULL,
	CONSTRAINT "billing_holds_request_id_key" UNIQUE("request_id"),
	CONSTRAINT "billing_holds_amount_atoms_check" CHECK (amount_atoms >= (0)::numeric),
	CONSTRAINT "billing_holds_state_check" CHECK (state = ANY (ARRAY['HELD'::text, 'RECONCILING'::text, 'SETTLED'::text, 'RELEASED'::text])),
	CONSTRAINT "billing_holds_check" CHECK ((credit_atoms >= (0)::numeric) AND (credit_atoms <= amount_atoms))
);

CREATE TABLE "billing_prices" (
	"id" uuid PRIMARY KEY NOT NULL,
	"currency" text NOT NULL,
	"rates" jsonb NOT NULL,
	"ceilings" jsonb NOT NULL,
	"basis" text DEFAULT 'PROVIDER_USAGE' NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "billing_price_currency_unique" UNIQUE("id","currency"),
	CONSTRAINT "billing_prices_basis_check" CHECK (basis = ANY (ARRAY['PROVIDER_USAGE'::text, 'INPUT_CHARACTERS'::text, 'REQUEST_COMPLETION'::text])),
	CONSTRAINT "billing_prices_ceilings_check" CHECK (jsonb_typeof(ceilings) = 'object'::text),
	CONSTRAINT "billing_prices_currency_check" CHECK (currency ~ '^[A-Z]{3,8}$'::text),
	CONSTRAINT "billing_prices_rates_check" CHECK (jsonb_typeof(rates) = 'object'::text)
);

CREATE TABLE "billing_refunds" (
	"id" uuid PRIMARY KEY NOT NULL,
	"charge_id" uuid NOT NULL,
	"external_ref" text NOT NULL,
	"amount_atoms" numeric(38, 0) NOT NULL,
	"fingerprint" text NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"reason" text DEFAULT 'CUSTOMER_REFUND' NOT NULL,
	CONSTRAINT "billing_refunds_charge_id_external_ref_key" UNIQUE("charge_id","external_ref"),
	CONSTRAINT "billing_refunds_amount_atoms_check" CHECK (amount_atoms > (0)::numeric),
	CONSTRAINT "billing_refunds_reason_check" CHECK (reason = ANY (ARRAY['CUSTOMER_REFUND'::text, 'ADMIN_CORRECTION'::text, 'PROVIDER_RECONCILIATION'::text]))
);

CREATE TABLE "billing_resolutions" (
	"id" uuid PRIMARY KEY NOT NULL,
	"request_id" uuid NOT NULL,
	"external_ref" text NOT NULL,
	"fingerprint" text NOT NULL,
	"action" text NOT NULL,
	"usage" jsonb NOT NULL,
	"reason" text NOT NULL,
	"pricing_id" uuid,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"seq" bigint GENERATED ALWAYS AS IDENTITY (sequence name "billing_resolutions_seq_seq" INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1),
	CONSTRAINT "billing_resolutions_request_id_external_ref_key" UNIQUE("request_id","external_ref"),
	CONSTRAINT "billing_resolutions_seq_key" UNIQUE("seq"),
	CONSTRAINT "billing_resolutions_action_check" CHECK (action = ANY (ARRAY['RELEASE'::text, 'SETTLE'::text])),
	CONSTRAINT "billing_resolutions_reason_check" CHECK (reason = ANY (ARRAY['PROVIDER_INVOICE'::text, 'ADMIN_WRITE_OFF'::text, 'EVIDENCE_RECONCILIATION'::text]))
);

CREATE TABLE "billing_tariffs" (
	"id" uuid PRIMARY KEY NOT NULL,
	"model" text NOT NULL,
	"operation" text NOT NULL,
	"provider_id" uuid,
	"currency" text NOT NULL,
	"pricing_id" uuid NOT NULL,
	"enabled" boolean DEFAULT true NOT NULL,
	CONSTRAINT "billing_tariffs_model_operation_provider_id_currency_key" UNIQUE NULLS NOT DISTINCT("model","operation","provider_id","currency"),
	CONSTRAINT "billing_tariffs_currency_check" CHECK (currency ~ '^[A-Z]{3,8}$'::text)
);

CREATE TABLE "charge_allocations" (
	"charge_id" uuid NOT NULL,
	"lot_id" uuid NOT NULL,
	"ordinal" integer NOT NULL,
	"amount_atoms" numeric(38, 0) NOT NULL,
	"refunded_atoms" numeric(38, 0) DEFAULT '0' NOT NULL,
	CONSTRAINT "charge_allocations_pkey" PRIMARY KEY("charge_id","lot_id"),
	CONSTRAINT "charge_allocations_amount_atoms_check" CHECK (amount_atoms > (0)::numeric),
	CONSTRAINT "charge_allocations_check" CHECK ((refunded_atoms >= (0)::numeric) AND (refunded_atoms <= amount_atoms))
);

CREATE TABLE "cost_versions" (
	"id" uuid PRIMARY KEY NOT NULL,
	"currency" text NOT NULL,
	"rates" jsonb NOT NULL,
	"ceilings" jsonb DEFAULT '{}'::jsonb NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "cost_versions_currency_check" CHECK (currency ~ '^[A-Z]{3,8}$'::text),
	CONSTRAINT "cost_versions_ceilings_check" CHECK (jsonb_typeof(ceilings)='object'),
	CONSTRAINT "cost_versions_rates_check" CHECK (jsonb_typeof(rates) = 'object'::text)
);

CREATE TABLE "event_consumers" (
	"id" uuid PRIMARY KEY NOT NULL,
	"name" text NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"replay_cursor" bigint DEFAULT 0 NOT NULL,
	"replay_until" bigint DEFAULT 0 NOT NULL,
	CONSTRAINT "event_consumers_name_key" UNIQUE("name")
);

CREATE TABLE "event_deliveries" (
	"consumer_id" uuid NOT NULL,
	"event_id" uuid NOT NULL,
	"state" text DEFAULT 'PENDING' NOT NULL,
	"lease_token" uuid,
	"lease_until" timestamp with time zone,
	"attempts" integer DEFAULT 0 NOT NULL,
	CONSTRAINT "event_deliveries_pkey" PRIMARY KEY("consumer_id","event_id"),
	CONSTRAINT "event_deliveries_state_check" CHECK (state = ANY (ARRAY['PENDING'::text, 'LEASED'::text, 'ACKED'::text]))
);

CREATE TABLE "funding_operations" (
	"id" uuid PRIMARY KEY NOT NULL,
	"wallet_id" uuid NOT NULL,
	"external_ref" text NOT NULL,
	"fingerprint" text NOT NULL,
	"kind" text NOT NULL,
	"amount_atoms" numeric(38, 0) NOT NULL,
	"lot_id" uuid,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "funding_operations_wallet_id_external_ref_key" UNIQUE("wallet_id","external_ref"),
	CONSTRAINT "funding_operations_amount_atoms_check" CHECK (amount_atoms > (0)::numeric),
	CONSTRAINT "funding_operations_kind_check" CHECK (kind = ANY (ARRAY['CREDIT'::text, 'REFUND'::text]))
);

CREATE TABLE "gateway_requests" (
	"id" uuid PRIMARY KEY NOT NULL,
	"project_id" uuid NOT NULL,
	"key_id" uuid NOT NULL,
	"operation" text NOT NULL,
	"model" text NOT NULL,
	"intent_digest" text,
	"fingerprint" text NOT NULL,
	"state" text NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL,
	"billing_currency" text,
	"billing_quote" jsonb,
	"selected_attempt_id" uuid,
	"billing_outcome" text DEFAULT 'PENDING' NOT NULL,
	CONSTRAINT "gateway_requests_project_id_key_id_operation_intent_digest_key" UNIQUE("project_id","key_id","operation","intent_digest"),
	CONSTRAINT "gateway_requests_billing_outcome_check" CHECK (billing_outcome = ANY (ARRAY['PENDING'::text, 'SUCCESS'::text, 'CANCELLED'::text, 'FAILED'::text, 'UNKNOWN'::text])),
	CONSTRAINT "gateway_requests_state_check" CHECK (state = ANY (ARRAY['ADMITTED'::text, 'RUNNING'::text, 'SUCCEEDED'::text, 'FAILED'::text, 'UNKNOWN'::text, 'CANCELLED'::text]))
);

CREATE TABLE "hold_allocations" (
	"hold_id" uuid NOT NULL,
	"lot_id" uuid NOT NULL,
	"ordinal" integer NOT NULL,
	"amount_atoms" numeric(38, 0) NOT NULL,
	CONSTRAINT "hold_allocations_pkey" PRIMARY KEY("hold_id","lot_id"),
	CONSTRAINT "hold_allocations_amount_atoms_check" CHECK (amount_atoms > (0)::numeric)
);

CREATE TABLE "journal_payloads" (
	"id" uuid NOT NULL,
	"created_at" timestamp with time zone NOT NULL,
	"lines" jsonb NOT NULL,
	CONSTRAINT "journal_payloads_id_created_at_pk" PRIMARY KEY("id","created_at"),
	CONSTRAINT "journal_payloads_lines_check" CHECK (jsonb_typeof(lines)='array')
) PARTITION BY RANGE ("created_at");

CREATE TABLE "models" (
	"name" text PRIMARY KEY NOT NULL,
	"operations" text[] NOT NULL,
	"enabled" boolean DEFAULT true NOT NULL,
	"max_output_tokens" integer NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"routing_strategy" text DEFAULT 'WEIGHTED' NOT NULL,
	CONSTRAINT "models_max_output_tokens_check" CHECK (max_output_tokens > 0),
	CONSTRAINT "models_routing_strategy_check" CHECK (routing_strategy = ANY (ARRAY['WEIGHTED'::text, 'ROUND_ROBIN'::text, 'LEAST_LATENCY'::text]))
);

CREATE TABLE "money_entries" (
	"id" uuid PRIMARY KEY NOT NULL,
	"wallet_id" uuid NOT NULL,
	"reference" text NOT NULL,
	"kind" text NOT NULL,
	"currency" text NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "journal_index_time" UNIQUE("id","created_at"),
	CONSTRAINT "money_journal_wallet_id_reference_key" UNIQUE("wallet_id","reference"),
	CONSTRAINT "money_journal_kind_check" CHECK (kind = ANY (ARRAY['CREDIT'::text, 'HOLD'::text, 'SETTLE'::text, 'RELEASE'::text, 'REFUND'::text, 'EXPIRE'::text, 'DEBT_COLLECTION'::text, 'ADJUSTMENT'::text]))
);

CREATE TABLE "organizations" (
	"id" uuid PRIMARY KEY NOT NULL,
	"external_ref" text NOT NULL,
	"name" text NOT NULL,
	"enabled" boolean DEFAULT true NOT NULL,
	"rpm" integer DEFAULT 100000 NOT NULL,
	"tpm" bigint DEFAULT 1000000000 NOT NULL,
	"concurrency" integer DEFAULT 10000 NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "organizations_external_ref_key" UNIQUE("external_ref"),
	CONSTRAINT "organizations_concurrency_check" CHECK (concurrency > 0),
	CONSTRAINT "organizations_rpm_check" CHECK (rpm > 0),
	CONSTRAINT "organizations_tpm_check" CHECK ((tpm > 0) AND (tpm <= '9007199254740991'::bigint))
);

CREATE TABLE "principals" (
	"id" uuid PRIMARY KEY NOT NULL,
	"organization_id" uuid NOT NULL,
	"external_ref" text NOT NULL,
	"role" text NOT NULL,
	"enabled" boolean DEFAULT true NOT NULL,
	"rpm" integer DEFAULT 10000 NOT NULL,
	"tpm" bigint DEFAULT 100000000 NOT NULL,
	"concurrency" integer DEFAULT 1000 NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "principals_organization_id_external_ref_key" UNIQUE("organization_id","external_ref"),
	CONSTRAINT "principals_concurrency_check" CHECK (concurrency > 0),
	CONSTRAINT "principals_role_check" CHECK (role = ANY (ARRAY['OWNER'::text, 'ADMIN'::text, 'MEMBER'::text, 'BILLING'::text, 'VIEWER'::text])),
	CONSTRAINT "principals_rpm_check" CHECK (rpm > 0),
	CONSTRAINT "principals_tpm_check" CHECK ((tpm > 0) AND (tpm <= '9007199254740991'::bigint))
);

CREATE TABLE "projects" (
	"id" uuid PRIMARY KEY NOT NULL,
	"name" text NOT NULL,
	"external_ref" text,
	"enabled" boolean DEFAULT true NOT NULL,
	"rpm" integer NOT NULL,
	"tpm" integer NOT NULL,
	"concurrency" integer NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"billing_currency" text DEFAULT 'USD' NOT NULL,
	"organization_id" uuid,
	CONSTRAINT "projects_external_ref_key" UNIQUE("external_ref"),
	CONSTRAINT "projects_billing_currency_check" CHECK (billing_currency ~ '^[A-Z]{3,8}$'::text),
	CONSTRAINT "projects_concurrency_check" CHECK (concurrency > 0),
	CONSTRAINT "projects_rpm_check" CHECK (rpm > 0),
	CONSTRAINT "projects_tpm_check" CHECK (tpm > 0)
);

CREATE TABLE "resources" (
	"binding_id" uuid,
	"id" text PRIMARY KEY NOT NULL,
	"project_id" uuid NOT NULL,
	"provider_id" uuid NOT NULL,
	"remote_id" text NOT NULL,
	"kind" text NOT NULL,
	"model" text,
	"state" text DEFAULT 'ACTIVE' NOT NULL,
	"attempt_id" uuid,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL,
	"next_poll_at" timestamp with time zone DEFAULT now() NOT NULL,
	"poll_token" uuid,
	"poll_until" timestamp with time zone,
	"batch_endpoint" text,
	"batch_upstream_model" text,
	"batch_manifest" jsonb,
	"poll_count" integer DEFAULT 0 NOT NULL,
	"upstream_state" text,
	CONSTRAINT "resources_project_id_provider_id_kind_remote_id_key" UNIQUE("project_id","provider_id","remote_id","kind"),
	CONSTRAINT "resource_opaque_identity" CHECK (length(remote_id) BETWEEN 1 AND 500 AND remote_id ~ '^[A-Za-z0-9_:/.-]+$' AND remote_id !~ '://' AND remote_id !~ '(^|/)[.]{1,2}(/|$)'),
	CONSTRAINT "resource_batch_metadata" CHECK (batch_manifest IS NULL OR (jsonb_typeof(batch_manifest)='object' AND batch_manifest-ARRAY['items','idsSha256','images']='{}'::jsonb AND batch_manifest ?& ARRAY['items','idsSha256'] AND jsonb_typeof(batch_manifest->'items')='number' AND batch_manifest->>'items' ~ '^[1-9][0-9]{0,15}$' AND (batch_manifest->>'items')::numeric<=9007199254740991 AND jsonb_typeof(batch_manifest->'idsSha256')='string' AND batch_manifest->>'idsSha256' ~ '^[a-f0-9]{64}$' AND (NOT batch_manifest?'images' OR jsonb_typeof(batch_manifest->'images')='number' AND batch_manifest->>'images' ~ '^[1-9][0-9]{0,15}$' AND (batch_manifest->>'images')::numeric<=9007199254740991)) IS TRUE),
	CONSTRAINT "resources_kind_check" CHECK (kind = ANY (ARRAY['file'::text, 'batch'::text, 'video'::text, 'response'::text, 'operation'::text])),
	CONSTRAINT "resources_upstream_state_check" CHECK (upstream_state = ANY (ARRAY['queued'::text, 'pending'::text, 'in_progress'::text, 'running'::text, 'completed'::text, 'succeeded'::text, 'failed'::text, 'cancelled'::text, 'canceled'::text, 'cancelling'::text, 'validating'::text, 'finalizing'::text, 'expired'::text, 'processed'::text]))
);

CREATE TABLE "routes" (
	"id" uuid PRIMARY KEY NOT NULL,
	"request_policy" jsonb DEFAULT '{}'::jsonb NOT NULL,
	"model_name" text NOT NULL,
	"provider_id" uuid NOT NULL,
	"upstream_model" text NOT NULL,
	"operations" text[] NOT NULL,
	"priority" integer NOT NULL,
	"weight" double precision NOT NULL,
	"pricing_id" uuid NOT NULL,
	"enabled" boolean DEFAULT true NOT NULL,
	CONSTRAINT "routes_binding_unique" UNIQUE("model_name","provider_id","upstream_model","operations"),
	CONSTRAINT "routes_request_policy_check" CHECK (jsonb_typeof(request_policy) = 'object'::text),
	CONSTRAINT "routes_weight_check" CHECK (weight > (0)::double precision)
);

CREATE TABLE "schema_migrations" (
	"version" integer PRIMARY KEY NOT NULL,
	"checksum" text NOT NULL,
	"manifest" jsonb NOT NULL,
	"applied_at" timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE "usage_conflicts" (
	"id" uuid PRIMARY KEY NOT NULL,
	"attempt_id" uuid NOT NULL,
	"existing_event_id" uuid NOT NULL,
	"source" text NOT NULL,
	"finality" text NOT NULL,
	"usage" jsonb NOT NULL,
	"fingerprint" text NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "usage_conflicts_attempt_id_source_fingerprint_key" UNIQUE("attempt_id","source","fingerprint")
);

CREATE TABLE "usage_event_index" (
	"id" uuid PRIMARY KEY NOT NULL,
	"seq" bigint GENERATED ALWAYS AS IDENTITY (sequence name "usage_events_seq_seq" INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1),
	"project_id" uuid NOT NULL,
	"request_id" uuid NOT NULL,
	"attempt_id" uuid NOT NULL,
	"source" text NOT NULL,
	"finality" text NOT NULL,
	"cost_atoms" numeric(38, 0) NOT NULL,
	"currency" text NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"cost_complete" boolean DEFAULT false NOT NULL,
	CONSTRAINT "usage_index_time" UNIQUE("id","created_at"),
	CONSTRAINT "usage_events_seq_key" UNIQUE("seq"),
	CONSTRAINT "usage_events_attempt_id_source_key" UNIQUE("attempt_id","source"),
	CONSTRAINT "usage_events_cost_atoms_check" CHECK (cost_atoms >= (0)::numeric),
	CONSTRAINT "usage_events_finality_check" CHECK (finality = ANY (ARRAY['FINAL'::text, 'PARTIAL'::text, 'UNKNOWN'::text, 'ESTIMATED'::text]))
);

CREATE TABLE "usage_payloads" (
	"id" uuid NOT NULL,
	"created_at" timestamp with time zone NOT NULL,
	"usage" jsonb NOT NULL,
	CONSTRAINT "usage_payloads_id_created_at_pk" PRIMARY KEY("id","created_at"),
	CONSTRAINT "usage_payloads_usage_check" CHECK (jsonb_typeof(usage)='object')
) PARTITION BY RANGE ("created_at");

CREATE TABLE "wallet_credit_policies" (
	"id" uuid PRIMARY KEY NOT NULL,
	"wallet_id" uuid NOT NULL,
	"external_ref" text NOT NULL,
	"fingerprint" text NOT NULL,
	"credit_limit_atoms" numeric(38, 0) NOT NULL,
	"version" numeric(38, 0) NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "wallet_credit_policies_wallet_id_external_ref_key" UNIQUE("wallet_id","external_ref"),
	CONSTRAINT "wallet_credit_policies_wallet_id_version_key" UNIQUE("wallet_id","version"),
	CONSTRAINT "wallet_credit_policies_external_ref_check" CHECK ((length(external_ref) >= 1) AND (length(external_ref) <= 200)),
	CONSTRAINT "wallet_credit_policies_credit_limit_atoms_check" CHECK (credit_limit_atoms >= (0)::numeric),
	CONSTRAINT "wallet_credit_policies_version_check" CHECK (version > (0)::numeric)
);

CREATE TABLE "wallet_disputes" (
	"wallet_id" uuid NOT NULL,
	"request_id" uuid NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "wallet_disputes_pkey" PRIMARY KEY("wallet_id","request_id")
);

CREATE TABLE "wallet_lots" (
	"id" uuid PRIMARY KEY NOT NULL,
	"wallet_id" uuid NOT NULL,
	"kind" text NOT NULL,
	"bucket_digest" text NOT NULL,
	"priority" integer DEFAULT 100 NOT NULL,
	"expires_at" timestamp with time zone,
	"models" text[] DEFAULT '{}'::text[] NOT NULL,
	"operations" text[] DEFAULT '{}'::text[] NOT NULL,
	"issued_atoms" numeric(38, 0) NOT NULL,
	"available_atoms" numeric(38, 0) DEFAULT '0' NOT NULL,
	"reserved_atoms" numeric(38, 0) DEFAULT '0' NOT NULL,
	"spent_atoms" numeric(38, 0) DEFAULT '0' NOT NULL,
	"expired_atoms" numeric(38, 0) DEFAULT '0' NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "wallet_lots_wallet_id_bucket_digest_key" UNIQUE("wallet_id","bucket_digest"),
	CONSTRAINT "wallet_lots_available_atoms_check" CHECK (available_atoms >= (0)::numeric),
	CONSTRAINT "wallet_lots_check" CHECK (issued_atoms = (((available_atoms + reserved_atoms) + spent_atoms) + expired_atoms)),
	CONSTRAINT "wallet_lots_check1" CHECK ((kind = 'GIFT'::text) OR (expires_at IS NULL)),
	CONSTRAINT "wallet_lots_expired_atoms_check" CHECK (expired_atoms >= (0)::numeric),
	CONSTRAINT "wallet_lots_issued_atoms_check" CHECK (issued_atoms >= (0)::numeric),
	CONSTRAINT "wallet_lots_kind_check" CHECK (kind = ANY (ARRAY['CASH'::text, 'GIFT'::text])),
	CONSTRAINT "wallet_lots_reserved_atoms_check" CHECK (reserved_atoms >= (0)::numeric),
	CONSTRAINT "wallet_lots_spent_atoms_check" CHECK (spent_atoms >= (0)::numeric)
);

CREATE TABLE "wallets" (
	"id" uuid PRIMARY KEY NOT NULL,
	"project_id" uuid NOT NULL,
	"currency" text NOT NULL,
	"policy" text DEFAULT 'GIFT_FIRST' NOT NULL,
	"available_atoms" numeric(38, 0) DEFAULT '0' NOT NULL,
	"reserved_atoms" numeric(38, 0) DEFAULT '0' NOT NULL,
	"spent_atoms" numeric(38, 0) DEFAULT '0' NOT NULL,
	"expired_atoms" numeric(38, 0) DEFAULT '0' NOT NULL,
	"debt_atoms" numeric(38, 0) DEFAULT '0' NOT NULL,
	"freeze_reason" text,
	"has_expiring_lots" boolean DEFAULT false NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"manual_frozen" boolean DEFAULT false NOT NULL,
	"evidence_disputed" boolean DEFAULT false NOT NULL,
	"credit_limit_atoms" numeric(38, 0) DEFAULT '0' NOT NULL,
	"credit_reserved_atoms" numeric(38, 0) DEFAULT '0' NOT NULL,
	"credit_policy_version" numeric(38, 0) DEFAULT '0' NOT NULL,
	CONSTRAINT "wallets_project_id_currency_key" UNIQUE("project_id","currency"),
	CONSTRAINT "wallets_available_atoms_check" CHECK (available_atoms >= (0)::numeric),
	CONSTRAINT "wallets_currency_check" CHECK (currency ~ '^[A-Z]{3,8}$'::text),
	CONSTRAINT "wallets_debt_atoms_check" CHECK (debt_atoms >= (0)::numeric),
	CONSTRAINT "wallets_expired_atoms_check" CHECK (expired_atoms >= (0)::numeric),
	CONSTRAINT "wallets_freeze_reason_check" CHECK (freeze_reason = ANY (ARRAY['MANUAL'::text, 'DEBT'::text, 'EVIDENCE_CONFLICT'::text])),
	CONSTRAINT "wallets_policy_check" CHECK (policy = ANY (ARRAY['GIFT_FIRST'::text, 'CASH_FIRST'::text, 'EARLIEST_EXPIRY'::text])),
	CONSTRAINT "wallets_reserved_atoms_check" CHECK (reserved_atoms >= (0)::numeric),
	CONSTRAINT "wallets_spent_atoms_check" CHECK (spent_atoms >= (0)::numeric),
	CONSTRAINT "wallets_credit_limit_atoms_check" CHECK (credit_limit_atoms >= (0)::numeric),
	CONSTRAINT "wallets_credit_reserved_atoms_check" CHECK (credit_reserved_atoms >= (0)::numeric),
	CONSTRAINT "wallets_credit_policy_version_check" CHECK (credit_policy_version >= (0)::numeric)
);

CREATE TABLE "balance_observations" (
	"id" uuid PRIMARY KEY NOT NULL,
	"account_id" uuid NOT NULL,
	"channel_id" uuid,
	"binding_id" uuid,
	"currency" text NOT NULL,
	"observed_atoms" numeric(38, 0),
	"observed_at" timestamp with time zone NOT NULL,
	"source" text NOT NULL,
	"result" text NOT NULL,
	"code" text,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "observation_currency" CHECK (currency ~ '^[A-Z]{3,8}$'),
	CONSTRAINT "observation_result" CHECK (result IN ('OBSERVED','UNSUPPORTED','FAILED'))
);

CREATE TABLE "execution_commands" (
	"id" uuid PRIMARY KEY NOT NULL,
	"command_id" uuid NOT NULL,
	"kind" text NOT NULL,
	"caller_identity" text NOT NULL,
	"caller_incarnation" uuid NOT NULL,
	"body_digest" text NOT NULL,
	"deadline_at" timestamp with time zone NOT NULL,
	"trace_id" text NOT NULL,
	"contract_version" integer NOT NULL,
	"state" text NOT NULL,
	"request_id" uuid,
	"attempt_id" uuid,
	"rejection_code" text,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "execution_commands_kind_caller_identity_command_id_unique" UNIQUE("kind","caller_identity","command_id"),
	CONSTRAINT "command_state" CHECK (state IN ('COMMITTED','REJECTED')),
	CONSTRAINT "command_digest" CHECK (body_digest ~ '^[a-f0-9]{64}$' AND contract_version>0)
);

CREATE TABLE "execution_permits" (
	"attempt_id" uuid PRIMARY KEY NOT NULL,
	"command_id" uuid NOT NULL,
	"gateway_instance_id" text NOT NULL,
	"incarnation_id" uuid NOT NULL,
	"permit_epoch" numeric(38, 0) NOT NULL,
	"binding_id" uuid NOT NULL,
	"dispatched_digest" text NOT NULL,
	"state" text NOT NULL,
	"expires_at" timestamp with time zone NOT NULL,
	"granted_at" timestamp with time zone,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "permit_state" CHECK (state IN ('RESERVED','SEND_GRANTED','ABORTED','UNKNOWN','TERMINAL')),
	CONSTRAINT "permit_epoch" CHECK (permit_epoch>=0 AND dispatched_digest ~ '^[a-f0-9]{64}$')
);

CREATE TABLE "reconciliations" (
	"id" uuid PRIMARY KEY NOT NULL,
	"account_id" uuid NOT NULL,
	"currency" text NOT NULL,
	"revision" numeric(38, 0) NOT NULL,
	"book_atoms" numeric(38, 0) NOT NULL,
	"observed_atoms" numeric(38, 0),
	"invoice_atoms" numeric(38, 0),
	"difference_atoms" numeric(38, 0) NOT NULL,
	"voucher_id" uuid,
	"adjustment_id" uuid,
	"actor" text NOT NULL,
	"reason" text NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "reconciliations_account_id_currency_revision_unique" UNIQUE("account_id","currency","revision"),
	CONSTRAINT "reconciliation_revision" CHECK (revision>=0 AND currency ~ '^[A-Z]{3,8}$')
);

CREATE TABLE "upstream_audit_events" (
	"id" uuid PRIMARY KEY NOT NULL,
	"account_id" uuid,
	"channel_id" uuid,
	"actor" text NOT NULL,
	"action" text NOT NULL,
	"operation_id" uuid,
	"revision" numeric(38, 0),
	"old_digest" text,
	"new_digest" text,
	"reason" text NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE "upstream_cost_heads" (
	"attempt_id" uuid PRIMARY KEY NOT NULL,
	"revision" numeric(38, 0) NOT NULL
);

CREATE TABLE "upstream_costs" (
	"attempt_id" uuid NOT NULL,
	"revision" numeric(38, 0) NOT NULL,
	"currency" text NOT NULL,
	"cost_atoms" numeric(38, 0) NOT NULL,
	"complete" boolean NOT NULL,
	"disputed" boolean DEFAULT false NOT NULL,
	"evidence_digest" text NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "upstream_costs_attempt_id_revision_pk" PRIMARY KEY("attempt_id","revision"),
	CONSTRAINT "upstream_cost_exact" CHECK (revision>=0 AND cost_atoms>=0 AND evidence_digest ~ '^[a-f0-9]{64}$')
);

CREATE TABLE "upstream_entries" (
	"journal_id" uuid NOT NULL,
	"ordinal" integer NOT NULL,
	"currency" text NOT NULL,
	"account" text NOT NULL,
	"atoms" numeric(38, 0) NOT NULL,
	CONSTRAINT "upstream_entries_journal_id_ordinal_pk" PRIMARY KEY("journal_id","ordinal"),
	CONSTRAINT "upstream_entry_ordinal" CHECK (ordinal>=0)
);

CREATE TABLE "upstream_funding_operations" (
	"id" uuid PRIMARY KEY NOT NULL,
	"account_id" uuid NOT NULL,
	"kind" text NOT NULL,
	"external_ref" text NOT NULL,
	"payload_digest" text NOT NULL,
	"payment_currency" text,
	"payment_atoms" numeric(38, 0),
	"credited_currency" text NOT NULL,
	"credited_atoms" numeric(38, 0) NOT NULL,
	"voucher_id" uuid,
	"voucher_digest" text,
	"completed_at" timestamp with time zone NOT NULL,
	"actor" text NOT NULL,
	"reason" text NOT NULL,
	"receipt" jsonb NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "upstream_funding_operations_account_id_kind_external_ref_unique" UNIQUE("account_id","kind","external_ref"),
	CONSTRAINT "upstream_funding_kind" CHECK (kind IN ('RECHARGE','ADJUSTMENT','TEST_OPENING')),
	CONSTRAINT "upstream_funding_ref" CHECK (length(external_ref) BETWEEN 1 AND 200 AND payload_digest ~ '^[a-f0-9]{64}$'),
	CONSTRAINT "upstream_funding_currency" CHECK (credited_currency ~ '^[A-Z]{3,8}$' AND (payment_currency IS NULL OR payment_currency ~ '^[A-Z]{3,8}$')),
	CONSTRAINT "upstream_recharge_fact" CHECK (kind<>'RECHARGE' OR (payment_atoms IS NOT NULL AND payment_atoms>0 AND credited_atoms>0 AND voucher_id IS NOT NULL AND voucher_digest IS NOT NULL AND payment_currency IS NOT NULL))
);

CREATE TABLE "upstream_holds" (
	"attempt_id" uuid PRIMARY KEY NOT NULL,
	"account_id" uuid NOT NULL,
	"channel_id" uuid NOT NULL,
	"currency" text NOT NULL,
	"estimated_atoms" numeric(38, 0) NOT NULL,
	"cost_version_id" uuid NOT NULL,
	"state" text NOT NULL,
	"final_cost_atoms" numeric(38, 0),
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "upstream_hold_state" CHECK (state IN ('HELD','UNKNOWN','PARTIAL','SETTLED','RELEASED')),
	CONSTRAINT "upstream_hold_amounts" CHECK (estimated_atoms>=0 AND (final_cost_atoms IS NULL OR final_cost_atoms>=0))
);

CREATE TABLE "upstream_journals" (
	"id" uuid PRIMARY KEY NOT NULL,
	"account_id" uuid NOT NULL,
	"channel_id" uuid,
	"attempt_id" uuid,
	"operation_id" uuid,
	"currency" text NOT NULL,
	"kind" text NOT NULL,
	"reference" text NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "upstream_journals_reference_unique" UNIQUE("reference"),
	CONSTRAINT "upstream_journals_id_currency_unique" UNIQUE("id","currency"),
	CONSTRAINT "upstream_journal_currency" CHECK (currency ~ '^[A-Z]{3,8}$')
);

ALTER TABLE "administrator_credentials" ADD CONSTRAINT "administrator_credentials_administrator_id_technical_administrators_id_fk" FOREIGN KEY ("administrator_id") REFERENCES "public"."technical_administrators"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "channel_plugin_bindings" ADD CONSTRAINT "channel_plugin_bindings_channel_id_channels_id_fk" FOREIGN KEY ("channel_id") REFERENCES "public"."channels"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "channel_plugin_bindings" ADD CONSTRAINT "channel_plugin_bindings_plugin_version_id_plugin_versions_id_fk" FOREIGN KEY ("plugin_version_id") REFERENCES "public"."plugin_versions"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "channels" ADD CONSTRAINT "channels_brand_id_supplier_brands_id_fk" FOREIGN KEY ("brand_id") REFERENCES "public"."supplier_brands"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "channels" ADD CONSTRAINT "channels_binding_id_channel_plugin_bindings_id_fk" FOREIGN KEY ("binding_id") REFERENCES "public"."channel_plugin_bindings"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "channels" ADD CONSTRAINT "channel_account_brand" FOREIGN KEY ("account_id","brand_id") REFERENCES "public"."upstream_accounts"("id","brand_id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "plugin_versions" ADD CONSTRAINT "plugin_versions_package_digest_plugin_packages_digest_fk" FOREIGN KEY ("package_digest") REFERENCES "public"."plugin_packages"("digest") ON DELETE no action ON UPDATE no action;
ALTER TABLE "upstream_account_balances" ADD CONSTRAINT "upstream_account_balances_account_id_upstream_accounts_id_fk" FOREIGN KEY ("account_id") REFERENCES "public"."upstream_accounts"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "upstream_accounts" ADD CONSTRAINT "upstream_accounts_brand_id_supplier_brands_id_fk" FOREIGN KEY ("brand_id") REFERENCES "public"."supplier_brands"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "api_keys" ADD CONSTRAINT "api_keys_principal_id_fkey" FOREIGN KEY ("principal_id") REFERENCES "public"."principals"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "api_keys" ADD CONSTRAINT "api_keys_project_id_fkey" FOREIGN KEY ("project_id") REFERENCES "public"."projects"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "attempts" ADD CONSTRAINT "attempts_binding_id_channel_plugin_bindings_id_fk" FOREIGN KEY ("binding_id") REFERENCES "public"."channel_plugin_bindings"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "attempts" ADD CONSTRAINT "attempts_billing_pricing_id_fkey" FOREIGN KEY ("billing_pricing_id") REFERENCES "public"."billing_prices"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "attempts" ADD CONSTRAINT "attempts_evidence_id_fkey" FOREIGN KEY ("evidence_id") REFERENCES "public"."usage_event_index"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "attempts" ADD CONSTRAINT "attempts_pricing_id_fkey" FOREIGN KEY ("pricing_id") REFERENCES "public"."cost_versions"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "attempts" ADD CONSTRAINT "attempts_provider_id_fkey" FOREIGN KEY ("provider_id") REFERENCES "public"."channels"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "attempts" ADD CONSTRAINT "attempts_request_id_fkey" FOREIGN KEY ("request_id") REFERENCES "public"."gateway_requests"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "billing_adjustments" ADD CONSTRAINT "billing_adjustments_charge_id_fkey" FOREIGN KEY ("charge_id") REFERENCES "public"."billing_charges"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "billing_charges" ADD CONSTRAINT "billing_charges_evidence_id_fkey" FOREIGN KEY ("evidence_id") REFERENCES "public"."usage_event_index"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "billing_charges" ADD CONSTRAINT "billing_charges_hold_id_fkey" FOREIGN KEY ("hold_id") REFERENCES "public"."billing_holds"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "billing_charges" ADD CONSTRAINT "billing_charges_pricing_id_fkey" FOREIGN KEY ("pricing_id") REFERENCES "public"."billing_prices"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "billing_charges" ADD CONSTRAINT "billing_charges_request_id_fkey" FOREIGN KEY ("request_id") REFERENCES "public"."gateway_requests"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "billing_holds" ADD CONSTRAINT "billing_holds_request_id_fkey" FOREIGN KEY ("request_id") REFERENCES "public"."gateway_requests"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "billing_holds" ADD CONSTRAINT "billing_holds_wallet_id_fkey" FOREIGN KEY ("wallet_id") REFERENCES "public"."wallets"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "billing_refunds" ADD CONSTRAINT "billing_refunds_charge_id_fkey" FOREIGN KEY ("charge_id") REFERENCES "public"."billing_charges"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "billing_resolutions" ADD CONSTRAINT "billing_resolutions_pricing_id_fkey" FOREIGN KEY ("pricing_id") REFERENCES "public"."billing_prices"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "billing_resolutions" ADD CONSTRAINT "billing_resolutions_request_id_fkey" FOREIGN KEY ("request_id") REFERENCES "public"."gateway_requests"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "billing_tariffs" ADD CONSTRAINT "billing_tariffs_model_fkey" FOREIGN KEY ("model") REFERENCES "public"."models"("name") ON DELETE no action ON UPDATE no action;
ALTER TABLE "billing_tariffs" ADD CONSTRAINT "billing_tariffs_pricing_id_fkey" FOREIGN KEY ("pricing_id") REFERENCES "public"."billing_prices"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "billing_tariffs" ADD CONSTRAINT "billing_tariffs_provider_id_fkey" FOREIGN KEY ("provider_id") REFERENCES "public"."channels"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "billing_tariffs" ADD CONSTRAINT "tariff_currency_matches" FOREIGN KEY ("pricing_id","currency") REFERENCES "public"."billing_prices"("id","currency") ON DELETE no action ON UPDATE no action;
ALTER TABLE "charge_allocations" ADD CONSTRAINT "charge_allocations_charge_id_fkey" FOREIGN KEY ("charge_id") REFERENCES "public"."billing_charges"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "charge_allocations" ADD CONSTRAINT "charge_allocations_lot_id_fkey" FOREIGN KEY ("lot_id") REFERENCES "public"."wallet_lots"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "event_deliveries" ADD CONSTRAINT "event_deliveries_consumer_id_fkey" FOREIGN KEY ("consumer_id") REFERENCES "public"."event_consumers"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "event_deliveries" ADD CONSTRAINT "event_deliveries_event_id_fkey" FOREIGN KEY ("event_id") REFERENCES "public"."usage_event_index"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "funding_operations" ADD CONSTRAINT "funding_operations_lot_id_fkey" FOREIGN KEY ("lot_id") REFERENCES "public"."wallet_lots"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "funding_operations" ADD CONSTRAINT "funding_operations_wallet_id_fkey" FOREIGN KEY ("wallet_id") REFERENCES "public"."wallets"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "gateway_requests" ADD CONSTRAINT "gateway_requests_key_id_fkey" FOREIGN KEY ("key_id") REFERENCES "public"."api_keys"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "gateway_requests" ADD CONSTRAINT "gateway_requests_project_id_fkey" FOREIGN KEY ("project_id") REFERENCES "public"."projects"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "gateway_requests" ADD CONSTRAINT "gateway_requests_selected_attempt_id_fkey" FOREIGN KEY ("selected_attempt_id") REFERENCES "public"."attempts"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "hold_allocations" ADD CONSTRAINT "hold_allocations_hold_id_fkey" FOREIGN KEY ("hold_id") REFERENCES "public"."billing_holds"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "hold_allocations" ADD CONSTRAINT "hold_allocations_lot_id_fkey" FOREIGN KEY ("lot_id") REFERENCES "public"."wallet_lots"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "journal_payloads" ADD CONSTRAINT "journal_payloads_id_created_at_fkey" FOREIGN KEY ("id","created_at") REFERENCES "public"."money_entries"("id","created_at") ON DELETE no action ON UPDATE no action;
ALTER TABLE "money_entries" ADD CONSTRAINT "money_journal_wallet_id_fkey" FOREIGN KEY ("wallet_id") REFERENCES "public"."wallets"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "money_entries" ADD CONSTRAINT "journal_index_payload" FOREIGN KEY ("id","created_at") REFERENCES "public"."journal_payloads"("id","created_at") ON DELETE no action ON UPDATE no action;
ALTER TABLE "principals" ADD CONSTRAINT "principals_organization_id_fkey" FOREIGN KEY ("organization_id") REFERENCES "public"."organizations"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "projects" ADD CONSTRAINT "projects_organization_id_fkey" FOREIGN KEY ("organization_id") REFERENCES "public"."organizations"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "resources" ADD CONSTRAINT "resources_binding_id_channel_plugin_bindings_id_fk" FOREIGN KEY ("binding_id") REFERENCES "public"."channel_plugin_bindings"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "resources" ADD CONSTRAINT "resources_attempt_id_fkey" FOREIGN KEY ("attempt_id") REFERENCES "public"."attempts"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "resources" ADD CONSTRAINT "resources_project_id_fkey" FOREIGN KEY ("project_id") REFERENCES "public"."projects"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "resources" ADD CONSTRAINT "resources_provider_id_fkey" FOREIGN KEY ("provider_id") REFERENCES "public"."channels"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "routes" ADD CONSTRAINT "routes_model_name_fkey" FOREIGN KEY ("model_name") REFERENCES "public"."models"("name") ON DELETE no action ON UPDATE no action;
ALTER TABLE "routes" ADD CONSTRAINT "routes_pricing_id_fkey" FOREIGN KEY ("pricing_id") REFERENCES "public"."cost_versions"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "routes" ADD CONSTRAINT "routes_provider_id_fkey" FOREIGN KEY ("provider_id") REFERENCES "public"."channels"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "usage_conflicts" ADD CONSTRAINT "usage_conflicts_attempt_id_fkey" FOREIGN KEY ("attempt_id") REFERENCES "public"."attempts"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "usage_conflicts" ADD CONSTRAINT "usage_conflicts_existing_event_id_fkey" FOREIGN KEY ("existing_event_id") REFERENCES "public"."usage_event_index"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "usage_event_index" ADD CONSTRAINT "usage_events_attempt_id_fkey" FOREIGN KEY ("attempt_id") REFERENCES "public"."attempts"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "usage_event_index" ADD CONSTRAINT "usage_events_project_id_fkey" FOREIGN KEY ("project_id") REFERENCES "public"."projects"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "usage_event_index" ADD CONSTRAINT "usage_events_request_id_fkey" FOREIGN KEY ("request_id") REFERENCES "public"."gateway_requests"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "usage_event_index" ADD CONSTRAINT "usage_index_payload" FOREIGN KEY ("id","created_at") REFERENCES "public"."usage_payloads"("id","created_at") ON DELETE no action ON UPDATE no action;
ALTER TABLE "usage_payloads" ADD CONSTRAINT "usage_payloads_id_created_at_fkey" FOREIGN KEY ("id","created_at") REFERENCES "public"."usage_event_index"("id","created_at") ON DELETE no action ON UPDATE no action;
ALTER TABLE "wallet_credit_policies" ADD CONSTRAINT "wallet_credit_policies_wallet_id_fkey" FOREIGN KEY ("wallet_id") REFERENCES "public"."wallets"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "wallet_lots" ADD CONSTRAINT "wallet_lots_wallet_id_fkey" FOREIGN KEY ("wallet_id") REFERENCES "public"."wallets"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "wallets" ADD CONSTRAINT "wallets_project_id_fkey" FOREIGN KEY ("project_id") REFERENCES "public"."projects"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "balance_observations" ADD CONSTRAINT "balance_observations_account_id_upstream_accounts_id_fk" FOREIGN KEY ("account_id") REFERENCES "public"."upstream_accounts"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "balance_observations" ADD CONSTRAINT "balance_observations_channel_id_channels_id_fk" FOREIGN KEY ("channel_id") REFERENCES "public"."channels"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "balance_observations" ADD CONSTRAINT "balance_observations_binding_id_channel_plugin_bindings_id_fk" FOREIGN KEY ("binding_id") REFERENCES "public"."channel_plugin_bindings"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "execution_commands" ADD CONSTRAINT "execution_commands_request_id_gateway_requests_id_fk" FOREIGN KEY ("request_id") REFERENCES "public"."gateway_requests"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "execution_commands" ADD CONSTRAINT "execution_commands_attempt_id_attempts_id_fk" FOREIGN KEY ("attempt_id") REFERENCES "public"."attempts"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "execution_permits" ADD CONSTRAINT "execution_permits_attempt_id_attempts_id_fk" FOREIGN KEY ("attempt_id") REFERENCES "public"."attempts"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "execution_permits" ADD CONSTRAINT "execution_permits_command_id_execution_commands_id_fk" FOREIGN KEY ("command_id") REFERENCES "public"."execution_commands"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "execution_permits" ADD CONSTRAINT "execution_permits_binding_id_channel_plugin_bindings_id_fk" FOREIGN KEY ("binding_id") REFERENCES "public"."channel_plugin_bindings"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "reconciliations" ADD CONSTRAINT "reconciliations_account_id_upstream_accounts_id_fk" FOREIGN KEY ("account_id") REFERENCES "public"."upstream_accounts"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "reconciliations" ADD CONSTRAINT "reconciliations_voucher_id_financial_vouchers_id_fk" FOREIGN KEY ("voucher_id") REFERENCES "public"."financial_vouchers"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "reconciliations" ADD CONSTRAINT "reconciliations_adjustment_id_upstream_funding_operations_id_fk" FOREIGN KEY ("adjustment_id") REFERENCES "public"."upstream_funding_operations"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "upstream_audit_events" ADD CONSTRAINT "upstream_audit_events_account_id_upstream_accounts_id_fk" FOREIGN KEY ("account_id") REFERENCES "public"."upstream_accounts"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "upstream_audit_events" ADD CONSTRAINT "upstream_audit_events_channel_id_channels_id_fk" FOREIGN KEY ("channel_id") REFERENCES "public"."channels"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "upstream_audit_events" ADD CONSTRAINT "upstream_audit_events_operation_id_upstream_funding_operations_id_fk" FOREIGN KEY ("operation_id") REFERENCES "public"."upstream_funding_operations"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "upstream_cost_heads" ADD CONSTRAINT "upstream_cost_heads_attempt_id_revision_upstream_costs_attempt_id_revision_fk" FOREIGN KEY ("attempt_id","revision") REFERENCES "public"."upstream_costs"("attempt_id","revision") ON DELETE no action ON UPDATE no action;
ALTER TABLE "upstream_costs" ADD CONSTRAINT "upstream_costs_attempt_id_attempts_id_fk" FOREIGN KEY ("attempt_id") REFERENCES "public"."attempts"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "upstream_entries" ADD CONSTRAINT "upstream_entries_journal_id_currency_upstream_journals_id_currency_fk" FOREIGN KEY ("journal_id","currency") REFERENCES "public"."upstream_journals"("id","currency") ON DELETE no action ON UPDATE no action;
ALTER TABLE "upstream_funding_operations" ADD CONSTRAINT "upstream_funding_operations_account_id_upstream_accounts_id_fk" FOREIGN KEY ("account_id") REFERENCES "public"."upstream_accounts"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "upstream_funding_operations" ADD CONSTRAINT "upstream_funding_operations_voucher_id_financial_vouchers_id_fk" FOREIGN KEY ("voucher_id") REFERENCES "public"."financial_vouchers"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "upstream_holds" ADD CONSTRAINT "upstream_holds_attempt_id_attempts_id_fk" FOREIGN KEY ("attempt_id") REFERENCES "public"."attempts"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "upstream_holds" ADD CONSTRAINT "upstream_holds_channel_id_channels_id_fk" FOREIGN KEY ("channel_id") REFERENCES "public"."channels"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "upstream_holds" ADD CONSTRAINT "upstream_holds_cost_version_id_cost_versions_id_fk" FOREIGN KEY ("cost_version_id") REFERENCES "public"."cost_versions"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "upstream_holds" ADD CONSTRAINT "upstream_holds_account_id_currency_upstream_account_balances_account_id_currency_fk" FOREIGN KEY ("account_id","currency") REFERENCES "public"."upstream_account_balances"("account_id","currency") ON DELETE no action ON UPDATE no action;
ALTER TABLE "upstream_journals" ADD CONSTRAINT "upstream_journals_account_id_upstream_accounts_id_fk" FOREIGN KEY ("account_id") REFERENCES "public"."upstream_accounts"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "upstream_journals" ADD CONSTRAINT "upstream_journals_channel_id_channels_id_fk" FOREIGN KEY ("channel_id") REFERENCES "public"."channels"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "upstream_journals" ADD CONSTRAINT "upstream_journals_attempt_id_attempts_id_fk" FOREIGN KEY ("attempt_id") REFERENCES "public"."attempts"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "upstream_journals" ADD CONSTRAINT "upstream_journals_operation_id_upstream_funding_operations_id_fk" FOREIGN KEY ("operation_id") REFERENCES "public"."upstream_funding_operations"("id") ON DELETE no action ON UPDATE no action;
CREATE INDEX "api_keys_project" ON "api_keys" USING btree ("project_id","created_at");
CREATE INDEX "attempts_stale" ON "attempts" USING btree ("started_at") WHERE (state = 'DISPATCHING'::text);
CREATE INDEX "charges_unpaid" ON "billing_charges" USING btree ("hold_id","created_at","id") WHERE (debt_atoms > (0)::numeric);
CREATE INDEX "billing_holds_wallet" ON "billing_holds" USING btree ("wallet_id","created_at","id") WHERE (state = ANY (ARRAY['HELD'::text, 'RECONCILING'::text]));
CREATE INDEX "holds_all_wallet" ON "billing_holds" USING btree ("wallet_id","request_id");
CREATE INDEX "holds_reconcile_due" ON "billing_holds" USING btree ("next_reconcile_at","request_id") WHERE (state = ANY (ARRAY['HELD'::text, 'RECONCILING'::text]));
CREATE INDEX "delivery_due" ON "event_deliveries" USING btree ("consumer_id","event_id") WHERE (state <> 'ACKED'::text);
CREATE INDEX "requests_project" ON "gateway_requests" USING btree ("project_id","created_at" DESC NULLS FIRST,"id");
CREATE INDEX "journal_archive_range" ON "money_entries" USING btree ("created_at","id");
CREATE INDEX "journal_wallet" ON "money_entries" USING btree ("wallet_id","created_at" DESC NULLS FIRST,"id");
CREATE INDEX "resources_poll" ON "resources" USING btree ("next_poll_at","id") WHERE ((state = ANY (ARRAY['ACTIVE'::text, 'PENDING_USAGE'::text])) AND (kind = ANY (ARRAY['batch'::text, 'video'::text, 'response'::text, 'operation'::text])));
CREATE INDEX "resources_project" ON "resources" USING btree ("project_id","kind","created_at" DESC NULLS FIRST,"id");
CREATE INDEX "routes_lookup" ON "routes" USING btree ("model_name","priority" DESC NULLS FIRST) WHERE enabled;
CREATE INDEX "usage_conflicts_attempt" ON "usage_conflicts" USING btree ("attempt_id");
CREATE INDEX "usage_archive_range" ON "usage_event_index" USING btree ("created_at","id");
CREATE INDEX "usage_project" ON "usage_event_index" USING btree ("project_id","created_at" DESC NULLS FIRST,"id");
CREATE INDEX "wallet_lots_expiring" ON "wallet_lots" USING btree ("expires_at","id") WHERE (kind = 'GIFT'::text);
CREATE INDEX "wallet_lots_wallet" ON "wallet_lots" USING btree ("wallet_id","kind","priority","expires_at","id");
CREATE VIEW "public"."money_journal" AS (SELECT i.id, i.wallet_id, i.reference, i.kind, i.currency, p.lines, i.created_at FROM money_entries i JOIN journal_payloads p ON p.id = i.id AND p.created_at = i.created_at);
CREATE VIEW "public"."usage_events" AS (SELECT i.id, i.seq, i.project_id, i.request_id, i.attempt_id, i.source, i.finality, p.usage, i.cost_atoms, i.currency, i.created_at, i.cost_complete FROM usage_event_index i JOIN usage_payloads p ON p.id = i.id AND p.created_at = i.created_at);