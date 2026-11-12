CREATE TABLE "plugin_catalog_receipts" (
	"command_id" uuid PRIMARY KEY NOT NULL,
	"receipt" jsonb NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "plugin_publication_receipts" (
	"id" uuid PRIMARY KEY NOT NULL,
	"version_id" uuid NOT NULL,
	"command_id" uuid NOT NULL,
	"validation_run_id" uuid NOT NULL,
	"package_digest" text NOT NULL,
	"result_digest" text NOT NULL,
	"policy_digest" text NOT NULL,
	"actor" text NOT NULL,
	"receipt" jsonb NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "plugin_publication_receipts_version_id_unique" UNIQUE("version_id"),
	CONSTRAINT "plugin_publication_receipts_command_id_unique" UNIQUE("command_id")
);
--> statement-breakpoint
CREATE TABLE "plugin_validation_runs" (
	"id" uuid PRIMARY KEY NOT NULL,
	"version_id" uuid NOT NULL,
	"command_id" uuid NOT NULL,
	"package_digest" text NOT NULL,
	"fixture_digest" text NOT NULL,
	"policy_digest" text NOT NULL,
	"runtime_digest" text NOT NULL,
	"actor" text NOT NULL,
	"state" text NOT NULL,
	"lease_owner" uuid NOT NULL,
	"lease_epoch" numeric(38, 0) NOT NULL,
	"lease_expires_at" timestamp with time zone NOT NULL,
	"resource_pid" integer,
	"resource_connection" text,
	"result" jsonb,
	"result_digest" text,
	"error_code" text,
	"started_at" timestamp with time zone DEFAULT now() NOT NULL,
	"finished_at" timestamp with time zone,
	CONSTRAINT "plugin_validation_runs_command_id_unique" UNIQUE("command_id"),
	CONSTRAINT "plugin_run_state" CHECK (state IN ('PENDING','RUNNING','CLEANUP_PENDING','SUCCEEDED','FAILED')),
	CONSTRAINT "plugin_run_epoch" CHECK (lease_epoch>0),
	CONSTRAINT "plugin_run_digests" CHECK (fixture_digest ~ '^[a-f0-9]{64}$' AND policy_digest ~ '^[a-f0-9]{64}$' AND runtime_digest ~ '^[a-f0-9]{64}$' AND (result_digest IS NULL OR result_digest ~ '^[a-f0-9]{64}$')),
	CONSTRAINT "plugin_run_result" CHECK ((result IS NULL OR (jsonb_typeof(result)='object' AND octet_length(result::text)<=4096)) AND (error_code IS NULL OR error_code ~ '^[A-Z][A-Z0-9_]{0,79}$'))
);
--> statement-breakpoint
ALTER TABLE "plugin_packages" DROP CONSTRAINT "plugin_package_size";--> statement-breakpoint
ALTER TABLE "channel_plugin_bindings" ADD COLUMN "account_id" uuid;--> statement-breakpoint
ALTER TABLE "channel_plugin_bindings" ADD COLUMN "brand_id" uuid;--> statement-breakpoint
ALTER TABLE "channel_plugin_bindings" ADD COLUMN "secret_reference" text;--> statement-breakpoint
ALTER TABLE "plugin_packages" ADD COLUMN "release_descriptor" jsonb;--> statement-breakpoint
ALTER TABLE "plugin_packages" ADD COLUMN "release_signature" text;--> statement-breakpoint
ALTER TABLE "plugin_packages" ADD COLUMN "release_key_id" text;--> statement-breakpoint
ALTER TABLE "audit_events" ADD COLUMN "actor" text;--> statement-breakpoint
ALTER TABLE "audit_events" ADD COLUMN "details" jsonb;--> statement-breakpoint
ALTER TABLE "plugin_catalog_receipts" ADD CONSTRAINT "plugin_catalog_receipts_command_id_execution_commands_id_fk" FOREIGN KEY ("command_id") REFERENCES "public"."execution_commands"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "plugin_publication_receipts" ADD CONSTRAINT "plugin_publication_receipts_version_id_plugin_versions_id_fk" FOREIGN KEY ("version_id") REFERENCES "public"."plugin_versions"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "plugin_publication_receipts" ADD CONSTRAINT "plugin_publication_receipts_command_id_execution_commands_id_fk" FOREIGN KEY ("command_id") REFERENCES "public"."execution_commands"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "plugin_publication_receipts" ADD CONSTRAINT "plugin_publication_receipts_validation_run_id_plugin_validation_runs_id_fk" FOREIGN KEY ("validation_run_id") REFERENCES "public"."plugin_validation_runs"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "plugin_publication_receipts" ADD CONSTRAINT "plugin_publication_receipts_package_digest_plugin_packages_digest_fk" FOREIGN KEY ("package_digest") REFERENCES "public"."plugin_packages"("digest") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "plugin_validation_runs" ADD CONSTRAINT "plugin_validation_runs_version_id_plugin_versions_id_fk" FOREIGN KEY ("version_id") REFERENCES "public"."plugin_versions"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "plugin_validation_runs" ADD CONSTRAINT "plugin_validation_runs_command_id_execution_commands_id_fk" FOREIGN KEY ("command_id") REFERENCES "public"."execution_commands"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "plugin_validation_runs" ADD CONSTRAINT "plugin_validation_runs_package_digest_plugin_packages_digest_fk" FOREIGN KEY ("package_digest") REFERENCES "public"."plugin_packages"("digest") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
CREATE UNIQUE INDEX "plugin_one_active_validation" ON "plugin_validation_runs" USING btree ("version_id") WHERE state IN ('PENDING','RUNNING','CLEANUP_PENDING');--> statement-breakpoint
ALTER TABLE "plugin_packages" ADD CONSTRAINT "plugin_package_size" CHECK (octet_length(package_bytes)>0 AND octet_length(package_bytes)<=46137344);