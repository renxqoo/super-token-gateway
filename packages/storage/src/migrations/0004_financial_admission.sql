CREATE TABLE "execution_contexts" (
	"id" uuid PRIMARY KEY NOT NULL,
	"project_id" uuid NOT NULL,
	"key_id" uuid NOT NULL,
	"owner_session_id" uuid NOT NULL,
	"operation" text NOT NULL,
	"model" text NOT NULL,
	"ip" text NOT NULL,
	"fingerprint" text NOT NULL,
	"snapshot" jsonb NOT NULL,
	"expires_at" timestamp with time zone NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "execution_quota_leases" (
	"lease_id" text PRIMARY KEY NOT NULL,
	"attempt_id" uuid NOT NULL,
	"command_id" uuid NOT NULL,
	"request_id" uuid NOT NULL,
	"scope_kind" text NOT NULL,
	"scope_id" text NOT NULL,
	"state" text NOT NULL,
	"revision" numeric(38, 0) DEFAULT '0' NOT NULL,
	"descriptor" jsonb NOT NULL,
	CONSTRAINT "execution_quota_leases_attempt_id_scope_kind_scope_id_unique" UNIQUE("attempt_id","scope_kind","scope_id"),
	CONSTRAINT "execution_quota_state" CHECK (state IN ('ADOPTION_PENDING','ACTIVE','RELEASE_PENDING','RELEASED','EXPIRED'))
);
--> statement-breakpoint
ALTER TABLE "gateway_requests" ADD COLUMN "execution_budget" jsonb;--> statement-breakpoint
ALTER TABLE "gateway_requests" ADD COLUMN "execution_closed_at" timestamp with time zone;--> statement-breakpoint
ALTER TABLE "execution_commands" ADD COLUMN "receipt" jsonb DEFAULT '{}'::jsonb NOT NULL;--> statement-breakpoint
ALTER TABLE "execution_commands" ADD COLUMN "context_id" uuid;--> statement-breakpoint
ALTER TABLE "execution_permits" ADD COLUMN "route_id" uuid;--> statement-breakpoint
ALTER TABLE "execution_permits" ADD COLUMN "context_id" uuid;--> statement-breakpoint
ALTER TABLE "execution_permits" ADD COLUMN "snapshot" jsonb DEFAULT '{}'::jsonb NOT NULL;--> statement-breakpoint
ALTER TABLE "execution_permits" ADD COLUMN "manifest" jsonb DEFAULT '{}'::jsonb NOT NULL;--> statement-breakpoint
ALTER TABLE "execution_permits" ADD COLUMN "manifest_digest" text;--> statement-breakpoint
ALTER TABLE "execution_permits" ADD COLUMN "candidate_digest" text;--> statement-breakpoint
ALTER TABLE "execution_permits" ADD COLUMN "authorization_plan_digest" text;--> statement-breakpoint
ALTER TABLE "execution_permits" ADD COLUMN "prep_session_id" uuid;--> statement-breakpoint
ALTER TABLE "execution_permits" ADD COLUMN "prep_binding_digest" text;--> statement-breakpoint
ALTER TABLE "upstream_holds" ADD COLUMN "estimated_quantities" jsonb;--> statement-breakpoint
ALTER TABLE "upstream_holds" ADD COLUMN "estimated_quantities_digest" text;--> statement-breakpoint
ALTER TABLE "upstream_holds" ADD COLUMN "estimate_manifest_digest" text;--> statement-breakpoint
ALTER TABLE "upstream_holds" ADD COLUMN "estimate_algorithm_version" integer;--> statement-breakpoint
ALTER TABLE "execution_contexts" ADD CONSTRAINT "execution_contexts_project_id_projects_id_fk" FOREIGN KEY ("project_id") REFERENCES "public"."projects"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "execution_contexts" ADD CONSTRAINT "execution_contexts_key_id_api_keys_id_fk" FOREIGN KEY ("key_id") REFERENCES "public"."api_keys"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "execution_contexts" ADD CONSTRAINT "execution_contexts_owner_session_id_internal_startup_sessions_session_id_fk" FOREIGN KEY ("owner_session_id") REFERENCES "public"."internal_startup_sessions"("session_id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "execution_quota_leases" ADD CONSTRAINT "execution_quota_leases_attempt_id_attempts_id_fk" FOREIGN KEY ("attempt_id") REFERENCES "public"."attempts"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "execution_quota_leases" ADD CONSTRAINT "execution_quota_leases_command_id_execution_commands_id_fk" FOREIGN KEY ("command_id") REFERENCES "public"."execution_commands"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "execution_quota_leases" ADD CONSTRAINT "execution_quota_leases_request_id_gateway_requests_id_fk" FOREIGN KEY ("request_id") REFERENCES "public"."gateway_requests"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
CREATE UNIQUE INDEX "execution_request_common_quota_unique" ON "execution_quota_leases" USING btree ("request_id","scope_kind","scope_id") WHERE scope_kind<>'channel';--> statement-breakpoint
ALTER TABLE "execution_commands" ADD CONSTRAINT "execution_commands_context_id_execution_contexts_id_fk" FOREIGN KEY ("context_id") REFERENCES "public"."execution_contexts"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "execution_permits" ADD CONSTRAINT "execution_permits_route_id_routes_id_fk" FOREIGN KEY ("route_id") REFERENCES "public"."routes"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "execution_permits" ADD CONSTRAINT "execution_permits_context_id_execution_contexts_id_fk" FOREIGN KEY ("context_id") REFERENCES "public"."execution_contexts"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
CREATE INDEX "upstream_hold_account_state_idx" ON "upstream_holds" USING btree ("account_id","currency","state","attempt_id");--> statement-breakpoint
ALTER TABLE "gateway_requests" ADD CONSTRAINT "gateway_requests_project_intent_unique" UNIQUE("project_id","intent_digest");