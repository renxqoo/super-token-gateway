DO $$ BEGIN IF EXISTS(SELECT 1 FROM public.upstream_costs) THEN RAISE EXCEPTION 'upstream_costs_history_unmapped';END IF;END $$;--> statement-breakpoint
CREATE TABLE "core_evidence_receipts" (
	"id" uuid PRIMARY KEY NOT NULL,
	"origin_identity" text NOT NULL,
	"origin_incarnation" uuid NOT NULL,
	"origin_session_id" uuid NOT NULL,
	"event_id" uuid NOT NULL,
	"attempt_id" uuid NOT NULL,
	"source" text NOT NULL,
	"source_revision" numeric(38, 0) NOT NULL,
	"body_digest" text NOT NULL,
	"event_claim" boolean NOT NULL,
	"revision_claim" boolean NOT NULL,
	"outcome" text NOT NULL,
	"finality" text NOT NULL,
	"attempt_state" text,
	"request_state" text,
	"error_code" text,
	"provider_request_id" text,
	"raw_evidence_id" uuid NOT NULL,
	"canonical_evidence_id" uuid,
	"disputed_evidence_id" uuid,
	"conflict_id" uuid,
	"risk_dimensions" text[] NOT NULL,
	"numeric_fault" text,
	"authentication_disposition" text NOT NULL,
	"authentication_reason" text NOT NULL,
	"producer_key_sha256" text NOT NULL,
	"signature_version" integer NOT NULL,
	"signature" "bytea" NOT NULL,
	"signature_payload_digest" text NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "core_receipt_delivery" UNIQUE("origin_identity","event_id","body_digest"),
	CONSTRAINT "core_receipt_source" CHECK (source IN ('execution','execution_outcome','recovery_interruption','async_partial','async_final','batch_completed','batch_cancelled','batch_failed','provider_invoice') AND source_revision>=1 AND body_digest ~ '^[a-f0-9]{64}$' AND origin_identity ~ '^gateway:[A-Za-z0-9_-]{1,100}$'),
	CONSTRAINT "core_receipt_outcome" CHECK (outcome IN ('ACCEPTED','STALE','ESTIMATED','REVISION_REPLAY','CONFLICT','OVERFLOW','QUARANTINED') AND finality IN ('FINAL','PARTIAL','UNKNOWN','ESTIMATED','NOT_EXECUTED')
        AND (outcome<>'CONFLICT' OR (conflict_id IS NOT NULL AND disputed_evidence_id IS NOT NULL AND NOT revision_claim))
        AND (outcome<>'OVERFLOW' OR (numeric_fault='COST_OVERFLOW' AND disputed_evidence_id IS NOT NULL))
        AND (outcome NOT IN ('ACCEPTED','REVISION_REPLAY') OR (canonical_evidence_id IS NOT NULL AND disputed_evidence_id IS NULL AND conflict_id IS NULL))
        AND (numeric_fault IS NULL OR numeric_fault='COST_OVERFLOW')
        AND (attempt_state IS NULL OR attempt_state IN ('SUCCEEDED','FAILED','UNKNOWN','PARTIAL','NOT_EXECUTED'))
        AND (request_state IS NULL OR request_state IN ('SUCCEEDED','FAILED','CANCELLED','UNKNOWN'))
        AND (error_code IS NULL OR error_code ~ '^[a-z][a-z0-9_]{0,79}$')
        AND (provider_request_id IS NULL OR provider_request_id ~ '^[A-Za-z0-9._:/-]{1,256}$')
        AND cardinality(risk_dimensions)<=16),
	CONSTRAINT "core_receipt_authentication" CHECK (authentication_disposition IN ('VERIFIED','QUARANTINED') AND authentication_reason IN ('NORMAL','SECURITY_REVOKED','UNGRANTED')
        AND (authentication_disposition='VERIFIED')=(authentication_reason='NORMAL')
        AND (authentication_disposition='QUARANTINED')=(outcome='QUARANTINED')
        AND (outcome<>'QUARANTINED' OR (canonical_evidence_id IS NULL AND disputed_evidence_id IS NULL AND conflict_id IS NULL AND NOT revision_claim AND cardinality(risk_dimensions)=0))
        AND producer_key_sha256 ~ '^[a-f0-9]{64}$' AND signature_version=1 AND octet_length(signature)=64 AND signature_payload_digest ~ '^[a-f0-9]{64}$')
);
--> statement-breakpoint
ALTER TABLE "usage_event_index" DROP CONSTRAINT "usage_events_attempt_id_source_key";--> statement-breakpoint
ALTER TABLE "upstream_costs" DROP CONSTRAINT "upstream_cost_exact";--> statement-breakpoint
ALTER TABLE "usage_event_index" ALTER COLUMN "cost_atoms" DROP NOT NULL;--> statement-breakpoint
ALTER TABLE "usage_conflicts" ADD COLUMN "incoming_evidence_id" uuid;--> statement-breakpoint
ALTER TABLE "usage_conflicts" ADD COLUMN "disputed_evidence_id" uuid;--> statement-breakpoint
ALTER TABLE "usage_conflicts" ADD COLUMN "incoming_receipt_id" uuid;--> statement-breakpoint
ALTER TABLE "usage_event_index" ADD COLUMN "record_kind" text DEFAULT 'GATEWAY' NOT NULL;--> statement-breakpoint
ALTER TABLE "usage_event_index" ADD COLUMN "snapshot_revision" numeric(38, 0);--> statement-breakpoint
ALTER TABLE "usage_event_index" ADD COLUMN "snapshot_digest" text;--> statement-breakpoint
ALTER TABLE "usage_event_index" ADD COLUMN "final_dimensions" text[];--> statement-breakpoint
ALTER TABLE "usage_event_index" ADD COLUMN "parent_evidence_id" uuid;--> statement-breakpoint
ALTER TABLE "usage_event_index" ADD COLUMN "input_fact_id" uuid;--> statement-breakpoint
ALTER TABLE "upstream_costs" ADD COLUMN "evidence_id" uuid NOT NULL;--> statement-breakpoint
ALTER TABLE "upstream_costs" ADD COLUMN "cost_version_id" uuid NOT NULL;--> statement-breakpoint
ALTER TABLE "usage_event_index" ADD CONSTRAINT "usage_index_attempt_identity" UNIQUE("id","attempt_id");--> statement-breakpoint
ALTER TABLE "core_evidence_receipts" ADD CONSTRAINT "core_evidence_receipts_origin_session_id_internal_startup_sessions_session_id_fk" FOREIGN KEY ("origin_session_id") REFERENCES "public"."internal_startup_sessions"("session_id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "core_evidence_receipts" ADD CONSTRAINT "core_evidence_receipts_attempt_id_attempts_id_fk" FOREIGN KEY ("attempt_id") REFERENCES "public"."attempts"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "core_evidence_receipts" ADD CONSTRAINT "core_receipt_raw" FOREIGN KEY ("raw_evidence_id","attempt_id") REFERENCES "public"."usage_event_index"("id","attempt_id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "core_evidence_receipts" ADD CONSTRAINT "core_receipt_canonical" FOREIGN KEY ("canonical_evidence_id","attempt_id") REFERENCES "public"."usage_event_index"("id","attempt_id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "core_evidence_receipts" ADD CONSTRAINT "core_receipt_disputed" FOREIGN KEY ("disputed_evidence_id","attempt_id") REFERENCES "public"."usage_event_index"("id","attempt_id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
CREATE UNIQUE INDEX "core_receipt_event_claim" ON "core_evidence_receipts" USING btree ("origin_identity","event_id") WHERE event_claim;--> statement-breakpoint
CREATE UNIQUE INDEX "core_receipt_revision_claim" ON "core_evidence_receipts" USING btree ("attempt_id","origin_identity","source","source_revision") WHERE revision_claim;--> statement-breakpoint
CREATE INDEX "core_receipt_attempt_source" ON "core_evidence_receipts" USING btree ("attempt_id","source","source_revision");--> statement-breakpoint
ALTER TABLE "usage_conflicts" ADD CONSTRAINT "usage_conflict_incoming" FOREIGN KEY ("incoming_evidence_id","attempt_id") REFERENCES "public"."usage_event_index"("id","attempt_id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "usage_conflicts" ADD CONSTRAINT "usage_conflict_disputed" FOREIGN KEY ("disputed_evidence_id","attempt_id") REFERENCES "public"."usage_event_index"("id","attempt_id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "usage_event_index" ADD CONSTRAINT "usage_parent_evidence" FOREIGN KEY ("parent_evidence_id","attempt_id") REFERENCES "public"."usage_event_index"("id","attempt_id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "usage_event_index" ADD CONSTRAINT "usage_input_fact" FOREIGN KEY ("input_fact_id","attempt_id") REFERENCES "public"."usage_event_index"("id","attempt_id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "upstream_costs" ADD CONSTRAINT "upstream_costs_cost_version_id_cost_versions_id_fk" FOREIGN KEY ("cost_version_id") REFERENCES "public"."cost_versions"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "upstream_costs" ADD CONSTRAINT "upstream_cost_evidence_attempt" FOREIGN KEY ("evidence_id","attempt_id") REFERENCES "public"."usage_event_index"("id","attempt_id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
CREATE UNIQUE INDEX "usage_gateway_source_unique" ON "usage_event_index" USING btree ("attempt_id","source") WHERE record_kind='GATEWAY';--> statement-breakpoint
CREATE UNIQUE INDEX "usage_snapshot_revision_unique" ON "usage_event_index" USING btree ("attempt_id","snapshot_revision") WHERE snapshot_revision IS NOT NULL;--> statement-breakpoint
CREATE INDEX "usage_attempt_source" ON "usage_event_index" USING btree ("attempt_id","source","seq");--> statement-breakpoint
ALTER TABLE "upstream_costs" ADD CONSTRAINT "upstream_cost_evidence" UNIQUE("evidence_id");--> statement-breakpoint
ALTER TABLE "usage_conflicts" ADD CONSTRAINT "usage_conflict_core_links" CHECK ((incoming_evidence_id IS NULL AND disputed_evidence_id IS NULL AND incoming_receipt_id IS NULL) OR (incoming_evidence_id IS NOT NULL AND disputed_evidence_id IS NOT NULL AND incoming_receipt_id IS NOT NULL));--> statement-breakpoint
ALTER TABLE "usage_event_index" ADD CONSTRAINT "usage_record_kind" CHECK (record_kind IN ('GATEWAY','RAW','CANONICAL','DISPUTED') AND CASE record_kind
        WHEN 'GATEWAY' THEN cost_atoms IS NOT NULL AND snapshot_revision IS NULL AND snapshot_digest IS NULL AND final_dimensions IS NULL AND parent_evidence_id IS NULL AND input_fact_id IS NULL
        WHEN 'RAW' THEN cost_atoms IS NULL AND NOT cost_complete AND snapshot_revision>=1 AND snapshot_digest ~ '^[a-f0-9]{64}$' AND final_dimensions IS NOT NULL AND parent_evidence_id IS NULL AND input_fact_id IS NULL
        WHEN 'CANONICAL' THEN cost_atoms IS NOT NULL AND snapshot_revision>=1 AND snapshot_digest ~ '^[a-f0-9]{64}$' AND final_dimensions IS NOT NULL AND input_fact_id IS NOT NULL
        ELSE snapshot_revision>=1 AND snapshot_digest ~ '^[a-f0-9]{64}$' AND final_dimensions IS NOT NULL AND input_fact_id IS NOT NULL AND (cost_atoms IS NOT NULL OR NOT cost_complete) END);--> statement-breakpoint
ALTER TABLE "upstream_costs" ADD CONSTRAINT "upstream_cost_exact" CHECK (revision>=1 AND cost_atoms>=0 AND evidence_digest ~ '^[a-f0-9]{64}$');--> statement-breakpoint
CREATE OR REPLACE VIEW "public"."usage_events" AS (SELECT i.id, i.seq, i.project_id, i.request_id, i.attempt_id, i.source, i.finality, p.usage, i.cost_atoms, i.currency, i.created_at, i.cost_complete, i.record_kind, i.snapshot_revision, i.final_dimensions FROM usage_event_index i JOIN usage_payloads p ON p.id = i.id AND p.created_at = i.created_at WHERE i.record_kind <> 'RAW');