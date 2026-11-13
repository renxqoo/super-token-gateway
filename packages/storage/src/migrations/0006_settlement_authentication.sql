DO $$ BEGIN IF EXISTS(SELECT 1 FROM public.core_evidence_receipts) THEN RAISE EXCEPTION 'core_evidence_receipts_unsigned_history';END IF;END $$;--> statement-breakpoint
CREATE TABLE "core_evidence_rejections" (
	"id" uuid PRIMARY KEY NOT NULL,
	"batch_id" uuid NOT NULL,
	"ordinal" integer NOT NULL,
	"reason" text NOT NULL,
	"origin_identity" text NOT NULL,
	"origin_session_id" uuid NOT NULL,
	"event_id" uuid NOT NULL,
	"attempt_id" uuid NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "core_rejection_position" UNIQUE("batch_id","ordinal"),
	CONSTRAINT "core_rejection_shape" CHECK (reason ~ '^[a-z][a-z0-9_]{0,79}$' AND origin_identity ~ '^gateway:[A-Za-z0-9_-]{1,100}$' AND ordinal BETWEEN 0 AND 8191)
);
--> statement-breakpoint
ALTER TABLE "core_evidence_receipts" DROP CONSTRAINT "core_receipt_outcome";--> statement-breakpoint
ALTER TABLE "core_evidence_receipts" ADD COLUMN "authentication_disposition" text NOT NULL;--> statement-breakpoint
ALTER TABLE "core_evidence_receipts" ADD COLUMN "authentication_reason" text NOT NULL;--> statement-breakpoint
ALTER TABLE "core_evidence_receipts" ADD COLUMN "producer_key_sha256" text NOT NULL;--> statement-breakpoint
ALTER TABLE "core_evidence_receipts" ADD COLUMN "signature_version" integer NOT NULL;--> statement-breakpoint
ALTER TABLE "core_evidence_receipts" ADD COLUMN "signature" "bytea" NOT NULL;--> statement-breakpoint
ALTER TABLE "core_evidence_receipts" ADD COLUMN "signature_payload_digest" text NOT NULL;--> statement-breakpoint
CREATE INDEX "core_rejection_origin" ON "core_evidence_rejections" USING btree ("origin_identity","created_at");--> statement-breakpoint
ALTER TABLE "core_evidence_receipts" ADD CONSTRAINT "core_receipt_authentication" CHECK (authentication_disposition IN ('VERIFIED','QUARANTINED') AND authentication_reason IN ('NORMAL','SECURITY_REVOKED','UNGRANTED')
        AND (authentication_disposition='VERIFIED')=(authentication_reason='NORMAL')
        AND (authentication_disposition='QUARANTINED')=(outcome='QUARANTINED')
        AND (outcome<>'QUARANTINED' OR (canonical_evidence_id IS NULL AND disputed_evidence_id IS NULL AND conflict_id IS NULL AND NOT revision_claim AND cardinality(risk_dimensions)=0))
        AND producer_key_sha256 ~ '^[a-f0-9]{64}$' AND signature_version=1 AND octet_length(signature)=64 AND signature_payload_digest ~ '^[a-f0-9]{64}$');--> statement-breakpoint
ALTER TABLE "core_evidence_receipts" ADD CONSTRAINT "core_receipt_outcome" CHECK (outcome IN ('ACCEPTED','STALE','ESTIMATED','REVISION_REPLAY','CONFLICT','OVERFLOW','QUARANTINED') AND finality IN ('FINAL','PARTIAL','UNKNOWN','ESTIMATED','NOT_EXECUTED')
        AND (outcome<>'CONFLICT' OR (conflict_id IS NOT NULL AND disputed_evidence_id IS NOT NULL AND NOT revision_claim))
        AND (outcome<>'OVERFLOW' OR (numeric_fault='COST_OVERFLOW' AND disputed_evidence_id IS NOT NULL))
        AND (outcome<>'ACCEPTED' OR canonical_evidence_id IS NOT NULL)
        AND (outcome NOT IN ('ACCEPTED','REVISION_REPLAY') OR (disputed_evidence_id IS NULL AND conflict_id IS NULL AND numeric_fault IS NULL))
        AND (numeric_fault IS NULL OR numeric_fault='COST_OVERFLOW')
        AND (attempt_state IS NULL OR attempt_state IN ('SUCCEEDED','FAILED','UNKNOWN','PARTIAL','NOT_EXECUTED'))
        AND (request_state IS NULL OR request_state IN ('SUCCEEDED','FAILED','CANCELLED','UNKNOWN'))
        AND (error_code IS NULL OR error_code ~ '^[a-z][a-z0-9_]{0,79}$')
        AND (provider_request_id IS NULL OR provider_request_id ~ '^[A-Za-z0-9._:/-]{1,256}$')
        AND cardinality(risk_dimensions)<=16);