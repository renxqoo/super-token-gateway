CREATE TABLE "internal_service_certificates" (
	"certificate_sha256" text PRIMARY KEY NOT NULL,
	"service" text NOT NULL,
	"instance_id" text NOT NULL,
	"uri_san" text NOT NULL,
	"subject_altname_exact" text NOT NULL,
	"not_before" timestamp with time zone NOT NULL,
	"not_after" timestamp with time zone NOT NULL,
	"retired_at" timestamp with time zone,
	"security_revoked_at" timestamp with time zone,
	"authorization_revision" numeric(38, 0) DEFAULT '1' NOT NULL,
	CONSTRAINT "internal_service_certificates_certificate_sha256_service_instance_id_unique" UNIQUE("certificate_sha256","service","instance_id"),
	CONSTRAINT "identity_cert_shape" CHECK (certificate_sha256 ~ '^[a-f0-9]{64}$' AND length(uri_san) BETWEEN 1 AND 250 AND length(subject_altname_exact) BETWEEN 1 AND 2000 AND not_after>not_before AND authorization_revision>0)
);
--> statement-breakpoint
CREATE TABLE "internal_service_instances" (
	"service" text NOT NULL,
	"instance_id" text NOT NULL,
	"enabled" boolean DEFAULT true NOT NULL,
	"authorization_revision" numeric(38, 0) DEFAULT '1' NOT NULL,
	"security_revoked_at" timestamp with time zone,
	"security_reason" text,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "internal_service_instances_service_instance_id_pk" PRIMARY KEY("service","instance_id"),
	CONSTRAINT "identity_instance_shape" CHECK (service IN ('gateway','core-api','core-worker') AND instance_id ~ '^[A-Za-z0-9_-]{1,100}$' AND authorization_revision>0),
	CONSTRAINT "identity_instance_security" CHECK ((security_revoked_at IS NULL AND security_reason IS NULL) OR (security_revoked_at IS NOT NULL AND security_reason='SECURITY_INCIDENT'))
);
--> statement-breakpoint
CREATE TABLE "internal_startup_registrations" (
	"challenge_id" uuid PRIMARY KEY NOT NULL,
	"service" text NOT NULL,
	"instance_id" text NOT NULL,
	"registration_id" uuid NOT NULL,
	"certificate_sha256" text NOT NULL,
	"audience" text NOT NULL,
	"public_key_spki" "bytea" NOT NULL,
	"public_key_sha256" text NOT NULL,
	"boot_nonce" "bytea" NOT NULL,
	"server_nonce" "bytea" NOT NULL,
	"registration_digest" text NOT NULL,
	"challenge_expires_at" timestamp with time zone NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"state" text DEFAULT 'PENDING' NOT NULL,
	"session_id" uuid,
	"receipt" jsonb,
	CONSTRAINT "internal_startup_registrations_session_id_unique" UNIQUE("session_id"),
	CONSTRAINT "internal_startup_registrations_service_instance_id_registration_id_unique" UNIQUE("service","instance_id","registration_id"),
	CONSTRAINT "identity_registration_shape" CHECK (octet_length(public_key_spki)=44 AND encode(sha256(public_key_spki),'hex')=public_key_sha256 AND octet_length(boot_nonce)=32 AND octet_length(server_nonce)=32 AND registration_digest ~ '^[a-f0-9]{64}$' AND audience ~ '^[A-Za-z0-9_.-]{1,100}/core-api$'),
	CONSTRAINT "identity_registration_state" CHECK ((state='REGISTERED' AND session_id IS NOT NULL AND receipt IS NOT NULL) OR (state IN ('PENDING','EXPIRED') AND session_id IS NULL AND receipt IS NULL))
);
--> statement-breakpoint
CREATE TABLE "internal_startup_sessions" (
	"session_id" uuid PRIMARY KEY NOT NULL,
	"incarnation_id" uuid NOT NULL,
	"service" text NOT NULL,
	"instance_id" text NOT NULL,
	"certificate_sha256" text NOT NULL,
	"public_key_spki" "bytea" NOT NULL,
	"public_key_sha256" text NOT NULL,
	"boot_nonce" "bytea" NOT NULL,
	"registered_at" timestamp with time zone NOT NULL,
	"expires_at" timestamp with time zone NOT NULL,
	"disabled_at" timestamp with time zone,
	"security_revoked_at" timestamp with time zone,
	"authorization_revision" numeric(38, 0) DEFAULT '1' NOT NULL,
	CONSTRAINT "internal_startup_sessions_incarnation_id_unique" UNIQUE("incarnation_id"),
	CONSTRAINT "internal_startup_sessions_public_key_sha256_unique" UNIQUE("public_key_sha256"),
	CONSTRAINT "internal_startup_sessions_service_instance_id_boot_nonce_unique" UNIQUE("service","instance_id","boot_nonce"),
	CONSTRAINT "identity_session_shape" CHECK (octet_length(public_key_spki)=44 AND encode(sha256(public_key_spki),'hex')=public_key_sha256 AND octet_length(boot_nonce)=32 AND expires_at>registered_at AND authorization_revision>0)
);
--> statement-breakpoint
ALTER TABLE "execution_commands" ADD COLUMN "owner_session_id" uuid;--> statement-breakpoint
ALTER TABLE "execution_permits" ADD COLUMN "owner_session_id" uuid;--> statement-breakpoint
ALTER TABLE "execution_permits" ADD COLUMN "owner_key_sha256" text;--> statement-breakpoint
ALTER TABLE "execution_permits" ADD COLUMN "owner_service" text;--> statement-breakpoint
ALTER TABLE "execution_permits" ADD COLUMN "certificate_sha256" text;--> statement-breakpoint
ALTER TABLE "internal_service_certificates" ADD CONSTRAINT "internal_service_certificates_service_instance_id_internal_service_instances_service_instance_id_fk" FOREIGN KEY ("service","instance_id") REFERENCES "public"."internal_service_instances"("service","instance_id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "internal_startup_registrations" ADD CONSTRAINT "internal_startup_registrations_session_id_internal_startup_sessions_session_id_fk" FOREIGN KEY ("session_id") REFERENCES "public"."internal_startup_sessions"("session_id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "internal_startup_registrations" ADD CONSTRAINT "internal_startup_registrations_service_instance_id_internal_service_instances_service_instance_id_fk" FOREIGN KEY ("service","instance_id") REFERENCES "public"."internal_service_instances"("service","instance_id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "internal_startup_registrations" ADD CONSTRAINT "internal_startup_registrations_certificate_sha256_service_instance_id_internal_service_certificates_certificate_sha256_service_instance_id_fk" FOREIGN KEY ("certificate_sha256","service","instance_id") REFERENCES "public"."internal_service_certificates"("certificate_sha256","service","instance_id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "internal_startup_sessions" ADD CONSTRAINT "internal_startup_sessions_service_instance_id_internal_service_instances_service_instance_id_fk" FOREIGN KEY ("service","instance_id") REFERENCES "public"."internal_service_instances"("service","instance_id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "internal_startup_sessions" ADD CONSTRAINT "internal_startup_sessions_certificate_sha256_service_instance_id_internal_service_certificates_certificate_sha256_service_instance_id_fk" FOREIGN KEY ("certificate_sha256","service","instance_id") REFERENCES "public"."internal_service_certificates"("certificate_sha256","service","instance_id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
CREATE INDEX "identity_cert_instance_idx" ON "internal_service_certificates" USING btree ("service","instance_id","certificate_sha256");--> statement-breakpoint
CREATE INDEX "identity_pending_expiry_idx" ON "internal_startup_registrations" USING btree ("state","challenge_expires_at","challenge_id");--> statement-breakpoint
CREATE INDEX "identity_session_instance_idx" ON "internal_startup_sessions" USING btree ("service","instance_id","expires_at","session_id");--> statement-breakpoint
ALTER TABLE "execution_commands" ADD CONSTRAINT "execution_commands_owner_session_id_internal_startup_sessions_session_id_fk" FOREIGN KEY ("owner_session_id") REFERENCES "public"."internal_startup_sessions"("session_id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "execution_permits" ADD CONSTRAINT "execution_permits_owner_session_id_internal_startup_sessions_session_id_fk" FOREIGN KEY ("owner_session_id") REFERENCES "public"."internal_startup_sessions"("session_id") ON DELETE no action ON UPDATE no action;