CREATE SCHEMA "core_execution_runtime";
CREATE TABLE "core_execution_runtime"."approvals" (
	"package_digest" text NOT NULL,
	"kind" text NOT NULL,
	"artifact_digest" text NOT NULL,
	"active" boolean NOT NULL,
	"facts" jsonb NOT NULL,
	"actor" text NOT NULL,
	"reason" text NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "approvals_package_digest_kind_artifact_digest_pk" PRIMARY KEY("package_digest","kind","artifact_digest"),
	CONSTRAINT "execution_runtime_kind" CHECK (kind IN ('wasm','builtin-worker')),
	CONSTRAINT "execution_runtime_digest" CHECK (artifact_digest ~ '^[a-f0-9]{64}$'),
	CONSTRAINT "execution_runtime_facts" CHECK (jsonb_typeof(facts)='object' AND octet_length(facts::text)<=16384),
	CONSTRAINT "execution_runtime_actor" CHECK (length(actor) BETWEEN 1 AND 100 AND length(reason) BETWEEN 1 AND 1000)
);
ALTER TABLE "core_execution_runtime"."approvals" ADD CONSTRAINT "approvals_package_digest_plugin_packages_digest_fk" FOREIGN KEY ("package_digest") REFERENCES "public"."plugin_packages"("digest") ON DELETE no action ON UPDATE no action;
