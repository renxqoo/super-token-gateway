CREATE SCHEMA "core_funding";
CREATE TABLE "core_funding"."billing_prices" (
	"price_id" uuid PRIMARY KEY NOT NULL,
	"compiler_version" integer NOT NULL,
	"source_digest" text NOT NULL,
	"dimensions" text[] NOT NULL,
	"weights" numeric[] NOT NULL,
	"denominator" numeric NOT NULL,
	CONSTRAINT "price_program_version" CHECK (compiler_version=1),
	CONSTRAINT "price_program_digest" CHECK (source_digest ~ '^[a-f0-9]{64}$'),
	CONSTRAINT "price_program_shape" CHECK (cardinality(dimensions)=cardinality(weights) AND array_position(dimensions,NULL) IS NULL AND array_position(weights,NULL) IS NULL AND denominator>0 AND denominator=trunc(denominator) AND 0<=ALL(weights))
);
CREATE TABLE "core_funding"."cost_prices" (
	"price_id" uuid PRIMARY KEY NOT NULL,
	"compiler_version" integer NOT NULL,
	"source_digest" text NOT NULL,
	"dimensions" text[] NOT NULL,
	"weights" numeric[] NOT NULL,
	"denominator" numeric NOT NULL,
	CONSTRAINT "price_program_version" CHECK (compiler_version=1),
	CONSTRAINT "price_program_digest" CHECK (source_digest ~ '^[a-f0-9]{64}$'),
	CONSTRAINT "price_program_shape" CHECK (cardinality(dimensions)=cardinality(weights) AND array_position(dimensions,NULL) IS NULL AND array_position(weights,NULL) IS NULL AND denominator>0 AND denominator=trunc(denominator) AND 0<=ALL(weights))
);
CREATE TABLE "core_funding"."balance_cells" (
	"account_id" uuid NOT NULL,
	"currency" text NOT NULL,
	"cell" smallint NOT NULL,
	"balance_atoms" numeric(38, 0) DEFAULT '0' NOT NULL,
	"reserved_atoms" numeric(38, 0) DEFAULT '0' NOT NULL,
	"spent_atoms" numeric(38, 0) DEFAULT '0' NOT NULL,
	"revision" numeric(38, 0) DEFAULT '0' NOT NULL,
	"spent_limit" numeric(38, 0) NOT NULL,
	"revision_limit" numeric(38, 0) NOT NULL,
	CONSTRAINT "balance_cells_account_id_currency_cell_pk" PRIMARY KEY("account_id","currency","cell"),
	CONSTRAINT "funding_cell_range" CHECK (cell BETWEEN 0 AND 15),
	CONSTRAINT "funding_cell_counters" CHECK (reserved_atoms>=0 AND spent_atoms>=0 AND revision>=0 AND spent_limit>=spent_atoms AND revision_limit>=revision)
);
CREATE TABLE "core_funding"."budget_transfers" (
	"id" uuid PRIMARY KEY NOT NULL,
	"account_id" uuid NOT NULL,
	"currency" text NOT NULL,
	"cell" smallint NOT NULL,
	"kind" text NOT NULL,
	"reference" text NOT NULL,
	"balance_delta" numeric(38, 0) NOT NULL,
	"spent_allocation_atoms" numeric(38, 0) DEFAULT '0' NOT NULL,
	"spent_limit_delta" numeric(38, 0) DEFAULT '0' NOT NULL,
	"revision_limit_delta" numeric(38, 0) DEFAULT '0' NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "funding_transfer_reference" UNIQUE("account_id","currency","reference"),
	CONSTRAINT "funding_transfer_kind" CHECK (kind IN ('GRANT','RECLAIM','HEADROOM','MIGRATION')),
	CONSTRAINT "funding_transfer_history" CHECK (spent_allocation_atoms>=0 AND (kind='MIGRATION' OR spent_allocation_atoms=0))
);
ALTER TABLE "core_funding"."billing_prices" ADD CONSTRAINT "billing_prices_price_id_billing_prices_id_fk" FOREIGN KEY ("price_id") REFERENCES "public"."billing_prices"("id") ON DELETE no action ON UPDATE no action;ALTER TABLE "core_funding"."cost_prices" ADD CONSTRAINT "cost_prices_price_id_cost_versions_id_fk" FOREIGN KEY ("price_id") REFERENCES "public"."cost_versions"("id") ON DELETE no action ON UPDATE no action;ALTER TABLE "core_funding"."balance_cells" ADD CONSTRAINT "balance_cells_account_id_currency_upstream_account_balances_account_id_currency_fk" FOREIGN KEY ("account_id","currency") REFERENCES "public"."upstream_account_balances"("account_id","currency") ON DELETE no action ON UPDATE no action;ALTER TABLE "core_funding"."budget_transfers" ADD CONSTRAINT "budget_transfers_account_id_currency_cell_balance_cells_account_id_currency_cell_fk" FOREIGN KEY ("account_id","currency","cell") REFERENCES "core_funding"."balance_cells"("account_id","currency","cell") ON DELETE no action ON UPDATE no action;