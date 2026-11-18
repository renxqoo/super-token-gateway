ALTER TABLE "upstream_holds" DROP CONSTRAINT "upstream_hold_amounts";
ALTER TABLE "core_funding"."budget_transfers" DROP CONSTRAINT "funding_transfer_kind";
ALTER TABLE "upstream_holds" ADD COLUMN "funding_cell" smallint;
ALTER TABLE "upstream_holds" ADD COLUMN "current_reserved_atoms" numeric(38, 0);
ALTER TABLE "upstream_holds" ADD CONSTRAINT "upstream_holds_account_id_currency_funding_cell_balance_cells_account_id_currency_cell_fk" FOREIGN KEY ("account_id","currency","funding_cell") REFERENCES "core_funding"."balance_cells"("account_id","currency","cell") ON DELETE no action ON UPDATE no action;
CREATE INDEX "upstream_hold_funding_idx" ON "upstream_holds" USING btree ("account_id","currency","funding_cell");
ALTER TABLE "upstream_holds" ADD CONSTRAINT "upstream_hold_amounts" CHECK (estimated_atoms>=0 AND current_reserved_atoms>=0 AND (final_cost_atoms IS NULL OR final_cost_atoms>=0));
ALTER TABLE "core_funding"."budget_transfers" ADD CONSTRAINT "funding_transfer_kind" CHECK (kind IN ('GRANT','RECLAIM','HEADROOM','MIGRATION','REBASE'));
