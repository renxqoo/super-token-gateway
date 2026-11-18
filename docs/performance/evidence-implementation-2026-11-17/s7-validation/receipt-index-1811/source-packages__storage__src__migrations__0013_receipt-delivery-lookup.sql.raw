ALTER TABLE "public"."core_evidence_receipts" DROP CONSTRAINT "core_receipt_delivery";
ALTER TABLE "public"."core_evidence_receipts" ADD CONSTRAINT "core_receipt_delivery" UNIQUE("event_id","origin_identity","body_digest");
