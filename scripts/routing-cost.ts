import { createDatabase, createLedgerQueries } from "@gateway/ledger";

const [model, from, to, ...extra] = process.argv.slice(2);
if (!model || !from || !to || extra.length) {
  throw new Error("Usage: bun scripts/routing-cost.ts <model> <from-ISO-8601> <to-ISO-8601>");
}
if (![from, to].every((value) => /T.*(?:Z|[+-]\d{2}:\d{2})$/i.test(value))) {
  throw new Error("Report timestamps must include a time zone");
}
const url = process.env.DATABASE_URL;
if (!url) throw new Error("DATABASE_URL is required");

const db = createDatabase(url, 1);
try {
  const result = await createLedgerQueries(db).getRoutingCostReport({
    model,
    from: new Date(from),
    to: new Date(to),
  });
  console.log(JSON.stringify({ model, from, to, ...result }, null, 2));
} finally {
  await db.close();
}
