import { SQL } from "bun";
import {
  createCatalogRepository,
  createDatabase,
  createLedgerEngine,
  migrate,
} from "@gateway/ledger";

export async function withLedgerDb(
  run: (db: SQL, dbUrl: string) => Promise<void>,
  initialize: (db: SQL) => Promise<void> = migrate,
): Promise<void> {
  const base = process.env.DATABASE_URL;
  if (!base) throw new Error("DATABASE_URL required");
  const name = `gateway_v4_ledger_${crypto.randomUUID().replaceAll("-", "")}`;
  const owner = createDatabase(base, 1);
  await owner.unsafe(`CREATE DATABASE ${name}`);
  const url = new URL(base);
  url.pathname = `/${name}`;
  const db = createDatabase(url.toString(), 16);
  try {
    await initialize(db);
    await run(db, url.toString());
  } finally {
    await db.close({ timeout: 1 });
    await owner.unsafe(`DROP DATABASE IF EXISTS ${name} WITH (FORCE)`);
    await owner.close({ timeout: 1 });
  }
}

export async function seedFixture(db: SQL) {
  const catalog = createCatalogRepository(db);
  const ledger = createLedgerEngine(db, { batchWindowMs: 2, maxBatchSize: 64 });
  const billingAccount = await catalog.upsertBillingAccount({
    name: "ledger-fixture",
    currency: "PTS",
    rpm: 10000,
    tpm: 10000000,
    concurrency: 1000,
  });
  const accountA = await catalog.upsertUpstreamAccount({ name: `acct-a-${crypto.randomUUID()}` });
  const accountB = await catalog.upsertUpstreamAccount({ name: `acct-b-${crypto.randomUUID()}` });
  const accountC = await catalog.upsertUpstreamAccount({ name: `acct-c-${crypto.randomUUID()}` });
  const channelA = await catalog.upsertChannel({
    account_id: accountA.id,
    name: "chan-a",
    kind: "openai",
    base_url: "https://a.invalid/v1",
    secret_ciphertext: "enc-a",
    rpm: 10000,
    concurrency: 1000,
  });
  const channelB = await catalog.upsertChannel({
    account_id: accountB.id,
    name: "chan-b",
    kind: "openai",
    base_url: "https://b.invalid/v1",
    secret_ciphertext: "enc-b",
    rpm: 10000,
    concurrency: 1000,
  });
  const channelC = await catalog.upsertChannel({
    account_id: accountC.id,
    name: "chan-c",
    kind: "openai",
    base_url: "https://c.invalid/v1",
    secret_ciphertext: "enc-c",
    rpm: 10000,
    concurrency: 1000,
  });
  await catalog.upsertModel({
    name: "gpt-4o",
    operations: ["chat"],
    routing_strategy: "WEIGHTED",
    max_output_tokens: 4096,
  });
  const billingPrice = await catalog.createPriceVersion({
    side: "CUSTOMER",
    currency: "PTS",
    basis: "PROVIDER_USAGE",
    rates: {
      input_tokens: { numerator: "1", denominator: "3" },
      output_tokens: { numerator: "1", denominator: "3" },
    },
  });
  const flatBillingPrice = await catalog.createPriceVersion({
    side: "CUSTOMER",
    currency: "PTS",
    basis: "PROVIDER_USAGE",
    rates: {
      input_tokens: { numerator: "1", denominator: "1" },
      output_tokens: { numerator: "1", denominator: "1" },
    },
  });
  const costPrice = await catalog.createPriceVersion({
    side: "SUPPLIER",
    currency: "USD",
    basis: "PROVIDER_USAGE",
    rates: {
      input_tokens: { numerator: "1", denominator: "2" },
      output_tokens: { numerator: "1", denominator: "2" },
    },
  });
  await ledger.rechargeSupplier({
    account_id: accountA.id,
    currency: "USD",
    external_ref: "init-a",
    amount_atoms: "1000000",
  });
  await ledger.rechargeSupplier({
    account_id: accountB.id,
    currency: "USD",
    external_ref: "init-b",
    amount_atoms: "1000000",
  });
  await ledger.rechargeSupplier({
    account_id: accountC.id,
    currency: "USD",
    external_ref: "init-c",
    amount_atoms: "1000000",
  });
  return {
    catalog,
    ledger,
    billingAccount,
    accountA,
    accountB,
    accountC,
    channelA,
    channelB,
    channelC,
    billingPrice,
    flatBillingPrice,
    costPrice,
  };
}

export async function seedPre0004Fixture(db: SQL) {
  const ledger = createLedgerEngine(db, { batchWindowMs: 2, maxBatchSize: 64 });
  const billingAccountId = crypto.randomUUID();
  const walletId = crypto.randomUUID();
  const accountId = crypto.randomUUID();
  const channelId = crypto.randomUUID();
  const billingPriceId = crypto.randomUUID();
  const costPriceId = crypto.randomUUID();
  await db`INSERT INTO public.billing_accounts (id, name, currency, rpm, tpm, concurrency) VALUES (${billingAccountId}::uuid, 'pre-0004-acct', 'PTS', 10000, 10000000, 1000)`;
  await db`INSERT INTO public.wallets (id, billing_account_id, currency) VALUES (${walletId}::uuid, ${billingAccountId}::uuid, 'PTS')`;
  await db`INSERT INTO public.upstream_accounts (id, name, state) VALUES (${accountId}::uuid, ${`acct-pre0004-${crypto.randomUUID()}`}, 'ACTIVE')`;
  await db`INSERT INTO public.channels (id, account_id, name, kind, base_url, secret_ciphertext, rpm, concurrency, enabled) VALUES (${channelId}::uuid, ${accountId}::uuid, 'chan-pre0004', 'openai', 'https://a.invalid/v1', 'enc-a', 10000, 1000, false)`;
  const custDigest = "a".repeat(64);
  const supDigest = "b".repeat(64);
  await db`INSERT INTO public.price_versions (id, side, currency, basis, rates, ceilings, source_digest) VALUES (${billingPriceId}::uuid, 'CUSTOMER', 'PTS', 'PROVIDER_USAGE', '{"input_tokens":{"numerator":"1","denominator":"3"},"output_tokens":{"numerator":"1","denominator":"3"}}'::jsonb, '{}'::jsonb, ${custDigest}), (${costPriceId}::uuid, 'SUPPLIER', 'USD', 'PROVIDER_USAGE', '{"input_tokens":{"numerator":"1","denominator":"2"},"output_tokens":{"numerator":"1","denominator":"2"}}'::jsonb, '{}'::jsonb, ${supDigest})`;
  await ledger.rechargeSupplier({
    account_id: accountId,
    currency: "USD",
    external_ref: "pre-0004-sup",
    amount_atoms: "1000000",
  });
  return {
    ledger,
    billingAccount: { id: billingAccountId },
    accountA: { id: accountId },
    channelA: { id: channelId },
    billingPrice: { id: billingPriceId },
    costPrice: { id: costPriceId },
  };
}
