import { seedMixedPlugins } from "./bench-mixed.ts";

export async function seedProductionBenchmarkCatalog({
  internalPorts,
  adminHeaders,
  mockPorts,
  baseEnv,
  maxInFlight,
  mockNodes,
  upstreamDelayMs,
  longStreamMs,
  isMixed,
  billingAccountCount,
}: {
  internalPorts: number[];
  adminHeaders: { authorization: string; "content-type": string };
  mockPorts: number[];
  baseEnv: Record<string, string>;
  maxInFlight: number;
  mockNodes: number;
  upstreamDelayMs: number;
  longStreamMs: 5000 | 2500;
  isMixed: boolean;
  billingAccountCount: number;
}) {
  const callInternal = async (method: string, path: string, body?: unknown) => {
    const res = await fetch(`http://127.0.0.1:${internalPorts[0]}${path}`, {
      method,
      headers: adminHeaders,
      ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
    });
    if (!res.ok) throw new Error(`Internal ${path} failed: ${res.status} ${await res.text()}`);
    return (await res.json()) as Record<string, unknown>;
  };

  const acc = await callInternal("PUT", "/internal/v1/upstream-accounts", { name: "bprod-acc" });
  await callInternal("POST", `/internal/v1/upstream-accounts/${acc.id}/recharges`, {
    currency: "USD",
    external_ref: "bprod-sup-1",
    amount_atoms: "100000000000000",
  });
  const primaryChannels = await Promise.all(
    mockPorts.map((port, idx) =>
      callInternal("PUT", "/internal/v1/channels", {
        account_id: acc.id,
        name: idx === 0 ? "bprod-primary" : `bprod-primary-${idx + 1}`,
        kind: "openai",
        base_url: `http://127.0.0.1:${port}/openai`,
        api_key: `sk-bprod-p-${idx}`,
        rpm: 10_000_000,
        concurrency: Math.max(10_000, maxInFlight),
      }),
    ),
  );
  const chPrimary = primaryChannels[0]!;
  const chBackup = await callInternal("PUT", "/internal/v1/channels", {
    account_id: acc.id,
    name: "bprod-backup",
    kind: "openai",
    base_url: `http://127.0.0.1:${mockPorts[0]}/openai`,
    api_key: "sk-bprod-b",
    rpm: 10_000_000,
    concurrency: Math.max(10_000, maxInFlight),
  });

  await callInternal("PUT", "/internal/v1/models", {
    name: "bprod-fast",
    operations: ["chat"],
    routing_strategy: mockNodes > 1 ? "ROUND_ROBIN" : "WEIGHTED",
    max_output_tokens: 64,
  });
  for (const name of ["bprod-long", "bprod-failover"]) {
    await callInternal("PUT", "/internal/v1/models", {
      name,
      operations: ["chat"],
      max_output_tokens: 64,
    });
  }
  const custPrice = await callInternal("POST", "/internal/v1/prices", {
    side: "CUSTOMER",
    currency: "USD",
    rates: {
      input_tokens: { numerator: "10", denominator: "1" },
      cached_input_tokens: { numerator: "5", denominator: "1" },
      output_tokens: { numerator: "20", denominator: "1" },
    },
    ceilings: { input_tokens: "1000", cached_input_tokens: "1000", output_tokens: "64" },
  });
  const supPrice = await callInternal("POST", "/internal/v1/prices", {
    side: "SUPPLIER",
    currency: "USD",
    rates: {
      input_tokens: { numerator: "2", denominator: "1" },
      cached_input_tokens: { numerator: "1", denominator: "1" },
      output_tokens: { numerator: "4", denominator: "1" },
    },
    ceilings: { input_tokens: "1000", cached_input_tokens: "1000", output_tokens: "64" },
  });

  for (const ch of primaryChannels) {
    await callInternal("PUT", "/internal/v1/routes", {
      model_name: "bprod-fast",
      channel_id: ch.id,
      upstream_model: "bprod-fast-up",
      operations: ["chat"],
      billing_price_id: custPrice.id,
      cost_version_id: supPrice.id,
    });
  }
  await callInternal("PUT", "/internal/v1/routes", {
    model_name: "bprod-long",
    channel_id: chPrimary.id,
    upstream_model: "bprod-long-up",
    operations: ["chat"],
    billing_price_id: custPrice.id,
    cost_version_id: supPrice.id,
  });
  await callInternal("PUT", "/internal/v1/routes", {
    model_name: "bprod-failover",
    channel_id: chPrimary.id,
    upstream_model: "bprod-broken-up",
    operations: ["chat"],
    priority: 10,
    billing_price_id: custPrice.id,
    cost_version_id: supPrice.id,
    retry_policy: { max_attempts: 2 },
  });
  await callInternal("PUT", "/internal/v1/routes", {
    model_name: "bprod-failover",
    channel_id: chBackup.id,
    upstream_model: "bprod-fast-up",
    operations: ["chat"],
    priority: 5,
    billing_price_id: custPrice.id,
    cost_version_id: supPrice.id,
    retry_policy: { max_attempts: 2 },
  });

  if (upstreamDelayMs > 0) {
    for (const port of mockPorts) {
      await fetch(`http://127.0.0.1:${port}/__control/scenarios`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${baseEnv.MOCK_CONTROL_TOKEN}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          model: "bprod-fast-up",
          scenario: { headerDelayMs: upstreamDelayMs },
        }),
      });
    }
  }
  await fetch(`http://127.0.0.1:${mockPorts[0]}/__control/scenarios`, {
    method: "POST",
    headers: {
      authorization: `Bearer ${baseEnv.MOCK_CONTROL_TOKEN}`,
      "content-type": "application/json",
    },
    body: JSON.stringify({
      model: "bprod-long-up",
      scenario: { repeat: 8, streamDurationMs: longStreamMs },
    }),
  });
  await fetch(`http://127.0.0.1:${mockPorts[0]}/__control/scenarios`, {
    method: "POST",
    headers: {
      authorization: `Bearer ${baseEnv.MOCK_CONTROL_TOKEN}`,
      "content-type": "application/json",
    },
    body: JSON.stringify({
      model: "bprod-broken-up",
      scenario: { status: 503 },
    }),
  });

  const mixedPlugins = isMixed
    ? await seedMixedPlugins({
        callInternal,
        mockPort: mockPorts[0]!,
        mockToken: baseEnv.MOCK_CONTROL_TOKEN!,
        channelId: chPrimary.id,
        billingPriceId: custPrice.id,
        costVersionId: supPrice.id,
      })
    : null;

  const keys: string[] = [];
  for (let i = 0; i < billingAccountCount; i++) {
    const p = await callInternal("PUT", "/internal/v1/billing-accounts", {
      name: `bprod-p-${i}`,
      currency: "USD",
      rpm: 10_000_000,
      tpm: 1_000_000_000,
      concurrency: Math.max(5000, maxInFlight),
    });
    const k = await callInternal("PUT", "/internal/v1/api-keys", {
      billing_account_id: p.id,
      name: `k-${i}`,
    });
    await callInternal("POST", `/internal/v1/wallets/${p.id}/credits`, {
      currency: "USD",
      external_ref: `bprod-c-${i}`,
      amount_atoms: "1000000000000",
      kind: "CASH",
    });
    keys.push(String(k.key));
  }

  return { keys, callInternal, mixedPlugins };
}
