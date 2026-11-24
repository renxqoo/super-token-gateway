type ConsoleDeployment = {
  publicUrl: string;
  adminUrl: string;
  adminAccessToken?: string;
  allowPublicLoopbackHttp?: boolean;
};

function parseOrigin(value: string, allowLoopbackHttp: boolean): URL {
  const url = new URL(value);
  const isLoopback = ["localhost", "127.0.0.1", "[::1]"].includes(url.hostname);
  if (
    url.username ||
    url.password ||
    url.pathname !== "/" ||
    url.search ||
    url.hash ||
    (url.protocol !== "https:" && !(url.protocol === "http:" && allowLoopbackHttp && isLoopback))
  ) {
    throw new Error(
      "Console base URL must be an HTTPS origin or an explicitly allowed loopback HTTP origin",
    );
  }
  return url;
}

export async function verifyConsoleDeployment(options: ConsoleDeployment) {
  const publicOrigin = parseOrigin(options.publicUrl, options.allowPublicLoopbackHttp ?? false);
  const adminOrigin = parseOrigin(options.adminUrl, true);
  if (publicOrigin.origin === adminOrigin.origin)
    throw new Error("Console listeners must use distinct origins");
  let checks = 0;

  async function probe(
    origin: URL,
    path: string,
    status: number,
    options?: {
      token?: string;
      body?: string;
      validate?: (value: Record<string, unknown>) => boolean;
    },
  ): Promise<void> {
    const response = await fetch(new URL(path, origin), {
      method: options?.body ? "POST" : "GET",
      headers: {
        accept: "application/json",
        ...(options?.body ? { "content-type": "application/json" } : {}),
        ...(options?.token ? { authorization: `Bearer ${options.token}` } : {}),
      },
      ...(options?.body ? { body: options.body } : {}),
      redirect: "manual",
      signal: AbortSignal.timeout(5_000),
    });
    if (response.status !== status) {
      await response.body?.cancel();
      throw new Error(`${origin.origin}${path} returned ${response.status}; expected ${status}`);
    }
    if (options?.validate) {
      const body: unknown = await response.json().catch(() => null);
      if (!body || typeof body !== "object" || !options.validate(body as Record<string, unknown>)) {
        throw new Error(`${origin.origin}${path} returned an unexpected response`);
      }
    } else {
      await response.body?.cancel();
    }
    checks++;
  }

  const emptyBatch = JSON.stringify({ events: [] });
  await probe(publicOrigin, "/livez", 200, { validate: (body) => body.status === "ok" });
  await probe(publicOrigin, "/readyz", 200, { validate: (body) => body.status === "ready" });
  await probe(publicOrigin, "/api/v1/models", 200, {
    validate: (body) => Array.isArray(body.items),
  });
  await probe(publicOrigin, "/admin/v1/users", 404);
  await probe(publicOrigin, "/internal/outbox/events", 404, { body: emptyBatch });
  await probe(adminOrigin, "/livez", 200, { validate: (body) => body.status === "ok" });
  await probe(adminOrigin, "/readyz", 200, { validate: (body) => body.status === "ready" });
  await probe(adminOrigin, "/api/v1/models", 404);
  await probe(adminOrigin, "/admin/v1/users", 401);
  await probe(adminOrigin, "/internal/outbox/events", 401, { body: emptyBatch });
  if (options.adminAccessToken) {
    const token = options.adminAccessToken;
    await probe(adminOrigin, "/admin/v1/sync/organizations", 200, {
      token,
      validate: (body) => body.pending === 0,
    });
    await probe(adminOrigin, "/admin/v1/channels/health", 200, {
      token,
      validate: (body) => Array.isArray(body.channels),
    });
  }
  return {
    console_deployment_verified: true,
    checks,
    authenticated_checks: Boolean(options.adminAccessToken),
  };
}

if (import.meta.main) {
  const publicUrl = process.env.CONSOLE_ACCEPTANCE_PUBLIC_URL;
  const adminUrl = process.env.CONSOLE_ACCEPTANCE_ADMIN_URL;
  const adminAccessToken = process.env.CONSOLE_ACCEPTANCE_ADMIN_TOKEN;
  if (!publicUrl || !adminUrl || !adminAccessToken) {
    throw new Error(
      "Set CONSOLE_ACCEPTANCE_PUBLIC_URL, CONSOLE_ACCEPTANCE_ADMIN_URL and CONSOLE_ACCEPTANCE_ADMIN_TOKEN",
    );
  }
  console.log(
    JSON.stringify(
      await verifyConsoleDeployment({
        publicUrl,
        adminUrl,
        adminAccessToken,
        allowPublicLoopbackHttp: process.env.CONSOLE_ACCEPTANCE_ALLOW_PUBLIC_LOOPBACK_HTTP === "1",
      }),
    ),
  );
}
