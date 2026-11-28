export function verify(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

export async function until<T>(
  condition: () => Promise<T | undefined>,
  label: string,
  budget = 20_000,
): Promise<T> {
  const deadline = performance.now() + budget;
  let lastError: unknown;
  while (performance.now() < deadline) {
    try {
      const result = await condition();
      if (result !== undefined) return result;
    } catch (error) {
      lastError = error;
    }
    await Bun.sleep(25);
  }
  throw new Error(`${label} exceeded ${budget} ms`, { cause: lastError });
}

export const consoleRequest = (path: string, method: string, body: unknown, token?: string) =>
  new Request(`http://console.test${path}`, {
    method,
    headers: {
      "content-type": "application/json",
      ...(token ? { authorization: `Bearer ${token}` } : {}),
    },
    body: JSON.stringify(body),
  });

export async function bootstrapConsoleEnrollment(
  fetchAdmin: (req: Request) => Response | Promise<Response>,
  config: { bootstrapAdminEmail?: string; bootstrapAdminToken?: string },
  email: string,
): Promise<{ adminToken: string; registrationToken: string }> {
  const bootstrap = await fetchAdmin(
    consoleRequest("/admin/v1/auth/bootstrap", "POST", {
      email: config.bootstrapAdminEmail,
      bootstrap_token: config.bootstrapAdminToken,
      password: crypto.randomUUID(),
      display_name: "Release operator",
    }),
  );
  verify(bootstrap.status === 201, `Console runtime role bootstrap returned ${bootstrap.status}`);
  const { access_token: adminToken } = (await bootstrap.json()) as { access_token: string };
  const approval = await fetchAdmin(
    consoleRequest("/admin/v1/auth/registration-token", "POST", { email }, adminToken),
  );
  verify(approval.status === 201, `Console runtime enrollment returned ${approval.status}`);
  const { registration_token: registrationToken } = (await approval.json()) as {
    registration_token: string;
  };
  return { adminToken, registrationToken };
}
