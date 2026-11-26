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
