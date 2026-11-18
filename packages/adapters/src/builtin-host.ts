type Frame = { input: string; output?: string };
let current: Frame | undefined;

function frame(): Frame {
  if (!current) throw new Error("BUILTIN_SCOPE");
  return current;
}

export const Host = Object.freeze({
  inputString: () => frame().input,
  outputString(value: string) {
    const active = frame();
    if (active.output !== undefined) throw new Error("BUILTIN_OUTPUT");
    active.output = value;
  },
  base64ToArrayBuffer(value: string) {
    const bytes = Buffer.from(value, "base64");
    if (bytes.toString("base64") !== value) throw new Error("BUILTIN_BASE64");
    return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
  },
  arrayBufferToBase64: (value: ArrayBuffer) => Buffer.from(value).toString("base64"),
});

export function invokeBuiltin(input: string, invoke: () => void): string {
  if (current) throw new Error("BUILTIN_REENTRANT");
  const active: Frame = { input };
  current = active;
  try {
    invoke();
    if (active.output === undefined) throw new Error("BUILTIN_OUTPUT");
    return active.output;
  } finally {
    current = undefined;
  }
}
