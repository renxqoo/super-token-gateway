import { expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { encodeForm, readForm } from "../form.ts";

const image = new Uint8Array([0, 255, 13, 10, 45, 45]);

async function multipart(build: (form: FormData) => void) {
  const form = new FormData();
  build(form);
  const response = new Response(form);
  const type = response.headers.get("content-type")!;
  return { body: new Uint8Array(await response.arrayBuffer()), type };
}

async function sample() {
  const encoded = await multipart((form) => {
    form.append("model", "logical");
    form.append("prompt", "加帽子");
    form.append("image[]", new File([image], "a.png", { type: "image/png" }));
    form.append("image[]", new File([image], "a.png", { type: "image/png" }));
  });
  return readForm(encoded.body, encoded.type);
}

test("a multipart body becomes a JSON view with text fields and digest-only file descriptors", async () => {
  const { view } = await sample();
  const descriptor = {
    file: {
      name: "a.png",
      type: "image/png",
      bytes: 6,
      sha256: createHash("sha256").update(image).digest("hex"),
    },
  };
  expect(view).toEqual({
    model: "logical",
    prompt: "加帽子",
    "image[]": [descriptor, descriptor],
  });
});

test("an edited view is re-encoded with the original file bytes and the edited text fields", async () => {
  const { view, files } = await sample();
  const body = encodeForm({ ...view, model: "upstream" }, files, "stg-boundary");
  const parsed = await new Response(body.slice(), {
    headers: { "content-type": "multipart/form-data; boundary=stg-boundary" },
  }).formData();
  expect(parsed.get("model")).toBe("upstream");
  expect(parsed.get("prompt")).toBe("加帽子");
  const parts = parsed.getAll("image[]") as File[];
  expect(parts).toHaveLength(2);
  for (const part of parts) {
    expect(part.name).toBe("a.png");
    expect(part.type).toBe("image/png");
    expect(new Uint8Array(await part.arrayBuffer())).toEqual(image);
  }
});

test("a plugin cannot forge, duplicate or drop file parts", async () => {
  const { view, files } = await sample();
  const [first] = view["image[]"] as Record<string, Record<string, unknown>>[];
  const forged = { file: { ...first!.file, bytes: 7 } };
  expect(() => encodeForm({ ...view, "image[]": [first, forged] }, files, "b")).toThrow();
  expect(() => encodeForm({ ...view, "image[]": [first, first, first] }, files, "b")).toThrow();
  expect(() => encodeForm({ ...view, "image[]": first }, files, "b")).toThrow();
  expect(() => encodeForm({ ...view, prompt: 1 }, files, "b")).toThrow();
});

test("singular multipart fields may appear only once", async () => {
  const encoded = await multipart((form) => {
    form.append("model", "logical");
    form.append("n", "1");
    form.append("n", "2");
  });
  await expect(readForm(encoded.body, encoded.type)).rejects.toMatchObject({
    status: 400,
    code: "invalid_request",
  });
});

test("malformed multipart bodies are a client error", async () => {
  await expect(
    readForm(new TextEncoder().encode("garbage"), "multipart/form-data; boundary=x"),
  ).rejects.toMatchObject({ status: 400, code: "invalid_request" });
});
