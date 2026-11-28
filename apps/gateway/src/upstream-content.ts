import { optionalObject } from "@gateway/contracts";
import type { Operation, Protocol } from "@gateway/kernel";
import { copyExact } from "@gateway/protocol";

export function extractRequestText(body: Record<string, unknown>, operation: Operation): string {
  const parts: string[] = [];
  if (operation === "chat" && Array.isArray(body.messages)) {
    for (const m of body.messages) {
      const msg = optionalObject(m);
      if (typeof msg.content === "string") parts.push(msg.content);
      else if (Array.isArray(msg.content)) {
        for (const p of msg.content) {
          const part = optionalObject(p);
          if (typeof part.text === "string") parts.push(part.text);
        }
      }
    }
  } else if (operation === "messages") {
    if (typeof body.system === "string") parts.push(body.system);
    else if (Array.isArray(body.system)) {
      for (const p of body.system) {
        const part = optionalObject(p);
        if (typeof part.text === "string") parts.push(part.text);
      }
    }
    if (Array.isArray(body.messages)) {
      for (const m of body.messages) {
        const msg = optionalObject(m);
        if (typeof msg.content === "string") parts.push(msg.content);
        else if (Array.isArray(msg.content)) {
          for (const p of msg.content) {
            const part = optionalObject(p);
            if (typeof part.text === "string") parts.push(part.text);
          }
        }
      }
    }
  } else if (operation === "responses") {
    if (typeof body.instructions === "string") parts.push(body.instructions);
    if (typeof body.input === "string") parts.push(body.input);
    else if (Array.isArray(body.input)) {
      for (const item of body.input) {
        const obj = optionalObject(item);
        const content = obj.type === "function_call_output" ? obj.output : obj.content;
        if (typeof content === "string") parts.push(content);
        else if (Array.isArray(content)) {
          for (const p of content) {
            const part = optionalObject(p);
            if (typeof part.text === "string") parts.push(part.text);
          }
        }
      }
    }
  } else if (operation === "gemini") {
    const sys = optionalObject(body.systemInstruction);
    if (Array.isArray(sys.parts)) {
      for (const p of sys.parts) {
        const part = optionalObject(p);
        if (typeof part.text === "string") parts.push(part.text);
      }
    }
    if (Array.isArray(body.contents)) {
      for (const c of body.contents) {
        const content = optionalObject(c);
        if (Array.isArray(content.parts)) {
          for (const p of content.parts) {
            const part = optionalObject(p);
            if (typeof part.text === "string") parts.push(part.text);
          }
        }
      }
    }
  } else if (typeof body.prompt === "string") {
    parts.push(body.prompt);
  } else if (typeof body.input === "string") {
    parts.push(body.input);
  }
  return parts.join("\n");
}

export function replaceRequestText(
  body: Record<string, unknown>,
  operation: Operation,
  nextText: string,
): Record<string, unknown> {
  if ((operation === "chat" || operation === "messages") && Array.isArray(body.messages)) {
    const messages = body.messages.map((m) => ({ ...optionalObject(m) }));
    for (let i = messages.length - 1; i >= 0; i--) {
      const msg = messages[i]!;
      if (msg.role === "user") {
        if (typeof msg.content === "string") {
          msg.content = nextText;
          return copyExact(body, { ...body, messages });
        }
        if (Array.isArray(msg.content)) {
          const parts = msg.content.map((p) => ({ ...optionalObject(p) }));
          const idx = parts.findIndex((p) => typeof p.text === "string");
          if (idx >= 0) {
            parts[idx]!.text = nextText;
            msg.content = parts;
            return copyExact(body, { ...body, messages });
          }
        }
      }
    }
    return copyExact(body, { ...body, messages });
  }
  if (operation === "responses") {
    if (typeof body.input === "string") {
      return copyExact(body, { ...body, input: nextText });
    }
    if (Array.isArray(body.input) && body.input.length > 0) {
      const items = body.input.map((it) => {
        const item = optionalObject(it);
        return copyExact(item, { ...item });
      });
      for (let i = items.length - 1; i >= 0; i--) {
        if (typeof items[i]!.content === "string") {
          items[i]!.content = nextText;
          return copyExact(body, { ...body, input: items });
        }
      }
    }
    return body;
  }
  if (operation === "gemini" && Array.isArray(body.contents) && body.contents.length > 0) {
    const contents = body.contents.map((c) => ({ ...optionalObject(c) }));
    const last = contents[contents.length - 1]!;
    last.parts = [{ text: nextText }];
    return copyExact(body, { ...body, contents });
  }
  if (typeof body.prompt === "string") {
    return copyExact(body, { ...body, prompt: nextText });
  }
  if (typeof body.input === "string") {
    return copyExact(body, { ...body, input: nextText });
  }
  return body;
}

export async function transformRequestTexts(
  body: Record<string, unknown>,
  operation: Operation,
  transform: (text: string) => Promise<{ blocked: boolean; text: string }>,
): Promise<{ blocked: boolean; body: Record<string, unknown> }> {
  if (operation === "chat" && Array.isArray(body.messages)) {
    let changed = false;
    const messages = body.messages.map((m) => ({ ...optionalObject(m) }));
    for (const msg of messages) {
      if (typeof msg.content === "string" && msg.content.length > 0) {
        const res = await transform(msg.content);
        if (res.blocked) return { blocked: true, body };
        if (res.text !== msg.content) {
          msg.content = res.text;
          changed = true;
        }
      } else if (Array.isArray(msg.content)) {
        const parts = msg.content.map((p) => ({ ...optionalObject(p) }));
        let partChanged = false;
        for (const part of parts) {
          if (typeof part.text === "string" && part.text.length > 0) {
            const res = await transform(part.text);
            if (res.blocked) return { blocked: true, body };
            if (res.text !== part.text) {
              part.text = res.text;
              partChanged = true;
            }
          }
        }
        if (partChanged) {
          msg.content = parts;
          changed = true;
        }
      }
    }
    return { blocked: false, body: changed ? copyExact(body, { ...body, messages }) : body };
  }

  if (operation === "messages") {
    let changed = false;
    let nextSystem = body.system;
    if (typeof body.system === "string" && body.system.length > 0) {
      const res = await transform(body.system);
      if (res.blocked) return { blocked: true, body };
      if (res.text !== body.system) {
        nextSystem = res.text;
        changed = true;
      }
    } else if (Array.isArray(body.system)) {
      const sysParts = body.system.map((p) => ({ ...optionalObject(p) }));
      let sysChanged = false;
      for (const part of sysParts) {
        if (typeof part.text === "string" && part.text.length > 0) {
          const res = await transform(part.text);
          if (res.blocked) return { blocked: true, body };
          if (res.text !== part.text) {
            part.text = res.text;
            sysChanged = true;
          }
        }
      }
      if (sysChanged) {
        nextSystem = sysParts;
        changed = true;
      }
    }
    const messages = Array.isArray(body.messages)
      ? body.messages.map((m) => ({ ...optionalObject(m) }))
      : null;
    if (messages) {
      for (const msg of messages) {
        if (typeof msg.content === "string" && msg.content.length > 0) {
          const res = await transform(msg.content);
          if (res.blocked) return { blocked: true, body };
          if (res.text !== msg.content) {
            msg.content = res.text;
            changed = true;
          }
        } else if (Array.isArray(msg.content)) {
          const parts = msg.content.map((p) => ({ ...optionalObject(p) }));
          let partChanged = false;
          for (const part of parts) {
            if (typeof part.text === "string" && part.text.length > 0) {
              const res = await transform(part.text);
              if (res.blocked) return { blocked: true, body };
              if (res.text !== part.text) {
                part.text = res.text;
                partChanged = true;
              }
            }
          }
          if (partChanged) {
            msg.content = parts;
            changed = true;
          }
        }
      }
    }
    if (!changed) return { blocked: false, body };
    return {
      blocked: false,
      body: copyExact(body, {
        ...body,
        ...(nextSystem !== body.system ? { system: nextSystem } : {}),
        ...(messages ? { messages } : {}),
      }),
    };
  }

  if (operation === "responses") {
    let changed = false;
    let nextInstructions = body.instructions;
    if (typeof body.instructions === "string" && body.instructions.length > 0) {
      const res = await transform(body.instructions);
      if (res.blocked) return { blocked: true, body };
      if (res.text !== body.instructions) {
        nextInstructions = res.text;
        changed = true;
      }
    }
    let nextInput = body.input;
    if (typeof body.input === "string" && body.input.length > 0) {
      const res = await transform(body.input);
      if (res.blocked) return { blocked: true, body };
      if (res.text !== body.input) {
        nextInput = res.text;
        changed = true;
      }
    } else if (Array.isArray(body.input)) {
      const items = body.input.map((it) => {
        const item = optionalObject(it);
        return copyExact(item, { ...item });
      });
      let inputChanged = false;
      for (const item of items) {
        const field = item.type === "function_call_output" ? "output" : "content";
        const content = item[field];
        if (typeof content === "string" && content.length > 0) {
          const res = await transform(content);
          if (res.blocked) return { blocked: true, body };
          if (res.text !== content) {
            item[field] = res.text;
            inputChanged = true;
          }
        } else if (Array.isArray(content)) {
          const parts = content.map((p) => ({ ...optionalObject(p) }));
          let partChanged = false;
          for (const part of parts) {
            if (typeof part.text === "string" && part.text.length > 0) {
              const res = await transform(part.text);
              if (res.blocked) return { blocked: true, body };
              if (res.text !== part.text) {
                part.text = res.text;
                partChanged = true;
              }
            }
          }
          if (partChanged) {
            item[field] = parts;
            inputChanged = true;
          }
        }
      }
      if (inputChanged) {
        nextInput = items;
        changed = true;
      }
    }
    if (!changed) return { blocked: false, body };
    return {
      blocked: false,
      body: copyExact(body, {
        ...body,
        ...(nextInstructions !== body.instructions ? { instructions: nextInstructions } : {}),
        ...(nextInput !== body.input ? { input: nextInput } : {}),
      }),
    };
  }

  if (operation === "gemini") {
    let changed = false;
    let nextSystemInstruction = body.systemInstruction;
    const sysObj = optionalObject(body.systemInstruction);
    if (Array.isArray(sysObj.parts)) {
      const sysParts = sysObj.parts.map((p) => ({ ...optionalObject(p) }));
      let sysChanged = false;
      for (const part of sysParts) {
        if (typeof part.text === "string" && part.text.length > 0) {
          const res = await transform(part.text);
          if (res.blocked) return { blocked: true, body };
          if (res.text !== part.text) {
            part.text = res.text;
            sysChanged = true;
          }
        }
      }
      if (sysChanged) {
        nextSystemInstruction = copyExact(sysObj, { ...sysObj, parts: sysParts });
        changed = true;
      }
    }
    const contents = Array.isArray(body.contents)
      ? body.contents.map((c) => ({ ...optionalObject(c) }))
      : null;
    if (contents) {
      for (const c of contents) {
        if (Array.isArray(c.parts)) {
          const parts = c.parts.map((p) => ({ ...optionalObject(p) }));
          let partChanged = false;
          for (const part of parts) {
            if (typeof part.text === "string" && part.text.length > 0) {
              const res = await transform(part.text);
              if (res.blocked) return { blocked: true, body };
              if (res.text !== part.text) {
                part.text = res.text;
                partChanged = true;
              }
            }
          }
          if (partChanged) {
            c.parts = parts;
            changed = true;
          }
        }
      }
    }
    if (!changed) return { blocked: false, body };
    return {
      blocked: false,
      body: copyExact(body, {
        ...body,
        ...(nextSystemInstruction !== body.systemInstruction
          ? { systemInstruction: nextSystemInstruction }
          : {}),
        ...(contents ? { contents } : {}),
      }),
    };
  }

  const singleText = extractRequestText(body, operation);
  if (!singleText) return { blocked: false, body };
  const res = await transform(singleText);
  if (res.blocked) return { blocked: true, body };
  if (res.text === singleText) return { blocked: false, body };
  return { blocked: false, body: replaceRequestText(body, operation, res.text) };
}

export function extractResponseText(body: Record<string, unknown>, protocol: Protocol): string {
  if (protocol === "chat") {
    const choices = Array.isArray(body.choices) ? body.choices : [];
    const first = optionalObject(choices[0]);
    const msg = optionalObject(first.message);
    return typeof msg.content === "string" ? msg.content : "";
  }
  if (protocol === "messages") {
    const content = Array.isArray(body.content) ? body.content : [];
    return content
      .map((b) => {
        const block = optionalObject(b);
        return block.type === "text" && typeof block.text === "string" ? block.text : "";
      })
      .join("");
  }
  if (protocol === "responses") {
    const output = Array.isArray(body.output) ? body.output : [];
    const out: string[] = [];
    for (const item of output) {
      const obj = optionalObject(item);
      if (Array.isArray(obj.content)) {
        for (const p of obj.content) {
          const part = optionalObject(p);
          if (typeof part.text === "string") out.push(part.text);
        }
      }
    }
    return out.join("");
  }
  const candidates = Array.isArray(body.candidates) ? body.candidates : [];
  const content = optionalObject(optionalObject(candidates[0]).content);
  const parts = Array.isArray(content.parts) ? content.parts : [];
  return parts
    .map((p) => {
      const part = optionalObject(p);
      return typeof part.text === "string" && part.thought !== true ? part.text : "";
    })
    .join("");
}

export function replaceResponseText(
  body: Record<string, unknown>,
  protocol: Protocol,
  nextText: string,
): Record<string, unknown> {
  if (protocol === "chat" && Array.isArray(body.choices) && body.choices.length > 0) {
    const choice = { ...optionalObject(body.choices[0]) };
    const message = { ...optionalObject(choice.message), content: nextText };
    choice.message = message;
    return { ...body, choices: [choice, ...body.choices.slice(1)] };
  }
  if (protocol === "messages") {
    const existing = Array.isArray(body.content) ? body.content : [];
    const nonText = existing.filter((b) => optionalObject(b).type !== "text");
    return { ...body, content: [{ type: "text", text: nextText }, ...nonText] };
  }
  if (protocol === "responses") {
    const existing = Array.isArray(body.output) ? body.output : [];
    const nonMsg = existing.filter((it) => optionalObject(it).type !== "message");
    return {
      ...body,
      output: [
        {
          id: "msg_0",
          type: "message",
          role: "assistant",
          status: "completed",
          content: [{ type: "output_text", text: nextText, annotations: [] }],
        },
        ...nonMsg,
      ],
    };
  }
  if (protocol === "gemini" && Array.isArray(body.candidates) && body.candidates.length > 0) {
    const cand = { ...optionalObject(body.candidates[0]) };
    const existingParts = Array.isArray(optionalObject(cand.content).parts)
      ? (optionalObject(cand.content).parts as unknown[])
      : [];
    const nonTextParts = existingParts.filter(
      (p) => typeof optionalObject(p).text !== "string" || optionalObject(p).thought === true,
    );
    cand.content = { role: "model", parts: [...nonTextParts, { text: nextText }] };
    return { ...body, candidates: [cand, ...body.candidates.slice(1)] };
  }
  return body;
}
