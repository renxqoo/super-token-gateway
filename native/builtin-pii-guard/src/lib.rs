use std::slice;

const PASS_JSON: &str = r#"{"action":"PASS"}"#;
const BLOCK_PRIVATE_KEY_JSON: &str = r#"{"action":"BLOCK","rule_code":"DLP_PRIVATE_KEY","http_status":400,"safe_message":"Private key material is not permitted"}"#;
const BLOCK_INJECTION_JSON: &str = r#"{"action":"BLOCK","rule_code":"PROMPT_INJECTION","http_status":400,"safe_message":"Prompt rejected by security policy"}"#;

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

fn decode_hex4(b: &[u8], pos: usize) -> Option<u16> {
    if pos + 4 > b.len() {
        return None;
    }
    let mut val: u16 = 0;
    for i in 0..4 {
        let d = match b[pos + i] {
            b'0'..=b'9' => (b[pos + i] - b'0') as u16,
            b'a'..=b'f' => (b[pos + i] - b'a' + 10) as u16,
            b'A'..=b'F' => (b[pos + i] - b'A' + 10) as u16,
            _ => return None,
        };
        val = (val << 4) | d;
    }
    Some(val)
}

fn parse_json_string(b: &[u8], start_quote: usize) -> Option<(String, usize)> {
    if start_quote >= b.len() || b[start_quote] != b'"' {
        return None;
    }
    let mut out = Vec::new();
    let mut i = start_quote + 1;
    while i < b.len() {
        let c = b[i];
        if c == b'"' {
            let s = String::from_utf8(out).ok()?;
            return Some((s, i + 1));
        }
        if c == b'\\' {
            i += 1;
            if i >= b.len() {
                return None;
            }
            match b[i] {
                b'"' => out.push(b'"'),
                b'\\' => out.push(b'\\'),
                b'/' => out.push(b'/'),
                b'b' => out.push(0x08),
                b'f' => out.push(0x0c),
                b'n' => out.push(b'\n'),
                b'r' => out.push(b'\r'),
                b't' => out.push(b'\t'),
                b'u' => {
                    let u1 = decode_hex4(b, i + 1)?;
                    i += 4;
                    let ch = if (0xd800..=0xdbff).contains(&u1)
                        && i + 6 < b.len()
                        && b[i + 1] == b'\\'
                        && b[i + 2] == b'u'
                    {
                        if let Some(u2) = decode_hex4(b, i + 3) {
                            if (0xdc00..=0xdfff).contains(&u2) {
                                i += 6;
                                let cp = 0x10000
                                    + (((u1 as u32 - 0xd800) << 10) | (u2 as u32 - 0xdc00));
                                char::from_u32(cp).unwrap_or('\u{FFFD}')
                            } else {
                                '\u{FFFD}'
                            }
                        } else {
                            '\u{FFFD}'
                        }
                    } else {
                        char::from_u32(u1 as u32).unwrap_or('\u{FFFD}')
                    };
                    let mut tmp = [0u8; 4];
                    out.extend_from_slice(ch.encode_utf8(&mut tmp).as_bytes());
                }
                other => out.push(other),
            }
            i += 1;
        } else {
            out.push(c);
            i += 1;
        }
    }
    None
}

fn skip_ws(b: &[u8], mut i: usize) -> usize {
    while i < b.len() && matches!(b[i], b' ' | b'\t' | b'\r' | b'\n') {
        i += 1;
    }
    i
}

fn skip_json_value(b: &[u8], start: usize) -> Option<usize> {
    let i = skip_ws(b, start);
    if i >= b.len() {
        return None;
    }
    match b[i] {
        b'"' => parse_json_string(b, i).map(|(_, next)| next),
        b'{' => {
            let mut cur = skip_ws(b, i + 1);
            if cur < b.len() && b[cur] == b'}' {
                return Some(cur + 1);
            }
            while cur < b.len() {
                let (_, after_key) = parse_json_string(b, cur)?;
                cur = skip_ws(b, after_key);
                if cur >= b.len() || b[cur] != b':' {
                    return None;
                }
                cur = skip_json_value(b, cur + 1)?;
                cur = skip_ws(b, cur);
                if cur < b.len() && b[cur] == b',' {
                    cur = skip_ws(b, cur + 1);
                } else if cur < b.len() && b[cur] == b'}' {
                    return Some(cur + 1);
                } else {
                    return None;
                }
            }
            None
        }
        b'[' => {
            let mut cur = skip_ws(b, i + 1);
            if cur < b.len() && b[cur] == b']' {
                return Some(cur + 1);
            }
            while cur < b.len() {
                cur = skip_json_value(b, cur)?;
                cur = skip_ws(b, cur);
                if cur < b.len() && b[cur] == b',' {
                    cur = skip_ws(b, cur + 1);
                } else if cur < b.len() && b[cur] == b']' {
                    return Some(cur + 1);
                } else {
                    return None;
                }
            }
            None
        }
        _ => {
            let mut cur = i;
            while cur < b.len() && !matches!(b[cur], b',' | b'}' | b']' | b' ' | b'\t' | b'\r' | b'\n') {
                cur += 1;
            }
            Some(cur)
        }
    }
}

fn extract_text_field(input: &[u8]) -> Option<String> {
    let mut cur = skip_ws(input, 0);
    if cur >= input.len() || input[cur] != b'{' {
        return None;
    }
    cur = skip_ws(input, cur + 1);
    while cur < input.len() && input[cur] != b'}' {
        let (key, after_key) = parse_json_string(input, cur)?;
        cur = skip_ws(input, after_key);
        if cur >= input.len() || input[cur] != b':' {
            return None;
        }
        cur = skip_ws(input, cur + 1);
        if key == "text" {
            let (val, _) = parse_json_string(input, cur)?;
            return Some(val);
        }
        cur = skip_json_value(input, cur)?;
        cur = skip_ws(input, cur);
        if cur < input.len() && input[cur] == b',' {
            cur = skip_ws(input, cur + 1);
        }
    }
    None
}

fn write_json_escaped(s: &str, out: &mut String) {
    out.push('"');
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                let code = c as u32;
                let hex = b"0123456789abcdef";
                out.push_str("\\u00");
                out.push(hex[((code >> 4) & 0xf) as usize] as char);
                out.push(hex[(code & 0xf) as usize] as char);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

fn contains_ascii_ci(haystack: &[u8], needle_lower: &[u8]) -> bool {
    if needle_lower.is_empty() || needle_lower.len() > haystack.len() {
        return false;
    }
    for i in 0..=haystack.len() - needle_lower.len() {
        let mut ok = true;
        for j in 0..needle_lower.len() {
            if haystack[i + j].to_ascii_lowercase() != needle_lower[j] {
                ok = false;
                break;
            }
        }
        if ok {
            return true;
        }
    }
    false
}

fn is_word_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

fn is_email_local_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'%' | b'+' | b'-')
}

fn is_token_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'-'
}

fn match_api_key(b: &[u8], i: usize) -> Option<(usize, &'static str)> {
    if i > 0 && is_word_byte(b[i - 1]) {
        return None;
    }
    if b[i..].starts_with(b"sk-") {
        let mut end = i + 3;
        while end < b.len() && is_token_byte(b[end]) {
            end += 1;
        }
        if end - (i + 3) >= 20 {
            return Some((end, "[REDACTED_API_KEY]"));
        }
    }
    if b[i..].starts_with(b"AKIA") && i + 20 <= b.len() {
        let valid = b[i + 4..i + 20]
            .iter()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit());
        let after_ok = i + 20 == b.len() || !b[i + 20].is_ascii_alphanumeric();
        if valid && after_ok {
            return Some((i + 20, "[REDACTED_AWS_KEY]"));
        }
    }
    if b[i..].starts_with(b"ghp_")
        || b[i..].starts_with(b"gho_")
        || b[i..].starts_with(b"ghu_")
        || b[i..].starts_with(b"ghs_")
    {
        let mut end = i + 4;
        while end < b.len() && (b[end].is_ascii_alphanumeric() || b[end] == b'_') {
            end += 1;
        }
        if end - (i + 4) >= 20 {
            return Some((end, "[REDACTED_GH_TOKEN]"));
        }
    }
    None
}

fn match_email(b: &[u8], i: usize) -> Option<usize> {
    if i > 0 && is_email_local_byte(b[i - 1]) {
        return None;
    }
    if !b[i].is_ascii_alphanumeric() {
        return None;
    }
    let mut cur = i;
    while cur < b.len() && is_email_local_byte(b[cur]) {
        cur += 1;
    }
    if cur == i || cur >= b.len() || b[cur] != b'@' || !b[cur - 1].is_ascii_alphanumeric() {
        return None;
    }
    cur += 1;
    let domain_start = cur;
    let mut last_dot: Option<usize> = None;
    while cur < b.len() && (b[cur].is_ascii_alphanumeric() || b[cur] == b'-' || b[cur] == b'.') {
        if b[cur] == b'.' {
            if cur == domain_start || b[cur - 1] == b'.' {
                break;
            }
            last_dot = Some(cur);
        }
        cur += 1;
    }
    while cur > domain_start && b[cur - 1] == b'.' {
        cur -= 1;
    }
    let dot = last_dot?;
    if dot <= domain_start || dot + 2 >= cur {
        return None;
    }
    let tld = &b[dot + 1..cur];
    if tld.len() < 2 || tld.len() > 24 || !tld.iter().all(|c| c.is_ascii_alphabetic()) {
        return None;
    }
    Some(cur)
}

fn match_us_ssn(b: &[u8], i: usize) -> Option<usize> {
    if i + 11 > b.len() {
        return None;
    }
    if i > 0 && b[i - 1].is_ascii_digit() {
        return None;
    }
    if i + 11 < b.len() && b[i + 11].is_ascii_digit() {
        return None;
    }
    if b[i + 3] != b'-' || b[i + 6] != b'-' {
        return None;
    }
    let idx = [0, 1, 2, 4, 5, 7, 8, 9, 10];
    if !idx.iter().all(|&k| b[i + k].is_ascii_digit()) {
        return None;
    }
    let area = (b[i] - b'0') as u16 * 100 + (b[i + 1] - b'0') as u16 * 10 + (b[i + 2] - b'0') as u16;
    let group = (b[i + 4] - b'0') as u16 * 10 + (b[i + 5] - b'0') as u16;
    let serial = (b[i + 7] - b'0') as u16 * 1000
        + (b[i + 8] - b'0') as u16 * 100
        + (b[i + 9] - b'0') as u16 * 10
        + (b[i + 10] - b'0') as u16;
    if area == 0 || area == 666 || area >= 900 || group == 0 || serial == 0 {
        return None;
    }
    Some(i + 11)
}

fn match_cn_id(b: &[u8], i: usize) -> Option<usize> {
    if i + 18 > b.len() {
        return None;
    }
    if i > 0 && b[i - 1].is_ascii_alphanumeric() {
        return None;
    }
    if i + 18 < b.len() && b[i + 18].is_ascii_alphanumeric() {
        return None;
    }
    if !(b'1'..=b'8').contains(&b[i]) {
        return None;
    }
    if !b[i..i + 17].iter().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let last = b[i + 17].to_ascii_uppercase();
    if !(last.is_ascii_digit() || last == b'X') {
        return None;
    }
    const WEIGHTS: [u32; 17] = [7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2];
    const CODES: &[u8; 11] = b"10X98765432";
    let mut sum: u32 = 0;
    for k in 0..17 {
        sum += ((b[i + k] - b'0') as u32) * WEIGHTS[k];
    }
    if CODES[(sum % 11) as usize] != last {
        return None;
    }
    Some(i + 18)
}

fn match_cn_phone(b: &[u8], i: usize) -> Option<usize> {
    if i + 11 > b.len() {
        return None;
    }
    if i > 0 && b[i - 1].is_ascii_digit() {
        return None;
    }
    if i + 11 < b.len() && b[i + 11].is_ascii_digit() {
        return None;
    }
    if b[i] != b'1' || !(b'3'..=b'9').contains(&b[i + 1]) {
        return None;
    }
    if !b[i + 2..i + 11].iter().all(|c| c.is_ascii_digit()) {
        return None;
    }
    Some(i + 11)
}

fn luhn_valid(digits: &[u8]) -> bool {
    if digits.len() < 13 || digits.len() > 19 {
        return false;
    }
    let mut sum: u32 = 0;
    let mut dbl = false;
    for &d in digits.iter().rev() {
        let mut n = d as u32;
        if dbl {
            n *= 2;
            if n > 9 {
                n -= 9;
            }
        }
        sum += n;
        dbl = !dbl;
    }
    sum % 10 == 0
}

fn match_card(b: &[u8], i: usize) -> Option<usize> {
    if !matches!(b[i], b'3' | b'4' | b'5' | b'6') {
        return None;
    }
    if i > 0 && b[i - 1].is_ascii_digit() {
        return None;
    }
    let mut digits = [0u8; 20];
    let mut dlen = 0usize;
    let mut cur = i;
    let mut last_digit_end = i;
    let mut prev_sep = false;
    while cur < b.len() && dlen < 20 {
        let c = b[cur];
        if c.is_ascii_digit() {
            digits[dlen] = c - b'0';
            dlen += 1;
            cur += 1;
            last_digit_end = cur;
            prev_sep = false;
        } else if (c == b' ' || c == b'-') && !prev_sep {
            prev_sep = true;
            cur += 1;
        } else {
            break;
        }
    }
    if dlen < 13 || dlen > 19 {
        return None;
    }
    if last_digit_end < b.len() && b[last_digit_end].is_ascii_digit() {
        return None;
    }
    if luhn_valid(&digits[..dlen]) {
        return Some(last_digit_end);
    }
    None
}

fn redact_text(text: &str) -> (String, bool) {
    let b = text.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0usize;
    let mut changed = false;

    while i < b.len() {
        if let Some((end, tag)) = match_api_key(b, i) {
            out.extend_from_slice(tag.as_bytes());
            i = end;
            changed = true;
            continue;
        }
        if let Some(end) = match_email(b, i) {
            out.extend_from_slice(b"[REDACTED_EMAIL]");
            i = end;
            changed = true;
            continue;
        }
        if let Some(end) = match_us_ssn(b, i) {
            out.extend_from_slice(b"[REDACTED_SSN]");
            i = end;
            changed = true;
            continue;
        }
        if let Some(end) = match_cn_id(b, i) {
            out.extend_from_slice(b"[REDACTED_CN_ID]");
            i = end;
            changed = true;
            continue;
        }
        if let Some(end) = match_card(b, i) {
            out.extend_from_slice(b"[REDACTED_CARD]");
            i = end;
            changed = true;
            continue;
        }
        if let Some(end) = match_cn_phone(b, i) {
            out.extend_from_slice(b"[REDACTED_PHONE]");
            i = end;
            changed = true;
            continue;
        }
        out.push(b[i]);
        i += 1;
    }

    if !changed {
        return (String::new(), false);
    }
    match String::from_utf8(out) {
        Ok(s) => (s, true),
        Err(_) => (String::new(), false),
    }
}

fn process_hook(ptr: i32, len: i32, check_prompt_injection: bool) -> i64 {
    if ptr == 0 || len <= 0 {
        return pack_output(PASS_JSON.as_bytes());
    }
    let input = unsafe { slice::from_raw_parts(ptr as usize as *const u8, len as usize) };
    let Some(text) = extract_text_field(input) else {
        return pack_output(PASS_JSON.as_bytes());
    };
    let text_bytes = text.as_bytes();
    if contains_ascii_ci(text_bytes, b"-----begin private key-----")
        || contains_ascii_ci(text_bytes, b"-----begin rsa private key-----")
        || contains_ascii_ci(text_bytes, b"-----begin ec private key-----")
        || contains_ascii_ci(text_bytes, b"-----begin openssh private key-----")
        || contains_ascii_ci(text_bytes, b"-----begin encrypted private key-----")
    {
        return pack_output(BLOCK_PRIVATE_KEY_JSON.as_bytes());
    }
    if check_prompt_injection
        && (contains_ascii_ci(text_bytes, b"ignore previous instructions")
            || contains_ascii_ci(text_bytes, b"ignore all previous instructions")
            || contains_ascii_ci(text_bytes, b"reveal your system prompt")
            || contains_ascii_ci(text_bytes, b"dump system prompt"))
    {
        return pack_output(BLOCK_INJECTION_JSON.as_bytes());
    }
    let (redacted, changed) = redact_text(&text);
    if !changed {
        return pack_output(PASS_JSON.as_bytes());
    }
    let mut reply = String::with_capacity(redacted.len() + 36);
    reply.push_str(r#"{"action":"TRANSFORM","text":"#);
    write_json_escaped(&redacted, &mut reply);
    reply.push('}');
    pack_output(reply.as_bytes())
}

#[unsafe(no_mangle)]
pub extern "C" fn on_request(ptr: i32, len: i32) -> i64 {
    process_hook(ptr, len, true)
}

#[unsafe(no_mangle)]
pub extern "C" fn on_response(ptr: i32, len: i32) -> i64 {
    process_hook(ptr, len, false)
}

#[unsafe(no_mangle)]
pub extern "C" fn on_stream_chunk(ptr: i32, len: i32) -> i64 {
    process_hook(ptr, len, false)
}

#[unsafe(no_mangle)]
pub extern "C" fn on_stream_end(ptr: i32, len: i32) -> i64 {
    process_hook(ptr, len, false)
}
