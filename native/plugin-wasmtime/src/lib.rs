use std::ptr;
use std::slice;
use std::sync::Arc;
use wasmtime::{Config, Engine, Linker, Module, ResourceLimiter, Store, StoreLimits, StoreLimitsBuilder};

pub struct EngineHandle {
    engine: Engine,
    max_memory_bytes: usize,
    default_fuel: u64,
}

pub struct ModuleHandle {
    module: Arc<Module>,
}

struct HostState {
    limits: StoreLimits,
}

fn write_error(buf: *mut u8, cap: usize, msg: &str) {
    if buf.is_null() || cap == 0 {
        return;
    }
    let bytes = msg.as_bytes();
    let n = bytes.len().min(cap - 1);
    unsafe {
        ptr::copy_nonoverlapping(bytes.as_ptr(), buf, n);
        *buf.add(n) = 0;
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn stg_wasm_engine_new(max_memory_bytes: u64, default_fuel: u64) -> *mut EngineHandle {
    let mut cfg = Config::new();
    cfg.consume_fuel(true);
    cfg.cranelift_opt_level(wasmtime::OptLevel::Speed);
    let Ok(engine) = Engine::new(&cfg) else {
        return ptr::null_mut();
    };
    Box::into_raw(Box::new(EngineHandle {
        engine,
        max_memory_bytes: max_memory_bytes as usize,
        default_fuel,
    }))
}

#[unsafe(no_mangle)]
pub extern "C" fn stg_wasm_engine_free(engine: *mut EngineHandle) {
    if !engine.is_null() {
        unsafe {
            drop(Box::from_raw(engine));
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn stg_wasm_module_compile(
    engine: *const EngineHandle,
    wasm_ptr: *const u8,
    wasm_len: usize,
    err_buf: *mut u8,
    err_cap: usize,
) -> *mut ModuleHandle {
    if engine.is_null() || wasm_ptr.is_null() || wasm_len == 0 {
        write_error(err_buf, err_cap, "invalid compile arguments");
        return ptr::null_mut();
    }
    let eng = unsafe { &*engine };
    let bytes = unsafe { slice::from_raw_parts(wasm_ptr, wasm_len) };
    match Module::new(&eng.engine, bytes) {
        Ok(module) => Box::into_raw(Box::new(ModuleHandle {
            module: Arc::new(module),
        })),
        Err(e) => {
            write_error(err_buf, err_cap, &e.to_string());
            ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn stg_wasm_module_free(module: *mut ModuleHandle) {
    if !module.is_null() {
        unsafe {
            drop(Box::from_raw(module));
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn stg_wasm_invoke(
    engine: *const EngineHandle,
    module: *const ModuleHandle,
    hook_ptr: *const u8,
    hook_len: usize,
    input_ptr: *const u8,
    input_len: usize,
    fuel_limit: u64,
    max_memory_bytes: u64,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
    err_buf: *mut u8,
    err_cap: usize,
) -> i32 {
    if engine.is_null() || module.is_null() || hook_ptr.is_null() || out_ptr.is_null() || out_len.is_null() {
        write_error(err_buf, err_cap, "invalid invoke arguments");
        return -1;
    }
    let eng = unsafe { &*engine };
    let mod_h = unsafe { &*module };
    let hook_bytes = unsafe { slice::from_raw_parts(hook_ptr, hook_len) };
    let Ok(hook_name) = std::str::from_utf8(hook_bytes) else {
        write_error(err_buf, err_cap, "invalid utf8 hook name");
        return -1;
    };
    let input = if input_ptr.is_null() || input_len == 0 {
        &[][..]
    } else {
        unsafe { slice::from_raw_parts(input_ptr, input_len) }
    };

    let mem_limit = if max_memory_bytes > 0 {
        (max_memory_bytes as usize).min(eng.max_memory_bytes)
    } else {
        eng.max_memory_bytes
    };
    let limits = StoreLimitsBuilder::new()
        .memory_size(mem_limit)
        .instances(1)
        .tables(4)
        .build();
    let mut store = Store::new(&eng.engine, HostState { limits });
    store.limiter(|s| &mut s.limits as &mut dyn ResourceLimiter);
    let fuel = if fuel_limit > 0 { fuel_limit } else { eng.default_fuel };
    if let Err(e) = store.set_fuel(fuel) {
        write_error(err_buf, err_cap, &e.to_string());
        return -2;
    }

    let linker = Linker::<HostState>::new(&eng.engine);
    let instance = match linker.instantiate(&mut store, &mod_h.module) {
        Ok(inst) => inst,
        Err(e) => {
            write_error(err_buf, err_cap, &e.to_string());
            return -3;
        }
    };
    let Some(memory) = instance.get_memory(&mut store, "memory") else {
        write_error(err_buf, err_cap, "missing exported memory");
        return -4;
    };
    let Ok(alloc_fn) = instance.get_typed_func::<i32, i32>(&mut store, "stg_alloc") else {
        write_error(err_buf, err_cap, "missing stg_alloc");
        return -4;
    };
    let Ok(hook_fn) = instance.get_typed_func::<(i32, i32), i64>(&mut store, hook_name) else {
        write_error(err_buf, err_cap, "missing exported hook");
        return -4;
    };

    let guest_in_ptr = match alloc_fn.call(&mut store, input.len() as i32) {
        Ok(p) => p,
        Err(e) => {
            write_error(err_buf, err_cap, &e.to_string());
            return -5;
        }
    };
    if let Err(e) = memory.write(&mut store, guest_in_ptr as usize, input) {
        write_error(err_buf, err_cap, &e.to_string());
        return -5;
    }
    let packed = match hook_fn.call(&mut store, (guest_in_ptr, input.len() as i32)) {
        Ok(v) => v as u64,
        Err(e) => {
            write_error(err_buf, err_cap, &e.to_string());
            return -6;
        }
    };
    let res_ptr = (packed >> 32) as usize;
    let res_len = (packed & 0xffff_ffff) as usize;
    if res_len > mem_limit {
        write_error(err_buf, err_cap, "output exceeds memory limit");
        return -7;
    }
    let mut output = vec![0u8; res_len];
    if res_len > 0 {
        if let Err(e) = memory.read(&store, res_ptr, &mut output) {
            write_error(err_buf, err_cap, &e.to_string());
            return -7;
        }
    }
    let boxed = output.into_boxed_slice();
    let len = boxed.len();
    let raw = Box::into_raw(boxed) as *mut u8;
    unsafe {
        *out_ptr = raw;
        *out_len = len;
    }
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn stg_wasm_buf_free(ptr: *mut u8, len: usize) {
    if !ptr.is_null() {
        unsafe {
            let slice_ptr = ptr::slice_from_raw_parts_mut(ptr, len);
            drop(Box::from_raw(slice_ptr));
        }
    }
}
