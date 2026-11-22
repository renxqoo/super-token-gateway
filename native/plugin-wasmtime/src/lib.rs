use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;
use std::slice;
use std::sync::Arc;
use wasmtime::{
    Config, Engine, InstanceAllocationStrategy, Linker, Module, PoolingAllocationConfig,
    ResourceLimiter, Store, StoreLimits, StoreLimitsBuilder,
};

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
    memory_limit_exceeded: bool,
}

impl ResourceLimiter for HostState {
    fn memory_growing(
        &mut self,
        current: usize,
        desired: usize,
        maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        let allowed = self.limits.memory_growing(current, desired, maximum)?;
        if !allowed {
            self.memory_limit_exceeded = true;
        }
        Ok(allowed)
    }

    fn table_growing(
        &mut self,
        current: usize,
        desired: usize,
        maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        self.limits.table_growing(current, desired, maximum)
    }

    fn instances(&self) -> usize {
        self.limits.instances()
    }

    fn tables(&self) -> usize {
        self.limits.tables()
    }

    fn memories(&self) -> usize {
        self.limits.memories()
    }
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
    catch_unwind(AssertUnwindSafe(|| {
        let max_mem = (max_memory_bytes as usize).max(64 << 20);
        let mut cfg = Config::new();
        cfg.consume_fuel(true);
        cfg.cranelift_opt_level(wasmtime::OptLevel::Speed);
        cfg.static_memory_maximum_size(max_mem as u64);
        cfg.memory_guard_size(64 << 10);
        let mut pool = PoolingAllocationConfig::default();
        pool.total_core_instances(2)
            .total_memories(2)
            .total_tables(8)
            .max_memory_size(max_mem);
        cfg.allocation_strategy(InstanceAllocationStrategy::Pooling(pool));
        let Ok(engine) = Engine::new(&cfg) else {
            return ptr::null_mut();
        };
        Box::into_raw(Box::new(EngineHandle {
            engine,
            max_memory_bytes: max_mem,
            default_fuel,
        }))
    }))
    .unwrap_or(ptr::null_mut())
}

#[unsafe(no_mangle)]
pub extern "C" fn stg_wasm_engine_free(engine: *mut EngineHandle) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if !engine.is_null() {
            unsafe {
                drop(Box::from_raw(engine));
            }
        }
    }));
}

#[unsafe(no_mangle)]
pub extern "C" fn stg_wasm_module_compile(
    engine: *const EngineHandle,
    wasm_ptr: *const u8,
    wasm_len: usize,
    err_buf: *mut u8,
    err_cap: usize,
) -> *mut ModuleHandle {
    catch_unwind(AssertUnwindSafe(|| {
        if engine.is_null() || wasm_ptr.is_null() || wasm_len == 0 {
            write_error(err_buf, err_cap, "invalid compile arguments");
            return ptr::null_mut();
        }
        let eng = unsafe { &*engine };
        let bytes = unsafe { slice::from_raw_parts(wasm_ptr, wasm_len) };
        match Module::new(&eng.engine, bytes) {
            Ok(module) => {
                if module.imports().next().is_some() {
                    write_error(err_buf, err_cap, "WASM plugin cannot declare host imports");
                    return ptr::null_mut();
                }
                Box::into_raw(Box::new(ModuleHandle {
                    module: Arc::new(module),
                }))
            }
            Err(e) => {
                write_error(err_buf, err_cap, &e.to_string());
                ptr::null_mut()
            }
        }
    }))
    .unwrap_or_else(|_| {
        write_error(err_buf, err_cap, "native panic during compile");
        ptr::null_mut()
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn stg_wasm_module_free(module: *mut ModuleHandle) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if !module.is_null() {
            unsafe {
                drop(Box::from_raw(module));
            }
        }
    }));
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
    catch_unwind(AssertUnwindSafe(|| {
        if engine.is_null()
            || module.is_null()
            || hook_ptr.is_null()
            || hook_len == 0
            || out_ptr.is_null()
            || out_len.is_null()
        {
            write_error(err_buf, err_cap, "invalid invoke arguments");
            return -1;
        }
        if input_len > i32::MAX as usize {
            write_error(err_buf, err_cap, "input exceeds i32 length bound");
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
            .memories(1)
            .tables(4)
            .build();
        let mut store = Store::new(
            &eng.engine,
            HostState {
                limits,
                memory_limit_exceeded: false,
            },
        );
        store.limiter(|s| s as &mut dyn ResourceLimiter);
        let fuel = if fuel_limit > 0 {
            fuel_limit
        } else {
            eng.default_fuel
        };
        if let Err(e) = store.set_fuel(fuel) {
            write_error(err_buf, err_cap, &e.to_string());
            return -2;
        }

        let linker = Linker::<HostState>::new(&eng.engine);
        let instance = match linker.instantiate(&mut store, &mod_h.module) {
            Ok(inst) => inst,
            Err(e) => {
                if store.data().memory_limit_exceeded {
                    write_error(err_buf, err_cap, "memory limit exceeded");
                    return -8;
                }
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
            Ok(p) if p >= 0 => p as usize,
            Ok(_) => {
                write_error(err_buf, err_cap, "guest allocation returned negative pointer");
                return -5;
            }
            Err(e) => {
                if store.data().memory_limit_exceeded {
                    write_error(err_buf, err_cap, "memory limit exceeded");
                    return -8;
                }
                write_error(err_buf, err_cap, &e.to_string());
                return -5;
            }
        };
        if let Err(e) = memory.write(&mut store, guest_in_ptr, input) {
            write_error(err_buf, err_cap, &e.to_string());
            return -5;
        }
        let packed = match hook_fn.call(&mut store, (guest_in_ptr as i32, input.len() as i32)) {
            Ok(v) => v as u64,
            Err(e) => {
                if store.data().memory_limit_exceeded {
                    write_error(err_buf, err_cap, "memory limit exceeded");
                    return -8;
                }
                write_error(err_buf, err_cap, &e.to_string());
                return -6;
            }
        };
        if store.data().memory_limit_exceeded {
            write_error(err_buf, err_cap, "memory limit exceeded");
            return -8;
        }
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
    }))
    .unwrap_or_else(|_| {
        write_error(err_buf, err_cap, "native panic during invoke");
        -99
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn stg_wasm_buf_free(ptr: *mut u8, len: usize) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if !ptr.is_null() {
            unsafe {
                let slice_ptr = ptr::slice_from_raw_parts_mut(ptr, len);
                drop(Box::from_raw(slice_ptr));
            }
        }
    }));
}
