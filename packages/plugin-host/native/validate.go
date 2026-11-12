package native

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"

	"github.com/tetratelabs/wabin/binary"
	"github.com/tetratelabs/wabin/leb128"
	"github.com/tetratelabs/wabin/wasm"
)

var extismImports = map[string]bool{}
var wasiImports = map[string]bool{}

func init() {
	for _, name := range []string{"alloc", "free", "load_u8", "input_load_u8", "store_u64", "store_u8", "input_set", "output_set", "input_length", "input_offset", "output_length", "output_offset", "length", "length_unsafe", "reset", "error_set", "error_get", "memory_bytes", "input_load_u64", "load_u64", "config_get", "var_get", "var_set", "http_request", "http_status_code", "http_headers", "get_log_level", "log_trace", "log_debug", "log_info", "log_warn", "log_error"} {
		extismImports[name] = true
	}
	for _, name := range []string{"random_get", "environ_get", "environ_sizes_get", "clock_time_get", "fd_close", "fd_fdstat_get", "fd_prestat_get", "fd_prestat_dir_name", "fd_read", "fd_seek", "fd_write", "proc_exit"} {
		wasiImports[name] = true
	}
}

func validateModule(data []byte, hash string, c Config) error {
	sum := sha256.Sum256(data)
	if !validHash.MatchString(hash) || hex.EncodeToString(sum[:]) != hash {
		return code("MODULE_HASH")
	}
	if len(data) > c.MaxModuleBytes {
		return code("MODULE_BUDGET")
	}
	if len(data) < 8 {
		return code("MODULE_INVALID")
	}
	r := bytes.NewReader(data[8:])
	seen := map[byte]bool{}
	for r.Len() > 0 {
		id, err := r.ReadByte()
		if err != nil || id > 12 {
			return code("MODULE_INVALID")
		}
		size, _, err := leb128.DecodeUint32(r)
		if err != nil || uint64(size) > uint64(r.Len()) {
			return code("MODULE_INVALID")
		}
		if id != 0 && seen[id] {
			return code("MODULE_INVALID")
		}
		seen[id] = true
		if _, err = r.Seek(int64(size), io.SeekCurrent); err != nil {
			return code("MODULE_INVALID")
		}
	}
	m, err := binary.DecodeModule(data, wasm.CoreFeaturesV2)
	if err != nil {
		return code("MODULE_INVALID")
	}
	for _, imp := range m.ImportSection {
		if imp.Type != wasm.ExternTypeFunc {
			return code("CAPABILITY_DENIED")
		}
		if !(imp.Module == "extism:host/env" && extismImports[imp.Name] || imp.Module == "wasi_snapshot_preview1" && wasiImports[imp.Name]) {
			return code("CAPABILITY_DENIED")
		}
	}
	if mem := m.MemorySection; mem != nil {
		if mem.Min > c.MemoryPages || mem.IsMaxEncoded && mem.Max > c.MemoryPages {
			return code("MEMORY_BUDGET")
		}
	}
	if len(m.TableSection) > 1 {
		return code("TABLE_BUDGET")
	}
	for _, table := range m.TableSection {
		if table.Max == nil || table.Min > c.TableElements || *table.Max > c.TableElements || table.Type != wasm.RefTypeFuncref {
			return code("TABLE_BUDGET")
		}
	}
	if len(m.FunctionSection) > 100000 || len(m.TypeSection) > 100000 || len(m.GlobalSection) > 16384 {
		return code("MODULE_BUDGET")
	}
	if m.StartSection != nil {
		return code("CAPABILITY_DENIED")
	}
	return nil
}
