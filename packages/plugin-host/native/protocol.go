package native

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"time"
)

type Config struct {
	MaxBufferedOutputBytes int    `json:"maxBufferedOutputBytes"`
	MaxPendingBytes        int    `json:"maxPendingBytes"`
	Slots                  int    `json:"slots"`
	Queue                  int    `json:"queue"`
	MaxFrameBytes          int    `json:"maxFrameBytes"`
	MaxModuleBytes         int    `json:"maxModuleBytes"`
	MaxCacheBytes          int    `json:"maxCacheBytes"`
	MaxModules             int    `json:"maxModules"`
	MaxInputBytes          int    `json:"maxInputBytes"`
	MaxOutputBytes         int    `json:"maxOutputBytes"`
	MaxStateBytes          int    `json:"maxStateBytes"`
	MemoryPages            uint32 `json:"memoryPages"`
	TableElements          uint32 `json:"tableElements"`
	CompileMS              int    `json:"compileMs"`
	CallMS                 int    `json:"callMs"`
	WriteMS                int    `json:"writeMs"`
	StartupMS              int    `json:"startupMs"`
	MaxTotalRSSBytes       uint64 `json:"maxTotalRssBytes"`
	MaxWorkerRSSBytes      uint64 `json:"maxWorkerRssBytes"`
	GoMemoryBytes          int64  `json:"goMemoryBytes"`
}

func DefaultConfig() Config {
	return Config{
		Slots: 8, Queue: 24,
		MaxBufferedOutputBytes: 8 << 20, MaxPendingBytes: 64 << 20,
		MaxFrameBytes: 12 << 20, MaxModuleBytes: 8 << 20,
		MaxCacheBytes: 32 << 20, MaxModules: 4,
		MaxInputBytes: 256 << 10, MaxOutputBytes: 256 << 10, MaxStateBytes: 256 << 10,
		MemoryPages: 1024, TableElements: 16384,
		CompileMS: 5000, CallMS: 1000, WriteMS: 2000, StartupMS: 5000,
		MaxTotalRSSBytes: 1 << 30, MaxWorkerRSSBytes: 512 << 20, GoMemoryBytes: 256 << 20,
	}
}

func ParseConfig(data []byte) (Config, error) {
	c := DefaultConfig()
	if decode(data, &c) != nil {
		return Config{}, code("CONFIG_INVALID")
	}
	return c, c.Validate()
}

func (c Config) Validate() error {
	bounds := [][3]int64{
		{int64(c.Slots), 1, 64}, {int64(c.Queue), 0, 1024},
		{int64(c.MaxBufferedOutputBytes), 4096, 256 << 20}, {int64(c.MaxPendingBytes), 4096, 256 << 20},
		{int64(c.MaxModuleBytes), 8, 32 << 20}, {int64(c.MaxCacheBytes), 8, 256 << 20}, {int64(c.MaxModules), 1, 32},
		{int64(c.MaxInputBytes), 1, 16 << 20}, {int64(c.MaxOutputBytes), 1, 16 << 20}, {int64(c.MaxStateBytes), 1, 1 << 20},
		{int64(c.MemoryPages), 64, 1024}, {int64(c.TableElements), 1, 65536},
		{int64(c.StartupMS), 1, 30000}, {int64(c.CompileMS), 1, 30000}, {int64(c.CallMS), 1, 30000}, {int64(c.WriteMS), 1, 30000},
		{int64(c.MaxTotalRSSBytes), 32 << 20, 8 << 30}, {int64(c.MaxWorkerRSSBytes), 32 << 20, 4 << 30}, {c.GoMemoryBytes, 16 << 20, 2 << 30},
		{int64(c.MaxFrameBytes), 4096, 48 << 20},
	}
	for _, b := range bounds {
		if b[0] < b[1] || b[0] > b[2] {
			return code("CONFIG_INVALID")
		}
	}
	if c.MaxCacheBytes < c.MaxModuleBytes || c.MaxTotalRSSBytes < c.MaxWorkerRSSBytes {
		return code("CONFIG_INVALID")
	}
	sizes := []int{c.MaxModuleBytes, c.MaxInputBytes + c.MaxStateBytes, c.MaxOutputBytes + c.MaxStateBytes}
	for _, size := range sizes {
		if c.MaxFrameBytes < (size+2)/3*4+4096 {
			return code("CONFIG_INVALID")
		}
	}
	return nil
}

type Request struct {
	Version   int    `json:"version"`
	ID        string `json:"id"`
	Op        string `json:"op"`
	Hash      string `json:"hash,omitempty"`
	Module    []byte `json:"module,omitempty"`
	Function  string `json:"function,omitempty"`
	Input     []byte `json:"input,omitempty"`
	State     []byte `json:"state,omitempty"`
	Target    string `json:"target,omitempty"`
	TimeoutMS int    `json:"timeoutMs,omitempty"`
}

type Response struct {
	Version int    `json:"version"`
	ID      string `json:"id"`
	Event   string `json:"event,omitempty"`
	Error   string `json:"error,omitempty"`
	Output  []byte `json:"output,omitempty"`
	State   []byte `json:"state,omitempty"`
	Stats   *Stats `json:"stats,omitempty"`
}

type Stats struct {
	PendingBytes        int    `json:"pendingBytes"`
	BufferedOutputBytes int    `json:"bufferedOutputBytes"`
	RSSBytes            uint64 `json:"rssBytes"`
	WorkerRSSBytes      uint64 `json:"workerRssBytes"`
	PeakTotalRSSBytes   uint64 `json:"peakTotalRssBytes"`
	Pending             int    `json:"pending"`
	Active              int    `json:"active"`
	Modules             int    `json:"modules"`
	CacheBytes          int    `json:"cacheBytes"`
	Workers             []int  `json:"workers"`
	HeapBytes           uint64 `json:"heapBytes"`
	HeapSysBytes        uint64 `json:"heapSysBytes"`
	Goroutines          int    `json:"goroutines"`
	Config              Config `json:"config"`
}

var validID = regexp.MustCompile(`^[A-Za-z0-9_.:-]{1,96}$`)
var validFunction = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]{0,63}$`)
var validHash = regexp.MustCompile(`^[a-f0-9]{64}$`)

func code(s string) error           { return errors.New(s) }
func duration(ms int) time.Duration { return time.Duration(ms) * time.Millisecond }

func decode(data []byte, value any) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if d.Decode(value) != nil {
		return code("FRAME_INVALID")
	}
	if d.Decode(new(any)) != io.EOF {
		return code("FRAME_INVALID")
	}
	return nil
}
