package ledger

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const (
	walMagic       uint32 = 0x53544757
	walHeaderBytes        = 16
	walVersion     uint8  = 1
)

const (
	RecWalletCredit   uint8 = 1
	RecSupplierCredit uint8 = 2
	RecGrant          uint8 = 3
	RecSettle         uint8 = 4
	RecAbort          uint8 = 5
	RecAck            uint8 = 6
)

var (
	crcTable      = crc32.MakeTable(crc32.Castagnoli)
	ErrWALClosed  = errors.New("wal_closed")
	ErrWALCorrupt = errors.New("wal_corrupt")
)

type Replicator interface {
	Replicate(epoch uint64, firstLSN uint64, lastLSN uint64, frameBytes []byte) error
}

type WALRecord struct {
	Type    uint8
	LSN     uint64
	Payload []byte
}

type walSubmission struct {
	recType uint8
	lsn     uint64
	payload []byte
	done    chan error
}

type WALStats struct {
	WrittenBytes uint64
	WrittenRecs  uint64
	SyncBatches  uint64
	MaxBatchRecs uint64
}

type WAL struct {
	path        string
	file        *os.File
	epoch       uint64
	syncDisk    bool
	maxBatch    int
	batchWait   time.Duration
	replicator  Replicator
	submit      chan *walSubmission
	stop        chan struct{}
	stopped     chan struct{}
	closeOnce   sync.Once
	closed      atomic.Bool
	writtenByte atomic.Uint64
	writtenRec  atomic.Uint64
	syncBatches atomic.Uint64
	maxBatchRec atomic.Uint64
	subPool     sync.Pool
}

type WALOptions struct {
	Path       string
	Epoch      uint64
	SyncDisk   bool
	MaxBatch   int
	BatchWait  time.Duration
	QueueDepth int
	Replicator Replicator
}

func OpenWAL(opts WALOptions) (*WAL, []WALRecord, error) {
	if opts.MaxBatch <= 0 {
		opts.MaxBatch = 4096
	}
	if opts.QueueDepth <= 0 {
		opts.QueueDepth = 65536
	}
	if opts.Epoch == 0 {
		opts.Epoch = 1
	}
	f, err := os.OpenFile(opts.Path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, nil, err
	}
	recs, validEnd, err := recoverWALFile(f)
	if err != nil {
		_ = f.Close()
		return nil, nil, err
	}
	if err := f.Truncate(validEnd); err != nil {
		_ = f.Close()
		return nil, nil, err
	}
	if _, err := f.Seek(validEnd, io.SeekStart); err != nil {
		_ = f.Close()
		return nil, nil, err
	}
	w := &WAL{
		path:       opts.Path,
		file:       f,
		epoch:      opts.Epoch,
		syncDisk:   opts.SyncDisk,
		maxBatch:   opts.MaxBatch,
		batchWait:  opts.BatchWait,
		replicator: opts.Replicator,
		submit:     make(chan *walSubmission, opts.QueueDepth),
		stop:       make(chan struct{}),
		stopped:    make(chan struct{}),
	}
	w.subPool.New = func() any {
		return &walSubmission{done: make(chan error, 1)}
	}
	go w.loop()
	return w, recs, nil
}

func EncodeFrame(dst []byte, recType uint8, payload []byte) []byte {
	start := len(dst)
	need := walHeaderBytes + len(payload)
	if cap(dst)-start < need {
		grown := make([]byte, start, max(cap(dst)*2, start+need))
		copy(grown, dst)
		dst = grown
	}
	dst = dst[:start+need]
	binary.LittleEndian.PutUint32(dst[start:start+4], walMagic)
	dst[start+4] = walVersion
	dst[start+5] = recType
	binary.LittleEndian.PutUint16(dst[start+6:start+8], 0)
	binary.LittleEndian.PutUint32(dst[start+8:start+12], uint32(len(payload)))
	copy(dst[start+walHeaderBytes:], payload)
	sum := crc32.Checksum(dst[start+walHeaderBytes:], crcTable)
	binary.LittleEndian.PutUint32(dst[start+12:start+16], sum)
	return dst
}

func recoverWALFile(f *os.File) ([]WALRecord, int64, error) {
	info, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	size := info.Size()
	if size == 0 {
		return nil, 0, nil
	}
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, 0, err
	}
	var records []WALRecord
	var offset int64
	for offset+walHeaderBytes <= int64(len(data)) {
		hdr := data[offset : offset+walHeaderBytes]
		magic := binary.LittleEndian.Uint32(hdr[0:4])
		if magic != walMagic || hdr[4] != walVersion {
			break
		}
		recType := hdr[5]
		plen := int64(binary.LittleEndian.Uint32(hdr[8:12]))
		expectedCRC := binary.LittleEndian.Uint32(hdr[12:16])
		if plen < 0 || plen > 1<<20 || offset+walHeaderBytes+plen > int64(len(data)) {
			break
		}
		payload := data[offset+walHeaderBytes : offset+walHeaderBytes+plen]
		if crc32.Checksum(payload, crcTable) != expectedCRC {
			break
		}
		copied := make([]byte, plen)
		copy(copied, payload)
		var lsn uint64
		if len(copied) >= 8 {
			lsn = binary.LittleEndian.Uint64(copied[0:8])
		}
		records = append(records, WALRecord{Type: recType, LSN: lsn, Payload: copied})
		offset += walHeaderBytes + plen
	}
	return records, offset, nil
}

func (w *WAL) Append(recType uint8, lsn uint64, payload []byte) error {
	if w.closed.Load() {
		return ErrWALClosed
	}
	sub := w.subPool.Get().(*walSubmission)
	sub.recType = recType
	sub.lsn = lsn
	sub.payload = payload
	select {
	case w.submit <- sub:
	case <-w.stop:
		w.subPool.Put(sub)
		return ErrWALClosed
	}
	err := <-sub.done
	sub.payload = nil
	w.subPool.Put(sub)
	return err
}

func (w *WAL) loop() {
	defer close(w.stopped)
	batch := make([]*walSubmission, 0, w.maxBatch)
	buf := make([]byte, 0, 256*1024)
	var timer *time.Timer
	for {
		batch = batch[:0]
		select {
		case first := <-w.submit:
			batch = append(batch, first)
		case <-w.stop:
			w.drainClosed()
			return
		}
		for len(batch) < w.maxBatch {
			select {
			case next := <-w.submit:
				batch = append(batch, next)
			default:
				goto maybeWait
			}
		}
	maybeWait:
		if w.batchWait > 0 && len(batch) < w.maxBatch {
			if timer == nil {
				timer = time.NewTimer(w.batchWait)
			} else {
				timer.Reset(w.batchWait)
			}
			waiting := true
			for waiting && len(batch) < w.maxBatch {
				select {
				case next := <-w.submit:
					batch = append(batch, next)
				case <-timer.C:
					waiting = false
				case <-w.stop:
					waiting = false
				}
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}
		buf = buf[:0]
		firstLSN := batch[0].lsn
		lastLSN := batch[0].lsn
		for _, sub := range batch {
			if sub.lsn < firstLSN {
				firstLSN = sub.lsn
			}
			if sub.lsn > lastLSN {
				lastLSN = sub.lsn
			}
			buf = EncodeFrame(buf, sub.recType, sub.payload)
		}
		var localErr, repErr error
		if w.replicator != nil {
			repPayload := make([]byte, len(buf))
			copy(repPayload, buf)
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				repErr = w.replicator.Replicate(w.epoch, firstLSN, lastLSN, repPayload)
			}()
			if _, werr := w.file.Write(buf); werr != nil {
				localErr = werr
			} else if w.syncDisk {
				localErr = w.file.Sync()
			}
			wg.Wait()
		} else {
			if _, werr := w.file.Write(buf); werr != nil {
				localErr = werr
			} else if w.syncDisk {
				localErr = w.file.Sync()
			}
		}
		err := localErr
		if err == nil {
			err = repErr
		}
		if err == nil {
			w.writtenByte.Add(uint64(len(buf)))
			w.writtenRec.Add(uint64(len(batch)))
			w.syncBatches.Add(1)
			n := uint64(len(batch))
			for {
				prev := w.maxBatchRec.Load()
				if n <= prev || w.maxBatchRec.CompareAndSwap(prev, n) {
					break
				}
			}
		}
		for _, sub := range batch {
			sub.done <- err
		}
	}
}

func (w *WAL) drainClosed() {
	for {
		select {
		case sub := <-w.submit:
			sub.done <- ErrWALClosed
		default:
			return
		}
	}
}

func (w *WAL) Stats() WALStats {
	return WALStats{
		WrittenBytes: w.writtenByte.Load(),
		WrittenRecs:  w.writtenRec.Load(),
		SyncBatches:  w.syncBatches.Load(),
		MaxBatchRecs: w.maxBatchRec.Load(),
	}
}

func (w *WAL) Close() error {
	var err error
	w.closeOnce.Do(func() {
		w.closed.Store(true)
		close(w.stop)
		<-w.stopped
		err = w.file.Close()
	})
	return err
}
