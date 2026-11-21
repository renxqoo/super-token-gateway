package ledger

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const (
	repStatusOK      uint8 = 1
	repStatusFenced  uint8 = 2
	repStatusCorrupt uint8 = 3
	repStatusError   uint8 = 4
	repReqHdrBytes         = 32
	repRespBytes           = 17
)

var (
	ErrFenced            = errors.New("primary_fenced")
	ErrQuorumUnavailable = errors.New("quorum_unavailable")
)

type ReplicaNode struct {
	ln       net.Listener
	file     *os.File
	syncDisk bool
	mu       sync.Mutex
	epoch    uint64
	lastLSN  uint64
	batches  atomic.Uint64
	bytes    atomic.Uint64
	closed   atomic.Bool
	connMu   sync.Mutex
	conns    map[net.Conn]struct{}
	wg       sync.WaitGroup
}

func StartReplicaNode(listenAddr, walPath string, initialEpoch uint64, syncDisk bool) (*ReplicaNode, error) {
	f, err := os.OpenFile(walPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	r := &ReplicaNode{
		ln:       ln,
		file:     f,
		syncDisk: syncDisk,
		epoch:    initialEpoch,
		conns:    make(map[net.Conn]struct{}),
	}
	r.wg.Add(1)
	go r.acceptLoop()
	return r, nil
}

func (r *ReplicaNode) Addr() string {
	return r.ln.Addr().String()
}

func (r *ReplicaNode) PromoteEpoch(newEpoch uint64) {
	r.mu.Lock()
	if newEpoch > r.epoch {
		r.epoch = newEpoch
	}
	r.mu.Unlock()
}

func (r *ReplicaNode) Stats() (epoch uint64, lastLSN uint64, batches uint64, totalBytes uint64) {
	r.mu.Lock()
	epoch = r.epoch
	lastLSN = r.lastLSN
	r.mu.Unlock()
	return epoch, lastLSN, r.batches.Load(), r.bytes.Load()
}

func (r *ReplicaNode) acceptLoop() {
	defer r.wg.Done()
	for {
		conn, err := r.ln.Accept()
		if err != nil {
			return
		}
		if tc, ok := conn.(*net.TCPConn); ok {
			_ = tc.SetNoDelay(true)
		}
		r.wg.Add(1)
		go r.serveConn(conn)
	}
}

func (r *ReplicaNode) serveConn(conn net.Conn) {
	defer r.wg.Done()
	r.connMu.Lock()
	if r.closed.Load() {
		r.connMu.Unlock()
		_ = conn.Close()
		return
	}
	r.conns[conn] = struct{}{}
	r.connMu.Unlock()
	defer func() {
		r.connMu.Lock()
		delete(r.conns, conn)
		r.connMu.Unlock()
		_ = conn.Close()
	}()
	br := bufio.NewReaderSize(conn, 256*1024)
	var hdr [repReqHdrBytes]byte
	var resp [repRespBytes]byte
	var payload []byte
	for {
		if _, err := io.ReadFull(br, hdr[:]); err != nil {
			return
		}
		epoch := binary.LittleEndian.Uint64(hdr[0:8])
		lastLSN := binary.LittleEndian.Uint64(hdr[16:24])
		length := binary.LittleEndian.Uint32(hdr[24:28])
		expectedCRC := binary.LittleEndian.Uint32(hdr[28:32])
		if length > 32<<20 {
			return
		}
		if cap(payload) < int(length) {
			payload = make([]byte, length)
		} else {
			payload = payload[:length]
		}
		if _, err := io.ReadFull(br, payload); err != nil {
			return
		}
		status := repStatusOK
		if crc32.Checksum(payload, crcTable) != expectedCRC {
			status = repStatusCorrupt
		}
		r.mu.Lock()
		curEpoch := r.epoch
		curLSN := r.lastLSN
		if status == repStatusOK {
			if epoch < r.epoch {
				status = repStatusFenced
			} else {
				if epoch > r.epoch {
					r.epoch = epoch
					curEpoch = epoch
				}
				if _, err := r.file.Write(payload); err != nil {
					status = repStatusError
				} else if r.syncDisk {
					if err := r.file.Sync(); err != nil {
						status = repStatusError
					}
				}
				if status == repStatusOK {
					if lastLSN > r.lastLSN {
						r.lastLSN = lastLSN
					}
					curLSN = r.lastLSN
					r.batches.Add(1)
					r.bytes.Add(uint64(len(payload)))
				}
			}
		}
		r.mu.Unlock()
		resp[0] = status
		binary.LittleEndian.PutUint64(resp[1:9], curEpoch)
		binary.LittleEndian.PutUint64(resp[9:17], curLSN)
		if _, err := conn.Write(resp[:]); err != nil {
			return
		}
	}
}

func (r *ReplicaNode) Close() error {
	if !r.closed.CompareAndSwap(false, true) {
		return nil
	}
	_ = r.ln.Close()
	r.connMu.Lock()
	for conn := range r.conns {
		_ = conn.Close()
	}
	r.connMu.Unlock()
	r.wg.Wait()
	return r.file.Close()
}

type replicaPeer struct {
	addr         string
	mu           sync.Mutex
	conn         net.Conn
	retryAfterNs int64
}

type QuorumReplicator struct {
	peers        []*replicaPeer
	requiredAcks int
	timeout      time.Duration
	fenced       atomic.Bool
}

func NewQuorumReplicator(addrs []string, requiredAcks int, timeout time.Duration) *QuorumReplicator {
	if timeout <= 0 {
		timeout = 500 * time.Millisecond
	}
	peers := make([]*replicaPeer, len(addrs))
	for i, addr := range addrs {
		peers[i] = &replicaPeer{addr: addr}
	}
	return &QuorumReplicator{
		peers:        peers,
		requiredAcks: requiredAcks,
		timeout:      timeout,
	}
}

func (q *QuorumReplicator) Replicate(epoch uint64, firstLSN uint64, lastLSN uint64, frameBytes []byte) error {
	if q.fenced.Load() {
		return ErrFenced
	}
	if q.requiredAcks <= 0 {
		return nil
	}
	if len(q.peers) < q.requiredAcks {
		return ErrQuorumUnavailable
	}
	var hdr [repReqHdrBytes]byte
	binary.LittleEndian.PutUint64(hdr[0:8], epoch)
	binary.LittleEndian.PutUint64(hdr[8:16], firstLSN)
	binary.LittleEndian.PutUint64(hdr[16:24], lastLSN)
	binary.LittleEndian.PutUint32(hdr[24:28], uint32(len(frameBytes)))
	binary.LittleEndian.PutUint32(hdr[28:32], crc32.Checksum(frameBytes, crcTable))

	results := make(chan uint8, len(q.peers))
	for _, peer := range q.peers {
		go func(p *replicaPeer) {
			results <- q.sendPeer(p, hdr, frameBytes)
		}(peer)
	}
	acks := 0
	for i := 0; i < len(q.peers); i++ {
		st := <-results
		if st == repStatusFenced {
			q.fenced.Store(true)
			return ErrFenced
		}
		if st == repStatusOK {
			acks++
			if acks >= q.requiredAcks {
				return nil
			}
		}
	}
	return ErrQuorumUnavailable
}

func (q *QuorumReplicator) sendPeer(p *replicaPeer, hdr [repReqHdrBytes]byte, frameBytes []byte) uint8 {
	p.mu.Lock()
	defer p.mu.Unlock()
	nowNs := time.Now().UnixNano()
	deadline := time.Now().Add(q.timeout)
	if p.conn == nil {
		if nowNs < p.retryAfterNs {
			return repStatusError
		}
		conn, err := net.DialTimeout("tcp", p.addr, q.timeout)
		if err != nil {
			p.retryAfterNs = time.Now().Add(200 * time.Millisecond).UnixNano()
			return repStatusError
		}
		if tc, ok := conn.(*net.TCPConn); ok {
			_ = tc.SetNoDelay(true)
		}
		p.conn = conn
		p.retryAfterNs = 0
	}
	_ = p.conn.SetDeadline(deadline)
	bufs := net.Buffers{hdr[:], frameBytes}
	if _, err := bufs.WriteTo(p.conn); err != nil {
		_ = p.conn.Close()
		p.conn = nil
		p.retryAfterNs = time.Now().Add(200 * time.Millisecond).UnixNano()
		return repStatusError
	}
	var resp [repRespBytes]byte
	if _, err := io.ReadFull(p.conn, resp[:]); err != nil {
		_ = p.conn.Close()
		p.conn = nil
		p.retryAfterNs = time.Now().Add(200 * time.Millisecond).UnixNano()
		return repStatusError
	}
	return resp[0]
}

func (q *QuorumReplicator) Close() {
	for _, p := range q.peers {
		p.mu.Lock()
		if p.conn != nil {
			_ = p.conn.Close()
			p.conn = nil
		}
		p.mu.Unlock()
	}
}
