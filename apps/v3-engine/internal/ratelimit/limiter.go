package ratelimit

import (
	"hash/fnv"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

const shardCount = 64

type scopeState struct {
	windowStartNs int64
	prevCount     int64
	currCount     int64
	prevTokens    int64
	currTokens    int64
	active        int64
}

type shard struct {
	mu     sync.Mutex
	scopes map[string]*scopeState
}

type channelState struct {
	mu          sync.Mutex
	fails       int
	openUntilNs int64
	probeActive bool
	ewmaLatency float64
}

type Limiter struct {
	shards   [shardCount]shard
	chMu     sync.RWMutex
	channels map[string]*channelState
	rr       atomic.Uint64
	origin   time.Time
}

func New() *Limiter {
	l := &Limiter{
		channels: make(map[string]*channelState, 32),
		origin:   time.Now(),
	}
	for i := range l.shards {
		l.shards[i].scopes = make(map[string]*scopeState, 64)
	}
	return l
}

func (l *Limiter) nowNs() int64 {
	return int64(time.Since(l.origin))
}

func shardIndex(scope string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(scope))
	return h.Sum32() & (shardCount - 1)
}

func (l *Limiter) Enter(scope string, rpm, maxConcurrency int64, tokens, tpm int64) (func(), bool) {
	sh := &l.shards[shardIndex(scope)]
	now := l.nowNs()
	window := int64(time.Minute)

	sh.mu.Lock()
	st := sh.scopes[scope]
	if st == nil {
		st = &scopeState{windowStartNs: now}
		sh.scopes[scope] = st
	}
	elapsed := now - st.windowStartNs
	if elapsed >= 2*window {
		st.prevCount = 0
		st.currCount = 0
		st.prevTokens = 0
		st.currTokens = 0
		st.windowStartNs = now
		elapsed = 0
	} else if elapsed >= window {
		st.prevCount = st.currCount
		st.currCount = 0
		st.prevTokens = st.currTokens
		st.currTokens = 0
		st.windowStartNs += window
		elapsed -= window
	}
	weight := float64(window-elapsed) / float64(window)
	estRPM := int64(math.Ceil(float64(st.prevCount)*weight)) + st.currCount
	estTPM := int64(math.Ceil(float64(st.prevTokens)*weight)) + st.currTokens
	if (rpm > 0 && estRPM >= rpm) || (maxConcurrency > 0 && st.active >= maxConcurrency) || (tpm > 0 && estTPM+tokens > tpm) {
		sh.mu.Unlock()
		return nil, false
	}
	st.currCount++
	st.currTokens += tokens
	st.active++
	sh.mu.Unlock()

	var released atomic.Bool
	return func() {
		if !released.CompareAndSwap(false, true) {
			return
		}
		sh.mu.Lock()
		if st.active > 0 {
			st.active--
		}
		sh.mu.Unlock()
	}, true
}

func (l *Limiter) getChannel(channelID string) *channelState {
	l.chMu.RLock()
	ch := l.channels[channelID]
	l.chMu.RUnlock()
	if ch != nil {
		return ch
	}
	l.chMu.Lock()
	defer l.chMu.Unlock()
	if ch = l.channels[channelID]; ch != nil {
		return ch
	}
	ch = &channelState{ewmaLatency: 50}
	l.channels[channelID] = ch
	return ch
}

func (l *Limiter) AllowChannel(channelID string) (halfOpen bool, ok bool) {
	ch := l.getChannel(channelID)
	now := l.nowNs()
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if ch.openUntilNs == 0 {
		return false, true
	}
	if now < ch.openUntilNs || ch.probeActive {
		return false, false
	}
	ch.probeActive = true
	return true, true
}

func (l *Limiter) ObserveChannel(channelID string, failed bool, latencyMs float64, cooldown time.Duration) {
	ch := l.getChannel(channelID)
	now := l.nowNs()
	ch.mu.Lock()
	defer ch.mu.Unlock()
	ch.probeActive = false
	ch.ewmaLatency = ch.ewmaLatency*0.8 + latencyMs*0.2
	if failed {
		ch.fails++
		if ch.fails >= 3 {
			ch.openUntilNs = now + int64(cooldown)
		}
	} else {
		ch.fails = 0
		ch.openUntilNs = 0
	}
}

func (l *Limiter) ChannelLatency(channelID string) (open bool, latency float64) {
	ch := l.getChannel(channelID)
	now := l.nowNs()
	ch.mu.Lock()
	defer ch.mu.Unlock()
	isOpen := ch.openUntilNs > now || (ch.openUntilNs != 0 && ch.probeActive)
	return isOpen, ch.ewmaLatency
}

func (l *Limiter) NextRoundRobin(n int) int {
	if n <= 1 {
		return 0
	}
	return int((l.rr.Add(1) - 1) % uint64(n))
}
