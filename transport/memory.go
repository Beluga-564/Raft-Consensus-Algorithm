package transport

import (
	"math/rand"
	"sync"
	"time"

	"github.com/pankaj/raft-go/raft"
)

const (
	minDelay = 1 * time.Millisecond
	maxDelay = 10 * time.Millisecond
)

// MemoryTransport is an in-process stand-in for the network. A partition is a
// flipped boolean rather than a socket, which is what makes it testable.
type MemoryTransport struct {
	mu        sync.Mutex
	handlers  []raft.Handler
	connected []bool
}

func NewMemoryTransport(numPeers int) *MemoryTransport {
	connected := make([]bool, numPeers)
	for i := range connected {
		connected[i] = true
	}
	return &MemoryTransport{
		handlers:  make([]raft.Handler, numPeers),
		connected: connected,
	}
}

// Register wires node id's handler in. Nodes are unreachable until registered.
func (t *MemoryTransport) Register(id int, h raft.Handler) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.handlers[id] = h
}

func (t *MemoryTransport) Connect(id int)    { t.setConnected(id, true) }
func (t *MemoryTransport) Disconnect(id int) { t.setConnected(id, false) }

func (t *MemoryTransport) setConnected(id int, up bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.connected[id] = up
}

// Node returns the Transport that node id sends through.
func (t *MemoryTransport) Node(id int) raft.Transport {
	return &memoryPeer{net: t, id: id}
}

// deliver returns the handler to call, or nil if either end is down.
// The lock is never held across the handler call: that would invert the
// transport/raft lock order and deadlock.
func (t *MemoryTransport) deliver(from, to int) raft.Handler {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.connected[from] || !t.connected[to] {
		return nil
	}
	return t.handlers[to]
}

func (t *MemoryTransport) reachable(from, to int) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.connected[from] && t.connected[to]
}

type memoryPeer struct {
	net *MemoryTransport
	id  int
}

func (p *memoryPeer) SendRequestVote(peer int, args *raft.RequestVoteArgs, reply *raft.RequestVoteReply) bool {
	time.Sleep(minDelay + time.Duration(rand.Int63n(int64(maxDelay-minDelay))))

	h := p.net.deliver(p.id, peer)
	if h == nil {
		return false
	}
	if err := h.RequestVote(args, reply); err != nil {
		return false
	}
	// A partition can form while the call is in flight; the reply is then lost.
	return p.net.reachable(p.id, peer)
}
