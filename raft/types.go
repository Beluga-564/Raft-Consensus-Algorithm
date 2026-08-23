package raft

import (
	"sync"
	"sync/atomic"
	"time"
)

// None means "no vote cast" or "no leader known".
const None = -1

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return "unknown"
}

type Raft struct {
	mu       sync.Mutex
	me       int // my node ID
	numPeers int // total nodes in the cluster, including me

	currentTerm int
	votedFor    int // None if no vote cast this term
	role        Role

	// electionDeadline is when this node gives up on the current leader.
	// Re-randomized on every reset -- see resetElectionDeadline.
	electionDeadline time.Time

	dead  atomic.Bool
	debug bool
}
