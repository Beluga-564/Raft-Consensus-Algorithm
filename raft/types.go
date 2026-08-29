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

type LogEntry struct {
	Term  int
	Index int
}

type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type Transport interface {
	SendRequestVote(peer int, args *RequestVoteArgs, reply *RequestVoteReply) bool
}

type Handler interface {
	RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error
}

type Raft struct {
	mu       sync.Mutex
	me       int // my node ID
	numPeers int // total nodes in the cluster, including me

	currentTerm int
	votedFor    int // None if no vote cast this term
	role        Role
	leaderID    int // None until a leader is known

	// electionDeadline is when this node gives up on the current leader.
	// Re-randomized on every reset -- see resetElectionDeadline.
	electionDeadline time.Time

	dead  atomic.Bool
	debug bool

	log       []LogEntry // 1-indexed; log[0] is a {Term: 0} sentinel
	transport Transport
}
