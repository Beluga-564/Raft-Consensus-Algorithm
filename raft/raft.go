package raft

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"
)

const (
	TickInterval       = 10 * time.Millisecond // how often we check the deadline
	ElectionTimeoutMin = 300 * time.Millisecond
	ElectionTimeoutMax = 600 * time.Millisecond
)

func Make(me, numPeers int, transport Transport) *Raft {
	if numPeers <= 0 {
		panic("raft: numPeers must be positive")
	}
	if me < 0 || me >= numPeers {
		panic("raft: me must be in [0, numPeers)")
	}

	// mu and dead are usable at their zero values.
	entries := make([]LogEntry, 1)
	entries[0] = LogEntry{
		Term:  0,
		Index: 0,
	}

	r := &Raft{
		me:       me,
		numPeers: numPeers,

		currentTerm: 0,
		votedFor:    None,
		role:        Follower,
		leaderID:    None,

		debug:     os.Getenv("RAFT_DEBUG") != "",
		log:       entries,
		transport: transport,
	}

	r.resetElectionDeadline()

	r.dlog("started in a %d-node cluster, quorum %d", numPeers, r.Majority())
	return r
}

func (r *Raft) Start() {
	go r.ticker()
}

func (r *Raft) Kill() {
	r.dead.Store(true)
}

func (r *Raft) Killed() bool {
	return r.dead.Load()
}

func (r *Raft) Role() Role {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.role
}

func (r *Raft) Term() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.currentTerm
}

func (r *Raft) LeaderID() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.leaderID
}

func (r *Raft) Majority() int {
	return r.numPeers/2 + 1
}

func (r *Raft) ticker() {
	ticker := time.NewTicker(TickInterval)
	defer ticker.Stop()

	for range ticker.C {
		if r.Killed() {
			return
		}

		r.mu.Lock()
		if r.role != Leader && time.Now().After(r.electionDeadline) {
			r.dlog("election timeout")
			r.startElection()
		}
		r.mu.Unlock()
	}
}

func randomElectionTimeout() time.Duration {
	spread := ElectionTimeoutMax - ElectionTimeoutMin
	return ElectionTimeoutMin + time.Duration(rand.Int63n(int64(spread)))
}

func (r *Raft) resetElectionDeadline() {
	r.electionDeadline = time.Now().Add(randomElectionTimeout())
}

func (r *Raft) dlog(format string, args ...any) {
	if !r.debug {
		return
	}
	prefix := fmt.Sprintf("[node %d term %d %s] ", r.me, r.currentTerm, r.role)
	log.Printf(prefix+format, args...)
}
