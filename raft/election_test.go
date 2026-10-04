package raft_test

import (
	"testing"
	"time"

	"github.com/pankaj/raft-go/raft"
	"github.com/pankaj/raft-go/transport"
)

// cluster spins up n connected nodes and kills them at the end of the test.
func cluster(t *testing.T, n int) ([]*raft.Raft, *transport.MemoryTransport) {
	t.Helper()

	net := transport.NewMemoryTransport(n)
	nodes := make([]*raft.Raft, n)
	for i := range nodes {
		nodes[i] = raft.Make(i, n, net.Node(i))
		net.Register(i, nodes[i])
	}
	for _, r := range nodes {
		r.Start()
	}
	t.Cleanup(func() {
		for _, r := range nodes {
			r.Kill()
		}
	})
	return nodes, net
}

func leaders(nodes []*raft.Raft) []int {
	var found []int
	for i, r := range nodes {
		if r.Role() == raft.Leader {
			found = append(found, i)
		}
	}
	return found
}

func TestElectsALeader(t *testing.T) {
	nodes, _ := cluster(t, 3)

	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if len(leaders(nodes)) > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no leader elected within 1s")
}

// TestAtMostOneLeaderPerTerm is the safety property: whatever happens to
// terms, two nodes must never claim leadership of the same one.
func TestAtMostOneLeaderPerTerm(t *testing.T) {
	nodes, _ := cluster(t, 3)

	seen := make(map[int]int) // term -> node that led it
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for i, r := range nodes {
			if r.Role() != raft.Leader {
				continue
			}
			term := r.Term()
			if prev, ok := seen[term]; ok && prev != i {
				t.Fatalf("two leaders in term %d: nodes %d and %d", term, prev, i)
			}
			seen[term] = i
		}
		time.Sleep(5 * time.Millisecond)
	}

	if len(seen) == 0 {
		t.Fatal("no leader elected in 3s; the test proved nothing")
	}
	t.Logf("observed %d leader terms", len(seen))
}

func TestNoLeaderWithoutQuorum(t *testing.T) {
	nodes, net := cluster(t, 3)
	net.Disconnect(1)
	net.Disconnect(2)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got := leaders(nodes); len(got) > 0 {
			t.Fatalf("node(s) %v became leader without a quorum", got)
		}
		time.Sleep(10 * time.Millisecond)
	}

	if nodes[0].Term() <= 1 {
		t.Errorf("survivor should keep timing out and raising its term, got %d", nodes[0].Term())
	}
}

// waitForLeader polls until exactly one of the given nodes reports Leader,
// and returns its index. Raft is asynchronous: asserting instantly is flaky.
func waitForLeader(t *testing.T, nodes []*raft.Raft, among []int, within time.Duration) int {
	t.Helper()

	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		var found []int
		for _, i := range among {
			if nodes[i].Role() == raft.Leader {
				found = append(found, i)
			}
		}
		if len(found) == 1 {
			return found[0]
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no single leader among nodes %v within %v", among, within)
	return -1
}

func all(nodes []*raft.Raft) []int {
	ids := make([]int, len(nodes))
	for i := range ids {
		ids[i] = i
	}
	return ids
}

func without(ids []int, drop int) []int {
	var kept []int
	for _, i := range ids {
		if i != drop {
			kept = append(kept, i)
		}
	}
	return kept
}

// TestStableLeadership is the test Task 3 could not pass: heartbeats must
// stop followers from timing out, so leader and term stay put.
func TestStableLeadership(t *testing.T) {
	nodes, _ := cluster(t, 3)

	leader := waitForLeader(t, nodes, all(nodes), 1*time.Second)
	term := nodes[leader].Term()

	time.Sleep(2 * time.Second)

	if got := leaders(nodes); len(got) != 1 || got[0] != leader {
		t.Fatalf("leaders = %v after 2s, want only node %d", got, leader)
	}
	if got := nodes[leader].Term(); got != term {
		t.Fatalf("term moved from %d to %d under a healthy leader", term, got)
	}
	for i, r := range nodes {
		if got := r.LeaderID(); got != leader {
			t.Errorf("node %d thinks the leader is %d, want %d", i, got, leader)
		}
	}
}

func TestReElectionAfterLeaderFailure(t *testing.T) {
	nodes, net := cluster(t, 3)

	old := waitForLeader(t, nodes, all(nodes), 1*time.Second)
	oldTerm := nodes[old].Term()

	net.Disconnect(old)

	// The isolated old leader still believes it leads -- it has no way to
	// learn otherwise -- so only look for a leader among the connected nodes.
	survivors := without(all(nodes), old)
	newLeader := waitForLeader(t, nodes, survivors, 1*time.Second)

	if got := nodes[newLeader].Term(); got <= oldTerm {
		t.Fatalf("new leader %d has term %d, want > %d", newLeader, got, oldTerm)
	}
}

func TestOldLeaderStepsDown(t *testing.T) {
	nodes, net := cluster(t, 3)

	old := waitForLeader(t, nodes, all(nodes), 1*time.Second)
	net.Disconnect(old)
	newLeader := waitForLeader(t, nodes, without(all(nodes), old), 1*time.Second)
	newTerm := nodes[newLeader].Term()

	net.Connect(old)

	// One heartbeat in either direction carries the higher term to the old
	// leader; give it a few rounds.
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if nodes[old].Role() == raft.Follower && nodes[old].Term() >= newTerm {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if got := nodes[old].Role(); got != raft.Follower {
		t.Fatalf("old leader %d is %s after reconnecting, want follower", old, got)
	}
	if got := nodes[old].Term(); got < newTerm {
		t.Fatalf("old leader %d stuck at term %d, want >= %d", old, got, newTerm)
	}
	if got := leaders(nodes); len(got) != 1 {
		t.Fatalf("leaders = %v after healing, want exactly one", got)
	}
}
