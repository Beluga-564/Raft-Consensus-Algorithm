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

// TestAtMostOneLeaderPerTerm is the safety property. Terms churn freely here
// because nothing maintains leadership yet; what must never happen is two
// nodes claiming the same term.
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
