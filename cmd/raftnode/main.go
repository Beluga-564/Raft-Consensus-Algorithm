package main

import (
	"fmt"
	"log"
	"time"

	"github.com/pankaj/raft-go/raft"
)

const (
	clusterSize = 3
	runFor      = 2 * time.Second
)

func main() {
	// Microsecond timestamps: when diagnosing elections, the relative timing
	// of interleaved node output is usually the thing you need most.
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	nodes := make([]*raft.Raft, clusterSize)
	for i := range nodes {
		nodes[i] = raft.Make(i, clusterSize)
	}
	for _, r := range nodes {
		r.Start()
	}
	fmt.Printf("started %d nodes, running for %v (set RAFT_DEBUG=1 to watch)\n",
		len(nodes), runFor)

	time.Sleep(runFor)

	for _, r := range nodes {
		r.Kill()
	}
	fmt.Println("all nodes shut down")
}
