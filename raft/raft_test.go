package raft

import (
	"bytes"
	"log"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// syncBuffer is an io.Writer safe to read while background goroutines log.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// captureLogs redirects the standard logger into a buffer for the duration of
// a test, stripping timestamps so assertions can be exact.
func captureLogs(t *testing.T) *syncBuffer {
	t.Helper()
	var out syncBuffer
	flags := log.Flags()
	log.SetOutput(&out)
	log.SetFlags(0)
	t.Cleanup(func() {
		log.SetOutput(os.Stderr)
		log.SetFlags(flags)
	})
	return &out
}

// ---------- Task 1 ----------

// TestStartStop is Task 1's gate: three nodes come up as followers in term 0
// and shut down cleanly.
func TestStartStop(t *testing.T) {
	const n = 3

	nodes := make([]*Raft, n)
	for i := range nodes {
		nodes[i] = Make(i, n, nopTransport{})
	}

	for i, r := range nodes {
		if r.me != i {
			t.Errorf("node %d: me = %d, want %d", i, r.me, i)
		}
		if r.role != Follower {
			t.Errorf("node %d: role = %v, want follower", i, r.role)
		}
		if r.currentTerm != 0 {
			t.Errorf("node %d: currentTerm = %d, want 0", i, r.currentTerm)
		}
		if r.votedFor != None {
			t.Errorf("node %d: votedFor = %d, want None (%d)", i, r.votedFor, None)
		}
		if r.Killed() {
			t.Errorf("node %d: Killed() = true before Kill", i)
		}
	}

	for _, r := range nodes {
		r.Kill()
	}
	for i, r := range nodes {
		if !r.Killed() {
			t.Errorf("node %d: Killed() = false after Kill", i)
		}
	}
}

// Kill must be safe from many goroutines at once, and idempotent -- every
// ticker and replication goroutine from Task 2 onward races on it.
func TestKillIsConcurrentSafeAndIdempotent(t *testing.T) {
	r := Make(0, 3, nopTransport{})

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.Kill()
			if !r.Killed() {
				t.Error("Killed() = false immediately after Kill")
			}
		}()
	}
	wg.Wait()

	if !r.Killed() {
		t.Error("Killed() = false after 50 concurrent Kills")
	}
}

// Majority decides every election and every commit, so pin the arithmetic.
func TestMajority(t *testing.T) {
	for _, tc := range []struct{ numPeers, want int }{
		{1, 1}, {2, 2}, {3, 2}, {4, 3}, {5, 3}, {6, 4}, {7, 4},
	} {
		r := Make(0, tc.numPeers, nopTransport{})
		if got := r.Majority(); got != tc.want {
			t.Errorf("numPeers=%d: Majority() = %d, want %d", tc.numPeers, got, tc.want)
		}
	}
}

func TestMakeRejectsBadIDs(t *testing.T) {
	for _, tc := range []struct{ me, numPeers int }{
		{0, 0}, {0, -1}, {-1, 3}, {3, 3}, {4, 3},
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("Make(%d, %d): want panic, got none", tc.me, tc.numPeers)
				}
			}()
			Make(tc.me, tc.numPeers, nopTransport{})
		}()
	}
}

func TestDlogRespectsDebugFlag(t *testing.T) {
	out := captureLogs(t)
	r := Make(1, 3, nopTransport{})

	r.debug = false
	r.dlog("this must not appear %d", 1)
	if out.String() != "" {
		t.Errorf("debug off: wrote %q, want nothing", out.String())
	}

	r.debug = true
	r.currentTerm, r.role = 7, Candidate
	r.dlog("got vote from %d, tally %d/%d", 2, 2, 3)

	want := "[node 1 term 7 candidate] got vote from 2, tally 2/3\n"
	if got := out.String(); got != want {
		t.Errorf("debug on:\n got %q\nwant %q", got, want)
	}
}

// ---------- Task 2 ----------

// TestElectionTimeoutIsRandomized is Task 2's gate. A fixed timeout causes
// infinite split votes, so this asserts both the range and the spread.
func TestElectionTimeoutIsRandomized(t *testing.T) {
	const draws = 50

	seen := make(map[time.Duration]bool, draws)
	for i := 0; i < draws; i++ {
		d := randomElectionTimeout()
		if d < ElectionTimeoutMin || d >= ElectionTimeoutMax {
			t.Errorf("draw %d: %v outside [%v, %v)", i, d, ElectionTimeoutMin, ElectionTimeoutMax)
		}
		seen[d] = true
	}

	if len(seen) < 20 {
		t.Errorf("got %d distinct timeouts in %d draws, want >= 20 "+
			"(is the timeout fixed, or drawn only once?)", len(seen), draws)
	}
}

// resetElectionDeadline must push the deadline into the future -- a node whose
// deadline stays in the past would log a timeout on every single tick.
func TestResetElectionDeadlineArmsTheFuture(t *testing.T) {
	r := Make(0, 3, nopTransport{})

	for i := 0; i < 20; i++ {
		before := time.Now()
		r.resetElectionDeadline()

		if !r.electionDeadline.After(before) {
			t.Fatalf("iteration %d: deadline %v is not after %v", i, r.electionDeadline, before)
		}
		if d := r.electionDeadline.Sub(before); d > ElectionTimeoutMax {
			t.Errorf("iteration %d: deadline is %v out, beyond max %v", i, d, ElectionTimeoutMax)
		}
	}
}

// A node left alone for 2s must notice between 3 and 6 timeouts: 2000ms of
// draws from [300, 600) admits at most 6 (6*300 = 1800) and at least 3
// (3*600 = 1800, with the 4th landing at 2400).
func TestTickerNoticesTimeouts(t *testing.T) {
	out := captureLogs(t)

	r := Make(0, 3, nopTransport{})
	r.debug = true
	r.resetElectionDeadline() // discount setup time
	r.Start()

	time.Sleep(2 * time.Second)
	r.Kill()

	got := strings.Count(out.String(), "election timeout")
	if got < 3 || got > 6 {
		t.Errorf("in 2s got %d election timeouts, want 3-6\nlog:\n%s", got, out.String())
	}
}

// A leader must not time out on itself.
func TestTickerIgnoresDeadlineWhenLeader(t *testing.T) {
	out := captureLogs(t)

	r := Make(0, 3, nopTransport{})
	r.debug = true
	r.role = Leader
	r.Start()
	defer r.Kill()

	time.Sleep(ElectionTimeoutMax + 20*TickInterval)

	if n := strings.Count(out.String(), "election timeout"); n != 0 {
		t.Errorf("leader logged %d election timeouts, want 0\nlog:\n%s", n, out.String())
	}
}

// settledGoroutines waits for the goroutine count to stop moving, so that
// tickers still winding down from earlier tests are not counted in a
// baseline. Without this, the count comparison below is flaky.
func settledGoroutines() int {
	prev := -1
	for i := 0; i < 200; i++ {
		n := runtime.NumGoroutine()
		if n == prev {
			return n
		}
		prev = n
		time.Sleep(TickInterval)
	}
	return runtime.NumGoroutine()
}

func waitFor(d time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(TickInterval)
	}
	return cond()
}

// The ticker goroutine must actually exit after Kill, or every cluster
// teardown leaks one goroutine per node. Checked two ways: it must stop
// doing work, and it must stop existing.
func TestTickerExitsAfterKill(t *testing.T) {
	out := captureLogs(t)
	base := settledGoroutines()

	r := Make(0, 3, nopTransport{})
	r.debug = true
	r.Start()

	// Behavioural proof the ticker is alive before we kill it.
	if !waitFor(4*ElectionTimeoutMax, func() bool {
		return strings.Contains(out.String(), "election timeout")
	}) {
		t.Fatal("ticker never logged an election timeout; is it running?")
	}

	r.Kill()

	// It must stop doing work.
	settled := strings.Count(out.String(), "election timeout")
	time.Sleep(2 * ElectionTimeoutMax)
	if got := strings.Count(out.String(), "election timeout"); got != settled {
		t.Errorf("ticker logged %d more timeouts after Kill", got-settled)
	}

	// And it must stop existing.
	if !waitFor(4*time.Second, func() bool { return runtime.NumGoroutine() <= base }) {
		t.Errorf("goroutine still running after Kill (%d, base %d)",
			runtime.NumGoroutine(), base)
	}
}

// nopTransport lets white-box tests drive the ticker without a cluster:
// every RPC reports itself dropped.
type nopTransport struct{}

func (nopTransport) SendRequestVote(int, *RequestVoteArgs, *RequestVoteReply) bool { return false }
