# Raft in Go — 2-Day Build Plan

**Goal:** a working, testable Raft implementation with a replicated key-value store on top, good enough to defend in an interview.

**Constraints:** ~2 days (~20 focused hours), Go 1.23, learning Go along the way.

**Reference:** the [Raft extended paper](https://raft.github.io/raft.pdf). **Figure 2 (page 4) is the entire spec.** Print it. When something breaks, the bug is almost always a Figure 2 rule you didn't implement literally.

---

## How this plan is organized

Every task is **independently buildable and independently verifiable**. Each one lists:

- **New types** — declared in this task, because this task is the first to use them
- **Changes to existing types** — when a task extends something an earlier task built, it is called out here, never front-loaded
- **Logic** — what to implement
- **Verify** — a concrete test that passes before you move on
- **Gotchas** — only the ones that apply to *this* task

Nothing forward-references a later task. If you stop after any task, what you have compiles and does something demonstrable.

---

## Scope decisions

Raft has ~5 parts. In 20 hours you build three properly and *understand* the other two.

| Feature | Decision | Why |
|---|---|---|
| Leader election | **Build** | The core. Non-negotiable. |
| Log replication | **Build** | The core. Non-negotiable. |
| Safety (commit rules, up-to-date check) | **Build** | Separates "I read a blog" from "I implemented Raft". |
| Persistence (crash recovery) | **Build** | ~1h, and interviewers ask. |
| Log compaction / snapshots | **Skip, document** | 4+ hours. README section instead. |
| Cluster membership changes | **Skip, document** | Joint consensus is a rabbit hole. |
| Linearizable reads (ReadIndex) | **Skip, document** | Mention it; explain why local reads are stale. |

*"I deliberately scoped out snapshots; here's how I'd add them"* is stronger than a half-broken snapshot implementation.

---

## Two architectural decisions

**1. Raft never touches a socket.** It calls a `Transport` interface. You get an in-process implementation for tests (partitions = flipping a boolean) and a TCP one for the demo. Skip this split and you cannot test partitions — and an untested Raft is a broken Raft you don't know is broken.

**2. One mutex per node, guarding all its state.** Do not get clever with fine-grained locking. The only rule: **never hold the lock across an RPC call.**

### File layout

Every file is annotated with the task that creates it. Nothing here is created early — a file appears in the task that first needs it.

```
raft-go/
├── go.mod
├── raft/
│   ├── types.go              # T1  every type and const in the package
│   ├── raft.go               # T1  Make, Run, Kill, ticker, becomeFollower
│   ├── election.go           # T3  RequestVote + candidate logic
│   ├── replication.go        # T4  AppendEntries + leader logic
│   ├── apply.go              # T6  commit advance + applier goroutine
│   ├── persist.go            # T7  durable state
│   ├── raft_test.go          # T1  package raft      -- white-box unit tests
│   └── cluster_test.go       # T10 package raft_test -- multi-node tests
├── transport/
│   ├── memory.go             # T3  in-process, partition-simulating
│   └── tcp.go                # T8  net/rpc
├── kv/
│   ├── store.go              # T9
│   └── server.go             # T9
├── cmd/raftnode/main.go      # T1, grown in T2, T8, T9
├── scripts/demo.sh           # T11
└── README.md                 # T11
```

Between Tasks 3 and 9 the multi-node tests live in per-feature files — `election_test.go`, `replication_test.go`, `apply_test.go`, `persist_test.go` — all in `package raft_test`. Task 10 folds them into `cluster_test.go`.

**Three rules that keep this from drifting:**

1. **Types in `types.go`, behaviour in the feature file.** Almost every task adds fields to `Raft` and types to `types.go`, then puts the logic in the file named for the feature. Skip this and `raft.go` is 900 lines by Task 6.
2. **`raft` imports nothing of yours.** `transport` and `kv` import `raft`; never the reverse. That one-way edge is what lets Task 8 add TCP without touching the algorithm.
3. **Two test packages, deliberately.** `package raft` for tests that read unexported state; `package raft_test` for anything that needs a `MemoryTransport`. Rule 2 is exactly why: `transport` imports `raft`, so an in-package test importing `transport` is a cycle. See Task 3.

---

# DAY 1 — The algorithm

---

## Task 1 — Skeleton, node lifecycle, logging (1h)

**Goal:** three nodes start, announce themselves, and shut down cleanly. No consensus yet.

### Files

- **`raft/types.go`** (new) — `None`, `Role` + its `String()`, the `Raft` struct
- **`raft/raft.go`** (new) — `Make`, `Kill`, `Killed`, `Majority`, `dlog`
- **`raft/raft_test.go`** (new, `package raft`) — `TestStartStop`
- **`cmd/raftnode/main.go`** (new) — construct N nodes, `Kill` them

### New types

```go
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
    case Follower:  return "follower"
    case Candidate: return "candidate"
    case Leader:    return "leader"
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

    dead  atomic.Bool
    debug bool
}
```

`numPeers` rather than a slice of peers: the Raft core needs the cluster *size* (to compute a majority) and its own ID. It has no business knowing about addresses. Iterate peers as `for p := 0; p < rf.numPeers; p++ { if p == rf.me { continue } ... }`.

`dead` is `atomic.Bool`, not `int32` — Go 1.19+ has typed atomics and you're on 1.23.

### Logic

- `Make(me, numPeers int) *Raft` — returns a node in Follower state, term 0, `votedFor = None`
- `Kill()` sets `dead`; `killed()` reads it. Every long-running goroutine you write from here on checks `killed()` and returns.
- `majority() int { return rf.numPeers/2 + 1 }`
- `dlog(format string, args ...any)` — gated on `rf.debug`, prefixed `[node %d term %d %s]` with id, term, role.

### Verify

`TestStartStop`: create 3 nodes, assert each reports `Follower` and term 0, call `Kill()` on all, assert `killed()` is true. Run with `-race`.

### Gotchas

- Build `dlog` now, not later. Your debug output is the only way to debug a distributed system, and `role=2` at 1am is useless — that's what `String()` is for.
- Decide your locking convention now and write it in a comment: **every helper method assumes `rf.mu` is already held by the caller.** Mixing "locks internally" and "caller locks" helpers is the standard route to a self-deadlock that presents as a silent hang.

---

## Task 2 — Election timer (45min)

**Goal:** each node independently notices "I haven't heard from a leader" on a randomized schedule. Still no networking.

### Files

- **`raft/types.go`** (edit) — add `electionDeadline time.Time` to `Raft`
- **`raft/raft.go`** (edit) — the three timing constants, `Run`, `ticker`, `randomElectionTimeout`, `resetElectionDeadline`
- **`raft/raft_test.go`** (edit) — the randomization and ticker tests
- **`cmd/raftnode/main.go`** (edit) — `Run` each node, sleep, then `Kill`

Pulling `randomElectionTimeout()` out as its own function pays for itself immediately: the draw can then be asserted exactly, with no tolerance needed for time spent inside the call.

### New types

```go
const (
    TickInterval       = 10 * time.Millisecond // how often we check the deadline
    ElectionTimeoutMin = 300 * time.Millisecond
    ElectionTimeoutMax = 600 * time.Millisecond
)
```

Keep any future heartbeat interval at least 5× below `ElectionTimeoutMin`. If they get close, followers launch spurious elections against a perfectly healthy leader.

### Changes to existing types

Add to `Raft`:

```go
electionDeadline time.Time
```

### Logic

- `resetElectionDeadline()` — sets `electionDeadline = time.Now().Add(random duration in [Min, Max])`. Pick a **new random value every time**; a fixed timeout causes infinite split votes.
- A ticker goroutine per node: every `TickInterval`, take the lock, and if `role != Leader && time.Now().After(electionDeadline)`, log `"election timeout"` and reset. Exit when `killed()`.

### Verify

`TestElectionTimeoutIsRandomized`: call `resetElectionDeadline()` 50 times on one node, collect the durations, assert they are all within `[Min, Max]` and that you saw at least 20 distinct values. Then run one node for 2s and assert it logged roughly 3–6 timeouts.

### Gotchas

- **Don't use `time.Timer.Reset`.** It is notoriously easy to misuse. A `time.Time` deadline polled by a 10ms ticker is less elegant and dramatically less buggy.
- Compare with `time.Now().After(deadline)`, not by subtracting wall-clock times — you want Go's monotonic clock reading, which `After` uses and arithmetic on parsed times does not.

---

## Task 3 — Leader election (2.5h) ⚠️

**Goal:** a 3-node cluster elects a leader. Leadership is *not* yet maintained — nodes will keep timing out and starting fresh elections, and terms will climb. That is expected and fine, because the property you can already test is the important one: **never two leaders in the same term.**

### Files

- **`raft/types.go`** (edit) — `LogEntry`, `RequestVoteArgs`, `RequestVoteReply`, the `Transport` and `Handler` interfaces; add `log` and `transport` to `Raft`
- **`raft/election.go`** (new) — `startElection`, the `RequestVote` handler, `lastLogIndex`, `lastLogTerm`, `isUpToDate`
- **`raft/raft.go`** (edit) — `becomeFollower`; seed the 1-indexed log in `Make`; point the `ticker` timeout branch at `startElection`
- **`transport/memory.go`** (new) — `MemoryTransport`, `Connect`, `Disconnect`
- **`raft/election_test.go`** (new, **`package raft_test`**) — the three election tests

`becomeFollower` belongs in `raft.go`, not `election.go`: every later task calls it, so it sits with the core lifecycle rather than with candidate logic.

⚠️ **Cluster tests must be `package raft_test`, not `package raft`.** `transport` imports `raft` for the RPC types, so an in-package test that imports `transport` is a cycle — Go rejects it outright with `import cycle not allowed in test`. The external test package (same directory, `package raft_test`) breaks it. The cost is that it sees only *exported* identifiers, which makes this the task to add the accessors Task 9 needs anyway — `Role()`, `Term()`, `LeaderID()`, each taking `mu`. White-box unit tests stay in `package raft`; anything needing a `MemoryTransport` goes external.

### New types

```go
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

// Transport is how a node reaches its peers. The bool return means
// "did I get a response" — false is a dropped or timed-out RPC, which
// is normal operation, not an error.
type Transport interface {
    SendRequestVote(peer int, args *RequestVoteArgs, reply *RequestVoteReply) bool
}

// Handler is what a Transport delivers incoming RPCs to. *Raft satisfies it.
type Handler interface {
    RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error
}
```

All RPC struct fields **must be exported**. Gob (used by `net/rpc`) silently ignores unexported fields — a lowercase field arrives as zero on the other side with no error reported.

Declare the handler returning `error` even though nothing consumes it yet. `net/rpc` only accepts methods that are exported, take two pointer args, and return `error`; matching that shape now costs nothing and saves you writing adapter wrappers later.

`LogEntry` exists because the vote decision depends on log recency. It holds no command yet — nothing is being replicated.

**On `LogEntry.Index`:** it duplicates the slice position, so it's redundant today. Keep it. It lets you assert `rf.log[i].Index == i` and catch truncation bugs the moment they happen, and it's the one field whose absence makes snapshots painful to add later, once the log is trimmed at the front and position stops equalling index.

### Changes to existing types

Add to `Raft`:

```go
log       []LogEntry // 1-indexed; log[0] is a {Term: 0} sentinel
transport Transport
```

Make the log **1-indexed** by seeding it with one `{Term: 0, Index: 0}` entry in `Make`. The paper is 1-indexed, and every off-by-one you'd otherwise fight comes from resisting that.

### Also build: `MemoryTransport`

In `transport/memory.go`: a registry of `Handler`s plus `connected []bool`.

- `Disconnect(i)` / `Connect(i)` — while disconnected, every RPC to or from node `i` returns `false`. **This is how you simulate a network partition**, with no networking involved.
- Add a random 1–10ms delay to each call so goroutine interleavings actually vary between runs.

### Logic

1. **Start election** on timeout: increment `currentTerm`, become Candidate, vote for self, reset the deadline, send `RequestVote` to every peer **in parallel goroutines**.
2. **`RequestVote` handler**, implemented literally:
   - if `args.Term < currentTerm` → reject
   - if `args.Term > currentTerm` → `becomeFollower(args.Term)`
   - grant if `votedFor ∈ {None, args.CandidateID}` **and** the candidate's log is at least as up-to-date
   - **only reset your election deadline if you actually granted the vote**
3. **Helpers** (all assume the lock is held):
   ```go
   func (rf *Raft) lastLogIndex() int { return len(rf.log) - 1 }
   func (rf *Raft) lastLogTerm() int  { return rf.log[len(rf.log)-1].Term }

   // isUpToDate implements the section 5.4.1 comparison.
   func (rf *Raft) isUpToDate(lastIdx, lastTerm int) bool {
       if lastTerm != rf.lastLogTerm() {
           return lastTerm > rf.lastLogTerm()
       }
       return lastIdx >= rf.lastLogIndex()
   }

   // becomeFollower steps down to term t. Call it from EVERYWHERE you
   // observe a term greater than currentTerm.
   func (rf *Raft) becomeFollower(term int) {
       rf.currentTerm = term
       rf.votedFor = None
       rf.role = Follower
   }
   ```
4. **On winning** (votes ≥ `majority()`): become Leader and log it.

**The global term rule:** on *every* RPC you send or receive, if you observe a term greater than `currentTerm`, immediately step down. One helper, called everywhere.

### Verify

- `TestElectsALeader`: 3 nodes, assert some node reports Leader within 1s.
- `TestAtMostOneLeaderPerTerm`: run 3 nodes for 3s, recording every `(term, leader)` transition. Assert no term ever has two leaders. This is the actual safety property, and it holds even while terms churn.
- `TestNoLeaderWithoutQuorum`: disconnect 2 of 3; assert the survivor never becomes Leader.

### Gotchas

- **The stale-reply bug, which you will hit.** You release the lock to send an RPC (correct), get a reply, re-acquire the lock, and act on it — but the world moved while you waited. **After every RPC, re-validate under the lock:** am I still a Candidate? Is `currentTerm` still the term I sent? If not, discard the reply. This is the single largest source of bugs in this project.
- Count votes in a local variable owned by the election, not a struct field, so a stale election's tally can't leak into a new one.
- Go 1.22+ scopes loop variables per iteration, so `for p := range n { go func(){ use(p) }() }` is safe on 1.23. Pass explicitly anyway if you ever target older Go.

---

## Task 4 — Heartbeats and stable leadership (1.5h)

**Goal:** the leader suppresses further elections. One leader, stable term, and clean failover.

### Files

- **`raft/types.go`** (edit) — `AppendEntriesArgs`/`Reply`, `HeartbeatInterval`, the new `Transport`/`Handler` methods, `leaderID` on `Raft`
- **`raft/replication.go`** (new) — the heartbeat loop and the `AppendEntries` handler
- **`raft/election.go`** (edit) — on winning, set `leaderID = rf.me` and launch the heartbeat loop
- **`raft/raft.go`** (edit) — `becomeFollower` also clears `leaderID` to `None`
- **`transport/memory.go`** (edit) — route the new RPC
- **`raft/election_test.go`** (edit) — stability and failover tests

### New types

```go
// Heartbeat-only for now: it carries authority, not data.
type AppendEntriesArgs struct {
    Term     int
    LeaderID int
}

type AppendEntriesReply struct {
    Term    int
    Success bool
}

const HeartbeatInterval = 50 * time.Millisecond
```

### Changes to existing types

Extend `Transport`:

```go
SendAppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool
```

Extend `Handler`:

```go
AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error
```

Add to `Raft`:

```go
leaderID int // None if unknown
```

Update `MemoryTransport` to route the new RPC. Set `leaderID = None` inside `becomeFollower`.

### Logic

1. **Heartbeat loop**, started when a node wins an election: every `HeartbeatInterval`, send `AppendEntries` to all peers in parallel. Exit as soon as the node is no longer Leader in the term it started in.
2. **`AppendEntries` handler:**
   - if `args.Term < currentTerm` → reply `Success: false`
   - if `args.Term > currentTerm` → `becomeFollower(args.Term)`
   - accept: set `role = Follower`, `leaderID = args.LeaderID`, **reset the election deadline**, reply `Success: true`
3. On winning an election, set `leaderID = rf.me`.

### Verify

- `TestStableLeadership`: 3 nodes, wait 1s, record the term, wait 2s more, assert the leader and term are unchanged. This is the test Task 3 could not pass.
- `TestReElectionAfterLeaderFailure`: disconnect the leader; assert a new leader emerges within 1s at a higher term.
- `TestOldLeaderStepsDown`: reconnect the old leader; assert it reports Follower and has adopted the new term.

### Gotchas

- Capture the term when you start the heartbeat loop and check `rf.currentTerm == startTerm && rf.role == Leader` on every iteration. A deposed leader that keeps heartbeating is a split brain.
- A follower resets its election deadline **only** for an `AppendEntries` it actually accepts. Resetting on a rejected one lets a stale leader keep a cluster hostage.

---

## Task 5 — Log replication (3h) ⚠️ hardest task

**Goal:** the leader accepts commands and every follower's log converges to match, byte for byte.

### Files

- **`raft/types.go`** (edit) — `Command` on `LogEntry`, the four new `AppendEntriesArgs` fields, `nextIndex`/`matchIndex`/`commitIndex` on `Raft`
- **`raft/replication.go`** (edit) — `Start(command any)`, the consistency check, truncate-and-append, leader-side replication
- **`raft/election.go`** (edit) — initialize `nextIndex`/`matchIndex` on an election win
- **`raft/replication_test.go`** (new, `package raft_test`) — the three replication tests

⚠️ **Name collision to settle before you start.** This task’s client entry point is `Start(command any)`, but Task 2 already used `Start()` for launching the background goroutines. Rename the launcher to `Run()` first — doing it midway through a half-written replication path is strictly worse.

### Changes to existing types

Add a command payload to `LogEntry`:

```go
type LogEntry struct {
    Term    int
    Index   int
    Command any // new in this task
}
```

Extend `AppendEntriesArgs` from a heartbeat into the replication RPC:

```go
type AppendEntriesArgs struct {
    Term     int
    LeaderID int

    // new in this task
    PrevLogIndex int
    PrevLogTerm  int
    Entries      []LogEntry
    LeaderCommit int
}
```

Add to `Raft`:

```go
nextIndex  []int // len == numPeers; leader-only, reset on election win
matchIndex []int // len == numPeers; leader-only, reset on election win
commitIndex int
```

`LeaderCommit` and `commitIndex` arrive here because the consistency check needs somewhere to put the leader's commit point; **advancing and applying it is Task 6.** For now, just store what the leader tells you.

### Logic

1. **`Start(command any) (index, term int, isLeader bool)`** — the client entry point. Not leader → `(-1, -1, false)`. Leader → append `{currentTerm, nextIndex, command}` to the log and return `(index, term, true)` **immediately**. Do not wait for commit.
2. **On winning an election**, initialize `nextIndex[i] = rf.lastLogIndex() + 1` and `matchIndex[i] = 0` for every peer.
3. **`AppendEntries` handler** — extend it with the Figure 2 rules:
   - reject if your log has no entry at `PrevLogIndex`, or its term ≠ `PrevLogTerm` — **this is the consistency check**
   - walk the incoming entries against yours; at the first index where the terms conflict, **truncate your log from there** and append the remainder
   - **do not blindly truncate** — a delayed duplicate RPC would wipe out good entries
   - set `commitIndex = min(args.LeaderCommit, index of last new entry)` if that's larger than your current value
4. **Leader replication**, folded into the heartbeat loop: for each peer send `PrevLogIndex = nextIndex[peer] - 1`, `PrevLogTerm = log[PrevLogIndex].Term`, `Entries = log[nextIndex[peer]:]`.
   - success → `matchIndex[peer] = args.PrevLogIndex + len(args.Entries)`, `nextIndex[peer] = matchIndex[peer] + 1`
   - failure that is *not* a term problem → decrement `nextIndex[peer]`, retry on the next tick

### Verify

- `TestReplicatesEntries`: leader accepts 10 commands; assert all 3 logs are identical in length, terms, and commands.
- `TestFollowerCatchesUp`: disconnect one follower, commit 20 entries, reconnect; assert it converges within 2s.
- `TestConflictingEntriesOverwritten`: hand-craft a follower log with 3 bogus trailing entries at a stale term; assert the leader overwrites exactly those and keeps the valid prefix.

### Gotchas

- **Compute `matchIndex` from the args you sent, never from `len(rf.log)`.** The log may have grown while your RPC was in flight, and you'd mark entries replicated that the follower never saw. This one silently corrupts commits.
- Re-read the stale-reply gotcha from Task 3. It applies to every reply you handle here too.
- `Command` is `any`. Gob cannot encode a value through an interface field unless the concrete type is registered — that bites in Task 9, and the plan flags it there.

---

## Task 6 — Commit and apply (1.5h)

**Goal:** entries replicated to a majority get committed and handed to a state machine, in the same order on every node.

### Files

- **`raft/types.go`** (edit) — `ApplyMsg`; `lastApplied`, `applyCh`, `applyCond` on `Raft`
- **`raft/apply.go`** (new) — the applier goroutine and `advanceCommitIndex`, which is where the Figure 8 rule lives
- **`raft/replication.go`** (edit) — signal `applyCond` wherever `commitIndex` moves, on both the leader and the follower path
- **`raft/raft.go`** (edit) — build `applyCond` in `Make`, launch the applier from `Run`
- **`raft/apply_test.go`** (new, `package raft_test`) — ordering, quorum, and the partitioned-leader test

### New types

```go
// ApplyMsg is what Raft hands to the state machine.
type ApplyMsg struct {
    Command any
    Index   int
    Term    int
}
```

### Changes to existing types

Add to `Raft`:

```go
lastApplied int
applyCh     chan ApplyMsg
applyCond   *sync.Cond // signalled when commitIndex advances
```

Construct `applyCond = sync.NewCond(&rf.mu)` in `Make`.

### Logic

1. **Leader advances `commitIndex`** after any `matchIndex` update. Find the largest `N` where:
   - `N > commitIndex`
   - `matchIndex[i] >= N` for a majority (counting yourself)
   - **`log[N].Term == currentTerm`**
2. **Applier goroutine**, one per node, separate from everything else. It waits on `applyCond`; when `commitIndex > lastApplied` it increments `lastApplied` and sends `log[lastApplied]` on `applyCh`. Signal the cond wherever `commitIndex` changes — in both the leader path and the `AppendEntries` handler.

### The one rule that matters most

That third commit condition is **Figure 8** in the paper. A leader may **never** commit a previous term's entry by counting replicas; prior-term entries get committed only indirectly, when a current-term entry commits and carries everything before it with it.

Omit it and every test you've written still passes while the cluster silently loses committed data under a specific partition-and-failover interleaving. Understand this one properly — it is the strongest thing you can say about this project in an interview.

### Verify

- `TestAppliesInOrder`: drain `applyCh` on all 3 nodes for 20 commands; assert identical sequences.
- `TestCommitRequiresMajority`: disconnect 2 of 3; submit a command to the leader; assert nothing is ever applied and `commitIndex` does not move.
- `TestPartitionedLeaderCannotCommit` — **the money test.** Isolate the leader with one follower. Its writes must not commit. Heal the partition, and assert its uncommitted entries are overwritten by the majority's log. This demonstrates the actual safety guarantee; put its output in the README.

### Gotchas

- **Never send on a channel while holding the mutex.** If the receiver needs that mutex, you deadlock. This is precisely why the applier is its own goroutine — release the lock, send, re-acquire.
- Apply strictly one index at a time in ascending order. A gap or a reorder here is a state machine divergence.

---

**End of Day 1.** Commit. You have real Raft; the rest is durability, packaging, and proof.

---

# DAY 2 — Durability, networking, product, proof

---

## Task 7 — Persistence (1.25h)

**Goal:** a node that crashes and restarts does not forget what it promised.

### Files

- **`raft/persist.go`** (new) — `persistentState`, `persist()`, `readPersist()`
- **`raft/raft.go`** (edit) — `readPersist()` from `Make`; `persist()` at the end of `becomeFollower`
- **`raft/election.go`** (edit) — `persist()` after granting a vote and after incrementing the term
- **`raft/replication.go`** (edit) — `persist()` after appending to the log
- **`raft/persist_test.go`** (new, `package raft_test`)

Persistence is cross-cutting: the new file is small, and most of the work is the four call sites spread across three existing files. Grep for every write to `currentTerm`, `votedFor`, and `log` to confirm you caught them all.

### New types

```go
// persistentState exists only because gob ignores unexported fields.
// Copy into it to save, out of it to restore.
type persistentState struct {
    CurrentTerm int
    VotedFor    int
    Log         []LogEntry
}
```

**This type is why the task isn't a one-liner.** `rf.currentTerm` and friends are unexported, so gob-encoding the `Raft` struct directly writes an empty record and returns **no error** — every restart would look like a fresh node and you'd chase it for an hour.

### Logic

- `persist()` — encode a `persistentState` to `raft-<id>.state` via `encoding/gob`. Assumes the lock is held.
- `readPersist()` — called from `Make`; on a missing file, start fresh.
- Call `persist()` wherever those three fields change: end of `becomeFollower`, after granting a vote, after appending to the log, and after a term increment.

Only these three fields are persistent. `commitIndex` is deliberately volatile — it gets rebuilt from the leader's `LeaderCommit`.

### Verify

`TestPersistence`: commit 10 entries, `Kill()` all 3 nodes, re-`Make` them from disk, assert every committed entry is present with matching terms. Then assert a restarted node does not vote twice in the same term.

### Gotchas

- Write to a temp file and `os.Rename` it into place. A crash mid-write otherwise leaves a truncated state file that fails to decode, and the node comes up having lost its vote.
- Register nothing here yet — `LogEntry.Command` is `any`, so if you persist entries carrying a concrete command type you'll need `gob.Register`. Task 9 introduces the command type and handles it.

---

## Task 8 — TCP transport and the node binary (1.75h)

**Goal:** three real OS processes, so you can kill one with `Ctrl-C`.

### Files

- **`transport/tcp.go`** (new) — `TCPTransport`
- **`cmd/raftnode/main.go`** (edit) — the `--id`, `--peers`, `--http`, `--debug` flags; register the RPC receiver and serve
- **`scripts/demo.sh`** (edit) — launch the three processes

**No changes to the `raft` package.** That is the test of Task 3’s abstraction: if you find yourself editing `raft/` here, the `Transport` interface has a leak worth fixing rather than working around.

### New types

`transport/tcp.go` — a `TCPTransport` implementing the existing `Transport` interface with `net/rpc`. No changes to the `raft` package at all; this is the payoff for Task 3's abstraction.

```go
type TCPTransport struct {
    mu      sync.Mutex
    addrs   []string           // peer index -> host:port
    clients map[int]*rpc.Client // lazily dialed, cached
}
```

### Logic

- Register the Raft node as an RPC receiver and serve on its own address. Your handlers already have the `net/rpc` signature from Task 3.
- Dial lazily, cache the client. On **any** error, drop the cached client and return `false` — a dropped RPC is normal, and the next tick redials.
- `cmd/raftnode/main.go`: flags `--id`, `--peers=host:port,host:port,host:port`, `--http=:8080`, `--debug`.

### Verify

Three terminals, three processes. Assert via the debug log that exactly one leader emerges, then `Ctrl-C` the leader and watch a real election complete.

### Gotchas

- Set a dial and call timeout. `net/rpc` will otherwise block a heartbeat goroutine indefinitely against a dead peer, and your leader stops heartbeating everyone else.
- Nodes will fail to reach peers that haven't started yet. That's correct behavior — the RPC returns `false` and the election retries. Don't add startup barriers.

---

## Task 9 — KV store and HTTP API (2h)

**Goal:** the thing that makes this a *project* rather than a library.

### Files

- **`kv/store.go`** (new) — `OpKind`, `Command`, `Result`, the map, and `func init() { gob.Register(Command{}) }`
- **`kv/server.go`** (new) — `Server`, `Status`, the `applyCh` drain loop, the HTTP handlers
- **`raft/raft.go`** (edit) — the exported accessors `/status` needs: `LeaderID()`, `Role()`, `Term()`, `CommitIndex()`, `LogLength()`, each taking `mu`
- **`cmd/raftnode/main.go`** (edit) — construct the `kv.Server` and serve HTTP

If you added those accessors back in Task 3 for the external test package, they already exist and this line is free.

### New types

`kv/store.go` and `kv/server.go`:

```go
type OpKind string

const (
    OpSet    OpKind = "set"
    OpDelete OpKind = "delete"
)

// Command is what gets replicated through the Raft log.
type Command struct {
    Op    OpKind
    Key   string
    Value string
}

// Result goes back to the waiting HTTP request once the command at
// its log index has been applied.
type Result struct {
    Value string
    Found bool
    Err   string
}

type Status struct {
    ID          int    `json:"id"`
    Role        string `json:"role"`
    Term        int    `json:"term"`
    LeaderID    int    `json:"leader_id"`
    CommitIndex int    `json:"commit_index"`
    LogLength   int    `json:"log_length"`
}

type Server struct {
    rf    *raft.Raft
    store map[string]string

    mu      sync.Mutex
    pending map[int]chan Result // log index -> waiter
}
```

**Required, exactly once:**

```go
func init() { gob.Register(Command{}) }
```

`LogEntry.Command` is `any`. Gob refuses to encode through an interface field unless the concrete type is registered, and the failure surfaces as a runtime error during replication — not at compile time, and possibly not on the node that made the mistake.

### Logic

- A goroutine draining `applyCh`: apply the `Command` to `store`, then look up `pending[msg.Index]` and send the `Result`.
- `PUT /kv/{key}` → `Start(Command{...})`; register a waiter at the returned index; block on it. **Time out after ~2s and return 503** — a write submitted to a leader that then loses its term will never be applied, and the handler must not hang forever.
- `GET /kv/{key}` → read the local map. Document in the code and README that this is a **stale read**, and that linearizable reads need ReadIndex. Interviewers care about you knowing the difference.
- `GET /status` → the `Status` struct.
- Not the leader → `307` redirect to the leader's HTTP address, resolved from `rf.LeaderID()`. Add that small exported getter to the `raft` package.

### Verify

`curl -L -X PUT localhost:8081/kv/foo -d bar` against **any** node succeeds, and `GET /kv/foo` returns `bar` from all three.

### Gotchas

- Register the waiter *before* the applier could plausibly run, and hold `Server.mu` while doing it, or a fast local apply lands before anyone is listening and the request hangs to its timeout.
- The applier must not block when no waiter exists (a follower applying a leader's entry). Use a buffered channel of size 1, or check for presence and skip.

---

## Task 10 — Test suite hardening (2.5h)

**Goal:** the tests are your credibility. Tighten what you wrote per-task into a suite you can paste into the README.

### Files

- **`raft/raft_test.go`** (`package raft`) — keep the white-box unit tests here
- **`raft/cluster_test.go`** (new, `package raft_test`) — the `cluster` helper: `makeCluster`, `disconnect`, `connect`, `checkOneLeader`, `checkLogsMatch`
- fold `election_test.go`, `replication_test.go`, `apply_test.go`, and `persist_test.go` into `raft/cluster_test.go`

Consolidate *within* each test package, never across them: internal unit tests cannot merge with external cluster tests, for the import-cycle reason in Task 3. Two files is the floor, not a compromise.

### Logic

- Consolidate into `raft/raft_test.go` with a `cluster` helper: `makeCluster(n)`, `disconnect(i)`, `connect(i)`, `checkOneLeader()`, `checkLogsMatch()`.
- **`checkOneLeader()` must retry for ~1s before asserting.** Raft is asynchronous; asserting instantly produces flaky tests that you'll wrongly blame on your code.
- Add `TestConcurrentClients`: 5 goroutines × 20 commands; assert no lost, duplicated, or reordered entries.
- Add `TestChurn`: randomly disconnect and reconnect one node every 200ms for 5s while writing; assert the logs converge once healed.
- **Run everything with `-race`, and run it 20 times** (`go test -race -count=20 ./...`). Distributed bugs are interleaving-dependent; a single green run proves very little. Race detector reports are real bugs, never flakiness.

### Verify

20 consecutive clean `-race` runs. If one in twenty fails, you have a real bug — almost always the stale-reply check from Task 3 or the `matchIndex` computation from Task 5.

---

## Task 11 — README and demo (2h)

Resumes get 20 seconds of attention. **The README is the project.**

### Files

- **`README.md`** (new)
- **`scripts/demo.sh`** (edit) — the script you actually record
- **`docs/`** (new, optional) — somewhere to keep the GIF or asciinema cast

### Include

1. **One-paragraph pitch** — what Raft is and what you built, in plain language.
2. **An asciinema recording or GIF**: three terminals, write a key, kill the leader, watch re-election, read the key back. Highest-value artifact in the repo.
3. **Architecture diagram** — Mermaid renders natively on GitHub.
4. **"What's implemented" / "What's deliberately not"** tables, with your reasoning from the scope section.
5. **A "Correctness notes" section** — this is where you prove you understand it. Short paragraphs on:
   - Why a leader can't commit prior-term entries by replica count (Figure 8)
   - Why the up-to-date check in `RequestVote` preserves Leader Completeness
   - Why randomized election timeouts are necessary
   - Why the heartbeat interval must sit far below the election timeout
   - Why local `GET` is a stale read, and what ReadIndex would fix
6. **Test output**, including `-race` and the 20× run.
7. **How to run it.**

### Resume bullet

> Implemented the Raft consensus algorithm in Go (~1,500 LOC): leader election, log replication, and crash-durable persistence, exposed as a replicated key-value store over HTTP. Verified safety with a deterministic in-memory network-partition harness covering split-brain, follower catch-up, and leader failover; race-detector clean over 20 consecutive runs.

---

## Stretch (only if genuinely ahead)

**1. Fast log backtracking (~45min).** Additive change to one type:

```go
type AppendEntriesReply struct {
    Term    int
    Success bool

    ConflictIndex int // new
    ConflictTerm  int // new
}
```

A follower rejecting on the consistency check reports the first index of its conflicting term; the leader jumps `nextIndex` there instead of decrementing by one per round trip. Section 5.3. Small, and a good thing to be able to mention.

**2. Snapshots (~4h).** Real work — only if Day 2 ends early. This is where `LogEntry.Index` stops being redundant.

---

## Time budget

| Day | Task | Est. |
|---|---|---|
| 1 | 1. Skeleton, lifecycle, logging | 1.00h |
| 1 | 2. Election timer | 0.75h |
| 1 | 3. Leader election | 2.50h |
| 1 | 4. Heartbeats, stable leadership | 1.50h |
| 1 | 5. Log replication | 3.00h |
| 1 | 6. Commit and apply | 1.50h |
| | **Day 1** | **10.25h** |
| 2 | 7. Persistence | 1.25h |
| 2 | 8. TCP transport + binary | 1.75h |
| 2 | 9. KV store + HTTP | 2.00h |
| 2 | 10. Test suite hardening | 2.50h |
| 2 | 11. README + demo | 2.00h |
| | **Day 2** | **9.50h** |

**If you fall behind:** cut Task 8 (TCP) and demo with the in-memory cluster — the algorithm is identical and the README reads the same. Then cut Task 9's blocking-write machinery and return 202 immediately. **Never cut Task 10 or 11**; untested code with no README isn't a resume project.
