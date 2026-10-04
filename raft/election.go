package raft

// startElection begins a new election. The caller must hold rf.mu.
func (rf *Raft) startElection() {
	rf.currentTerm++
	rf.role = Candidate
	rf.votedFor = rf.me
	rf.resetElectionDeadline()

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}

	// Owned by this election: a stale election's tally can't leak into a new one.
	votes := 1
	term := rf.currentTerm

	for peer := 0; peer < rf.numPeers; peer++ {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			reply := &RequestVoteReply{}
			if !rf.transport.SendRequestVote(peer, args, reply) {
				return // dropped or timed out; reply is meaningless
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				rf.resetElectionDeadline()
				return
			}

			// The world moved while the RPC was in flight.
			if rf.role != Candidate || rf.currentTerm != term {
				return
			}
			if !reply.VoteGranted {
				return
			}

			votes++
			if votes == rf.Majority() {
				rf.role = Leader
				rf.leaderID = rf.me
				go rf.HeartbeatLoop(rf.currentTerm)
				rf.dlog("became leader with %d votes", votes)
			}
		}(peer)
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return nil
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	reply.Term = rf.currentTerm
	canVote := rf.votedFor == None || rf.votedFor == args.CandidateID
	reply.VoteGranted = canVote && rf.isUpToDate(args.LastLogIndex, args.LastLogTerm)

	if reply.VoteGranted {
		rf.votedFor = args.CandidateID
		// Only a granted vote earns the candidate more time.
		rf.resetElectionDeadline()
	}
	return nil
}
