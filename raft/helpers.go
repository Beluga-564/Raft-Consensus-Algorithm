package raft

func (rf *Raft) getLastLogIndex() int { return len(rf.log) - 1 }
func (rf *Raft) getLastLogTerm() int  { return rf.log[len(rf.log)-1].Term }

// becomeFollower steps down to term. Call it from everywhere a term greater
// than currentTerm is observed. The caller must hold rf.mu.
func (rf *Raft) becomeFollower(term int) {
	rf.currentTerm = term
	rf.role = Follower
	rf.votedFor = None
	rf.leaderID = None
}

// isUpToDate implements the section 5.4.1 comparison.
func (rf *Raft) isUpToDate(lastIdx, lastTerm int) bool {
	if lastTerm != rf.getLastLogTerm() {
		return lastTerm > rf.getLastLogTerm()
	}
	return lastIdx >= rf.getLastLogIndex()
}
