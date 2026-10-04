package raft

import "time"

func (rf *Raft) heartbeatLoop(term int) {
	for !rf.Killed() {
		rf.mu.Lock()
		if rf.role != Leader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		args := &AppendEntriesArgs{Term: term, LeaderID: rf.me}
		rf.mu.Unlock()

		for peer := 0; peer < rf.numPeers; peer++ {
			if peer == rf.me {
				continue
			}
			go func(peer int) {
				reply := &AppendEntriesReply{}
				if !rf.transport.SendAppendEntries(peer, args, reply) {
					return
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.currentTerm {
					rf.becomeFollower(reply.Term)
					rf.resetElectionDeadline()
				}
			}(peer)
		}
		time.Sleep(HeartBeatInterval)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return nil
	}

	rf.becomeFollower(args.Term)
	rf.leaderID = args.LeaderID
	rf.resetElectionDeadline()

	reply.Term = rf.currentTerm
	reply.Success = true

	return nil
}
