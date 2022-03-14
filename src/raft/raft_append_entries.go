package raft

import "time"

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PervLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term      int
	Success   bool
	NextIndex int
}

func (rf *Raft) getNextIndex() int {
	_, idx := rf.lastLogTermIndex()
	return idx + 1
}

func (rf *Raft) outOfOrderAppendEntries(args *AppendEntriesArgs) bool {
	// prevlog matches
	argsLastIndex := args.PrevLogIndex + len(args.Entries)
	lastTerm, lastIndex := rf.lastLogTermIndex()
	if argsLastIndex < lastIndex && lastTerm == args.Term {
		return true
	}
	return false
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	reply.Term = rf.term

	if rf.term > args.Term {
		rf.mu.Unlock()
		return
	}

	rf.term = args.Term
	rf.changeRole(Follower)
	rf.resetElectionTimer()
	_, lastLogIndex := rf.lastLogTermIndex()

	if args.PrevLogIndex < rf.lastSnapshotIndex {
		// cuz lastsnapshotindex should've been applied, wouldn't occur when things went right
		reply.Success = false
		reply.NextIndex = rf.lastSnapshotIndex + 1
	} else if args.PrevLogIndex > lastLogIndex {
		// log holes
		reply.Success = false
		reply.NextIndex = rf.getNextIndex()
	} else if args.PrevLogIndex == rf.lastSnapshotIndex {
		// last one's snapshot
		if rf.outOfOrderAppendEntries(args) {
			reply.Success = false
			reply.NextIndex = 0
		} else {
			reply.Success = true
			rf.logEntries = append(rf.logEntries[:1], args.Entries...) // 保留 logs[0]
			reply.NextIndex = rf.getNextIndex()
		}
	} else if rf.logEntries[rf.getRealIdxByLogIndex(args.PrevLogIndex)].Term == args.PervLogTerm {
		// out-of-order request should return false
		if rf.outOfOrderAppendEntries(args) {
			reply.Success = false
			reply.NextIndex = 0
		} else {
			reply.Success = true
			rf.logEntries = append(rf.logEntries[0:rf.getRealIdxByLogIndex(args.PrevLogIndex)+1], args.Entries...)
			reply.NextIndex = rf.getNextIndex()
		}
	} else {
		reply.Success = false
		// skip one term
		term := rf.logEntries[rf.getRealIdxByLogIndex(args.PrevLogIndex)].Term
		idx := args.PrevLogIndex
		for idx > rf.commitIndex && idx > rf.lastSnapshotIndex && rf.logEntries[rf.getRealIdxByLogIndex(idx)].Term == term {
			idx -= 1
		}
		reply.NextIndex = idx + 1
	}
	if reply.Success {
		if rf.commitIndex < args.LeaderCommit {
			rf.commitIndex = args.LeaderCommit
			rf.notifyApplyCh <- struct{}{}
		}
	}

	rf.persist()
	rf.mu.Unlock()
}

func (rf *Raft) getAppendLogs(peerIdx int) (prevLogIndex, prevLogTerm int, res []LogEntry) {
	nextIdx := rf.nextIndex[peerIdx]
	lastLogTerm, lastLogIndex := rf.lastLogTermIndex()
	if nextIdx <= rf.lastSnapshotIndex || nextIdx > lastLogIndex {
		prevLogIndex, prevLogTerm = lastLogIndex, lastLogTerm
		return
	}

	res = append([]LogEntry{}, rf.logEntries[rf.getRealIdxByLogIndex(nextIdx):]...)
	prevLogIndex = nextIdx - 1
	if prevLogIndex == rf.lastSnapshotIndex {
		prevLogTerm = rf.lastSnapshotTerm
	} else {
		prevLogTerm = rf.getLogByIndex(prevLogIndex).Term
	}
	return
}

func (rf *Raft) getAppendEntriesArgs(peerIdx int) AppendEntriesArgs {
	prevLogIndex, prevLogTerm, logs := rf.getAppendLogs(peerIdx)
	args := AppendEntriesArgs{
		Term:         rf.term,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PervLogTerm:  prevLogTerm,
		Entries:      logs,
		LeaderCommit: rf.commitIndex,
	}
	return args
}

func (rf *Raft) resetHeartbeatTimers() {
	for i := range rf.appendEntriesTimers {
		rf.appendEntriesTimers[i].Stop()
		rf.appendEntriesTimers[i].Reset(0)
	}
}

func (rf *Raft) resetHeartbeatTimer(peerIdx int) {
	rf.appendEntriesTimers[peerIdx].Stop()
	rf.appendEntriesTimers[peerIdx].Reset(HeartbeatTimeout)
}

func (rf *Raft) appendEntriesToPeer(peerIdx int) {
	RPCTimer := time.NewTimer(RPCTimeout)
	defer RPCTimer.Stop()

	for !rf.killed() {
		rf.mu.Lock()
		if rf.role != Leader {
			rf.resetHeartbeatTimer(peerIdx)
			rf.mu.Unlock()
			return
		}
		args := rf.getAppendEntriesArgs(peerIdx)
		rf.resetHeartbeatTimer(peerIdx)
		rf.mu.Unlock()

		RPCTimer.Stop()
		RPCTimer.Reset(RPCTimeout)
		reply := AppendEntriesReply{}
		resCh := make(chan bool, 1)
		go func(args *AppendEntriesArgs, reply *AppendEntriesReply) {
			ok := rf.peers[peerIdx].Call("Raft.AppendEntries", args, reply)
			if !ok {
				time.Sleep(time.Millisecond * 10)
			}
			resCh <- ok
		}(&args, &reply)

		select {
		case <-rf.stopCh:
			return
		case <-RPCTimer.C:
			continue
		case ok := <-resCh:
			if !ok {
				continue
			}
		}

		// call ok, check reply
		rf.mu.Lock()
		if reply.Term > rf.term {
			rf.term = reply.Term
			rf.changeRole(Follower)
			rf.votedFor = -1
			rf.resetElectionTimer()
			rf.persist()
			rf.mu.Unlock()
			return
		}

		if rf.role != Leader || rf.term != args.Term {
			rf.mu.Unlock()
			return
		}

		if reply.Success {
			if reply.NextIndex > rf.nextIndex[peerIdx] {
				rf.nextIndex[peerIdx] = reply.NextIndex
				rf.matchIndex[peerIdx] = reply.NextIndex - 1
			}
			if len(args.Entries) > 0 && args.Entries[len(args.Entries)-1].Term == rf.term {
				rf.commitLogEntries()
			}
			rf.persist()
			rf.mu.Unlock()
			return
		} else {
			if reply.NextIndex != 0 {
				if reply.NextIndex > rf.lastSnapshotIndex {
					rf.nextIndex[peerIdx] = reply.NextIndex
					rf.mu.Unlock()
					continue
				} else {
					go rf.sendInstallSnapshot(peerIdx)
					rf.mu.Unlock()
					return
				}
			} else {
				// out-of-order?
				rf.mu.Unlock()
			}
		}
	}
}

func (rf *Raft) commitLogEntries() {
	hasCommit := false
	for i := rf.commitIndex + 1; i <= rf.lastSnapshotIndex+len(rf.logEntries); i++ {
		acks := 0
		for _, m := range rf.matchIndex {
			if m >= i {
				acks += 1
				if acks > len(rf.peers)/2 {
					rf.commitIndex = i
					hasCommit = true
					break
				}
			}
		}
		if rf.commitIndex != i {
			break
		}
	}
	if hasCommit {
		rf.notifyApplyCh <- struct{}{}
	}
}
