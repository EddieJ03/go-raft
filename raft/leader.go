package raft

import (
	"context"
	"log"
	"time"

	pb "github.com/EddieJ03/go-raft/raft/github.com/EddieJ03/go-raft"
)

func (rn *RaftNode) leaderInit() {
	rn.Mu.Lock()
	for pid := range rn.peers {
		if pid == rn.Id {
			continue
		}

		rn.leaderNextIndex[pid] = rn.getLastLogIndex() + 1
		rn.leaderMatchIndex[pid] = 0
	}
	go rn.sendHeartbeats()
	rn.Mu.Unlock()
}

// Leader increments matchIndex and updates CommitIndex
func (rn *RaftNode) setMatchIndex(id int32, index int32) {
	logCounter := 0

	// move logCounter to the first log that is not committed
	for logCounter < len(rn.Logs) {
		if rn.Logs[logCounter].Index <= rn.CommitIndex {
			logCounter++
		} else {
			break
		}
	}

	for i := rn.CommitIndex + 1; i <= index; i++ {
		// if in snapshot, skip
		if rn.Snapshot != nil && i <= rn.Snapshot.LastIncludedIndex {
			continue
		}

		if rn.Logs[logCounter].Term == rn.CurrentTerm {
			count := 0

			for _, matchIdx := range rn.leaderMatchIndex {
				if matchIdx >= i {
					count++
				}
			}

			if count > len(rn.peers)/2 {
				rn.CommitIndex = i
			}
		}

		logCounter++
	}

	rn.leaderMatchIndex[id] = index

	// may be a new commit index, so apply state
	rn.applyState()
}

// repeat AppendEntries to follower until successful
func (rn *RaftNode) UpdateFollower(id int32, client pb.RaftClient) {
	term := rn.CurrentTerm
	lastIndex := rn.getLastLogIndex()
	clientIndex := rn.leaderNextIndex[id]

	// already up to date, empty heartbeat
	if clientIndex > lastIndex {
		ctx, cancel := context.WithTimeout(context.Background(), DefaultRPCTimeout*time.Second)
		defer cancel()

		req := &pb.AppendEntriesRequest{
			Term:         term,
			LeaderId:     rn.Id,
			PrevLogIndex: rn.getLastLogIndex(),
			PrevLogTerm:  rn.getLastLogTerm(),
			Entries:      []*pb.Entry{}, // empty entries for heartbeat
			LeaderCommit: rn.CommitIndex,
		}

		resp, err := client.AppendEntries(ctx, req)

		if err == nil {
			if resp.Success {
				// all logs still up to date
				rn.MuMap.Lock()
				rn.leaderNextIndex[id] = lastIndex + 1
				rn.setMatchIndex(id, lastIndex)
				rn.MuMap.Unlock()
				return
			} else {
				// there is another leader
				if resp.Term > rn.CurrentTerm {
					rn.CurrentTerm = resp.Term
					rn.VotedFor = -1
					rn.State = Follower
					return
				} else {
					// follower is not up to date, will run the for loop below
					clientIndex--
				}
			}
		} else {
			// failed to send, next heartbeat will retry
			return
		}
	}

	logIdx := int32(len(rn.Logs)) - 1

	// update follower with entries
	for clientIndex <= lastIndex {
		// first check if we need to send a snapshot, next heartbeat will send other entries to follower
		if rn.Snapshot != nil && clientIndex <= rn.Snapshot.LastIncludedIndex {
			rn.sendSnapshot(id, client)
			return
		}

		// make sure logIdx points to right log entry index, aka it should not be ahead of clientIndex
		for logIdx >= 0 && rn.Logs[logIdx].Index > clientIndex {
			logIdx--
		}

		ctx, cancel := context.WithTimeout(context.Background(), DefaultRPCTimeout*time.Second)
		defer cancel()

		// it could be in the snapshot
		prevLogIndex := int32(0)

		if logIdx-1 >= 0 {
			prevLogIndex = rn.Logs[logIdx-1].Index
		} else {
			// if no more logs, then prevLogIndex is the last included index of the snapshot
			prevLogIndex = rn.Snapshot.LastIncludedIndex
		}

		prevLogTerm := int32(0)

		if logIdx-1 >= 0 {
			prevLogTerm = rn.Logs[logIdx-1].Term
		} else {
			// if no more logs, then prevLogIndex is the last included index of the snapshot
			prevLogTerm = rn.Snapshot.LastIncludedTerm
		}

		req := &pb.AppendEntriesRequest{
			Term:         term,
			LeaderId:     rn.Id,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      convertToRPCEntries(rn.Logs[logIdx:]),
			LeaderCommit: rn.CommitIndex,
		}

		resp, err := client.AppendEntries(ctx, req)

		if err == nil {
			if resp.Success {
				// all logs up to date
				rn.MuMap.Lock()
				rn.leaderNextIndex[id] = lastIndex + 1
				rn.setMatchIndex(id, lastIndex)
				rn.MuMap.Unlock()
				return
			} else {
				// there is another leader
				if resp.Term > rn.CurrentTerm {
					rn.CurrentTerm = resp.Term
					rn.VotedFor = -1
					rn.State = Follower
					return
				} else {
					// inconsistent log
					clientIndex--
					logIdx--
				}
			}
		} else {
			// failed to send, next heartbeat will retry
			return
		}
	}
}

func (rn *RaftNode) sendHeartbeats() {
	heartbeatTick := time.NewTicker(getHeartbeatInterval())

	for {
		select {
		case <-rn.Shutdown:
			log.Printf("Heartbeat stopped for node %d", rn.Id)
			return
		case <-heartbeatTick.C:
			rn.Mu.Lock()

			if rn.State != Leader {
				rn.Mu.Unlock()
				return
			}

			for pid, client := range rn.grpcClients {
				go rn.UpdateFollower(pid, client)
			}

			rn.Mu.Unlock()
		}
	}
}

func (rn *RaftNode) sendSnapshot(id int32, client pb.RaftClient) {
	if rn.Snapshot == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), DefaultRPCTimeout*time.Second)
	defer cancel()

	req := &pb.InstallSnapshotRequest{
		Term:              rn.CurrentTerm,
		LeaderId:          rn.Id,
		LastIncludedIndex: rn.Snapshot.LastIncludedIndex,
		LastIncludedTerm:  rn.Snapshot.LastIncludedTerm,
		Data:              rn.Snapshot.Data,
	}

	resp, err := client.InstallSnapshot(ctx, req)

	if err != nil {
		// Network error, will retry on next heartbeat
		return
	}

	rn.MuMap.Lock()
	defer rn.MuMap.Unlock()

	if resp.Term > rn.CurrentTerm {
		rn.CurrentTerm = resp.Term
		rn.VotedFor = -1
		rn.State = Follower
		return
	}

	// update follower's nextIndex to just after the snapshot
	rn.leaderNextIndex[id] = rn.Snapshot.LastIncludedIndex + 1
	rn.setMatchIndex(id, rn.Snapshot.LastIncludedIndex)
}
