package raft

import (
	"context"
	"time"

	pb "github.com/EddieJ03/223b-raft/raft/github.com/EddieJ03/223b-raft"
)

func (rn *RaftNode) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	rn.Mu.Lock()
	defer rn.Mu.Unlock()

	if req.Term < rn.CurrentTerm {
		return &pb.RequestVoteResponse{Term: rn.CurrentTerm, VoteGranted: false}, nil
	}

	if req.Term > rn.CurrentTerm {
		rn.CurrentTerm = req.Term
		rn.VotedFor = -1
		rn.State = Follower
	}

	lastLogIndex := rn.getLastLogIndex()
	lastLogTerm := rn.getLastLogTerm()

	// 	If the logs have last entries with different terms, then
	//  the log with the later term is more up-to-date. If the logs
	//  end with the same term, then whichever log is longer is
	//  more up-to-date.
	candidateUpToDate := lastLogTerm < req.LastLogTerm ||
		(lastLogTerm == req.LastLogTerm && lastLogIndex <= req.LastLogIndex)

	if (rn.VotedFor == -1 || rn.VotedFor == req.CandidateId) && candidateUpToDate {
		rn.VotedFor = req.CandidateId
		rn.electionReset = time.Now()
		return &pb.RequestVoteResponse{Term: rn.CurrentTerm, VoteGranted: true}, nil
	}

	return &pb.RequestVoteResponse{Term: rn.CurrentTerm, VoteGranted: false}, nil
}

func (rn *RaftNode) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	rn.Mu.Lock()
	defer rn.Mu.Unlock()

	if req.Term < rn.CurrentTerm {
		return &pb.AppendEntriesResponse{Term: rn.CurrentTerm, Success: false}, nil
	}

	rn.electionReset = time.Now()

	if req.Term > rn.CurrentTerm {
		rn.CurrentTerm = req.Term
		rn.VotedFor = -1
		rn.State = Follower
	}

	rn.leaderId = req.LeaderId

	reqEntries := make([]Log, len(req.Entries))

	for i, entry := range req.Entries {
		reqEntries[i] = Log{
			Term:  entry.Term,
			Op:    entry.Op,
			Key:   entry.Key,
			Value: entry.Value,
			Index: entry.Index,
		}
	}

	if rn.Snapshot == nil { // no snapshot yet, everything is in the Logs
		if int(req.PrevLogIndex) >= len(rn.Logs) || (req.PrevLogTerm != rn.Logs[req.PrevLogIndex].Term) {
			return &pb.AppendEntriesResponse{Term: rn.CurrentTerm, Success: false}, nil
		}

		rn.Logs = rn.Logs[:req.PrevLogIndex+1]

	} else {
		if req.PrevLogIndex < rn.Snapshot.LastIncludedIndex {
			// prev log index is before the snapshot, so we cannot append
			return &pb.AppendEntriesResponse{Term: rn.CurrentTerm, Success: false}, nil
		}

		upToKeepIndex := int32(len(rn.Logs)) - 1

		if req.PrevLogIndex == rn.Snapshot.LastIncludedIndex {
			if req.PrevLogTerm != rn.Snapshot.LastIncludedTerm {
				return &pb.AppendEntriesResponse{Term: rn.CurrentTerm, Success: false}, nil
			}

			// snapshot contains everything Prev, so we need to keep all the logs in rn.Logs
		} else {
			// keep decrementing if no match
			for upToKeepIndex >= 0 && (rn.Logs[upToKeepIndex].Index != req.PrevLogIndex || rn.Logs[upToKeepIndex].Term != req.PrevLogTerm) {
				upToKeepIndex--
			}

			if upToKeepIndex < 0 { // if we get upToKeepIndex < 0, it means nothing was able to get matched so we cannot append
				return &pb.AppendEntriesResponse{Term: rn.CurrentTerm, Success: false}, nil
			}
		}

		rn.Logs = rn.Logs[:upToKeepIndex+1]
	}
	if len(reqEntries) > 0 {
		rn.Logs = append(rn.Logs, reqEntries...)
	}

	if req.LeaderCommit > rn.CommitIndex {
		rn.CommitIndex = min(req.LeaderCommit, rn.getLastLogIndex())
	}

	rn.applyState()

	// always persist logs
	rn.WriteLogFile()

	return &pb.AppendEntriesResponse{Term: rn.CurrentTerm, Success: true}, nil
}

func (rn *RaftNode) InstallSnapshot(ctx context.Context, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error) {
	rn.Mu.Lock()
	defer rn.Mu.Unlock()

	if req.Term < rn.CurrentTerm {
		return &pb.InstallSnapshotResponse{Term: rn.CurrentTerm}, nil
	}

	if req.Term > rn.CurrentTerm {
		rn.CurrentTerm = req.Term
		rn.VotedFor = -1
		rn.State = Follower
	}

	rn.electionReset = time.Now()
	rn.leaderId = req.LeaderId

	// if we already have a more recent snapshot, then ignore this one
	if rn.Snapshot != nil && req.LastIncludedIndex <= rn.Snapshot.LastIncludedIndex && req.LastIncludedTerm <= rn.Snapshot.LastIncludedTerm {
		return &pb.InstallSnapshotResponse{Term: rn.CurrentTerm}, nil
	}

	matchFound := false
	keepFrom := -1

	for i := len(rn.Logs) - 1; i >= 0; i-- {
		if rn.Logs[i].Index == req.LastIncludedIndex {
			if rn.Logs[i].Term == req.LastIncludedTerm {
				matchFound = true
				keepFrom = i + 1
			}

			break
		}
	}

	if matchFound {
		rn.Logs = rn.Logs[keepFrom:]
	} else {
		rn.Logs = []Log{} // discard entire log, snapshot is the new starting point
	}

	// install new snapshot and reset state machine
	snapshotData := make(map[string]string)
	rn.StateMachine = make(map[string]string)

	for k, v := range req.Data {
		snapshotData[k] = v
		rn.StateMachine[k] = v
	}

	rn.Snapshot = &Snapshot{
		LastIncludedIndex: req.LastIncludedIndex,
		LastIncludedTerm:  req.LastIncludedTerm,
		Data:              snapshotData,
	}

	// make sure to persist first, then update lastapplied and commit
	rn.WriteLogFile()

	rn.lastApplied = req.LastIncludedIndex + 1
	rn.CommitIndex = max(rn.CommitIndex, req.LastIncludedIndex)

	return &pb.InstallSnapshotResponse{Term: rn.CurrentTerm}, nil
}

func convertToRPCEntries(logs []Log) []*pb.Entry {
	entries := make([]*pb.Entry, len(logs))
	for i, log := range logs {
		entries[i] = &pb.Entry{
			Term:  log.Term,
			Op:    log.Op,
			Key:   log.Key,
			Value: log.Value,
			Index: log.Index,
		}
	}
	return entries
}