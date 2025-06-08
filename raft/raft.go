package raft

import (
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "github.com/EddieJ03/223b-raft/raft/github.com/EddieJ03/223b-raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type State int

const (
	Follower  State = 0
	Candidate State = 1
	Leader    State = 2
)

const (
	defaultHeartbeatInterval   = 1000 // default heartbeat every second
	defaultElectionTimeoutMin  = 1500 // miniMum election timeout in milliseconds
	defaultElectionTimeoutMax  = 3000 // maxiMum election timeout in milliseconds
	defaultCompactionThreshold = 200  // compact when log grows beyond this size
	DefaultRPCTimeout          = 1    // default RPC timeout in seconds
)

const (
	Set int32 = iota
	Delete
	NoOp
)

type Log struct {
	Term  int32
	Op    int32
	Key   string
	Value string
	Index int32
}

type Snapshot struct {
	LastIncludedIndex int32
	LastIncludedTerm  int32
	Data              map[string]string
}

type RaftNode struct {
	pb.UnimplementedRaftServer

	Path          string
	Mu            sync.Mutex
	MuMap         sync.Mutex
	Id            int32
	State         State
	CurrentTerm   int32
	VotedFor      int32
	peers         map[int32]string
	VoteCount     int
	electionReset time.Time
	grpcClients   map[int32]pb.RaftClient
	clientConns   map[int32]*grpc.ClientConn
	Shutdown      chan struct{}
	Logs          []Log
	CommitIndex   int32

	// one more than what was last applied, technically
	lastApplied int32

	StateMachine map[string]string
	leaderId     int32

	// snapshot fields
	Snapshot            *Snapshot
	compactionThreshold int32

	// used only by leader
	leaderNextIndex  map[int32]int32
	leaderMatchIndex map[int32]int32
}

type PersistentState struct {
	CurrentTerm int32
	VotedFor    int32
	Logs        []Log
	Snapshot    *Snapshot
}

func NewRaftNode(Id int32, peers map[int32]string, Shutdown chan struct{}, path string) *RaftNode {
	clients := make(map[int32]pb.RaftClient)
	clientConns := make(map[int32]*grpc.ClientConn)

	for pid, addr := range peers {
		if pid == Id {
			continue
		}

		conn, _ := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))

		clientConns[pid] = conn

		clients[pid] = pb.NewRaftClient(conn)
	}

	rn := &RaftNode{
		Path:                path,
		Id:                  Id,
		State:               Follower,
		CurrentTerm:         0,
		VotedFor:            -1,
		peers:               peers,
		grpcClients:         clients,
		electionReset:       time.Now(),
		clientConns:         clientConns,
		Shutdown:            Shutdown,
		Logs:                []Log{{Term: 0, Op: NoOp, Key: "", Value: "", Index: 0}},
		CommitIndex:         0,
		lastApplied:         0,
		StateMachine:        make(map[string]string),
		leaderNextIndex:     make(map[int32]int32),
		leaderMatchIndex:    make(map[int32]int32),
		leaderId:            -1,
		compactionThreshold: getCompactionThreshold(),
		Snapshot:            nil,
	}

	err := os.MkdirAll(rn.Path, 0755)
	if err != nil {
		fmt.Println("Error creating directory:", err)
	}

	if err := rn.ReadLogFile(); err != nil {
		// start with initial state
		rn.WriteLogFile()
	}

	go rn.runElectionTimer()
	return rn
}

func (rn *RaftNode) WriteLogFile() {
	file, err := os.OpenFile(filepath.Join(rn.Path, "raft.log"), os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		log.Printf("Error opening log file: %v", err)
		return
	}

	defer file.Close()

	encoder := gob.NewEncoder(file)

	if err := encoder.Encode(PersistentState{CurrentTerm: rn.CurrentTerm, VotedFor: rn.VotedFor, Logs: rn.Logs, Snapshot: rn.Snapshot}); err != nil {
		panic(err)
	}

	if err := file.Sync(); err != nil {
		log.Printf("Error syncing log file: %v", err)
	}
}

func (rn *RaftNode) ReadLogFile() error {
	file, err := os.Open(filepath.Join(rn.Path, "raft.log"))

	if err != nil {
		if os.IsNotExist(err) {
			return err
		}

		return err
	}

	defer file.Close()

	decoder := gob.NewDecoder(file)

	var state PersistentState

	if err := decoder.Decode(&state); err != nil {
		log.Printf("Error decoding log file: %v", err)
		return err
	}

	rn.CurrentTerm = state.CurrentTerm
	rn.VotedFor = state.VotedFor
	rn.Logs = state.Logs
	rn.Snapshot = state.Snapshot

	// make sure to restore state machine from snapshot if it exists!
	if rn.Snapshot != nil {
		rn.StateMachine = make(map[string]string)

		for k, v := range rn.Snapshot.Data {
			rn.StateMachine[k] = v
		}
	}

	fmt.Println("Read logs from file: ", prettyPrintLogs(rn.Logs))
	fmt.Println("Read snapshot including index: ", rn.Snapshot.LastIncludedIndex)
	return nil
}

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

func (rn *RaftNode) applyState() {
	// skip logs in Logs already applied
	logIdx := 0

	for logIdx < len(rn.Logs) && rn.Logs[logIdx].Index < rn.lastApplied {
		logIdx++
	}

	for logIdx < len(rn.Logs) && rn.lastApplied <= rn.CommitIndex {
		// skip everything in snapshot
		if rn.Snapshot != nil && rn.lastApplied <= rn.Snapshot.LastIncludedIndex {
			rn.lastApplied++
			continue
		}

		// now we start applying stuff in the log

		to_apply := rn.Logs[logIdx]

		switch to_apply.Op {
		case Set:
			rn.StateMachine[to_apply.Key] = to_apply.Value
		case Delete:
			delete(rn.StateMachine, to_apply.Key)
		case NoOp:
			// No operation, do nothing
		}

		logIdx++

		rn.lastApplied++
	}

	rn.maybeCompactLog()
}

func (rn *RaftNode) maybeCompactLog() {
	if int32(len(rn.Logs)) >= rn.compactionThreshold {
		if rn.lastApplied == 0 {
			// this check is just for defenseive programming, in case lastApplied is 0
			return
		}

		rn.compactLog(rn.lastApplied - 1) // lastApplied is one more than what was last applied since it gets increments in applystate
	}
}

func (rn *RaftNode) compactLog(upToAppliedIndex int32) {
	// make snapshot of current state machine
	snapshotData := make(map[string]string)
	for k, v := range rn.StateMachine {
		snapshotData[k] = v
	}

	// Find the term of the last included entry
	lastIncludedTerm := rn.Logs[0].Term
	i := 0

	for i < len(rn.Logs) && rn.Logs[i].Index <= upToAppliedIndex {
		lastIncludedTerm = rn.Logs[i].Term
		i++
	}

	rn.Snapshot = &Snapshot{
		LastIncludedIndex: upToAppliedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		Data:              snapshotData,
	}

	if i < len(rn.Logs) {
		rn.Logs = rn.Logs[i:]
	} else {
		rn.Logs = []Log{}
	}

	rn.WriteLogFile()
}

func (rn *RaftNode) runElectionTimer() {
	for {
		select {
		case <-time.After(500 * time.Millisecond):
			rn.Mu.Lock()

			timeout := getElectionTimeout()
			if rn.State != Leader && time.Since(rn.electionReset) >= timeout {
				rn.startElection()
			}

			rn.Mu.Unlock()
		case <-rn.Shutdown:
			log.Printf("election timer stopped for node %d", rn.Id)
			return
		}
	}
}

func (rn *RaftNode) startElection() {
	rn.State = Candidate
	rn.CurrentTerm++
	rn.VotedFor = rn.Id
	rn.VoteCount = 1
	rn.electionReset = time.Now()

	for pid, client := range rn.grpcClients {
		go func(pid int32, client pb.RaftClient) {
			ctx, cancel := context.WithTimeout(context.Background(), DefaultRPCTimeout*time.Second)
			defer cancel()
			req := &pb.RequestVoteRequest{
				Term:         rn.CurrentTerm,
				CandidateId:  rn.Id,
				LastLogIndex: rn.getLastLogIndex(),
				LastLogTerm:  rn.getLastLogTerm(),
			}

			resp, err := client.RequestVote(ctx, req)
			if err != nil {
				return
			}

			rn.Mu.Lock()
			defer rn.Mu.Unlock()

			if resp.Term > rn.CurrentTerm {
				rn.CurrentTerm = resp.Term
				rn.State = Follower
				rn.VotedFor = -1
				return
			}

			if rn.State != Candidate || rn.CurrentTerm != req.Term { // request term becomes invalid
				return
			}

			if resp.VoteGranted {
				rn.VoteCount++

				if rn.VoteCount > len(rn.peers)/2 {
					rn.State = Leader
					log.Printf("Node %d became leader for term %d", rn.Id, rn.CurrentTerm)
					go rn.leaderInit()
				}
			}
		}(pid, client)
	}
}

func (rn *RaftNode) leaderInit() {
	rn.Mu.Lock()

	// append NoOp at start of term?
	// rn.Logs = append(rn.Logs, Log{Term: rn.CurrentTerm, Op: NoOp, Key: "", Value: "", Index: int32(len(rn.Logs))})

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

func (rn *RaftNode) ClientRequest(op int32, key, value string) (string, error) {
	rn.Mu.Lock()
	defer rn.Mu.Unlock()

	if rn.State != Leader {
		return fmt.Sprintf("%d is the leader", rn.leaderId), errors.New("not the leader")
	}
	entry := Log{
		Term:  rn.CurrentTerm,
		Op:    op,
		Key:   key,
		Value: value,
		Index: rn.getLastLogIndex() + 1,
	}
	rn.Logs = append(rn.Logs, entry)
	rn.leaderMatchIndex[rn.Id] = entry.Index

	for pid, client := range rn.grpcClients {
		go rn.UpdateFollower(pid, client)
	}

	return fmt.Sprintf("CLIENT: request received: %v", entry), nil
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

func (rn *RaftNode) getLastLogIndex() int32 {
	if len(rn.Logs) == 0 {
		if rn.Snapshot == nil {
			return 0
		}

		return rn.Snapshot.LastIncludedIndex
	}

	return rn.Logs[len(rn.Logs)-1].Index
}

func (rn *RaftNode) getLastLogTerm() int32 {
	if len(rn.Logs) == 0 {
		if rn.Snapshot == nil {
			return 0
		}

		return rn.Snapshot.LastIncludedTerm
	}

	return rn.Logs[len(rn.Logs)-1].Term
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

func (rn *RaftNode) CleanResources() {
	log.Printf("cleaning resources for node %d", rn.Id)

	for pid, conn := range rn.clientConns {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection to %d: %v", pid, err)
		}
	}
}

// Helper functions

func getCompactionThreshold() int32 {
	if val := os.Getenv("RAFT_COMPACTION_THRESHOLD"); val != "" {
		if threshold, err := strconv.Atoi(val); err == nil {
			return int32(threshold)
		}
	}

	return defaultCompactionThreshold
}

func getHeartbeatInterval() time.Duration {
	if val := os.Getenv("RAFT_HEARTBEAT_INTERVAL"); val != "" {
		if interval, err := strconv.Atoi(val); err == nil {
			return time.Duration(interval) * time.Millisecond
		}
	}

	return defaultHeartbeatInterval * time.Millisecond
}

func getElectionTimeout() time.Duration {
	minTimeout := defaultElectionTimeoutMin
	maxTimeout := defaultElectionTimeoutMax

	if val := os.Getenv("RAFT_ELECTION_TIMEOUT_MIN"); val != "" {
		if timeout, err := strconv.Atoi(val); err == nil {
			minTimeout = timeout
		}
	}

	if val := os.Getenv("RAFT_ELECTION_TIMEOUT_MAX"); val != "" {
		if timeout, err := strconv.Atoi(val); err == nil {
			maxTimeout = timeout
		}
	}

	return time.Duration(minTimeout+rand.Intn(maxTimeout-minTimeout)) * time.Millisecond
}

func prettyPrintLogs(logs []Log) string {
	var result strings.Builder
	result.WriteString("[")

	for _, log := range logs {
		var op string
		switch log.Op {
		case Set:
			op = "Set"
		case Delete:
			op = "Del"
		case NoOp:
			op = "Nop"
		}

		result.WriteString(fmt.Sprintf("(%d-%d: %s %s %s),", log.Index, log.Term, op, log.Key, log.Value))
	}

	result.WriteString("]")

	return result.String()
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
