package raft

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
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
	defaultHeartbeatInterval  = 1000 // default heartbeat every second
	defaultElectionTimeoutMin = 1500 // miniMum election timeout in milliseconds
	defaultElectionTimeoutMax = 3000 // maxiMum election timeout in milliseconds
	DefaultRPCTimeout         = 1    // default RPC timeout in seconds
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

type RaftNode struct {
	pb.UnimplementedRaftServer

	Mu            sync.Mutex
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
	commitIndex   int32
	lastApplied   int32
	stateMachine  map[string]string
	leaderId      int32
	// used only by leader
	leaderNextIndex  map[int32]int32
	leaderMatchIndex map[int32]int32
}

func NewRaftNode(Id int32, peers map[int32]string, Shutdown chan struct{}) *RaftNode {
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
		Id:               Id,
		State:            Follower,
		CurrentTerm:      0,
		VotedFor:         -1,
		peers:            peers,
		grpcClients:      clients,
		electionReset:    time.Now(),
		clientConns:      clientConns,
		Shutdown:         Shutdown,
		Logs:             []Log{{Term: 0, Op: NoOp, Key: "", Value: "", Index: 0}},
		commitIndex:      0,
		lastApplied:      0,
		stateMachine:     make(map[string]string),
		leaderNextIndex:  make(map[int32]int32),
		leaderMatchIndex: make(map[int32]int32),
		leaderId:         -1,
	}

	go rn.runElectionTimer()
	return rn
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

	// 	If the logs have last entries with different terms, then
	//  the log with the later term is more up-to-date. If the logs
	//  end with the same term, then whichever log is longer is
	//  more up-to-date.
	candidateUpToDate := rn.Logs[len(rn.Logs)-1].Term < req.LastLogTerm ||
		(rn.Logs[len(rn.Logs)-1].Term == req.LastLogTerm && len(rn.Logs)-1 <= int(req.LastLogIndex))

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

	if req.Term > rn.CurrentTerm {
		rn.CurrentTerm = req.Term
		rn.VotedFor = -1
		rn.State = Follower
	}

	rn.leaderId = req.LeaderId

	if int(req.PrevLogIndex) >= len(rn.Logs) || (req.PrevLogTerm != rn.Logs[req.PrevLogIndex].Term) {
		return &pb.AppendEntriesResponse{Term: rn.CurrentTerm, Success: false}, nil
	}
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

	rn.Logs = append(rn.Logs, reqEntries...)

	if req.LeaderCommit > rn.commitIndex {
		rn.commitIndex = min(req.LeaderCommit, int32(len(rn.Logs)-1))
	}

	rn.electionReset = time.Now()
	return &pb.AppendEntriesResponse{Term: rn.CurrentTerm, Success: true}, nil
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
				LastLogIndex: int32(len(rn.Logs) - 1),
				LastLogTerm:  rn.Logs[len(rn.Logs)-1].Term,
			}

			resp, err := client.RequestVote(ctx, req)
			if err != nil {
				// log.Printf("can't request vote from %d: %v", pid, err)
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
	go rn.sendHeartbeats()
	rn.Mu.Lock()
	for pid := range rn.peers {
		if pid == rn.Id {
			continue
		}
		rn.leaderNextIndex[pid] = int32(len(rn.Logs))
		rn.leaderMatchIndex[pid] = 0
	}
	rn.Mu.Unlock()
}

func (rn *RaftNode) ClientRequest(op int32, key, value string) (string, error) {
	rn.Mu.Lock()
	defer rn.Mu.Unlock()

	if rn.State != Leader {
		return "", errors.New("not the leader")
	}

	entry := Log{
		Term:  rn.CurrentTerm,
		Op:    op,
		Key:   key,
		Value: value,
		Index: int32(len(rn.Logs)),
	}

	rn.Logs = append(rn.Logs, entry)
	rn.commitIndex = int32(len(rn.Logs) - 1)

	return fmt.Sprintf("Client request received: %v", entry), nil
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

			term := rn.CurrentTerm
			rn.Mu.Unlock()

			for pid, client := range rn.grpcClients {
				go func(pid int32, client pb.RaftClient) {
					ctx, cancel := context.WithTimeout(context.Background(), DefaultRPCTimeout*time.Second)
					defer cancel()
					_, _ = client.AppendEntries(ctx, &pb.AppendEntriesRequest{Term: term})
				}(pid, client)
			}
		}
	}
}

func (rn *RaftNode) CleanResources() {
	log.Printf("cleaning resources for node %d", rn.Id)

	for pid, conn := range rn.clientConns {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection to %d: %v", pid, err)
		}
	}
}
