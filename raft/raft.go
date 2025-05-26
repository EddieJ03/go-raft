package raft

import (
	"context"
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
	CandIdate State = 1
	Leader    State = 2
)

const (
	defaultHeartbeatInterval  = 1000 // default heartbeat every second
	defaultElectionTimeoutMin = 1500 // miniMum election timeout in milliseconds
	defaultElectionTimeoutMax = 3000 // maxiMum election timeout in milliseconds
	DefaultRPCTimeout		 = 1 // default RPC timeout in seconds
)

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
		Id:            Id,
		State:         Follower,
		CurrentTerm:   0,
		VotedFor:      -1,
		peers:         peers,
		grpcClients:   clients,
		electionReset: time.Now(),
		clientConns:   clientConns,
		Shutdown:      Shutdown,
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

	if rn.VotedFor == -1 || rn.VotedFor == req.CandidateId {
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
	rn.State = CandIdate
	rn.CurrentTerm++
	rn.VotedFor = rn.Id
	rn.VoteCount = 1
	rn.electionReset = time.Now()

	for pid, client := range rn.grpcClients {
		go func(pid int32, client pb.RaftClient) {
			ctx, cancel := context.WithTimeout(context.Background(), DefaultRPCTimeout*time.Second)
			defer cancel()

			req := &pb.RequestVoteRequest{
				Term:        rn.CurrentTerm,
				CandidateId: rn.Id,
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

			if rn.State != CandIdate || rn.CurrentTerm != req.Term { // request term becomes invalId
				return
			}

			if resp.VoteGranted {
				rn.VoteCount++

				if rn.VoteCount > len(rn.peers)/2 {
					rn.State = Leader
					log.Printf("Node %d became leader for term %d", rn.Id, rn.CurrentTerm)
					go rn.sendHeartbeats()
				}
			}
		}(pid, client)
	}
}

func (rn *RaftNode) sendHeartbeats() {
	heartbeatInterval := getHeartbeatInterval()

	for {
		select {
		case <-rn.Shutdown:
			log.Printf("Heartbeat stopped for node %d", rn.Id)
			return
		default:
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

			time.Sleep(heartbeatInterval)
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
