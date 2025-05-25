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
	Candidate State = 1
	Leader    State = 2
)

const (
	defaultHeartbeatInterval  = 1000 // default heartbeat every second
	defaultElectionTimeoutMin = 1500 // minimum election timeout in milliseconds
	defaultElectionTimeoutMax = 3000 // maximum election timeout in milliseconds
)

type RaftNode struct {
	pb.UnimplementedRaftServer

	mu            sync.Mutex
	id            int32
	state         State
	currentTerm   int32
	votedFor      int32
	peers         map[int32]string
	voteCount     int
	electionReset time.Time
	grpcClients   map[int32]pb.RaftClient
	clientConns   map[int32]*grpc.ClientConn
	shutdown      chan struct{}
}

func NewRaftNode(id int32, peers map[int32]string, shutdown chan struct{}) *RaftNode {
	clients := make(map[int32]pb.RaftClient)
	clientConns := make(map[int32]*grpc.ClientConn)

	for pid, addr := range peers {
		if pid == id {
			continue
		}

		conn, _ := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))

		clientConns[pid] = conn

		clients[pid] = pb.NewRaftClient(conn)
	}

	rn := &RaftNode{
		id:            id,
		state:         Follower,
		currentTerm:   0,
		votedFor:      -1,
		peers:         peers,
		grpcClients:   clients,
		electionReset: time.Now(),
		clientConns:   clientConns,
		shutdown:      shutdown,
	}

	go rn.runElectionTimer()
	return rn
}

func (rn *RaftNode) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if req.Term < rn.currentTerm {
		return &pb.RequestVoteResponse{Term: rn.currentTerm, VoteGranted: false}, nil
	}

	if req.Term > rn.currentTerm {
		rn.currentTerm = req.Term
		rn.votedFor = -1
		rn.state = Follower
	}

	if rn.votedFor == -1 || rn.votedFor == req.CandidateId {
		rn.votedFor = req.CandidateId
		rn.electionReset = time.Now()
		return &pb.RequestVoteResponse{Term: rn.currentTerm, VoteGranted: true}, nil
	}

	return &pb.RequestVoteResponse{Term: rn.currentTerm, VoteGranted: false}, nil
}

func (rn *RaftNode) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if req.Term < rn.currentTerm {
		return &pb.AppendEntriesResponse{Term: rn.currentTerm, Success: false}, nil
	}

	if req.Term > rn.currentTerm {
		rn.currentTerm = req.Term
		rn.votedFor = -1
		rn.state = Follower
	}

	rn.electionReset = time.Now()
	return &pb.AppendEntriesResponse{Term: rn.currentTerm, Success: true}, nil
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
			rn.mu.Lock()

			timeout := getElectionTimeout()
			if rn.state != Leader && time.Since(rn.electionReset) >= timeout {
				rn.startElection()
			}

			rn.mu.Unlock()
		case <-rn.shutdown:
			log.Printf("election timer stopped for node %d", rn.id)
			return
		}
	}
}

func (rn *RaftNode) startElection() {
	rn.state = Candidate
	rn.currentTerm++
	rn.votedFor = rn.id
	rn.voteCount = 1
	rn.electionReset = time.Now()

	for pid, client := range rn.grpcClients {
		go func(pid int32, client pb.RaftClient) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			req := &pb.RequestVoteRequest{
				Term:        rn.currentTerm,
				CandidateId: rn.id,
			}

			resp, err := client.RequestVote(ctx, req)
			if err != nil {
				// log.Printf("can't request vote from %d: %v", pid, err)
				return
			}

			rn.mu.Lock()
			defer rn.mu.Unlock()

			if resp.Term > rn.currentTerm {
				rn.currentTerm = resp.Term
				rn.state = Follower
				rn.votedFor = -1
				return
			}

			if rn.state != Candidate || rn.currentTerm != req.Term { // request term becomes invalid
				return
			}

			if resp.VoteGranted {
				rn.voteCount++

				if rn.voteCount > len(rn.peers)/2 {
					rn.state = Leader
					log.Printf("Node %d became leader for term %d", rn.id, rn.currentTerm)
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
		case <-rn.shutdown:
			log.Printf("Heartbeat stopped for node %d", rn.id)
			return
		default:
			rn.mu.Lock()
			if rn.state != Leader {
				rn.mu.Unlock()
				return
			}

			term := rn.currentTerm
			rn.mu.Unlock()

			for pid, client := range rn.grpcClients {
				go func(pid int32, client pb.RaftClient) {
					ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
					defer cancel()
					_, _ = client.AppendEntries(ctx, &pb.AppendEntriesRequest{Term: term})
				}(pid, client)
			}

			time.Sleep(heartbeatInterval)
		}
	}
}

func (rn *RaftNode) CleanResources() {
	log.Printf("cleaning resources for node %d", rn.id)

	for pid, conn := range rn.clientConns {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection to %d: %v", pid, err)
		}
	}
}
