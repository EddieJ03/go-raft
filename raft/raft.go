package raft

import (
	"fmt"
	"log"
	"os"
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

func (rn *RaftNode) CleanResources() {
	log.Printf("cleaning resources for node %d", rn.Id)

	for pid, conn := range rn.clientConns {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection to %d: %v", pid, err)
		}
	}
}
