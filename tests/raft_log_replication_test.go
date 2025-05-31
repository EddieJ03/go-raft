package raft_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	raft "github.com/EddieJ03/223b-raft/raft"
	utils "github.com/EddieJ03/223b-raft/utils"
)

// getNodeLogs retrieves logs from a node
func getNodeLogs(node *raft.RaftNode) []raft.Log {
	if node == nil {
		return nil
	}
	
	node.Mu.Lock()
	defer node.Mu.Unlock()
	
	logs := make([]raft.Log, len(node.Logs))
	for i, log := range node.Logs {
		logs[i] = raft.Log{
			Term:  log.Term,
			Op:    log.Op,
			Key:   log.Key,
			Value: log.Value,
			Index: log.Index,
		}
	}
	return logs
}

func getNodeCommitIndex(node *raft.RaftNode) int32 {
	if node == nil {
		return -1
	}
	
	node.Mu.Lock()
	defer node.Mu.Unlock()
	return node.CommitIndex
}

func getNodeStateMachine(node *raft.RaftNode) map[string]string {
	if node == nil {
		return nil
	}
	
	node.Mu.Lock()
	defer node.Mu.Unlock()
	
	sm := make(map[string]string)
	for k, v := range node.StateMachine {
		sm[k] = v
	}
	return sm
}

func waitForCommitIndex(nodes []*raft.RaftNode, expectedCommitIndex int32, timeout time.Duration) bool {
	deadline := time.After(timeout)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	
	for {
		select {
		case <-deadline:
			return false
		case <-ticker.C:
			allMatch := true
			
			for _, node := range nodes {
				if node == nil {
					continue
				}
				
				commitIndex := getNodeCommitIndex(node)
				if commitIndex < expectedCommitIndex {
					allMatch = false
					break
				}
			}
			
			if allMatch {
				return true
			}
		}
	}
}

func waitForLogReplication(nodes []*raft.RaftNode, expectedLength int, timeout time.Duration) bool {
	deadline := time.After(timeout)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	
	for {
		select {
		case <-deadline:
			return false
		case <-ticker.C:
			allMatch := true
			var referenceLogs []raft.Log
			
			for _, node := range nodes {
				if node != nil {
					referenceLogs = getNodeLogs(node)
					break
				}
			}
			
			if len(referenceLogs) < expectedLength {
				continue
			}
			
			for _, node := range nodes {
				if node == nil {
					continue
				}
				
				nodeLogs := getNodeLogs(node)
				if len(nodeLogs) < expectedLength {
					allMatch = false
					break
				}
				
				for i := 0; i < expectedLength; i++ {
					if nodeLogs[i] != referenceLogs[i] {
						allMatch = false
						break
					}
				}
				
				if !allMatch {
					break
				}
			}
			
			if allMatch {
				return true
			}
		}
	}
}

func TestOneOperationLogReplication(t *testing.T) {
	fmt.Println("Running:", t.Name())
	
	os.Setenv("RAFT_HEARTBEAT_INTERVAL", "500")
	os.Setenv("RAFT_ELECTION_TIMEOUT_MIN", "1000")
	os.Setenv("RAFT_ELECTION_TIMEOUT_MAX", "2000")
	
	peers := map[int32]string{
		0: "localhost:50051",
		1: "localhost:50052",
		2: "localhost:50053",
	}
	
	nodes := make([]*raft.RaftNode, 3)
	shutdowns := make([]chan struct{}, 3)
	
	// Start all nodes
	for i := range 3 {
		shutdowns[i] = make(chan struct{})
		nodes[i] = raft.NewRaftNode(int32(i), peers, shutdowns[i])
		go utils.ServeBackend(int32(i), peers, shutdowns[i], nodes[i])
	}
	
	// wait for leader election
	statusChan := make(chan struct{})
	statusUpdates := checkAllStatus(nodes, 100*time.Millisecond, statusChan)

	var leaderID int
	if leaderID = waitForStableLeader(statusUpdates, 10*time.Second); leaderID == -1 {
		t.Fatal("FAILURE: could not achieve stable leadership in 10 seconds")
	}
	
	fmt.Printf("Leader elected: %d\n", leaderID)
	
	_, err := nodes[leaderID].ClientRequest(raft.Set, "key1", "value1")
	if err != nil {
		t.Fatalf("Failed to submit client request: %v", err)
	}
	
	// expecting 2 entries: initial NoOp + the Set
	if !waitForLogReplication(nodes, 2, 5*time.Second) {
		t.Fatal("FAILURE: log replication could not be achieved in 5 seconds")
	}
	
	if !waitForCommitIndex(nodes, 1, 5*time.Second) {
		t.Fatal("FAILURE: right commit index could not be achieved in 5 seconds")
	}
	
	for i, node := range nodes {
		logs := getNodeLogs(node)
		if len(logs) != 2 {
			t.Errorf("Node %d has %d logs, expected 2", i, len(logs))
		}
		
		if logs[1].Op != raft.Set || logs[1].Key != "key1" || logs[1].Value != "value1" {
			t.Errorf("Node %d has incorrect log entry: %+v", i, logs[1])
		}
		
		// also check state machine
		sm := getNodeStateMachine(node)
		if sm["key1"] != "value1" {
			t.Errorf("Node %d state machine incorrect: expected key1=value1, got %v", i, sm)
		}
	}
	
	for i := range 3 {
		close(shutdowns[i])
	}
}