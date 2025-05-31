package raft_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	raft "github.com/EddieJ03/223b-raft/raft"
	utils "github.com/EddieJ03/223b-raft/utils"
)

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
				if commitIndex < expectedCommitIndex || commitIndex > expectedCommitIndex {
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

func TestOneOperationLogReplicationNoFailures(t *testing.T) {
	fmt.Println("Running:", t.Name())
	
	os.Setenv("RAFT_HEARTBEAT_INTERVAL", "500")
	os.Setenv("RAFT_ELECTION_TIMEOUT_MIN", "1000")
	os.Setenv("RAFT_ELECTION_TIMEOUT_MAX", "2000")

	defer utils.CleanLogs("test_logs")
	
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
		nodes[i] = raft.NewRaftNode(int32(i), peers, shutdowns[i], filepath.Join("test_logs", fmt.Sprintf("raft_node_%d", int32(i))))
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
	
	expectedState := map[string]string{
		"key1": "value1",
	}
	
	for i, node := range nodes {
		sm := getNodeStateMachine(node)
		
		if !mapsEqual(sm, expectedState) {
			t.Fatalf("FAILURE: node %d map not expected", i)
		}
	}
	
	for i := range 3 {
		close(shutdowns[i])
	}
}

func TestMultipleOperationsLogReplicationNoFailures(t *testing.T) {
	fmt.Println("Running:", t.Name())
	
	os.Setenv("RAFT_HEARTBEAT_INTERVAL", "500")
	os.Setenv("RAFT_ELECTION_TIMEOUT_MIN", "1000")
	os.Setenv("RAFT_ELECTION_TIMEOUT_MAX", "2000")

	defer utils.CleanLogs("test_logs")
	
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
		nodes[i] = raft.NewRaftNode(int32(i), peers, shutdowns[i], filepath.Join("test_logs", fmt.Sprintf("raft_node_%d", int32(i))))
		go utils.ServeBackend(int32(i), peers, shutdowns[i], nodes[i])
	}
	
	statusChan := make(chan struct{})
	statusUpdates := checkAllStatus(nodes, 100*time.Millisecond, statusChan)

	var leaderID int
	if leaderID = waitForStableLeader(statusUpdates, 10*time.Second); leaderID == -1 {
		t.Fatal("FAILURE: could not achieve stable leadership in 10 seconds")
	}
	
	// spam multiple client requests
	requests := []struct {
		op    int32
		key   string
		value string
	}{
		{raft.Set, "key1", "value1"},
		{raft.Set, "key2", "value2"},
		{raft.Set, "key3", "value3"},
		{raft.Delete, "key2", ""},
		{raft.Set, "key4", "value4"},
	}
	
	for _, req := range requests {
		_, err := nodes[leaderID].ClientRequest(req.op, req.key, req.value)
		if err != nil {
			t.Fatalf("Failed to submit client request: %v", err)
		}
		time.Sleep(10 * time.Millisecond) 
	}
	
	if !waitForLogReplication(nodes, 6, 5*time.Second) {
		t.Fatal("FAILURE: log replication could not be achieved in 5 seconds")
	}
	
	if !waitForCommitIndex(nodes, 5, 5*time.Second) {
		t.Fatal("FAILURE: right commit index could not be achieved in 5 seconds")
	}
	
	expectedState := map[string]string{
		"key1": "value1",
		"key3": "value3",
		"key4": "value4",
	}
	
	for i, node := range nodes {
		sm := getNodeStateMachine(node)
		
		if !mapsEqual(sm, expectedState) {
			t.Fatalf("FAILURE: node %d map not expected", i)
		}
	}
	
	for i := range 3 {
		close(shutdowns[i])
	}
}

func TestLogReplicationFollowerFailureThenRecovery(t *testing.T) {
	fmt.Println("Running:", t.Name())
	
	os.Setenv("RAFT_HEARTBEAT_INTERVAL", "500")
	os.Setenv("RAFT_ELECTION_TIMEOUT_MIN", "1000")
	os.Setenv("RAFT_ELECTION_TIMEOUT_MAX", "2000")

	defer utils.CleanLogs("test_logs")
	
	peers := map[int32]string{
		0: "localhost:50051",
		1: "localhost:50052",
		2: "localhost:50053",
	}
	
	nodes := make([]*raft.RaftNode, 3)
	shutdowns := make([]chan struct{}, 3)
	
	for i := range 3 {
		shutdowns[i] = make(chan struct{})
		nodes[i] = raft.NewRaftNode(int32(i), peers, shutdowns[i], filepath.Join("test_logs", fmt.Sprintf("raft_node_%d", int32(i))))
		go utils.ServeBackend(int32(i), peers, shutdowns[i], nodes[i])
	}
	
	statusChan := make(chan struct{})
	statusUpdates := checkAllStatus(nodes, 100*time.Millisecond, statusChan)

	var leaderID int
	if leaderID = waitForStableLeader(statusUpdates, 10*time.Second); leaderID == -1 {
		t.Fatal("FAILURE: could not achieve stable leadership in 10 seconds")
	}
	
	_, err := nodes[leaderID].ClientRequest(raft.Set, "key1", "value1")
	if err != nil {
		t.Fatalf("Failed to submit client request: %v", err)
	}
	
	// Wait for initial replication
	if !waitForLogReplication(nodes, 2, 5*time.Second) {
		t.Fatal("FAILURE: could not achieve log replication in 5 seconds up to length 2")
	}
	
	// kill a follower
	followerID := -1
	for i := range 3 {
		if i != leaderID {
			followerID = i
			break
		}
	}
	
	close(shutdowns[followerID])
	nodes[followerID] = nil
	fmt.Printf("Killed follower %d\n", followerID)
	time.Sleep(1 * time.Second) // sleep to further ensure killed node is cleaned
	
	_, err = nodes[leaderID].ClientRequest(raft.Set, "key2", "value2")
	if err != nil {
		t.Fatalf("Failed to submit client request: %v", err)
	}
	
	_, err = nodes[leaderID].ClientRequest(raft.Set, "key3", "value3")
	if err != nil {
		t.Fatalf("Failed to submit client request: %v", err)
	}
	
	if !waitForLogReplication(nodes, 4, 5*time.Second) {
		t.Fatal("FAILURE: could not achieve log replication in 5 seconds up to length 4")
	}
	
	// restart
	shutdowns[followerID] = make(chan struct{})
	nodes[followerID] = raft.NewRaftNode(int32(followerID), peers, shutdowns[followerID], filepath.Join("test_logs", fmt.Sprintf("raft_node_%d", int32(followerID))))
	go utils.ServeBackend(int32(followerID), peers, shutdowns[followerID], nodes[followerID])
	fmt.Printf("Restarted follower %d\n", followerID)
	
	if !waitForLogReplication(nodes, 4, 5*time.Second) {
		t.Fatal("FAILURE: could not achieve log replication in 5 seconds up to length 4 after recovery")
	}
	
	// Verify state machines are consistent
	expectedState := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}
	
	for i, node := range nodes {
		sm := getNodeStateMachine(node)
		if !mapsEqual(sm, expectedState) {
			t.Fatalf("FAILURE: node %d map not expected", i)
		}
	}
	
	for i := range 3 {
		close(shutdowns[i])
	}
}

func TestLogReplicationMinorityAlive(t *testing.T) {
	fmt.Println("Running:", t.Name())
	
	os.Setenv("RAFT_HEARTBEAT_INTERVAL", "500")
	os.Setenv("RAFT_ELECTION_TIMEOUT_MIN", "1000")
	os.Setenv("RAFT_ELECTION_TIMEOUT_MAX", "2000")

	defer utils.CleanLogs("test_logs")
	
	peers := map[int32]string{
		0: "localhost:50051",
		1: "localhost:50052",
		2: "localhost:50053",
	}

	nodes := make([]*raft.RaftNode, 3)
	shutdowns := make([]chan struct{}, 3)
	
	for i := range 3 {
		shutdowns[i] = make(chan struct{})
		nodes[i] = raft.NewRaftNode(int32(i), peers, shutdowns[i], filepath.Join("test_logs", fmt.Sprintf("raft_node_%d", int32(i))))
		go utils.ServeBackend(int32(i), peers, shutdowns[i], nodes[i])
	}
	
	statusChan := make(chan struct{})
	statusUpdates := checkAllStatus(nodes, 100*time.Millisecond, statusChan)

	var leaderID int
	if leaderID = waitForStableLeader(statusUpdates, 10*time.Second); leaderID == -1 {
		t.Fatal("FAILURE: could not achieve stable leadership in 10 seconds")
	}

	_, err := nodes[leaderID].ClientRequest(raft.Set, "key1", "value1")
	if err != nil {
		t.Fatalf("Failed to submit client request: %v", err)
	}
	
	// Wait for initial replication
	if !waitForLogReplication(nodes, 2, 5*time.Second) {
		t.Fatal("FAILURE: could not achieve log replication in 5 seconds up to length 2")
	}
	
	// kill all followers
	for i := range 3 {
		if i != leaderID {
			close(shutdowns[i])
			nodes[i] = nil
		}
	}

	// sleep to make sure appropriate resources are cleaned
	time.Sleep(1*time.Second)

	_, err = nodes[leaderID].ClientRequest(raft.Set, "key2", "value2")
	if err != nil {
		t.Fatalf("Failed to submit client request: %v", err)
	}

	if waitForCommitIndex(nodes, 2, 15*time.Second) {
		t.Fatal("FAILURE: right commit index should not be achieved in 15 seconds")
	}
	
	// key2 and value2 should NOT be applied
	expectedState := map[string]string{
		"key1": "value1",
	}
	
	for i, node := range nodes {
		if node == nil {
			continue
		}

		sm := getNodeStateMachine(node)
		
		if !mapsEqual(sm, expectedState) {
			t.Fatalf("FAILURE: node %d map not expected", i)
		}
	}
	
	close(shutdowns[leaderID])
}

func mapsEqual(a, b map[string]string) bool {
    if len(a) != len(b) {
        return false
    }
    for key, valA := range a {
        valB, ok := b[key]
        if !ok || valA != valB {
            return false
        }
    }
    return true
}