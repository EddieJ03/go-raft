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
				if commitIndex != expectedCommitIndex {
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

			if len(referenceLogs) != expectedLength {
				continue
			}

			for _, node := range nodes {
				if node == nil {
					continue
				}

				nodeLogs := getNodeLogs(node)
				if len(nodeLogs) != expectedLength {
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
	defer close(statusChan)
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
	time.Sleep(1*time.Second)
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
	defer close(statusChan)
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
	time.Sleep(1*time.Second)
}

func TestLogReplicationSingleFollowerFailureThenRecovery(t *testing.T) {
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
	defer close(statusChan)
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

	if !waitForCommitIndex(nodes, 3, 5*time.Second) {
		t.Fatal("FAILURE: could not achieve right commit index of 3 within 5 seconds")
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
	defer close(statusChan)
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
	time.Sleep(1 * time.Second)

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
	time.Sleep(1*time.Second)
}

func TestLogReplicationMultipleFollowerFailAndVariableRecover(t *testing.T) {
	runLogReplicationFailuresPartialRecoveryTest(t, 5, 2, 1) // 5 servers, 2 crash, 1 recovers
	time.Sleep(1*time.Second)
	runLogReplicationFailuresPartialRecoveryTest(t, 5, 2, 2) // 5 servers, 2 crash, 2 recovers
	time.Sleep(1*time.Second)
	runLogReplicationFailuresPartialRecoveryTest(t, 5, 2, 0) // 5 servers, 2 crash, 0 recovers
	time.Sleep(1*time.Second)
}

func TestLogReplicationLeaderFailureThenRecovery(t *testing.T) {
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
	defer close(statusChan)
	statusUpdates := checkAllStatus(nodes, 100*time.Millisecond, statusChan)

	var originalLeaderID int
	if originalLeaderID = waitForStableLeader(statusUpdates, 10*time.Second); originalLeaderID == -1 {
		t.Fatal("FAILURE: could not achieve stable leadership in 10 seconds")
	}
	fmt.Printf("Initial leader: %d\n", originalLeaderID)

	_, err := nodes[originalLeaderID].ClientRequest(raft.Set, "key1", "value1")
	if err != nil {
		t.Fatalf("Failed to submit client request: %v", err)
	}

	_, err = nodes[originalLeaderID].ClientRequest(raft.Set, "key2", "value2")
	if err != nil {
		t.Fatalf("Failed to submit client request: %v", err)
	}

	if !waitForLogReplication(nodes, 3, 5*time.Second) {
		t.Fatal("FAILURE: could not achieve log replication in 5 seconds up to length 3")
	}

	if !waitForCommitIndex(nodes, 2, 5*time.Second) {
		t.Fatal("FAILURE: could not achieve right commit index of 2 within 5 seconds")
	}

	// kill the current leader
	fmt.Printf("Killing leader %d\n", originalLeaderID)
	close(shutdowns[originalLeaderID])
	nodes[originalLeaderID] = nil
	time.Sleep(1 * time.Second) 

	var newLeaderID int
	if newLeaderID = waitForStableLeader(statusUpdates, 10*time.Second); newLeaderID == -1 {
		t.Fatal("FAILURE: could not elect new leader within 10 seconds after leader failure")
	}

	if newLeaderID == originalLeaderID {
		t.Fatal("FAILURE: new leader ID should be different from killed leader")
	}
	fmt.Printf("New leader elected: %d\n", newLeaderID)

	_, err = nodes[newLeaderID].ClientRequest(raft.Set, "key3", "value3")
	if err != nil {
		t.Fatalf("Failed to submit client request to new leader: %v", err)
	}

	_, err = nodes[newLeaderID].ClientRequest(raft.Set, "key4", "value4")
	if err != nil {
		t.Fatalf("Failed to submit second client request to new leader: %v", err)
	}

	// Wait for replication among remaining nodes
	if !waitForLogReplication(nodes, 5, 5*time.Second) {
		t.Fatal("FAILURE: could not achieve log replication in 5 seconds up to length 5 with new leader")
	}

	if !waitForCommitIndex(nodes, 4, 5*time.Second) {
		t.Fatal("FAILURE: could not achieve right commit index of 4 within 5 seconds with new leader")
	}

	// restart original leader
	shutdowns[originalLeaderID] = make(chan struct{})
	nodes[originalLeaderID] = raft.NewRaftNode(int32(originalLeaderID), peers, shutdowns[originalLeaderID], filepath.Join("test_logs", fmt.Sprintf("raft_node_%d", int32(originalLeaderID))))
	go utils.ServeBackend(int32(originalLeaderID), peers, shutdowns[originalLeaderID], nodes[originalLeaderID])
	fmt.Printf("Restarted original leader %d (now follower)\n", originalLeaderID)

	// Wait for the restarted node to catch up
	if !waitForLogReplication(nodes, 5, 5*time.Second) {
		t.Fatal("FAILURE: could not achieve log replication in 5 seconds up to length 5 after recovery")
	}

	if !waitForCommitIndex(nodes, 4, 5*time.Second) {
		t.Fatal("FAILURE: could not achieve right commit index of 4 within 5 seconds after recovery")
	}

	// Submit one more request to verify continued operation
	_, err = nodes[newLeaderID].ClientRequest(raft.Set, "key5", "value5")
	if err != nil {
		t.Fatalf("Failed to submit final client request: %v", err)
	}

	if !waitForLogReplication(nodes, 6, 5*time.Second) {
		t.Fatal("FAILURE: could not achieve final log replication in 5 seconds up to length 6")
	}

	if !waitForCommitIndex(nodes, 5, 5*time.Second) {
		t.Fatal("FAILURE: could not achieve commit index in 5 seconds up to index 5")
	}

	// Verify all nodes have consistent state
	expectedState := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
		"key4": "value4",
		"key5": "value5",
	}

	for i, node := range nodes {
		if node == nil {
			continue
		}
		sm := getNodeStateMachine(node)
		if !mapsEqual(sm, expectedState) {
			t.Fatalf("FAILURE: node %d state machine not as expected. Got: %v, Expected: %v", i, sm, expectedState)
		}
	}

	// Clean shutdown
	for i := range 3 {
		if shutdowns[i] != nil {
			close(shutdowns[i])
		}
	}
	
	time.Sleep(1*time.Second)
}

func TestLogReplicationLeaderFailureWithUncommittedThenRecovery(t *testing.T) {
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
	defer close(statusChan)
	statusUpdates := checkAllStatus(nodes, 100*time.Millisecond, statusChan)

	var originalLeaderID int
	if originalLeaderID = waitForStableLeader(statusUpdates, 10*time.Second); originalLeaderID == -1 {
		t.Fatal("FAILURE: could not achieve stable leadership in 10 seconds")
	}
	fmt.Printf("Initial leader: %d\n", originalLeaderID)

	_, err := nodes[originalLeaderID].ClientRequest(raft.Set, "key1", "value1")
	if err != nil {
		t.Fatalf("Failed to submit client request: %v", err)
	}

	_, err = nodes[originalLeaderID].ClientRequest(raft.Set, "key2", "value2")
	if err != nil {
		t.Fatalf("Failed to submit client request: %v", err)
	}

	if !waitForLogReplication(nodes, 3, 5*time.Second) {
		t.Fatal("FAILURE: could not achieve log replication in 5 seconds up to length 3")
	}

	if !waitForCommitIndex(nodes, 2, 5*time.Second) {
		t.Fatal("FAILURE: could not achieve right commit index of 2 within 5 seconds")
	}

	// kill the current leader
	fmt.Printf("Killing leader %d\n", originalLeaderID)
	close(shutdowns[originalLeaderID])

	// replace leaders PersistentState with uncommitted stuff
	nodes[originalLeaderID].Logs = append(nodes[originalLeaderID].Logs, []raft.Log{{
		Term:  nodes[originalLeaderID].CurrentTerm,
		Op:    raft.Set,
		Key:   "uncommitted key 1",
		Value: "uncommited value 1",
		Index: int32(len(nodes[originalLeaderID].Logs)),
	},{
		Term:  nodes[originalLeaderID].CurrentTerm,
		Op:    raft.Set,
		Key:   "uncommitted key 2",
		Value: "uncommited value 2",
		Index: int32(len(nodes[originalLeaderID].Logs)+1),
	}}...)

	// persist it
	nodes[originalLeaderID].WriteLogFile()

	nodes[originalLeaderID] = nil

	var newLeaderID int
	if newLeaderID = waitForStableLeader(statusUpdates, 10*time.Second); newLeaderID == -1 {
		t.Fatal("FAILURE: could not elect new leader within 10 seconds after leader failure")
	}

	if newLeaderID == originalLeaderID {
		t.Fatal("FAILURE: new leader ID should be different from killed leader")
	}
	fmt.Printf("New leader elected: %d\n", newLeaderID)

	_, err = nodes[newLeaderID].ClientRequest(raft.Set, "key3", "value3")
	if err != nil {
		t.Fatalf("Failed to submit client request to new leader: %v", err)
	}

	_, err = nodes[newLeaderID].ClientRequest(raft.Set, "key4", "value4")
	if err != nil {
		t.Fatalf("Failed to submit second client request to new leader: %v", err)
	}

	if !waitForLogReplication(nodes, 5, 5*time.Second) {
		t.Fatal("FAILURE: could not achieve log replication in 5 seconds up to length 5 with new leader")
	}

	if !waitForCommitIndex(nodes, 4, 5*time.Second) {
		t.Fatal("FAILURE: could not achieve right commit index of 4 within 5 seconds with new leader")
	}

	// restart former leader
	shutdowns[originalLeaderID] = make(chan struct{})
	nodes[originalLeaderID] = raft.NewRaftNode(int32(originalLeaderID), peers, shutdowns[originalLeaderID], filepath.Join("test_logs", fmt.Sprintf("raft_node_%d", int32(originalLeaderID))))
	go utils.ServeBackend(int32(originalLeaderID), peers, shutdowns[originalLeaderID], nodes[originalLeaderID])
	fmt.Printf("Restarted original leader %d (now follower)\n", originalLeaderID)

	if !waitForLogReplication(nodes, 5, 5*time.Second) {
		t.Fatal("FAILURE: could not achieve log replication in 5 seconds up to length 5 after recovery")
	}

	if !waitForCommitIndex(nodes, 4, 5*time.Second) {
		t.Fatal("FAILURE: could not achieve right commit index of 4 within 5 seconds after recovery")
	}

	_, err = nodes[newLeaderID].ClientRequest(raft.Set, "key5", "value5")
	if err != nil {
		t.Fatalf("Failed to submit final client request: %v", err)
	}

	if !waitForLogReplication(nodes, 6, 5*time.Second) {
		t.Fatal("FAILURE: could not achieve final log replication in 5 seconds up to length 6")
	}

	if !waitForCommitIndex(nodes, 5, 5*time.Second) {
		t.Fatal("FAILURE: could not achieve commit index in 5 seconds up to index 5")
	}

	expectedState := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
		"key4": "value4",
		"key5": "value5",
	}

	for i, node := range nodes {
		if node == nil {
			continue
		}
		sm := getNodeStateMachine(node)
		if !mapsEqual(sm, expectedState) {
			t.Fatalf("FAILURE: node %d state machine not as expected. Got: %v, Expected: %v", i, sm, expectedState)
		}
	}

	for i := range 3 {
		if shutdowns[i] != nil {
			close(shutdowns[i])
		}
	}
	time.Sleep(1*time.Second)
}

func runLogReplicationFailuresPartialRecoveryTest(t *testing.T, numServers, numCrash, numRecover int) {
	fmt.Printf("Running: %d servers, %d crash, %d recover\n", numServers, numCrash, numRecover)
	os.Setenv("RAFT_HEARTBEAT_INTERVAL", "500")
	os.Setenv("RAFT_ELECTION_TIMEOUT_MIN", "1000")
	os.Setenv("RAFT_ELECTION_TIMEOUT_MAX", "2000")

	defer utils.CleanLogs("test_logs")

	peers := make(map[int32]string)
	for i := 0; i < numServers; i++ {
		peers[int32(i)] = fmt.Sprintf("localhost:5%03d", 51+i)
	}

	nodes := make([]*raft.RaftNode, numServers)
	shutdowns := make([]chan struct{}, numServers)

	for i := 0; i < numServers; i++ {
		shutdowns[i] = make(chan struct{})
		nodes[i] = raft.NewRaftNode(int32(i), peers, shutdowns[i], filepath.Join("test_logs", fmt.Sprintf("raft_node_%d", int32(i))))
		go utils.ServeBackend(int32(i), peers, shutdowns[i], nodes[i])
	}

	statusChan := make(chan struct{})
	defer close(statusChan)
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

	if !waitForLogReplication(nodes, 2, 5*time.Second) {
		t.Fatal("FAILURE: could not achieve initial log replication in 5 seconds")
	}

	// Crash numCrash followers, but not the leader
	crashed := 0
	for i := 0; i < numServers && crashed < numCrash; i++ {
		if i != leaderID {
			close(shutdowns[i])
			nodes[i] = nil
			fmt.Printf("Killed follower %d\n", i)
			crashed++
		}
	}
	time.Sleep(1 * time.Second)

	// send more operations while some nodes are down
	operations := []struct {
		op    int32
		key   string
		value string
	}{
		{raft.Set, "key2", "value2"},
		{raft.Set, "key3", "value3"},
	}
	for _, op := range operations {
		_, err := nodes[leaderID].ClientRequest(op.op, op.key, op.value)
		if err != nil {
			t.Fatalf("Failed to submit client request: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	if !waitForLogReplication(nodes, 4, 5*time.Second) {
		t.Fatal("FAILURE: could not achieve log replication in 5 seconds with nodes down")
	}

	// recover numRecover of the crashed nodes
	recovered := 0
	for i := 0; i < numServers && recovered < numRecover; i++ {
		if nodes[i] == nil && i != leaderID {
			shutdowns[i] = make(chan struct{})
			nodes[i] = raft.NewRaftNode(int32(i), peers, shutdowns[i], filepath.Join("test_logs", fmt.Sprintf("raft_node_%d", int32(i))))
			go utils.ServeBackend(int32(i), peers, shutdowns[i], nodes[i])
			fmt.Printf("Restarted follower %d\n", i)
			recovered++
		}
	}

	if !waitForLogReplication(nodes, 4, 5*time.Second) {
		t.Fatal("FAILURE: could not achieve log replication in 5 seconds after partial recovery")
	}

	_, err = nodes[leaderID].ClientRequest(raft.Set, "key4", "value4")
	if err != nil {
		t.Fatalf("Failed to submit client request: %v", err)
	}

	if !waitForLogReplication(nodes, 5, 5*time.Second) {
		t.Fatal("FAILURE: could not achieve log replication in 5 seconds after final operation")
	}

	if !waitForCommitIndex(nodes, 4, 5*time.Second) {
		t.Fatal("FAILURE: right commit index could not be achieved in 5 seconds after final operation")
	}

	expectedState := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
		"key4": "value4",
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

	for i := 0; i < numServers; i++ {
		if nodes[i] != nil {
			close(shutdowns[i])
		}
	}
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
