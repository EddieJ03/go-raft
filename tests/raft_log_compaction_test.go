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

func TestLogCompactionFollowerRestartsButNotTooFarBehind(t *testing.T) {
	fmt.Println("Running:", t.Name())

	// set small compaction threshold for testing
	os.Setenv("RAFT_COMPACTION_THRESHOLD", "5")
	os.Setenv("RAFT_HEARTBEAT_INTERVAL", "1000")
	os.Setenv("RAFT_ELECTION_TIMEOUT_MIN", "2000")
	os.Setenv("RAFT_ELECTION_TIMEOUT_MAX", "3000")

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

	operations := []struct {
		op    int32
		key   string
		value string
	}{
		{raft.Set, "key1", "value1"},
		{raft.Set, "key2", "value2"},
		{raft.Set, "key3", "value3"},
		{raft.Set, "key4", "value4"},
		{raft.Set, "key5", "value5"},
		{raft.Delete, "key2", ""},
		{raft.Set, "key6", "value6"},
	}

	for _, op := range operations {
		_, err := nodes[leaderID].ClientRequest(op.op, op.key, op.value)
		if err != nil {
			t.Fatalf("Failed to submit client request: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	time.Sleep(2 * time.Second)

	expectedState := map[string]string{
		"key1": "value1",
		"key3": "value3",
		"key4": "value4",
		"key5": "value5",
		"key6": "value6",
	}

	for i, node := range nodes {
		if node == nil {
			continue
		}

		sm := getNodeStateMachine(node)
		if !mapsEqual(sm, expectedState) {
			t.Fatalf("Node %d state machine not as expected\nGot: %v\nWant: %v", i, sm, expectedState)
		}

		node.Mu.Lock()
		if node.Snapshot == nil {
			t.Fatalf("Node %d did not create a snapshot", i)
		}

		logSize := len(node.Logs)

		if logSize >= 5 {
			t.Fatalf("Node %d log not compacted. Size: %d", i, logSize)
		}

		node.Mu.Unlock()
	}

	followerID := (leaderID + 1) % 3
	close(shutdowns[followerID])
	nodes[followerID] = nil
	time.Sleep(1 * time.Second)

	_, err := nodes[leaderID].ClientRequest(raft.Set, "key7", "value7")
	if err != nil {
		t.Fatalf("Failed to submit client request: %v", err)
	}

	shutdowns[followerID] = make(chan struct{})
	nodes[followerID] = raft.NewRaftNode(int32(followerID), peers, shutdowns[followerID], filepath.Join("test_logs", fmt.Sprintf("raft_node_%d", int32(followerID))))
	go utils.ServeBackend(int32(followerID), peers, shutdowns[followerID], nodes[followerID])

	// wait for follower catch-up
	time.Sleep(2 * time.Second)

	expectedState["key7"] = "value7"

	for i, node := range nodes {
		if node == nil {
			continue
		}

		sm := getNodeStateMachine(node)
		if !mapsEqual(sm, expectedState) {
			t.Fatalf("Node %d final state not as expected\nGot: %v\nWant: %v", i, sm, expectedState)
		}
	}

	for i := range 3 {
		if shutdowns[i] != nil {
			close(shutdowns[i])
		}
	}

	time.Sleep(1 * time.Second)
}

func TestLogCompactionFollowerIsFarBehind(t *testing.T) {
	fmt.Println("Running:", t.Name())

	// set small compaction threshold for testing
	os.Setenv("RAFT_COMPACTION_THRESHOLD", "5")
	os.Setenv("RAFT_HEARTBEAT_INTERVAL", "1000")
	os.Setenv("RAFT_ELECTION_TIMEOUT_MIN", "2000")
	os.Setenv("RAFT_ELECTION_TIMEOUT_MAX", "3000")

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

	fmt.Printf("Leader elected: %d\n", leaderID)

	operations := []struct {
		op    int32
		key   string
		value string
	}{
		{raft.Set, "key1", "value1"},
		{raft.Set, "key2", "value2"},
		{raft.Set, "key3", "value3"},
		{raft.Set, "key4", "value4"},
		{raft.Set, "key5", "value5"},
		{raft.Delete, "key2", ""},
		{raft.Set, "key6", "value6"},
	}

	for _, op := range operations {
		_, err := nodes[leaderID].ClientRequest(op.op, op.key, op.value)
		if err != nil {
			t.Fatalf("Failed to submit client request: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	time.Sleep(2 * time.Second)

	expectedState := map[string]string{
		"key1": "value1",
		"key3": "value3",
		"key4": "value4",
		"key5": "value5",
		"key6": "value6",
	}

	for i, node := range nodes {
		if node == nil {
			continue
		}

		sm := getNodeStateMachine(node)
		if !mapsEqual(sm, expectedState) {
			t.Fatalf("Node %d state machine not as expected\nGot: %v\nWant: %v", i, sm, expectedState)
		}

		node.Mu.Lock()
		if node.Snapshot == nil {
			t.Fatalf("Node %d did not create a snapshot", i)
		}

		logSize := len(node.Logs)

		if logSize >= 5 {
			t.Fatalf("Node %d log not compacted. Size: %d", i, logSize)
		}

		node.Mu.Unlock()
	}

	followerID := (leaderID + 1) % 3
	close(shutdowns[followerID])
	nodes[followerID] = nil
	time.Sleep(1 * time.Second)

	operations = []struct {
		op    int32
		key   string
		value string
	}{
		{raft.Set, "key7", "value7"},
		{raft.Set, "key8", "value8"},
		{raft.Set, "key9", "value9"},
		{raft.Set, "key10", "value10"},
		{raft.Set, "key11", "value11"},
		{raft.Set, "key12", "value12"},
	}

	for _, op := range operations {
		_, err := nodes[leaderID].ClientRequest(op.op, op.key, op.value)
		if err != nil {
			t.Fatalf("Failed to submit client request: %v", err)
		}

		time.Sleep(20 * time.Millisecond)
	}

	time.Sleep(2 * time.Second)

	shutdowns[followerID] = make(chan struct{})
	nodes[followerID] = raft.NewRaftNode(int32(followerID), peers, shutdowns[followerID], filepath.Join("test_logs", fmt.Sprintf("raft_node_%d", int32(followerID))))
	go utils.ServeBackend(int32(followerID), peers, shutdowns[followerID], nodes[followerID])

	// wait for follower catch-up
	time.Sleep(3 * time.Second)

	expectedState["key7"] = "value7"
	expectedState["key8"] = "value8"
	expectedState["key9"] = "value9"
	expectedState["key10"] = "value10"
	expectedState["key11"] = "value11"
	expectedState["key12"] = "value12"

	for i, node := range nodes {
		if node == nil {
			continue
		}

		sm := getNodeStateMachine(node)
		if !mapsEqual(sm, expectedState) {
			t.Fatalf("Node %d final state not as expected\nGot: %v\nWant: %v", i, sm, expectedState)
		}
	}

	for i := range 3 {
		if shutdowns[i] != nil {
			close(shutdowns[i])
		}
	}

	time.Sleep(1 * time.Second)
}

// follower loses all file logs and shutdown
func TestLogCompactionLostPersistantData(t *testing.T) {
	// set small compaction threshold for testing
	os.Setenv("RAFT_COMPACTION_THRESHOLD", "5")
	os.Setenv("RAFT_HEARTBEAT_INTERVAL", "1000")
	os.Setenv("RAFT_ELECTION_TIMEOUT_MIN", "2000")
	os.Setenv("RAFT_ELECTION_TIMEOUT_MAX", "3000")

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

	fmt.Printf("Leader elected: %d\n", leaderID)

	operations := []struct {
		op    int32
		key   string
		value string
	}{
		{raft.Set, "key1", "value1"},
		{raft.Set, "key2", "value2"},
		{raft.Set, "key3", "value3"},
		{raft.Set, "key4", "value4"},
		{raft.Set, "key5", "value5"},
		{raft.Delete, "key2", ""},
		{raft.Set, "key6", "value6"},
	}

	for _, op := range operations {
		_, err := nodes[leaderID].ClientRequest(op.op, op.key, op.value)
		if err != nil {
			t.Fatalf("Failed to submit client request: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	time.Sleep(5 * time.Second)

	expectedState := map[string]string{
		"key1": "value1",
		"key3": "value3",
		"key4": "value4",
		"key5": "value5",
		"key6": "value6",
	}

	for i, node := range nodes {
		if node == nil {
			continue
		}

		sm := getNodeStateMachine(node)
		if !mapsEqual(sm, expectedState) {
			t.Fatalf("Node %d state machine not as expected\nGot: %v\nWant: %v", i, sm, expectedState)
		}

		node.Mu.Lock()
		if node.Snapshot == nil {
			t.Fatalf("Node %d did not create a snapshot", i)
		}

		logSize := len(node.Logs)

		if logSize >= 5 {
			t.Fatalf("Node %d log not compacted. Size: %d", i, logSize)
		}

		node.Mu.Unlock()
	}

	followerID := (leaderID + 1) % 3
	close(shutdowns[followerID])
	// delete the file for the follower
	logPath := filepath.Join("test_logs", fmt.Sprintf("raft_node_%d", int32(followerID)))
	os.RemoveAll(logPath)
	// verify the files are deleted
	if _, err := os.Stat(logPath); !os.IsNotExist(err) {
		t.Fatalf("logs for follower %d to be deleted, but they still exist", followerID)
	}
	nodes[followerID] = nil
	time.Sleep(2 * time.Second)

	operations = []struct {
		op    int32
		key   string
		value string
	}{
		{raft.Set, "key7", "value7"},
		{raft.Set, "key8", "value8"},
		{raft.Set, "key9", "value9"},
		{raft.Set, "key10", "value10"},
		{raft.Set, "key11", "value11"},
		{raft.Set, "key12", "value12"},
	}

	for _, op := range operations {
		_, err := nodes[leaderID].ClientRequest(op.op, op.key, op.value)
		if err != nil {
			t.Fatalf("Failed to submit client request: %v", err)
		}

		time.Sleep(20 * time.Millisecond)
	}

	time.Sleep(3 * time.Second)

	shutdowns[followerID] = make(chan struct{})
	nodes[followerID] = raft.NewRaftNode(int32(followerID), peers, shutdowns[followerID], filepath.Join("test_logs", fmt.Sprintf("raft_node_%d", int32(followerID))))
	go utils.ServeBackend(int32(followerID), peers, shutdowns[followerID], nodes[followerID])

	// wait for follower catch-up
	time.Sleep(3 * time.Second)

	expectedState["key7"] = "value7"
	expectedState["key8"] = "value8"
	expectedState["key9"] = "value9"
	expectedState["key10"] = "value10"
	expectedState["key11"] = "value11"
	expectedState["key12"] = "value12"

	for i, node := range nodes {
		if node == nil {
			continue
		}

		sm := getNodeStateMachine(node)
		if !mapsEqual(sm, expectedState) {
			t.Fatalf("Node %d final state not as expected\nGot: %v\nWant: %v", i, sm, expectedState)
		}
	}

	for i := range 3 {
		if shutdowns[i] != nil {
			close(shutdowns[i])
		}
	}

	time.Sleep(1 * time.Second)
}

// logs can be snapshot multiple times and restored
func TestLogCompactionManySnapshots(t *testing.T) {
	// set small compaction threshold for testing
	os.Setenv("RAFT_COMPACTION_THRESHOLD", "5")
	os.Setenv("RAFT_HEARTBEAT_INTERVAL", "1000")
	os.Setenv("RAFT_ELECTION_TIMEOUT_MIN", "2000")
	os.Setenv("RAFT_ELECTION_TIMEOUT_MAX", "3000")

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

	fmt.Printf("Leader elected: %d\n", leaderID)

	for i := range 25 {
		_, err := nodes[leaderID].ClientRequest(raft.Set, fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i))
		if err != nil {
			t.Fatalf("Failed to submit client request: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	time.Sleep(10 * time.Second)

	expectedState := make(map[string]string)
	for i := range 25 {
		expectedState[fmt.Sprintf("key%d", i)] = fmt.Sprintf("value%d", i)
	}
	for i, node := range nodes {
		if node == nil {
			continue
		}

		sm := getNodeStateMachine(node)
		if !mapsEqual(sm, expectedState) {
			t.Fatalf("Node %d state machine not as expected\nGot: %v\nWant: %v", i, sm, expectedState)
		}

		node.Mu.Lock()
		if node.Snapshot == nil {
			t.Fatalf("Node %d did not create a snapshot", i)
		}

		logSize := len(node.Logs)

		if logSize >= 5 {
			t.Fatalf("Node %d log not compacted. Size: %d", i, logSize)
		}

		node.Mu.Unlock()
	}

	followerID := (leaderID + 1) % 3
	close(shutdowns[followerID])
	nodes[followerID] = nil
	time.Sleep(1 * time.Second)
	shutdowns[followerID] = make(chan struct{})
	nodes[followerID] = raft.NewRaftNode(int32(followerID), peers, shutdowns[followerID], filepath.Join("test_logs", fmt.Sprintf("raft_node_%d", int32(followerID))))
	go utils.ServeBackend(int32(followerID), peers, shutdowns[followerID], nodes[followerID])

	// wait for follower catch-up
	time.Sleep(3 * time.Second)

	for i, node := range nodes {
		if node == nil {
			continue
		}

		sm := getNodeStateMachine(node)
		if !mapsEqual(sm, expectedState) {
			t.Fatalf("Node %d final state not as expected\nGot: %v\nWant: %v", i, sm, expectedState)
		}
	}

	for i := range 3 {
		if shutdowns[i] != nil {
			close(shutdowns[i])
		}
	}

	time.Sleep(1 * time.Second)
}
