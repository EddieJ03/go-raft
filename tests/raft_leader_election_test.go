package raft_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	raft "github.com/EddieJ03/223b-raft/raft"
	utils "github.com/EddieJ03/223b-raft/utils"
)

const STABILITY_VALUE int = 3

// state of a node for testing purposes
type TestNodeStatus struct {
	ID       int32
	State    raft.State
	Term     int32
	VotedFor int32
	IsLeader bool
}

// checkAllStatus periodically checks the status of all nodes and returns a channel that receives updates
func checkAllStatus(nodes []*raft.RaftNode, interval time.Duration, done chan struct{}) chan []TestNodeStatus {
	statusChan := make(chan []TestNodeStatus)

	go func() {
		defer close(statusChan)
		for {
			select {
			case <-done:
				return
			case <-time.After(interval):
				statuses := make([]TestNodeStatus, len(nodes))

				for i, node := range nodes {
					if node == nil {
						continue
					}

					// we need to lock the node to safely read its status
					node.Mu.Lock()

					statuses[i] = TestNodeStatus{
						ID:       node.Id,
						State:    node.State,
						Term:     node.CurrentTerm,
						VotedFor: node.VotedFor,
						IsLeader: node.State == raft.Leader,
					}

					node.Mu.Unlock()
				}

				select {
				case statusChan <- statuses:
				case <-done:
					return
				}
			}
		}
	}()

	return statusChan
}

/*
waitForStableLeader waits for the cluster to have a stable leader
returns id of the leader if found else returns -1 
*/
func waitForStableLeader(statusChan chan []TestNodeStatus, timeout time.Duration) int {
	deadline := time.After(timeout)
	stableCount := 0
	lastLeaderID := int32(-1)

	for {
		select {
		case <-deadline:
			return -1
		case statuses, ok := <-statusChan:
			if !ok { // somehow channel is closed or broken :(
				return -1
			}

			// count leaders
			leaderCount := 0
			var currentLeaderID int32 = -1
			for _, status := range statuses {
				if status.IsLeader {
					leaderCount++
					currentLeaderID = status.ID
				}
			}

			// make sure we have exactly one leader
			if leaderCount == 1 {
				if currentLeaderID == lastLeaderID {
					stableCount++

					// we are basically going to assume stability if the same leader is seen 3 times in a row
					if stableCount >= STABILITY_VALUE {
						return int(currentLeaderID)
					}
				} else {
					stableCount = 1
					lastLeaderID = currentLeaderID
				}
			} else {
				stableCount = 0
				lastLeaderID = -1
			}
		}
	}
}

// Test if leadership can be established within 10 seconds, no failures 
func TestLeaderElection(t *testing.T) {
	fmt.Println("Running:", t.Name())

	os.Setenv("RAFT_HEARTBEAT_INTERVAL", "500")
	os.Setenv("RAFT_ELECTION_TIMEOUT_MIN", "1000")
	os.Setenv("RAFT_ELECTION_TIMEOUT_MAX", "2000")

	// test node configuration
	peers := map[int32]string{
		1: "localhost:50051",
		2: "localhost:50052",
		3: "localhost:50053",
	}

	nodes := make([]*raft.RaftNode, 3)

	for i := 0; i < 3; i++ {
		shutdown := make(chan struct{})
		nodes[i] = raft.NewRaftNode(int32(i+1), peers, shutdown)
		go utils.ServeBackend(int32(i+1), peers, shutdown, nodes[i])
	}

	// start status checking
	statusChan := make(chan struct{})
	statusUpdates := checkAllStatus(nodes, 100*time.Millisecond, statusChan)

	// wait for stable leader
	if waitForStableLeader(statusUpdates, 10*time.Second) == -1 {
		t.Error("FAILURE: could not achieve stable leadership in 10 seconds")
	}

	close(statusChan)

	// close each backend
	for i := 0; i < 3; i++ {
		close(nodes[i].Shutdown)
	}
}

// Test if leadership can be established after leader fails
func TestLeaderFailElection(t *testing.T) {
	fmt.Println("Running:", t.Name())

	os.Setenv("RAFT_HEARTBEAT_INTERVAL", "500")
	os.Setenv("RAFT_ELECTION_TIMEOUT_MIN", "1000")
	os.Setenv("RAFT_ELECTION_TIMEOUT_MAX", "2000")

	// test node configuration
	peers := map[int32]string{
		0: "localhost:50071",
		1: "localhost:50072",
		2: "localhost:50073",
	}

	nodes := make([]*raft.RaftNode, 3)

	for i := range 3 {
		shutdown := make(chan struct{})
		nodes[i] = raft.NewRaftNode(int32(i), peers, shutdown)
		go utils.ServeBackend(int32(i), peers, shutdown, nodes[i])
	}

	// start status checking
	statusChan := make(chan struct{})
	statusUpdates := checkAllStatus(nodes, 100*time.Millisecond, statusChan)

	var previousLeader int
	if previousLeader = waitForStableLeader(statusUpdates, 10*time.Second); previousLeader == -1 {
		t.Fatal("FAILURE: could not achieve stable leadership in 10 seconds")
	}

	fmt.Printf("Previous leader: %d\n", previousLeader)

	// shutdown the previous leader
	close(nodes[previousLeader].Shutdown)

	// wait for shutdown
	time.Sleep(2 * raft.DefaultRPCTimeout * time.Second)

	newStatusChan := make(chan struct{})
	nodes[previousLeader] = nil
	newStatusUpdates := checkAllStatus(nodes, 100*time.Millisecond, newStatusChan)

	if leader := waitForStableLeader(newStatusUpdates, 10*time.Second); leader == -1 || leader == previousLeader {
		t.Errorf("FAILURE: no leader or previous leader is still active after shutdown %d", leader)
	}

	// close each backend server
	for i := range 3 {
		if i != previousLeader {
			close(nodes[i].Shutdown)
		}
	}
}

// Test if a node joining the cluster becomes a follower 
func TestJoinCluster(t *testing.T) {
	fmt.Println("Running:", t.Name())

	os.Setenv("RAFT_HEARTBEAT_INTERVAL", "500")
	os.Setenv("RAFT_ELECTION_TIMEOUT_MIN", "1000")
	os.Setenv("RAFT_ELECTION_TIMEOUT_MAX", "2000")

	peers := map[int32]string{
		0: "localhost:50061",
		1: "localhost:50062",
		2: "localhost:50063",
	}
	numNodes := len(peers)
	nodes := make([]*raft.RaftNode, numNodes)
	shutdownChans := make([]chan struct{}, numNodes)
	initialNodeCount := numNodes - 1 

	// leave one node out intentionally
	initialNodesSlice := make([]*raft.RaftNode, initialNodeCount)
	for i := range initialNodesSlice {
		id := int32(i)
		shutdownChans[i] = make(chan struct{})
		nodes[i] = raft.NewRaftNode(id, peers, shutdownChans[i])
		initialNodesSlice[i] = nodes[i]
		go utils.ServeBackend(id, peers, shutdownChans[i], nodes[i])
	}

	initialStatusDone := make(chan struct{})
	initialStatusUpdates := checkAllStatus(initialNodesSlice, 100*time.Millisecond, initialStatusDone)

	leaderID := waitForStableLeader(initialStatusUpdates, 15*time.Second)
	if leaderID == -1 {
		t.Error("FAILURE: Could not elect a leader in the initial cluster")
	}

	nodes[leaderID].Mu.Lock()
	initialLeaderTerm := nodes[leaderID].CurrentTerm
	nodes[leaderID].Mu.Unlock()
	fmt.Printf("Initial leader term: %d\n", initialLeaderTerm)

	close(initialStatusDone)

	// now we have the left out node join
	joiningNodeID := int32(numNodes - 1)
	fmt.Printf("Starting joining node: %d\n", joiningNodeID)
	shutdownChans[joiningNodeID] = make(chan struct{})
	nodes[joiningNodeID] = raft.NewRaftNode(joiningNodeID, peers, shutdownChans[joiningNodeID])
	go utils.ServeBackend(joiningNodeID, peers, shutdownChans[joiningNodeID], nodes[joiningNodeID])

	// wait for the joining node to start
	time.Sleep(2 * time.Second)

	// start status checking for all nodes
	allNodesStatusDone := make(chan struct{})
	allNodesStatusUpdates := checkAllStatus(nodes, 100*time.Millisecond, allNodesStatusDone)

	finalLeaderID := waitForStableLeader(allNodesStatusUpdates, 10*time.Second)
	if finalLeaderID == -1 {
		t.Fatalf("FAILURE: Cluster did not stabilize after node %d joined", joiningNodeID)
	}

	var joinedNodeStatus TestNodeStatus
	var finalLeaderStatus TestNodeStatus

	// check for consistent state
	checkTimeout := time.After(5 * time.Second)
CheckLoop:
	for {
		select {
		case <-checkTimeout:
			t.Fatalf("FAILURE: Timed out waiting for joined node %d to become Follower with correct term.", joiningNodeID)
		case statuses, ok := <-allNodesStatusUpdates:
			if !ok {
				t.Fatalf("FAILURE: Status channel closed unexpectedly")
			}
			for _, status := range statuses {
				if status.ID == joiningNodeID {
					joinedNodeStatus = status
				}
				if status.ID == int32(finalLeaderID) {
					finalLeaderStatus = status
				}
			}

			if joinedNodeStatus.ID == joiningNodeID && finalLeaderStatus.ID == int32(finalLeaderID) { 
				if joinedNodeStatus.State == raft.Follower && joinedNodeStatus.Term == finalLeaderStatus.Term {
					break CheckLoop
				}
			}
		}
	}

	fmt.Printf("Joined node %d status: State=%d, Term=%d\n", joiningNodeID, joinedNodeStatus.State, joinedNodeStatus.Term)
	fmt.Printf("Leader %d status: State=%d, Term=%d\n", finalLeaderID, finalLeaderStatus.State, finalLeaderStatus.Term)

	if joinedNodeStatus.State != raft.Follower {
		t.Errorf("FAILURE: Joined node %d is not a Follower. Actual state: %d", joiningNodeID, joinedNodeStatus.State)
	}
	if joinedNodeStatus.Term != finalLeaderStatus.Term {
		t.Errorf("FAILURE: Joined node %d term (%d) does not match leader's term (%d)",
			joiningNodeID, joinedNodeStatus.Term, finalLeaderStatus.Term)
	}
	if finalLeaderStatus.Term != initialLeaderTerm {
		t.Errorf("FAILURE: Final leader term (%d) is not equal to initial leader term (%d)",
			finalLeaderStatus.Term, initialLeaderTerm)
	}

	// cleanup
	close(allNodesStatusDone)
	fmt.Println("Shutting down nodes...")
	for i := range numNodes {
		if shutdownChans[i] != nil {
			close(shutdownChans[i])
		}
	}
}

// Test no leader elected if minority nodes alive
func TestNoLeaderWithMinorityNodes(t *testing.T) {
	fmt.Println("Running:", t.Name())

	os.Setenv("RAFT_HEARTBEAT_INTERVAL", "500")
	os.Setenv("RAFT_ELECTION_TIMEOUT_MIN", "1000")
	os.Setenv("RAFT_ELECTION_TIMEOUT_MAX", "2000")

	peers := map[int32]string{
		1: "localhost:50051",
		2: "localhost:50052",
		3: "localhost:50053",
	}

	nodes := make([]*raft.RaftNode, 3)

	// only start 1 node
	shutdown := make(chan struct{})
	nodes[0] = raft.NewRaftNode(1, peers, shutdown)
	go utils.ServeBackend(1, peers, shutdown, nodes[0])

	nodes[1] = nil
	nodes[2] = nil

	// start status checking
	statusChan := make(chan struct{})
	statusUpdates := checkAllStatus(nodes, 100*time.Millisecond, statusChan)

	// wait then verify no leader is elected in 15 seconds
	timer := time.NewTimer(15 * time.Second)
	defer timer.Stop()

	leaderElected := false
loop:
	for {
		select {
		case statuses := <-statusUpdates:
			for _, status := range statuses {
				if status.IsLeader {
					leaderElected = true
					break
				}
			}
		case <-timer.C:
			break loop
		}
	}

	close(statusChan)
	close(nodes[0].Shutdown)

	if leaderElected {
		t.Error("FAIL: A leader was elected but only minority alive")
	} else {
		fmt.Println("SUCCESS: No leader was elected with minority nodes")
	}
}

// Test to make sure 1 follower failure does not trigger an election change
func TestFollowerFailure(t *testing.T) {
	fmt.Println("Running:", t.Name())

	os.Setenv("RAFT_HEARTBEAT_INTERVAL", "500")
	os.Setenv("RAFT_ELECTION_TIMEOUT_MIN", "1000")
	os.Setenv("RAFT_ELECTION_TIMEOUT_MAX", "2000")

	// test node configuration
	peers := map[int32]string{
		0: "localhost:50071",
		1: "localhost:50072",
		2: "localhost:50073",
	}

	nodes := make([]*raft.RaftNode, 3)

	for i := range 3 {
		shutdown := make(chan struct{})
		nodes[i] = raft.NewRaftNode(int32(i), peers, shutdown)
		go utils.ServeBackend(int32(i), peers, shutdown, nodes[i])
	}

	// start status checking
	statusChan := make(chan struct{})
	statusUpdates := checkAllStatus(nodes, 100*time.Millisecond, statusChan)

	// wait for initial stable leader election
	leader := waitForStableLeader(statusUpdates, 10*time.Second)
	if leader == -1 {
		t.Fatal("FAILURE: could not achieve stable leadership in 10 seconds")
	}
	fmt.Printf("Initial leader: %d\n", leader)

	// find a follower to kill
	var followerToKill int
	for i := range 3 {
		if i != leader {
			followerToKill = i
			break
		}
	}

	// shut down a follower
	close(nodes[followerToKill].Shutdown)
	fmt.Printf("Follower %d shut down\n", followerToKill)

	// wait a bit for cluster to stabilize
	time.Sleep(3 * raft.DefaultRPCTimeout * time.Second)

	// leader should remain the same
	stableLeader := waitForStableLeader(statusUpdates, 10*time.Second)
	if stableLeader != leader {
		t.Errorf("FAILURE: expected leader %d to remain, but got %d", leader, stableLeader)
	} else {
		fmt.Printf("Leader %d is still active after follower failure\n", stableLeader)
	}

	for i := range 3 {
		if i != followerToKill {
			close(nodes[i].Shutdown)
		}
	}
}

// Test if no leader can be elected after leader AND a follower fails
func TestLeaderAndFollowerFailElection(t *testing.T) {
	fmt.Println("Running:", t.Name())

	os.Setenv("RAFT_HEARTBEAT_INTERVAL", "500")
	os.Setenv("RAFT_ELECTION_TIMEOUT_MIN", "1000")
	os.Setenv("RAFT_ELECTION_TIMEOUT_MAX", "2000")

	// test node configuration
	peers := map[int32]string{
		0: "localhost:50071",
		1: "localhost:50072",
		2: "localhost:50073",
	}

	nodes := make([]*raft.RaftNode, 3)

	for i := range 3 {
		shutdown := make(chan struct{})
		nodes[i] = raft.NewRaftNode(int32(i), peers, shutdown)
		go utils.ServeBackend(int32(i), peers, shutdown, nodes[i])
	}

	// start status checking
	statusChan := make(chan struct{})
	statusUpdates := checkAllStatus(nodes, 100*time.Millisecond, statusChan)

	var previousLeader int
	if previousLeader = waitForStableLeader(statusUpdates, 10*time.Second); previousLeader == -1 {
		t.Fatal("FAILURE: could not achieve stable leadership in 10 seconds")
	}

	fmt.Printf("Previous leader: %d\n", previousLeader)

	// shutdown a follower
	var followerToKill int
	for i := range 3 {
		if i != previousLeader {
			followerToKill = i
			break
		}
	}

	close(nodes[followerToKill].Shutdown)

	fmt.Printf("Follower killed: %d\n", followerToKill)

	// shutdown the previous leader
	close(nodes[previousLeader].Shutdown)

	// see if we elect a new leader in 15 seconds
	newStatusChan := make(chan struct{})
	nodes[previousLeader] = nil
	nodes[followerToKill] = nil
	newStatusUpdates := checkAllStatus(nodes, 100*time.Millisecond, newStatusChan)

	var newLeader int
	if newLeader = waitForStableLeader(newStatusUpdates, 15*time.Second); newLeader != -1 {
		t.Fatal("FAILURE: achieved stable leadership in 15 seconds")
	}

	close(statusChan)
	close(newStatusChan)
}