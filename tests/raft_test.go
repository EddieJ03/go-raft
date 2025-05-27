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

// waitForStableLeader waits for the cluster to have a stable leader
// returns id of the leader if found else returns -1
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

// Tests if leadership can be established within 10 seconds with 3 Raft nodes (no crashes)
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

// Tests if leadership can be established after node failure
func TestFailElection(t *testing.T) {
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
	if leader := waitForStableLeader(statusUpdates, 10*time.Second); leader == -1 || leader == previousLeader {
		t.Errorf("FAILURE: no leader or previous leader is still active after shutdown %d", leader)
	}
	// close each backend
	for i := range 3 {
		if i != previousLeader {
			close(nodes[i].Shutdown)
		}
	}
}

// test that node joining the cluster becomes a follower
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
	initialNodeCount := numNodes - 1 // N-1 initial nodes

	// leave one out
	initialNodesSlice := make([]*raft.RaftNode, initialNodeCount)
	for i := range initialNodesSlice {
		id := int32(i)
		shutdownChans[i] = make(chan struct{})
		nodes[i] = raft.NewRaftNode(id, peers, shutdownChans[i])
		initialNodesSlice[i] = nodes[i]
		go utils.ServeBackend(id, peers, shutdownChans[i], nodes[i])
	}

	// Start status checking for the initial N-1 nodes
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

	// Start the Nth node (the joining node, ID 2)
	joiningNodeID := int32(numNodes - 1)
	fmt.Printf("Starting joining node: %d\n", joiningNodeID)
	shutdownChans[joiningNodeID] = make(chan struct{})
	nodes[joiningNodeID] = raft.NewRaftNode(joiningNodeID, peers, shutdownChans[joiningNodeID])
	go utils.ServeBackend(joiningNodeID, peers, shutdownChans[joiningNodeID], nodes[joiningNodeID])

	// wait for the joining node to start
	time.Sleep(2 * time.Second)

	// Start status checking for all N nodes
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

			if joinedNodeStatus.ID == joiningNodeID && finalLeaderStatus.ID == int32(finalLeaderID) { // Ensure both statuses are updated
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

	// Cleanup
	close(allNodesStatusDone)
	fmt.Println("Shutting down nodes...")
	for i := range numNodes {
		if shutdownChans[i] != nil {
			close(shutdownChans[i])
		}
	}
}
