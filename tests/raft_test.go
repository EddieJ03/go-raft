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
func waitForStableLeader(statusChan chan []TestNodeStatus, timeout time.Duration) bool {
	deadline := time.After(timeout)
	stableCount := 0
	lastLeaderID := int32(-1)

	for {
		select {
		case <-deadline:
			return false
		case statuses, ok := <-statusChan:
			if !ok { // somehow channel is closed or broken :(
				return false
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
						return true
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
	if !waitForStableLeader(statusUpdates, 10*time.Second) {
		t.Error("FAILURE: could not achieve stable leadership in 10 seconds")
	}

	close(statusChan)
	
	// close each backend
	for i := 0; i < 3; i++ {
		close(nodes[i].Shutdown)
	}

	for _, node := range nodes {
		node.CleanResources()
	}
}

