package raft

import (
	"os"
	"testing"
	"time"
)

// TestNodeStatus represents the current state of a node for testing purposes
type TestNodeStatus struct {
	ID       int32
	State    State
	Term     int32
	VotedFor int32
	IsLeader bool
}

// checkAllStatus periodically checks the status of all nodes and returns a channel that receives updates
func checkAllStatus(nodes []*RaftNode, interval time.Duration, done chan struct{}) chan []TestNodeStatus {
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
					node.mu.Lock()
					statuses[i] = TestNodeStatus{
						ID:       node.id,
						State:    node.state,
						Term:     node.currentTerm,
						VotedFor: node.votedFor,
						IsLeader: node.state == Leader,
					}
					node.mu.Unlock()
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

// validateStatus checks if the current status of nodes is valid according to RAFT rules
func validateStatus(statuses []TestNodeStatus) bool {
	// Check if there is exactly one leader
	leaderCount := 0
	for _, status := range statuses {
		if status.IsLeader {
			leaderCount++
		}
	}
	if leaderCount != 1 {
		return false
	}

	// Check if all nodes in the same term have consistent votedFor
	termVotes := make(map[int32]map[int32]int)
	for _, status := range statuses {
		if _, exists := termVotes[status.Term]; !exists {
			termVotes[status.Term] = make(map[int32]int)
		}
		termVotes[status.Term][status.VotedFor]++
	}

	// For each term, there should be at most one candidate that received votes
	for _, votes := range termVotes {
		voteCount := 0
		for _, count := range votes {
			if count > 0 {
				voteCount++
			}
		}
		if voteCount > 1 {
			return false
		}
	}

	return true
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
			if !ok {
				return false
			}

			// Count leaders
			leaderCount := 0
			var currentLeaderID int32 = -1
			for _, status := range statuses {
				if status.IsLeader {
					leaderCount++
					currentLeaderID = status.ID
				}
			}

			// Check if we have exactly one leader
			if leaderCount == 1 {
				if currentLeaderID == lastLeaderID {
					stableCount++
					if stableCount >= 3 { // Require 3 consecutive stable states
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

// TestLeaderElection tests the basic leader election functionality
func TestLeaderElection(t *testing.T) {
	// Set test environment variables
	os.Setenv("RAFT_HEARTBEAT_INTERVAL", "500")
	os.Setenv("RAFT_ELECTION_TIMEOUT_MIN", "1000")
	os.Setenv("RAFT_ELECTION_TIMEOUT_MAX", "2000")

	// Create test nodes
	peers := map[int32]string{
		1: "localhost:50051",
		2: "localhost:50052",
		3: "localhost:50053",
	}

	nodes := make([]*RaftNode, 3)
	shutdown := make(chan struct{})

	for i := 0; i < 3; i++ {
		nodes[i] = NewRaftNode(int32(i+1), peers, shutdown)
	}

	// Start status checking
	statusChan := make(chan struct{})
	statusUpdates := checkAllStatus(nodes, 100*time.Millisecond, statusChan)

	// Wait for stable leader
	if !waitForStableLeader(statusUpdates, 10*time.Second) {
		t.Error("Failed to achieve stable leadership")
	}

	// Cleanup
	close(statusChan)
	close(shutdown)
	for _, node := range nodes {
		node.CleanResources()
	}
}

// TestSingleNodeElection tests the election behavior of a single node
func TestSingleNodeElection(t *testing.T) {
	peers := map[int32]string{
		1: "localhost:50051",
	}

	shutdown := make(chan struct{})
	node := NewRaftNode(1, peers, shutdown)

	// Wait for election with timeout
	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("Timeout waiting for single node to become leader")
		case <-time.After(100 * time.Millisecond):
			node.mu.Lock()
			if node.state == Leader {
				if node.currentTerm != 1 {
					t.Errorf("Expected term 1, got: %d", node.currentTerm)
				}
				node.mu.Unlock()
				close(shutdown)
				node.CleanResources()
				return
			}
			node.mu.Unlock()
		}
	}
}
