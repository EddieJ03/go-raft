package raft

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

func getCompactionThreshold() int32 {
	if val := os.Getenv("RAFT_COMPACTION_THRESHOLD"); val != "" {
		if threshold, err := strconv.Atoi(val); err == nil {
			return int32(threshold)
		}
	}
	return defaultCompactionThreshold
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


func (rn *RaftNode) getLastLogIndex() int32 {
	if len(rn.Logs) == 0 {
		if rn.Snapshot == nil {
			return 0
		}

		return rn.Snapshot.LastIncludedIndex
	}

	return rn.Logs[len(rn.Logs)-1].Index
}

func (rn *RaftNode) getLastLogTerm() int32 {
	if len(rn.Logs) == 0 {
		if rn.Snapshot == nil {
			return 0
		}

		return rn.Snapshot.LastIncludedTerm
	}

	return rn.Logs[len(rn.Logs)-1].Term
}

func prettyPrintLogs(logs []Log) string {
	var result strings.Builder
	result.WriteString("[")
	for _, log := range logs {
		var op string
		switch log.Op {
		case Set:
			op = "Set"
		case Delete:
			op = "Del"
		case NoOp:
			op = "Nop"
		}
		result.WriteString(fmt.Sprintf("(%d-%d: %s %s %s),", log.Index, log.Term, op, log.Key, log.Value))
	}
	result.WriteString("]")
	return result.String()
}
