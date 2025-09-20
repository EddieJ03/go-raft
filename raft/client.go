package raft

import (
	"errors"
	"fmt"
)

func (rn *RaftNode) ClientRequest(op int32, key, value string) (string, error) {
	rn.Mu.Lock()
	defer rn.Mu.Unlock()

	if rn.State != Leader {
		return fmt.Sprintf("%d is the leader", rn.leaderId), errors.New("not the leader")
	}

	if op == Get {
		val, ok := rn.StateMachine[key]
		if !ok {
			return fmt.Sprintf("Key '%s' not found", key), nil
		}
		return fmt.Sprintf("%s", val), nil
	}

	entry := Log{
		Term:  rn.CurrentTerm,
		Op:    op,
		Key:   key,
		Value: value,
		Index: rn.getLastLogIndex() + 1,
	}
	rn.Logs = append(rn.Logs, entry)
	rn.leaderMatchIndex[rn.Id] = entry.Index

	for pid, client := range rn.grpcClients {
		go rn.UpdateFollower(pid, client)
	}

	return fmt.Sprintf("CLIENT: request received: %v", entry), nil
}
