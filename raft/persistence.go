package raft

import (
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

func (rn *RaftNode) WriteLogFile() {
	file, err := os.OpenFile(filepath.Join(rn.Path, "raft.log"), os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		log.Printf("Error opening log file: %v", err)
		return
	}

	defer file.Close()

	encoder := gob.NewEncoder(file)

	if err := encoder.Encode(PersistentState{CurrentTerm: rn.CurrentTerm, VotedFor: rn.VotedFor, Logs: rn.Logs, Snapshot: rn.Snapshot}); err != nil {
		panic(err)
	}

	if err := file.Sync(); err != nil {
		log.Printf("Error syncing log file: %v", err)
	}
}

func (rn *RaftNode) ReadLogFile() error {
	file, err := os.Open(filepath.Join(rn.Path, "raft.log"))

	if err != nil {
		if os.IsNotExist(err) {
			return err
		}

		return err
	}

	defer file.Close()

	decoder := gob.NewDecoder(file)

	var state PersistentState

	if err := decoder.Decode(&state); err != nil {
		log.Printf("Error decoding log file: %v", err)
		return err
	}

	rn.CurrentTerm = state.CurrentTerm
	rn.VotedFor = state.VotedFor
	rn.Logs = state.Logs
	rn.Snapshot = state.Snapshot

	// make sure to restore state machine from snapshot if it exists!
	if rn.Snapshot != nil {
		rn.StateMachine = make(map[string]string)

		for k, v := range rn.Snapshot.Data {
			rn.StateMachine[k] = v
		}

		fmt.Println("Read snapshot including index: ", rn.Snapshot.LastIncludedIndex)
	}

	fmt.Println("Read logs from file: ", prettyPrintLogs(rn.Logs))
	return nil
}

func (rn *RaftNode) maybeCompactLog() {
	if int32(len(rn.Logs)) >= rn.compactionThreshold {
		if rn.lastApplied == 0 {
			// this check is just for defenseive programming, in case lastApplied is 0
			return
		}

		rn.compactLog(rn.lastApplied - 1) // lastApplied is one more than what was last applied since it gets increments in applystate
	}
}

func (rn *RaftNode) compactLog(upToAppliedIndex int32) {
	// make snapshot of current state machine
	snapshotData := make(map[string]string)
	for k, v := range rn.StateMachine {
		snapshotData[k] = v
	}

	// Find the term of the last included entry
	lastIncludedTerm := rn.Logs[0].Term
	i := 0

	for i < len(rn.Logs) && rn.Logs[i].Index <= upToAppliedIndex {
		lastIncludedTerm = rn.Logs[i].Term
		i++
	}

	rn.Snapshot = &Snapshot{
		LastIncludedIndex: upToAppliedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		Data:              snapshotData,
	}

	if i < len(rn.Logs) {
		rn.Logs = rn.Logs[i:]
	} else {
		rn.Logs = []Log{}
	}

	rn.WriteLogFile()
}
