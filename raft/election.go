package raft

import (
	"context"
	"log"
	"time"

	pb "github.com/EddieJ03/223b-raft/raft/github.com/EddieJ03/223b-raft"
)

func (rn *RaftNode) runElectionTimer() {
	for {
		select {
		case <-time.After(500 * time.Millisecond):
			rn.Mu.Lock()

			timeout := getElectionTimeout()
			if rn.State != Leader && time.Since(rn.electionReset) >= timeout {
				rn.startElection()
			}

			rn.Mu.Unlock()
		case <-rn.Shutdown:
			log.Printf("election timer stopped for node %d", rn.Id)
			return
		}
	}
}

func (rn *RaftNode) startElection() {
	rn.State = Candidate
	rn.CurrentTerm++
	rn.VotedFor = rn.Id
	rn.VoteCount = 1
	rn.electionReset = time.Now()

	for pid, client := range rn.grpcClients {
		go func(pid int32, client pb.RaftClient) {
			ctx, cancel := context.WithTimeout(context.Background(), DefaultRPCTimeout*time.Second)
			defer cancel()
			req := &pb.RequestVoteRequest{
				Term:         rn.CurrentTerm,
				CandidateId:  rn.Id,
				LastLogIndex: rn.getLastLogIndex(),
				LastLogTerm:  rn.getLastLogTerm(),
			}

			resp, err := client.RequestVote(ctx, req)
			if err != nil {
				return
			}

			rn.Mu.Lock()
			defer rn.Mu.Unlock()

			if resp.Term > rn.CurrentTerm {
				rn.CurrentTerm = resp.Term
				rn.State = Follower
				rn.VotedFor = -1
				return
			}

			if rn.State != Candidate || rn.CurrentTerm != req.Term { // request term becomes invalid
				return
			}

			if resp.VoteGranted {
				rn.VoteCount++

				if rn.VoteCount > len(rn.peers)/2 {
					rn.State = Leader
					log.Printf("Node %d became leader for term %d", rn.Id, rn.CurrentTerm)
					go rn.leaderInit()
				}
			}
		}(pid, client)
	}
}
