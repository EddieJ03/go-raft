package utils

import (
	"log"
	"net"

	"github.com/EddieJ03/223b-raft/raft"
	pb "github.com/EddieJ03/223b-raft/raft/github.com/EddieJ03/223b-raft"
	"google.golang.org/grpc"
)


func ServeBackend(nodeID int32, peers map[int32]string, shutdown chan struct{}, rn *raft.RaftNode) {
    addr, ok := peers[nodeID]
    if !ok {
        log.Printf("Node ID %d not found in config\n", nodeID)
		return
    }

    lis, err := net.Listen("tcp", addr)
    if err != nil {
        log.Printf("Failed to listen on %s: %v \n", addr, err)
		return
    }

    grpcServer := grpc.NewServer()

    pb.RegisterRaftServer(grpcServer, rn)

	go func() {
		<-shutdown
		rn.CleanResources()
		grpcServer.GracefulStop()
	}()

    log.Printf("Node %d starting at %s", nodeID, addr)
    if err := grpcServer.Serve(lis); err != nil {
        log.Printf("Failed to serve: %v \n", err)
		return
    }
}
