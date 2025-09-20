package utils

import (
	"fmt"
	"log"
	"net"
	"os"

	"github.com/EddieJ03/go-raft/raft"
	pb "github.com/EddieJ03/go-raft/raft/github.com/EddieJ03/go-raft"
	"google.golang.org/grpc"
)

func CleanLogs(dir string) error {
    err := os.RemoveAll(dir)
    if err != nil {
        fmt.Printf("Error removing folder: %v\n", err)
    } else {
        fmt.Println("Folder and all contents removed.")
    }

    return nil
}

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

func MapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for key, valA := range a {
		valB, ok := b[key]
		if !ok || valA != valB {
			return false
		}
	}
	return true
}