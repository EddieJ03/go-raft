package main

import (
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
	"gopkg.in/yaml.v3"

	raft "github.com/EddieJ03/223b-raft/raft"
	pb "github.com/EddieJ03/223b-raft/raft/github.com/EddieJ03/223b-raft"
)

func serveBackend(nodeID int32, peers map[int32]string, shutdown chan struct{}) {
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

    rn := raft.NewRaftNode(nodeID, peers, shutdown)

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

func main() {
    configPath := flag.String("config", "config.yaml", "path to YAML config file")
    nodeID := flag.Int("id", -1, "node ID")
    flag.Parse()

    if *nodeID == -1 {
        log.Fatal("Please pass in a node ID using --id flag.")
    }

    file, err := os.ReadFile(*configPath)
    if err != nil {
        log.Fatalf("Failed to read config file: %v", err)
    }

    var peers map[int32]string
    if err := yaml.Unmarshal(file, &peers); err != nil {
        log.Fatalf("Failed to parse YAML: %v", err)
    }

	shutdown := make(chan struct{})

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	go serveBackend(int32(*nodeID), peers, shutdown)

	go func() {
		<-signals
		close(shutdown)
	}()

	<-shutdown
}
