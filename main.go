package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"gopkg.in/yaml.v3"

	raft "github.com/EddieJ03/223b-raft/raft"
    utils "github.com/EddieJ03/223b-raft/utils"
)

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

    rn := raft.NewRaftNode(int32(*nodeID), peers, shutdown)

	go utils.ServeBackend(int32(*nodeID), peers, shutdown, rn)

	go func() {
		<-signals
		close(shutdown)
	}()

	<-shutdown
}
