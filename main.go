package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

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

	go func() {
		time.Sleep(1 * time.Second) // Wait for the node to start
		for {
			fmt.Print("> ")
			var op, k, v string
			_, err := fmt.Scanf("%s %s %s\n", &op, &k, &v)
			if err != nil {
				log.Printf("Error reading command: %v", err)
				continue
			}
			var cmd int32
			switch op {
			case "set":
				cmd = raft.Set
			case "delete":
				cmd = raft.Delete
			default:
				log.Printf("Unknown command: %s", op)
				continue
			}
			out, _ := rn.ClientRequest(cmd, k, v)
			fmt.Println(out)
		}
	}()

	<-shutdown
}
