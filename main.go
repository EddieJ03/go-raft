package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"gopkg.in/yaml.v3"

	raft "github.com/EddieJ03/go-raft/raft"
	utils "github.com/EddieJ03/go-raft/utils"
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

	rn := raft.NewRaftNode(int32(*nodeID), peers, shutdown, filepath.Join("logs", fmt.Sprintf("raft_node_%d", int32(*nodeID))))

	go utils.ServeBackend(int32(*nodeID), peers, shutdown, rn)

	go func() {
		<-signals
		close(shutdown)
	}()

	go func() {
		time.Sleep(1 * time.Second) // Wait for the node to start
		reader := bufio.NewReader(os.Stdin)
		for {
			fmt.Print("> ")
			line, err := reader.ReadString('\n')
			if err != nil {
				log.Printf("Error reading command: %v", err)
				continue
			}
			fields := strings.Fields(line)
			if len(fields) < 2 {
				log.Printf("Usage: set <key> <value> | delete <key> | get <key>")
				continue
			}
			op, k, v := fields[0], fields[1], ""
			if len(fields) > 2 {
				v = fields[2]
			}
			var cmd int32
			switch op {
			case "set":
				if v == "" {
					log.Printf("Usage: set <key> <value>")
					continue
				}
				cmd = raft.Set
			case "delete":
				cmd = raft.Delete
			case "get":
				cmd = raft.Get
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
