# GoRaft

An implementation of the Raft consensus protocol, with a simple CLI to interact with it. Supports `Set`, `Delete`, and `Get` operations on key-value pairs. All operations go through the leader to provide strong consistency guarantees. 

* **Heartbeat interval**

  * Default: `1s`
  * Override with:

    ```bash
    export RAFT_HEARTBEAT_INTERVAL=1000   # value in milliseconds
    ```

* **Election timeout**

  * Default: random value between `1.5s` and `3s`
  * Override with:

    ```bash
    export RAFT_ELECTION_TIMEOUT_MIN=1500   # value in milliseconds
    export RAFT_ELECTION_TIMEOUT_MAX=3000
    ```

![video1632731170](https://github.com/user-attachments/assets/9749489b-10d3-4b0d-ace0-aed1d7fa8ae7)

## Building

### Generating proto code
1. First run `export PATH="$PATH:$(go env GOPATH)/bin"` in the root directory
2. Reload shell: `source ~/.bashrc` (or `source ~/.zshrc` if on Mac)
3. Run `protoc --go_out=. --go-grpc_out=. proto/raft.proto` inside `raft/` directory

Run `go build goraft.go`, which creates `goraft.exe`

## Running CLI
Run `./goraft --id <number>` to start a node
- `goraft` by default loads in the address of each node from `config.yaml` 

After nodes are initialized, you can use standard input to send commands to the leader for testing:
- `set <key> <value>` to set a key-value pair
- `delete <key>` to delete a key
- `get <key>` to get a value for a key, or it will say it does not exist

## Testing
Do not run tests below concurrently! There will be clashes with processes running on the same port.

- Running just leader election tests: `go test ./tests -run Election -count=1 -p=1 -failfast`
- Running just replication tests: `go test ./tests -run Replication -count=1 -p=1 -failfast`
- Running just compaction tests: `go test ./tests -run Compaction -count=1 -p=1 -failfast`

Also note that these tests can be flaky, but should never consistently fail.
