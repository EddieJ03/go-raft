# GoRaft

![video1632731170](https://github.com/user-attachments/assets/9749489b-10d3-4b0d-ace0-aed1d7fa8ae7)

## Generating proto code

1. First run `export PATH="$PATH:$(go env GOPATH)/bin"` in the root directory
2. Reload shell: `source ~/.bashrc` (or `source ~/.zshrc` if on Mac)
3. Run `protoc --go_out=. --go-grpc_out=. proto/raft.proto` inside `raft/` directory

## Building
Run `go build main.go`, which creates `main.exe`

## Running 
Run `./main --id <number>` to start a node
- main by default uses `config.yaml`

After nodes are initialized, you can use standard input to send commands to the leader for testing:
- `set <key> <value>` to set a key-value pair
- `delete <key>` to delete a key
- `get <key>` to get a value for a key, or it will say it does not exist

We decided to force all reads/gets through the leader to provide strong consistency guarantees.

## Testing
Do not run tests below concurrently! There will be clashes with processes running on the same port.

- Running just leader election tests: `go test ./tests -run Election -count=1 -p=1 -failfast`
- Running just replication tests: `go test ./tests -run Replication -count=1 -p=1 -failfast`
- Running just compaction tests: `go test ./tests -run Compaction -count=1 -p=1 -failfast`

Also note that these tests can be flaky, but should never consistently fail.
