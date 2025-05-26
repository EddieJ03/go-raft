# 223b-raft

## Generating proto code

1. First run `export PATH="$PATH:$(go env GOPATH)/bin"` in the root directory
2. Reload shell: `source ~/.bashrc` (or `source ~/.zshrc` if on Mac)
3. Run `protoc --go_out=. --go-grpc_out=. proto/raft.proto` inside `raft/` directory

## Build
Run `go build main.go`, which creates `main.exe`

## Running 
Run `./main --id <number>` to start a node
- main by default uses `config.yaml`

## Tests
Run `go test ./tests -count=1`