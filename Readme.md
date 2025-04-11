# Distributed File System
This is a **Master-Slave Distributed File System** implemented in **Go**, designed specifically for handling `.mp4` files. It supports **upload** and **download** operations and data replication across multiple storage nodes, known as **DataNodes**.

A **MasterNode** coordinates the system by:

- Managing the metadata and health of all connected DataNodes through Heartbeats every 1 sec.
- Handling client requests for uploading and downloading files.
- Ensuring data is properly distributed and accessible across the network by replicating files on atleast 3 datanodes.

This system is designed to be lightweight, scalable, and fault-tolerant for media file storage and retrieval in distributed environments.
# Installation Guide **(Linux)**
## Download and Install GO 
```bash
wget https://go.dev/dl/go1.21.5.linux-amd64.tar.gz
sudo rm -rf /usr/local/go
sudo tar -C /usr/local -xzf go1.21.5.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin
export GOPATH=$HOME/go
export PATH=$PATH:$GOPATH/bin
source ~/.bashrc 
```
## verify it's installed
```bash
go version
```
## Instal GRPC and Protobuf
```bash
sudo apt install -y protobuf-compiler
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```
# Build and Run Guide
## generate GRPC files
```bash
protoc --go_out=. --go-grpc_out=. services.proto
```
## Identify dependencies and generate go.sum file
```bash
go mod tidy
```
## Build Go files
```bash
go build
```
## Run GO files 
The MasterNode must be online before others, replace # with one of four configs
```bash
go run MasterNode.go
go run client/Client.go
go run Datanode/DataNode.go Datanode/DataNode_#_Config.json
```