package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	pb "proj/Services"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	masterAddress = "localhost:50061" // Address of the master node
	maxGRPCSize   = 1024 * 1024 * 100 // 100 MB
)

type DataNodeServer struct {
	IP            string
	PortForMaster string `json:"MasterNodePort"`
	PortForClient string `json:"ClientNodePort"`
	PortForDN     string `json:"DataNodePort"`
	ID            int32  `json:"ID"`
	pb.UnimplementedFileServiceServer
}

/*
Handles file upload from client
*/
func (d *DataNodeServer) UploadFile(ctx context.Context, req *pb.FileUploadRequest) (*pb.FileUploadResponse, error) {
	log.Printf("Received upload request for: %s", req.FileName)

	// Metadata extraction (client IP and port)
	md, exists := metadata.FromIncomingContext(ctx)
	if !exists {
		log.Println("No metadata in request")
	}
	clientIP := strings.Join(md.Get("client-ip"), ",")
	clientPort := strings.Join(md.Get("client-port"), ",")

	outMeta := metadata.Pairs("client-ip", clientIP, "client-port", clientPort)
	outCtx := metadata.NewOutgoingContext(context.Background(), outMeta)

	// Save directory for this DataNode
	nodeDir := fmt.Sprintf("./uploaded_%s", d.PortForClient)
	if err := os.MkdirAll(nodeDir, 0755); err != nil {
		return nil, fmt.Errorf("error creating upload dir: %v", err)
	}

	// File saving path
	savePath := filepath.Join(nodeDir, req.FileName)
	file, err := os.Create(savePath)
	if err != nil {
		return nil, fmt.Errorf("error creating file: %v", err)
	}
	defer file.Close()

	if _, err := file.Write(req.FileContent); err != nil {
		return nil, fmt.Errorf("error writing file content: %v", err)
	}

	log.Printf("File stored at: %s", savePath)

	// Write the content to the file

	log.Printf("File uploaded success at %s", savePath)

	// Asynchronously notify the master node about the upload
	go notifyMasterOfUpload(d, outCtx, req.FileName, savePath)

	return &pb.FileUploadResponse{Message: "Upload successful"}, nil
}

func notifyMasterOfUpload(d *DataNodeServer, ctx context.Context, filename, path string) {
	conn, err := grpc.Dial(masterAddress, grpc.WithInsecure())
	if err != nil {
		log.Printf("Failed to notify master: %v", err)
		return
	}
	defer conn.Close()

	client := pb.NewFileServiceClient(conn)

	_, err = client.NotifyUploaded(ctx, &pb.NotifyUploadedRequest{
		FileName: filename,
		DataNode: d.ID,
		FilePath: path,
	})
	if err != nil {
		log.Printf("Master notification failed: %v", err)
	}
}

func (d *DataNodeServer) DownloadFile(ctx context.Context, in *pb.FileDownloadRequest) (*pb.FileDownloadResponse, error) {
	log.Printf("FileDownloadRequest %s", in.FileName)
	dir := fmt.Sprintf("./uploaded_%s", d.PortForClient)

	filePath := filepath.Join(dir, in.FileName)

	fileContent, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("ReadFile fail %v", err)
	}
	// Create and return the response with the file content
	response := &pb.FileDownloadResponse{
		FileContent: fileContent,
	}
	return response, nil
}

func (d *DataNodeServer) sendHeartbeat() {

	masterConn, err := grpc.Dial(masterAddress, grpc.WithInsecure())

	if err != nil {
		log.Fatalf("Cannot connect to Master %v", err)
	}
	defer masterConn.Close()
	masterClient := pb.NewFileServiceClient(masterConn)
	for {

		time.Sleep(time.Second)
		keepAliveRequest := &pb.KeepAliveRequest{
			DataNode_IP: d.IP,
			PortNumber:  []string{d.PortForMaster, d.PortForClient, d.PortForDN},
			IsAlive:     true,
		}

		_, err := masterClient.KeepAlive(context.Background(), keepAliveRequest)
		if err != nil {
			log.Printf("Cannot Send KeepAlive %v", err)
		}
	}
}
func (d *DataNodeServer) Replicate(ctx context.Context, req *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {

	log.Printf("Replicating file: %s to %d node(s)", req.FileName, len(req.IpAddresses))

	// Read the content of the file
	content, err := os.ReadFile(req.FilePath)
	if err != nil {
		return nil, fmt.Errorf("replication failed, cannot read file: %v", err)
	}

	// Iterate over the provided IP addresses and ports
	for i, ip := range req.IpAddresses {

		addr := fmt.Sprintf("%s:%d", ip, req.PortNumbers[i])

		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			log.Printf("Connection failed to %s: %v", addr, err)
			continue
		}
		defer conn.Close()

		client := pb.NewFileServiceClient(conn)

		// Invoke the UploadFile service
		_, err = client.UploadFile(ctx, &pb.FileUploadRequest{
			FileName:    req.FileName,
			FileContent: content,
		})
		if err != nil {
			log.Printf("Replication upload failed to %s: %v", addr, err)
		} else {
			log.Printf("Replicated to %s", addr)
		}

	}

	return &pb.ReplicateResponse{}, nil
}

/*
This function extracts the local IP that the data node runs on
*/
func GetMachineIP() (string, error) {
	// get the network interfaces within machine (ethernet0, wifi ..)
	addrs, err := net.InterfaceAddrs()

	if err != nil {
		return "", err
	}

	for _, addr := range addrs {
		// Check the address type and skip loopback
		// loopback is virtual network that computer use to communicate to itself aka 127.0.0.1
		// so we don't need that since datanodes will run on external physical machines
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			// is it ipv4 type ?
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String(), nil
			}
		}
	}
	return "", fmt.Errorf("no non-loopback IP address found")
}

func main() {
	// the config file must be passed
	if len(os.Args) < 2 {
		log.Fatalf("Please pass the dataNode configuration file by terminal")
	}

	config_file_path := os.Args[1]
	config, err := os.ReadFile(config_file_path)
	if err != nil {
		log.Fatalf("couldn't read the file specified")
	}

	ip, err := GetMachineIP()
	if err != nil {
		fmt.Println("Error in extracting IP of machine", err)
	}
	// Start to configure our data node server
	dataServer := &DataNodeServer{
		IP: ip,
	}
	// parse the json configuration to the data Node server
	err = json.Unmarshal(config, dataServer)
	if err != nil {
		log.Fatalf("couldn't parse config file")
	}

	// open TCP ports for future connections with Master, Client, DataNodes
	lisC, err := net.Listen("tcp", dataServer.PortForClient)
	if err != nil {
		log.Fatalf("tcp portForClient listen fail %v", err)
	}
	lisD, err := net.Listen("tcp", dataServer.PortForDN)
	if err != nil {
		log.Fatalf("tcp portForDN listen fail %v", err)
	}

	lisMaster, err := net.Listen("tcp", dataServer.PortForMaster)
	if err != nil {
		log.Fatalf("tcp portForM listen fail %v", err)
	}

	// create a Grpc server and bind our data node server to it
	grpcServer := grpc.NewServer(grpc.MaxRecvMsgSize(maxGRPCSize))
	pb.RegisterFileServiceServer(grpcServer, dataServer)

	// Start serving each listener in separate goroutines
	go grpcServer.Serve(lisC)      // Serve on client port
	go grpcServer.Serve(lisD)      // Serve on DataNode port
	go grpcServer.Serve(lisMaster) // Serve on master port
	// tell the master I'm online
	go dataServer.sendHeartbeat()

	log.Printf("DataNode running at %s for client and %s for DataNodes and %s for Master", lisC.Addr(), lisD.Addr(), lisMaster.Addr())
	// blocker so that the code doesn't terminate
	select {}
}
