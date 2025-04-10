package main

import (
	"context"
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

var id int32

const (
	masterAddress = "localhost:50060" // Address of the master node
	maxGRPCSize   = 1024 * 1024 * 100 // 100 MB

)

type dataNodeServer struct {
	pb.UnimplementedFileServiceServer
}

// Handles file upload from a client node

func (d *dataNodeServer) UploadFile(ctx context.Context, req *pb.FileUploadRequest) (*pb.FileUploadResponse, error) {
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
	nodeDir := fmt.Sprintf("./uploaded_%d", id)
	if err := os.MkdirAll(nodeDir, 0755); err != nil {
		return nil, fmt.Errorf("error creating upload dir: %v", err)
	}

	// File saving path
	savePath := filepath.Join(nodeDir, req.FileName+".mp4")
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
	go notifyMasterOfUpload(outCtx, req.FileName, savePath)

	return &pb.FileUploadResponse{Message: "Upload successful"}, nil
}

func notifyMasterOfUpload(ctx context.Context, filename, path string) {
	conn, err := grpc.Dial(masterAddress, grpc.WithInsecure())
	if err != nil {
		log.Printf("Failed to notify master: %v", err)
		return
	}
	defer conn.Close()

	client := pb.NewFileServiceClient(conn)

	_, err = client.NotifyUploaded(ctx, &pb.NotifyUploadedRequest{
		FileName: filename,
		DataNode: id,
		FilePath: path,
	})
	if err != nil {
		log.Printf("Master notification failed: %v", err)
	}
}

func (d *dataNodeServer) DownloadFile(ctx context.Context, in *pb.FileDownloadRequest) (*pb.FileDownloadResponse, error) {
	log.Printf("FileDownloadRequest %s", in.FileName)
	dir := fmt.Sprintf("./uploaded_%d", id)

	filePath := filepath.Join(dir, in.FileName+".mp4")

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

func (d *dataNodeServer) sendHeartbeat() {
	masterConn, err := grpc.Dial(masterAddress, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Cannot connect to Master %v", err)
	}
	defer masterConn.Close()
	masterClient := pb.NewFileServiceClient(masterConn)

	for {
		time.Sleep(time.Second)

		keepAliveRequest := &pb.KeepAliveRequest{
			DataNode: fmt.Sprintf("%d", id),
			IsAlive:  1,
		}

		_, err := masterClient.KeepAlive(context.Background(), keepAliveRequest)
		if err != nil {
			log.Printf("Cannot Send KeepAlive %v", err)
		}
	}
}
func (d *dataNodeServer) Replicate(ctx context.Context, req *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {

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

func main() {
	var port string

	fmt.Print("Enter port Number for datanode: ")
	fmt.Scanf("%s %d", &port, &id)
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("tcp listen fail %v", err)
	}

	grpcServer := grpc.NewServer(grpc.MaxRecvMsgSize(maxGRPCSize))
	dataServer := &dataNodeServer{}
	pb.RegisterFileServiceServer(grpcServer, dataServer)
	log.Printf("DataNode running at %s", lis.Addr())
	go grpcServer.Serve(lis)

	go dataServer.sendHeartbeat()
	select {}
}
