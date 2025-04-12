package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	pb "proj/Services"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var fileStoragePath = "./toupload"
var downloadDir = "./downloads"

const (
	masterAddress = "localhost:50060" // Address of the master node
	clientAddress = "localhost:12345" // Address of the client server
)

// Client server for Notification on upload finish
type ClientServer struct {
	pb.UnimplementedFileServiceServer
}

func (c *ClientServer) SendNotification(ctx context.Context, req *pb.SendNotificationRequest) (*pb.SendNotificationResponse, error) {
	fmt.Printf("Notificatoin from master: %s\n", req.Message)
	return &pb.SendNotificationResponse{}, nil
}

func main() {
	// Launch client-side gRPC server for notifications

	md := metadata.Pairs("client-ip", "localhost", "client-port", "12345")
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	masterConn, err := grpc.Dial(masterAddress, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Cannot Dial Masternode %v", err)
	}
	defer masterConn.Close()
	masterClient := pb.NewFileServiceClient(masterConn)

	for {
		fmt.Print("'u', 'd', or 'e'")
		reader := bufio.NewReader(os.Stdin)
		answer, err := reader.ReadString('\n')
		if err != nil {
			log.Fatalf("Failed to initialize read string: %v", err)
		}
		command := strings.TrimSpace(strings.ToLower(answer))

		switch command {
		case "u":
			uploadFile(ctx, masterClient)

		case "d":
			downloadFile(ctx, masterClient)

		case "e":
			fmt.Println("Shutting down client...")
			return

		default:
			fmt.Println("Invalid answer. Enter 'u', 'd', or 'e'.")
		}
	}
}

// Upload file to the distributed system
func uploadFile(ctx context.Context, masterClient pb.FileServiceClient) {

	var fileName string
	fmt.Print("Enter file name (without extension): ")
	fmt.Scanln(&fileName)

	filePath := filepath.Join(fileStoragePath, fileName+".mp4")
	fileData, err := os.ReadFile(filePath)
	if err != nil {
		log.Fatalf("Error reading file: %v", err)
	}

	// Request upload destination from master
	response, err := masterClient.HandleUploadFile(ctx, &pb.HandleUploadFileRequest{})
	if err != nil {
		log.Fatalf("Failed to get upload details: %v", err)
	}

	dataNodeAddr := fmt.Sprintf("%s%d", response.IpAddress, response.PortNumber)
	fmt.Println("Uploading to:", dataNodeAddr)

	// Connect to DataNode
	dataConn, err := grpc.Dial(dataNodeAddr, grpc.WithInsecure(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1024*1024*100)))
	if err != nil {
		log.Fatalf("Could not connect to DataNode: %v", err)
	}
	defer dataConn.Close()
	dataClient := pb.NewFileServiceClient(dataConn)

	// Upload file to DataNode
	uploadResponse, err := dataClient.UploadFile(ctx, &pb.FileUploadRequest{
		FileName:    fileName,
		FileContent: fileData,
	})
	if err != nil {
		log.Fatalf("File upload failed: %v", err)
	}
	fmt.Println("Upload response:", uploadResponse.Message)
}

// Download file from the distributed system
func downloadFile(ctx context.Context, masterClient pb.FileServiceClient) {
	var fileName string
	fmt.Print("Enter file name (without extension): ")
	fmt.Scanln(&fileName)

	// Request file locations from master
	response, err := masterClient.HandleDownloadFile(ctx, &pb.HandleDownloadFileRequest{
		FileName: fileName,
	})
	if err != nil {
		log.Fatalf("Download request failed: %v", err)
	}

	// Ensure download directory exists
	if _, err := os.Stat(downloadDir); os.IsNotExist(err) {
		if err := os.Mkdir(downloadDir, 0755); err != nil {
			log.Fatalf("Failed to create download directory: %v", err)
		}
	}

	// Select a random DataNode
	availableNodes := len(response.IpAddress)
	if availableNodes == 0 {
		log.Fatal("No available DataNodes for download")
	}

	selectedIndex := rand.Intn(availableNodes)
	dataNodeAddr := fmt.Sprintf("%s%d", response.IpAddress[selectedIndex], response.PortNumbers[selectedIndex])
	fmt.Println("Downloading from:", dataNodeAddr)

	// Connect to DataNode
	dataConn, err := grpc.Dial(dataNodeAddr, grpc.WithInsecure(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1024*1024*100)))
	if err != nil {
		log.Fatalf("Could not connect to DataNode: %v", err)
	}
	defer dataConn.Close()
	dataClient := pb.NewFileServiceClient(dataConn)

	// Request file download
	downloadResponse, err := dataClient.DownloadFile(ctx, &pb.FileDownloadRequest{
		FileName: fileName,
	})
	if err != nil {
		log.Fatalf("Download failed: %v", err)
	}

	// Save downloaded file
	filePath := filepath.Join(downloadDir, fileName+".mp4")
	if err := os.WriteFile(filePath, downloadResponse.FileContent, 0644); err != nil {
		log.Fatalf("Failed to save downloaded file: %v", err)
	}
	fmt.Printf("Download successful. File saved at: %s\n", filePath)

}
