package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	pb "proj/Services"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	portClient       = ":50060"
	portDataNode     = ":50061"
	keepAliveTimeout = 2 * time.Second
)

type FileRecord struct {
	FileName  string
	FilePaths []string
	DataNodes []int32
}

type MachineRecord struct {
	IPAddress      string
	MasterNodePort int32
	ClientNodePort int32
	DataNodePort   int32
	Liveness       bool
}

type server struct {
	fileRecords      map[string]*FileRecord
	machineRecords   []*MachineRecord
	lastKeepAliveMap map[int]time.Time
	mutex            sync.Mutex
	pb.UnimplementedFileServiceServer
}

func (s *server) PrintMachineRecords() {
	fmt.Println("Machine Records:")
	for i, record := range s.machineRecords {
		AvailablePorts := []int32{record.MasterNodePort, record.ClientNodePort, record.DataNodePort}
		fmt.Printf("Machine %d:\n  IP: %s\n  Alive: %t\n  Ports: %v\n", i, record.IPAddress, record.Liveness, AvailablePorts)
	}
}

func (s *server) PrintFileRecords() {
	fmt.Println("File Records:")
	for filename, record := range s.fileRecords {
		fmt.Printf("File: %s\n  Paths: %v\n  Nodes: %v\n", filename, record.FilePaths, record.DataNodes)
	}
}

/*
Client Initialization intent to upload file
*/
func (s *server) HandleUploadFile(ctx context.Context, in *pb.HandleUploadFileRequest) (*pb.HandleUploadFileResponse, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	// a map of the alive machines from the present DataNodes registered to our system
	aliveMachines := make([]*MachineRecord, 0)

	for _, machine := range s.machineRecords {
		if machine.Liveness {
			aliveMachines = append(aliveMachines, machine)
		}
	}
	// we can't accept upload requests right now since no datanodes online
	if len(aliveMachines) == 0 {
		return nil, errors.New("no aliveMachines")
	}

	selectedMachine := aliveMachines[rand.Intn(len(aliveMachines))]

	selectedPort := selectedMachine.ClientNodePort
	selectedIP := selectedMachine.IPAddress

	response := &pb.HandleUploadFileResponse{
		PortNumber: selectedPort,
		IpAddress:  selectedIP,
	}

	return response, nil
}

/*
Client Initialization intent to download file
*/
func (s *server) HandleDownloadFile(ctx context.Context, in *pb.HandleDownloadFileRequest) (*pb.HandleDownloadFileResponse, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	fileRecord, ok := s.fileRecords[in.FileName]
	if !ok {
		return nil, errors.New("No such filename exist")
	}

	var ipAddresses []string
	var portNumbers []int32

	for i, nodeID := range fileRecord.DataNodes {
		if s.machineRecords[nodeID].Liveness {
			datanode := s.machineRecords[fileRecord.DataNodes[i]]
			ipAddresses = append(ipAddresses, datanode.IPAddress)
			portNumbers = append(portNumbers, datanode.ClientNodePort)
		}
	}

	response := &pb.HandleDownloadFileResponse{
		IpAddress:   ipAddresses,
		PortNumbers: portNumbers,
	}

	return response, nil
}

func (s *server) NotifyUploaded(ctx context.Context, in *pb.NotifyUploadedRequest) (*pb.NotifyUploadedResponse, error) {

	rand.Seed(time.Now().UnixNano())

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if record, ok := s.fileRecords[in.FileName]; ok {
		record.DataNodes = append(record.DataNodes, in.DataNode)
		record.FilePaths = append(record.FilePaths, in.FilePath)

		s.PrintFileRecords()
		return &pb.NotifyUploadedResponse{}, nil
	}

	s.fileRecords[in.FileName] = &FileRecord{
		FileName:  in.FileName,
		FilePaths: []string{in.FilePath},
		DataNodes: []int32{in.DataNode},
	}

	// Get client metadata

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		log.Println("No metadata found in the context")
	}
	clientIP := md.Get("client-ip")
	clientPort := md.Get("client-port")

	fmt.Printf("Client IP: %s | Port: %s\n", clientIP, clientPort)

	// Notify client asynchronously
	go func() {
		clientAddr := fmt.Sprintf("%s:%s", clientIP[0], clientPort[0])
		conn, err := grpc.Dial(clientAddr, grpc.WithInsecure())
		if err != nil {
			log.Printf("Dial client fail %v", err)
			return
		}
		defer conn.Close()

		client := pb.NewFileServiceClient(conn)

		useless, err := client.SendNotification(context.Background(), &pb.SendNotificationRequest{
			Message: "File Upload Finish",
		})
		if err != nil {
			log.Printf("SendNotification to client fail %v", err)
		}
		fmt.Printf(useless.String())
	}()

	// Trigger replication
	sourceID := in.DataNode
	var replicateIPs []string
	var replicatePorts []int32
	var replicateIds []int32

	for i := 1; i <= 2; i++ {
		// 0 , 1 , 2 , 3 , 4
		// file originally in 0 and 2
		// Source is a random ID that comes from the Datanodes that have the file record ?
		// which i will replicate from
		// source id = 2 , 2 + 1 = 3 % 3 = 0
		// replicate = 2 + 2 = 4 % 3 = 1
		replicateId := (sourceID + int32(i)) % int32(len(s.machineRecords))
		isExist := false
		// check on datanodes that have the filerecord
		for _, node := range s.fileRecords[in.FileName].DataNodes {

			if node == replicateId {
				isExist = true
				break
			}
		}
		if !isExist && s.machineRecords[replicateId].Liveness {
			// From my machines take the IP, PORT, ID to send the file to
			replicateIPs = append(replicateIPs, s.machineRecords[replicateId].IPAddress)
			replicatePorts = append(replicatePorts, s.machineRecords[replicateId].DataNodePort)
			replicateIds = append(replicateIds, replicateId)
		} else if !s.machineRecords[replicateId].Liveness {
			log.Printf("machine %s not alive.", s.machineRecords[replicateId].IPAddress)
		}
	}
	replicateRequest := &pb.ReplicateRequest{
		FileName:    in.FileName,
		FilePath:    in.FilePath,
		IpAddresses: replicateIPs,
		PortNumbers: replicatePorts,
		Ids:         replicateIds,
	}

	if s.machineRecords[sourceID].Liveness {
		go func() {
			clientAddress := fmt.Sprintf("%s:%d", s.machineRecords[sourceID].IPAddress, s.machineRecords[sourceID].MasterNodePort)
			conn, err := grpc.Dial(clientAddress, grpc.WithInsecure())
			if err != nil {
				log.Printf("Dial source data node fail %v", err)
				return
			}
			defer conn.Close()

			sourceClient := pb.NewFileServiceClient(conn)

			_, err = sourceClient.Replicate(context.Background(), replicateRequest)
			if err != nil {
				log.Printf("Replicate fail on source Datanode machine %v", err)
				return
			}
		}()
	}
	s.PrintFileRecords()
	return &pb.NotifyUploadedResponse{}, nil
}

// =======================
// Background Processes
// =======================

func (s *server) replicationScheduler() {
	for {

		time.Sleep(10 * time.Second)
		s.mutex.Lock()

		for _, fileRecord := range s.fileRecords {
			var liveNodeIndexes []int
			for i, datanode := range fileRecord.DataNodes {
				if s.machineRecords[datanode].Liveness {
					liveNodeIndexes = append(liveNodeIndexes, i)
				}
			}
			if len(liveNodeIndexes) < 3 && len(liveNodeIndexes) > 0 {

				randomIndex := rand.Intn(len(liveNodeIndexes))
				chosenNodeIndex := liveNodeIndexes[randomIndex]
				sourceID := fileRecord.DataNodes[chosenNodeIndex]

				//
				var replicateIPs []string
				var replicatePorts []int32
				var replicateIds []int32
				for i := 1; i <= 2; i++ {
					// 0 , 1 , 2 , 3 , 4
					// file originally in 0 and 2
					// Source is a random ID that comes from the Datanodes that have the file record ?
					// which i will replicate from
					// source id = 2 , 2 + 1 = 3 % 3 = 0
					// replicate = 2 + 2 = 4 % 3 = 1
					replicateId := (sourceID + int32(i)) % int32(len(s.machineRecords))
					isExist := false
					// check on datanodes that have the filerecord
					for _, node := range fileRecord.DataNodes {

						if node == replicateId {
							isExist = true
							break
						}
					}
					if !isExist && s.machineRecords[replicateId].Liveness {
						// From my machines take the IP, PORT, ID to send the file to
						replicateIPs = append(replicateIPs, s.machineRecords[replicateId].IPAddress)
						replicatePorts = append(replicatePorts, s.machineRecords[replicateId].DataNodePort)
						replicateIds = append(replicateIds, replicateId)
					} else if !s.machineRecords[replicateId].Liveness {
						log.Printf("machine %s not alive.", s.machineRecords[replicateId].IPAddress)
					}
				}
				replicateRequest := &pb.ReplicateRequest{
					FileName:    fileRecord.FileName,
					FilePath:    fileRecord.FilePaths[chosenNodeIndex],
					IpAddresses: replicateIPs,
					PortNumbers: replicatePorts,
					Ids:         replicateIds,
				}
				if s.machineRecords[sourceID].Liveness {

					addr := fmt.Sprintf("%s:%d", s.machineRecords[sourceID].IPAddress, s.machineRecords[sourceID].MasterNodePort)

					conn, err := grpc.Dial(addr, grpc.WithInsecure())
					if err != nil {
						log.Printf("Dial source data node fail %v", err)
						continue
					}
					defer conn.Close()

					sourceClient := pb.NewFileServiceClient(conn)

					_, err = sourceClient.Replicate(context.Background(), replicateRequest)
					if err != nil {
						log.Printf("Replicate fail on source Datanode machine %v", err)
						continue
					}
				}
			}
		}

		s.mutex.Unlock()
	}
}

func (s *server) monitorKeepAlive() {
	ticker := time.NewTicker(keepAliveTimeout)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.mutex.Lock()
			for nodeID, lastTime := range s.lastKeepAliveMap {

				active := time.Since(lastTime) < keepAliveTimeout
				s.machineRecords[nodeID].Liveness = active

				// log.Printf("DataNode #%d Active: %t", nodeID, active)
			}
			s.mutex.Unlock()
		}
	}
}
func (s *server) AddDataNodeMachine(DataNode_IP string, PortNumbers []string) {
	// Order of ports : []string{d.PortForMaster, d.PortForClient, d.PortForDN},
	var DataNodePorts []int32
	for _, port := range PortNumbers {
		nodePortNum, err := strconv.Atoi(port[1:])
		DataNodePorts = append(DataNodePorts, int32(nodePortNum))
		if err != nil {
			log.Fatalf("couldn't extract port number exiting %s", port)
		}
	}

	s.machineRecords = append(s.machineRecords, &MachineRecord{DataNode_IP, DataNodePorts[0],
		DataNodePorts[1], DataNodePorts[2], true})
}
func (s *server) KeepAlive(ctx context.Context, in *pb.KeepAliveRequest) (*pb.KeepAliveResponse, error) {
	var nodeID int
	nodeIP := in.DataNode_IP
	s.mutex.Lock()
	isExist := false

	for i, machinerecord := range s.machineRecords {
		if fmt.Sprintf("%s:%d", machinerecord.IPAddress,
			machinerecord.MasterNodePort) == in.DataNode_IP+in.PortNumber[0] {
			// we found the datanode that sent the heartbeat
			// && seems like it went off then on
			isExist = true
			nodeID = i
			break
		}
	}
	if !isExist {
		nodeID = len(s.machineRecords)
		s.AddDataNodeMachine(nodeIP, in.PortNumber)
	}
	// log.Printf("Data node with ID %d KeepAlive sent", nodeID)

	s.lastKeepAliveMap[nodeID] = time.Now()

	defer s.mutex.Unlock()
	return &pb.KeepAliveResponse{}, nil
}

func main() {
	grpcServer := grpc.NewServer()

	server := &server{
		fileRecords:      make(map[string]*FileRecord),
		machineRecords:   []*MachineRecord{},
		lastKeepAliveMap: make(map[int]time.Time),
	}
	go server.monitorKeepAlive()

	go server.replicationScheduler()

	pb.RegisterFileServiceServer(grpcServer, server)

	lisC, err := net.Listen("tcp", portClient)
	if err != nil {
		log.Fatalf("tcp listen fail: %v", err)
	}
	defer lisC.Close()

	lisD, err := net.Listen("tcp", portDataNode)
	if err != nil {
		log.Fatalf("tcp listen fail: %v", err)
	}
	defer lisD.Close()

	go func() {
		log.Printf("listening on %s", portClient)
		if err := grpcServer.Serve(lisC); err != nil {
			log.Fatalf("s.Serve fail %v", err)
		}
	}()

	go func() {
		log.Printf("listening on %s", portDataNode)
		if err := grpcServer.Serve(lisD); err != nil {
			log.Fatalf("s.Serve fail %v", err)
		}
	}()

	// Wait for both servers to be running
	select {} // This will keep the main goroutine alive, allowing the servers to keep running

}

// Server : Master/DataNode -> listen on port () <-
// go routine : <-
// Server/ master
