package main

import (
	"context"
	"log"
	"net"
	"os"
	"strconv"

	"github.com/firstrow/tcp_server"
	"github.com/jasonrogena/lightraft/configuration"
	"github.com/jasonrogena/lightraft/consensus/raft"
	"google.golang.org/grpc"
)

type consensusInterface interface {
	Exit() error
	Init() error
	RegisterNeighbor(neighbor interface{}) error
}

type persistenceInterface interface {
	IsQueryUpdate(query string) (bool, error)
	TryRead(query string) (string, error)
}

func main() {
	if len(os.Args) == 2 {
		nodeIndex, parseErr := strconv.Atoi(os.Args[1])
		if parseErr != nil {
			log.Fatalln(parseErr)
		}

		config, configErr := configuration.GetConfig()
		if configErr != nil {
			log.Fatalln(configErr)
		}
		// 1. Initialize the node
		// 2. Make sure node can talk to other nodes
		// 3. Start listening on TCP port
		startListening(&config, nodeIndex)
	} else {
		log.Fatalln(getHelp())
	}
}

// startListening binds to the TCP port and starts listening for client connections
func startListening(config *configuration.Config, nodeIndex int) {
	//persistenceInterface := persistence.Init(&config, nodeIndex)

	if len(config.Nodes) > nodeIndex {
		// Initialize RAFT node
		raftNode := raft.NewNode(nodeIndex, config)

		// Initialize the gRPC Server
		go initGRPCServer(raftNode, nodeIndex, config)

		// Initialize tcp port for clients to hook up to
		initTCPServer(raftNode, nodeIndex, config)
	} else {
		log.Printf("Number nodes %d\n", len(config.Nodes))
		log.Fatalf("No node with index %d\n", nodeIndex)
	}
}

func initGRPCServer(raftNode *raft.Node, nodeIndex int, config *configuration.Config) {
	// Initialize gRPC server
	grpcListener, grpcErr := net.Listen("tcp", ":"+strconv.Itoa(config.Nodes[nodeIndex].RPCBindPort))
	if grpcErr != nil {
		log.Fatalf(grpcErr.Error())
	}
	grpcServer := grpc.NewServer(getGRPCServerUnaryInterceptor(raftNode))
	raftNode.RegisterGRPCHandlers(grpcServer)

	if err := grpcServer.Serve(grpcListener); err != nil {
		log.Fatalf(err.Error())
	}
}

func initTCPServer(raftNode *raft.Node, nodeIndex int, config *configuration.Config) {
	tcpServer := tcp_server.New(config.Nodes[nodeIndex].ClientBindAddress + ":" + strconv.Itoa(config.Nodes[nodeIndex].ClientBindPort))
	tcpServer.OnNewClient(func(client *tcp_server.Client) {
		// new client connected
		client.Send(ansiLogo + "Connected to node " + strconv.Itoa(nodeIndex) + "\n\n")
	})
	tcpServer.OnNewMessage(func(client *tcp_server.Client, message string) {
		// Check if message is an update
		// if persistenceInterface.IsQueryUpdate(message) {

		// } else {

		// }
	})
	tcpServer.OnClientConnectionClosed(func(client *tcp_server.Client, err error) {
		// connection with client lost
	})

	tcpServer.Listen()
}

// getGRPCServerUnaryInterceptor injects the relevant objects (including the raftNode)
// into the context passed into the gRPC handlers
// You can use this function to add middlewares like authentication
func getGRPCServerUnaryInterceptor(raftNode *raft.Node) grpc.ServerOption {
	return grpc.UnaryInterceptor(func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		ctx = context.WithValue(ctx, raft.NAME, raftNode)
		return handler(ctx, req)
	})
}

func getHelp() string {
	return "Usage: " + os.Args[0] + " <node index>"
}

func tellNode() {
	// You should be able to tell node to:
	// 1. "Exit" cluster
	// 2. Initialize
	// 3. Register a neighbor
}

var ansiLogo = `
╦  ┬┌─┐┬ ┬┌┬┐┬─┐┌─┐┌─┐┌┬┐
║  ││ ┬├─┤ │ ├┬┘├─┤├┤  │ 
╩═╝┴└─┘┴ ┴ ┴ ┴└─┴ ┴└   ┴ 
`
