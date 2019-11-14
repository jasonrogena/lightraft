package main

import (
	"log"
	"os"
	"strconv"

	"github.com/firstrow/tcp_server"
	"github.com/jasonrogena/lightraft/configuration"
	"github.com/jasonrogena/lightraft/consensus/raft"
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
		nodeIndex, parseErr := strconv.ParseInt(os.Args[1], 10, 64)
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
		startListening(config, nodeIndex)
	} else {
		log.Fatalln(getHelp())
	}
}

// startListening binds to the TCP port and starts listening for client connections
func startListening(config configuration.Config, nodeIndex int64) {
	//persistenceInterface := persistence.Init(&config, nodeIndex)

	if int64(len(config.Nodes)) > nodeIndex {
		raftNode := raft.NewNode(nodeIndex)
		raftNode.GetElectionTimer()
		tcpServer := tcp_server.New(config.Nodes[nodeIndex].BindAddress + ":" + strconv.Itoa(config.Nodes[nodeIndex].BindPort))
		tcpServer.OnNewClient(func(client *tcp_server.Client) {
			// new client connected
			client.Send(ansiLogo + "Connected to node " + strconv.FormatInt(nodeIndex, 10) + "\n\n")
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
	} else {
		log.Printf("Number nodes %d\n", len(config.Nodes))
		log.Fatalf("No node with index %d\n", nodeIndex)
	}
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
