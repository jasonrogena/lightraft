package main

import (
	"github.com/firstrow/tcp_server"
)

type nodeToMainInterface interface {
	Exit() error
	Init() error
	RegisterNeighbor(neighbor interface{}) error
}

func main() {
	// 1. Initialize the node
	// 2. Make sure node can talk to other nodes
	// 3. Start listening on TCP port
	startListening()
}

// startListening binds to the TCP port and starts listening for client connections
func startListening() {
	// TODO: remove hardcoded bind host and port
	server := tcp_server.New("localhost:7654")

	server.OnNewClient(func(client *tcp_server.Client) {
		// new client connected
		// TODO: remove hardcoded node ID
		client.Send(ansiLogo + "Connected to node 0\n\n")
	})
	server.OnNewMessage(func(client *tcp_server.Client, message string) {
		// new message received
		client.Send("Received:\n--------\n" + message + "--------\n")
	})
	server.OnClientConnectionClosed(func(client *tcp_server.Client, err error) {
		// connection with client lost
	})

	server.Listen()
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
