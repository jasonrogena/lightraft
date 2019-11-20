package raft

import (
	"context"
	fmt "fmt"
	"log"
	"time"

	"github.com/go-errors/errors"
	grpc "google.golang.org/grpc"
)

type proxyServer struct {
	UnimplementedProxyServiceServer
}

// ReceiveEntry is an implementation of the receive log method in the gRPC proxy server.
// ReceiveEntry is ideally called on the leader node and begins the process of writing the entry in the leader's
// log
func (s *proxyServer) ReceiveEntry(ctx context.Context, req *ReceiveEntryRequest) (*ReceiveEntryResponse, error) {
	resp := &ReceiveEntryResponse{}

	// Extract the node from the context
	node, nodeOk := ctx.Value(NAME).(*Node)
	if !nodeOk {
		return resp, errors.New("Could not get data from gRPC server")
	}

	if node.state != LEADER {
		return resp, errors.New(fmt.Sprintf("Proxied log could not be processed because expected leader is not a leader. Its state is %s and term %d", node.state, node.currentTerm))
	}

	return resp, node.addLogEntry(req.ForwardOutputToAddress, logEntry{
		id:      req.EntryID,
		command: req.Command,
	})
}

func (s *proxyServer) ReceiveCommitOutput(ctx context.Context, req *ReceiveCommitOutputRequest) (*ReceiveCommitOutputResponse, error) {
	resp := &ReceiveCommitOutputResponse{}

	// Extract the node from the context
	node, nodeOk := ctx.Value(NAME).(*Node)
	if !nodeOk {
		return resp, errors.New("Could not get data from gRPC server")
	}

	client, clientOk := node.entryClients[req.EntryID]

	if !clientOk || client == nil || !client.IsValid() {
		return resp, errors.New("Could not connect to client to deliver response")
	}

	client.WriteOutput(req.Message, req.Success)

	return resp, nil
}

func (node *Node) forwardEntry(address string, entry logEntry) error {
	conn, connErr := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if connErr != nil {
		log.Fatalf(connErr.Error())
	}
	defer conn.Close()
	client := NewProxyServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	addr, addrErr := node.getGRPCAddress(node.index)

	if addrErr != nil {
		return addrErr
	}

	_, respErr := client.ReceiveEntry(ctx, &ReceiveEntryRequest{
		EntryID:                entry.id,
		Command:                entry.command,
		ForwardOutputToAddress: addr,
	})

	return respErr
}
