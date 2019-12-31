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
		currentTerm, _ := node.getCurrentTerm()
		return resp, errors.New(fmt.Sprintf("Proxied log could not be processed because expected leader is not a leader. Its state is %s and term %d", node.state, currentTerm))
	}

	return resp, node.addLogEntry(
		stateMachineClient{
			clientType: cluster,
			address:    req.ForwardOutputToAddress,
		}, req.Entry)
}

// ReceiveCommitOutput is an implementation of the receive commit output method in the gRPC proxy server.
// ReceiveCommitOutput is ideally called on the follower node that had sent a ReceiveEntry gRPC to its leader
func (s *proxyServer) ReceiveCommitOutput(ctx context.Context, req *ReceiveCommitOutputRequest) (*ReceiveCommitOutputResponse, error) {
	resp := &ReceiveCommitOutputResponse{}

	// Extract the node from the context
	node, nodeOk := ctx.Value(NAME).(*Node)
	if !nodeOk {
		return resp, errors.New("Could not get data from gRPC server")
	}

	sendCommandErr := node.sendCommandOutputToClient(req.EntryID, req.Message, req.Success)

	return resp, sendCommandErr
}

func (node *Node) forwardEntry(leaderAddr string, tcpClient Client, entry *LogEntry) error {
	conn, connErr := grpc.Dial(leaderAddr, grpc.WithInsecure(), grpc.WithBlock())
	if connErr != nil {
		log.Fatalf(connErr.Error())
	}
	defer conn.Close()
	grpcClient := NewProxyServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	addr, addrErr := node.getGRPCAddress(node.index)

	if addrErr != nil {
		return addrErr
	}

	node.registerStateMachineClient(
		stateMachineClient{
			clientType: tcp,
			address:    tcpClient,
		},
		entry.Id)

	_, respErr := grpcClient.ReceiveEntry(ctx, &ReceiveEntryRequest{
		Entry:                  entry,
		ForwardOutputToAddress: addr,
	})

	return respErr
}

func (node *Node) forwardCommitOutput(followerAddr string, entryID string, message string, success bool) error {
	conn, connErr := grpc.Dial(followerAddr, grpc.WithInsecure(), grpc.WithBlock())
	if connErr != nil {
		log.Fatalf(connErr.Error())
	}
	defer conn.Close()
	grpcClient := NewProxyServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, respErr := grpcClient.ReceiveCommitOutput(ctx, &ReceiveCommitOutputRequest{
		EntryID: entryID,
		Message: message,
		Success: success,
	})

	return respErr
}
