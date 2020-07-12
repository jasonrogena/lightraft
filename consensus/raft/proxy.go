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

	curTerm, curTermErr := node.getCurrentTerm()
	if curTermErr != nil {
		return resp, fmt.Errorf("Could not add log entry: %w", curTermErr)
	}

	if node.state != LEADER {
		return resp, errors.New(fmt.Sprintf("Proxied log could not be processed because expected leader is not a leader. Its state is %s and term %d", node.state, curTerm))
	}

	// Make sure the term is set to the lead's current term
	req.Entry.Term = curTerm

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

	regClientErr := node.registerStateMachineClient(
		stateMachineClient{
			clientType: tcp,
			address:    tcpClient,
		},
		entry.Id)
	if regClientErr != nil {
		return regClientErr
	}

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
