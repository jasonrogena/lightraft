package raft

import (
	context "context"
	"log"
	"time"

	"github.com/jasonlvhit/gocron"
	"github.com/pkg/errors"
	grpc "google.golang.org/grpc"
)

type appendEntriesResposeHandler func(*AppendEntriesResponse, error)

type replicationServer struct {
	UnimplementedReplicationServiceServer
}

func (node *Node) resetHeartbeatTimer() {
	if !node.stopHeartbeatTimer() {
		node.heartbeatScheduler = gocron.NewScheduler()
		go func() {
			<-node.heartbeatScheduler.Start()
		}()
	}

	node.heartbeatScheduler.Every(node.getHeartbeatTimeoutDuration()).Seconds().Do(node.sendHeartbeat)
}

func (node *Node) stopHeartbeatTimer() bool {
	if node.heartbeatScheduler != nil {
		node.heartbeatScheduler.Clear()
		return true
	}

	return false
}

func (node *Node) sendHeartbeat() {
	node.sendEntriesToAllNodes(make([]string, 0))
	log.Println("Heartbeat sent")
}

func (node *Node) getHeartbeatTimeoutDuration() uint64 {
	// TODO: Use the recommended method for determining how long timeout should be
	return 1
}

// sendEntriesAllNodes sends all the provided log entries to followers
func (node *Node) sendEntriesToAllNodes(entries []string) error {
	prevLogIndex, prevLogTerm, prevLogErr := node.getLastLogEntryDetails()
	if prevLogErr != nil {
		return prevLogErr
	}

	lastCommitIndex, lastCommitErr := node.getLastCommittedLogEntryIndex()
	if lastCommitErr != nil {
		return lastCommitErr
	}

	currentTerm, currentTermErr := node.getCurrentTerm()
	if currentTermErr != nil {
		return currentTermErr
	}

	req := AppendEntriesRequest{
		Term:         currentTerm,
		LeaderID:     node.getID(),
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: lastCommitIndex,
	}
	for curIndex := 0; curIndex < len(node.config.Nodes); curIndex++ {
		if curIndex == node.index {
			continue
		}

		address, addrErr := node.getGRPCAddress(curIndex)
		if addrErr != nil {
			log.Fatalf(addrErr.Error())
			continue
		}

		go node.sendAppendEntriesRequest(address, req, func(resp *AppendEntriesResponse, err error) {
			// TODO: Implement incrementing count of number of nodes that have successfully added log entry
			// When count is greater than half of the nodes, commit the log entry
			if err != nil {
				log.Printf("Remote node returned the error while appending entries:%+v", errors.Wrap(err, ""))
			}
		})
	}

	return nil
}

func (node *Node) sendAppendEntriesRequest(address string, req AppendEntriesRequest, handler appendEntriesResposeHandler) {
	conn, connErr := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if connErr != nil {
		log.Fatalf(connErr.Error())
	}
	defer conn.Close()
	client := NewReplicationServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	resp, respErr := client.AppendEntries(ctx, &req)
	handler(resp, respErr)
}

// AppendEntries is an implementation of the append entries method in the gRPC replication service
func (s *replicationServer) AppendEntries(ctx context.Context, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	resp := AppendEntriesResponse{
		Term:    req.Term,
		Success: false,
	}

	// Extract the node from the context
	node, nodeOk := ctx.Value(NAME).(*Node)
	if !nodeOk {
		return &resp, errors.New("Could not get data from gRPC server")
	}

	leaderErr := node.registerLeader(req.LeaderID, req.Term)
	if leaderErr != nil {
		return &resp, leaderErr
	}

	// Workaround for timer bug
	node.lastHeartbeatTimestamp = time.Now().UnixNano()
	// end workaround

	for _, curEntry := range req.Entries {
		addr, addrErr := node.getGRPCAddressFromID(req.LeaderID)
		if addrErr != nil {
			return &resp, addrErr
		}

		node.addLogEntry(addr, logEntry{
			id:      node.generateEntryID(),
			command: curEntry,
		})
	}
	// TODO: implement commiting logs up to last committed entry
	resp.Success = true
	return &resp, nil
}

// addLogEntry inserts an entry into the log
func (node *Node) addLogEntry(sourceAddress string, entry logEntry) error {
	curTerm, curTermErr := node.getCurrentTerm()
	if curTermErr != nil {
		return curTermErr
	}

	_, inErr := node.metaDB.RunRawWriteQuery(`INSERT INTO log (id, term, committed, command)
		VALUES ($1, $2, $3, $4)`, entry.id, curTerm, 0, entry.command)
	return inErr
}
