package raft

import (
	context "context"
	fmt "fmt"
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
	node.sendEntriesToAllNodes(make([]*LogEntry, 0))
	log.Println("Heartbeat sent")
}

func (node *Node) getHeartbeatTimeoutDuration() uint64 {
	// TODO: Use the recommended method for determining how long timeout should be
	return 1
}

// sendEntriesAllNodes sends all the provided log entries to followers
func (node *Node) sendEntriesToAllNodes(entries []*LogEntry) error {
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
			if err != nil {
				log.Printf("Remote node returned the error while appending entries:%+v", errors.Wrap(err, ""))
				return
			}

			node.recordAppendEntryResponse(resp.EntryStatuses)
		})
	}

	return nil
}

// recordAppendEntryResponse records whether a follower has successfully recorded a log entry and commits the entry if
// it turns out that more than half of the nodes have successfully recorded the entry
func (node *Node) recordAppendEntryResponse(entryStatuses []*LogEntryReplicationStatus) {
	for _, curStatus := range entryStatuses {
		_, statusOk := node.recordedLogEntries[curStatus.LogEntryID]
		if !statusOk {
			log.Printf("Could not record entry with ID %s was recorded in follower because count of number of records for this entry doesn't exist\n", curStatus.LogEntryID)
			continue
		}

		node.recordedLogEntries[curStatus.LogEntryID] = node.recordedLogEntries[curStatus.LogEntryID] + 1

		if !node.isValueGreaterThanHalfNodes(node.recordedLogEntries[curStatus.LogEntryID]) {
			continue
		}

		// Commit the log entry
		commitErr := node.commitLogEntry(curStatus.LogEntryID)

		if commitErr != nil {
			log.Printf("Could not record entry with ID %s because of error: %w\n", curStatus.LogEntryID, commitErr)
			continue
		}

		// TODO: send AppendEntries request to all nodes
	}
}

func (node *Node) commitLogEntry(id string) error {
	// Check if entry has already been committed
	data, dataErr := node.metaDB.RunSelectQuery(`SELECT committed, command FROM log WHERE id = $1`, false, id)
	if dataErr != nil {
		return fmt.Errorf("Error occurred when checking if log entry with id '%s' is committed: %w", id, dataErr)
	}

	dataC := data.([][]interface{})
	if len(dataC) != 1 {
		return fmt.Errorf("Was expecting 1 row of log entry data for entry with id '%s', but %d returned", id, len(dataC))
	}

	if *dataC[0][0].(*int) == 1 {
		log.Printf("Log entry with id '%s' already committed, not commiting it again\n", id)
		return nil
	}

	// Send command to state machine
	output, outputErr := node.stateMachine.Commit(*dataC[0][1].(*string))

	node.sendCommandOutputToClient(id, output, true)
	if outputErr != nil {
		node.sendCommandOutputToClient(id, outputErr.Error(), false)
	}

	_, updateCommitError := node.metaDB.RunWriteQuery(`UPDATE log SET committed = 1 WHERE id = $1`, id)

	return updateCommitError
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
		Term: req.Term,
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
	var statuses []*LogEntryReplicationStatus
	for _, curEntry := range req.Entries {
		addr, addrErr := node.getGRPCAddressFromID(req.LeaderID)
		curStatus := LogEntryReplicationStatus{
			LogEntryID: curEntry.Id,
			Success:    true,
		}
		if addrErr != nil {
			curStatus.Success = false
			log.Printf("Could not add log entry because of error \n\t%s", addrErr.Error())
		}

		entryErr := node.addLogEntry(
			stateMachineClient{
				clientType: cluster,
				address:    addr,
			}, curEntry)
		if entryErr != nil {
			curStatus.Success = false
			log.Printf("Could not add log entry because of error \n\t%s", entryErr.Error())
		}

		statuses = append(statuses, &curStatus)
	}
	// TODO: implement commiting logs up to last committed entry
	resp.EntryStatuses = statuses
	return &resp, nil
}

// addLogEntry inserts an entry into the log. The function is also responsible for saving the source address
// in memory (if the current node is a leader)
func (node *Node) addLogEntry(sourceAddress stateMachineClient, entry *LogEntry) error {
	curTerm, curTermErr := node.getCurrentTerm()
	if curTermErr != nil {
		return curTermErr
	}

	_, inErr := node.metaDB.RunRawWriteQuery(`INSERT INTO log (id, term, committed, command)
		VALUES ($1, $2, $3, $4)`, entry.Id, curTerm, 0, entry.Command)
	if inErr != nil {
		return inErr
	}

	// Register the source address so that the command output will be sent back to it whenever the command
	// is executed in the state machine
	if node.state == LEADER {
		node.registerStateMachineClient(sourceAddress, entry.Id)
	}

	return nil
}
