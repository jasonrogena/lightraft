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
	//log.Println("Heartbeat sent")
}

func (node *Node) getHeartbeatTimeoutDuration() uint64 {
	// TODO: Use the recommended method for determining how long timeout should be
	return 1
}

// sendEntriesAllNodes sends all the provided log entries to followers
func (node *Node) sendEntriesToAllNodes(entries []*LogEntry) error {
	var firstIdx int64 = -1
	if len(entries) > 0 {
		rawRowData, rawRowDataErr := node.metaDB.RunSelectQuery(`SELECT idx FROM log WHERE id = $1`, false, entries[0].Id)
		if rawRowDataErr != nil {
			return fmt.Errorf("Could not get the index of entry with ID '%s': %w", entries[0].Id, rawRowDataErr)
		}

		rowData := rawRowData.([][]interface{})
		firstIdxPtr, assertIdxErr := rowData[0][0].(*int64)
		if !assertIdxErr {
			return fmt.Errorf("Could not assert the type of the data returned as the index of the entry with ID '%s' is *int64: %w", entries[0].Id, assertIdxErr)
		}

		firstIdx = *firstIdxPtr
	}

	prevLogIndex, prevLogTerm, prevLogErr := node.getLastLogEntryDetails(firstIdx)
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
		state, stateOK := node.logEntryStates[curStatus.LogEntryID]
		if !stateOK {
			log.Printf("Could not record entry with ID %s was recorded in follower because count of number of records for this entry doesn't exist\n", curStatus.LogEntryID)
			continue
		}

		state.incrementNumberNodesWithEntry()

		if !node.isValueGreaterThanHalfNodes(state.getNumberNodesWithEntry()) {
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
	state, stateOK := node.logEntryStates[id]
	if !stateOK {
		return fmt.Errorf("Could not get the state details for log entry with ID '%s'. Cannot commit entry", id)
	}

	state.commitLock.Lock()
	defer state.commitLock.Unlock()
	log.Printf("Checking if we can commit log entry with ID '%s'\n", id)

	// Check if entry has already been committed
	data, dataErr := node.metaDB.RunSelectQuery(`SELECT committed, command FROM log WHERE id = $1`, false, id)
	if dataErr != nil {
		return fmt.Errorf("Error occurred when checking if log entry with ID '%s' is committed: %w", id, dataErr)
	}

	dataC := data.([][]interface{})
	if len(dataC) != 1 {
		return fmt.Errorf("Was expecting 1 row of log entry data for entry with ID '%s', but %d returned", id, len(dataC))
	}

	if *dataC[0][0].(*int64) == 1 {
		log.Printf("Log entry with ID '%s' already committed, not commiting it again\n", id)
		return nil
	}

	// Send command to state machine
	output, outputErr := node.stateMachine.Commit(*dataC[0][1].(*string))
	formattedOutput, outputOK := node.formatOutputToClient(output, outputErr)
	node.sendCommandOutputToClient(id, formattedOutput, outputOK)

	_, updateCommitError := node.metaDB.RunWriteQuery(`UPDATE log SET committed = 1 WHERE id = $1`, id)

	log.Printf("Finishing commit sequence for log entry with ID '%s'\n", id)

	// TODO: Update node.commitIndex

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
		Term:    req.Term,
		Success: false,
	}

	// Extract the node from the context
	node, nodeOk := ctx.Value(NAME).(*Node)
	if !nodeOk {
		return &resp, errors.New("Could not get data from gRPC server")
	}

	curTerm, curTermErr := node.getCurrentTerm()
	if curTermErr != nil {
		return &resp, fmt.Errorf("Could not determine the term for the node: %w", curTermErr)
	}

	if req.Term < curTerm {
		return &resp, fmt.Errorf("The term in the AppendEntries request (%d) is less than the node's term (%d)", req.Term, curTerm)
	}

	if req.PrevLogIndex > 0 && req.PrevLogTerm > 0 {
		prevLogEntryStatus, prevLogEntryStatusErr := node.doesLogEntryWithDetailsExist(req.PrevLogIndex, req.PrevLogTerm)
		if prevLogEntryStatusErr != nil {
			return &resp, fmt.Errorf("Could not determine if previous log entry exists in node: %w", prevLogEntryStatusErr)
		}

		if !prevLogEntryStatus {
			return &resp, fmt.Errorf("Previous log entry with index %d and term %d is not available in node", req.PrevLogIndex, req.PrevLogTerm)
		}
	}

	// TODO: should we register the leader at this point?
	leaderErr := node.registerLeader(req.LeaderID, req.Term)
	if leaderErr != nil {
		return &resp, leaderErr
	}

	// Workaround for timer bug
	node.lastHeartbeatTimestamp = time.Now().UnixNano()
	// end workaround
	var statuses []*LogEntryReplicationStatus
	for _, curEntry := range req.Entries {
		curStatus := LogEntryReplicationStatus{
			LogEntryID: curEntry.Id,
			Success:    true,
		}

		existingEntry, existingEntryErr := node.getLogEntryWithIndex(curEntry.Index)
		if existingEntryErr != nil {
			curStatus.Success = false
			log.Printf("Could not add log entry because of error: %w", existingEntryErr)
			break
		}
		if len(existingEntry.Id) > 0 {
			if existingEntry.Term != curEntry.Term { // There's a conflict
				// Delete the existing entry and all entries that follow it
				deleteEntryErr := node.deleteLogEntriesFromIndex(curEntry.Index)
				if deleteEntryErr != nil {
					curStatus.Success = false
					log.Printf("Could not add log entry because of error: %w", deleteEntryErr)
					break
				}
			} else { // Entry already exists in log
				log.Printf("Log entry with index '%d' already exists in log. Not adding it", curEntry.Index)
				continue
			}
		}

		addr, addrErr := node.getGRPCAddressFromID(req.LeaderID)
		if addrErr != nil {
			curStatus.Success = false
			log.Printf("Could not add log entry because of error: %w", addrErr)
			break
		}

		entryErr := node.addLogEntry(
			stateMachineClient{
				clientType: cluster,
				address:    addr,
			}, curEntry)
		if entryErr != nil {
			curStatus.Success = false
			log.Printf("Could not add log entry because of error: %w", entryErr)
			break
		}

		statuses = append(statuses, &curStatus)
	}

	// Commit logs up to last committed entry
	commitErr := node.commitUpToIndex(req.LeaderCommit)
	if commitErr != nil {
		log.Printf("An error occurred while trying to commit log entries up to index %d: %v\n", req.LeaderCommit, commitErr)
	}

	resp.EntryStatuses = statuses
	resp.Success = true
	return &resp, nil
}

// commitUpToIndex attempts to commit log entries up to the provided index
func (node *Node) commitUpToIndex(index int64) error {
	data, dataErr := node.metaDB.RunSelectQuery(`SELECT id FROM log WHERE committed = 0 AND idx <= $1 ORDER BY id ASC`, false, index)
	if dataErr != nil {
		return fmt.Errorf("Error occurred when getting uncommitted entries up to '%d' is committed: %w", index, dataErr)
	}

	dataC := data.([][]interface{})
	for i := 0; i < len(dataC); i++ {
		commitErr := node.commitLogEntry(*dataC[i][0].(*string))

		if commitErr != nil {
			return commitErr
		}
	}

	return nil
}

// addLogEntry inserts an entry into the log. The function is also responsible for saving the source address
// in memory (if the current node is a leader)
func (node *Node) addLogEntry(sourceAddress stateMachineClient, entry *LogEntry) error {
	node.logEntryStates[entry.Id] = &logEntryState{
		numberNodesWithEntry: 0,
	}

	switch entry.Index {
	case -1:
		_, inErr := node.metaDB.RunRawWriteQuery(`INSERT INTO log (id, idx, term, committed, command)
		VALUES ($1, (SELECT IFNULL(MAX(idx), 0) + 1 FROM log), $2, $3, $4)`, entry.Id, entry.Term, 0, entry.Command)
		if inErr != nil {
			return inErr
		}
	default:
		_, inErr := node.metaDB.RunRawWriteQuery(`INSERT INTO log (id, idx, term, committed, command)
		VALUES ($1, $2, $3, $4, $5)`, entry.Id, entry.Index, entry.Term, 0, entry.Command)
		if inErr != nil {
			return inErr
		}
	}

	// Register the source address so that the command output will be sent back to it whenever the command
	// is executed in the state machine
	if node.state == LEADER {
		regClientErr := node.registerStateMachineClient(sourceAddress, entry.Id)
		if regClientErr != nil {
			return regClientErr
		}
	}

	node.logEntryStates[entry.Id].incrementNumberNodesWithEntry()

	return nil
}
