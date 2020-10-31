package raft

import (
	"errors"
	fmt "fmt"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jasonlvhit/gocron"
	"github.com/jasonrogena/lightraft/configuration"
	"github.com/jasonrogena/lightraft/consensus"
	persistence "github.com/jasonrogena/lightraft/persistence/sqlite"
	grpc "google.golang.org/grpc"
)

type State int

const (
	// FOLLOWER state is the default state. Node cannot control writing to its log in this state
	FOLLOWER State = 1 << iota

	// CANDIDATE state shows node offering itself to be elected
	CANDIDATE

	// LEADER state allows controlling the log in the cluster
	LEADER
)

type Client interface {
	WriteOutput(message string, success bool)
	IsValid() bool
}

// Flow diagram showing how to transition between node states
//
//                              times
//                               out    receives
//  starts            times      ___    majority
//    up               out      |   \     vote
// -------> FOLLOWER -------> CANDIDATE -------> LEADER
//            ^ ^                  /               /
//             \ \----------------/               /
//              \ discovers leader               /
//               \  or new term                 /
//                \                            /
//                 ----------------------------
//                       discovers node with
//                        a higher term

type Node struct {
	// Volatile on all nodes
	commitIndex int64 // Index of highest log entry known to be committed. Initialized to 0
	lastApplied int64 // Index of highest log entry applied to the state machine

	// Volatile on leader
	// Should be reinitialized after election
	nextIndex  map[string]int64 // For each node, index of the next log entry to send to that node. Initialized to leader last log index + 1
	matchIndex map[string]int64 // For each node, index of the highest log entry known to be replicated on server. Initialized to 0

	// Volatile, added by me
	state                  State // Default to FOLLOWER
	stateMachine           consensus.StateMachine
	electionTimer          *time.Timer
	heartbeatScheduler     *gocron.Scheduler
	lastHeartbeatTimestamp int64 // The unix epoch (in seconds) for the last time the leader sent a heartbeat
	index                  int
	candidateTermVote      map[int64]string // Map of terms and IDs of candidates that this node voted for in each of the terms. ID will be nil if node hasn't voted
	termVoteCount          map[int64]int    // Map of terms and number of votes this node got for each of the terms
	config                 *configuration.Config
	metaDB                 *persistence.Driver
	logEntryStates         map[string]*logEntryState
}

// logEntryState is for storing the volatile state of a log entry
type logEntryState struct {
	commitLock               sync.Mutex // Mutual exclusion locks corresponding to each of the entries to be used when committing the log entry to the state machine
	client                   stateMachineClient
	numberNodesWithEntry     int
	numberNodesWithEntryLock sync.Mutex
}

func (state *logEntryState) incrementNumberNodesWithEntry() {
	state.numberNodesWithEntryLock.Lock()
	state.numberNodesWithEntry = state.numberNodesWithEntry + 1
	state.numberNodesWithEntryLock.Unlock()
}

func (state *logEntryState) getNumberNodesWithEntry() int {
	state.numberNodesWithEntryLock.Lock()
	number := state.numberNodesWithEntry
	state.numberNodesWithEntryLock.Unlock()

	return number
}

// stateMachineClient holds the details of a client from where
// a command has been gotten from. This can either be a client connected
// to the node's port or another node in the cluster that has proxied the command
// to the leader node
type stateMachineClient struct {
	clientType clientType
	address    interface{}
}

type clientType int

const (
	// cluster clientType stands for nodes in the Raft consensus cluster
	cluster clientType = 1 << iota
	// tcp clientType stands for clients connected to the non-gRPC TCP port of a node
	tcp
)

const NAME string = "raft-node"

// NewNode should be called whenever the node is initialized
// gets saved node data from the database and initializes ephemeral node data
func NewNode(index int, config *configuration.Config, stateMachine consensus.StateMachine) (*Node, error) {
	node := new(Node)
	metaDb, dbErr := initMetaDB(config, index)
	if dbErr != nil {
		return nil, dbErr
	}
	node.metaDB = metaDb

	// Init volatile data
	node.stateMachine = stateMachine
	node.index = index
	node.config = config
	node.commitIndex = 0
	node.lastApplied = 0
	node.setState(FOLLOWER)
	node.candidateTermVote = make(map[int64]string)
	node.termVoteCount = make(map[int64]int)
	node.logEntryStates = make(map[string]*logEntryState)

	return node, nil
}

func (node *Node) getID() string {
	return node.config.Cluster.Name + strconv.Itoa(node.index)
}

// initMetaDB initializes the meta database (where the non-volatile data for the node, including the log, are stored)
func initMetaDB(config *configuration.Config, nodeIndex int) (*persistence.Driver, error) {
	dbPath := config.Cluster.Name + "-" + strconv.Itoa(nodeIndex) + ".meta" + persistence.EXTENSION
	metaDB, metaDBErr := persistence.NewDriver(dbPath)
	if metaDBErr != nil {
		return nil, metaDBErr
	}

	_, nodeErr := metaDB.RunWriteQuery(`CREATE TABLE IF NOT EXISTS node (
		idx INTEGER PRIMARY KEY AUTOINCREMENT,
		currentTerm INTEGER,
		votedFor TEXT)`)
	if nodeErr != nil {
		return nil, fmt.Errorf("Could not create the node table: %w", nodeErr)
	}
	_, inNodeErr := metaDB.RunWriteQuery(`INSERT OR IGNORE INTO node(idx, currentTerm)
	VALUES ($1, $2)`, nodeIndex, 0)
	if inNodeErr != nil {
		return nil, fmt.Errorf("Could not insert the default term into the node table: %w", inNodeErr)
	}

	_, logErr := metaDB.RunWriteQuery(`CREATE TABLE IF NOT EXISTS log (
		id TEXT PRIMARY KEY,
		idx INTEGER,
		term INTEGER,
		committed INTEGER,
		command TEXT NOT NULL,
		UNIQUE(idx, term) ON CONFLICT ROLLBACK)`)
	if logErr != nil {
		return nil, fmt.Errorf("Could not create the log table: %w", logErr)
	}

	return metaDB, nil
}

func (node *Node) setCurrentTerm(newTerm int64) error {
	result, inErr := node.metaDB.RunRawWriteQuery(`UPDATE node SET currentTerm = $1 WHERE idx = $2`, newTerm, node.index)
	rowsAffected, rowsAffectedErr := result.RowsAffected()
	errString := "An error occurred while trying to update currentTerm: %w"
	if rowsAffectedErr != nil {
		return fmt.Errorf(errString, rowsAffectedErr)
	}
	if rowsAffected != 1 {
		return fmt.Errorf(errString, inErr)
	}

	return nil
}

func (node *Node) getCurrentTerm() (int64, error) {
	data, dataErr := node.metaDB.RunSelectQuery(`SELECT currentTerm FROM node WHERE idx = $1`, false, node.index)
	if dataErr != nil {
		return -1, fmt.Errorf("Error occurred when getting the current term: %w", dataErr)
	}

	dataC := data.([][]interface{})
	if len(dataC) != 1 {
		return -1, fmt.Errorf("Was expecting 1 row of node data, but %d returned", len(dataC))
	}

	dataAsserted, dataOK := dataC[0][0].(*int64)
	if !dataOK {
		return -1, fmt.Errorf("Could not convert %v to *int64 while getting current term", dataC[0][0])
	}

	return *dataAsserted, nil
}

func (node *Node) setVotedFor(leaderID string) error {
	result, inErr := node.metaDB.RunRawWriteQuery(`UPDATE node SET votedFor = $1 WHERE idx = $2`, leaderID, node.index)
	rowsAffected, rowsAffectedErr := result.RowsAffected()
	errString := "An error occurred while trying to update votedFor: %w"
	if rowsAffectedErr != nil {
		return fmt.Errorf(errString, rowsAffectedErr)
	}
	if rowsAffected != 1 {
		return fmt.Errorf(errString, inErr)
	}

	return nil
}

func (node *Node) getVotedFor() (string, error) {
	data, dataErr := node.metaDB.RunSelectQuery(`SELECT votedFor FROM node WHERE idx = $1`, false, node.index)
	if dataErr != nil {
		return "", fmt.Errorf("An error occurred while trying to get votedFor: %w", dataErr)
	}

	dataC := data.([][]interface{})
	if len(dataC) != 1 {
		return "", fmt.Errorf("Was expecting 1 row of node data, but %d returned", len(dataC))
	}

	dataAsserted, dataOK := dataC[0][0].(*string)
	if !dataOK {
		return "", fmt.Errorf("Could not convert %v to *string while getting ID for node that was voted for", dataC[0][0])
	}

	return *dataAsserted, nil
}

// RegisterGRPCHandlers adds handlers to the provided gRPC server
func (node *Node) RegisterGRPCHandlers(grpcServer *grpc.Server) {
	RegisterElectionServiceServer(grpcServer, &electionServer{})
	RegisterReplicationServiceServer(grpcServer, &replicationServer{})
	RegisterProxyServiceServer(grpcServer, &proxyServer{})
}

// becomeLeader resets the node data for the leader to what is recommended when the leader is
// first elected
func (node *Node) becomeLeader() {
	node.nextIndex = make(map[string]int64)
	node.matchIndex = make(map[string]int64)

	node.stopElectionTimer()
	node.sendHeartbeat()
	node.resetHeartbeatTimer()
	currentTerm, _ := node.getCurrentTerm()
	log.Printf("Node %d is now leader in term %d\n", node.index, currentTerm)
}

// getAddress returns the gRPC address for node with the corresponding index
func (node *Node) getGRPCAddress(nodeIndex int) (string, error) {
	if len(node.config.Nodes) > nodeIndex {
		return node.config.Nodes[nodeIndex].RPCBindAddress + ":" + strconv.Itoa(node.config.Nodes[nodeIndex].RPCBindPort), nil
	}

	return "", errors.New(fmt.Sprintf("No node with specified index %d", nodeIndex))
}

// getGRPCAddressFromID extracts the address from the provided node ID
func (node *Node) getGRPCAddressFromID(nodeID string) (string, error) {
	indexString := strings.Replace(nodeID, node.config.Cluster.Name, "", -1)
	index, intErr := strconv.Atoi(indexString)
	if intErr != nil {
		return "", intErr
	}

	addr, addrErr := node.getGRPCAddress(index)
	return addr, addrErr
}

func (node *Node) getLeaderGRPCAddress() (string, error) {
	votedFor, votedForErr := node.getVotedFor()
	if votedForErr != nil {
		return "", votedForErr
	}

	leaderAddr, leaderAddrErr := node.getGRPCAddressFromID(votedFor)
	return leaderAddr, leaderAddrErr
}

func (node *Node) becomeFollower() {
	node.stopHeartbeatTimer()
	node.resetElectionTimer()
	currentTerm, _ := node.getCurrentTerm()
	votedFor, _ := node.getVotedFor()
	log.Printf("Node %d is now follower in term %d. Leader is %s\n", node.index, currentTerm, votedFor)
}

func (node *Node) becomeCandidate() {
	node.stopHeartbeatTimer()
	node.resetElectionTimer()
	currentTerm, _ := node.getCurrentTerm()
	log.Printf("Node %d is now candidate in term %d\n", node.index, currentTerm)
}

// setState updates the state of the node. Function will throw an error if you try to update a
// node's state to one it's already in
func (node *Node) setState(newState State) error {
	node.state = newState

	switch node.state {
	case LEADER:
		node.becomeLeader()
	case FOLLOWER:
		node.becomeFollower()
	case CANDIDATE:
		node.becomeCandidate()
	}

	return nil
}

func (node *Node) IngestCommand(client Client, command string) {
	if !node.stateMachine.RequiresConsensus(command) {
		output, outputErr := node.stateMachine.Commit(command)
		node.sendMessageToTCPClient(client, output, outputErr)

		return
	}

	id := node.generateEntryID()
	entry := &LogEntry{
		Id:      id,
		Command: command,
		Index:   -1,
	}

	switch node.state {
	case LEADER:
		processEntryErr := node.processLogEntryAsLeader(
			stateMachineClient{
				clientType: tcp,
				address:    client,
			},
			entry)
		if processEntryErr != nil {
			node.sendMessageToTCPClient(client, "", processEntryErr)
			return
		}
	case FOLLOWER:
		leaderAddr, leaderAddrErr := node.getLeaderGRPCAddress()
		if leaderAddrErr != nil {
			node.sendMessageToTCPClient(client, "", leaderAddrErr)
			return
		}

		forwardErr := node.forwardEntry(leaderAddr, client, entry)
		if forwardErr != nil {
			node.sendMessageToTCPClient(client, "", forwardErr)
			return
		}
	default:
		node.sendMessageToTCPClient(client, "", fmt.Errorf("Node not in a state able to accept commands"))
		return
	}
}

// processLogEntryAsLeader acts on a log entry as if it were a leader
func (node *Node) processLogEntryAsLeader(stateMachineClient stateMachineClient, entry *LogEntry) error {
	if node.state != LEADER {
		return fmt.Errorf("Log could not be processed because expected leader is not a leader. Its state is '%s'", node.state)
	}

	curTerm, curTermErr := node.getCurrentTerm()
	if curTermErr != nil {
		return curTermErr
	}
	entry.Term = curTerm

	addEntryErr := node.addLogEntry(stateMachineClient, entry)
	if addEntryErr != nil {
		return addEntryErr
	}

	sendEntryErr := node.sendEntriesToAllNodes([]*LogEntry{entry})
	if sendEntryErr != nil {
		return sendEntryErr
	}

	return nil
}

func (node *Node) sendMessageToTCPClient(client Client, message string, err error) {
	if !client.IsValid() {
		return
	}

	client.WriteOutput(node.formatOutputToClient(message, err))
}

func (node *Node) formatOutputToClient(message string, err error) (string, bool) {
	messageOK := true
	if err != nil {
		message = message + "\n" + err.Error()
		messageOK = false
	}

	return message, messageOK
}

func (node *Node) generateEntryID() string {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		log.Fatal(err)
	}
	return fmt.Sprintf("%x-%x-%x-%x-%x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

// getLastLogEntryDetails returns the details of the last log entry before the provided index in
// this order:
//   - index
//   - term
//
// Set beforeIndex to -1 if you want the last entry in the log
func (node *Node) getLastLogEntryDetails(beforeIndex int64) (int64, int64, error) {
	var data interface{}
	var dataErr error
	switch beforeIndex {
	case -1:
		data, dataErr = node.metaDB.RunSelectQuery(`SELECT idx, term FROM log ORDER BY idx DESC LIMIT 1`, false)
	default:
		data, dataErr = node.metaDB.RunSelectQuery(`SELECT idx, term FROM log WHERE idx < $1 ORDER BY idx DESC LIMIT 1`, false, beforeIndex)
	}

	if dataErr != nil {
		return -1, -1, fmt.Errorf("Could not ge the index and term from the log in getLastLogEntryDetails. Value of ceiling is %d: %w", beforeIndex, dataErr)
	}

	dataC := data.([][]interface{})
	switch len(dataC) {
	case 0:
		return 0, 0, nil
	case 1:
		indexAsserted, indexOK := dataC[0][0].(*int64)
		if !indexOK {
			return -1, -1, fmt.Errorf("Could not convert %v to *int64 while getting the last log entry index", dataC[0][0])
		}

		termAsserted, termOK := dataC[0][1].(*int64)
		if !termOK {
			return -1, -1, fmt.Errorf("Could not convert %v to *int64 while getting the last log entry term", dataC[0][1])
		}

		return *indexAsserted, *termAsserted, nil
	default:
		return -1, -1, fmt.Errorf("Was expecting 1 row of node data, but %d returned", len(dataC))
	}
}

// doesLogEntryWithIDExist checks whether a log entry with the provided index and
// term exists.
func (node *Node) doesLogEntryWithDetailsExist(index int64, term int64) (bool, error) {
	data, dataErr := node.metaDB.RunSelectQuery(`SELECT * FROM log WHERE idx = $1 AND term = $2`, false, index, term)
	if dataErr != nil {
		return false, dataErr
	}

	dataC := data.([][]interface{})
	if len(dataC) > 1 {
		return false, fmt.Errorf("More than one log entry with the index %d and term %d", index, term)
	}

	return len(dataC) == 1, nil
}

// getLogEntryWithIndex returns a log entry if found in the database, otherwise an empty
// LogEntry object is returned. Check if the log entry's ID is empty to determine if the
// entry is blank or not.
func (node *Node) getLogEntryWithIndex(index int64) (LogEntry, error) {
	var entry LogEntry

	data, dataErr := node.metaDB.RunSelectQuery(`SELECT id, idx, term, committed, command from log WHERE idx = $1`, false, index)
	if dataErr != nil {
		return entry, dataErr
	}

	dataC := data.([][]interface{})
	if len(dataC) == 0 {
		return entry, nil
	} else if len(dataC) > 1 {
		return entry, fmt.Errorf("More than one log entry exists with index '%d'", index)
	}

	curRow := dataC[0]

	id, idOK := curRow[0].(*string)
	if !idOK {
		return entry, fmt.Errorf("Could not extract log entry id from query response for entry exists with index '%d'", index)
	}
	entry.Id = *id

	idx, idxOK := curRow[1].(*int64)
	if !idxOK {
		return entry, fmt.Errorf("Could not extract log entry index from query response for entry exists with index '%d'", index)
	}
	entry.Index = *idx

	term, termOK := curRow[2].(*int64)
	if !termOK {
		return entry, fmt.Errorf("Could not extract log entry term from query response for entry exists with index '%d'", index)
	}
	entry.Term = *term

	committed, committedOK := curRow[3].(*int)
	if !committedOK {
		return entry, fmt.Errorf("Could not extract log entry committed status from query response for entry exists with index '%d'", index)
	}
	entry.Committed = *committed == 1

	command, commandOK := curRow[4].(*string)
	if !commandOK {
		return entry, fmt.Errorf("Could not extract log entry command from query response for entry exists with index '%d'", index)
	}
	entry.Command = *command

	return entry, nil
}

func (node *Node) deleteLogEntriesFromIndex(index int64) error {
	// Check if index has already been committed
	data, dataErr := node.metaDB.RunSelectQuery(`SELECT * FROM log WHERE idx = $1 AND committed = 1`, false, index)
	if dataErr != nil {
		return dataErr
	}
	dataC := data.([][]interface{})
	if len(dataC) > 0 {
		return fmt.Errorf("Could not delete log entry with index '%d' and all entries that follow it because it is already committed", index)
	}

	// Delete the log entry
	_, deleteErr := node.metaDB.RunWriteQuery(`DELETE FROM log WHERE idx >= $1`, index)

	return deleteErr
}

func (node *Node) getLastCommittedLogEntryIndex() (int64, error) {
	data, dataErr := node.metaDB.RunSelectQuery(`SELECT idx FROM log WHERE committed = $1 ORDER BY idx DESC LIMIT 1`, false, 1)
	if dataErr != nil {
		return -1, dataErr
	}

	dataC := data.([][]interface{})
	switch len(dataC) {
	case 0:
		return 0, nil
	case 1:
		dataAsserted, dataOK := dataC[0][0].(*int64)
		if !dataOK {
			return -1, fmt.Errorf("Could not convert %v to *int64 while getting the last committed log entry details", dataC[0][0])
		}

		return *dataAsserted, nil
	default:
		return -1, fmt.Errorf("Was expecting 1 row of node data, but %d returned", len(dataC))
	}
}

// registerStateMachineClient records the client that sent the log entry with the provided ID
// for the purposes of sending the command output when the log entry is committed to the state
// machine
func (node *Node) registerStateMachineClient(client stateMachineClient, logEntryID string) error {
	state, stateOK := node.logEntryStates[logEntryID]
	if !stateOK {
		return fmt.Errorf("Could not find state data for log entry with id '%s'. Cannot record the state machine client", logEntryID)
	}

	state.client = client
	return nil
}

func (node *Node) sendCommandOutputToClient(entryID string, output string, success bool) error {
	state, stateOK := node.logEntryStates[entryID]

	if !stateOK {
		return fmt.Errorf("Could not connect to client to deliver response")
	}

	// At this point after the command output has been sent to the client,
	// we have no need for the client
	defer node.unsetLogEntryClient(entryID)
	switch state.client.clientType {
	case tcp:
		tcpClient := state.client.address.(Client)
		if !tcpClient.IsValid() {
			return fmt.Errorf("Could not connect to client to deliver response")
		}

		tcpClient.WriteOutput(output, success)

		return nil
	case cluster:
		fmt.Printf("Sending back the following output to the cluster client \n %s", output)
		forwardErr := node.forwardCommitOutput(state.client.address.(string), entryID, output, success)
		return forwardErr
	default:
		return fmt.Errorf("Could not send back response to client for log entry '%s' because its type '%v' is unknown", entryID, state.client.clientType)
	}
}

func (node *Node) unsetLogEntryClient(entryID string) error {
	_, stateOK := node.logEntryStates[entryID]

	if !stateOK {
		return fmt.Errorf("Could not connect to client to deliver response")
	}

	node.logEntryStates[entryID].client.clientType = 0

	return nil
}

func (node *Node) isValueGreaterThanHalfNodes(value int) bool {
	return float64(value) > (float64(len(node.config.Nodes)) / 2)
}
