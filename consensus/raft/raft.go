package raft

import (
	"errors"
	fmt "fmt"
	"log"
	"math/rand"
	"strconv"
	"strings"
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
	entryClients           map[string]stateMachineClient
	metaDB                 *persistence.Driver
	recordedLogEntries     map[string]int
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
	node.entryClients = make(map[string]stateMachineClient)
	node.recordedLogEntries = make(map[string]int)

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
	id := node.generateEntryID()
	entry := &LogEntry{
		Id:      id,
		Command: command,
	}

	switch node.state {
	case LEADER:
		addEntryErr := node.addLogEntry(
			stateMachineClient{
				clientType: tcp,
				address:    client,
			},
			entry, -1)
		if addEntryErr != nil {
			node.sendErrorToTCPClient(client, addEntryErr)
			return
		}

		node.recordedLogEntries[entry.Id] = 1
		sendEntryErr := node.sendEntriesToAllNodes([]*LogEntry{entry})
		if sendEntryErr != nil {
			node.sendErrorToTCPClient(client, sendEntryErr)
			return
		}
	case FOLLOWER:
		if !node.stateMachine.ShouldForwardToLeader(command) {
			output, outputErr := node.stateMachine.Commit(command)

			client.WriteOutput(output, true)
			if outputErr != nil {
				node.sendErrorToTCPClient(client, outputErr)
			}

			return
		}

		leaderAddr, leaderAddrErr := node.getLeaderGRPCAddress()
		if leaderAddrErr != nil {
			node.sendErrorToTCPClient(client, leaderAddrErr)
			return
		}

		forwardErr := node.forwardEntry(leaderAddr, client, entry)
		if forwardErr != nil {
			node.sendErrorToTCPClient(client, forwardErr)
			return
		}
	default:
		node.sendErrorToTCPClient(client, fmt.Errorf("Node not in a state able to accept commands"))
		return
	}
}

func (node *Node) sendErrorToTCPClient(client Client, err error) {
	if client.IsValid() {
		client.WriteOutput(err.Error(), false)
	}
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
func (node *Node) registerStateMachineClient(client stateMachineClient, logEntryID string) {
	node.entryClients[logEntryID] = client
}

func (node *Node) sendCommandOutputToClient(entryID string, output string, success bool) error {
	client, clientOk := node.entryClients[entryID]

	if !clientOk {
		return fmt.Errorf("Could not connect to client to deliver response")
	}

	switch client.clientType {
	case tcp:
		tcpClient := client.address.(Client)
		if !tcpClient.IsValid() {
			return fmt.Errorf("Could not connect to client to deliver response")
		}

		tcpClient.WriteOutput(output+"\n", success)

		return nil
	case cluster:
		forwardErr := node.forwardCommitOutput(client.address.(string), entryID, output, success)
		return forwardErr
	default:
		return fmt.Errorf("Could not send back response to client because its type is unknown")
	}
}

func (node *Node) isValueGreaterThanHalfNodes(value int) bool {
	return float64(value) > (float64(len(node.config.Nodes)) / 2)
}
