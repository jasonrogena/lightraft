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
	electionTimer          *time.Timer
	heartbeatScheduler     *gocron.Scheduler
	lastHeartbeatTimestamp int64 // The unix epoch (in seconds) for the last time the leader sent a heartbeat
	index                  int
	candidateTermVote      map[int64]string // Map of terms and IDs of candidates that this node voted for in each of the terms. ID will be nil if node hasn't voted
	termVoteCount          map[int64]int64  // Map of terms and number of votes this node got for each of the terms
	config                 *configuration.Config
	entryClients           map[string]Client
	metaDB                 *persistence.Driver
}

type logEntry struct {
	id      string
	command string
}

const NAME string = "raft-node"

// NewNode should be called whenever the node is initialized
// gets saved node data from the database and initializes ephemeral node data
func NewNode(index int, config *configuration.Config) (*Node, error) {
	node := new(Node)
	metaDb, dbErr := initMetaDB(config, index)
	if dbErr != nil {
		return nil, dbErr
	}
	node.metaDB = metaDb

	// Init volatile data
	node.index = index
	node.config = config
	node.commitIndex = 0
	node.lastApplied = 0
	node.setState(FOLLOWER)
	node.candidateTermVote = make(map[int64]string)
	node.termVoteCount = make(map[int64]int64)
	node.entryClients = make(map[string]Client)

	return node, nil
}

func (node *Node) getID() string {
	return node.config.Cluster.Name + strconv.Itoa(node.index)
}

// initMetaDB initializes the meta database (where the non-volatile data for the node, including the log, are stored)
func initMetaDB(config *configuration.Config, nodeIndex int) (*persistence.Driver, error) {
	dbPath := config.Cluster.Name + "-" + strconv.Itoa(nodeIndex) + "-meta" + persistence.EXTENSION
	metaDB, metaDBErr := persistence.NewDriver(dbPath)
	if metaDBErr != nil {
		return nil, metaDBErr
	}

	_, nodeErr := metaDB.RunWriteQuery(`CREATE TABLE IF NOT EXISTS node (
		idx INTEGER PRIMARY KEY,
		currentTerm INTEGER,
		votedFor TEXT)`)
	if nodeErr != nil {
		return nil, nodeErr
	}
	_, inNodeErr := metaDB.RunWriteQuery(`INSERT OR IGNORE INTO node(idx, currentTerm)
	VALUES ($1, $2)`, nodeIndex, 0)
	if inNodeErr != nil {
		return nil, inNodeErr
	}

	// TODO: What should be unique in the log table? Just the index, or a combination of the index and the term?
	_, logErr := metaDB.RunWriteQuery(`CREATE TABLE IF NOT EXISTS log (
		idx INTEGER PRIMARY KEY,
		term INTEGER,
		committed INTEGER,
		command TEXT NOT NULL)`)
	if logErr != nil {
		return nil, logErr
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

	return *dataC[0][0].(*int64), nil
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

	return *dataC[0][0].(*string), nil
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

func (node *Node) IngestCommand(client Client, command string) error {
	id := node.generateEntryID()
	node.entryClients[id] = client
	entry := logEntry{
		id:      id,
		command: command,
	}

	if node.state == LEADER {
		addr, addrErr := node.getGRPCAddress(node.index)
		if addrErr != nil {
			return addrErr
		}

		return node.addLogEntry(addr, entry)
	}

	leaderAddr, leaderAddrErr := node.getLeaderGRPCAddress()
	if leaderAddrErr != nil {
		return leaderAddrErr
	}

	return node.forwardEntry(leaderAddr, entry)
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

func (node *Node) getLastLogIndex() (int64, error) {
	// TODO: Implement this
	return 0, nil
}

func (node *Node) getLastLogTerm() (int64, error) {
	// TODO: Implement this
	return 0, nil
}

func (node *Node) getLastCommitIndex() (int64, error) {
	// TODO: Implement this
	return 0, nil
}
