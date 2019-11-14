package raft

import (
	"log"
	"strconv"
	"time"

	"github.com/jasonrogena/lightraft/configuration"
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
	// Should be saved to database before responding to RPC
	currentTerm int64
	votedFor    string
	// log         []string will be written directly to the metadata database

	// Volatile on all nodes
	commitIndex int64 // Index of highest log entry known to be committed. Initialized to 0
	lastApplied int64 // Index of highest log entry applied to the state machine

	// Volatile on leader
	// Should be reinitialized after election
	nextIndex  map[string]int64 // For each node, index of the next log entry to send to that node. Initialized to leader last log index + 1
	matchIndex map[string]int64 // For each node, index of the highest log entry known to be replicated on server. Initialized to 0

	// Volatile, added by me
	state             State // Default to FOLLOWER
	electionTimer     *time.Timer
	index             int
	candidateTermVote map[int64]string // Map of terms and IDs of candidates that this node voted for in each of the terms. ID will be nil if node hasn't voted
	termVoteCount     map[int64]int64  // Map of terms and number of votes this node got for each of the terms
	config            *configuration.Config
}

const NAME string = "raft-node"

// NewNode should be called whenever the node is initialized
// gets saved node data from the database and initializes ephemeral node data
func NewNode(index int, config *configuration.Config) *Node {
	node := new(Node)
	// TODO: Get persistent node details from the database

	// Init volatile data
	node.index = index
	node.config = config
	node.commitIndex = 0
	node.lastApplied = 0
	node.setState(FOLLOWER)
	node.candidateTermVote = make(map[int64]string)
	node.termVoteCount = make(map[int64]int64)

	return node
}

func (node *Node) getID() string {
	return node.config.Cluster.Name + strconv.Itoa(node.index)
}

// RegisterGRPCHandlers adds handlers to the provided gRPC server
func (node *Node) RegisterGRPCHandlers(grpcServer *grpc.Server) {
	RegisterElectionServiceServer(grpcServer, &electionServer{})
}

// becomeLeader resets the node data for the leader to what is recommended when the leader is
// first elected
func (node *Node) becomeLeader() {
	node.nextIndex = make(map[string]int64)
	node.matchIndex = make(map[string]int64)

	if node.electionTimer != nil {
		node.electionTimer.Stop()
	}
	log.Printf("Node %d is now leader\n", node.index)
}

func (node *Node) becomeFollower() {
	node.resetElectionTimer()
	log.Printf("Node %d is now follower\n", node.index)
}

func (node *Node) becomeCandidate() {
	node.resetElectionTimer()
	log.Printf("Node %d is now candidate\n", node.index)
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

func (node *Node) getLastLogIndex() (int64, error) {
	// TODO: Implement this
	return 0, nil
}

func (node *Node) getLastLogTerm() (int64, error) {
	// TODO: Implement this
	return 0, nil
}
