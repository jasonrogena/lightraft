package raft

import (
	"errors"
	"log"
	"math/rand"
	"time"
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
	log         []string

	// Volatile on all nodes
	commitIndex int64 // Index of highest log entry known to be committed. Initialized to 0
	lastApplied int64 // Index of highest log entry applied to the state machine

	// Volatile on leader
	// Should be reinitialized after election
	nextIndex  map[string]int64 // For each node, index of the next log entry to send to that node. Initialized to leader last log index + 1
	matchIndex map[string]int64 // For each node, index of the highest log entry known to be replicated on server. Initialized to 0

	// Volatile, added by me
	state         State // Default to FOLLOWER
	electionTimer *time.Timer
	index         int64
}

// NewNode should be called whenever the node is initialized
// gets saved node data from the database and initializes ephemeral node data
func NewNode(index int64) *Node {
	node := new(Node)
	// TODO: Get persistent node details from the database

	// Init volatile data
	node.index = index
	node.commitIndex = 0
	node.lastApplied = 0
	node.setState(FOLLOWER)

	return node
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
	if node.state == newState {
		return errors.New("Trying to change state of node to one it's already in")
	}

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

// resetElectionTimer resets the election timer which upon expiry initializes the election
// process
func (node *Node) resetElectionTimer() {
	if node.electionTimer != nil {
		node.electionTimer.Stop()
		node.electionTimer.Reset(node.getElectionTimeoutDuration())
	} else {
		node.electionTimer = time.NewTimer(node.getElectionTimeoutDuration())
	}

	go node.startElection()
}

// startElection waits for the election timer to expire then starts the election process
func (node *Node) startElection() {
	// Wait for timer to expire
	<-node.electionTimer.C

	log.Printf("Election started by node %d\n", node.index)
	node.resetElectionTimer()
}

func (node *Node) getElectionTimeoutDuration() time.Duration {
	// TODO: Use the recommended method for determining now long timeout should be
	return time.Duration(rand.Intn(500)) * time.Millisecond
}

func (node *Node) GetElectionTimer() *time.Timer {
	return node.electionTimer
}
