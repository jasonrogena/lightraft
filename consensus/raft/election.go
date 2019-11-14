package raft

import (
	"context"
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/go-errors/errors"
	grpc "google.golang.org/grpc"
)

type electionServer struct {
	UnimplementedElectionServiceServer
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

	// Increment node's term
	node.currentTerm = node.currentTerm + 1
	// Transition state to CANDIDATE
	node.setState(CANDIDATE)
	// Vote for self
	node.voteForCandidate(node.getID(), node.currentTerm)
	node.registerVote(node.currentTerm)

	// Request for votes in parallel from other nodes
	lastLogIndex, lastLogIndexErr := node.getLastLogIndex()
	if lastLogIndexErr != nil {
		log.Fatalf(lastLogIndexErr.Error())
		return
	}

	lastLogTerm, lastLogTermErr := node.getLastLogTerm()
	if lastLogTermErr != nil {
		log.Fatalf(lastLogTermErr.Error())
		return
	}

	voteReq := VoteRequest{
		Term:         node.currentTerm,
		CandidateID:  node.getID(),
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	for curIndex := 0; curIndex < len(node.config.Nodes); curIndex++ {
		if curIndex != node.index {
			address := node.config.Nodes[curIndex].RPCBindAddress + ":" + strconv.Itoa(node.config.Nodes[curIndex].RPCBindPort)
			go node.requestForVote(address, voteReq)
		}
	}
}

func (node *Node) requestForVote(address string, voteReq VoteRequest) {
	conn, connErr := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if connErr != nil {
		log.Fatalf(connErr.Error())
	}
	defer conn.Close()
	client := NewElectionServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp, respErr := client.Vote(ctx, &voteReq)
	if respErr != nil {
		log.Fatalf(respErr.Error())
	} else {
		log.Printf("Got remote vote response for term %d with value %t\n", resp.Term, resp.VoteGranted)
		if resp.VoteGranted {
			node.registerVote(resp.Term)
		}
	}
}

func (node *Node) getElectionTimeoutDuration() time.Duration {
	// TODO: Use the recommended method for determining now long timeout should be
	return time.Duration(rand.Intn(500)) * time.Millisecond
}

func (node *Node) GetElectionTimer() *time.Timer {
	return node.electionTimer
}

// Vote is a gRPC handler for processing a vote request from a gRPC client
func (s *electionServer) Vote(ctx context.Context, voteReq *VoteRequest) (*VoteResponse, error) {
	voteResp := &VoteResponse{
		Term:        voteReq.Term,
		VoteGranted: false,
	}

	// Extract the node from the context
	node, nodeOk := ctx.Value(NAME).(*Node)
	if !nodeOk {
		return voteResp, errors.New("Could not get data from gRPC server")
	}

	// Check if node already voted for another candidate in the same term
	if len(node.candidateTermVote[voteResp.Term]) > 0 {
		// Node has already voted for another candidate in this term
		return voteResp, nil
	}

	// Check if candidate is up to date
	vote, voteErr := node.isCandidateUpToDate(voteReq)
	voteResp.VoteGranted = vote

	if vote {
		node.voteForCandidate(voteReq.CandidateID, voteResp.Term)
	}

	return voteResp, voteErr
}

func (node *Node) voteForCandidate(candidateId string, term int64) {
	node.candidateTermVote[term] = candidateId
}

func (node *Node) registerVote(term int64) {
	if _, there := node.termVoteCount[term]; !there {
		node.termVoteCount[term] = 0
	}

	node.termVoteCount[term] = node.termVoteCount[term] + 1

	// TODO: Implement sending a message when node wins the election
	if node.currentTerm == term && node.state == CANDIDATE {
		if float64(node.termVoteCount[term]) > (float64(len(node.config.Nodes)) / 2) {
			// make leader
			node.votedFor = node.getID()
			node.setState(LEADER)
		}
	}
}

func (node *Node) send

func (node *Node) isCandidateUpToDate(voteReq *VoteRequest) (bool, error) {
	nodeLastLogTerm, nodeLastLogTermErr := node.getLastLogTerm()
	if nodeLastLogTermErr != nil {
		log.Fatalf(nodeLastLogTermErr.Error())
		return false, nodeLastLogTermErr
	}

	if nodeLastLogTerm == voteReq.GetLastLogTerm() {
		nodeLastLogIndex, nodeLastLogIndexErr := node.getLastLogIndex()
		if nodeLastLogIndexErr != nil {
			log.Fatalf(nodeLastLogIndexErr.Error())
			return false, nodeLastLogIndexErr
		}

		return voteReq.GetLastLogIndex() >= nodeLastLogIndex, nil
	}

	return voteReq.GetLastLogTerm() > nodeLastLogTerm, nil
}
