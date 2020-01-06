package raft

import (
	"context"
	fmt "fmt"
	"log"
	"math/rand"
	"time"

	"github.com/go-errors/errors"
	grpc "google.golang.org/grpc"
)

const electionMaxTimeout int = 2000 // Maximum time in milliseconds follower should wait before starting an election if leader hasn't sent a heartbeat

type electionServer struct {
	UnimplementedElectionServiceServer
}

// resetElectionTimer resets the election timer which upon expiry initializes the election
// process
func (node *Node) resetElectionTimer() {
	node.stopElectionTimer()

	node.electionTimer = time.AfterFunc(node.getElectionTimeoutDuration(), node.startElection)
}

func (node *Node) stopElectionTimer() bool {
	if node.electionTimer != nil {
		node.electionTimer.Stop()
		node.electionTimer = nil
		return true
	}

	return false
}

// startElection waits for the election timer to expire then starts the election process
func (node *Node) startElection() {
	// Workaround for timer bug
	timeDiffNano := time.Now().UnixNano() - node.lastHeartbeatTimestamp
	log.Printf("Leader last sent heartbeat %dns ago\n", timeDiffNano)
	if timeDiffNano < (int64(electionMaxTimeout) * 1000000) {
		node.resetElectionTimer()
		return
	}
	// end workaround

	log.Printf("Election started by node %d\n", node.index)

	// Increment node's term
	oldTerm, oldTermErr := node.getCurrentTerm()
	if oldTermErr != nil {
		log.Fatalf(oldTermErr.Error())
		return
	}

	newTerm := oldTerm + 1
	newTermErr := node.setCurrentTerm(newTerm)
	if newTermErr != nil {
		log.Fatalf(newTermErr.Error())
		return
	}

	// Transition state to CANDIDATE
	node.setState(CANDIDATE)
	// Vote for self
	node.voteForCandidate(node.getID(), newTerm)
	node.registerVote(newTerm)

	// Request for votes in parallel from other nodes
	lastLogIndex, lastLogTerm, lastLogErr := node.getLastLogEntryDetails()
	if lastLogErr != nil {
		log.Fatalf(lastLogErr.Error())
		return
	}

	voteReq := VoteRequest{
		Term:         newTerm,
		CandidateID:  node.getID(),
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
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

		go node.requestForVote(address, voteReq)
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
	// TODO: Use the recommended method for determining how long timeout should be
	return time.Duration(rand.Intn(electionMaxTimeout)) * time.Millisecond
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

func (node *Node) registerVote(term int64) error {
	if _, there := node.termVoteCount[term]; !there {
		node.termVoteCount[term] = 0
	}

	node.termVoteCount[term] = node.termVoteCount[term] + 1

	currentTerm, currentTermErr := node.getCurrentTerm()
	if currentTermErr != nil {
		return currentTermErr
	}

	if currentTerm == term && node.state == CANDIDATE {
		if node.isValueGreaterThanHalfNodes(node.termVoteCount[term]) {
			// make leader
			votedForErr := node.setVotedFor(node.getID())
			if votedForErr != nil {
				return votedForErr
			}

			node.setState(LEADER)
		}
	}

	return nil
}

// registerLeader registers the provided node ID as the leader in this node
func (node *Node) registerLeader(nodeID string, term int64) error {
	currentTerm, currentTermErr := node.getCurrentTerm()
	if currentTermErr != nil {
		return currentTermErr
	}

	if term < currentTerm {
		return errors.New(fmt.Sprintf("Cannot register node with ID %s as leader since its term %d is lower than node's term %d", nodeID, term, currentTerm))
	}

	if term == currentTerm && node.state == LEADER {
		return errors.New(fmt.Sprintf("Cannot promote node with id %s to leader since this node is currently leader and both share the term %d", nodeID, term))
	}

	newTermErr := node.setCurrentTerm(term)
	if newTermErr != nil {
		return newTermErr
	}

	votedForErr := node.setVotedFor(nodeID)
	if votedForErr != nil {
		return votedForErr
	}
	node.setState(FOLLOWER)

	return nil
}

func (node *Node) isCandidateUpToDate(voteReq *VoteRequest) (bool, error) {
	nodeLastLogIndex, nodeLastLogTerm, nodeLastLogErr := node.getLastLogEntryDetails()
	if nodeLastLogErr != nil {
		log.Fatalf(nodeLastLogErr.Error())
		return false, nodeLastLogErr
	}

	if nodeLastLogTerm == voteReq.GetLastLogTerm() {
		return voteReq.GetLastLogIndex() >= nodeLastLogIndex, nil
	}

	return voteReq.GetLastLogTerm() > nodeLastLogTerm, nil
}
