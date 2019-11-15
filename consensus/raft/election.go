package raft

import (
	"context"
	fmt "fmt"
	"log"
	"math/rand"
	"time"

	"github.com/go-errors/errors"
	"github.com/jasonlvhit/gocron"
	grpc "google.golang.org/grpc"
)

type electionServer struct {
	UnimplementedElectionServiceServer
}

// resetElectionTimer resets the election timer which upon expiry initializes the election
// process
func (node *Node) resetElectionTimer() {
	if !node.stopElectionTimer() {
		node.electionScheduler = gocron.NewScheduler()
		go func() {
			<-node.electionScheduler.Start()
		}()
	}
	node.electionScheduler.Every(node.getElectionTimeoutDuration()).Seconds().Do(node.startElection)
}

func (node *Node) stopElectionTimer() bool {
	if node.electionScheduler != nil {
		node.electionScheduler.Clear()
		return true
	}

	return false
}

// startElection waits for the election timer to expire then starts the election process
func (node *Node) startElection() {
	// Workaround for timer bug
	// timeDiffNano := time.Now().UnixNano() - node.lastHeartbeatTimestamp
	// log.Printf("Time difference is %d\n", timeDiffNano)
	// if timeDiffNano < 2000000000 {
	// 	return
	// }
	// end workaround

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

func (node *Node) getElectionTimeoutDuration() uint64 {
	// TODO: Use the recommended method for determining how long timeout should be
	return uint64(rand.Intn(5))
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

	if node.currentTerm == term && node.state == CANDIDATE {
		if float64(node.termVoteCount[term]) > (float64(len(node.config.Nodes)) / 2) {
			// make leader
			node.votedFor = node.getID()
			node.setState(LEADER)
		}
	}
}

// registerLeader registers the provided node ID as the leader in this node
func (node *Node) registerLeader(nodeID string, term int64) error {
	if term < node.currentTerm {
		return errors.New(fmt.Sprintf("Cannot register node with ID %s as leader since its term %d is lower than node's term %d", nodeID, term, node.currentTerm))
	}

	if term == node.currentTerm && node.state == LEADER {
		return errors.New(fmt.Sprintf("Cannot promote node with id %s to leader since this node is currently leader and both share the term %d", nodeID, term))
	}

	node.currentTerm = term
	node.votedFor = nodeID
	node.setState(FOLLOWER)

	return nil
}

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
