package consensus

// ToNodeInterface defines node to node communication
type ToNodeInterface interface {
	RequestVote() error
	StepDown() error
	PropagateEntry(lastEntryTerm int, lastEntryIndex int, logEntry LogEntry) error
}

// State is the type defining the state of the node in the cluster
type State int

const (
	// FOLLOWER state is the default state. Node cannot control writing to its log in this state
	FOLLOWER State = 1 << iota
	// CANDIDATE state shows node offering itself to be elected
	CANDIDATE
	// LEADER state allows controlling the log in the cluster
	LEADER
)

type ElectionOutcome int

const (
	// MAJORITY indicates the node received a majority vote
	// If outcome is set to this, node promotes itself to leader and tells everyone else
	MAJORITY ElectionOutcome = 1 << iota
	// HEARTBEAT indicates the current leader responded to node's vote request
	// If outcome is set to this, become follower of node that sent the heartbeat
	HEARTBEAT
	// TIMEOUT indicates the election timed out
	// If outcome is set to this, node to stay in candidate state and start a new election
	TIMEOUT
)

type LogEntry struct {
	id    string
	index int64
	term  int64
	data  string
}

type Node struct {
	state State
	// electionTimeout in milliseconds (default to random). After expiry, follower becomes a candidate.
	// Should be more than the longest time it takes to send and receive a message from the furthest
	// node in the cluster
	electionTimeout int64
	// votes defaults to 0. Indicates number of votes node gets after it starts an election
	votes int
	// term defaults to 0. Increments everytime an election is held
	term int64
}

// Procedure for starting election:
//   1. Increment term
//   2. Reset electionTimeout
//   3. Vote for yourself
//   4. Ask for votes from everyone else

// Rules for voting:
//   Don't vote for candidate if:
//     - Node's term is better than the candidate's term
//     - Node's term is the same but index is better than the candidate's index

// Procedure for replicating log entry:
//   1. Send the data (last entry's term, last entry's index, the log entry)
//   2. Reset the electionTimeout?

// Rules for commiting:
//   Only commit if:
//     - entry is stored in majority of the nodes
//     - at least one entry from the current leader's term is also majority stored??
