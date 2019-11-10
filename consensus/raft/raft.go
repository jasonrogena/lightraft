package raft

type Node struct {
	// Should be saved when node about to die
	currentTerm int64
	votedFor    string
	log         []string

	// Volatile on all nodes
	commitIndex int64 // Index of highest log entry known to be committed. Initialized to 0
	lastApplied int64

	// Volatile on leader
	nextIndex  map[string]int64 // For each node, index of the next log entry to send to that node. Initialized to leader last log index + 1
	matchIndex map[string]int64 // For each node, index of the highest log entry known to be replicated on server. Initialized to 0
}
