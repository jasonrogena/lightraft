package consensus

type StateMachine interface {
	RequiresConsensus(command string) bool
	Commit(command string) (string, error)
}
