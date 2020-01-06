package consensus

type StateMachine interface {
	ShouldForwardToLeader(command string) bool
	Commit(command string) (string, error)
}
