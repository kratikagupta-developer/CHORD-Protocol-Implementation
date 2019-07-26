package chord

type VnodeRPC interface {
	GetPredecessor() (*Vnode, error)
	Notify(*Vnode) ([]*Vnode, error)
	FindSuccessors(int, []byte) ([]*Vnode, int, int, error)
	ClearPredecessor(*Vnode) error
	SkipSuccessor(*Vnode) error
}

type Storage interface {
	Get(string) ([]byte, error)
	Set(string, string) error
	Delete(string) error
}

type Delegate interface {
	NewPredecessor(local, remoteNew, remotePrev *Vnode)
	Leaving(local, pred, succ *Vnode)
	PredecessorLeaving(local, remote *Vnode)
	SuccessorLeaving(local, remote *Vnode)
	Shutdown()
}

type Transport interface {
	// Gets a list of the vnodes on the box
	ListVnodes(string) ([]*Vnode, error)

	// Ping a Vnode, check for liveness
	Ping(*Vnode) (bool, error)

	// Request a nodes predecessor
	GetPredecessor(*Vnode) (*Vnode, error)

	// Notify our successor of ourselves
	Notify(target, self *Vnode) ([]*Vnode, error)

	// Find a successor
	FindSuccessors(*Vnode, int, []byte) ([]*Vnode, int, int, error)

	// Clears a predecessor if it matches a given vnode. Used to leave.
	ClearPredecessor(target, self *Vnode) error

	// Instructs a chord to skip a given successor. Used to leave.
	SkipSuccessor(target, self *Vnode) error

	// Register for an RPC callbacks
	Register(*Vnode, VnodeRPC)

	Deregister(*Vnode)
}
