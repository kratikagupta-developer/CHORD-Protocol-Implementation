package chord

import (
	"fmt"
)

// BlackholeTransport is used to provide an implemenation of the Transport that
// does not actually do anything. Any operation will result in an error.
type BlackholeTransport struct {
}

func (b *BlackholeTransport) ListVnodes(host string) ([]*Vnode, error) {
	return nil, fmt.Errorf("Failed to connect! Blackhole: %s.", host)
}

func (b *BlackholeTransport) Ping(vn *Vnode) (bool, error) {
	return false, nil
}

func (b *BlackholeTransport) GetPredecessor(vn *Vnode) (*Vnode, error) {
	return nil, fmt.Errorf("Failed to connect! Blackhole: %s.", vn.String())
}

func (b *BlackholeTransport) Notify(vn, self *Vnode) ([]*Vnode, error) {
	return nil, fmt.Errorf("Failed to connect! Blackhole: %s", vn.String())
}

func (b *BlackholeTransport) FindSuccessors(vn *Vnode, n int, key []byte) ([]*Vnode, int, int, error) {
	return nil, 0, 0, fmt.Errorf("Failed to connect! Blackhole: %s", vn.String())
}

func (b *BlackholeTransport) ClearPredecessor(target, self *Vnode) error {
	return fmt.Errorf("Failed to connect! Blackhole: %s", target.String())
}

func (b *BlackholeTransport) SkipSuccessor(target, self *Vnode) error {
	return fmt.Errorf("Failed to connect! Blackhole: %s", target.String())
}

func (b *BlackholeTransport) Register(v *Vnode, o VnodeRPC) {
}

func (b *BlackholeTransport) Deregister(v *Vnode) {
}
