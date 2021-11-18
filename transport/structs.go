package transport

import (
	"fmt"
	"github.com/go-chord/vnode"
)

// BlackholeTransport is used to provide an implementation of the Transport that
// does not actually do anything. Any operation will result in an error.
type BlackholeTransport struct {
}

func (*BlackholeTransport) ListVnodes(host string) ([]*vnode.Vnode, error) {
	return nil, fmt.Errorf("Failed to connect! Blackhole: %s.", host)
}

func (trans *BlackholeTransport) Ping(vn *vnode.Vnode) (bool, error) {
	return false, nil
}

func (*BlackholeTransport) GetPredecessor(vn *vnode.Vnode) (*vnode.Vnode, error) {
	return nil, fmt.Errorf("Failed to connect! Blackhole: %s.", vn.String())
}

func (*BlackholeTransport) Notify(vn, self *vnode.Vnode) ([]*vnode.Vnode, error) {
	return nil, fmt.Errorf("Failed to connect! Blackhole: %s", vn.String())
}

func (*BlackholeTransport) FindSuccessors(vn *vnode.Vnode, n int, key []byte) ([]*vnode.Vnode, error) {
	return nil, fmt.Errorf("Failed to connect! Blackhole: %s", vn.String())
}

func (*BlackholeTransport) ClearPredecessor(target, self *vnode.Vnode) error {
	return fmt.Errorf("Failed to connect! Blackhole: %s", target.String())
}

func (*BlackholeTransport) SkipSuccessor(target, self *vnode.Vnode) error {
	return fmt.Errorf("Failed to connect! Blackhole: %s", target.String())
}

func (*BlackholeTransport) Register(v *vnode.Vnode, o VnodeRPC) {
}