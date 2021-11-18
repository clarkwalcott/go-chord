package transport

import (
	"github.com/go-chord/vnode"
	"sync"
)

// Implements the methods needed for a Chord ring
type Transport interface {
	// Gets a list of the vnodes on the box
	ListVnodes(string) ([]*vnode.Vnode, error)

	// Ping a Vnode, check if it's alive
	Ping(vn *vnode.Vnode) (bool, error)

	// Request a nodes predecessor
	GetPredecessor(*vnode.Vnode) (*vnode.Vnode, error)

	// Notify our successor of ourselves
	Notify(target, self *vnode.Vnode) ([]*vnode.Vnode, error)

	// Find a successor
	FindSuccessors(*vnode.Vnode, int, []byte) ([]*vnode.Vnode, error)

	// Clears a predecessor if it matches a given vnode. Used to leave.
	ClearPredecessor(target, self *vnode.Vnode) error

	// Instructs a node to skip a given successor. Used to leave.
	SkipSuccessor(target, self *vnode.Vnode) error

	// Register for an RPC callbacks
	Register(*vnode.Vnode, VnodeRPC)
}

// These are the methods to invoke on the registered vnodes
type VnodeRPC interface {
	GetPredecessor() (*vnode.Vnode, error)
	Notify(*LocalTransport, *vnode.Vnode) ([]*vnode.Vnode, error)
	FindSuccessors(*LocalTransport, int, []byte) ([]*vnode.Vnode, error)
	ClearPredecessor(*vnode.Vnode) error
	SkipSuccessor(*vnode.Vnode) error
}

// Wraps vnode and object
type localRPC struct {
	vnode *vnode.Vnode
	obj   VnodeRPC
}

// LocalTransport is used to provides fast routing to Vnodes running
// locally using direct method calls. For any non-local vnodes, the
// request is passed on to another transport.
type LocalTransport struct {
	host   string
	remote Transport
	lock   sync.RWMutex
	local  map[string]*localRPC
}

// Creates a local transport to wrap a remote transport
func InitLocalTransport(remote Transport) Transport {
	// Replace a nil transport with black hole
	if remote == nil {
		remote = &BlackholeTransport{}
	}

	local := make(map[string]*localRPC)
	return &LocalTransport{remote: remote, local: local}
}

// Checks for a local vnode
func (lt *LocalTransport) get(vn *vnode.Vnode) (VnodeRPC, bool) {
	key := vn.String()
	lt.lock.RLock()
	defer lt.lock.RUnlock()
	w, ok := lt.local[key]
	if ok {
		return w.obj, ok
	} else {
		return nil, ok
	}
}

func (lt *LocalTransport) ListVnodes(host string) ([]*vnode.Vnode, error) {
	// Check if this is a local host
	if host == lt.host {
		// Generate all the local clients
		res := make([]*vnode.Vnode, 0, len(lt.local))

		// Build list
		lt.lock.RLock()
		for _, v := range lt.local {
			res = append(res, v.vnode)
		}
		lt.lock.RUnlock()

		return res, nil
	}

	// Pass onto remote
	return lt.remote.ListVnodes(host)
}

func (lt *LocalTransport) Ping(vn *vnode.Vnode) (bool, error) {
	// Look for it locally
	_, ok := lt.get(vn)

	// If it exists locally, handle it
	if ok {
		return true, nil
	}

	// Pass onto remote
	return lt.remote.Ping(vn)
}

func (lt *LocalTransport) GetPredecessor(vn *vnode.Vnode) (*vnode.Vnode, error) {
	// Look for it locally
	obj, ok := lt.get(vn)

	// If it exists locally, handle it
	if ok {
		return obj.GetPredecessor()
	}

	// Pass onto remote
	return lt.remote.GetPredecessor(vn)
}

func (lt *LocalTransport) Notify(vn, self *vnode.Vnode) ([]*vnode.Vnode, error) {
	// Look for it locally
	obj, ok := lt.get(vn)

	// If it exists locally, handle it
	if ok {
		return obj.Notify(lt, self)
	}

	// Pass onto remote
	return lt.remote.Notify(vn, self)
}

func (lt *LocalTransport) FindSuccessors(vn *vnode.Vnode, n int, key []byte) ([]*vnode.Vnode, error) {
	// Look for it locally
	obj, ok := lt.get(vn)

	// If it exists locally, handle it
	if ok {
		return obj.FindSuccessors(lt, n, key)
	}

	// Pass onto remote
	return lt.remote.FindSuccessors(vn, n, key)
}

func (lt *LocalTransport) ClearPredecessor(target, self *vnode.Vnode) error {
	// Look for it locally
	obj, ok := lt.get(target)

	// If it exists locally, handle it
	if ok {
		return obj.ClearPredecessor(self)
	}

	// Pass onto remote
	return lt.remote.ClearPredecessor(target, self)
}

func (lt *LocalTransport) SkipSuccessor(target, self *vnode.Vnode) error {
	// Look for it locally
	obj, ok := lt.get(target)

	// If it exists locally, handle it
	if ok {
		return obj.SkipSuccessor(self)
	}

	// Pass onto remote
	return lt.remote.SkipSuccessor(target, self)
}

func (lt *LocalTransport) Register(v *vnode.Vnode, o VnodeRPC) {
	// Register local instance
	key := v.String()
	lt.lock.Lock()
	lt.host = v.Host
	lt.local[key] = &localRPC{v, o}
	lt.lock.Unlock()

	// Register with remote transport
	lt.remote.Register(v, o)
}

func (lt *LocalTransport) Deregister(v *vnode.Vnode) {
	key := v.String()
	lt.lock.Lock()
	delete(lt.local, key)
	lt.lock.Unlock()
}


