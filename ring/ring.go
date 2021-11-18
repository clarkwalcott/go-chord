package ring

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"github.com/go-chord/transport"
	"github.com/go-chord/util"
	"github.com/go-chord/vnode"
	"hash"
	"log"
	"sort"
	"time"
)

// Stores the state required for a Chord ring
type Ring struct {
	Config     *Config
	Transport  transport.Transport
	Vnodes     []*vnode.LocalVnode
	DelegateCh chan func()
	ShutdownCh   chan bool
}

// Configuration for Chord nodes
type Config struct {
	Hostname      string        // Local host name
	NumVnodes     int           // Number of vnodes per physical node
	HashFunc      hash.Hash     // Hash function to use
	StabilizeMin  time.Duration // Minimum stabilization time
	StabilizeMax  time.Duration // Maximum stabilization time
	NumSuccessors int           // Number of successors to maintain
	HashBits      int           // Bit size of the hash function
}

// Returns the default Ring configuration
func DefaultConfig(hostname string) *Config {
	hashFunc := sha1.New()
	return &Config{
		hostname,
		8,        // 8 vnodes
		hashFunc, // SHA1
		15 * time.Second,
		45 * time.Second,
		8,                   // 8 successors
		hashFunc.Size() * 8, // 160bit hash function
	}
}

// Creates a new Chord ring given the config and transport
func New(conf *Config, trans transport.Transport) (*Ring, error) {
	r := Ring{}
	r.Config = conf
	r.Vnodes = make([]*vnode.LocalVnode, conf.NumVnodes)
	r.Transport = transport.InitLocalTransport(trans)
	r.DelegateCh = make(chan func(), 32)
	r.ShutdownCh = make(chan bool, conf.NumVnodes)

	// Initializes the vnodes
	for i := 0; i < conf.NumVnodes; i++ {
		vn := vnode.New(&r, i)
		r.Vnodes[i] = vn
	}

	// Sort the vnodes
	sort.Sort(&r)

	r.setLocalSuccessors()
	r.schedule()
	return &r, nil
}

// Joins an existing Chord ring
func Join(conf *Config, trans transport.Transport, existing string) (*Ring, error) {
	// Request a list of Vnodes from the remote host
	hosts, err := trans.ListVnodes(existing)
	if err != nil {
		return nil, err
	}
	if hosts == nil || len(hosts) == 0 {
		return nil, fmt.Errorf("remote host has no vnodes")
	}

	// New a ring
	ring, err := New(conf, trans)
	if err != nil {
		return nil, err
	}

	// Acquire a live successor for each Vnode
	for _, vn := range ring.Vnodes {
		// Get the nearest remote vnode
		nearest := vnode.NearestVnodeToKey(hosts, vn.Id)

		// Query for a list of successors to this Vnode
		succs, err := trans.FindSuccessors(nearest, conf.NumSuccessors, vn.Id)
		if err != nil {
			return nil, fmt.Errorf("failed to find successor for vnodes. Got %s", err)
		}
		if succs == nil || len(succs) == 0 {
			return nil, fmt.Errorf("failed to find successor for vnodes. Got no vnodes")
		}

		// Assign the successors
		for idx, s := range succs {
			vn.Successors[idx] = s
		}
	}

	// Do a fast stabilization, will schedule regular execution
	for _, vn := range ring.Vnodes {
		vnode.Stabilize(ring, vn)
	}
	return ring, nil
}

// Leaves a given Chord ring and shuts down the local vnodes
func (r *Ring) Leave() error {
	// Shutdown the vnodes first to avoid further stabilization runs
	r.stopVnodes()

	// Instruct each vnode to leave
	var err error
	for _, vn := range r.Vnodes {
		err = MergeErrors(err, vn.Leave(r))
	}

	return err
}

// Shutdown shuts down the local processes in a given Chord ring
// Blocks until all the vnodes terminate.
func (r *Ring) Shutdown() {
	r.stopVnodes()
}

// Does a key lookup for up to N successors of a key
func (r *Ring) Lookup(n int, key []byte) ([]*vnode.Vnode, error) {
	// Ensure that n is sane
	if n > r.Config.NumSuccessors {
		return nil, fmt.Errorf("cannot ask for more successors than NumSuccessors")
	}

	// Hash the key
	h := r.Config.HashFunc
	h.Write(key)
	keyHash := h.Sum(nil)

	// Find the nearest local vnode
	nearest := r.NearestVnode(keyHash)

	// Use the nearest node for the lookup
	successors, err := nearest.FindSuccessors(r.Transport, n, keyHash)
	if err != nil {
		return nil, err
	}

	// Trim the nil successors
	for successors[len(successors)-1] == nil {
		successors = successors[:len(successors)-1]
	}
	return successors, nil
}

// Len is the number of vnodes
func (r *Ring) Len() int {
	return len(r.Vnodes)
}

// Less returns whether the vnode with index i should sort
// before the vnode with index j.
func (r *Ring) Less(i, j int) bool {
	return bytes.Compare(r.Vnodes[i].Id, r.Vnodes[j].Id) == -1
}

// Swap swaps the vnodes with indexes i and j.
func (r *Ring) Swap(i, j int) {
	r.Vnodes[i], r.Vnodes[j] = r.Vnodes[j], r.Vnodes[i]
}

// Returns the nearest local vnode to the key
func (r *Ring) NearestVnode(key []byte) *vnode.LocalVnode {
	for i := len(r.Vnodes) - 1; i >= 0; i-- {
		if bytes.Compare(r.Vnodes[i].Id, key) == -1 {
			return r.Vnodes[i]
		}
	}
	// Return the last vnode
	return r.Vnodes[len(r.Vnodes)-1]
}

// Schedules each vnode in the ring
func (r *Ring) schedule() {
	for i := 0; i < len(r.Vnodes); i++ {
		vnode.Schedule(r, r.Vnodes[i])
	}
}

// Wait for all the Vnodes to shutdownCh
func (r *Ring) stopVnodes() {
	for i := 0; i < r.Config.NumVnodes; i++ {
		<-r.ShutdownCh
	}
}

// Initializes the Vnodes with their local successors
func (r *Ring) setLocalSuccessors() {
	numV := len(r.Vnodes)
	numSuc := util.Min(r.Config.NumSuccessors, numV-1)
	for idx, vn := range r.Vnodes {
		for i := 0; i < numSuc; i++ {
			vn.Successors[i] = &r.Vnodes[(idx+i+1)%numV].Vnode
		}
	}
}

// Invokes a function on the delegate and returns completion channel
func (r *Ring) InvokeDelegate(f func()) chan struct{} {
	ch := make(chan struct{}, 1)
	wrapper := func() {
		defer func() {
			ch <- struct{}{}
		}()
		f()
	}

	r.DelegateCh <- wrapper
	return ch
}

// This handler runs in a go routine to invoke methods on the delegate
func (r *Ring) delegateHandler() {
	for {
		f, ok := <-r.DelegateCh
		if !ok {
			break
		}
		r.safeInvoke(f)
	}
}

// Called to safely call a function on the delegate
func (r *Ring) safeInvoke(f func()) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("caught a panic invoking a delegated function! Got: %s", r)
		}
	}()
	f()
}

// Merges errors together
func MergeErrors(err1, err2 error) error {
	switch  {
	case err1 == nil:
		return err2
	case err2 == nil:
		return err1
	default:
		return fmt.Errorf("%s\n%s", err1, err2)
	}
}
