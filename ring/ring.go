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
	Vnodes     []*vnode.Vnode
	DelegateCh chan func()
	ShutdownCh chan bool
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
	r.Vnodes = make([]*vnode.Vnode, conf.NumVnodes)
	r.Transport = transport.InitLocalTransport(trans)
	r.DelegateCh = make(chan func(), 32)
	r.ShutdownCh = make(chan bool, conf.NumVnodes)

	// Initializes the vnodes
	for i := 0; i < conf.NumVnodes; i++ {
		vn := vnode.New(i, conf.Hostname, conf.NumSuccessors, conf.HashBits)
		// Register with the RPC mechanism
		r.Transport.Register(vn)
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
		Stabilize(ring, vn)
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
		err = MergeErrors(err, Leave(r, vn))
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
	successors, err := r.Transport.FindSuccessors(nearest, n, keyHash)
	if err != nil {
		return nil, err
	}

	// Trim the nil successors
	for successors[len(successors)-1] == nil {
		successors = successors[:len(successors)-1]
	}
	return successors, nil
}

// Schedules the Vnode to do regular maintenance
func Schedule(ring *Ring, vn *vnode.Vnode) {
	// Setup our stabilize timer
	vn.Timer = time.AfterFunc(util.RandStabilize(ring.Config.StabilizeMin, ring.Config.StabilizeMax), func() { Stabilize(ring, vn) })
}

// Called to periodically stabilize the vnode
func Stabilize(ring *Ring, vn *vnode.Vnode) {
	// Clear the timer
	vn.Timer = nil

	// Check for shutdown
	if ring.Shutdown != nil {
		ring.ShutdownCh <- true
		return
	}

	// Setup the next stabilize timer
	defer Schedule(ring, vn)

	// Check for new successor
	if err := CheckNewSuccessor(ring.Transport, vn); err != nil {
		log.Printf("[ERR] Error checking for new successor: %s", err)
	}

	// Notify the successor
	if err := NotifySuccessor(ring.Transport, vn, ring.Config.NumSuccessors); err != nil {
		log.Printf("[ERR] Error notifying successor: %s", err)
	}

	// Finger table fix up
	if err := FixFingerTable(vn, ring.Transport); err != nil {
		log.Printf("[ERR] Error fixing finger table: %s", err)
	}

	// Check the predecessor
	if err := CheckPredecessor(ring.Transport, vn); err != nil {
		log.Printf("[ERR] Error checking predecessor: %s", err)
	}

	// Set the last stabilized time
	vn.Stabilized = time.Now()
}

// Instructs the vnode to leave
func Leave(ring *Ring, vn *vnode.Vnode) error {
	// Notify predecessor to advance to their next successor
	var err error
	trans := ring.Transport
	if vn.Predecessor != nil {
		err = trans.SkipSuccessor(vn.Predecessor, vn)
	}

	// Notify successor to clear old predecessor
	err = MergeErrors(err, trans.ClearPredecessor(vn.Successors[0], vn))
	return err
}

// Checks for a new successor
func CheckNewSuccessor(trans transport.Transport, vn *vnode.Vnode) error {
	// Ask our successor for it's predecessor

CHECK_NEW_SUC:
	succ := vn.Successors[0]
	if succ == nil {
		panic("Node has no successor!")
	}
	maybeSuc, err := trans.GetPredecessor(succ)
	if err != nil {
		// Check if we have succ list, try to contact next live succ
		known := vn.KnownSuccessors()
		if known > 1 {
			for i := 0; i < known; i++ {
				if alive, _ := trans.Ping(vn.Successors[0]); !alive {
					// Don't eliminate the last successor we know of
					if i+1 == known {
						return fmt.Errorf("All known successors dead!")
					}

					// Advance the successors list past the dead one
					copy(vn.Successors[0:], vn.Successors[1:])
					vn.Successors[known-1-i] = nil
				} else {
					// Found live successor, check for new one
					goto CHECK_NEW_SUC
				}
			}
		}
		return err
	}

	// Check if we should replace our successor
	if maybeSuc != nil && util.Between(vn.Id, succ.Id, maybeSuc.Id) {
		// Check if new successor is alive before switching
		alive, err := trans.Ping(maybeSuc)
		if alive && err == nil {
			copy(vn.Successors[1:], vn.Successors[0:len(vn.Successors)-1])
			vn.Successors[0] = maybeSuc
		} else {
			return err
		}
	}
	return nil
}

// Notifies our successor of us, updates successor list
func NotifySuccessor(trans transport.Transport, vn *vnode.Vnode, numSuccessors int) error {
	// Notify successor
	succ := vn.Successors[0]
	succList, err := trans.Notify(succ, vn)
	if err != nil {
		return err
	}

	// Trim the successors list if too long
	maxSucc := numSuccessors
	if len(succList) > maxSucc-1 {
		succList = succList[:maxSucc-1]
	}

	// Update local successors list
	for idx, s := range succList {
		if s == nil {
			break
		}
		// Ensure we don't set ourselves as a successor!
		if s == nil || s.String() == vn.String() {
			break
		}
		vn.Successors[idx+1] = s
	}
	return nil
}

// RPC: Notify is invoked when a Vnode gets notified
func Notify(trans *transport.LocalTransport, vn *vnode.Vnode, maybePred *vnode.Vnode) ([]*vnode.Vnode, error) {
	// Check if we should update our predecessor
	if vn.Predecessor == nil || util.Between(vn.Predecessor.Id, vn.Id, maybePred.Id) {
		vn.Predecessor = maybePred
	}

	// Return our successors list
	return vn.Successors, nil
}

// Fixes up the finger table
func FixFingerTable(vn *vnode.Vnode, trans transport.Transport) error {
	// Determine the offset
	hb := vn.HashFunc.Size() * 8
	offset := util.PowerOffset(vn.Id, vn.LastFinger, hb)

	// Find the successor
	nodes, err := FindSuccessors(trans, vn, 1, offset)
	if nodes == nil || len(nodes) == 0 || err != nil {
		return err
	}
	node := nodes[0]

	// Update the finger table
	vn.Finger[vn.LastFinger] = node

	// Try to skip as many finger entries as possible
	for {
		next := vn.LastFinger + 1
		if next >= hb {
			break
		}
		offset = util.PowerOffset(vn.Id, next, hb)

		// While the node is the successor, update the finger entries
		if util.BetweenRightIncl(vn.Id, node.Id, offset) {
			vn.Finger[next] = node
			vn.LastFinger = next
		} else {
			break
		}
	}

	// Increment to the index to repair
	if vn.LastFinger+1 == hb {
		vn.LastFinger = 0
	} else {
		vn.LastFinger++
	}

	return nil
}

// Checks the health of our predecessor
func CheckPredecessor(trans transport.Transport, vn *vnode.Vnode) error {
	// Check predecessor
	if vn.Predecessor != nil {
		res, err := trans.Ping(vn.Predecessor)
		if err != nil {
			return err
		}

		// Predecessor is dead
		if !res {
			vn.Predecessor = nil
		}
	}
	return nil
}

// Finds next N successors. N must be <= NumSuccessors
func FindSuccessors(trans transport.Transport, vn *vnode.Vnode, n int, key []byte) ([]*vnode.Vnode, error) {
	// Check if we are the immediate predecessor
	if util.BetweenRightIncl(vn.Id, vn.Successors[0].Id, key) {
		return vn.Successors[:n], nil
	}

	// Try the closest preceding nodes
	cp := vnode.NewClosestIter(vn, key)
	for {
		// Get the next closest node
		next := cp.Next()
		if next == nil {
			break
		}

		// Try that node, break on success
		res, err := trans.FindSuccessors(next, n, key)
		if err == nil {
			return res, nil
		} else {
			log.Printf("[ERR] Failed to contact %s. Got %s", next.String(), err)
		}
	}

	// Determine how many successors we know of
	successors := vn.KnownSuccessors()

	// Check if the ID is between us and any non-immediate successors
	for i := 1; i <= successors-n; i++ {
		if util.BetweenRightIncl(vn.Id, vn.Successors[i].Id, key) {
			remain := vn.Successors[i:]
			if len(remain) > n {
				remain = remain[:n]
			}
			return remain, nil
		}
	}

	// Checked all closer nodes and our successors!
	return nil, fmt.Errorf("exhausted all preceeding nodes")
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
func (r *Ring) NearestVnode(key []byte) *vnode.Vnode {
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
		Schedule(r, r.Vnodes[i])
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
			vn.Successors[i] = r.Vnodes[(idx+i+1)%numV]
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
	switch {
	case err1 == nil:
		return err2
	case err2 == nil:
		return err1
	default:
		return fmt.Errorf("%s\n%s", err1, err2)
	}
}
