package vnode

import (
	"bytes"
	"encoding/binary"
	"fmt"
	ring2 "github.com/go-chord/ring"
	"github.com/go-chord/transport"
	"github.com/go-chord/util"
	"github.com/go-chord/vnode/closest"
	"hash"
	"log"
	"time"
)

// TODO: extract Vnode to an interface so LocalVnode can actually be used interchangeably

// Represents an Vnode, local or remote
type Vnode struct {
	Id   []byte // Virtual ID
	Host string // Host identifier
}

// Represents a local Vnode
type LocalVnode struct {
	Vnode
	HashFunc    hash.Hash
	Successors  []*Vnode
	Finger      []*Vnode
	lastFinger  int
	predecessor *Vnode
	stabilized  time.Time
	stabilizeMin			time.Duration
	stabilizeMax			time.Duration
	Timer       *time.Timer
}

// Converts the ID to string
func (vn *Vnode) String() string {
	return fmt.Sprintf("%x", vn.Id)
}

// Initializes a local vnode
func New(ring *ring2.Ring, idx int) *LocalVnode {
	vn := LocalVnode{}
	// Generate an ID
	vn.genId(uint16(idx))

	// Set our host
	vn.Host = ring.Config.Hostname

	// Initialize all state
	vn.Successors = make([]*Vnode, ring.Config.NumSuccessors)
	vn.Finger = make([]*Vnode, ring.Config.HashBits)

	// Register with the RPC mechanism
	ring.Transport.Register(&vn.Vnode, &vn)

	return &vn
}

// Schedules the Vnode to do regular maintenance
func Schedule(ring *ring2.Ring, vn *LocalVnode) {
	// Setup our stabilize timer
	vn.Timer = time.AfterFunc(util.RandStabilize(vn.stabilizeMin, vn.stabilizeMax), func(){Stabilize(ring, vn)})
}

// Generates an ID for the node
func (vn *LocalVnode) genId(idx uint16) {
	// Use the hash function
	h := vn.HashFunc
	h.Write([]byte(vn.Host))
	binary.Write(h, binary.BigEndian, idx)

	// Use the hash as the ID
	vn.Id = h.Sum(nil)
}

// Called to periodically stabilize the vnode
func Stabilize(ring *ring2.Ring, vn *LocalVnode) {
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
	if err := checkNewSuccessor(ring.Transport, vn); err != nil {
		log.Printf("[ERR] Error checking for new successor: %s", err)
	}

	// Notify the successor
	if err := notifySuccessor(ring.Transport, vn, ring.Config.NumSuccessors); err != nil {
		log.Printf("[ERR] Error notifying successor: %s", err)
	}

	// Finger table fix up
	if err := vn.fixFingerTable(ring.Transport); err != nil {
		log.Printf("[ERR] Error fixing finger table: %s", err)
	}

	// Check the predecessor
	if err := checkPredecessor(ring.Transport, vn); err != nil {
		log.Printf("[ERR] Error checking predecessor: %s", err)
	}

	// Set the last stabilized time
	vn.stabilized = time.Now()
}

// Checks for a new successor
func checkNewSuccessor(trans transport.Transport, vn *LocalVnode) error {
	// Ask our successor for it's predecessor

CHECK_NEW_SUC:
	succ := vn.Successors[0]
	if succ == nil {
		panic("Node has no successor!")
	}
	maybeSuc, err := trans.GetPredecessor(succ)
	if err != nil {
		// Check if we have succ list, try to contact next live succ
		known := vn.knownSuccessors()
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

// RPC: Invoked to return out predecessor
func (vn *LocalVnode) GetPredecessor() (*Vnode, error) {
	return vn.predecessor, nil
}

// Notifies our successor of us, updates successor list
func notifySuccessor(trans transport.Transport, vn *LocalVnode, numSuccessors int) error {
	// Notify successor
	succ := vn.Successors[0]
	succList, err := trans.Notify(succ, &vn.Vnode)
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
func (vn *LocalVnode) Notify(trans *transport.LocalTransport, maybePred *Vnode) ([]*Vnode, error) {
	// Check if we should update our predecessor
	if vn.predecessor == nil || util.Between(vn.predecessor.Id, vn.Id, maybePred.Id) {
		vn.predecessor = maybePred
	}

	// Return our successors list
	return vn.Successors, nil
}

// Fixes up the finger table
func (vn *LocalVnode) fixFingerTable(trans transport.Transport) error {
	// Determine the offset
	hb := vn.HashFunc.Size() * 8
	offset := util.PowerOffset(vn.Id, vn.lastFinger, hb)

	// Find the successor
	nodes, err := trans.FindSuccessors(vn ,1, offset)
	if nodes == nil || len(nodes) == 0 || err != nil {
		return err
	}
	node := nodes[0]

	// Update the finger table
	vn.Finger[vn.lastFinger] = node

	// Try to skip as many finger entries as possible
	for {
		next := vn.lastFinger + 1
		if next >= hb {
			break
		}
		offset = util.PowerOffset(vn.Id, next, hb)

		// While the node is the successor, update the finger entries
		if util.BetweenRightIncl(vn.Id, node.Id, offset) {
			vn.Finger[next] = node
			vn.lastFinger = next
		} else {
			break
		}
	}

	// Increment to the index to repair
	if vn.lastFinger+1 == hb {
		vn.lastFinger = 0
	} else {
		vn.lastFinger++
	}

	return nil
}

// Checks the health of our predecessor
func checkPredecessor(trans transport.Transport, vn *LocalVnode) error {
	// Check predecessor
	if vn.predecessor != nil {
		res, err := trans.Ping(vn.predecessor)
		if err != nil {
			return err
		}

		// Predecessor is dead
		if !res {
			vn.predecessor = nil
		}
	}
	return nil
}

// Finds next N successors. N must be <= NumSuccessors
func (vn *LocalVnode) FindSuccessors(trans *transport.LocalTransport, n int, key []byte) ([]*Vnode, error) {
	// Check if we are the immediate predecessor
	if util.BetweenRightIncl(vn.Id, vn.Successors[0].Id, key) {
		return vn.Successors[:n], nil
	}

	// Try the closest preceding nodes
	cp := closest.New(vn, key)
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
	successors := vn.knownSuccessors()

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

// Instructs the vnode to leave
func (vn *LocalVnode) Leave(ring *ring2.Ring) error {
	// Notify predecessor to advance to their next successor
	var err error
	trans := ring.Transport
	if vn.predecessor != nil {
		err = trans.SkipSuccessor(vn.predecessor, &vn.Vnode)
	}

	// Notify successor to clear old predecessor
	err = ring2.MergeErrors(err, trans.ClearPredecessor(vn.Successors[0], &vn.Vnode))
	return err
}

// Used to clear our predecessor when a node is leaving
func (vn *LocalVnode) ClearPredecessor(p *Vnode) error {
	if vn.predecessor != nil && vn.predecessor.String() == p.String() {
		vn.predecessor = nil
	}
	return nil
}

// Used to skip a successor when a node is leaving
func (vn *LocalVnode) SkipSuccessor(s *Vnode) error {
	// Skip if we have a match
	if vn.Successors[0].String() == s.String() {
		known := vn.knownSuccessors()
		copy(vn.Successors[0:], vn.Successors[1:])
		vn.Successors[known-1] = nil
	}
	return nil
}

// Determine how many successors we know of
func (vn *LocalVnode) knownSuccessors() (successors int) {
	for i := 0; i < len(vn.Successors); i++ {
		if vn.Successors[i] != nil {
			successors = i + 1
		}
	}
	return
}

// Returns the vnode nearest a key
func NearestVnodeToKey(vnodes []*Vnode, key []byte) *Vnode {
	for i := len(vnodes) - 1; i >= 0; i-- {
		if bytes.Compare(vnodes[i].Id, key) == -1 {
			return vnodes[i]
		}
	}
	// Return the last vnode
	return vnodes[len(vnodes)-1]
}
