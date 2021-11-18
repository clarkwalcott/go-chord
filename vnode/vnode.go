package vnode

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/go-chord/util"
	"hash"
	"math/big"
	"time"
)

// Represents a local Vnode
type Vnode struct {
	Id           []byte // Virtual ID
	Host         string // Host identifier
	HashFunc     hash.Hash
	Successors   []*Vnode
	Finger       []*Vnode
	LastFinger   int
	Predecessor  *Vnode
	Stabilized   time.Time
	stabilizeMin time.Duration
	stabilizeMax time.Duration
	Timer        *time.Timer
}

// Converts the ID to string
func (vn *Vnode) String() string {
	return fmt.Sprintf("%x", vn.Id)
}

// Initializes a local vnode
func New(idx int, hostname string, numSuccessors int, hashBits int) *Vnode {
	vn := Vnode{}
	// Generate an ID
	vn.genId(uint16(idx))

	// Set our host
	vn.Host = hostname

	// Initialize all state
	vn.Successors = make([]*Vnode, numSuccessors)
	vn.Finger = make([]*Vnode, hashBits)

	return &vn
}

// Generates an ID for the node
func (vn *Vnode) genId(idx uint16) {
	// Use the hash function
	h := vn.HashFunc
	h.Write([]byte(vn.Host))
	binary.Write(h, binary.BigEndian, idx)

	// Use the hash as the ID
	vn.Id = h.Sum(nil)
}

// RPC: Invoked to return out predecessor
func (vn *Vnode) GetPredecessor() (*Vnode, error) {
	return vn.Predecessor, nil
}

// Used to clear our predecessor when a node is leaving
func (vn *Vnode) ClearPredecessor(p *Vnode) error {
	if vn.Predecessor != nil && vn.Predecessor.String() == p.String() {
		vn.Predecessor = nil
	}
	return nil
}

// Used to skip a successor when a node is leaving
func (vn *Vnode) SkipSuccessor(s *Vnode) error {
	// Skip if we have a match
	if vn.Successors[0].String() == s.String() {
		known := vn.KnownSuccessors()
		copy(vn.Successors[0:], vn.Successors[1:])
		vn.Successors[known-1] = nil
	}
	return nil
}

// Determine how many successors we know of
func (vn *Vnode) KnownSuccessors() (successors int) {
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

type closestPrecedingVnodeIterator struct {
	key          []byte
	vn           *Vnode
	fingerIdx    int
	successorIdx int
	yielded      map[string]struct{}
}

func NewClosestIter(vn *Vnode, key []byte) *closestPrecedingVnodeIterator {
	cp := closestPrecedingVnodeIterator{}
	cp.key = key
	cp.vn = vn
	cp.successorIdx = len(vn.Successors) - 1
	cp.fingerIdx = len(vn.Finger) - 1
	cp.yielded = make(map[string]struct{})

	return &cp
}

func (cp *closestPrecedingVnodeIterator) Next() *Vnode {
	// Try to find each node
	var successorNode *Vnode
	var fingerNode *Vnode

	// Scan to find the next successor
	vn := cp.vn
	var i int
	for i = cp.successorIdx; i >= 0; i-- {
		if vn.Successors[i] == nil {
			continue
		}
		if _, ok := cp.yielded[vn.Successors[i].String()]; ok {
			continue
		}
		if util.Between(vn.Id, cp.key, vn.Successors[i].Id) {
			successorNode = vn.Successors[i]
			break
		}
	}
	cp.successorIdx = i

	// Scan to find the next finger
	for i = cp.fingerIdx; i >= 0; i-- {
		if vn.Finger[i] == nil {
			continue
		}
		if _, ok := cp.yielded[vn.Finger[i].String()]; ok {
			continue
		}
		if util.Between(vn.Id, cp.key, vn.Finger[i].Id) {
			fingerNode = vn.Finger[i]
			break
		}
	}
	cp.fingerIdx = i

	// Determine which node is better
	if successorNode != nil && fingerNode != nil {
		// Determine the closer node
		hb := cp.vn.HashFunc.Size() * 8
		closest := closestPrecedingVnode(successorNode,
			fingerNode, cp.key, hb)
		if closest == successorNode {
			cp.successorIdx--
		} else {
			cp.fingerIdx--
		}
		cp.yielded[closest.String()] = struct{}{}
		return closest

	} else if successorNode != nil {
		cp.successorIdx--
		cp.yielded[successorNode.String()] = struct{}{}
		return successorNode

	} else if fingerNode != nil {
		cp.fingerIdx--
		cp.yielded[fingerNode.String()] = struct{}{}
		return fingerNode
	}

	return nil
}

// Returns the closest preceding Vnode to the key
func closestPrecedingVnode(a, b *Vnode, key []byte, bits int) *Vnode {
	aDist := distance(a.Id, key, bits)
	bDist := distance(b.Id, key, bits)
	if aDist.Cmp(bDist) <= 0 {
		return a
	} else {
		return b
	}
}

// Computes the forward distance from a to b modulus a ring size
func distance(a, b []byte, bits int) *big.Int {
	// Get the ringLen size in bits
	var ringLen big.Int
	ringLen.Exp(big.NewInt(2), big.NewInt(int64(bits)), nil)

	// Convert to int
	var x, y big.Int
	x.SetBytes(a)
	y.SetBytes(b)

	// Compute the distance y - x
	var dist big.Int
	dist.Sub(&y, &x)

	// Distance modulus the size of the ring
	dist.Mod(&dist, &ringLen)
	return &dist
}
