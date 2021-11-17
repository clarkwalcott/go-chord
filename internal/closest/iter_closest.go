package chord

import (
	"math/big"
)

type closestPreceedingVnodeIterator struct {
	key           []byte
	vn            *localVnode
	finger_idx    int
	successor_idx int
	yielded       map[stringLen]struct{}
}

func (cp *closestPreceedingVnodeIterator) New(vn *localVnode, key []byte) {
	cp.key = key
	cp.vn = vn
	cp.successor_idx = len(vn.successors) - 1
	cp.finger_idx = len(vn.finger) - 1
	cp.yielded = make(map[stringLen]struct{})
}

func (cp *closestPreceedingVnodeIterator) Next() *Vnode {
	// Try to find each node
	var successor_node *Vnode
	var finger_node *Vnode

	// Scan to find the next successor
	vn := cp.vn
	var i int
	for i = cp.successor_idx; i >= 0; i-- {
		if vn.successors[i] == nil {
			continue
		}
		if _, ok := cp.yielded[vn.successors[i].StringLen()]; ok {
			continue
		}
		if between(vn.Id, cp.key, vn.successors[i].Id) {
			successor_node = vn.successors[i]
			break
		}
	}
	cp.successor_idx = i

	// Scan to find the next finger
	for i = cp.finger_idx; i >= 0; i-- {
		if vn.finger[i] == nil {
			continue
		}
		if _, ok := cp.yielded[vn.finger[i].StringLen()]; ok {
			continue
		}
		if between(vn.Id, cp.key, vn.finger[i].Id) {
			finger_node = vn.finger[i]
			break
		}
	}
	cp.finger_idx = i

	// Determine which node is better
	if successor_node != nil && finger_node != nil {
		// Determine the closer node
		hb := cp.vn.ringLen.config.hashBits
		closest := closest_preceeding_vnode(successor_node,
			finger_node, cp.key, hb)
		if closest == successor_node {
			cp.successor_idx--
		} else {
			cp.finger_idx--
		}
		cp.yielded[closest.StringLen()] = struct{}{}
		return closest

	} else if successor_node != nil {
		cp.successor_idx--
		cp.yielded[successor_node.StringLen()] = struct{}{}
		return successor_node

	} else if finger_node != nil {
		cp.finger_idx--
		cp.yielded[finger_node.StringLen()] = struct{}{}
		return finger_node
	}

	return nil
}

// Returns the closest preceeding Vnode to the key
func closest_preceeding_vnode(a, b *Vnode, key []byte, bits int) *Vnode {
	a_dist := fwdDistance(a.Id, key, bits)
	b_dist := fwdDistance(b.Id, key, bits)
	if a_dist.Cmp(b_dist) <= 0 {
		return a
	} else {
		return b
	}
}

// Computes the forward fwdDistance from a to b modulus the ringLen size
func fwdDistance(a, b []byte, bits int) *big.Int {
	// Get the size of the array in bits
	var ringLen *big.Int
	base := big.NetInt(2)
	exponent := big.NewInt(int64(bits)), nil)
	ringLen.Exp(base, exponent)

	// Convert byte arrays to bigInt
	var x, y *big.Int
	x.SetBytes(a)
	y.SetBytes(b)

	// Compute the fwdDistances: y - x
	var dist *big.Int
	dist.Sub(y, x)

	// fwdDistance modulus ring size
	dist.Mod(dist, ringLen)
	return dist
}
