package closest

import (
	"github.com/go-chord/util"
	"github.com/go-chord/vnode"
	"math/big"
)

type closestPrecedingVnodeIterator struct {
	key          []byte
	vn           *vnode.LocalVnode
	fingerIdx    int
	successorIdx int
	yielded      map[string]struct{}
}

func New(vn *vnode.LocalVnode, key []byte) *closestPrecedingVnodeIterator {
	cp := closestPrecedingVnodeIterator{}
	cp.key = key
	cp.vn = vn
	cp.successorIdx = len(vn.Successors) - 1
	cp.fingerIdx = len(vn.Finger) - 1
	cp.yielded = make(map[string]struct{})

	return &cp
}

func (cp *closestPrecedingVnodeIterator) Next() *vnode.Vnode {
	// Try to find each node
	var successorNode *vnode.Vnode
	var fingerNode *vnode.Vnode

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
func closestPrecedingVnode(a, b *vnode.Vnode, key []byte, bits int) *vnode.Vnode {
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
