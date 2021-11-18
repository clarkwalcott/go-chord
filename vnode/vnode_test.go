package vnode

import (
	"bytes"
	"crypto/sha1"
	ring2 "github.com/go-chord/ring"
	"github.com/go-chord/transport"
	"math/big"
	"sort"
	"testing"
	"time"
)

func makeVnode(idx int) *Vnode {
	min := 10 * time.Second
	max := 30 * time.Second
	conf := &ring2.Config{
		NumSuccessors: 8,
		StabilizeMin:  min,
		StabilizeMax:  max,
		HashFunc:      sha1.New(),
	}
	return New(idx, conf.Hostname, conf.NumSuccessors, conf.HashBits)
}

func makeRing() *ring2.Ring {
	conf := &ring2.Config{
		NumVnodes:     5,
		NumSuccessors: 8,
		HashFunc:      sha1.New(),
		HashBits:      160,
		StabilizeMin:  time.Second,
		StabilizeMax:  5 * time.Second,
	}

	ring, err := ring2.New(conf, nil)
	if err != nil {
		return nil
	}
	return ring
}

func TestVnodeNew(t *testing.T) {
	actual := makeVnode(0)

	if actual.Id == nil {
		t.Fatalf("unexpected nil")
	}
	if actual.Successors == nil {
		t.Fatalf("unexpected nil")
	}
	if actual.Finger == nil {
		t.Fatalf("unexpected nil")
	}
	if actual.Timer != nil {
		t.Fatalf("unexpected timer")
	}
}

func TestGenId(t *testing.T) {
	vn := makeVnode(0)
	var ids [][]byte
	for i := 0; i < 16; i++ {
		vn.genId(uint16(i))
		ids = append(ids, vn.Id)
	}

	for idx, val := range ids {
		for i := 0; i < len(ids); i++ {
			if idx != i && bytes.Compare(ids[i], val) == 0 {
				t.Fatalf("unexpected id collision!")
			}
		}
	}
}

func TestVnodeKnownSucc(t *testing.T) {
	vn := makeVnode(0)
	if vn.KnownSuccessors() != 0 {
		t.Fatalf("wrong num known!")
	}
	vn.Successors[0] = &Vnode{Id: []byte{1}}
	if vn.KnownSuccessors() != 1 {
		t.Fatalf("wrong num known!")
	}
}

// Checks panic if no successors
func TestVnodeCheckNewSuccAlivePanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic!")
		}
	}()
	vn1 := makeVnode(1)
	ring2.CheckNewSuccessor(&transport.BlackholeTransport{}, vn1)
}

// Checks pinging a live successor with no changes
func TestVnodeCheckNewSuccAlive(t *testing.T) {
	ring := makeRing()
	vn1 := makeVnode(1)

	vn2 := makeVnode(2)
	vn2.Predecessor = vn1
	vn1.Successors[0] = vn2

	if pred, _ := vn2.GetPredecessor(); pred != vn1 {
		t.Fatalf("expected vn1 as predecessor")
	}

	if err := ring2.CheckNewSuccessor(ring.Transport, vn1); err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	if vn1.Successors[0] != vn2 {
		t.Fatalf("unexpected successor!")
	}
}

// Checks pinging a dead successor with no alternates
func TestVnodeCheckNewSuccDead(t *testing.T) {
	ring := makeRing()
	vn1 := makeVnode(1)
	vn1.Successors[0] = &Vnode{Id: []byte{0}}

	if err := ring2.CheckNewSuccessor(ring.Transport, vn1); err == nil {
		t.Fatalf("err: %+v", err)
	}

	if vn1.Successors[0].String() != "00" {
		t.Fatalf("unexpected successor!")
	}
}

// Checks pinging a dead successor with alternate
func TestVnodeCheckNewSuccDeadAlternate(t *testing.T) {
	r := makeRing()
	sort.Sort(r)

	vn1 := r.Vnodes[0]
	vn2 := r.Vnodes[1]
	vn3 := r.Vnodes[2]

	vn1.Successors[0] = vn2
	vn1.Successors[1] = vn3
	vn2.Predecessor = vn1
	vn3.Predecessor = vn2

	// Remove vn2
	(r.Transport.(*transport.LocalTransport)).Deregister(vn2)

	// Should not get an error
	if err := ring2.CheckNewSuccessor(r.Transport, vn1); err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	// Should become vn3
	if vn1.Successors[0] != vn3 {
		t.Fatalf("unexpected successor!")
	}
}

// Checks pinging a dead successor with all dead alternates
func TestVnodeCheckNewSuccAllDeadAlternates(t *testing.T) {
	r := makeRing()
	sort.Sort(r)

	vn1 := r.Vnodes[0]
	vn2 := r.Vnodes[1]
	vn3 := r.Vnodes[2]

	vn1.Successors[0] = vn2
	vn1.Successors[1] = vn3
	vn2.Predecessor = vn1
	vn3.Predecessor = vn2

	// Remove vn2
	(r.Transport.(*transport.LocalTransport)).Deregister(vn2)
	(r.Transport.(*transport.LocalTransport)).Deregister(vn3)

	// Should get an error
	if err := ring2.CheckNewSuccessor(r.Transport, vn1); err != nil && err.Error() != "All known successors dead!" {
		t.Fatalf("unexpected err %s", err)
	}

	// Should just be vn3
	if vn1.Successors[0] != vn3 {
		t.Fatalf("unexpected successor!")
	}
}

// Checks pinging a successor, and getting a new successor
func TestVnodeCheckNewSuccNewSucc(t *testing.T) {
	r := makeRing()
	sort.Sort(r)

	vn1 := r.Vnodes[0]
	vn2 := r.Vnodes[1]
	vn3 := r.Vnodes[2]

	vn1.Successors[0] = vn3
	vn2.Predecessor = vn1
	vn3.Predecessor = vn2

	// vn3 pred is vn2
	if pred, _ := vn3.GetPredecessor(); pred != vn2 {
		t.Fatalf("expected vn2 as predecessor")
	}

	// Should not get an error
	if err := ring2.CheckNewSuccessor(r.Transport, vn1); err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	// Should become vn2
	if vn1.Successors[0] != vn2 {
		t.Fatalf("unexpected successor! %s", vn1.Successors[0])
	}

	// 2nd successor should become vn3
	if vn1.Successors[1] != vn3 {
		t.Fatalf("unexpected 2nd successor!")
	}
}

// Checks pinging a successor, and getting a new successor
// which is not alive
func TestVnodeCheckNewSuccNewSuccDead(t *testing.T) {
	r := makeRing()
	sort.Sort(r)

	vn1 := r.Vnodes[0]
	vn2 := r.Vnodes[1]
	vn3 := r.Vnodes[2]

	vn1.Successors[0] = vn3
	vn2.Predecessor = vn1
	vn3.Predecessor = vn2

	// Remove vn2
	(r.Transport.(*transport.LocalTransport)).Deregister(vn2)

	// Should not get an error
	if err := ring2.CheckNewSuccessor(r.Transport, vn1); err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	// Should stay vn3
	if vn1.Successors[0] != vn3 {
		t.Fatalf("unexpected successor!")
	}
}

// Test notifying a successor successfully
func TestVnodeNotifySucc(t *testing.T) {
	r := makeRing()
	sort.Sort(r)

	s1 := &Vnode{Id: []byte{1}}
	s2 := &Vnode{Id: []byte{2}}
	s3 := &Vnode{Id: []byte{3}}

	vn1 := r.Vnodes[0]
	vn2 := r.Vnodes[1]
	vn1.Successors[0] = vn2
	vn2.Predecessor = vn1
	vn2.Successors[0] = s1
	vn2.Successors[1] = s2
	vn2.Successors[2] = s3

	// Should get no error
	if err := ring2.NotifySuccessor(r.Transport, vn1, r.Config.NumSuccessors); err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	// Successor list should be updated
	if vn1.Successors[1] != s1 {
		t.Fatalf("bad succ 1")
	}
	if vn1.Successors[2] != s2 {
		t.Fatalf("bad succ 2")
	}
	if vn1.Successors[3] != s3 {
		t.Fatalf("bad succ 3")
	}

	// Predecessor should not updated
	if vn2.Predecessor != vn1 {
		t.Fatalf("bad predecessor")
	}
}

// Test notifying a dead successor
func TestVnodeNotifySuccDead(t *testing.T) {
	r := makeRing()
	sort.Sort(r)

	vn1 := r.Vnodes[0]
	vn2 := r.Vnodes[1]
	vn1.Successors[0] = vn2
	vn2.Predecessor = vn1

	// Remove vn2
	(r.Transport.(*transport.LocalTransport)).Deregister(vn2)

	// Should get error
	if err := ring2.NotifySuccessor(r.Transport, vn1, r.Config.NumSuccessors); err == nil {
		t.Fatalf("expected err!")
	}
}

func TestVnodeNotifySamePred(t *testing.T) {
	r := makeRing()
	sort.Sort(r)

	s1 := &Vnode{Id: []byte{1}}
	s2 := &Vnode{Id: []byte{2}}
	s3 := &Vnode{Id: []byte{3}}

	vn1 := r.Vnodes[0]
	vn2 := r.Vnodes[1]
	vn1.Successors[0] = vn2
	vn2.Predecessor = vn1
	vn2.Successors[0] = s1
	vn2.Successors[1] = s2
	vn2.Successors[2] = s3

	succs, err := r.Transport.Notify(vn2, vn1)
	if err != nil {
		t.Fatalf("unexpected error! %s", err)
	}
	if succs[0] != s1 {
		t.Fatalf("unexpected succ 0")
	}
	if succs[1] != s2 {
		t.Fatalf("unexpected succ 1")
	}
	if succs[2] != s3 {
		t.Fatalf("unexpected succ 2")
	}
	if vn2.Predecessor != vn1 {
		t.Fatalf("unexpected pred")
	}
}

func TestVnodeNotifyNoPred(t *testing.T) {
	r := makeRing()
	sort.Sort(r)

	s1 := &Vnode{Id: []byte{1}}
	s2 := &Vnode{Id: []byte{2}}
	s3 := &Vnode{Id: []byte{3}}

	vn1 := r.Vnodes[0]
	vn2 := r.Vnodes[1]
	vn2.Successors[0] = s1
	vn2.Successors[1] = s2
	vn2.Successors[2] = s3

	succs, err := r.Transport.Notify(vn2, vn1)
	if err != nil {
		t.Fatalf("unexpected error! %s", err)
	}
	if succs[0] != s1 {
		t.Fatalf("unexpected succ 0")
	}
	if succs[1] != s2 {
		t.Fatalf("unexpected succ 1")
	}
	if succs[2] != s3 {
		t.Fatalf("unexpected succ 2")
	}
	if vn2.Predecessor != vn1 {
		t.Fatalf("unexpected pred")
	}
}

func TestVnodeNotifyNewPred(t *testing.T) {
	r := makeRing()
	sort.Sort(r)

	vn1 := r.Vnodes[0]
	vn2 := r.Vnodes[1]
	vn3 := r.Vnodes[2]
	vn3.Predecessor = vn1

	_, err := r.Transport.Notify(vn3, vn2)
	if err != nil {
		t.Fatalf("unexpected error! %s", err)
	}
	if vn3.Predecessor != vn2 {
		t.Fatalf("unexpected pred")
	}
}

func TestVnodeFixFinger(t *testing.T) {
	r := makeRing()
	sort.Sort(r)
	num := len(r.Vnodes)
	for i := 0; i < num; i++ {
		r.Vnodes[i] = New(i, r.Config.Hostname, r.Config.NumSuccessors, r.Config.HashBits)
		r.Vnodes[i].Successors[0] = r.Vnodes[(i+1)%num]
	}

	// Fix finger should not error
	vn := r.Vnodes[0]
	if err := ring2.FixFingerTable(vn, r.Transport); err != nil {
		t.Fatalf("unexpected err, %s", err)
	}

	// Check we've progressed
	if vn.LastFinger != 158 {
		t.Fatalf("unexpected last finger! %d", vn.LastFinger)
	}

	// Ensure that we've setup our successor as the initial entries
	for i := 0; i < vn.LastFinger; i++ {
		if vn.Finger[i] != vn.Successors[0] {
			t.Fatalf("unexpected finger entry!")
		}
	}

	// Fix next index
	if err := ring2.FixFingerTable(vn, r.Transport); err != nil {
		t.Fatalf("unexpected err, %s", err)
	}
	if vn.LastFinger != 0 {
		t.Fatalf("unexpected last finger! %d", vn.LastFinger)
	}
}

func TestVnodeCheckPredNoPred(t *testing.T) {
	r := makeRing()
	v := makeVnode(0)
	if err := ring2.CheckPredecessor(r.Transport, v); err != nil {
		t.Fatalf("unpexected err! %s", err)
	}
}

func TestVnodeCheckLivePred(t *testing.T) {
	r := makeRing()
	sort.Sort(r)

	vn1 := r.Vnodes[0]
	vn2 := r.Vnodes[1]
	vn2.Predecessor = vn1

	if err := ring2.CheckPredecessor(r.Transport, vn2); err != nil {
		t.Fatalf("unexpected error! %s", err)
	}
	if vn2.Predecessor != vn1 {
		t.Fatalf("unexpected pred")
	}
}

func TestVnodeCheckDeadPred(t *testing.T) {
	r := makeRing()
	sort.Sort(r)

	vn1 := r.Vnodes[0]
	vn2 := r.Vnodes[1]
	vn2.Predecessor = vn1

	// Deregister vn1
	(r.Transport.(*transport.LocalTransport)).Deregister(vn1)

	if err := ring2.CheckPredecessor(r.Transport, vn2); err != nil {
		t.Fatalf("unexpected error! %s", err)
	}
	if vn2.Predecessor != nil {
		t.Fatalf("unexpected pred")
	}
}

func TestVnodeFindSuccessors(t *testing.T) {
	r := makeRing()
	sort.Sort(r)
	num := len(r.Vnodes)
	for i := 0; i < num; i++ {
		r.Vnodes[i].Successors[0] = r.Vnodes[(i+1)%num]
	}

	// Get a random key
	h := r.Config.HashFunc
	h.Write([]byte("test"))
	key := h.Sum(nil)

	// Local only, should be nearest in the ring
	nearest := r.NearestVnode(key)
	exp := nearest.Successors[0]

	// Do a lookup on the key
	for i := 0; i < len(r.Vnodes); i++ {
		vn := r.Vnodes[i]
		succ, err := ring2.FindSuccessors(r.Transport.(*transport.LocalTransport), vn, 1, key)
		if err != nil {
			t.Fatalf("unexpected err! %s", err)
		}

		// Local only, should be nearest in the ring
		if exp != succ[0] {
			t.Fatalf("unexpected succ! K:%x Exp: %s Got:%s",
				key, exp, succ[0])
		}
	}
}

// Ensure each node has multiple successors
func TestVnodeFindSuccessorsMultSucc(t *testing.T) {
	r := makeRing()
	sort.Sort(r)
	num := len(r.Vnodes)
	for i := 0; i < num; i++ {
		r.Vnodes[i].Successors[0] = r.Vnodes[(i+1)%num]
		r.Vnodes[i].Successors[1] = r.Vnodes[(i+2)%num]
		r.Vnodes[i].Successors[2] = r.Vnodes[(i+3)%num]
	}

	// Get a random key
	h := r.Config.HashFunc
	h.Write([]byte("test"))
	key := h.Sum(nil)

	// Local only, should be nearest in the ring
	nearest := r.NearestVnode(key)
	exp := nearest.Successors[0]

	// Do a lookup on the key
	for i := 0; i < len(r.Vnodes); i++ {
		vn := r.Vnodes[i]
		succ, err := ring2.FindSuccessors(r.Transport.(*transport.LocalTransport), vn, 1, key)
		if err != nil {
			t.Fatalf("unexpected err! %s", err)
		}

		// Local only, should be nearest in the ring
		if exp != succ[0] {
			t.Fatalf("unexpected succ! K:%x Exp: %s Got:%s",
				key, exp, succ[0])
		}
	}
}

// Kill off a part of the ring and see what happens
func TestVnodeFindSuccessorsSomeDead(t *testing.T) {
	r := makeRing()
	sort.Sort(r)
	num := len(r.Vnodes)
	for i := 0; i < num; i++ {
		r.Vnodes[i].Successors[0] = r.Vnodes[(i+1)%num]
		r.Vnodes[i].Successors[1] = r.Vnodes[(i+2)%num]
	}

	// Kill 2 of the nodes
	(r.Transport.(*transport.LocalTransport)).Deregister(r.Vnodes[0])
	(r.Transport.(*transport.LocalTransport)).Deregister(r.Vnodes[3])

	// Get a random key
	h := r.Config.HashFunc
	h.Write([]byte("test"))
	key := h.Sum(nil)

	// Local only, should be nearest in the ring
	nearest := r.NearestVnode(key)
	exp := nearest.Successors[0]

	// Do a lookup on the key
	for i := 0; i < len(r.Vnodes); i++ {
		vn := r.Vnodes[i]
		succ, err := ring2.FindSuccessors(r.Transport.(*transport.LocalTransport), vn, 1, key)
		if err != nil {
			t.Fatalf("(%d) unexpected err! %s", i, err)
		}

		// Local only, should be nearest in the ring
		if exp != succ[0] {
			t.Fatalf("(%d) unexpected succ! K:%x Exp: %s Got:%s",
				i, key, exp, succ[0])
		}
	}
}

func TestVnodeClearPred(t *testing.T) {
	v := makeVnode(0)
	p := &Vnode{Id: []byte{12}}
	v.Predecessor = p
	v.ClearPredecessor(p)
	if v.Predecessor != nil {
		t.Fatalf("expect no predecessor!")
	}

	np := &Vnode{Id: []byte{14}}
	v.Predecessor = p
	v.ClearPredecessor(np)
	if v.Predecessor != p {
		t.Fatalf("expect p predecessor!")
	}
}

func TestVnodeSkipSucc(t *testing.T) {
	v := makeVnode(0)

	s1 := &Vnode{Id: []byte{10}}
	s2 := &Vnode{Id: []byte{11}}
	s3 := &Vnode{Id: []byte{12}}

	v.Successors[0] = s1
	v.Successors[1] = s2
	v.Successors[2] = s3

	// s2 should do nothing
	if err := v.SkipSuccessor(s2); err != nil {
		t.Fatalf("unexpected err")
	}
	if v.Successors[0] != s1 {
		t.Fatalf("unexpected suc")
	}

	// s1 should skip
	if err := v.SkipSuccessor(s1); err != nil {
		t.Fatalf("unexpected err")
	}
	if v.Successors[0] != s2 {
		t.Fatalf("unexpected suc")
	}
	if v.KnownSuccessors() != 2 {
		t.Fatalf("bad num of suc")
	}
}

func TestVnodeLeave(t *testing.T) {
	r := makeRing()
	sort.Sort(r)
	num := len(r.Vnodes)
	for i := int(0); i < num; i++ {
		r.Vnodes[i].Predecessor = r.Vnodes[(i+num-1)%num]
		r.Vnodes[i].Successors[0] = r.Vnodes[(i+1)%num]
		r.Vnodes[i].Successors[1] = r.Vnodes[(i+2)%num]
	}

	// Make node 0 leave
	if err := ring2.Leave(r, r.Vnodes[0]); err != nil {
		t.Fatalf("unexpected err")
	}

	if r.Vnodes[4].Successors[0] != r.Vnodes[1] {
		t.Fatalf("unexpected suc!")
	}
	if r.Vnodes[1].Predecessor != nil {
		t.Fatalf("unexpected pred!")
	}
}

func TestNearestVnodesKey(t *testing.T) {
	Vnodes := make([]*Vnode, 5)
	Vnodes[0] = &Vnode{Id: []byte{2}}
	Vnodes[1] = &Vnode{Id: []byte{4}}
	Vnodes[2] = &Vnode{Id: []byte{7}}
	Vnodes[3] = &Vnode{Id: []byte{10}}
	Vnodes[4] = &Vnode{Id: []byte{14}}
	key := []byte{6}

	near := NearestVnodeToKey(Vnodes, key)
	if near != Vnodes[1] {
		t.Fatalf("got wrong node back!")
	}

	key = []byte{0}
	near = NearestVnodeToKey(Vnodes, key)
	if near != Vnodes[4] {
		t.Fatalf("got wrong node back!")
	}
}

func TestNextClosest(t *testing.T) {
	// Make the vnodes on the ring (mod 64)
	v1 := &Vnode{Id: []byte{1}}
	v2 := &Vnode{Id: []byte{10}}
	//v3 := &Vnode{Id: []byte{20}}
	v4 := &Vnode{Id: []byte{32}}
	//v5 := &Vnode{Id: []byte{40}}
	v6 := &Vnode{Id: []byte{59}}
	v7 := &Vnode{Id: []byte{62}}

	// Make a vnode
	vn := &Vnode{}
	vn.Id = []byte{54}
	vn.Successors = []*Vnode{v6, v7, nil}
	vn.Finger = []*Vnode{v6, v6, v7, v1, v2, v4, nil}

	// Make an iterator
	k := []byte{32}
	cp := NewClosestIter(vn, k)

	// Iterate until we are done
	s1 := cp.Next()
	if s1 != v2 {
		t.Fatalf("Expect v2. %v", s1)
	}

	s2 := cp.Next()
	if s2 != v1 {
		t.Fatalf("Expect v1. %v", s2)
	}

	s3 := cp.Next()
	if s3 != v7 {
		t.Fatalf("Expect v7. %v", s3)
	}

	s4 := cp.Next()
	if s4 != v6 {
		t.Fatalf("Expect v6. %v", s4)
	}

	s5 := cp.Next()
	if s5 != nil {
		t.Fatalf("Expect nil. %v", s5)
	}
}

func TestNextClosestNoSucc(t *testing.T) {
	// Make the vnodes on the ring (mod 64)
	v1 := &Vnode{Id: []byte{1}}
	v2 := &Vnode{Id: []byte{10}}
	//v3 := &Vnode{Id: []byte{20}}
	v4 := &Vnode{Id: []byte{32}}
	//v5 := &Vnode{Id: []byte{40}}
	v6 := &Vnode{Id: []byte{59}}
	v7 := &Vnode{Id: []byte{62}}

	// Make a vnode
	vn := &Vnode{}
	vn.Id = []byte{54}
	vn.Successors = []*Vnode{nil}
	vn.Finger = []*Vnode{v6, v6, v7, v1, v2, v4, nil}

	// Make an iterator
	k := []byte{32}
	cp := NewClosestIter(vn, k)

	// Iterate until we are done
	s1 := cp.Next()
	if s1 != v2 {
		t.Fatalf("Expect v2. %v", s1)
	}

	s2 := cp.Next()
	if s2 != v1 {
		t.Fatalf("Expect v1. %v", s2)
	}

	s3 := cp.Next()
	if s3 != v7 {
		t.Fatalf("Expect v7. %v", s3)
	}

	s4 := cp.Next()
	if s4 != v6 {
		t.Fatalf("Expect v6. %v", s4)
	}

	s5 := cp.Next()
	if s5 != nil {
		t.Fatalf("Expect nil. %v", s5)
	}
}

func TestNextClosestNoFinger(t *testing.T) {
	// Make the vnodes on the ring (mod 64)
	//v1 := &Vnode{Id: []byte{1}}
	//v2 := &Vnode{Id: []byte{10}}
	//v3 := &Vnode{Id: []byte{20}}
	//v4 := &Vnode{Id: []byte{32}}
	//v5 := &Vnode{Id: []byte{40}}
	v6 := &Vnode{Id: []byte{59}}
	v7 := &Vnode{Id: []byte{62}}

	// Make a vnode
	vn := &Vnode{}
	vn.Id = []byte{54}
	vn.Successors = []*Vnode{v6, v7, v7, nil}
	vn.Finger = []*Vnode{nil, nil, nil}

	// Make an iterator
	k := []byte{32}
	cp := NewClosestIter(vn, k)

	// Iterate until we are done
	s3 := cp.Next()
	if s3 != v7 {
		t.Fatalf("Expect v7. %v", s3)
	}

	s4 := cp.Next()
	if s4 != v6 {
		t.Fatalf("Expect v6. %v", s4)
	}

	s5 := cp.Next()
	if s5 != nil {
		t.Fatalf("Expect nil. %v", s5)
	}
}

func TestClosest(t *testing.T) {
	a := &Vnode{Id: []byte{128}}
	b := &Vnode{Id: []byte{32}}
	k := []byte{45}
	c := closestPrecedingVnode(a, b, k, 8)
	if c != b {
		t.Fatalf("expect b to be closer!")
	}
	c = closestPrecedingVnode(b, a, k, 8)
	if c != b {
		t.Fatalf("expect b to be closer!")
	}
}

func TestDistance(t *testing.T) {
	a := []byte{63}
	b := []byte{3}
	d := distance(a, b, 6) // Ring size of 64
	if d.Cmp(big.NewInt(4)) != 0 {
		t.Fatalf("expect distance 4! %v", d)
	}

	a = []byte{0}
	b = []byte{65}
	d = distance(a, b, 7) // Ring size of 128
	if d.Cmp(big.NewInt(65)) != 0 {
		t.Fatalf("expect distance 65! %v", d)
	}

	a = []byte{1}
	b = []byte{255}
	d = distance(a, b, 8) // Ring size of 256
	if d.Cmp(big.NewInt(254)) != 0 {
		t.Fatalf("expect distance 254! %v", d)
	}
}
