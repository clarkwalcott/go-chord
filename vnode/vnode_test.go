package vnode

import (
	"bytes"
	"crypto/sha1"
	ring2 "github.com/go-chord/ring"
	"github.com/go-chord/transport"
	"sort"
	"testing"
	"time"
)

func makeVnode(idx int) *LocalVnode {
	min := 10 * time.Second
	max := 30 * time.Second
	conf := &ring2.Config{
		NumSuccessors: 8,
		StabilizeMin:  min,
		StabilizeMax:  max,
		HashFunc:      sha1.New(),
	}
	trans := transport.InitLocalTransport(nil)
	ring, _ := ring2.New(conf, trans)
	return New(ring, idx)
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

func TestVnodeSchedule(t *testing.T) {
	ring := makeRing()
	vn := makeVnode(0)
	Schedule(ring, vn)
	if vn.Timer == nil {
		t.Fatalf("unexpected nil")
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

func TestVnodeStabilizeShutdown(t *testing.T) {
	ring := makeRing()
	vn := makeVnode(0)
	Schedule(ring, vn)
	Stabilize(ring, vn)

	if vn.Timer != nil {
		t.Fatalf("unexpected timer")
	}
	if !vn.stabilized.IsZero() {
		t.Fatalf("unexpected time")
	}
	select {
	case <-ring.ShutdownCh:
		return
	default:
		t.Fatalf("expected message")
	}
}

func TestVnodeStabilizeReschedule(t *testing.T) {
	ring := makeRing()
	vn := makeVnode(1)
	vn.Successors[0] = &vn.Vnode
	Schedule(ring, vn)
	Stabilize(ring, vn)

	if vn.Timer == nil {
		t.Fatalf("expected timer")
	}
	if vn.stabilized.IsZero() {
		t.Fatalf("expected time")
	}
	vn.Timer.Stop()
}

func TestVnodeKnownSucc(t *testing.T) {
	vn := makeVnode(0)
	if vn.knownSuccessors() != 0 {
		t.Fatalf("wrong num known!")
	}
	vn.Successors[0] = &Vnode{Id: []byte{1}}
	if vn.knownSuccessors() != 1 {
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
	checkNewSuccessor(&transport.BlackholeTransport{}, vn1)
}

// Checks pinging a live successor with no changes
func TestVnodeCheckNewSuccAlive(t *testing.T) {
	vn1 := makeVnode(1)

	vn2 := makeVnode(2)
	vn2.Ring = vn1.Ring
	vn2.predecessor = &vn1.Vnode
	vn1.Successors[0] = &vn2.Vnode

	if pred, _ := vn2.GetPredecessor(); pred != &vn1.Vnode {
		t.Fatalf("expected vn1 as predecessor")
	}

	if err := vn1.checkNewSuccessor(); err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	if vn1.Successors[0] != &vn2.Vnode {
		t.Fatalf("unexpected successor!")
	}
}

// Checks pinging a dead successor with no alternates
func TestVnodeCheckNewSuccDead(t *testing.T) {
	vn1 := makeVnode(1)
	vn1.Successors[0] = &Vnode{Id: []byte{0}}

	if err := vn1.checkNewSuccessor(); err == nil {
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

	vn1.Successors[0] = &vn2.Vnode
	vn1.Successors[1] = &vn3.Vnode
	vn2.predecessor = &vn1.Vnode
	vn3.predecessor = &vn2.Vnode

	// Remove vn2
	(r.Transport.(*transport.LocalTransport)).Deregister(&vn2.Vnode)

	// Should not get an error
	if err := vn1.checkNewSuccessor(); err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	// Should become vn3
	if vn1.Successors[0] != &vn3.Vnode {
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

	vn1.Successors[0] = &vn2.Vnode
	vn1.Successors[1] = &vn3.Vnode
	vn2.predecessor = &vn1.Vnode
	vn3.predecessor = &vn2.Vnode

	// Remove vn2
	(r.Transport.(*transport.LocalTransport)).Deregister(&vn2.Vnode)
	(r.Transport.(*transport.LocalTransport)).Deregister(&vn3.Vnode)

	// Should get an error
	if err := vn1.checkNewSuccessor(); err != nil && err.Error() != "All known successors dead!" {
		t.Fatalf("unexpected err %s", err)
	}

	// Should just be vn3
	if vn1.Successors[0] != &vn3.Vnode {
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

	vn1.Successors[0] = &vn3.Vnode
	vn2.predecessor = &vn1.Vnode
	vn3.predecessor = &vn2.Vnode

	// vn3 pred is vn2
	if pred, _ := vn3.GetPredecessor(); pred != &vn2.Vnode {
		t.Fatalf("expected vn2 as predecessor")
	}

	// Should not get an error
	if err := vn1.checkNewSuccessor(); err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	// Should become vn2
	if vn1.Successors[0] != &vn2.Vnode {
		t.Fatalf("unexpected successor! %s", vn1.Successors[0])
	}

	// 2nd successor should become vn3
	if vn1.Successors[1] != &vn3.Vnode {
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

	vn1.Successors[0] = &vn3.Vnode
	vn2.predecessor = &vn1.Vnode
	vn3.predecessor = &vn2.Vnode

	// Remove vn2
	(r.Transport.(*transport.LocalTransport)).Deregister(&vn2.Vnode)

	// Should not get an error
	if err := vn1.checkNewSuccessor(); err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	// Should stay vn3
	if vn1.Successors[0] != &vn3.Vnode {
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
	vn1.Successors[0] = &vn2.Vnode
	vn2.predecessor = &vn1.Vnode
	vn2.Successors[0] = s1
	vn2.Successors[1] = s2
	vn2.Successors[2] = s3

	// Should get no error
	if err := vn1.notifySuccessor(); err != nil {
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
	if vn2.predecessor != &vn1.Vnode {
		t.Fatalf("bad predecessor")
	}
}

// Test notifying a dead successor
func TestVnodeNotifySuccDead(t *testing.T) {
	r := makeRing()
	sort.Sort(r)

	vn1 := r.Vnodes[0]
	vn2 := r.Vnodes[1]
	vn1.Successors[0] = &vn2.Vnode
	vn2.predecessor = &vn1.Vnode

	// Remove vn2
	(r.Transport.(*transport.LocalTransport)).Deregister(&vn2.Vnode)

	// Should get error
	if err := vn1.notifySuccessor(); err == nil {
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
	vn1.Successors[0] = &vn2.Vnode
	vn2.predecessor = &vn1.Vnode
	vn2.Successors[0] = s1
	vn2.Successors[1] = s2
	vn2.Successors[2] = s3

	succs, err := vn2.Notify(&vn1.Vnode)
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
	if vn2.predecessor != &vn1.Vnode {
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

	succs, err := vn2.Notify(&vn1.Vnode)
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
	if vn2.predecessor != &vn1.Vnode {
		t.Fatalf("unexpected pred")
	}
}

func TestVnodeNotifyNewPred(t *testing.T) {
	r := makeRing()
	sort.Sort(r)

	vn1 := r.Vnodes[0]
	vn2 := r.Vnodes[1]
	vn3 := r.Vnodes[2]
	vn3.predecessor = &vn1.Vnode

	_, err := vn3.Notify(&vn2.Vnode)
	if err != nil {
		t.Fatalf("unexpected error! %s", err)
	}
	if vn3.predecessor != &vn2.Vnode {
		t.Fatalf("unexpected pred")
	}
}

func TestVnodeFixFinger(t *testing.T) {
	r := makeRing()
	sort.Sort(r)
	num := len(r.Vnodes)
	for i := 0; i < num; i++ {
		r.Vnodes[i] = New(r, i)
		r.Vnodes[i].Successors[0] = &r.Vnodes[(i+1)%num].Vnode
	}

	// Fix finger should not error
	vn := r.Vnodes[0]
	if err := vn.fixFingerTable(); err != nil {
		t.Fatalf("unexpected err, %s", err)
	}

	// Check we've progressed
	if vn.lastFinger != 158 {
		t.Fatalf("unexpected last finger! %d", vn.lastFinger)
	}

	// Ensure that we've setup our successor as the initial entries
	for i := 0; i < vn.lastFinger; i++ {
		if vn.Finger[i] != vn.Successors[0] {
			t.Fatalf("unexpected finger entry!")
		}
	}

	// Fix next index
	if err := vn.fixFingerTable(); err != nil {
		t.Fatalf("unexpected err, %s", err)
	}
	if vn.lastFinger != 0 {
		t.Fatalf("unexpected last finger! %d", vn.lastFinger)
	}
}

func TestVnodeCheckPredNoPred(t *testing.T) {
	v := makeVnode(0)
	if err := v.checkPredecessor(); err != nil {
		t.Fatalf("unpexected err! %s", err)
	}
}

func TestVnodeCheckLivePred(t *testing.T) {
	r := makeRing()
	sort.Sort(r)

	vn1 := r.Vnodes[0]
	vn2 := r.Vnodes[1]
	vn2.predecessor = &vn1.Vnode

	if err := vn2.checkPredecessor(); err != nil {
		t.Fatalf("unexpected error! %s", err)
	}
	if vn2.predecessor != &vn1.Vnode {
		t.Fatalf("unexpected pred")
	}
}

func TestVnodeCheckDeadPred(t *testing.T) {
	r := makeRing()
	sort.Sort(r)

	vn1 := r.Vnodes[0]
	vn2 := r.Vnodes[1]
	vn2.predecessor = &vn1.Vnode

	// Deregister vn1
	(r.Transport.(*transport.LocalTransport)).Deregister(&vn1.Vnode)

	if err := vn2.checkPredecessor(); err != nil {
		t.Fatalf("unexpected error! %s", err)
	}
	if vn2.predecessor != nil {
		t.Fatalf("unexpected pred")
	}
}

func TestVnodeFindSuccessors(t *testing.T) {
	r := makeRing()
	sort.Sort(r)
	num := len(r.Vnodes)
	for i := 0; i < num; i++ {
		r.Vnodes[i].Successors[0] = &r.Vnodes[(i+1)%num].Vnode
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
		succ, err := vn.FindSuccessors(1, key)
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
		r.Vnodes[i].Successors[0] = &r.Vnodes[(i+1)%num].Vnode
		r.Vnodes[i].Successors[1] = &r.Vnodes[(i+2)%num].Vnode
		r.Vnodes[i].Successors[2] = &r.Vnodes[(i+3)%num].Vnode
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
		succ, err := vn.FindSuccessors(1, key)
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
		r.Vnodes[i].Successors[0] = &r.Vnodes[(i+1)%num].Vnode
		r.Vnodes[i].Successors[1] = &r.Vnodes[(i+2)%num].Vnode
	}

	// Kill 2 of the nodes
	(r.Transport.(*transport.LocalTransport)).Deregister(&r.Vnodes[0].Vnode)
	(r.Transport.(*transport.LocalTransport)).Deregister(&r.Vnodes[3].Vnode)

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
		succ, err := vn.FindSuccessors(1, key)
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
	v.predecessor = p
	v.ClearPredecessor(p)
	if v.predecessor != nil {
		t.Fatalf("expect no predecessor!")
	}

	np := &Vnode{Id: []byte{14}}
	v.predecessor = p
	v.ClearPredecessor(np)
	if v.predecessor != p {
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
	if v.knownSuccessors() != 2 {
		t.Fatalf("bad num of suc")
	}
}

func TestVnodeLeave(t *testing.T) {
	r := makeRing()
	sort.Sort(r)
	num := len(r.Vnodes)
	for i := int(0); i < num; i++ {
		r.Vnodes[i].predecessor = &r.Vnodes[(i+num-1)%num].Vnode
		r.Vnodes[i].Successors[0] = &r.Vnodes[(i+1)%num].Vnode
		r.Vnodes[i].Successors[1] = &r.Vnodes[(i+2)%num].Vnode
	}

	// Make node 0 leave
	if err := r.Vnodes[0].Leave(); err != nil {
		t.Fatalf("unexpected err")
	}

	if r.Vnodes[4].Successors[0] != &r.Vnodes[1].Vnode {
		t.Fatalf("unexpected suc!")
	}
	if r.Vnodes[1].predecessor != nil {
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
