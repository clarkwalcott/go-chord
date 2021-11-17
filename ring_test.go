package chord

import (
	"bytes"
	"crypto/sha1"
	"runtime"
	"sort"
	"testing"
	"time"
)

type MockDelegate struct {
	shutdown bool
}

func (m *MockDelegate) NewPredecessor(local, remoteNew, remotePrev *Vnode) {
}
func (m *MockDelegate) Leaving(local, pred, succ *Vnode) {
}
func (m *MockDelegate) PredecessorLeaving(local, remote *Vnode) {
}
func (m *MockDelegate) SuccessorLeaving(local, remote *Vnode) {
}
func (m *MockDelegate) Shutdown() {
	m.shutdown = true
}

type MultiLocalTrans struct {
	remote Transport
	hosts  map[string]*LocalTransport
}

func makeMLTransport() *MultiLocalTrans {
	hosts := make(map[string]*LocalTransport)
	remote := &BlackholeTransport{}
	ml := &MultiLocalTrans{hosts: hosts}
	ml.remote = remote
	return ml
}

func (ml *MultiLocalTrans) ListVnodes(host string) ([]*Vnode, error) {
	if local, ok := ml.hosts[host]; ok {
		return local.ListVnodes(host)
	}
	return ml.remote.ListVnodes(host)
}

// Ping a Vnode, check for liveness
func (ml *MultiLocalTrans) Ping(v *Vnode) (bool, error) {
	if local, ok := ml.hosts[v.Host]; ok {
		return local.Ping(v)
	}
	return ml.remote.Ping(v)
}

// Request a nodes predecessor
func (ml *MultiLocalTrans) GetPredecessor(v *Vnode) (*Vnode, error) {
	if local, ok := ml.hosts[v.Host]; ok {
		return local.GetPredecessor(v)
	}
	return ml.remote.GetPredecessor(v)
}

// Notify our successor of ourselves
func (ml *MultiLocalTrans) Notify(target, self *Vnode) ([]*Vnode, error) {
	if local, ok := ml.hosts[target.Host]; ok {
		return local.Notify(target, self)
	}
	return ml.remote.Notify(target, self)
}

// Find a successor
func (ml *MultiLocalTrans) FindSuccessors(v *Vnode, n int, k []byte) ([]*Vnode, error) {
	if local, ok := ml.hosts[v.Host]; ok {
		return local.FindSuccessors(v, n, k)
	}
	return ml.remote.FindSuccessors(v, n, k)
}

// Clears a predecessor if it matches a given vnode. Used to leave.
func (ml *MultiLocalTrans) ClearPredecessor(target, self *Vnode) error {
	if local, ok := ml.hosts[target.Host]; ok {
		return local.ClearPredecessor(target, self)
	}
	return ml.remote.ClearPredecessor(target, self)
}

// Instructs a node to skip a given successor. Used to leave.
func (ml *MultiLocalTrans) SkipSuccessor(target, self *Vnode) error {
	if local, ok := ml.hosts[target.Host]; ok {
		return local.SkipSuccessor(target, self)
	}
	return ml.remote.SkipSuccessor(target, self)
}

func (ml *MultiLocalTrans) Register(v *Vnode, o VnodeRPC) {
	local, ok := ml.hosts[v.Host]
	if !ok {
		local = InitLocalTransport(nil).(*LocalTransport)
		ml.hosts[v.Host] = local
	}
	local.Register(v, o)
}

func (ml *MultiLocalTrans) Deregister(host string) {
	delete(ml.hosts, host)
}

func makeRing() *Ring {
	conf := &Config{
		NumVnodes:     5,
		NumSuccessors: 8,
		HashFunc:      sha1.New,
		hashBits:      160,
		StabilizeMin:  time.Second,
		StabilizeMax:  5 * time.Second,
	}

	ring := &Ring{}
	ring.init(conf, nil)
	return ring
}

func TestRingInit(t *testing.T) {
	// Create a ring
	ring := &Ring{}
	conf := DefaultConfig("test")
	ring.init(conf, nil)

	// Test features
	if ring.config != conf {
		t.Fatalf("wrong config")
	}
	if ring.transport == nil {
		t.Fatalf("missing transport")
	}

	// Check the vnodes
	for i := 0; i < conf.NumVnodes; i++ {
		if ring.vnodes[i] == nil {
			t.Fatalf("missing vnode!")
		}
		if ring.vnodes[i].ring != ring {
			t.Fatalf("ring missing!")
		}
		if ring.vnodes[i].Id == nil {
			t.Fatalf("ID not initialized!")
		}
	}
}

func TestCreateShutdown(t *testing.T) {
	// Start the timer thread
	time.After(15)
	conf := fastConf()
	numGo := runtime.NumGoroutine()
	r, err := Create(conf, nil)
	if err != nil {
		t.Fatalf("unexpected err. %s", err)
	}
	r.Shutdown()
	after := runtime.NumGoroutine()
	if after != numGo {
		t.Fatalf("unexpected routines! A:%d B:%d", after, numGo)
	}
}

func TestJoin(t *testing.T) {
	// Create a multi transport
	ml := makeMLTransport()

	// Create the initial ring
	conf := fastConf()
	r, err := Create(conf, ml)
	if err != nil {
		t.Fatalf("unexpected err. %s", err)
	}

	// Create a second ring
	conf2 := fastConf()
	conf2.Hostname = "test2"
	r2, err := Join(conf2, ml, "test")
	if err != nil {
		t.Fatalf("failed to join local node! Got %s", err)
	}

	// Shutdown
	r.Shutdown()
	r2.Shutdown()
}

func TestJoinDeadHost(t *testing.T) {
	// Create a multi transport
	ml := makeMLTransport()

	// Create the initial ring
	conf := fastConf()
	_, err := Join(conf, ml, "noop")
	if err == nil {
		t.Fatalf("expected err!")
	}
}

func TestLeave(t *testing.T) {
	// Create a multi transport
	ml := makeMLTransport()

	// Create the initial ring
	conf := fastConf()
	r, err := Create(conf, ml)
	if err != nil {
		t.Fatalf("unexpected err. %s", err)
	}

	// Create a second ring
	conf2 := fastConf()
	conf2.Hostname = "test2"
	r2, err := Join(conf2, ml, "test")
	if err != nil {
		t.Fatalf("failed to join local node! Got %s", err)
	}

	// Wait for some stabilization
	<-time.After(100 * time.Millisecond)

	// Node 1 should leave
	r.Leave()
	ml.Deregister("test")

	// Wait for stabilization
	<-time.After(100 * time.Millisecond)

	// Verify r2 ring is still in tact
	num := len(r2.vnodes)
	for idx, vn := range r2.vnodes {
		if vn.successors[0] != &r2.vnodes[(idx+1)%num].Vnode {
			t.Fatalf("bad successor! Got:%s:%s", vn.successors[0].Host,
				vn.successors[0])
		}
	}
}

func TestLookupBadN(t *testing.T) {
	// Create a multi transport
	ml := makeMLTransport()

	// Create the initial ring
	conf := fastConf()
	r, err := Create(conf, ml)
	if err != nil {
		t.Fatalf("unexpected err. %s", err)
	}

	_, err = r.Lookup(10, []byte("test"))
	if err == nil {
		t.Fatalf("expected err!")
	}
}

func TestLookup(t *testing.T) {
	// Create a multi transport
	ml := makeMLTransport()

	// Create the initial ring
	conf := fastConf()
	r, err := Create(conf, ml)
	if err != nil {
		t.Fatalf("unexpected err. %s", err)
	}

	// Create a second ring
	conf2 := fastConf()
	conf2.Hostname = "test2"
	r2, err := Join(conf2, ml, "test")
	if err != nil {
		t.Fatalf("failed to join local node! Got %s", err)
	}

	// Wait for some stabilization
	<-time.After(100 * time.Millisecond)

	// Try key lookup
	keys := [][]byte{[]byte("test"), []byte("foo"), []byte("bar")}
	for _, k := range keys {
		vn1, err := r.Lookup(3, k)
		if err != nil {
			t.Fatalf("unexpected err %s", err)
		}
		vn2, err := r2.Lookup(3, k)
		if err != nil {
			t.Fatalf("unexpected err %s", err)
		}
		if len(vn1) != len(vn2) {
			t.Fatalf("result len differs!")
		}
		for idx := range vn1 {
			if vn1[idx].String() != vn2[idx].String() {
				t.Fatalf("results differ!")
			}
		}
	}
}

func TestRingLen(t *testing.T) {
	ring := makeRing()
	if ring.Len() != 5 {
		t.Fatalf("wrong len")
	}
}

func TestRingSort(t *testing.T) {
	ring := makeRing()
	sort.Sort(ring)
	if bytes.Compare(ring.vnodes[0].Id, ring.vnodes[1].Id) != -1 {
		t.Fatalf("bad sort")
	}
	if bytes.Compare(ring.vnodes[1].Id, ring.vnodes[2].Id) != -1 {
		t.Fatalf("bad sort")
	}
	if bytes.Compare(ring.vnodes[2].Id, ring.vnodes[3].Id) != -1 {
		t.Fatalf("bad sort")
	}
	if bytes.Compare(ring.vnodes[3].Id, ring.vnodes[4].Id) != -1 {
		t.Fatalf("bad sort")
	}
}

func TestRingNearest(t *testing.T) {
	ring := makeRing()
	ring.vnodes[0].Id = []byte{2}
	ring.vnodes[1].Id = []byte{4}
	ring.vnodes[2].Id = []byte{7}
	ring.vnodes[3].Id = []byte{10}
	ring.vnodes[4].Id = []byte{14}
	key := []byte{6}

	near := ring.nearestVnode(key)
	if near != ring.vnodes[1] {
		t.Fatalf("got wrong node back!")
	}

	key = []byte{0}
	near = ring.nearestVnode(key)
	if near != ring.vnodes[4] {
		t.Fatalf("got wrong node back!")
	}
}

func TestRingSchedule(t *testing.T) {
	ring := makeRing()
	ring.setLocalSuccessors()
	ring.schedule()
	for i := 0; i < len(ring.vnodes); i++ {
		if ring.vnodes[i].timer == nil {
			t.Fatalf("expected timer!")
		}
	}
	ring.stopVnodes()
}

func TestRingSetLocalSucc(t *testing.T) {
	ring := makeRing()
	ring.setLocalSuccessors()
	for i := 0; i < len(ring.vnodes); i++ {
		for j := 0; j < 4; j++ {
			if ring.vnodes[i].successors[j] == nil {
				t.Fatalf("expected successor!")
			}
		}
		if ring.vnodes[i].successors[4] != nil {
			t.Fatalf("should not have 5th successor!")
		}
	}

	// Verify the successor manually for node 3
	vn := ring.vnodes[2]
	if vn.successors[0] != &ring.vnodes[3].Vnode {
		t.Fatalf("bad succ!")
	}
	if vn.successors[1] != &ring.vnodes[4].Vnode {
		t.Fatalf("bad succ!")
	}
	if vn.successors[2] != &ring.vnodes[0].Vnode {
		t.Fatalf("bad succ!")
	}
	if vn.successors[3] != &ring.vnodes[1].Vnode {
		t.Fatalf("bad succ!")
	}
}

func TestRingDelegate(t *testing.T) {
	d := &MockDelegate{}
	ring := makeRing()
	ring.setLocalSuccessors()
	ring.config.Delegate = d
	ring.schedule()

	var b bool
	f := func() {
		println("run!")
		b = true
	}
	ch := ring.invokeDelegate(f)
	if ch == nil {
		t.Fatalf("expected chan")
	}
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatalf("timeout")
	}
	if !b {
		t.Fatalf("b should be true")
	}

	ring.stopDelegate()
	if !d.shutdown {
		t.Fatalf("delegate did not get shutdown")
	}
}
