package ring

import (
	"bytes"
	"crypto/sha1"
	"errors"
	"github.com/go-chord/transport"
	"github.com/go-chord/vnode"
	"runtime"
	"sort"
	"testing"
	"time"
)

type MultiLocalTrans struct {
	remote transport.Transport
	hosts  map[string]*transport.LocalTransport
}

func makeMultiLocalTransport() *MultiLocalTrans {
	hosts := make(map[string]*transport.LocalTransport)
	remote := &transport.BlackholeTransport{}
	ml := &MultiLocalTrans{hosts: hosts}
	ml.remote = remote
	return ml
}

func (ml *MultiLocalTrans) ListVnodes(host string) ([]*vnode.Vnode, error) {
	if local, ok := ml.hosts[host]; ok {
		return local.ListVnodes(host)
	}
	return ml.remote.ListVnodes(host)
}

// Ping a Vnode, check for liveness
func (ml *MultiLocalTrans) Ping(v *vnode.Vnode) (bool, error) {
	if local, ok := ml.hosts[v.Host]; ok {
		return local.Ping(v)
	}
	return ml.remote.Ping(v)
}

// Request a nodes predecessor
func (ml *MultiLocalTrans) GetPredecessor(v *vnode.Vnode) (*vnode.Vnode, error) {
	if local, ok := ml.hosts[v.Host]; ok {
		return local.GetPredecessor(v)
	}
	return ml.remote.GetPredecessor(v)
}

// Notify our successor of ourselves
func (ml *MultiLocalTrans) Notify(target, self *vnode.Vnode) ([]*vnode.Vnode, error) {
	if local, ok := ml.hosts[target.Host]; ok {
		return local.Notify(target, self)
	}
	return ml.remote.Notify(target, self)
}

// Find a successor
func (ml *MultiLocalTrans) FindSuccessors(v *vnode.Vnode, n int, k []byte) ([]*vnode.Vnode, error) {
	if local, ok := ml.hosts[v.Host]; ok {
		return local.FindSuccessors(v, n, k)
	}
	return ml.remote.FindSuccessors(v, n, k)
}

// Clears a predecessor if it matches a given vnode. Used to leave.
func (ml *MultiLocalTrans) ClearPredecessor(target, self *vnode.Vnode) error {
	if local, ok := ml.hosts[target.Host]; ok {
		return local.ClearPredecessor(target, self)
	}
	return ml.remote.ClearPredecessor(target, self)
}

// Instructs a node to skip a given successor. Used to leave.
func (ml *MultiLocalTrans) SkipSuccessor(target, self *vnode.Vnode) error {
	if local, ok := ml.hosts[target.Host]; ok {
		return local.SkipSuccessor(target, self)
	}
	return ml.remote.SkipSuccessor(target, self)
}

func (ml *MultiLocalTrans) Register(v *vnode.Vnode) {
	local, ok := ml.hosts[v.Host]
	if !ok {
		local = transport.InitLocalTransport(nil).(*transport.LocalTransport)
		ml.hosts[v.Host] = local
	}
	local.Register(v)
}

func (ml *MultiLocalTrans) Deregister(host string) {
	delete(ml.hosts, host)
}

func fastConf() *Config {
	conf := DefaultConfig("test")
	conf.StabilizeMin = 15 * time.Millisecond
	conf.StabilizeMax = 45 * time.Millisecond
	return conf
}

func makeRing() *Ring {
	conf := &Config{
		NumVnodes:     5,
		NumSuccessors: 8,
		HashFunc:      sha1.New(),
		HashBits:      160,
		StabilizeMin:  time.Second,
		StabilizeMax:  5 * time.Second,
	}

	ring, _ := New(conf, nil)
	return ring
}

func makeVnode(idx int) *vnode.Vnode {
	min := 10 * time.Second
	max := 30 * time.Second
	conf := &Config{
		NumSuccessors: 8,
		StabilizeMin:  min,
		StabilizeMax:  max,
		HashFunc:      sha1.New(),
	}
	return vnode.New(idx, conf.Hostname, conf.NumSuccessors, conf.HashBits)
}

func TestDefaultConfig(t *testing.T) {
	conf := DefaultConfig("test")
	if conf.Hostname != "test" {
		t.Fatalf("bad hostname")
	}
	if conf.NumVnodes != 8 {
		t.Fatalf("bad num vnodes")
	}
	if conf.NumSuccessors != 8 {
		t.Fatalf("bad num succ")
	}
	if conf.HashFunc == nil {
		t.Fatalf("bad hash")
	}
	if conf.HashBits != 160 {
		t.Fatalf("bad hash bits")
	}
	if conf.StabilizeMin != 15*time.Second {
		t.Fatalf("bad min stable")
	}
	if conf.StabilizeMax != 45*time.Second {
		t.Fatalf("bad max stable")
	}
}

func TestRingInit(t *testing.T) {
	// Create a ring
	conf := DefaultConfig("test")
	r, err := New(conf, nil)
	if err != nil {
		t.Fatalf("expected err to be nil, but got: %+v", err)
	}

	// Test features
	if r.Config != conf {
		t.Fatalf("bad config")
	}
	if r.Transport == nil {
		t.Fatalf("missing transport")
	}

	// Check the vnodes
	for i := 0; i < conf.NumVnodes; i++ {
		if r.Vnodes[i] == nil {
			t.Fatalf("missing vnode!")
		}
		if r.Vnodes[i].Id == nil {
			t.Fatalf("ID not initialized!")
		}
	}
}

func TestCreateShutdown(t *testing.T) {
	// Start the timer thread
	time.After(15)
	conf := fastConf()
	numGo := runtime.NumGoroutine()
	r, err := New(conf, nil)
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
	ml := makeMultiLocalTransport()

	// Create the initial ring
	conf := fastConf()
	r, err := New(conf, ml)
	if err != nil {
		t.Fatalf("unexpected err. %s", err)
	}

	// New a second ring
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
	// New a multi transport
	ml := makeMultiLocalTransport()

	// New the initial ring
	conf := fastConf()
	_, err := Join(conf, ml, "noop")
	if err == nil {
		t.Fatalf("expected err!")
	}
}

func TestLeave(t *testing.T) {
	// New a multi transport
	ml := makeMultiLocalTransport()

	// New the initial ring
	conf := fastConf()
	r, err := New(conf, ml)
	if err != nil {
		t.Fatalf("unexpected err. %s", err)
	}

	// New a second ring
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
	num := len(r2.Vnodes)
	for idx, vn := range r2.Vnodes {
		if vn.Successors[0] != r2.Vnodes[(idx+1)%num] {
			t.Fatalf("bad successor! Got:%s:%s", vn.Successors[0].Host,
				vn.Successors[0])
		}
	}
}

func TestLookupBadN(t *testing.T) {
	// New a multi transport
	ml := makeMultiLocalTransport()

	// New the initial ring
	conf := fastConf()
	r, err := New(conf, ml)
	if err != nil {
		t.Fatalf("unexpected err. %s", err)
	}

	_, err = r.Lookup(10, []byte("test"))
	if err == nil {
		t.Fatalf("expected err!")
	}
}

func TestLookup(t *testing.T) {
	// New a multi transport
	ml := makeMultiLocalTransport()

	// New the initial ring
	conf := fastConf()
	r, err := New(conf, ml)
	if err != nil {
		t.Fatalf("unexpected err. %s", err)
	}

	// New a second ring
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
	if bytes.Compare(ring.Vnodes[0].Id, ring.Vnodes[1].Id) != -1 {
		t.Fatalf("bad sort")
	}
	if bytes.Compare(ring.Vnodes[1].Id, ring.Vnodes[2].Id) != -1 {
		t.Fatalf("bad sort")
	}
	if bytes.Compare(ring.Vnodes[2].Id, ring.Vnodes[3].Id) != -1 {
		t.Fatalf("bad sort")
	}
	if bytes.Compare(ring.Vnodes[3].Id, ring.Vnodes[4].Id) != -1 {
		t.Fatalf("bad sort")
	}
}

func TestRingNearest(t *testing.T) {
	ring := makeRing()
	ring.Vnodes[0].Id = []byte{2}
	ring.Vnodes[1].Id = []byte{4}
	ring.Vnodes[2].Id = []byte{7}
	ring.Vnodes[3].Id = []byte{10}
	ring.Vnodes[4].Id = []byte{14}
	key := []byte{6}

	near := ring.NearestVnode(key)
	if near != ring.Vnodes[1] {
		t.Fatalf("got wrong node back!")
	}

	key = []byte{0}
	near = ring.NearestVnode(key)
	if near != ring.Vnodes[4] {
		t.Fatalf("got wrong node back!")
	}
}

func TestRingSchedule(t *testing.T) {
	ring := makeRing()
	ring.setLocalSuccessors()
	ring.schedule()
	for i := 0; i < len(ring.Vnodes); i++ {
		if ring.Vnodes[i].Timer == nil {
			t.Fatalf("expected timer!")
		}
	}
	ring.stopVnodes()
}

func TestRingSetLocalSucc(t *testing.T) {
	ring := makeRing()
	ring.setLocalSuccessors()
	for i := 0; i < len(ring.Vnodes); i++ {
		for j := 0; j < 4; j++ {
			if ring.Vnodes[i].Successors[j] == nil {
				t.Fatalf("expected successor!")
			}
		}
		if ring.Vnodes[i].Successors[4] != nil {
			t.Fatalf("should not have 5th successor!")
		}
	}

	// Verify the successor manually for node 3
	vn := ring.Vnodes[2]
	if vn.Successors[0] != ring.Vnodes[3] {
		t.Fatalf("bad succ!")
	}
	if vn.Successors[1] != ring.Vnodes[4] {
		t.Fatalf("bad succ!")
	}
	if vn.Successors[2] != ring.Vnodes[0] {
		t.Fatalf("bad succ!")
	}
	if vn.Successors[3] != ring.Vnodes[1] {
		t.Fatalf("bad succ!")
	}
}

func TestMergeErrors(t *testing.T) {
	e1 := errors.New("test1")
	e2 := errors.New("test2")

	if MergeErrors(e1, nil) != e1 {
		t.Fatalf("bad merge")
	}
	if MergeErrors(nil, e1) != e1 {
		t.Fatalf("bad merge")
	}
	if MergeErrors(nil, nil) != nil {
		t.Fatalf("bad merge")
	}
	if MergeErrors(e1, e2).Error() != "test1\ntest2" {
		t.Fatalf("bad merge")
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

func TestVnodeStabilizeShutdown(t *testing.T) {
	ring := makeRing()
	vn := makeVnode(0)
	Schedule(ring, vn)
	Stabilize(ring, vn)

	if vn.Timer != nil {
		t.Fatalf("unexpected timer")
	}
	if !vn.Stabilized.IsZero() {
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
	vn.Successors[0] = vn
	Schedule(ring, vn)
	Stabilize(ring, vn)

	if vn.Timer == nil {
		t.Fatalf("expected timer")
	}
	if vn.Stabilized.IsZero() {
		t.Fatalf("expected time")
	}
	vn.Timer.Stop()
}
