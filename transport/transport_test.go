package transport

import (
	"bytes"
	"github.com/go-chord/vnode"
	"testing"
)

type MockVnodeRPC struct {
	err      error
	pred     *vnode.Vnode
	notPred  *vnode.Vnode
	succList []*vnode.Vnode
	key      []byte
	succ     []*vnode.Vnode
	skip     *vnode.Vnode
}

func (mv *MockVnodeRPC) GetPredecessor() (*vnode.Vnode, error) {
	return mv.pred, mv.err
}
func (mv *MockVnodeRPC) Notify(trans *LocalTransport, vn *vnode.Vnode) ([]*vnode.Vnode, error) {
	mv.notPred = vn
	return mv.succList, mv.err
}
func (mv *MockVnodeRPC) FindSuccessors(trans *LocalTransport, n int, key []byte) ([]*vnode.Vnode, error) {
	mv.key = key
	return mv.succ, mv.err
}

func (mv *MockVnodeRPC) ClearPredecessor(p *vnode.Vnode) error {
	mv.pred = nil
	return nil
}

func (mv *MockVnodeRPC) SkipSuccessor(s *vnode.Vnode) error {
	mv.skip = s
	return nil
}

func makeLocal() *LocalTransport {
	return InitLocalTransport(nil).(*LocalTransport)
}

func TestInitLocalTransport(t *testing.T) {
	local := InitLocalTransport(nil).(*LocalTransport)
	if local.remote == nil {
		t.Fatalf("bad remote")
	}
	if local.local == nil {
		t.Fatalf("missing map")
	}
}

func TestLocalList(t *testing.T) {
	l := makeLocal()
	vn := &vnode.Vnode{Id: []byte{1}, Host: "test"}
	l.Register(vn)

	list, err := l.ListVnodes("test")
	if err != nil {
		t.Fatalf("unexpected err. %s", err)
	}
	if len(list) != 1 || list[0] != vn {
		t.Fatalf("local list failed: %+v", list)
	}
}

func TestLocalListRemote(t *testing.T) {
	l := makeLocal()
	vn := &vnode.Vnode{Id: []byte{1}, Host: "test"}
	l.Register(vn)

	_, err := l.ListVnodes("remote")
	if err == nil {
		t.Fatalf("expected err!")
	}
}

func TestLocalPing(t *testing.T) {
	l := makeLocal()
	vn := &vnode.Vnode{Id: []byte{1}}
	l.Register(vn)
	if res, err := l.Ping(vn); !res || err != nil {
		t.Fatalf("local ping failed")
	}
}

func TestLocalMissingPing(t *testing.T) {
	l := makeLocal()
	vn := &vnode.Vnode{Id: []byte{2}}
	l.Register(vn)

	// Print some random node
	vn2 := &vnode.Vnode{Id: []byte{3}}
	if res, _ := l.Ping(vn2); res {
		t.Fatalf("ping succeeded")
	}
}

func TestLocalGetPredecessor(t *testing.T) {
	l := makeLocal()
	pred := &vnode.Vnode{Id: []byte{10}}
	vn := &vnode.Vnode{Id: []byte{42}}
	l.Register(vn)

	vn2 := &vnode.Vnode{Id: []byte{42}}
	res, err := l.GetPredecessor(vn2)
	if err != nil {
		t.Fatalf("local GetPredecessor failed")
	}
	if res != pred {
		t.Fatalf("got wrong predecessor")
	}

	unknown := &vnode.Vnode{Id: []byte{1}}
	res, err = l.GetPredecessor(unknown)
	if err == nil {
		t.Fatalf("expected error!")
	}
}

func TestLocalNotify(t *testing.T) {
	l := makeLocal()
	suc1 := &vnode.Vnode{Id: []byte{10}}
	suc2 := &vnode.Vnode{Id: []byte{20}}
	suc3 := &vnode.Vnode{Id: []byte{30}}
	succ_list := []*vnode.Vnode{suc1, suc2, suc3}

	mockVN := &MockVnodeRPC{succList: succ_list, err: nil}
	vn := &vnode.Vnode{Id: []byte{0}}
	l.Register(vn)

	self := &vnode.Vnode{Id: []byte{60}}
	res, err := l.Notify(vn, self)
	if err != nil {
		t.Fatalf("local notify failed")
	}
	if res == nil || res[0] != suc1 || res[1] != suc2 || res[2] != suc3 {
		t.Fatalf("got wrong successor list")
	}
	if mockVN.notPred != self {
		t.Fatalf("didn't get notified correctly!")
	}

	unknown := &vnode.Vnode{Id: []byte{1}}
	res, err = l.Notify(unknown, self)
	if err == nil {
		t.Fatalf("remote notify should fail")
	}
}

func TestLocalFindSucc(t *testing.T) {
	l := makeLocal()
	suc := []*vnode.Vnode{&vnode.Vnode{Id: []byte{40}}}

	mockVN := &MockVnodeRPC{succ: suc, err: nil}
	vn := &vnode.Vnode{Id: []byte{12}}
	l.Register(vn)

	key := []byte("test")
	res, err := l.FindSuccessors(vn, 1, key)
	if err != nil {
		t.Fatalf("local FindSuccessor failed")
	}
	if res[0] != suc[0] {
		t.Fatalf("got wrong successor")
	}
	if bytes.Compare(mockVN.key, key) != 0 {
		t.Fatalf("didn't get key correctly!")
	}

	unknown := &vnode.Vnode{Id: []byte{1}}
	res, err = l.FindSuccessors(unknown, 1, key)
	if err == nil {
		t.Fatalf("remote find should fail")
	}
}

func TestLocalClearPred(t *testing.T) {
	l := makeLocal()
	pred := &vnode.Vnode{Id: []byte{10}}
	mockVN := &MockVnodeRPC{pred: pred}
	vn := &vnode.Vnode{Id: []byte{12}}
	l.Register(vn)

	err := l.ClearPredecessor(vn, pred)
	if err != nil {
		t.Fatalf("local ClearPredecessor failed")
	}
	if mockVN.pred != nil {
		t.Fatalf("clear failed")
	}

	unknown := &vnode.Vnode{Id: []byte{1}}
	err = l.ClearPredecessor(unknown, pred)
	if err == nil {
		t.Fatalf("remote clear should fail")
	}
}

func TestLocalSkipSucc(t *testing.T) {
	l := makeLocal()
	suc := []*vnode.Vnode{{Id: []byte{40}}}
	mockVN := &MockVnodeRPC{succ: suc}
	vn := &vnode.Vnode{Id: []byte{12}}
	l.Register(vn)

	s := &vnode.Vnode{Id: []byte{40}}
	err := l.SkipSuccessor(vn, s)
	if err != nil {
		t.Fatalf("local Skip failed")
	}
	if mockVN.skip != s {
		t.Fatalf("skip failed")
	}

	unknown := &vnode.Vnode{Id: []byte{1}}
	err = l.SkipSuccessor(unknown, s)
	if err == nil {
		t.Fatalf("remote skip should fail")
	}
}

func TestLocalDeregister(t *testing.T) {
	l := makeLocal()
	vn := &vnode.Vnode{Id: []byte{1}}
	l.Register(vn)
	if res, err := l.Ping(vn); !res || err != nil {
		t.Fatalf("local ping failed")
	}
	l.Deregister(vn)
	if res, _ := l.Ping(vn); res {
		t.Fatalf("local ping succeeded")
	}
}

func TestBHList(t *testing.T) {
	bh := BlackholeTransport{}
	res, err := bh.ListVnodes("test")
	if res != nil || err == nil {
		t.Fatalf("expected fail")
	}
}

func TestBHPing(t *testing.T) {
	bh := BlackholeTransport{}
	vn := &vnode.Vnode{Id: []byte{12}}
	res, err := bh.Ping(vn)
	if res || err != nil {
		t.Fatalf("expected fail")
	}
}

func TestBHGetPred(t *testing.T) {
	bh := BlackholeTransport{}
	vn := &vnode.Vnode{Id: []byte{12}}
	_, err := bh.GetPredecessor(vn)
	if err.Error()[:18] != "Failed to connect!" {
		t.Fatalf("expected fail")
	}
}

func TestBHNotify(t *testing.T) {
	bh := BlackholeTransport{}
	vn := &vnode.Vnode{Id: []byte{12}}
	vn2 := &vnode.Vnode{Id: []byte{42}}
	_, err := bh.Notify(vn, vn2)
	if err.Error()[:18] != "Failed to connect!" {
		t.Fatalf("expected fail")
	}
}

func TestBHFindSuccessors(t *testing.T) {
	bh := BlackholeTransport{}
	vn := &vnode.Vnode{Id: []byte{12}}
	_, err := bh.FindSuccessors(vn, 1, []byte("test"))
	if err.Error()[:18] != "Failed to connect!" {
		t.Fatalf("expected fail")
	}
}

func TestBHClearPred(t *testing.T) {
	bh := BlackholeTransport{}
	vn := &vnode.Vnode{Id: []byte{12}}
	s := &vnode.Vnode{Id: []byte{50}}
	err := bh.ClearPredecessor(vn, s)
	if err.Error()[:18] != "Failed to connect!" {
		t.Fatalf("expected fail")
	}
}

func TestBHSkipSucc(t *testing.T) {
	bh := BlackholeTransport{}
	vn := &vnode.Vnode{Id: []byte{12}}
	s := &vnode.Vnode{Id: []byte{50}}
	err := bh.SkipSuccessor(vn, s)
	if err.Error()[:18] != "Failed to connect!" {
		t.Fatalf("expected fail")
	}
}