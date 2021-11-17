package chord

import (
	"testing"
	"time"
)

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
	if conf.hashBits != 160 {
		t.Fatalf("bad hash bits")
	}
	if conf.StabilizeMin != time.Duration(15*time.Second) {
		t.Fatalf("bad min stable")
	}
	if conf.StabilizeMax != time.Duration(45*time.Second) {
		t.Fatalf("bad max stable")
	}
	if conf.Delegate != nil {
		t.Fatalf("bad delegate")
	}
}

func fastConf() *Config {
	conf := DefaultConfig("test")
	conf.StabilizeMin = time.Duration(15 * time.Millisecond)
	conf.StabilizeMax = time.Duration(45 * time.Millisecond)
	return conf
}
