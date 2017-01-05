package flag

import (
	"testing"
	"time"

	"github.com/achilleasa/usrv/config/store"
)

func TestBoolFlagUpdate(t *testing.T) {
	var s store.Store
	s.SetKey(1, "bool-flag", "false")

	f := NewBool(&s, "bool-flag")
	go func() {
		<-time.After(100 * time.Millisecond)
		s.SetKey(1, "bool-flag", "true")
	}()
	select {
	case <-f.ChangeChan():
	case <-time.After(1000 * time.Millisecond):
		t.Fatal("timeout waiting for flag change event")
	}

	expVal := true
	val := f.Get()
	if val != expVal {
		t.Fatalf("expected val to be %v; got %v", expVal, val)
	}

	expVal = false
	f.Set(expVal)
	val = f.Get()
	if val != expVal {
		t.Fatalf("expected val to be %v; got %v", expVal, val)
	}
}

func TestBoolFlagUpdateWithInvalidValue(t *testing.T) {
	var s store.Store
	s.SetKey(1, "bool-flag-invalid", "non-bool-value")

	f := NewBool(&s, "bool-flag-invalid")
	select {
	case <-f.ChangeChan():
		t.Fatal("unexpected configuration change event")
	case <-time.After(1000 * time.Millisecond):
	}
}

func TestBoolFlagValueMapper(t *testing.T) {
	specs := []struct {
		in     string
		expVal interface{}
		expErr error
	}{
		{"TrUE", true, nil},
		{"1", true, nil},
		{"FalSE", false, nil},
		{"0", false, nil},
		{"foo", nil, errNotBoolean},
	}

	f := NewBool(nil, "")

	for specIndex, spec := range specs {
		val, err := f.mapCfgValue(map[string]string{"value": spec.in})
		if val != spec.expVal {
			t.Errorf("[spec %d] expected mapped value to be %v; got %v", specIndex, spec.expVal, val)
			continue
		}
		if err != spec.expErr {
			t.Errorf("[spec %d] expected value mapper to return err %v; got %v", specIndex, spec.expErr, err)
		}
	}
}
