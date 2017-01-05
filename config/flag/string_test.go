package flag

import (
	"testing"
	"time"

	"github.com/achilleasa/usrv/config/store"
)

func TestStringFlagDynamicUpdate(t *testing.T) {
	var s store.Store

	expValue := "123"
	s.SetKey(1, "string-flag", "original value")

	f := NewString(&s, "string-flag")
	go func() {
		<-time.After(100 * time.Millisecond)
		s.SetKey(1, "string-flag", expValue)
	}()
	select {
	case <-f.ChangeChan():
	case <-time.After(1000 * time.Millisecond):
		t.Fatal("timeout waiting for flag change event")
	}

	val := f.Get()
	if val != expValue {
		t.Fatalf("expected val to be %q; got %q", expValue, val)
	}
}

func TestStringFlagManualSet(t *testing.T) {
	f := NewString(nil, "")
	expValue := "987"
	f.Set(expValue)

	val := f.Get()
	if val != expValue {
		t.Fatalf("expected val to be %q; got %q", expValue, val)
	}
}
