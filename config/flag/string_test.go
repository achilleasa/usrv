package flag

import (
	"testing"
	"time"

	"github.com/achilleasa/usrv/config"
)

func TestStringFlagDynamicUpdate(t *testing.T) {
	defer config.Store.Reset()

	expValue := "123"
	config.Store.SetKey(1, "string-flag", expValue)

	f := NewString("string-flag")
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
	f := NewString("")
	expValue := "987"
	f.Set(expValue)

	val := f.Get()
	if val != expValue {
		t.Fatalf("expected val to be %q; got %q", expValue, val)
	}
}
