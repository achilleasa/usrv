package flag

import (
	"testing"
	"time"

	"github.com/achilleasa/usrv/config"
)

func TestFloat32FlagUpdate(t *testing.T) {
	defer config.Store.Reset()
	config.Store.SetKey(1, "float32-flag", "123.0")

	f := NewFloat32("float32-flag")
	select {
	case <-f.ChangeChan():
	case <-time.After(1000 * time.Millisecond):
		t.Fatal("timeout waiting for flag change event")
	}

	var expVal float32 = 123.0
	val := f.Get()
	if val != expVal {
		t.Fatalf("expected val to be %f; got %f", expVal, val)
	}

	expVal = 999.0
	f.Set(expVal)
	val = f.Get()
	if val != expVal {
		t.Fatalf("expected val to be %f; got %f", expVal, val)
	}
}

func TestFloat32FlagUpdateWithInvalidValue(t *testing.T) {
	defer config.Store.Reset()
	config.Store.SetKey(1, "float32-flag-invalid", "invalid")

	f := NewFloat32("float32-flag-invalid")
	select {
	case <-f.ChangeChan():
		t.Fatal("unexpected configuration change event")
	case <-time.After(1000 * time.Millisecond):
	}
}

func TestFloat64FlagUpdate(t *testing.T) {
	defer config.Store.Reset()
	config.Store.SetKey(1, "float64-flag", "1234567890.0")

	f := NewFloat64("float64-flag")
	select {
	case <-f.ChangeChan():
	case <-time.After(1000 * time.Millisecond):
		t.Fatal("timeout waiting for flag change event")
	}

	var expVal = 1234567890.0
	val := f.Get()
	if val != expVal {
		t.Fatalf("expected val to be %f; got %f", expVal, val)
	}

	expVal = 999.0
	f.Set(expVal)
	val = f.Get()
	if val != expVal {
		t.Fatalf("expected val to be %f; got %f", expVal, val)
	}
}

func TestFloat64FlagUpdateWithInvalidValue(t *testing.T) {
	defer config.Store.Reset()
	config.Store.SetKey(1, "float64-flag-invalid", "invalid")

	f := NewFloat64("float64-flag-invalid")
	select {
	case <-f.ChangeChan():
		t.Fatal("unexpected configuration change event")
	case <-time.After(1000 * time.Millisecond):
	}
}
