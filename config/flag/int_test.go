package flag

import (
	"testing"
	"time"

	"github.com/achilleasa/usrv/config/store"
)

func TestUint32FlagUpdate(t *testing.T) {
	var s store.Store
	s.SetKey(1, "uint32-flag", "0")

	f := NewUint32(&s, "uint32-flag")
	go func() {
		<-time.After(100 * time.Millisecond)
		s.SetKey(1, "uint32-flag", "123")
	}()
	select {
	case <-f.ChangeChan():
	case <-time.After(1000 * time.Millisecond):
		t.Fatal("timeout waiting for flag change event")
	}

	var expVal uint32 = 123
	val := f.Get()
	if val != expVal {
		t.Fatalf("expected val to be %d; got %d", expVal, val)
	}

	expVal = 999
	f.Set(expVal)
	val = f.Get()
	if val != expVal {
		t.Fatalf("expected val to be %d; got %d", expVal, val)
	}
}

func TestUint32FlagUpdateWithInvalidValue(t *testing.T) {
	var s store.Store
	s.SetKey(1, "uint32-flag-invalid", "invalid")

	f := NewUint32(&s, "uint32-flag-invalid")
	select {
	case <-f.ChangeChan():
		t.Fatal("unexpected configuration change event")
	case <-time.After(1000 * time.Millisecond):
	}
}

func TestInt32FlagUpdate(t *testing.T) {
	var s store.Store
	s.SetKey(1, "int32-flag", "0")

	f := NewInt32(&s, "int32-flag")
	go func() {
		<-time.After(100 * time.Millisecond)
		s.SetKey(1, "int32-flag", "-123")
	}()
	select {
	case <-f.ChangeChan():
	case <-time.After(1000 * time.Millisecond):
		t.Fatal("timeout waiting for flag change event")
	}

	var expVal int32 = -123
	val := f.Get()
	if val != expVal {
		t.Fatalf("expected val to be %d; got %d", expVal, val)
	}

	expVal = 999
	f.Set(expVal)
	val = f.Get()
	if val != expVal {
		t.Fatalf("expected val to be %d; got %d", expVal, val)
	}
}

func TestInt32FlagUpdateWithInvalidValue(t *testing.T) {
	var s store.Store
	s.SetKey(1, "int32-flag-invalid", "-foo")

	f := NewInt32(&s, "int32-flag-invalid")
	select {
	case <-f.ChangeChan():
		t.Fatal("unexpected configuration change event")
	case <-time.After(1000 * time.Millisecond):
	}
}

func TestUint64FlagUpdate(t *testing.T) {
	var s store.Store
	s.SetKey(1, "uint64-flag", "0")

	f := NewUint64(&s, "uint64-flag")
	go func() {
		<-time.After(100 * time.Millisecond)
		s.SetKey(1, "uint64-flag", "1234567890")
	}()
	select {
	case <-f.ChangeChan():
	case <-time.After(1000 * time.Millisecond):
		t.Fatal("timeout waiting for flag change event")
	}

	var expVal uint64 = 1234567890
	val := f.Get()
	if val != expVal {
		t.Fatalf("expected val to be %d; got %d", expVal, val)
	}

	expVal = 999
	f.Set(expVal)
	val = f.Get()
	if val != expVal {
		t.Fatalf("expected val to be %d; got %d", expVal, val)
	}
}

func TestUint64FlagUpdateWithInvalidValue(t *testing.T) {
	var s store.Store
	s.SetKey(1, "uint64-flag-invalid", "-foo")

	f := NewUint64(&s, "uint64-flag-invalid")
	select {
	case <-f.ChangeChan():
		t.Fatal("unexpected configuration change event")
	case <-time.After(1000 * time.Millisecond):
	}
}

func TestInt64FlagUpdate(t *testing.T) {
	var s store.Store
	s.SetKey(1, "int64-flag", "0")

	f := NewInt64(&s, "int64-flag")
	go func() {
		<-time.After(100 * time.Millisecond)
		s.SetKey(1, "int64-flag", "-1234567890")
	}()
	select {
	case <-f.ChangeChan():
	case <-time.After(1000 * time.Millisecond):
		t.Fatal("timeout waiting for flag change event")
	}

	var expVal int64 = -1234567890
	val := f.Get()
	if val != expVal {
		t.Fatalf("expected val to be %d; got %d", expVal, val)
	}

	expVal = 999
	f.Set(expVal)
	val = f.Get()
	if val != expVal {
		t.Fatalf("expected val to be %d; got %d", expVal, val)
	}
}

func TestInt64FlagUpdateWithInvalidValue(t *testing.T) {
	var s store.Store
	s.SetKey(1, "int64-flag-invalid", "-foo")

	f := NewInt64(&s, "int64-flag-invalid")
	select {
	case <-f.ChangeChan():
		t.Fatal("unexpected configuration change event")
	case <-time.After(1000 * time.Millisecond):
	}
}
