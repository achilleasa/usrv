package flag

import (
	"errors"
	"testing"
	"time"

	"github.com/achilleasa/usrv/config/store"
)

func TestFirstMapElement(t *testing.T) {
	specs := []struct {
		in     map[string]string
		expVal string
	}{
		{map[string]string{"_": "foo"}, "foo"},
		{map[string]string{}, ""},
	}

	for specIndex, spec := range specs {
		val := firstMapElement(spec.in)
		if val != spec.expVal {
			t.Errorf("[spec %d] expected %q; got %q\n", specIndex, spec.expVal, val)
		}
	}
}

func TestFlagPanicWhenNonEmptyPathAndNilStore(t *testing.T) {
	defer func() {
		expErr := "store cannot be nil"
		if err := recover(); err != nil {
			if err.(string) != expErr {
				t.Fatalf("expected error %q; got %v", expErr, err)
			}

			return
		}

		t.Fatalf("expected init to panic")
	}()

	f := &flagImpl{}
	f.init(nil, stringMapper, "/goo")
}

func TestFlagVersionComparisonLogic(t *testing.T) {
	var s store.Store
	f := &flagImpl{}

	// Init with no value in store
	f.init(&s, stringMapper, "/foo/bar")

	// Set with a version > 0
	go func() {
		expVersion := 1
		expValue := "value"

		<-time.After(100 * time.Millisecond)
		f.set(expVersion, expValue, true)

		f.rwMutex.RLock()
		defer f.rwMutex.RUnlock()

		if f.version != expVersion {
			t.Fatalf("expected version to be %d; got %d", expVersion, f.version)
		}
		if f.value != expValue {
			t.Fatalf("expected value to be %s; got %v", expValue, f.value)
		}
	}()
	<-f.changedChan

	// Set with a version less than the one stored should be a no-op
	go func() {
		expVersion := 1
		expValue := "value"

		<-time.After(100 * time.Millisecond)
		f.set(0, "new value", true)

		f.rwMutex.RLock()
		defer f.rwMutex.RUnlock()

		if f.version != expVersion {
			t.Fatalf("expected version to be %d; got %d", expVersion, f.version)
		}
		if f.value != expValue {
			t.Fatalf("expected value to be %s; got %v", expValue, f.value)
		}
	}()
	select {
	case <-f.changedChan:
		t.Fatalf("expected set not to trigger a change notification")
	case <-time.After(500 * time.Millisecond):
	}

	// Set with greater version but same value should only update the version but not trigger an update
	go func() {
		expVersion := 2
		expValue := "value"

		<-time.After(100 * time.Millisecond)
		f.set(expVersion, expValue, true)

		f.rwMutex.RLock()
		defer f.rwMutex.RUnlock()

		if f.version != expVersion {
			t.Fatalf("expected version to be %d; got %d", expVersion, f.version)
		}
	}()

	select {
	case <-f.changedChan:
		t.Fatalf("expected set not to trigger a change notification")
	case <-time.After(500 * time.Millisecond):
	}

	// Set with greater version and different valueMapper
	go func() {
		expValue := "new value"
		expVersion := 3

		<-time.After(100 * time.Millisecond)
		f.set(expVersion, expValue, true)

		f.rwMutex.RLock()
		defer f.rwMutex.RUnlock()

		if f.version != expVersion {
			t.Fatalf("expected version to be %d; got %d", expVersion, f.version)
		}
		if f.value != expValue {
			t.Fatalf("expected value to be %s; got %v", expValue, f.value)
		}
	}()
	<-f.changedChan
}

func TestFlagBlockOnGet(t *testing.T) {
	f := &flagImpl{}
	f.init(nil, stringMapper, "")
	go func() {
		<-time.After(300 * time.Millisecond)
		f.set(-1, "hello", false)
	}()

	expValue := "hello"
	value := f.get().(string)

	if value != expValue {
		t.Fatalf("expected value %q; got %q", expValue, value)
	}
}

func TestFlagChangeNotification(t *testing.T) {
	f := &flagImpl{}
	f.init(nil, stringMapper, "")
	go func() {
		<-time.After(300 * time.Millisecond)
		f.set(-1, "hello", false)
	}()

	select {
	case <-f.ChangeChan():
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for change evnet")
	}

	// Calling set twice with no notification listeners should
	// drop the second notification event
	f.set(-1, "hello1", false)
	f.set(-1, "hello2", false)
}

func TestFlagDynamicChange(t *testing.T) {
	var s store.Store

	f := &flagImpl{}
	f.init(&s, stringMapper, "generic-flag-1")
	expValue := "hello"
	go func() {
		<-time.After(100 * time.Millisecond)
		s.SetKey(1, "generic-flag-1", expValue)
	}()

	select {
	case <-f.ChangeChan():
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for change evnet")
	}

	value := f.get().(string)

	if value != expValue {
		t.Fatalf("expected value %q; got %q", expValue, value)
	}

	if !f.HasValue() {
		t.Fatalf("expected HasValue() to return true")
	}
}

func TestFlagCancelDynamicUpdates(t *testing.T) {
	var s store.Store
	s.SetKey(1, "generic-flag-2", "hello")

	f := &flagImpl{}
	f.init(&s, stringMapper, "generic-flag-2")
	f.CancelDynamicUpdates()

	// Calling cancel a second time should be a no-op
	f.CancelDynamicUpdates()

	expValue := "test"
	go func() {
		<-time.After(100 * time.Millisecond)
		f.set(10, expValue, true)
	}()

	select {
	case <-f.ChangeChan():
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for change evnet")
	}

	value := f.get().(string)

	if value != expValue {
		t.Fatalf("expected value %q; got %q", expValue, value)
	}
}

func TestFlagValueMapperError(t *testing.T) {
	var s store.Store
	s.SetKey(1, "generic-flag-3", "hello")

	f := &flagImpl{}
	f.init(&s, func(_ map[string]string) (interface{}, error) { return nil, errors.New("") }, "generic-flag-3")

	select {
	case <-f.ChangeChan():
		t.Fatal("unexpected change event")
	case <-time.After(1 * time.Second):
	}

	expValue := false
	hasValue := f.HasValue()
	if hasValue != expValue {
		t.Fatalf("expected HasValue() to return %t; got %t", expValue, hasValue)
	}
}

func stringMapper(cfg map[string]string) (interface{}, error) {
	return firstMapElement(cfg), nil
}
