package flag

import (
	"errors"
	"testing"
	"time"

	"github.com/achilleasa/usrv/config"
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

func TestFlagBlockOnGet(t *testing.T) {
	f := &flagImpl{}
	f.init(stringMapper, "")
	go func() {
		<-time.After(300 * time.Millisecond)
		f.set("hello")
	}()

	expValue := "hello"
	value := f.get().(string)

	if value != expValue {
		t.Fatalf("expected value %q; got %q", expValue, value)
	}
}

func TestFlagChangeNotification(t *testing.T) {
	f := &flagImpl{}
	f.init(stringMapper, "")
	go func() {
		<-time.After(300 * time.Millisecond)
		f.set("hello")
	}()

	select {
	case <-f.ChangeChan():
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for change evnet")
	}

	// Calling set twice with no notification listeners should
	// drop the second notification event
	f.set("hello1")
	f.set("hello2")
}

func TestFlagDynamicChange(t *testing.T) {
	defer config.Store.Reset()
	expValue := "hello"
	config.Store.SetKey(1, "generic-flag-1", expValue)

	f := &flagImpl{}
	f.init(stringMapper, "generic-flag-1")

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

func TestFlagCancelDynamicUpdates(t *testing.T) {
	defer config.Store.Reset()
	config.Store.SetKey(1, "generic-flag-2", "hello")

	f := &flagImpl{}
	f.init(stringMapper, "generic-flag-2")
	f.CancelDynamicUpdates()

	// Calling cancel a second time should be a no-op
	f.CancelDynamicUpdates()

	expValue := "test"
	f.set(expValue)

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
	defer config.Store.Reset()
	config.Store.SetKey(1, "generic-flag-3", "hello")

	f := &flagImpl{}
	f.init(func(_ map[string]string) (interface{}, error) { return nil, errors.New("") }, "generic-flag-3")

	select {
	case <-f.ChangeChan():
		t.Fatal("unexpected change event")
	case <-time.After(1 * time.Second):
	}
}

func stringMapper(cfg map[string]string) (interface{}, error) {
	return firstMapElement(cfg), nil
}
