package flag

import (
	"reflect"
	"testing"
	"time"

	"github.com/achilleasa/usrv/config/store"
)

func TestMapFlagDynamicUpdate(t *testing.T) {
	var s store.Store

	expValue := map[string]string{
		"foo":       "bar",
		"key1":      "value1",
		"key2/key3": "value2",
	}
	s.SetKeys(1, "map-flag", map[string]string{"foo": "bar"})

	f := NewMap(&s, "map-flag")
	go func() {
		<-time.After(100 * time.Millisecond)
		s.SetKeys(1, "map-flag", expValue)
	}()
	select {
	case <-f.ChangeChan():
	case <-time.After(1000 * time.Millisecond):
		t.Fatal("timeout waiting for flag change event")
	}

	val := f.Get()
	if !reflect.DeepEqual(val, expValue) {
		t.Fatalf("expected val to be:\n%v\n\ngot:\n%v", expValue, val)
	}
}

func TestMapFlagManualSet(t *testing.T) {
	f := NewMap(nil, "")
	expValue := map[string]string{
		"key1":      "value1",
		"key2/key3": "value2",
	}
	f.Set(expValue)

	// Second attempt to set should bail out as the value has not changed
	f.Set(expValue)

	val := f.Get()
	if !reflect.DeepEqual(val, expValue) {
		t.Fatalf("expected val to be:\n%v\n\ngot:\n%v", expValue, val)
	}
}
