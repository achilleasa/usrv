package flag

import (
	"reflect"
	"testing"
	"time"

	"github.com/achilleasa/usrv/config"
)

func TestMapFlagDynamicUpdate(t *testing.T) {
	defer config.Store.Reset()

	expValue := map[string]string{
		"key1":      "value1",
		"key2/key3": "value2",
	}
	config.Store.SetKeys(1, "map-flag", expValue)

	f := NewMap("map-flag")
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
	f := NewMap("")
	expValue := map[string]string{
		"key1":      "value1",
		"key2/key3": "value2",
	}
	f.Set(expValue)

	val := f.Get()
	if !reflect.DeepEqual(val, expValue) {
		t.Fatalf("expected val to be:\n%v\n\ngot:\n%v", expValue, val)
	}
}
