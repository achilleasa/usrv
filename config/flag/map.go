package flag

import (
	"reflect"

	"github.com/achilleasa/usrv/config/store"
)

// Map provides a thread-safe flag wrapping a map[string]string value. Its
// value can be dynamically updated via a watched configuration key or manually
// set using its Set method.
//
// The flag also provides a mechanism for listening for changes.
type Map struct {
	flagImpl
}

// NewMap creates a map flag. If a non-empty config path is specified, the flag
// will register a watcher to the supplied configuration store instance and
// automatically update its value.
//
// Passing a nil store instance and a non-empty cfgPath will cause this function
// to panic.
//
// Dynamic updates can be disabled by invoking the CancelDynamicUpdates method.
func NewMap(store *store.Store, cfgPath string) *Map {
	f := &Map{}
	f.flagImpl.checkEquality = equalMaps
	f.init(store, f.mapCfgValue, cfgPath)
	return f
}

// Get the stored flag value. If no initial value has been set for this flag,
// this method will block.
func (f *Map) Get() map[string]string {
	return f.get().(map[string]string)
}

// Set the stored flag value. Calling Set will also trigger a change event to
// be emitted.
func (f *Map) Set(val map[string]string) {
	f.set(-1, val, false)
}

func equalMaps(v1, v2 interface{}) bool {
	return reflect.DeepEqual(v1, v2)
}

// mapCfgValue validates and converts a dynamic config value into the expected type for this flag.
func (f *Map) mapCfgValue(cfg map[string]string) (interface{}, error) {
	return cfg, nil
}
