package flag

import (
	"errors"
	"strings"

	"github.com/achilleasa/usrv/config/store"
)

var (
	errNotBoolean = errors.New("not a boolean value")
)

// Bool provides a thread-safe flag wrapping a boolean value. Its value can be
// dynamically updated via a watched configuration key or manually set using its
// Set method.
//
// When processing dynamic updates, Bool will treat the values "true" (case-insensitive)
// and "1" as true and values "false" (also case-insensitive) and "0" as false.
//
// The flag also provides a mechanism for listening for changes.
type Bool struct {
	flagImpl
}

// NewBool creates a bool flag. If a non-empty config path is specified, the flag
// will register a watcher to the supplied configuration store instance and
// automatically update its value.
//
// Passing a nil store instance and a non-empty cfgPath will cause this function
// to panic.
//
// Dynamic updates can be disabled by invoking the CancelDynamicUpdates method.
func NewBool(store *store.Store, cfgPath string) *Bool {
	f := &Bool{}
	f.init(store, f.mapCfgValue, cfgPath)
	return f
}

// Get the stored flag value. If no initial value has been set for this flag,
// this method will block.
func (f *Bool) Get() bool {
	return f.get().(bool)
}

// Set the stored flag value. Calling Set will also trigger a change event to
// be emitted.
func (f *Bool) Set(val bool) {
	f.set(-1, val, false)
}

// mapCfgValue validates and converts a dynamic config value into the expected type for this flag.
func (f *Bool) mapCfgValue(cfg map[string]string) (interface{}, error) {
	switch strings.ToLower(firstMapElement(cfg)) {
	case "true", "1":
		return true, nil
	case "false", "0":
		return false, nil
	default:
		return nil, errNotBoolean
	}
}
