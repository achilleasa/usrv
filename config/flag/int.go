package flag

import (
	"strconv"

	"github.com/achilleasa/usrv/config/store"
)

// Uint32 provides a thread-safe flag wrapping an int32 value. Its value can be
// dynamically updated via a watched configuration key or manually set using its
// Set method.
//
// The flag also provides a mechanism for listening for changes.
type Uint32 struct {
	flagImpl
}

// NewUint32 creates a uint32 flag. If a non-empty config path is specified, the flag
// will register a watcher to the supplied configuration store instance and
// automatically update its value.
//
// Passing a nil store instance and a non-empty cfgPath will cause this function
// to panic.
//
// Dynamic updates can be disabled by invoking the CancelDynamicUpdates method.
func NewUint32(store *store.Store, cfgPath string) *Uint32 {
	f := &Uint32{}
	f.init(store, f.mapCfgValue, cfgPath)
	return f
}

// Get the stored flag value. If no initial value has been set for this flag,
// this method will block.
func (f *Uint32) Get() uint32 {
	return f.get().(uint32)
}

// Set the stored flag value. Calling Set will also trigger a change event to
// be emitted.
func (f *Uint32) Set(val uint32) {
	f.set(-1, val, false)
}

// mapCfgValue validates and converts a dynamic config value into the expected type for this flag.
func (f *Uint32) mapCfgValue(cfg map[string]string) (interface{}, error) {
	v, err := strconv.ParseUint(firstMapElement(cfg), 10, 32)
	if err != nil {
		return nil, err
	}
	return uint32(v), nil
}

// Int32 provides a thread-safe flag wrapping an int32 value. Its value can be
// dynamically updated via a watched configuration key or manually set using its
// Set method.
//
// The flag also provides a mechanism for listening for changes.
type Int32 struct {
	flagImpl
}

// NewInt32 creates a int32 flag. If a non-empty config path is specified, the flag
// will register a watcher to the supplied configuration store instance and
// automatically update its value.
//
// Passing a nil store instance and a non-empty cfgPath will cause this function
// to panic.
//
// Dynamic updates can be disabled by invoking the CancelDynamicUpdates method.
func NewInt32(store *store.Store, cfgPath string) *Int32 {
	f := &Int32{}
	f.init(store, f.mapCfgValue, cfgPath)
	return f
}

// Get the stored flag value. If no initial value has been set for this flag,
// this method will block.
func (f *Int32) Get() int32 {
	return f.get().(int32)
}

// Set the stored flag value. Calling Set will also trigger a change event to
// be emitted.
func (f *Int32) Set(val int32) {
	f.set(-1, val, false)
}

// mapCfgValue validates and converts a dynamic config value into the expected type for this flag.
func (f *Int32) mapCfgValue(cfg map[string]string) (interface{}, error) {
	v, err := strconv.ParseInt(firstMapElement(cfg), 10, 32)
	if err != nil {
		return nil, err
	}
	return int32(v), nil
}

// Int64 provides a thread-safe flag wrapping an int64 value. Its value can be
// dynamically updated via a watched configuration key or manually set using its
// Set method.
//
// The flag also provides a mechanism for listening for changes.
type Int64 struct {
	flagImpl
}

// NewInt64 creates a int64 flag. If a non-empty config path is specified, the flag
// will register a watcher to the supplied configuration store instance and
// automatically update its value.
//
// Passing a nil store instance and a non-empty cfgPath will cause this function
// to panic.
//
// Dynamic updates can be disabled by invoking the CancelDynamicUpdates method.
func NewInt64(store *store.Store, cfgPath string) *Int64 {
	f := &Int64{}
	f.init(store, f.mapCfgValue, cfgPath)
	return f
}

// Get the stored flag value. If no initial value has been set for this flag,
// this method will block.
func (f *Int64) Get() int64 {
	return f.get().(int64)
}

// Set the stored flag value. Calling Set will also trigger a change event to
// be emitted.
func (f *Int64) Set(val int64) {
	f.set(-1, val, false)
}

// mapCfgValue validates and converts a dynamic config value into the expected type for this flag.
func (f *Int64) mapCfgValue(cfg map[string]string) (interface{}, error) {
	return strconv.ParseInt(firstMapElement(cfg), 10, 64)
}

// Uint64 provides a thread-safe flag wrapping an uint64 value. Its value can be
// dynamically updated via a watched configuration key or manually set using its
// Set method.
//
// The flag also provides a mechanism for listening for changes.
type Uint64 struct {
	flagImpl
}

// NewUint64 creates a uint64 flag. If a non-empty config path is specified, the flag
// will register a watcher to the supplied configuration store instance and
// automatically update its value.
//
// Passing a nil store instance and a non-empty cfgPath will cause this function
// to panic.
//
// Dynamic updates can be disabled by invoking the CancelDynamicUpdates method.
func NewUint64(store *store.Store, cfgPath string) *Uint64 {
	f := &Uint64{}
	f.init(store, f.mapCfgValue, cfgPath)
	return f
}

// Get the stored flag value. If no initial value has been set for this flag,
// this method will block.
func (f *Uint64) Get() uint64 {
	return f.get().(uint64)
}

// Set the stored flag value. Calling Set will also trigger a change event to
// be emitted.
func (f *Uint64) Set(val uint64) {
	f.set(-1, val, false)
}

// mapCfgValue validates and converts a dynamic config value into the expected type for this flag.
func (f *Uint64) mapCfgValue(cfg map[string]string) (interface{}, error) {
	return strconv.ParseUint(firstMapElement(cfg), 10, 64)
}
