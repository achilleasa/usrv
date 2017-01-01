package flag

import "strconv"

// Uint32Flag provides a thread-safe flag wrapping an int32 value. Its value can be
// dynamically updated via a watched configuration key or manually set using its
// Set method.
//
// The flag also provides a mechanism for listening for changes.
type Uint32Flag struct {
	flagImpl
}

// NewUint32 creates a uint32 flag. If a non-empty config path is specified, the flag
// will register a watcher to the global configuration store and automatically
// update its value.
//
// Dynamic updates can be disabled by invoking the CancelDynamicUpdates method.
func NewUint32(cfgPath string) *Uint32Flag {
	f := &Uint32Flag{}
	f.init(f.mapCfgValue, cfgPath)
	return f
}

// Get the stored flag value. If no initial value has been set for this flag,
// this method will block.
func (f *Uint32Flag) Get() uint32 {
	return f.get().(uint32)
}

// Set the stored flag value. Calling Set will also trigger a change event to
// be emitted.
func (f *Uint32Flag) Set(val uint32) {
	f.set(val)
}

// mapCfgValue validates and converts a dynamic config value into the expected type for this flag.
func (f *Uint32Flag) mapCfgValue(cfg map[string]string) (interface{}, error) {
	v, err := strconv.ParseUint(firstMapElement(cfg), 10, 32)
	if err != nil {
		return nil, err
	}
	return uint32(v), nil
}

// Int32Flag provides a thread-safe flag wrapping an int32 value. Its value can be
// dynamically updated via a watched configuration key or manually set using its
// Set method.
//
// The flag also provides a mechanism for listening for changes.
type Int32Flag struct {
	flagImpl
}

// NewInt32 creates a int32 flag. If a non-empty config path is specified, the flag
// will register a watcher to the global configuration store and automatically
// update its value.
//
// Dynamic updates can be disabled by invoking the CancelDynamicUpdates method.
func NewInt32(cfgPath string) *Int32Flag {
	f := &Int32Flag{}
	f.init(f.mapCfgValue, cfgPath)
	return f
}

// Get the stored flag value. If no initial value has been set for this flag,
// this method will block.
func (f *Int32Flag) Get() int32 {
	return f.get().(int32)
}

// Set the stored flag value. Calling Set will also trigger a change event to
// be emitted.
func (f *Int32Flag) Set(val int32) {
	f.set(val)
}

// mapCfgValue validates and converts a dynamic config value into the expected type for this flag.
func (f *Int32Flag) mapCfgValue(cfg map[string]string) (interface{}, error) {
	v, err := strconv.ParseInt(firstMapElement(cfg), 10, 32)
	if err != nil {
		return nil, err
	}
	return int32(v), nil
}

// Int64Flag provides a thread-safe flag wrapping an int64 value. Its value can be
// dynamically updated via a watched configuration key or manually set using its
// Set method.
//
// The flag also provides a mechanism for listening for changes.
type Int64Flag struct {
	flagImpl
}

// NewInt64 creates a int64 flag. If a non-empty config path is specified, the flag
// will register a watcher to the global configuration store and automatically
// update its value.
//
// Dynamic updates can be disabled by invoking the CancelDynamicUpdates method.
func NewInt64(cfgPath string) *Int64Flag {
	f := &Int64Flag{}
	f.init(f.mapCfgValue, cfgPath)
	return f
}

// Get the stored flag value. If no initial value has been set for this flag,
// this method will block.
func (f *Int64Flag) Get() int64 {
	return f.get().(int64)
}

// Set the stored flag value. Calling Set will also trigger a change event to
// be emitted.
func (f *Int64Flag) Set(val int64) {
	f.set(val)
}

// mapCfgValue validates and converts a dynamic config value into the expected type for this flag.
func (f *Int64Flag) mapCfgValue(cfg map[string]string) (interface{}, error) {
	return strconv.ParseInt(firstMapElement(cfg), 10, 64)
}

// Uint64Flag provides a thread-safe flag wrapping an uint64 value. Its value can be
// dynamically updated via a watched configuration key or manually set using its
// Set method.
//
// The flag also provides a mechanism for listening for changes.
type Uint64Flag struct {
	flagImpl
}

// NewUint64 creates a uint64 flag. If a non-empty config path is specified, the flag
// will register a watcher to the global configuration store and automatically
// update its value.
//
// Dynamic updates can be disabled by invoking the CancelDynamicUpdates method.
func NewUint64(cfgPath string) *Uint64Flag {
	f := &Uint64Flag{}
	f.init(f.mapCfgValue, cfgPath)
	return f
}

// Get the stored flag value. If no initial value has been set for this flag,
// this method will block.
func (f *Uint64Flag) Get() uint64 {
	return f.get().(uint64)
}

// Set the stored flag value. Calling Set will also trigger a change event to
// be emitted.
func (f *Uint64Flag) Set(val uint64) {
	f.set(val)
}

// mapCfgValue validates and converts a dynamic config value into the expected type for this flag.
func (f *Uint64Flag) mapCfgValue(cfg map[string]string) (interface{}, error) {
	return strconv.ParseUint(firstMapElement(cfg), 10, 64)
}
