package flag

import "strconv"

// Float32Flag provides a thread-safe flag wrapping an int32 value. Its value can be
// dynamically updated via a watched configuration key or manually set using its
// Set method.
//
// The flag also provides a mechanism for listening for changes.
type Float32Flag struct {
	flagImpl
}

// NewFloat32 creates a float32 flag. If a non-empty config path is specified, the flag
// will register a watcher to the global configuration store and automatically
// update its value.
//
// Dynamic updates can be disabled by invoking the CancelDynamicUpdates method.
func NewFloat32(cfgPath string) *Float32Flag {
	f := &Float32Flag{}
	f.init(f.mapCfgValue, cfgPath)
	return f
}

// Get the stored flag value. If no initial value has been set for this flag,
// this method will block.
func (f *Float32Flag) Get() float32 {
	return f.get().(float32)
}

// Set the stored flag value. Calling Set will also trigger a change event to
// be emitted.
func (f *Float32Flag) Set(val float32) {
	f.set(val)
}

// mapCfgValue validates and converts a dynamic config value into the expected type for this flag.
func (f *Float32Flag) mapCfgValue(cfg map[string]string) (interface{}, error) {
	v, err := strconv.ParseFloat(firstMapElement(cfg), 32)
	if err != nil {
		return nil, err
	}
	return float32(v), nil
}

// Float64Flag provides a thread-safe flag wrapping an float64 value. Its value can be
// dynamically updated via a watched configuration key or manually set using its
// Set method.
//
// The flag also provides a mechanism for listening for changes.
type Float64Flag struct {
	flagImpl
}

// NewFloat64 creates a Float64 flag. If a non-empty config path is specified, the flag
// will register a watcher to the global configuration store and automatically
// update its value.
//
// Dynamic updates can be disabled by invoking the CancelDynamicUpdates method.
func NewFloat64(cfgPath string) *Float64Flag {
	f := &Float64Flag{}
	f.init(f.mapCfgValue, cfgPath)
	return f
}

// Get the stored flag value. If no initial value has been set for this flag,
// this method will block.
func (f *Float64Flag) Get() float64 {
	return f.get().(float64)
}

// Set the stored flag value. Calling Set will also trigger a change event to
// be emitted.
func (f *Float64Flag) Set(val float64) {
	f.set(val)
}

// mapCfgValue validates and converts a dynamic config value into the expected type for this flag.
func (f *Float64Flag) mapCfgValue(cfg map[string]string) (interface{}, error) {
	return strconv.ParseFloat(firstMapElement(cfg), 64)
}
