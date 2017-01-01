package flag

// MapFlag provides a thread-safe flag wrapping a map[string]string value. Its
// value can be dynamically updated via a watched configuration key or manually
// set using its Set method.
//
// The flag also provides a mechanism for listening for changes.
type MapFlag struct {
	flagImpl
}

// NewMap creates a map flag. If a non-empty config path is specified, the flag
// will register a watcher to the global configuration store and automatically
// update its value.
//
// Dynamic updates can be disabled by invoking the CancelDynamicUpdates method.
func NewMap(cfgPath string) *MapFlag {
	f := &MapFlag{}
	f.init(f.mapCfgValue, cfgPath)
	return f
}

// Get the stored flag value. If no initial value has been set for this flag,
// this method will block.
func (f *MapFlag) Get() map[string]string {
	return f.get().(map[string]string)
}

// Set the stored flag value. Calling Set will also trigger a change event to
// be emitted.
func (f *MapFlag) Set(val map[string]string) {
	f.set(val)
}

// mapCfgValue validates and converts a dynamic config value into the expected type for this flag.
func (f *MapFlag) mapCfgValue(cfg map[string]string) (interface{}, error) {
	return cfg, nil
}
