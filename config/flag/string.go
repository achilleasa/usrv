package flag

// StringFlag provides a thread-safe flag wrapping an string value. Its value can be
// dynamically updated via a watched configuration key or manually set using its
// Set method.
//
// The flag also provides a mechanism for listening for changes.
type StringFlag struct {
	flagImpl
}

// NewString creates a string flag. If a non-empty config path is specified, the flag
// will register a watcher to the global configuration store and automatically
// update its value.
//
// Dynamic updates can be disabled by invoking the CancelDynamicUpdates method.
func NewString(cfgPath string) *StringFlag {
	f := &StringFlag{}
	f.init(f.mapCfgValue, cfgPath)
	return f
}

// Get the stored flag value. If no initial value has been set for this flag,
// this method will block.
func (f *StringFlag) Get() string {
	return f.get().(string)
}

// Set the stored flag value. Calling Set will also trigger a change event to
// be emitted.
func (f *StringFlag) Set(val string) {
	f.set(val)
}

// mapCfgValue validates and converts a dynamic config value into the expected type for this flag.
func (f *StringFlag) mapCfgValue(cfg map[string]string) (interface{}, error) {
	return firstMapElement(cfg), nil
}
