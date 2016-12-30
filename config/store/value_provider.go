package store

// A ValueProvider is a pluggable repository of configuration values that can
// be attached to a configuration store.
//
// Get returns a map containing configuration values associated with a particular
// path. Similar to the store's Get method, if the path points to a tree of values,
// this method should populate the map keys using the full path to the tree leaves.
// If the value provider cannot provide values for the requested path it should
// return back a nil map.
//
// Watch instructs the provider to monitor a specific configuration path and
// call the supplied value setter when the monitored value changes. The call
// returns an UnsubscribeFunc that should be used to cancel the watcher.
type ValueProvider interface {
	Get(path string) map[string]string
	Watch(path string, updateFunc func(path string, values map[string]string)) (unsubscribeFunc func())
}
