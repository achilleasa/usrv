// Package flag provides typed thread-safe flags that can be dynamically updated
// by the registered usrv config providers.
package flag

import (
	"sync"

	"github.com/achilleasa/usrv/config/store"
)

type flagImpl struct {
	// A pointer to the store used by this value
	store *store.Store

	// A mutex guarding access to the data
	rwMutex sync.RWMutex

	// The wrapped value.
	value interface{}

	// The current version of the flag's value. Initially, it is set to -1
	// to indicate that the flag contains no value. Once  a value is received,
	// hasValueChan gets closed to unblock any readers waiting on get()
	version int

	// A channel that is closed when the initial value for the flag is set.
	hasValueChan chan struct{}

	// A channel for receiving notifications when the flag value changes.
	changedChan chan struct{}

	// A function for cancelling the watcher for this value.
	unsubscribeFn store.UnsubscribeFunc

	// A function for mapping incoming config events to a value that can be set.
	valueMapper cfgEventToValueMapper

	// A function for comparing values for equality.
	checkEquality func(_, _ interface{}) bool
}

type cfgEventToValueMapper func(map[string]string) (interface{}, error)

func (f *flagImpl) init(store *store.Store, valueMapper cfgEventToValueMapper, cfgPath string) {
	f.store = store
	f.valueMapper = valueMapper
	f.changedChan = make(chan struct{}, 0)
	f.hasValueChan = make(chan struct{}, 0)
	f.version = -1
	if f.checkEquality == nil {
		f.checkEquality = func(v1, v2 interface{}) bool {
			return v1 == v2
		}
	}

	if cfgPath == "" {
		return
	}

	if store == nil {
		panic("store cannot be nil")
	}

	// Fetch and set initial value if present in store
	f.storeValueChanged(f.store.Get(cfgPath))

	// Register watch
	f.unsubscribeFn = f.store.Watch(cfgPath, f.storeValueChanged)
}

// StoreValueChanged implements a watcher change handler for the flag value.
func (f *flagImpl) storeValueChanged(values map[string]string, version int) {
	// No value in store
	if version == -1 {
		return
	}

	mappedVal, err := f.valueMapper(values)
	if err != nil {
		return
	}

	f.set(version, mappedVal, true)
}

// Get the stored value. If the flag does not have a value yet this method will
// block until a call to set is made.
func (f *flagImpl) get() interface{} {
	// Block until the flag value is set
	<-f.hasValueChan

	f.rwMutex.RLock()
	val := f.value
	f.rwMutex.RUnlock()

	return val
}

// Set the stored value and notify changeChan.
//
// If the compareVersions flag is set and version is less than the currently
// stored version then this is a no-op. Otherwise, the value is
// overwritten and changeChan is notified.
func (f *flagImpl) set(version int, val interface{}, compareVersions bool) {
	f.rwMutex.Lock()
	defer f.rwMutex.Unlock()

	if compareVersions && (version == -1 || version < f.version) {
		return
	}

	// Unblock any readers waiting on Get()
	if f.version == -1 {
		close(f.hasValueChan)
	}

	// If compareVersions is not true we need to cap version to 0
	if version < 0 {
		version = 0
	}
	f.version = version

	// We only changed the version but not the value
	if f.checkEquality(f.value, val) {
		return
	}

	f.value = val

	// Notify anyone interested in change events
	select {
	case f.changedChan <- struct{}{}:
	default:
	}
}

// ChangeChan returns a channel where clients can listen for flag value change events.
func (f *flagImpl) ChangeChan() <-chan struct{} {
	return f.changedChan
}

// CancelDynamicUpdates disables dynamic flag updates from the configuration system.
func (f *flagImpl) CancelDynamicUpdates() {
	if f.unsubscribeFn != nil {
		f.unsubscribeFn()
	}
}

// HasValue returns true if the flag's value is set either by a call to Set
// or dynamically through a configuration store value.
func (f *flagImpl) HasValue() bool {
	f.rwMutex.RLock()
	defer f.rwMutex.RUnlock()

	return f.version != -1
}

// firstMapElement returns the first element in a map. Due to the way that map
// iterators work this method will only return consistent results if the map
// contains a single entry.
func firstMapElement(m map[string]string) string {
	for _, v := range m {
		return v
	}
	return ""
}
