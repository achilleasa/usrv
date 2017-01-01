// Package flag provides typed thread-safe flags that can be dynamically updated
// by the registered usrv config providers.
package flag

import (
	"sync/atomic"

	"github.com/achilleasa/usrv/config"
)

type flagImpl struct {
	// The wrapped value.
	val atomic.Value

	// A value set to 1 when we receive the initial flag value. It is used
	// as a guard to ensure that we only close hasValueChan once.
	hasValue uint32

	// A channel that is closed when the initial value for the flag is set.
	hasValueChan chan struct{}

	// A channel for receiving notifications when the flag value changes.
	changedChan chan struct{}

	// A channel for signalling the watcher goroutine to shutdown.
	doneChan chan struct{}

	// A function for mapping incoming config events to a value that can be set.
	valueMapper cfgEventToValueMapper
}

type cfgEventToValueMapper func(map[string]string) (interface{}, error)

func (f *flagImpl) init(valueMapper cfgEventToValueMapper, cfgPath string) {
	f.valueMapper = valueMapper
	f.changedChan = make(chan struct{}, 1)
	f.hasValueChan = make(chan struct{}, 0)

	if cfgPath == "" {
		return
	}

	f.doneChan = make(chan struct{}, 0)

	go func() {
		cfgChan, unsubFn := config.Store.Watch(cfgPath)
		defer unsubFn()

		for {
			select {
			case cfg := <-cfgChan:
				val, err := f.valueMapper(cfg)
				if err != nil {
					continue
				}
				f.set(val)
			case <-f.doneChan:
				f.doneChan <- struct{}{}
				return
			}
		}
	}()
}

// Get the stored value. If the flag does not have a value yet this method will
// block until a call to set is made.
func (f *flagImpl) get() interface{} {
	// Block until the flag value is set
	<-f.hasValueChan

	return f.val.Load()
}

// Set the stored value.
func (f *flagImpl) set(val interface{}) {
	f.val.Store(val)

	// If we received the intial flag value we need to close hasValueChan
	// to unblock readers waiting on get(). We use a compare and swap operation
	// to only close the channel once and avoid panics
	if atomic.CompareAndSwapUint32(&f.hasValue, 0, 1) {
		close(f.hasValueChan)
	}

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
	if f.doneChan == nil {
		return
	}

	// Signal the watcher and wait for it to exit
	f.doneChan <- struct{}{}
	<-f.doneChan
	close(f.doneChan)
	f.doneChan = nil
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
