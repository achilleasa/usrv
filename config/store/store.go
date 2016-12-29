// Package store implements a thread-safe versioned configuration store.
package store

import (
	"fmt"
	"regexp"
	"strings"
	"sync"
)

var (
	splitRegex = regexp.MustCompile(pathDelimiter + "+")

	// Internal hooks for testing notification fanout go-routines in our tests.
	beforeUpdateHookFn func()
	afterUpdateHookFn  func()
)

// UnsubscribeFunc cancels a change watcher associated with a configuration store.
// After the first call, subsequent calls to UnsubscribeFunc have no effect.
type UnsubscribeFunc func()

type changeWatcher struct {
	id int

	// A channel where configuration changes are published.
	changeChan chan map[string]string

	// A signal channel that is closed when the watcher is destroyed and before
	// changeChan is closed. As the actual event fanout is facilitated via a
	// goroutine this channel ensures that the goroutine will not attempt
	// to write to the closed changeChan.
	doneChan chan struct{}
}

// Store implements a versioned and thread-safe configuration store. A tree
// structure is used to store configuration values where non-leaf nodes are
// used to define a path through the tree and leaf nodes store the actual
// configuration value. Values are represented as strings.
//
// A configuration key (or path) consists of a sequence of segments separated
// by a "/" character. For example, a key like: "foo/bar/baz" with value "1"
// would be equivalent to the following tree structure (displayed as JSON):
//  {
//   "foo": {
//    "bar": {
//     "baz": "1"
//    }
//   }
//  }
//
// When getting or setting values, paths are always specified using the flattened
// form described above.
//
// Both the nodes and the leaves in this tree store a version number for their
// contents. The version is used when setting configuration values to decide
// whether a value should be overwritten or not. This allows you to overlay
// multiple configuration sets ensuring that the store always contains the
// latest values.
//
// For example, performing two set operations:
//
//  version: 1
//  {
//   "key1": "1",
//   "key2/key3": "3"
//  }
//
// followed by:
//  version: 2
//  {
//   "key1/nested": "value",
//   "key2/key4": "4"
//  }
//
// will result in the store containing the following tree (displayed as JSON):
//  {
//   "key1": {
//    "nested: "value"
//   },
//   "key2": {
//     "key3": "3",
//     "key4": "4"
//   }
//  }
type Store struct {
	mutex         sync.Mutex
	root          *node
	nextWatcherID int
	watchers      map[string][]changeWatcher
}

// Reset deletes the store's contents and removes any associated change watchers.
func (s *Store) Reset() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.root = nil
	s.watchers = nil
}

// Get retrieves the configuration sub-tree rooted at the given path and returns
// a map containing the configuration values stored at its leaves. The map keys
// are the full paths to the leaves relative to the node rooted at the given path.
// If the given path does not exist, Get will return an empty map.
//
// The given path can optionally begin and/or end with the path delimiter "/".
// This method treats paths "/foo", "foo/" and "/foo/" as equal.
//
// For example, given the following internal configuration tree representation:
//  {
//   "key1": {
//     "key2": "2",
//     "key3": {
//      "key4": "4"
//     }
//   }
//  }
//
// Get("") or Get("/") will return:
//  {
//   "key1/key2": "2",
//   "key1/key3/key4": "4"
//  }
//
// Get("key1") or Get("/key1/") will return:
//  {
//   "key2": "2",
//   "key3/key4": "4"
//  }
//
// Get("key1/key3/key4") will return:
//  {
//   "key4": "4"
//  }
func (s *Store) Get(path string) map[string]string {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.get(path)
}

// SetKey sets the node at the given path to the given value if version is
// greater than or equal to the version already present in the store.
//
// The given path can optionally begin and/or end with the path delimiter "/".
// This method treats paths "/foo", "foo/" and "/foo/" as equal.
//
// If path points to an existing non-leaf node with a smaller version then
// all of its children will be deleted and the node will be converted into a leaf.
//
// SetKey returns a boolean flag to indicate whether the store was updated. If
// a version mismatch occured then SetKey will return false to indicate that no.
// update took place.
func (s *Store) SetKey(version int, path, value string) (storeUpdated bool, err error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.set(version, path, value), nil
}

// SetKeys applies a set of values whose keys are defined relative to the
// specified path.  The given path can optionally begin and/or end with the path
// delimiter "/". This method treats paths "/foo", "foo/" and "/foo/" as equal.
//
// SetKeys will iterate each provided key/value tuple and attempt to apply it
// at the absolute path location constructed by concatenating path and key
// using the same vesion comparison logic as SetKey.
//
// The following two examples are functionally equivalent and set exactly the
// same values:
//  SetKeys(
//   1,
//   "/key1",
//   map[string]string{
//     "key2": "2",
//     "key3": "3"
//   },
//  )
//
//  SetKeys(
//   1,
//   "/",
//   map[string]string{
//     "key1/key2": "2",
//     "key1/key3": "3"
//   },
//  )
//
// The keys in the provided value map must adhere to the following constraints
// or an error will be returned:
//  - keys cannot be empty
//  - keys cannot point to a leaf and non-leaf path at the same time. For example,
//    the following value map is invalid as the updated store value would depend
//    on the order that the map keys were iterated: {"/key1": "1", "/key/key2": "3"}.
//
// The method returns a boolean flag to indicate whether the store was updated
// by any of the supplied values.
func (s *Store) SetKeys(version int, path string, values map[string]string) (storeUpdated bool, err error) {
	if values == nil || len(values) == 0 {
		return false, nil
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	// convert values from flat key format to a nested tree format:
	// from:
	// {"segment1/segment2.../segment_n": value}
	//
	// to:
	// {"segment1": {
	//   "segment2": { ...
	//     {"segment_n": value}
	//   }
	//}
	root := make(map[string]interface{}, 0)
	var segmentMap *map[string]interface{}
	for path, v := range values {
		if len(path) == 0 {
			return false, fmt.Errorf("supplied value map contains empty path key")
		}
		segments := splitRegex.Split(path, -1)
		lastSegment := len(segments) - 1

		segmentMap = &root
		for index, segment := range segments {
			if len(segment) == 0 {
				return false, fmt.Errorf("supplied value map contains empty segment for path %q", path)
			}

			switch {
			case index == lastSegment:
				(*segmentMap)[segment] = v
			case (*segmentMap)[segment] != nil:
				// Subpath already created by another path; we need to ensure
				// that the it points to a map and not a string
				subPath := (*segmentMap)[segment]
				subPathMap, isMap := subPath.(map[string]interface{})
				if !isMap {
					return false, fmt.Errorf("supplied value map contains both a value and a sub-path for %q", strings.Join(segments[:index+1], pathDelimiter))
				}
				segmentMap = &subPathMap
			default:
				subPathMap := make(map[string]interface{})
				(*segmentMap)[segment] = subPathMap
				segmentMap = &subPathMap
			}
		}
	}

	return s.set(version, path, root), nil
}

// Watch registers a new change watcher that gets notified whenever the configuration
// tree rooted at path is modified. This method returns back a read-only channel
// for receiving the updated configuration (equivalent to invoking Get(path)) as
// well as a function for deleting the watcher.
//
// Before returning, Watch will query the store for the current configuration
// and push that into the returned notification channel. The notification channel
// itself is buffered ensuring that calls to Watch do not block.
//
// It is possible to attach a watcher to a path that does not yet exist in the
// store. In that case, the returned notification channel will receive an empty
// map as the current configuration for that path.
//
// Each registered watcher will receive a fresh copy of the updated store data.
// This is by design; passing the same map by value would create different
// map instances that reused the same storage. Creating map copies ensures that
// watchers cannot mutate the values that other watchers receive.
func (s *Store) Watch(path string) (<-chan map[string]string, UnsubscribeFunc) {
	// Normalize path
	path = pathDelimiter + strings.Trim(path, pathDelimiter)

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.watchers == nil {
		s.watchers = make(map[string][]changeWatcher, 0)
	}

	if s.watchers[path] == nil {
		s.watchers[path] = make([]changeWatcher, 0)
	}

	s.nextWatcherID++
	watcher := changeWatcher{
		id:         s.nextWatcherID,
		changeChan: make(chan map[string]string, 1),
		doneChan:   make(chan struct{}, 0),
	}

	// Lookup current value at path and push it to the watcher's buffered event chan
	watcher.changeChan <- s.get(path)
	s.watchers[path] = append(s.watchers[path], watcher)

	return watcher.changeChan, s.unwatch(path, watcher.id)
}

// Unwatch generates a function that deletes a watcher by its assigned ID.
func (s *Store) unwatch(path string, watcherID int) UnsubscribeFunc {
	return func() {
		s.mutex.Lock()
		defer s.mutex.Unlock()

		// No watchers defined
		if s.watchers == nil || s.watchers[path] == nil {
			return
		}

		// Match watcher by ID
		for index, watcher := range s.watchers[path] {
			if watcher.id != watcherID {
				continue
			}

			// Found watcher. Close doneChan to unblock any go-routines
			// that are trying to write to its event chan and clean up
			close(watcher.doneChan)
			close(watcher.changeChan)
			s.watchers[path] = append(s.watchers[path][0:index], s.watchers[path][index+1:]...)
			return
		}
	}
}

// NotifyWatchers implements a node change callback passed that is responsible
// for notifying the appropriate watchers when a node's value changes. The
// actual notification delivery is facilitated using a separate go-routine
// per watcher.
func (s *Store) notifyWatchers(n *node) {
	if s.watchers == nil {
		return
	}

	path := n.path()
	if s.watchers[path] == nil {
		return
	}

	// Pass a copy of the leaf values to each watcher. This ensures that no
	// watcher can modify the values that others watchers receive.
	valueMap := n.leafValues("", true)
	valueMapCopy := valueMap
	for index, watcher := range s.watchers[path] {
		// Use the original value for the first watcher and a deep-copy for each other watcher
		if index != 0 {
			valueMapCopy = make(map[string]string, len(valueMap))
			for k, v := range valueMap {
				valueMapCopy[k] = v
			}
		}

		// Spawn go-routines to fanout notifications to registered watchers
		go func(watcher changeWatcher, valueMap map[string]string) {
			if afterUpdateHookFn != nil {
				defer afterUpdateHookFn()
			}
			if beforeUpdateHookFn != nil {
				beforeUpdateHookFn()
			}

			select {
			case <-watcher.doneChan:
				// event chan is closed; bail out
			case watcher.changeChan <- valueMap:
				// queued update
			default:
				// event chan is full; drop update
			}
		}(watcher, valueMapCopy)
	}
}

// Get looks up the sub-tree rooted at path and returnes the leaf values
// as a map. This function must only be called after locking the store's mutex.
func (s *Store) get(path string) map[string]string {
	root := s.lookup(path, false)
	if root == nil {
		return make(map[string]string, 0)
	}

	return root.leafValues("", true)
}

// Set attempts to merge the given string or map value against the sub-tree
// rooted at path. This function must only be called after locking the store's
// mutex.
func (s *Store) set(version int, path string, value interface{}) bool {
	root := s.lookup(path, true)
	return root.merge(version, value, s.notifyWatchers)
}

// Lookup searches the configuration tree for a particular path and returns
// the node that corresponds to the last path segment.
//
// A valid path consists of any number of segments delimited by pathDelimiter.
// For example: "/foo/bar/baz". The initial delimiter may be omitted;
// "foo/bar" is equivalent to "/foo/bar".
//
// While searching the tree, Lookup will automatically create nodes for any
// missing path segments if createMissingNodes is true. If createMissing nodes
// is set to false and the path does not already exist in the tree, lookup
// will return nil.
func (s *Store) lookup(path string, createMissingNodes bool) *node {
	path = strings.Trim(path, pathDelimiter)

	if s.root == nil {
		s.root = makeNode("", 0, nil)
	}

	if path == "" {
		return s.root
	}

	segments := strings.Split(path, pathDelimiter)
	curNode := s.root
nextSegment:
	for ; len(segments) != 0; segments = segments[1:] {
		for _, subPath := range curNode.paths {
			if subPath.segment == segments[0] {
				curNode = subPath
				continue nextSegment
			}
		}

		// No subpath found
		if !createMissingNodes {
			return nil
		}
		curNode = makeNode(segments[0], curNode.depth+1, curNode)
	}

	return curNode
}
