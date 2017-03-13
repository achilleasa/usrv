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
)

// UnsubscribeFunc cancels a change watcher associated with a configuration store.
// After the first call, subsequent calls to UnsubscribeFunc have no effect.
type UnsubscribeFunc func()

// ChangeHandlerFunc is invoked by the store to handle changes to a watched path.
// It receives as arguments the updated data for the path and its current version.
//
// The handler can safely invoke all store methods but should never block; if
// it blocks then a deadlock will occur.
type ChangeHandlerFunc func(values map[string]string, version int)

type changeWatcher struct {
	id int

	// A callback for processing changes.
	changeHandler ChangeHandlerFunc

	// A list of unsubscribe functions for terminating watches set on value providers.
	providerUnsubscribeFn []func()
}

type pendingNotification struct {
	// The version of the data
	version int

	// An immutable copy of the tree values to be passed to the handlers.
	values map[string]string

	// A list of handlers to notify.
	changeHandlers []ChangeHandlerFunc
}

type valueProvider struct {
	instance ValueProvider

	// A value setter that links the provider instance with a particular
	// store and config value version.
	valueSetFunc func(string, map[string]string)
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
//
// The store access semantics are multiple readers / single writer; calls to
// Get() are processed concurrently while all other store operations use a
// write lock to ensure data integrity.
//
// Applications that are interested in changes to a particular store path can
// register a watcher for that path. Watchers are guaranteed to be always executed
// in DFS order based on their path; however no guarantee is provided for the
// order in which watchers on the same path segment are executed. Given the
// following set of watchers:
//
//  - "/foo" -> watcher1
//  - "/foo/bar" -> [watcher2, watcher3]
//
// when the path "/foo/bar" changes, the store will invoke watcher2 and watcher3
// first (in random order) and only after they both return will watcher1 be invoked.
type Store struct {
	rwMutex       sync.RWMutex
	root          *node
	nextWatcherID int
	watchers      map[string][]*changeWatcher
	providers     []*valueProvider
}

// Reset deletes the store's contents and removes any associated change watchers.
func (s *Store) Reset() {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()

	if s.watchers != nil {
		for _, pathWatchers := range s.watchers {
			for _, watcher := range pathWatchers {
				for _, unsubFn := range watcher.providerUnsubscribeFn {
					unsubFn()
				}
			}
		}
	}
	s.watchers = nil
	s.root = nil
}

// RegisterValueProvider connects a configuration value provider instance with
// this store. The order of provider registration is important as defines the
// priority for the configuration values emitted by each provider. Providers
// should be registered in low to high priority order.
func (s *Store) RegisterValueProvider(provider ValueProvider) {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()

	if s.providers == nil {
		s.providers = make([]*valueProvider, 0)
	}

	priority := len(s.providers) + 1
	s.providers = append(s.providers, &valueProvider{
		instance: provider,
		valueSetFunc: func(path string, values map[string]string) {
			// Use a go-routine to set the keys to avoid a deadlock if
			// the provider decides to call the value setter while inside
			// the Watch() method.
			go s.SetKeys(priority, path, values)
		},
	})
}

// Get retrieves the configuration sub-tree rooted at the given path and returns
// a map containing the configuration values stored at its leaves as well as the
// version currently stored in that particular path. The map keys are the full paths
// to the leaves relative to the node rooted at the given path. If the given path does
// not exist, Get() will return an empty map and -1 for the version.
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
func (s *Store) Get(path string) (map[string]string, int) {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()

	root := s.lookup(path, false)
	if root == nil {
		return make(map[string]string, 0), -1
	}

	return root.leafValues("", true), root.version
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
// a version mismatch occurred then SetKey will return false to indicate that no.
// update took place.
func (s *Store) SetKey(version int, path, value string) (storeUpdated bool, err error) {
	return s.setAndNotify(version, path, value), nil
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

	valueTree, err := flattenedMapToTree(values)
	if err != nil {
		return false, err
	}

	return s.setAndNotify(version, path, valueTree), err
}

// Watch registers a new change watcher that gets notified whenever the configuration
// tree rooted at path is modified. The method returns an UnsubscribeFunc that
// must be invoked to cancel the watcher.
//
// It is possible to attach a watcher to a path that does not yet exist in the
// store. In that case, the returned notification channel will receive an empty
// map as the current configuration for that path.
//
// Each registered watcher will receive a fresh copy of the updated store data.
// This is by design; passing the same map by value would create different
// map instances that reused the same storage. Creating map copies ensures that
// watchers cannot mutate the values that other watchers receive.
func (s *Store) Watch(path string, changeHandler ChangeHandlerFunc) UnsubscribeFunc {
	// Normalize path
	path = pathDelimiter + strings.Trim(path, pathDelimiter)

	s.rwMutex.Lock()

	s.nextWatcherID++
	numProviders := 0

	if s.providers != nil {
		numProviders = len(s.providers)
	}
	if s.watchers == nil {
		s.watchers = make(map[string][]*changeWatcher, 0)
	}

	if s.watchers[path] == nil {
		s.watchers[path] = make([]*changeWatcher, 0)
	}

	watcher := &changeWatcher{
		id:                    s.nextWatcherID,
		changeHandler:         changeHandler,
		providerUnsubscribeFn: make([]func(), numProviders),
	}
	s.watchers[path] = append(s.watchers[path], watcher)

	// For each registered value provider fetch its value for the path and
	// apply it to the store; then register a watch for the same path. Provider
	// values are applied in high to low priority order. This ensures the
	// minimum number of nodes flagged as modified.
	treeModified := false
	modFlag := false
	var pathRoot *node
	if numProviders > 0 {
		for index := len(s.providers) - 1; index >= 0; index-- {
			provider := s.providers[index]
			values := provider.instance.Get(path)
			if values != nil && len(values) != 0 {
				valueTree, err := flattenedMapToTree(values)
				if err == nil {
					modFlag, pathRoot = s.set(index, path, valueTree)
					treeModified = treeModified || modFlag
				}
			}
			watcher.providerUnsubscribeFn[index] = provider.instance.Watch(path, provider.valueSetFunc)
		}
	}

	// If the tree was modified by the provider values, collect the handlers to be notified
	var pendingNotifications []*pendingNotification
	if treeModified {
		pendingNotifications = s.pendingNotifications(pathRoot)
	}

	// Generate unsubscribe func
	unsubFn := s.unwatch(path, watcher.id)

	// Release write lock
	s.rwMutex.Unlock()

	// Since our pending notifications include an immutable copy of our
	// data and the affected change handlers, we can fanout the notifications
	// without requiring any lock.
	if pendingNotifications != nil {
		go s.notifyWatchers(pendingNotifications)
	}

	return unsubFn
}

// Unwatch generates a function that deletes a watcher by its assigned ID. The
// generated function uses sync.Once to ensure that the unsubscriber can only
// be invoked one time and that further invocations are a no-op.
func (s *Store) unwatch(path string, watcherID int) UnsubscribeFunc {
	// Ensure that the generated unsubscribe func can only be invoked only once
	var once sync.Once
	return func() {
		once.Do(func() {
			s.rwMutex.Lock()
			defer s.rwMutex.Unlock()

			// No watchers defined
			if s.watchers == nil || s.watchers[path] == nil {
				return
			}

			// Match watcher by ID
			for index, watcher := range s.watchers[path] {
				if watcher.id != watcherID {
					continue
				}

				// Unsubscribe provider watchers
				for _, unsubFn := range watcher.providerUnsubscribeFn {
					unsubFn()
				}

				s.watchers[path] = append(s.watchers[path][0:index], s.watchers[path][index+1:]...)
				return
			}
		})
	}
}

// NotifyWatchers iterates a list of pending notifications and invokes the
// associated change handlers. For each pending notification, this method
// will spawn a go-routine for each one of its handlers and wait for them
// to return before processing the next pending notification entry.
func (s *Store) notifyWatchers(list []*pendingNotification) {
	var wg sync.WaitGroup

	for _, notification := range list {
		wg.Add(len(notification.changeHandlers))

		valueMapCopy := notification.values
		for index, watcher := range notification.changeHandlers {
			// Use the original value for the first watcher and a deep-copy for each other watcher
			if index != 0 {
				valueMapCopy = make(map[string]string, len(notification.values))
				for k, v := range notification.values {
					valueMapCopy[k] = v
				}
			}

			go func(version int, val map[string]string, w ChangeHandlerFunc) {
				w(val, version)
				wg.Done()
			}(notification.version, valueMapCopy, watcher)
		}

		// Wait for all watchers to return
		wg.Wait()
	}
}

// SetAndNotify attempts to merge the given value against the sub-tree rooted
// at path and invoke any registered change watchers.
//
// The method uses a write lock to apply the values and collect the list of
// change watchers to notify as well as an immutable copy of the tree data that
// needs to be passed to each watcher. The watchers are invoked using a
// go-routine and do not require any locks to be held.
func (s *Store) setAndNotify(version int, path string, values interface{}) bool {
	s.rwMutex.Lock()
	treeModified, pathRoot := s.set(version, path, values)

	// If the tree was modified by the provider values, collect the handlers to be notified
	var pendingNotifications []*pendingNotification
	if treeModified {
		pendingNotifications = s.pendingNotifications(pathRoot)
	}

	// Release write lock
	s.rwMutex.Unlock()

	// Since our pending notifications include an immutable copy of our
	// data and the affected change handlers, we can fanout the notifications
	// without requiring any lock.
	if pendingNotifications != nil {
		go s.notifyWatchers(pendingNotifications)
	}

	return treeModified
}

// Set attempts to merge the given string or map value against the sub-tree
// rooted at path. This function must only be called after locking the store's
// write mutex.
//
// It returns a bool flag to indicate whether the path was modified and the
// root node corresponding to path.
func (s *Store) set(version int, path string, value interface{}) (bool, *node) {
	root := s.lookup(path, true)
	treeModified := root.merge(version, value)

	// Visit all ancestors and ensure that their version is at least equal
	// to this version.
	if treeModified {
		root.visitAncestors(func(n *node) {
			if n.version < version {
				n.version = version
			}
		})
	}

	return treeModified, root
}

// PendingNotifications first performs a DFS on a modified root to generate a list
// of watchers (in DFS order) that need to be notified due to a node value change.
// The list is augmented by visiting all ancestor nodes of root and including
// any watchers attached to them.
//
// If no watchers are defined for the affected nodes, this method returns nil.
//
// This method must only be called after locking the store's write mutex.
func (s *Store) pendingNotifications(root *node) []*pendingNotification {
	if s.watchers == nil {
		return nil
	}

	var pendingNotifications []*pendingNotification
	var visitor = func(n *node) {
		path := n.path()
		if s.watchers[path] == nil || len(s.watchers[path]) == 0 {
			return
		}

		if pendingNotifications == nil {
			pendingNotifications = make([]*pendingNotification, 0)
		}

		fnList := make([]ChangeHandlerFunc, len(s.watchers[path]))
		for index, watcher := range s.watchers[path] {
			fnList[index] = watcher.changeHandler
		}

		pendingNotifications = append(pendingNotifications, &pendingNotification{
			version:        n.version,
			values:         n.leafValues("", true),
			changeHandlers: fnList,
		})
	}

	// Collect pending notifications for the sub-tree from root and then
	// append notifications for root's ancestors.
	root.visitModifiedNodes(visitor)
	root.visitAncestors(visitor)
	return pendingNotifications
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
		if !createMissingNodes {
			return nil
		}
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

// FlattenedMapToTree converts a map from flat key format to a nested tree format.
// Given an input like:
//  {"segment1/segment2.../segment_n": value}
//
// it returns:
//  {"segment1": {
//    "segment2": { ...
//      {"segment_n": value}
//    }
//  }
func flattenedMapToTree(values map[string]string) (map[string]interface{}, error) {
	root := make(map[string]interface{}, 0)
	var segmentMap *map[string]interface{}
	for path, v := range values {
		if len(path) == 0 {
			return nil, fmt.Errorf("supplied value map contains empty path key")
		}
		segments := splitRegex.Split(path, -1)
		lastSegmentIndex := len(segments) - 1

		segmentMap = &root
		for index, segment := range segments {
			if len(segment) == 0 {
				return nil, fmt.Errorf("supplied value map contains empty segment for path %q", path)
			}

			subPath := (*segmentMap)[segment]
			subPathAsMap, isMap := subPath.(map[string]interface{})
			if subPath != nil && ((index == lastSegmentIndex && isMap) || (index != lastSegmentIndex && !isMap)) {
				return nil, fmt.Errorf("supplied value map contains both a value and a sub-path for %q", strings.Join(segments[:index+1], pathDelimiter))
			}

			switch {
			case index == lastSegmentIndex:
				(*segmentMap)[segment] = v
			case subPathAsMap != nil:
				segmentMap = &subPathAsMap
			default:
				subPathAsMap = make(map[string]interface{})
				(*segmentMap)[segment] = subPathAsMap
				segmentMap = &subPathAsMap
			}
		}
	}

	return root, nil
}
