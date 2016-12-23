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
	mutex sync.Mutex
	root  *node
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

	root := s.lookup(path, false)
	if root == nil {
		return make(map[string]string, 0)
	}

	return root.leafValues("", true)
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

func (s *Store) set(version int, path string, value interface{}) bool {
	root := s.lookup(path, true)
	return root.merge(version, value)
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
