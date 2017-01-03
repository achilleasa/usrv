package store

import (
	"fmt"
	"reflect"
	"strings"
)

const (
	pathDelimiter = "/"
)

type changeCallbackFn func(*node)

// Node is used as the building block of a configuration tree. Nodes that
// serve as the tree leaves store a string value while non-leaf nodes maintain
// a map containing their child nodes.
//
// All nodes include a version value which is used when merging to preserve
// or replace existing values.
type node struct {
	// The path segment where this node is located.
	segment string

	// The stored value for leaf nodes.
	value string

	// The version of the currently stored value.
	version int

	// The number of hops from the root of the configuration tree.
	depth int

	// The set of sub-paths originating from this segment. If this is a leaf
	// node then the paths map length will be 0.
	paths map[string]*node

	// The parent node in this path.
	parent *node
}

// Path returns the full path to this node. The full path consists of all segments
// from the root of the tree up to this node separated by a '/' character.
func (n *node) path() string {
	segments := make([]string, n.depth)
	index := n.depth - 1
	for node := n; node.parent != nil; node = node.parent {
		segments[index] = node.segment
		index--
	}

	return pathDelimiter + strings.Join(segments, pathDelimiter)
}

// IsLeaf returns true if this is a leaf node.
func (n *node) isLeaf() bool {
	return len(n.paths) == 0
}

// LeafValues performs a DFS on a subtree rooted at the current node and populates
// a map with the leaf values. The map keys are built by concatenating the supplied
// path prefix and the full path from this node to each leaf.
//
// Given a node structure that looks like:
//  {
//   "key1": {
//     "key2": "2",
//     "key3": {
//      "key4": "4"
//     }
//   }
//  }
//
// Calling TreeValues on the root node with pathPrefix = "test/" and pathDelimiter
// set to "/" will return:
//  {
//   "test/key1/key2": "2",
//   "test/key1/key3/key4": "4"
//  }
func (n *node) leafValues(pathPrefix string, isRoot bool) map[string]string {
	// If this is the subtree root and pathPrefix is not empty, ensure that
	// it ends with a single pathDelimiter
	if isRoot && len(pathPrefix) != 0 {
		pathPrefix = strings.TrimRight(pathPrefix, pathDelimiter) + pathDelimiter
	}

	// This is a leaf
	if n.isLeaf() {
		return map[string]string{
			pathPrefix + n.segment: n.value,
		}
	}

	if !isRoot {
		pathPrefix += n.segment + pathDelimiter
	}

	// Recursively build map by calling TreeValues on the child nodes and
	// merging the maps into a single value map
	valueMap := make(map[string]string, 0)
	for _, subPathNode := range n.paths {
		for k, v := range subPathNode.leafValues(pathPrefix, false) {
			valueMap[k] = v
		}
	}

	return valueMap
}

// Merge recursively attempts to apply the given value to this node and its children
// updating leaf nodes for which the curently stored value version is less than or
// equal to the supplied version.
//
// If the caller needs to be notified when a particular node in the tree is modified
// either directly (its value changed) or indirectly (its subtree got modified)
// it can specify a callback which will be invoked with every modified node as
// its argument.
//
// Merge accepts an interface for its value but in practice only supports
// two types of values:
//  - A string value which only gets applied if this node is a leaf after a version check
//  - A map[string]interface{} value for which merge iterates its keys and propagates
//    the values to child nodes, creating them if they do not exist.
//
// If the value argument does not match the expected values then merge will panic.
//
// A boolean flag is returned as an indicator of whether the subtree rooted
// at this node was modified as a result of applying the given value.
func (n *node) merge(version int, value interface{}, changeCallback changeCallbackFn) (modified bool) {
	switch mergeValue := value.(type) {
	case string:
		// Current node version is newer than the value we are trying to merge
		if version < n.version {
			return false
		}

		// Update the version
		n.version = version

		// If the value is equal to the one currently stored, then skip the update
		if n.value == mergeValue {
			return false
		}

		// Update the value and clear any existing child nodes
		n.value = mergeValue
		if len(n.paths) != 0 {
			n.paths = make(map[string]*node, 0)
		}

		if changeCallback != nil {
			changeCallback(n)
		}

		return true
	case map[string]interface{}:
		// Skip value if the map is empty
		if len(mergeValue) == 0 {
			return false
		}

		// If we have reached a leaf node we need to compare versions
		// to decide if we will overwrite the value with the map contents
		if n.isLeaf() {
			if version < n.version {
				return false
			}
			n.value = ""
		}

		// Ensure that this node tracks the latest version
		if version > n.version {
			n.version = version
		}

		// recursively merge the map values and create missing nodes
		modified := false
		var subPathNode *node
		for k, v := range mergeValue {
			subPathNode = n.paths[k]
			if subPathNode == nil {
				subPathNode = makeNode(k, n.depth+1, n)
			}
			modified = subPathNode.merge(version, v, changeCallback) || modified
		}

		if modified && changeCallback != nil {
			changeCallback(n)
		}

		return modified
	}

	panic(fmt.Errorf(`merge() only supports values of type "string" or map[string]interface{}; got %q for key %q`, reflect.TypeOf(value), n.path()))
}

// Initialize a new config node and attach it to its parent's subPath map.
func makeNode(segment string, depth int, parent *node) *node {
	node := &node{
		segment: segment,
		depth:   depth,
		paths:   make(map[string]*node, 0),
		parent:  parent,
	}

	if parent != nil {
		parent.paths[segment] = node
	}

	return node
}
