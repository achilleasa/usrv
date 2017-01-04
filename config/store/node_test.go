package store

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestNodeLeafValues(t *testing.T) {
	root := makeNode("", 0, nil)
	node1 := makeNode("key1", 1, root)
	node2 := makeNode("key2", 2, node1)
	node2.value = "2"
	node3 := makeNode("key3", 2, node1)
	node4 := makeNode("key4", 3, node3)
	node4.value = "4"

	specs := []struct {
		prefix string
		expMap map[string]string
	}{
		{
			prefix: "",
			expMap: map[string]string{
				"key1/key2":      "2",
				"key1/key3/key4": "4",
			},
		},
		{
			prefix: "/path/to/key/",
			expMap: map[string]string{
				"/path/to/key/key1/key2":      "2",
				"/path/to/key/key1/key3/key4": "4",
			},
		},
		{
			prefix: "/path/to/key",
			expMap: map[string]string{
				"/path/to/key/key1/key2":      "2",
				"/path/to/key/key1/key3/key4": "4",
			},
		},
	}

	for specIndex, spec := range specs {
		valueMap := root.leafValues(spec.prefix, true)
		if !reflect.DeepEqual(valueMap, spec.expMap) {
			t.Errorf("[spec %d] expected value map:\n%v\n\ngot:\n%v", specIndex, spec.expMap, valueMap)
		}
	}
}

func TestNodeLeafValuesOnSubPath(t *testing.T) {
	root := makeNode("", 0, nil)
	node1 := makeNode("key1", 1, root)
	node2 := makeNode("key2", 2, node1)
	node2.value = "2"
	node3 := makeNode("key3", 2, node1)
	node4 := makeNode("key4", 3, node3)
	node4.value = "4"

	specs := []struct {
		prefix string
		expMap map[string]string
	}{
		{
			prefix: "",
			expMap: map[string]string{
				"key2":      "2",
				"key3/key4": "4",
			},
		},
		{
			prefix: "/path/to/key/",
			expMap: map[string]string{
				"/path/to/key/key2":      "2",
				"/path/to/key/key3/key4": "4",
			},
		},
		{
			prefix: "/path/to/key",
			expMap: map[string]string{
				"/path/to/key/key2":      "2",
				"/path/to/key/key3/key4": "4",
			},
		},
	}

	for specIndex, spec := range specs {
		valueMap := node1.leafValues(spec.prefix, true)
		if !reflect.DeepEqual(valueMap, spec.expMap) {
			t.Errorf("[spec %d] expected value map:\n%v\n\ngot:\n%v", specIndex, spec.expMap, valueMap)
		}
	}
}

func TestNodeSegmentAndPath(t *testing.T) {
	root := makeNode("", 0, nil)
	node1 := makeNode("key1", 1, root)
	node2 := makeNode("key2", 2, node1)
	node2.value = "2"
	node3 := makeNode("key3", 2, node1)
	node4 := makeNode("key4", 3, node3)
	node4.value = "4"

	specs := []struct {
		node       *node
		expSegment string
		expPath    string
	}{
		{
			node:       root,
			expSegment: "",
			expPath:    "/",
		},
		{
			node:       node1,
			expSegment: "key1",
			expPath:    "/key1",
		},
		{
			node:       node2,
			expSegment: "key2",
			expPath:    "/key1/key2",
		},
		{
			node:       node3,
			expSegment: "key3",
			expPath:    "/key1/key3",
		},
		{
			node:       node4,
			expSegment: "key4",
			expPath:    "/key1/key3/key4",
		},
	}

	for specIndex, spec := range specs {
		if spec.node.segment != spec.expSegment {
			t.Errorf("[spec %d] expected segment to be %q; got %q", specIndex, spec.expSegment, spec.node.segment)
		}

		path := spec.node.path()
		if path != spec.expPath {
			t.Errorf("[spec %d] expected path to be %q; got %q", specIndex, spec.expPath, path)
		}
	}
}

func TestNodeMerge(t *testing.T) {
	specs := []struct {
		cfg         string
		version     int
		expValues   map[string]string
		expModified bool
	}{
		{
			cfg:     `{"key1":{"key2":"2","key3":{"key4":"4"}}}`,
			version: 2,
			expValues: map[string]string{
				"key1/key2":      "2",
				"key1/key3/key4": "4",
			},
			expModified: true,
		},
		{
			cfg:     `{"key1":{"key2":"2"}}`,
			version: 3,
			expValues: map[string]string{
				"key1/key2":      "2",
				"key1/key3/key4": "4",
			},
			// bumped version but key value not actually modified
			expModified: false,
		},
		{
			cfg:     `{"key1":{"key2":"new","key3":{"key4":"new"}}}`,
			version: 1,
			expValues: map[string]string{
				"key1/key2":      "2",
				"key1/key3/key4": "4",
			},
			expModified: false,
		},
		{
			cfg:     `{"key1":{"key2":"2","key3":{"key4":"new","key5":"5"}}}`,
			version: 1,
			expValues: map[string]string{
				"key1/key2":      "2",
				"key1/key3/key4": "4",
				"key1/key3/key5": "5",
			},
			expModified: true,
		},
		{
			cfg:     `{"key1":{"key2":"2","key3":"overwrite!"}}`,
			version: 10,
			expValues: map[string]string{
				"key1/key2": "2",
				"key1/key3": "overwrite!",
			},
			expModified: true,
		},
		{
			cfg:     `{"key1":{"key2":"2","key3":{"key4":"4","key5":"5"}}}`,
			version: 9,
			expValues: map[string]string{
				"key1/key2": "2",
				"key1/key3": "overwrite!",
			},
			expModified: false,
		},
		{
			cfg:     `{"key1":{"key2":"2","key3":{}}}`,
			version: 10,
			expValues: map[string]string{
				"key1/key2": "2",
				"key1/key3": "overwrite!",
			},
			expModified: false,
		},
	}
	var cfg map[string]interface{}
	var err error
	root := makeNode("", 0, nil)

	for specIndex, spec := range specs {
		cfg = nil
		err = json.Unmarshal([]byte(spec.cfg), &cfg)
		if err != nil {
			t.Fatal(err)
		}

		modified := root.merge(spec.version, cfg, nil)
		if modified != spec.expModified {
			t.Errorf("[spec %d] expected root.merge to return %t; got %t", specIndex, spec.expModified, modified)
		}

		values := root.leafValues("", true)
		if !reflect.DeepEqual(values, spec.expValues) {
			t.Errorf("[spec %d] expected merged tree values to be:\n%v\n\ngot:\n%v", specIndex, spec.expValues, values)
		}
	}
}

func TestNodeMergeCallback(t *testing.T) {
	var cfg map[string]interface{}
	err := json.Unmarshal([]byte(`{"key1":{"key2":"2","key3":{"key4":"4"}}}`), &cfg)
	if err != nil {
		t.Fatal(err)
	}
	root := makeNode("", 0, nil)

	changeFnCallCount := 0
	root.merge(1, cfg, func(_ *node) { changeFnCallCount++ })

	// We expect 1 call for each key + 1 call for the store root node
	expCount := 5
	if changeFnCallCount != expCount {
		t.Fatalf("expected change callback to be invoked %d times; got %d", expCount, changeFnCallCount)
	}
}

func TestNodeMergePanic(t *testing.T) {
	defer func() {
		if err := recover(); err != nil {
			expError := `merge() only supports values of type "string" or map[string]interface{}; got "int" for key "/key1/key2"`
			if pErr, isError := err.(error); isError {
				if pErr.Error() == expError {
					return
				}
				t.Fatalf("expected merge to panic with %q; got %q", expError, pErr.Error())
			}
		}

		t.Fatalf("expected merge() to panic")
	}()

	root := makeNode("", 0, nil)
	root.merge(1, map[string]interface{}{
		"key1": map[string]interface{}{
			"key2": 1,
		},
	}, nil)
}
