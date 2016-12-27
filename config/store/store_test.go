package store

import (
	"reflect"
	"testing"
	"time"
)

func TestLookup(t *testing.T) {
	var s Store

	s.lookup("foo/bar", true)
	s.lookup("foo/baz", true)
	if s.root == nil {
		t.Fatal("store root is nil")
	}
	node := s.root
	expPathCount := 1
	expSegment := ""
	expPath := "/"
	if node.segment != expSegment {
		t.Fatalf("expected node segment to be %q; got %q", expSegment, node.segment)
	}
	if node.path() != expPath {
		t.Fatalf("expected node path to be %q; got %q", expPath, node.path())
	}
	if len(node.paths) != expPathCount {
		t.Fatalf("expected root node to have %d subpaths; got %d", expPathCount, len(node.paths))
	}

	// Node should be "foo"
	node = node.paths["foo"]
	expPathCount = 2
	expSegment = "foo"
	expPath = "/foo"
	if node.segment != expSegment {
		t.Fatalf("expected node segment to be %q; got %q", expSegment, node.segment)
	}
	if node.path() != expPath {
		t.Fatalf("expected node path to be %q; got %q", expPath, node.path())
	}
	if len(node.paths) != expPathCount {
		t.Fatalf("expected root node to have %d subpaths; got %d", expPathCount, len(node.paths))
	}

	// Node should be "bar"
	node = node.paths["bar"]
	expPathCount = 0
	expSegment = "bar"
	expPath = "/foo/bar"
	if node.segment != expSegment {
		t.Fatalf("expected node segment to be %q; got %q", expSegment, node.segment)
	}
	if node.path() != expPath {
		t.Fatalf("expected node path to be %q; got %q", expPath, node.path())
	}
	if len(node.paths) != expPathCount {
		t.Fatalf("expected root node to have %d subpaths; got %d", expPathCount, len(node.paths))
	}

	// Visit sibling via parent pointer; node should be "baz"
	node = node.parent.paths["baz"]
	expPathCount = 0
	expSegment = "baz"
	expPath = "/foo/baz"
	if node.segment != expSegment {
		t.Fatalf("expected node segment to be %q; got %q", expSegment, node.segment)
	}
	if node.path() != expPath {
		t.Fatalf("expected node path to be %q; got %q", expPath, node.path())
	}
	if len(node.paths) != expPathCount {
		t.Fatalf("expected root node to have %d subpaths; got %d", expPathCount, len(node.paths))
	}

	// Calling lookup with a non-existing path and createMissingNodes = false
	// should return nil
	values := s.lookup("/foo/bar/unknown", false)
	if values != nil {
		t.Fatalf("expected lookup with createMissingNodes = false and non-existing path to return nil; got %v", values)
	}
}

func TestSetKey(t *testing.T) {
	var s Store

	modified, err := s.SetKey(1, "/foo/bar/baz", "test")
	if err != nil {
		t.Fatal(err)
	}
	if !modified {
		t.Fatal("expected SetKey to modify the store")
	}

	expValues := map[string]string{
		"foo/bar/baz": "test",
	}

	values := s.Get("/")
	if !reflect.DeepEqual(values, expValues) {
		t.Fatalf("expected config store values to be:\n%v\n\ngot:\n%v", expValues, values)
	}
}

func TestSetKeys(t *testing.T) {
	var s Store

	valueMap := map[string]string{
		"key1/key2":      "2",
		"key1/key3/key4": "4",
	}
	expValues := map[string]string{
		"path/to/key/key1/key2":      "2",
		"path/to/key/key1/key3/key4": "4",
	}
	expValues2 := map[string]string{
		"key2":      "2",
		"key3/key4": "4",
	}

	modified, err := s.SetKeys(1, "/path/to/key", valueMap)
	if err != nil {
		t.Fatal(err)
	}
	if !modified {
		t.Fatal("expected SetKeys to modify the store")
	}

	values := s.root.leafValues("", true)
	if !reflect.DeepEqual(values, expValues) {
		t.Fatalf("expected config store values to be:\n%v\n\ngot:\n%v", expValues, values)
	}

	values = s.Get("/path/to/key/key1")
	if !reflect.DeepEqual(values, expValues2) {
		t.Fatalf("expected config store values to be:\n%v\n\ngot:\n%v", expValues2, values)
	}

	// Initial delimiter may be omitted
	values = s.Get("path/to/key/key1")
	if !reflect.DeepEqual(values, expValues2) {
		t.Fatalf("expected config store values to be:\n%v\n\ngot:\n%v", expValues2, values)
	}
}

func TestSetKeysErrors(t *testing.T) {
	var s Store

	valueMapWithPathClash := map[string]string{
		"key1/key2":      "2",
		"key1/key2/key3": "3",
	}
	expError := `supplied value map contains both a value and a sub-path for "key1/key2"`
	_, err := s.SetKeys(1, "", valueMapWithPathClash)
	if err == nil || err.Error() != expError {
		t.Fatalf("expected to get error %q; got %v", expError, err)
	}

	valueMapWithEmptyPath := map[string]string{
		"key1/key2": "2",
		"":          "3",
	}
	expError = `supplied value map contains empty path key`
	_, err = s.SetKeys(1, "", valueMapWithEmptyPath)
	if err == nil || err.Error() != expError {
		t.Fatalf("expected to get error %q; got %v", expError, err)
	}

	valueMapWithEmptySegment := map[string]string{
		"key1/key2": "2",
		"key1//":    "3",
	}
	expError = `supplied value map contains empty segment for path "key1//"`
	_, err = s.SetKeys(1, "", valueMapWithEmptySegment)
	if err == nil || err.Error() != expError {
		t.Fatalf("expected to get error %q; got %v", expError, err)
	}
}

func TestGet(t *testing.T) {
	var s Store

	valueMap := map[string]string{
		"key1/key2":      "2",
		"key1/key3/key4": "4",
	}

	specs := []struct {
		path      string
		expValues map[string]string
	}{
		{
			path: "/",
			expValues: map[string]string{
				"key1/key2":      "2",
				"key1/key3/key4": "4",
			},
		},
		{
			path: "key1/",
			expValues: map[string]string{
				"key2":      "2",
				"key3/key4": "4",
			},
		},
		{
			path: "key1",
			expValues: map[string]string{
				"key2":      "2",
				"key3/key4": "4",
			},
		},
		{
			path: "key1/key3/key4",
			expValues: map[string]string{
				"key4": "4",
			},
		},
		{
			path: "key1/key3/key4/",
			expValues: map[string]string{
				"key4": "4",
			},
		},
	}

	modified, err := s.SetKeys(1, "", valueMap)
	if err != nil {
		t.Fatal(err)
	}
	if !modified {
		t.Fatal("expected SetKeys to modify the store")
	}

	for specIndex, spec := range specs {
		values := s.Get(spec.path)
		if !reflect.DeepEqual(values, spec.expValues) {
			t.Errorf("[spec %d] expected config store values to be:\n%v\n\ngot:\n%v", specIndex, spec.expValues, values)
		}
	}
}

func TestGetWithNonExistingPath(t *testing.T) {
	var s Store

	_, err := s.SetKey(1, "/foo/bar", "v1")
	if err != nil {
		t.Fatal(err)
	}

	values := s.Get("/boo/bar")
	if len(values) != 0 {
		t.Fatalf("expected Get with non-existing path to return an empty map; got %v", values)
	}
}

func TestWatchersForKeyNotYetInStore(t *testing.T) {
	var s Store

	var cfgChans [2]<-chan map[string]string
	var unsubFn UnsubscribeFunc
	cfgChans[0], unsubFn = s.Watch("/foo/bar")
	defer unsubFn()

	cfgChans[1], unsubFn = s.Watch("foo/bar")
	defer unsubFn()

	// Both channels should receive an empty map as the key does not exist yet
	for index, cfgChan := range cfgChans {
		select {
		case cfg := <-cfgChan:
			if len(cfg) != 0 {
				t.Errorf("expected cfgChan %d to receive an empty map", index)
			}
		case <-time.After(1 * time.Second):
			t.Errorf("time out waiting for cfgChan %d to receive initial cfg", index)
		}

	}

	// Set value and trigger registered watchers
	expValue := "test"
	s.SetKey(1, "/foo/bar", expValue)

	// Both channels should receive an update with the value we just set
	for index, cfgChan := range cfgChans {
		select {
		case cfg := <-cfgChan:
			if len(cfg) != 1 {
				t.Errorf("expected cfgChan %d to receive a non-empty map", index)
			}
			for _, v := range cfg {
				if v != expValue {
					t.Errorf("expected cfgChan %d to receive value %q; got %q", index, expValue, v)
				}
			}
		case <-time.After(1 * time.Second):
			t.Errorf("time out waiting for cfgChan %d to receive updated cfg", index)
		}
	}
}

func TestWatchersUnsubscribe(t *testing.T) {
	var s Store

	// Calling unwatch when no watchers are defined should be ok
	s.unwatch("/a/path", 1)()

	cfgChan, unsubFn := s.Watch("/foo/bar")
	<-cfgChan

	// Setting a key for a path not watched should be ok
	s.SetKey(1, "/another/path", "new value")

	// Unsubscribe should prevent further changes from beeing sent to the channel
	unsubFn()

	// Ensure that the watcher has been removed
	remainingPathWatchers := len(s.watchers["/foo/bar"])
	if remainingPathWatchers != 0 {
		t.Fatalf("expected path wathers list to be empty; got %d entries\n", remainingPathWatchers)
	}

	s.SetKey(1, "/foo/bar", "new value")

	_, opened := <-cfgChan
	if opened {
		t.Fatalf("expected cfg channel to be closed after unsubscribing")
	}

	// Calling unwatch on an unknown path while at least one watcher is defined should be ok
	s.unwatch("/a/path", 1)()
}

func TestWatchersNotificationFanout(t *testing.T) {
	var s Store
	defer func() {
		beforeUpdateHookFn = nil
		afterUpdateHookFn = nil
	}()

	cfgChan, unsubFn := s.Watch("/foo/bar")
	checkpoint := make(chan struct{}, 0)

	afterUpdateHookFn = func() {
		checkpoint <- struct{}{}
	}

	// Trigger an update before dequeuing current value
	s.SetKey(1, "/foo/bar", "new value v1")

	// wait for fanout go-routine to exit
	<-checkpoint

	cfg := <-cfgChan
	if len(cfg) != 0 {
		t.Fatalf("expected to receive empty map for the original path values")
	}

	// Block the fanout go-routine and then trigger an unsubscribe before
	// proceeding with the fanout
	beforeUpdateHookFn = func() {
		<-checkpoint
	}
	s.SetKey(2, "/foo/bar", "new value v2")
	unsubFn()

	// Unblock goroutine and wait til it exits
	checkpoint <- struct{}{}
	<-checkpoint
}
