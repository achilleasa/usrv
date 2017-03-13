package store

import (
	"fmt"
	"reflect"
	"runtime"
	"sync"
	"testing"
)

var nopHandler = func(_ map[string]string, _ int) {}

func TestReset(t *testing.T) {
	var s Store

	provider := &mockProvider{}
	s.RegisterValueProvider(provider)

	path := "bar"
	expValue := "value"
	s.SetKey(1, path, expValue)
	unsubFn := s.Watch(path, nopHandler)
	cfg, _ := s.Get(path)
	if cfg[path] != expValue {
		t.Fatalf("expected cfg value for path %q to be %q; got %q", path, expValue, cfg[path])
	}

	s.Reset()
	cfg, _ = s.Get(path)
	if len(cfg) != 0 {
		t.Fatalf("expected cfg after reset to be empty; got %v", cfg)
	}

	// Unusbscribing post-reset should be a no-op
	unsubFn()
}

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

	expVersion := 1
	modified, err := s.SetKey(expVersion, "/foo/bar/baz", "test")
	if err != nil {
		t.Fatal(err)
	}
	if !modified {
		t.Fatal("expected SetKey to modify the store")
	}

	expValues := map[string]string{
		"foo/bar/baz": "test",
	}

	values, version := s.Get("/")
	if !reflect.DeepEqual(values, expValues) {
		t.Errorf("expected config store values to be:\n%v\n\ngot:\n%v", expValues, values)
	}
	if version != expVersion {
		t.Errorf("expected config store value version to be %d; got %d", expVersion, version)
	}
}

func TestSetKeysWithNilValues(t *testing.T) {
	var s Store

	modified, err := s.SetKeys(1, "/foo", nil)
	if err != nil {
		t.Fatal(err)
	}
	if modified {
		t.Fatal("expected SetKeys with nil map not to modify store")
	}

	modified, err = s.SetKeys(1, "/foo", map[string]string{})
	if err != nil {
		t.Fatal(err)
	}
	if modified {
		t.Fatal("expected SetKeys with empty map not to modify store")
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

	values, _ = s.Get("/path/to/key/key1")
	if !reflect.DeepEqual(values, expValues2) {
		t.Fatalf("expected config store values to be:\n%v\n\ngot:\n%v", expValues2, values)
	}

	// Initial delimiter may be omitted
	values, _ = s.Get("path/to/key/key1")
	if !reflect.DeepEqual(values, expValues2) {
		t.Fatalf("expected config store values to be:\n%v\n\ngot:\n%v", expValues2, values)
	}
}

func TestStoreSetKeysErrors(t *testing.T) {
	var s Store

	valueMapWithPathClash := map[string]string{
		"key1/key2":      "2",
		"key1/key2/key3": "3",
	}
	expError := `supplied value map contains both a value and a sub-path for "key1/key2"`
	_, err := s.SetKeys(1, "", valueMapWithPathClash)
	if err == nil || err.Error() != expError {
		t.Errorf("expected to get error %q; got %v", expError, err)
	}

	valueMapWithEmptyPath := map[string]string{
		"key1/key2": "2",
		"":          "3",
	}
	expError = `supplied value map contains empty path key`
	_, err = s.SetKeys(1, "", valueMapWithEmptyPath)
	if err == nil || err.Error() != expError {
		t.Errorf("expected to get error %q; got %v", expError, err)
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
		path       string
		expValues  map[string]string
		expVersion int
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

	expVersion := 1
	modified, err := s.SetKeys(expVersion, "", valueMap)
	if err != nil {
		t.Fatal(err)
	}
	if !modified {
		t.Fatal("expected SetKeys to modify the store")
	}

	for specIndex, spec := range specs {
		values, version := s.Get(spec.path)
		if !reflect.DeepEqual(values, spec.expValues) {
			t.Errorf("[spec %d] expected config store values to be:\n%v\n\ngot:\n%v", specIndex, spec.expValues, values)
		}
		if version != expVersion {
			t.Errorf("[spec %d] expected config store value version to be %d; got %d", specIndex, expVersion, version)
		}
	}
}

func TestGetWithNonExistingPath(t *testing.T) {
	var s Store

	_, err := s.SetKey(1, "/foo/bar", "v1")
	if err != nil {
		t.Fatal(err)
	}

	values, version := s.Get("/boo/bar")
	if len(values) != 0 {
		t.Errorf("expected Get with non-existing path to return an empty map; got %v", values)
	}
	if version != -1 {
		t.Errorf("expected Get with non-existing path to return version equal to -1; got %d", version)
	}
}

func TestWatchersForKeyNotYetInStore(t *testing.T) {
	var s Store
	var wg sync.WaitGroup

	expVersion := 1
	expValues := map[string]string{"bar": "test"}

	wg.Add(2)
	var handler = func(values map[string]string, version int) {
		defer wg.Done()

		if version != expVersion {
			t.Errorf("expected handler to receive values with version %d; got %d", expVersion, version)
		}

		if !reflect.DeepEqual(values, expValues) {
			t.Errorf("expected handler to receive values:\n%v\n\ngot:\n%v", expValues, values)
		}
	}

	unsubFn := s.Watch("/foo/bar", handler)
	defer unsubFn()

	unsubFn = s.Watch("foo/bar", handler)
	defer unsubFn()

	// Set value and wait for handlers to be triggered
	s.SetKey(expVersion, "/foo/bar", expValues["bar"])

	wg.Wait()
}

func TestWatchersUnsubscribe(t *testing.T) {
	var s Store

	// Calling unwatch when no watchers are defined should be ok
	s.unwatch("/a/path", 1)()

	var handler = func(values map[string]string, version int) {
		t.Fatal("unexpected call to handler")
	}

	unsubFn := s.Watch("/foo/bar", handler)

	// Setting a key for a path not watched should be ok
	s.SetKey(1, "/another/path", "new value")

	// Calling unwatch on an unknown path while at least one watcher is defined should be ok
	s.unwatch("/a/path", 1)()

	// Unsubscribe should prevent further changes from being sent to the channel
	unsubFn()

	// Ensure that the watcher has been removed
	remainingPathWatchers := len(s.watchers["/foo/bar"])
	if remainingPathWatchers != 0 {
		t.Fatalf("expected path wathers list to be empty; got %d entries\n", remainingPathWatchers)
	}

	s.SetKey(1, "/foo/bar", "new value")
	runtime.Gosched()
}

func TestWatcherHandlerCannotDeadlockStore(t *testing.T) {
	var s Store
	var wg sync.WaitGroup

	wg.Add(2)
	var unsubFn UnsubscribeFunc
	var handler = func(values map[string]string, version int) {
		defer wg.Done()

		expValue := fmt.Sprintf("value%d", version)
		if values["bar"] != expValue {
			t.Errorf(`expected values["bar"] to be %s; got %v`, expValue, values["bar"])
		}

		// Ensure that calling get does not block
		s.Get("/foo/bar")

		// Ensure that calling set does not block but still invokes the handler one more time
		if version == 1 {
			s.SetKey(2, "/foo/bar", "value2")
		} else {
			// Unsubscribing from inside the handler should work; same if the store gets reset
			unsubFn()
			s.Reset()
		}
	}

	unsubFn = s.Watch("/foo/bar", handler)
	defer unsubFn()

	s.SetKey(1, "/foo/bar", "value1")
	wg.Wait()
}

func TestValueProvider(t *testing.T) {
	var s Store
	var wg sync.WaitGroup
	var path = "/foo/bar"
	var expValue = "value"
	var expCfgMap = map[string]string{
		"bar": expValue,
	}

	provider := &mockProvider{}
	s.RegisterValueProvider(provider)

	wg.Add(1)
	var handler = func(values map[string]string, version int) {
		defer wg.Done()

		if !reflect.DeepEqual(values, expCfgMap) {
			t.Fatalf("expected to receive config value:\n%v\n\ngot:\n%v", expCfgMap, values)
		}
	}

	unsubFn := s.Watch(path, handler)
	defer unsubFn()

	// Trigger the mockProvider watch for path and ensure that the value gets picked up by the watcher
	provider.store.SetKey(1, path, expValue)
	wg.Wait()
}

func TestValueProviderThatAlreadyContainsPath(t *testing.T) {
	var s Store
	var wg sync.WaitGroup
	var path = "/foo/bar"
	var expValue = "value"
	var expCfgMap = map[string]string{
		"bar": expValue,
	}

	provider := &mockProvider{}
	provider.store.SetKey(1, path, expValue)
	s.RegisterValueProvider(provider)

	wg.Add(1)
	var handler = func(values map[string]string, version int) {
		defer wg.Done()

		if !reflect.DeepEqual(values, expCfgMap) {
			t.Fatalf("expected to receive config value:\n%v\n\ngot:\n%v", expCfgMap, values)
		}
	}

	unsubFn := s.Watch(path, handler)
	defer unsubFn()

	// Ensure that the value gets picked up by the watcher
	wg.Wait()
}

func TestMisbehavingValueProvider(t *testing.T) {
	var s Store
	provider := &badDataProvider{}

	path := "/foo/bar"

	// Register provider and add watch
	s.RegisterValueProvider(provider)
	unsubFn := s.Watch(path, nopHandler)
	defer unsubFn()
}

func TestValueProviderShouldNotDeadlockStore(t *testing.T) {
	var s Store
	provider := &badDataProvider{watchTriggersSetter: true}

	path := "/foo/bar"

	// Register provider and add watch; s.Watch calls the value setter while inside
	// Watch(); this shouldn't cause a deadlock
	s.RegisterValueProvider(provider)
	unsubFn := s.Watch(path, nopHandler)
	defer unsubFn()
}

type mockProvider struct {
	store Store
}

func (p *mockProvider) Get(path string) map[string]string {
	v, _ := p.store.Get(path)
	return v
}

func (p *mockProvider) Watch(path string, valueSetter func(string, map[string]string)) func() {
	return p.store.Watch(path, func(cfg map[string]string, _ int) {
		valueSetter(path, cfg)
	})
}

type badDataProvider struct {
	watchTriggersSetter bool
}

func (p *badDataProvider) Get(path string) map[string]string {
	return map[string]string{
		"key1/key2":      "2",
		"key1/key2/key3": "3",
	}
}

func (p *badDataProvider) Watch(path string, valueSetter func(string, map[string]string)) func() {
	if p.watchTriggersSetter {
		valueSetter(path, map[string]string{"foo": "bar"})
	}
	return func() {}
}
