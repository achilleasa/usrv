package config

import (
	"reflect"
	"testing"
)

func TestSetDefaults(t *testing.T) {
	defer Store.Reset()

	expValues := map[string]string{
		"bar/baz": "key1",
		"boo":     "key2",
	}

	err := SetDefaults("/test/path", expValues)
	if err != nil {
		t.Fatal(err)
	}

	values := Store.Get("/test/path")
	if !reflect.DeepEqual(values, expValues) {
		t.Fatalf("expected Store.Get to return:\n%v\n\ngot:\n%v", expValues, values)
	}
}
