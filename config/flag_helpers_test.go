package config

import (
	"reflect"
	"testing"
)

func TestBoolFlag(t *testing.T) {
	f := BoolFlag("")
	expValue := true
	f.Set(expValue)
	if f.Get() != expValue {
		t.Fatalf("expected get() to return %t; got %t", expValue, f.Get())
	}
}

func TestFloat32Flag(t *testing.T) {
	f := Float32Flag("")
	var expValue float32 = 123.456
	f.Set(expValue)
	if f.Get() != expValue {
		t.Fatalf("expected get() to return %f; got %f", expValue, f.Get())
	}
}

func TestFloat64Flag(t *testing.T) {
	f := Float64Flag("")
	expValue := 123.456
	f.Set(expValue)
	if f.Get() != expValue {
		t.Fatalf("expected get() to return %f; got %f", expValue, f.Get())
	}
}

func TestUint32Flag(t *testing.T) {
	f := Uint32Flag("")
	var expValue uint32 = 123
	f.Set(expValue)
	if f.Get() != expValue {
		t.Fatalf("expected get() to return %d; got %d", expValue, f.Get())
	}
}

func TestInt32Flag(t *testing.T) {
	f := Int32Flag("")
	var expValue int32 = 123
	f.Set(expValue)
	if f.Get() != expValue {
		t.Fatalf("expected get() to return %d; got %d", expValue, f.Get())
	}
}

func TestUint64Flag(t *testing.T) {
	f := Uint64Flag("")
	var expValue uint64 = 123
	f.Set(expValue)
	if f.Get() != expValue {
		t.Fatalf("expected get() to return %d; got %d", expValue, f.Get())
	}
}

func TestInt64Flag(t *testing.T) {
	f := Int64Flag("")
	var expValue int64 = 123
	f.Set(expValue)
	if f.Get() != expValue {
		t.Fatalf("expected get() to return %d; got %d", expValue, f.Get())
	}
}

func TestStringFlag(t *testing.T) {
	f := StringFlag("")
	expValue := "test"
	f.Set(expValue)
	if f.Get() != expValue {
		t.Fatalf("expected get() to return %s; got %s", expValue, f.Get())
	}
}

func TestMapFlag(t *testing.T) {
	f := MapFlag("")
	expValue := map[string]string{
		"key1":      "value1",
		"key2/key3": "value2",
	}
	f.Set(expValue)
	if !reflect.DeepEqual(f.Get(), expValue) {
		t.Fatalf("expected get() to return:\n%v\n\ngot:\n%v", expValue, f.Get())
	}
}
