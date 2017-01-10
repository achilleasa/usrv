package gob

import (
	"reflect"
	"testing"
)

func TestCodec(t *testing.T) {
	type Example struct {
		Field1 string
		Field2 int
	}

	example := &Example{
		Field1: "field1",
		Field2: 128,
	}

	codec := Codec()
	marshal := codec.Marshaler()
	unmarshal := codec.Unmarshaler()

	data, err := marshal(example)
	if err != nil {
		t.Fatal(err)
	}

	target := &Example{}
	err = unmarshal(data, target)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(example, target) {
		t.Fatalf("expected unmarshaled object to be:\n%#+v\n\ngot:\n%#+v", example, target)
	}
}
