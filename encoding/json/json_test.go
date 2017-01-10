package json

import (
	"reflect"
	"testing"
)

func TestMarshaler(t *testing.T) {
	type Example struct {
		Field1 string `json:"field_1"`
		Field2 int    `json:"field_2"`
	}

	example := &Example{
		Field1: "field1",
		Field2: 128,
	}

	codec := Codec()
	marshal := codec.Marshaler()

	data, err := marshal(example)
	if err != nil {
		t.Fatal(err)
	}

	expData := `{"field_1":"field1","field_2":128}`
	if string(data) != expData {
		t.Fatalf("expected marshaled data to be %q; got %q", expData, string(data))
	}
}

func TestUnmarshaler(t *testing.T) {
	type Example struct {
		Field1 string `json:"field_1"`
		Field2 int    `json:"field_2"`
	}

	expValue := &Example{
		Field1: "field1",
		Field2: 128,
	}
	example := &Example{}

	codec := Codec()
	unmarshal := codec.Unmarshaler()

	data := []byte(`{"field_1":"field1","field_2":128}`)
	err := unmarshal(data, example)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(example, expValue) {
		t.Fatalf("expected unmarshaled object to be:\n%#+v\n\ngot:\n%#+v", expValue, example)
	}
}
