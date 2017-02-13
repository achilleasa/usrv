package protobuf

import (
	"reflect"
	"testing"

	"github.com/golang/protobuf/proto"
)

func TestCodec(t *testing.T) {
	example := &Example{
		Label: proto.String("example"),
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

func TestCodecErrors(t *testing.T) {
	codec := Codec()
	marshal := codec.Marshaler()
	unmarshal := codec.Unmarshaler()

	_, err := marshal("not-a-protobuf")
	if err != errSrcNotProtobuf {
		t.Fatalf("expected marshal to return errSrcNotProtobuf; got %v", err)
	}

	type foo struct{}
	err = unmarshal([]byte("not-a-protobuf"), &foo{})
	if err != errDstNotProtobuf {
		t.Fatalf("expected unmarshal to return errDstNotProtobuf; got %v", err)
	}
}
