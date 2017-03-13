// Package protobuf provides a codec for encoding and decoding of data using protocol buffers.
package protobuf

import (
	"errors"

	"github.com/achilleasa/usrv/encoding"
	"github.com/golang/protobuf/proto"
)

var (
	errSrcNotProtobuf = errors.New("marshal target does not implement proto.Message")
	errDstNotProtobuf = errors.New("unmarshal target does not implement proto.Message")
)

type protobufCodec struct{}

func (c *protobufCodec) Marshaler() encoding.Marshaler {
	return func(value interface{}) ([]byte, error) {
		srcMsg, valid := value.(proto.Message)
		if !valid {
			return nil, errSrcNotProtobuf
		}

		return proto.Marshal(srcMsg)
	}
}

func (c *protobufCodec) Unmarshaler() encoding.Unmarshaler {
	return func(data []byte, target interface{}) error {
		dstMsg, valid := target.(proto.Message)
		if !valid {
			return errDstNotProtobuf
		}
		return proto.Unmarshal(data, dstMsg)
	}
}

// Codec returns a codec that implements encoding and decoding of protocol buffers.
func Codec() encoding.Codec {
	return &protobufCodec{}
}
