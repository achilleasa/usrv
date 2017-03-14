// Package msgpack provides a codec for encoding and decoding data using msgpack.
package msgpack

import (
	"github.com/achilleasa/usrv/encoding"
	impl "gopkg.in/vmihailenco/msgpack.v2"
)

type msgpackCodec struct{}

func (c *msgpackCodec) Marshaler() encoding.Marshaler {
	return func(v interface{}) ([]byte, error) {
		return impl.Marshal(v)
	}
}

func (c *msgpackCodec) Unmarshaler() encoding.Unmarshaler {
	return func(data []byte, target interface{}) error {
		return impl.Unmarshal(data, target)
	}
}

// Codec returns a codec that implements encoding and decoding of data using msgpack.
func Codec() encoding.Codec {
	return &msgpackCodec{}
}
