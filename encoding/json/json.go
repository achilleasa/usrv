// Package json provides a codec for encoding and decoding JSON data.
package json

import (
	"encoding/json"

	"github.com/achilleasa/usrv/encoding"
)

type jsonCodec struct{}

func (c *jsonCodec) Marshaler() encoding.Marshaler {
	return json.Marshal
}

func (c *jsonCodec) Unmarshaler() encoding.Unmarshaler {
	return json.Unmarshal
}

// Codec returns a codec that implements encoding and decoding of JSON data.
func Codec() encoding.Codec {
	return &jsonCodec{}
}
