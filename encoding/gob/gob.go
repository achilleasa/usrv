// Package gob provides a codec for encoding and decoding of data using the gob format.
package gob

import (
	"bytes"
	"encoding/gob"

	"github.com/achilleasa/usrv/encoding"
)

type gobCodec struct{}

func (c *gobCodec) Marshaler() encoding.Marshaler {
	return func(value interface{}) ([]byte, error) {
		var buf bytes.Buffer
		encoder := gob.NewEncoder(&buf)
		err := encoder.Encode(value)
		return buf.Bytes(), err
	}
}

func (c *gobCodec) Unmarshaler() encoding.Unmarshaler {
	return func(data []byte, target interface{}) error {
		return gob.NewDecoder(bytes.NewReader(data)).Decode(target)
	}
}

// Codec retuns a codec that implements encoding and decoding of gob data.
func Codec() encoding.Codec {
	return &gobCodec{}
}
