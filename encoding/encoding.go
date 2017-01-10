// Package encoding defines interfaces shared by other packages that are used by
// usrv servers and clients to encode and decode the endpoint-specific request
// and response messages into a byte-level format suitable for transmitting over
// a transport.
package encoding

// Marshaler is the interface implemented by objects that can produce a byte
// representation of another object.
type Marshaler func(interface{}) ([]byte, error)

// Unmarshaler is the interface implemented by objects that can unmarshal a byte
// representation of an object into an object instance.
type Unmarshaler func([]byte, interface{}) error

// Codec is implemented by objects that can produce marshalers and unmarshalers
// for a given object type.
//
// Marshaler returns a Marshaler implementation that can marshal instances of a
// particular type into a byte slice.
//
// Unmarshaler returns a Unmarshaler implementation that can unmarshal instances
// of a particular type from a byte slice.
type Codec interface {
	Marshaler() Marshaler
	Unmarshaler() Unmarshaler
}
