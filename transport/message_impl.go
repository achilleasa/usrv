package transport

import (
	"net/http"
	"sync"

	"github.com/google/uuid"
)

var (
	// We allocate our messages of a pool so we can reduce GC pressure.
	msgPool = sync.Pool{
		New: func() interface{} {
			return &GenericMessage{}
		},
	}
)

// GenericMessage provides a re-usable message implementation that implements both
// Message and ImmutableMessage interfaces. Transports can use this as a basis
// for their own message implementations.
type GenericMessage struct {
	IDField               string
	SenderField           string
	ReceiverField         string
	SenderEndpointField   string
	ReceiverEndpointField string
	ReceiverVersionField  string
	HeadersField          map[string]string
	PayloadField          []byte
	ErrField              error
}

// ID returns a UUID for this message.
func (m *GenericMessage) ID() string {
	return m.IDField
}

// Close implements io.Closer and should be called when the message is no longer used.
func (m *GenericMessage) Close() error {
	msgPool.Put(m)

	return nil
}

// Sender returns the name of the service that sent the mesage.
func (m *GenericMessage) Sender() string {
	return m.SenderField
}

// Receiver returns the name of the service that will receive the message.
func (m *GenericMessage) Receiver() string {
	return m.ReceiverField
}

// SenderEndpoint returns the sending service's endpoint.
func (m *GenericMessage) SenderEndpoint() string {
	return m.SenderEndpointField
}

// ReceiverEndpoint returns the receiving service's endpoint.
func (m *GenericMessage) ReceiverEndpoint() string {
	return m.ReceiverEndpointField
}

// ReceiverVersion returns the requested receiving service's version.
func (m *GenericMessage) ReceiverVersion() string {
	return m.ReceiverVersionField
}

// SetReceiverVersion sets the requested receiving service's version.
func (m *GenericMessage) SetReceiverVersion(version string) {
	m.ReceiverVersionField = version
}

// Headers returns a map of header values associated with the message.
func (m *GenericMessage) Headers() map[string]string {
	return m.HeadersField
}

// SetHeader sets the content of a message header to the specified value.
// If the specified header already exists, its value will be overwritten
// by the new value.
//
// This function ensures that header names are always canonicalized by passing
// them through http.CanonicalHeaderKey.
func (m *GenericMessage) SetHeader(name, value string) {
	m.HeadersField[http.CanonicalHeaderKey(name)] = value
}

// SetHeaders sets the contents of a batch of headers. This is equivalent
// to iterating the map and calling SetHeader for each key/value.
func (m *GenericMessage) SetHeaders(values map[string]string) {
	for k, v := range values {
		m.HeadersField[http.CanonicalHeaderKey(k)] = v
	}
}

// Payload returns the payload associated with the message as a byte
// slice or an error if one is encoded in the message.
func (m *GenericMessage) Payload() ([]byte, error) {
	return m.PayloadField, m.ErrField
}

// SetPayload sets the content of this message. The content may be either
// a byte slice or an error message.
func (m *GenericMessage) SetPayload(payload []byte, err error) {
	m.PayloadField = payload
	m.ErrField = err
}

// MakeGenericMessage creates a new GenericMessage instance
func MakeGenericMessage() *GenericMessage {
	m := msgPool.Get().(*GenericMessage)
	m.IDField = GenerateID()
	m.HeadersField = make(map[string]string)
	m.PayloadField = nil
	m.ErrField = nil
	m.ReceiverVersionField = ""
	return m
}

// GenerateID generates a formatted V1 UUID that can be used as a message ID.
func GenerateID() string {
	return uuid.New().String()
}
