package transport

import "io"

// ImmutableMessage defines an interface for a read-only message.
type ImmutableMessage interface {
	// ImmutableMessage implements io.Closer. All messages should be closed
	// when they are no longer being used.
	io.Closer

	// ID returns a UUID for this message.
	ID() string

	// Sender returns the name of the service that sent the mesage.
	Sender() string

	// SenderEndpoint returns the endpoint where the message originated.
	SenderEndpoint() string

	// Receiver returns the name of the service that will receive the message.
	Receiver() string

	// ReceiverEndpoint returns the endpoint where the message should be delivered at.
	ReceiverEndpoint() string

	// ReceiverVersion returns the requested receiving service's version.
	ReceiverVersion() string

	// Headers returns a map of header values associated with the message.
	Headers() map[string]string

	// Payload returns the payload associated with the message as a byte
	// slice or an error if one is encoded in the message.
	Payload() ([]byte, error)
}

// Message defines an interface for messages whose headers and payload can be
// modified by the sender. Message instances are typically used by clients to
// send RPC requests or by servers to respond to incoming messages.
type Message interface {
	ImmutableMessage

	// SetPayload sets the content of this message. The content may be either
	// a byte slice or an error message.
	SetPayload(payload []byte, err error)

	// SetHeader sets the content of a message header to the specified value.
	// If the specified header already exists, its value will be overwritten
	// by the new value.
	//
	// This method must ensure that header names are always canonicalized
	// using an algorithm similar to http.CanonicalHeaderKey.
	SetHeader(name, value string)

	// SetHeaders sets the contents of a batch of headers. This is equivalent
	// to iterating the map and calling SetHeader for each key/value.
	SetHeaders(headers map[string]string)

	// SetReceiverVersion sets the required version for the receiving service.
	SetReceiverVersion(string)
}
