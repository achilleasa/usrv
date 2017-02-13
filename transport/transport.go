package transport

import "io"

// A Handler responds to an RPC request.
//
// Process is invoked to handle an incoming request. The handler should process
// the request and must update the response message with either the response
// payload or an error.
type Handler interface {
	Process(req ImmutableMessage, res Message)
}

// The HandlerFunc type is an adapter to allow the use of
// ordinary functions as RPC handlers. If f is a function
// with the appropriate signature, HandlerFunc(f) is a
// Handler that calls f.
type HandlerFunc func(req ImmutableMessage, res Message)

// Process calls f(req, res).
func (f HandlerFunc) Process(req ImmutableMessage, res Message) {
	f(req, res)
}

// Provider defines an interface implemented by transports that can be used
// by usrv. The transport layer is responsible for the exchange of encoded
// messages between RPC clients and servers.
type Provider interface {
	// All transports must implement io.Closer to clean up and shutdown.
	io.Closer

	// Dial connects the transport, establishes any declared bindings and
	// starts relaying messages.
	Dial() error

	// Bind listens for messages send to a particular service and
	// endpoint combination and invokes the supplied handler to process them.
	//
	// Transports may implement versioning for bindings in order to support
	// complex deployment flows such as blue-green deployments.
	//
	// Bindings can only be established on a closed transport. Calls to Bind
	// after a call to Dial will result in an error.
	Bind(version, service, endpoint string, handler Handler) error

	// Request performs an RPC and returns back a read-only channel for
	// receiving the result.
	Request(msg Message) <-chan ImmutableMessage
}
