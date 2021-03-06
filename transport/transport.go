package transport

// Mode describes the way that a transport is being used.
type Mode bool

// The modes that can be passed to calls to Dial() and Close().
const (
	ModeServer Mode = true
	ModeClient      = false
)

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
	// Dial connects to the transport using the specified dial mode. When
	// the dial mode is set to DialModeServer the transport will start
	// relaying messages to registered bindings.
	Dial(mode Mode) error

	// Close terminates a dialed connection using the specified dial mode.
	Close(mode Mode) error

	// Bind listens for messages send to a particular service and
	// endpoint combination and invokes the supplied handler to process them.
	//
	// Transports may implement versioning for bindings in order to support
	// complex deployment flows such as blue-green deployments.
	Bind(version, service, endpoint string, handler Handler) error

	// Unbind removes a message handler previously registered by a call to Bind().
	// Calling Unbind with a (version, service, endpoint) tuple that is not
	// registered has no effect.
	Unbind(version, service, endpoint string)

	// Request performs an RPC and returns back a read-only channel for
	// receiving the result.
	Request(msg Message) <-chan ImmutableMessage
}
