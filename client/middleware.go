package client

import (
	"context"

	"github.com/achilleasa/usrv/transport"
)

// A MiddlewareFactory generates a Middleware instances.
type MiddlewareFactory func() Middleware

// Middleware is an interface implemented by objects that can be injected into
// a client's outgoing request flow.
//
// Pre is invoked before passing the request message to the transport. Calls to
// Pre may modify the outgoing request or the request context. In the latter case
// the updated context must be returned back from the call to Pre. If the call
// to Pre returns an error then the client will abort the request, execute
// Post hooks for middleware that has been already invoked and return the error
// back to the caller.
//
// Post is invoked after receiving a response from the remote endpoint.
//
// The client may opt to recycle the supplied request messages for handling future
// requests to reduce memory allocations. It is not valid to access the request
// message or modify the response message after or concurrently with the completion
// of the middleware handler.
type Middleware interface {
	Pre(ctx context.Context, req transport.Message) (context.Context, error)
	Post(ctx context.Context, req, res transport.ImmutableMessage)
}

var (
	globalMiddleware = []MiddlewareFactory{}
)

// RegisterGlobalMiddleware appends one or more MiddlewareFactory to the global
// set of middleware that is automatically executed by all RPC clients
func RegisterGlobalMiddleware(factories ...MiddlewareFactory) {
	for _, factory := range factories {
		if factory == nil {
			continue
		}
		globalMiddleware = append(globalMiddleware, factory)
	}
}

// ClearGlobalMiddleware clears the list of global middleware.
func ClearGlobalMiddleware() {
	globalMiddleware = []MiddlewareFactory{}
}
