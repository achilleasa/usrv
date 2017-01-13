package server

import (
	"context"

	"github.com/achilleasa/usrv/transport"
)

// A MiddlewareFactory generates a Middleware which wraps another middleware
// forming a middleware chain.
//
// After applying its logic the returned middleware instance is expected to invoke
// the next middleware in the chain before returning. The middleware can also
// opt to return without invoking next, which will  prevent the rest of the chain from
// executing.
type MiddlewareFactory func(next Middleware) Middleware

// Middleware is an interface implemented by objects that can be injected into
// incoming requests' handling flow before the registered endpoint handler
// is invoked. While executing, middlewares may freely modify the context and/or
// the response message.
//
// The server may opt to recycle the supplied request messages for handling future
// requests to reduce memory allocations. It is not valid to access the request
// message or modify the response message after or concurrently with the completion
// of the middleware handler.
//
// If the middleware panics, the server (the caller of HandleRequest) assumes that
// the effect of the panic was isolated to the active request. The panic will
// be recovered and handled by the server's panic handler.
type Middleware interface {
	Handle(ctx context.Context, req transport.ImmutableMessage, res transport.Message)
}

// The MiddlewareFunc type is an adapter to allow the use of ordinary functions
// as middlewares. If f is a function with the appropriate signature, MiddlwareFunc(f)
// is a Middleware that calls f.
type MiddlewareFunc func(ctx context.Context, req transport.ImmutableMessage, res transport.Message)

// Handle calls f(ctx, req, res).
func (f MiddlewareFunc) Handle(ctx context.Context, req transport.ImmutableMessage, res transport.Message) {
	f(ctx, req, res)
}

var (
	globalMiddleware = []MiddlewareFactory{}
)

// RegisterGlobalMiddleware appends one or more MiddlewareFactory to the global
// set of middleware that is automatically prepended to all defined server endpoints.
func RegisterGlobalMiddleware(factories ...MiddlewareFactory) {
	globalMiddleware = append(globalMiddleware, factories...)
}

// ClearGlobalMiddleware clears the list of global middleware.
func ClearGlobalMiddleware() {
	globalMiddleware = []MiddlewareFactory{}
}
