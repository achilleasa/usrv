package middleware

import (
	"context"
	"time"

	"github.com/achilleasa/usrv/server"
	"github.com/achilleasa/usrv/transport"
)

// MaxConcurrentRequests returns a middleware factory that limits the number
// of concurrent requests to all defined endpoints using a token-based system.
//
// Incoming requests will be blocked until a token can be acquired or the
// timeout expires in which case the response will be populated with a
// transport.ErrTimeout error.
//
// The token pool is shared between all endpoints that use this middleware. To
// apply a per-endpoint concurency limit, the MaxConcurrentRequestsPerEndpoint
// function should be used instead.
func MaxConcurrentRequests(maxConcurrent int, timeout time.Duration) server.MiddlewareFactory {
	tokens := genTokens(maxConcurrent)
	return func(next server.Middleware) server.Middleware {
		return maxConcurrentFn(
			tokens,
			timeout,
			next,
		)
	}
}

// MaxConcurrentRequests returns a middleware factory that limits the number
// of concurrent requests to each individual endpoint using a token-based system.
//
// Incoming requests will be blocked until a token can be acquired or the
// timeout expires in which case the response will be populated with a
// transport.ErrTimeout error.
//
// Each endpoint that uses this middleware gets assigned its own private token
// pool. To apply a shared concurency limit to multiple endpoints, the
// MaxConcurrentRequests function should be used instead.
func MaxConcurrentRequestsPerEndpoint(maxConcurrent int, timeout time.Duration) server.MiddlewareFactory {
	return func(next server.Middleware) server.Middleware {
		return maxConcurrentFn(
			genTokens(maxConcurrent),
			timeout,
			next,
		)
	}
}

// genTokens initializes a channel with a buffer size equal to count and inserts
// count entries into it.
func genTokens(count int) chan struct{} {
	tokens := make(chan struct{}, count)
	for i := 0; i < count; i++ {
		tokens <- struct{}{}
	}
	return tokens
}

// maxConcurrentFn implements the concurrent request limiter middleware.
func maxConcurrentFn(tokens chan struct{}, timeout time.Duration, next server.Middleware) server.Middleware {
	return server.MiddlewareFunc(func(ctx context.Context, req transport.ImmutableMessage, res transport.Message) {
		select {
		case token := <-tokens:
			// Make sure we return the token even if next.Handle panics
			defer func() { tokens <- token }()
			next.Handle(ctx, req, res)
			return
		case <-time.After(timeout):
		case <-ctx.Done():
		}

		// Timed out waiting for a token
		res.SetPayload(nil, transport.ErrTimeout)
	})
}
