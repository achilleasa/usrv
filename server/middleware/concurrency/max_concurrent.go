// Package concurrency provides a concurrency limiting middleware that uses
// a token pool to constraint the number of concurrent requests that can be
// handled by a particular endpoint.
//
// The middleware blocks incoming requests until a token can be acquired or
// a token acquisition timeout expires. In the latter case, requests will fail
// with transport.ErrTimeout
package concurrency

import (
	"context"
	"time"

	"github.com/achilleasa/usrv/server"
	"github.com/achilleasa/usrv/transport"
)

// SingletonFactory generates a concurrency limit middleware factory that always
// returns a singleton concurrency limiter middleware instances. The instance
// uses a token pool to limit the number of concurrent requests that can
// pass through it.
//
// Incoming requests will be blocked until a token can be acquired or the
// timeout expires in which case the response will be populated with a
// transport.ErrTimeout error.
//
// The token pool is shared between all endpoints that use this factory. To
// apply a per-endpoint concurency limit, the Factory function should be used
// instead.
func SingletonFactory(maxConcurrent int, timeout time.Duration) server.MiddlewareFactory {
	tokens := genTokens(maxConcurrent)
	return func(next server.Middleware) server.Middleware {
		return maxConcurrentFn(
			tokens,
			timeout,
			next,
		)
	}
}

// Factory generates a concurrency limit middleware factory that returns new
// concurrency limiter middleware instances. Each instance uses a token pool
// to limit the number of concurrent requests that can pass through it.
//
// Incoming requests will be blocked until a token can be acquired or the
// timeout expires in which case the response will be populated with a
// transport.ErrTimeout error.
//
// Each endpoint that uses this middleware gets assigned its own private token
// pool. To apply a shared concurency limit to multiple endpoints, the
// SingletonFactory function should be used instead.
func Factory(maxConcurrent int, timeout time.Duration) server.MiddlewareFactory {
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
