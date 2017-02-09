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
	"runtime"
	"strings"
	"time"

	"github.com/achilleasa/usrv/config"
	"github.com/achilleasa/usrv/config/flag"
	"github.com/achilleasa/usrv/config/store"
	"github.com/achilleasa/usrv/server"
	"github.com/achilleasa/usrv/transport"
)

// A set of endpoints that can be hooked by tests
var (
	setFinalizer = runtime.SetFinalizer
)

// Config is an interface implemented by objects that can be passed to the
// concurrency-limiter factories.
//
// GetMaxConcurrent returns the max number of concurrent requests
//
// GetTimeout returns the number of nanoseconds that a request can spend waiting
// to acquire a token before timing out.
type Config interface {
	GetMaxConcurrent() *flag.Int32
	GetTimeout() *flag.Int64
}

// StaticConfig defines a static concurrency-limiter configuration.
type StaticConfig struct {
	MaxConcurrent int
	Timeout       time.Duration
}

// GetMaxConcurrent returns the max number of concurrent requests
func (c *StaticConfig) GetMaxConcurrent() *flag.Int32 {
	f := flag.NewInt32(nil, "")
	f.Set(int32(c.MaxConcurrent))
	return f
}

// GetTimeout returns the number of nanoseconds that a request can spend waiting
// to acquire a token before timing out.
func (c *StaticConfig) GetTimeout() *flag.Int64 {
	f := flag.NewInt64(nil, "")
	f.Set(c.Timeout.Nanoseconds())
	return f
}

// DynamicConfig defines a concurrency-limiter configuration which is synced to a configuration store.
type DynamicConfig struct {
	// The config store to use. If undefined it defaults to the global shared store.
	store *store.Store

	// The configuration store prefix for fetching the concurrency-limiter
	// configuration options. This prefix is prepended to the following keys:
	//  - max_concurrent
	//  - timeout
	ConfigPath string
}

// GetMaxConcurrent returns the max number of concurrent requests
func (c *DynamicConfig) GetMaxConcurrent() *flag.Int32 {
	return flag.NewInt32(c.getStore(), c.configPath("max_concurrent"))
}

// GetTimeout returns the number of nanoseconds that a request can spend waiting
// to acquire a token before timing out.
func (c *DynamicConfig) GetTimeout() *flag.Int64 {
	return flag.NewInt64(c.getStore(), c.configPath("timeout"))
}

// getStore returns the config store instance to use for fetching the dynaimc configuration.
func (c *DynamicConfig) getStore() *store.Store {
	if c.store == nil {
		return &config.Store
	}

	return c.store
}

// configPath returns a config path for the given key by concatenating the
// ConfigPath field value with key.
func (c *DynamicConfig) configPath(key string) string {
	return strings.TrimSuffix(c.ConfigPath, "/") + "/" + key
}

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
func SingletonFactory(cfg Config) server.MiddlewareFactory {
	tokenPool := newResizableTokenPool(cfg.GetMaxConcurrent())
	return func(next server.Middleware) server.Middleware {
		return maxConcurrentFn(
			tokenPool,
			cfg.GetTimeout(),
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
func Factory(cfg Config) server.MiddlewareFactory {
	return func(next server.Middleware) server.Middleware {
		return maxConcurrentFn(
			newResizableTokenPool(cfg.GetMaxConcurrent()),
			cfg.GetTimeout(),
			next,
		)
	}
}

// maxConcurrentFn implements the concurrent request limiter middleware.
func maxConcurrentFn(tokenPool *resizableTokenPool, timeout *flag.Int64, next server.Middleware) server.Middleware {
	return server.MiddlewareFunc(func(ctx context.Context, req transport.ImmutableMessage, res transport.Message) {
		select {
		case token, ok := <-tokenPool.AcquireChan():
			// Make sure we return the token even if next.Handle panics
			if ok {
				defer func() { tokenPool.ReleaseChan() <- token }()
			}
			next.Handle(ctx, req, res)
			return
		case <-time.After(time.Duration(timeout.Get())):
		case <-ctx.Done():
		}

		// Timed out waiting for a token
		res.SetPayload(nil, transport.ErrTimeout)
	})
}

// resizableTokenPool implements a token pool that can be dynamically resized.
type resizableTokenPool struct {
	maxTokens *flag.Int32

	doneChan    chan struct{}
	acquireChan chan struct{}
	releaseChan chan struct{}
}

// newResizableTokenPool returns a resizable token pool initialized with maxTokens.
func newResizableTokenPool(maxTokens *flag.Int32) *resizableTokenPool {
	pool := &resizableTokenPool{
		maxTokens:   maxTokens,
		doneChan:    make(chan struct{}, 0),
		acquireChan: make(chan struct{}, 0),
		releaseChan: make(chan struct{}, 0),
	}
	pool.spawnWorker()
	setFinalizer(pool, func(p *resizableTokenPool) { p.Close() })
	return pool
}

// ReleaseChan returns a write-only channel for returning an acquired token back to the pool.
func (p *resizableTokenPool) ReleaseChan() chan<- struct{} {
	return p.releaseChan
}

// AcquireChan returns a read-only channel for acquiring a token from the pool.
func (p *resizableTokenPool) AcquireChan() <-chan struct{} {
	return p.acquireChan
}

// Close shuts down the pool worker.
func (p *resizableTokenPool) Close() {
	close(p.doneChan)
}

// worker handles token acquisitions and releases.
func (p *resizableTokenPool) spawnWorker() {
	go func(acquireChan, releaseChan, doneChan chan struct{}) {
		defer func() {
			close(acquireChan)
			close(releaseChan)
		}()

		// Latch onto the current maxTokens value
		latchedMaxTokens := p.maxTokens.Get()
		availTokens := latchedMaxTokens

		for {
			// Check whether maxTokens has changed; if so resize the pool
			newMaxTokens := p.maxTokens.Get()
			if latchedMaxTokens != newMaxTokens {
				availTokens += newMaxTokens - latchedMaxTokens
				if availTokens < 0 {
					availTokens = 0
				}
				latchedMaxTokens = newMaxTokens
			}

			// If the pool contains any tokens, handle both acquisition and release requests
			if availTokens > 0 {
				select {
				case acquireChan <- struct{}{}:
					availTokens--
				case <-releaseChan:
					// Make sure we don't overflow our token pool size.
					if availTokens < latchedMaxTokens {
						availTokens++
					}
				case <-p.maxTokens.ChangeChan():
				case <-doneChan:
					return
				}
				continue
			}

			// Wait until a token is released or we need to exit
			select {
			case <-doneChan:
				return
			case <-p.maxTokens.ChangeChan():
			case <-releaseChan:
				// Make sure we don't overflow our token pool size.
				if availTokens < latchedMaxTokens {
					availTokens++
				}
			}
		}
	}(p.acquireChan, p.releaseChan, p.doneChan)
}
