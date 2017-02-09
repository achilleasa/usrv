// Package circuitbreaker provides middlewares that implement the circuit-breaker
// pattern as described in https://martinfowler.com/bliki/CircuitBreaker.html.
//
// The circuit-breaker is implemented as a state-machine with three possible
// states:
//
// - Open. In this state the circuit-breaker forwards requests to the remote endpoint
//   while also tracking request errors. If the number of errors exceeds TripThreshold,
//   the circuit-breaker enters the Closed state.
//
// - Closed. While in this state, all remote requests automatically fail with an error
//   without forwarding any requerst to the remote endpoint. When entering this state,
//   the circuit breaker starts a timer that switches the circuit-breaker to the
//   Half-Open state.
//
//  - Half-Open. While in this state, the circuit-breaker will try making remote
//    requests to see if the remote endpoint issue has been resolved. If a number
//    of requests can be performed without an error, the circuit-breaker switches
//    to the Open state. If any of the test requests fails, the circuit-breaker
//    will switch back to the Closed state.
//
// Circuit-breakers can be configured either using a static configuration or a
// fully dynamic configuration built on top of the flags package.
package circuitbreaker

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/achilleasa/usrv/client"
	"github.com/achilleasa/usrv/config/flag"
	"github.com/achilleasa/usrv/transport"
)

// State represents the circuit-breaker state.
type State int8

// The possible circuit-breaker states.
const (
	Open State = iota
	Closed
	HalfOpen
)

// SingletonFactory generates a circuit-breaker middleware factory that always
// returns a singleton circuit-breaker instance using the supplied configuration.
func SingletonFactory(cfg Config) client.MiddlewareFactory {
	cb := newCiruitBreaker(cfg)
	return func() client.Middleware {
		return cb
	}
}

// Factory generates a circuit-breaker middleware factory that returns new
// circuit-breaker instances using the supplied configuration.
func Factory(cfg Config) client.MiddlewareFactory {
	return func() client.Middleware {
		return newCiruitBreaker(cfg)
	}
}

// Ensure circuitBreaker implements client.Middleware
var _ client.Middleware = (*circuitBreaker)(nil)

type circuitBreaker struct {
	closedError     error
	tripErrors      []error
	tripThreshold   *flag.Uint32
	resetThreshold  *flag.Uint32
	coolOffPeriod   *flag.Int64
	stateChangeChan chan<- State

	mutex            sync.Mutex
	curState         State
	trippedAt        time.Time
	trackedErrors    uint32
	trackedSuccesses uint32
}

// newCiruitBreaker creates a new circuitBreaker instance.
func newCiruitBreaker(cfg Config) *circuitBreaker {
	cb := &circuitBreaker{
		tripErrors:      cfg.GetTripErrors(),
		closedError:     cfg.GetClosedError(),
		tripThreshold:   cfg.GetTripThreshold(),
		resetThreshold:  cfg.GetResetThreshold(),
		coolOffPeriod:   cfg.GetCoolOffPeriod(),
		stateChangeChan: cfg.GetStateChangeChan(),
	}

	if cb.coolOffPeriod.Get() == 0 {
		cb.coolOffPeriod.Set(DefaultCoolOffPeriod.Nanoseconds())
	}

	if cb.closedError == nil {
		cb.closedError = transport.ErrServiceUnavailable
	}

	if cb.tripErrors == nil {
		cb.tripErrors = DefaultTripErrors
	}

	return cb
}

// Pre implements a pre-hook as part of the client Middleware interface.
func (cb *circuitBreaker) Pre(ctx context.Context, req transport.Message) (context.Context, error) {
	cb.mutex.Lock()
	if cb.curState == Closed {
		// We cannot transition to the half-open state
		if time.Since(cb.trippedAt) < time.Duration(cb.coolOffPeriod.Get()) {
			cb.mutex.Unlock()
			return ctx, cb.closedError
		}

		// Transition to half-open
		cb.curState = HalfOpen
		cb.trackedSuccesses = 0
		if cb.stateChangeChan != nil {
			select {
			case cb.stateChangeChan <- cb.curState:
			default:
			}
		}
	}
	cb.mutex.Unlock()

	// Allow the call to proceed
	return ctx, nil
}

// Post implements a post-hook as part of the client Middleware interface.
func (cb *circuitBreaker) Post(ctx context.Context, req, res transport.ImmutableMessage) {
	_, err := res.Payload()

	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	switch {
	case cb.curState == Open && cb.trackError(err):
		cb.trackedErrors++

		// Check if we can transition to closed state
		if cb.trackedErrors < cb.tripThreshold.Get() {
			return
		}

		// Transition to closed state
		cb.curState = Closed
		cb.trippedAt = time.Now()
	case cb.curState == HalfOpen && err != nil:
		// Immediately transition back to closed
		cb.curState = Closed
		cb.trippedAt = time.Now()
	case cb.curState == HalfOpen && err == nil:
		cb.trackedSuccesses++

		// Check if we can transition back to open sate
		if cb.trackedSuccesses < cb.resetThreshold.Get() {
			return
		}

		cb.curState = Open
		cb.trackedErrors = 0
	default:
		return
	}

	if cb.stateChangeChan != nil {
		select {
		case cb.stateChangeChan <- cb.curState:
		default:
		}
	}
}

// trackError returns true if err should bump the tracked errors counter.
func (cb *circuitBreaker) trackError(err error) bool {
	if err == nil {
		return false
	}

	for _, tripErr := range cb.tripErrors {
		if err == tripErr || strings.Contains(err.Error(), tripErr.Error()) {
			return true
		}
	}
	return false

}
