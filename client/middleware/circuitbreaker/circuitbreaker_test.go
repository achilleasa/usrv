package circuitbreaker

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/achilleasa/usrv/config"
	"github.com/achilleasa/usrv/config/store"
	"github.com/achilleasa/usrv/transport"
)

func TestFactory(t *testing.T) {
	f := Factory(&StaticConfig{})

	m1 := f("").(*circuitBreaker)
	m2 := f("").(*circuitBreaker)

	if m1 == m2 {
		t.Fatalf("expected Factory to return different instance")
	}

	ctx := context.Background()
	req := transport.MakeGenericMessage()
	res := transport.MakeGenericMessage()
	defer func() {
		req.Close()
		res.Close()
	}()

	res.SetPayload(nil, transport.ErrTimeout)

	// m1 middleware invocation should trip m1 circuit-breaker
	_, err := m1.Pre(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	m1.Post(ctx, req, res)

	// m2 middleware invocation should not fail due to a tripped circuit-breaker
	_, err = m2.Pre(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	res.SetPayload(nil, nil)
	m2.Post(ctx, req, res)

	if getState(m1) != Closed {
		t.Errorf("expected m1 circuit-breaker state to be Closed; got %d", getState(m1))
	}

	if getState(m2) != Open {
		t.Errorf("expected m2 circuit-breaker state to be Opened; got %d", getState(m2))
	}
}

func TestSingletonFactory(t *testing.T) {
	f := SingletonFactory(&StaticConfig{})

	m1 := f("").(*circuitBreaker)
	m2 := f("").(*circuitBreaker)

	if m1 != m2 {
		t.Fatalf("expected SingletonFactory to return the same instance")
	}

	ctx := context.Background()
	req := transport.MakeGenericMessage()
	res := transport.MakeGenericMessage()
	defer func() {
		req.Close()
		res.Close()
	}()

	res.SetPayload(nil, transport.ErrTimeout)

	// m1 middleware invocation should trip the shared circuit-breaker
	_, err := m1.Pre(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	m1.Post(ctx, req, res)

	// m2 middleware invocation should fail due to a tripped circuit-breaker
	_, err = m2.Pre(ctx, req)
	if err != transport.ErrServiceUnavailable {
		t.Fatalf("expected m2 call to Pre() to fail with ErrServiceUnavailable; got %v", err)
	}
}

func TestCircuitBreaker(t *testing.T) {
	stateChangeChan := make(chan State, 1)
	cb := newCiruitBreaker(&StaticConfig{
		TripThreshold:   2,
		ResetThreshold:  3,
		CoolOffPeriod:   500 * time.Millisecond,
		StateChangeChan: stateChangeChan,
	})

	ctx := context.Background()
	req := transport.MakeGenericMessage()
	res := transport.MakeGenericMessage()
	defer func() {
		req.Close()
		res.Close()
	}()

	specs := []struct {
		ResErr         error
		ExpErr         error
		Delay          time.Duration
		ExpState       State
		ExpStateChange State
	}{
		{
			ResErr:         nil,
			ExpErr:         nil,
			Delay:          time.Duration(0),
			ExpState:       Open,
			ExpStateChange: State(-1),
		},
		// First error should not trip the circuit-breaker
		{
			ResErr:         transport.ErrTimeout,
			ExpErr:         nil,
			Delay:          time.Duration(0),
			ExpState:       Open,
			ExpStateChange: State(-1),
		},
		// A non-tracked error should NOT trip the circuit-breaker
		{
			ResErr:         errors.New("something weird happened"),
			ExpErr:         nil,
			Delay:          time.Duration(0),
			ExpState:       Open,
			ExpStateChange: State(-1),
		},
		// Second error should trip the circuit-breaker
		{
			ResErr:         transport.ErrTimeout,
			ExpErr:         nil,
			Delay:          time.Duration(0),
			ExpState:       Closed,
			ExpStateChange: Closed,
		},
		// While in closed state any attempt before the coolOffPeriod should fail immediately
		{
			ResErr:         nil,
			ExpErr:         transport.ErrServiceUnavailable,
			Delay:          time.Duration(0),
			ExpState:       Closed,
			ExpStateChange: State(-1),
		},
		// A successful attempt after the coolOffPeriod should switch to half-open
		{
			ResErr:         nil,
			ExpErr:         nil,
			Delay:          time.Duration(510 * time.Millisecond),
			ExpState:       HalfOpen,
			ExpStateChange: HalfOpen,
		},
		// An unsuccessful attempt in half-open state should immediately trip to closed
		{
			ResErr:         transport.ErrTimeout,
			ExpErr:         nil,
			Delay:          time.Duration(0),
			ExpState:       Closed,
			ExpStateChange: Closed,
		},
		// A successful attempt after the coolOffPeriod should switch to half-open
		{
			ResErr:         nil,
			ExpErr:         nil,
			Delay:          time.Duration(510 * time.Millisecond),
			ExpState:       HalfOpen,
			ExpStateChange: HalfOpen,
		},
		// A second successful attempt while in the half-open state should not affect the state (but gets tallied)
		{
			ResErr:         nil,
			ExpErr:         nil,
			Delay:          time.Duration(0),
			ExpState:       HalfOpen,
			ExpStateChange: State(-1),
		},
		// A third successful attempt while in the half-open state should switch to open
		{
			ResErr:         nil,
			ExpErr:         nil,
			Delay:          time.Duration(0),
			ExpState:       Open,
			ExpStateChange: Open,
		},
	}

	for specIndex, spec := range specs {
		if spec.Delay != 0 {
			<-time.After(spec.Delay)
		}

		res.SetPayload(nil, spec.ResErr)
		_, err := cb.Pre(ctx, req)
		if err != spec.ExpErr {
			t.Errorf("[spec %d] expected Pre() to return error %v; got %v", specIndex, spec.ExpErr, err)
		}
		cb.Post(ctx, req, res)

		curState := getState(cb)
		if curState != spec.ExpState {
			t.Errorf("[spec %d] expected circuit-breaker state to be %d; got %d", specIndex, spec.ExpState, curState)
		}

		select {
		case newState := <-stateChangeChan:
			if spec.ExpStateChange == -1 || newState != spec.ExpStateChange {
				t.Errorf("[spec %d] circuit-breaker emitted state change event to state %d; expected %v", specIndex, newState, spec.ExpStateChange)
			}
		default:
			if spec.ExpStateChange != -1 {
				t.Errorf("[spec %d] expected circuit-breaker to emit state change event to state %d", specIndex, spec.ExpStateChange)
			}
		}
	}
}

func TestCircuitBreakerWithEventChanWithoutListener(t *testing.T) {
	cb := newCiruitBreaker(&StaticConfig{
		TripThreshold:   2,
		ResetThreshold:  3,
		CoolOffPeriod:   500 * time.Millisecond,
		StateChangeChan: make(chan State),
	})

	ctx := context.Background()
	req := transport.MakeGenericMessage()
	res := transport.MakeGenericMessage()
	defer func() {
		req.Close()
		res.Close()
	}()

	specs := []struct {
		ResErr   error
		ExpErr   error
		Delay    time.Duration
		ExpState State
	}{
		{
			ResErr:   nil,
			ExpErr:   nil,
			Delay:    time.Duration(0),
			ExpState: Open,
		},
		// First error should not trip the circuit-breaker
		{
			ResErr:   transport.ErrTimeout,
			ExpErr:   nil,
			Delay:    time.Duration(0),
			ExpState: Open,
		},
		// Second error should trip  the circuit-breaker
		{
			ResErr:   transport.ErrTimeout,
			ExpErr:   nil,
			Delay:    time.Duration(0),
			ExpState: Closed,
		},
		// While in closed state any attempt before the coolOffPeriod should fail immediately
		{
			ResErr:   nil,
			ExpErr:   transport.ErrServiceUnavailable,
			Delay:    time.Duration(0),
			ExpState: Closed,
		},
		// A successful attempt after the coolOffPeriod should switch to half-open
		{
			ResErr:   nil,
			ExpErr:   nil,
			Delay:    time.Duration(510 * time.Millisecond),
			ExpState: HalfOpen,
		},
		// An unsuccessful attempt in half-open state should immediately trip to closed
		{
			ResErr:   transport.ErrTimeout,
			ExpErr:   nil,
			Delay:    time.Duration(0),
			ExpState: Closed,
		},
		// A successful attempt after the coolOffPeriod should switch to half-open
		{
			ResErr:   nil,
			ExpErr:   nil,
			Delay:    time.Duration(510 * time.Millisecond),
			ExpState: HalfOpen,
		},
		// A second successful attempt while in the half-open state should not affect the state (but gets tallied)
		{
			ResErr:   nil,
			ExpErr:   nil,
			Delay:    time.Duration(0),
			ExpState: HalfOpen,
		},
		// A third successful attempt while in the half-open state should switch to open
		{
			ResErr:   nil,
			ExpErr:   nil,
			Delay:    time.Duration(0),
			ExpState: Open,
		},
	}

	for specIndex, spec := range specs {
		if spec.Delay != 0 {
			<-time.After(spec.Delay)
		}

		res.SetPayload(nil, spec.ResErr)
		_, err := cb.Pre(ctx, req)
		if err != spec.ExpErr {
			t.Errorf("[spec %d] expected Pre() to return error %v; got %v", specIndex, spec.ExpErr, err)
		}
		cb.Post(ctx, req, res)

		curState := getState(cb)
		if curState != spec.ExpState {
			t.Errorf("[spec %d] expected circuit-breaker state to be %d; got %d", specIndex, spec.ExpState, curState)
		}
	}
}

func TestCircuitBreakerWithDynamicConfig(t *testing.T) {
	var s store.Store
	configPath := "test/circuitbreaker"
	s.SetKeys(1, configPath, map[string]string{
		"trip_threshold":  "2",
		"reset_threshold": "3",
		"cool_off_period": fmt.Sprintf("%d", 500*time.Millisecond),
	})

	stateChangeChan := make(chan State, 1)
	cb := newCiruitBreaker(&DynamicConfig{
		store:           &s,
		ConfigPath:      configPath,
		StateChangeChan: stateChangeChan,
	})

	ctx := context.Background()
	req := transport.MakeGenericMessage()
	res := transport.MakeGenericMessage()
	defer func() {
		req.Close()
		res.Close()
	}()

	specs := []struct {
		ResErr         error
		ExpErr         error
		Delay          time.Duration
		ExpState       State
		ExpStateChange State
	}{
		{
			ResErr:         nil,
			ExpErr:         nil,
			Delay:          time.Duration(0),
			ExpState:       Open,
			ExpStateChange: State(-1),
		},
		// First error should not trip the circuit-breaker
		{
			ResErr:         transport.ErrTimeout,
			ExpErr:         nil,
			Delay:          time.Duration(0),
			ExpState:       Open,
			ExpStateChange: State(-1),
		},
		// A non-tracked error should NOT trip the circuit-breaker
		{
			ResErr:         errors.New("something weird happened"),
			ExpErr:         nil,
			Delay:          time.Duration(0),
			ExpState:       Open,
			ExpStateChange: State(-1),
		},
		// Second error should trip the circuit-breaker
		{
			ResErr:         transport.ErrTimeout,
			ExpErr:         nil,
			Delay:          time.Duration(0),
			ExpState:       Closed,
			ExpStateChange: Closed,
		},
		// While in closed state any attempt before the coolOffPeriod should fail immediately
		{
			ResErr:         nil,
			ExpErr:         transport.ErrServiceUnavailable,
			Delay:          time.Duration(0),
			ExpState:       Closed,
			ExpStateChange: State(-1),
		},
		// A successful attempt after the coolOffPeriod should switch to half-open
		{
			ResErr:         nil,
			ExpErr:         nil,
			Delay:          time.Duration(510 * time.Millisecond),
			ExpState:       HalfOpen,
			ExpStateChange: HalfOpen,
		},
		// An unsuccessful attempt in half-open state should immediately trip to closed
		{
			ResErr:         transport.ErrTimeout,
			ExpErr:         nil,
			Delay:          time.Duration(0),
			ExpState:       Closed,
			ExpStateChange: Closed,
		},
		// A successful attempt after the coolOffPeriod should switch to half-open
		{
			ResErr:         nil,
			ExpErr:         nil,
			Delay:          time.Duration(510 * time.Millisecond),
			ExpState:       HalfOpen,
			ExpStateChange: HalfOpen,
		},
		// A second successful attempt while in the half-open state should not affect the state (but gets tallied)
		{
			ResErr:         nil,
			ExpErr:         nil,
			Delay:          time.Duration(0),
			ExpState:       HalfOpen,
			ExpStateChange: State(-1),
		},
		// A third successful attempt while in the half-open state should switch to open
		{
			ResErr:         nil,
			ExpErr:         nil,
			Delay:          time.Duration(0),
			ExpState:       Open,
			ExpStateChange: Open,
		},
	}

	for specIndex, spec := range specs {
		if spec.Delay != 0 {
			<-time.After(spec.Delay)
		}

		res.SetPayload(nil, spec.ResErr)
		_, err := cb.Pre(ctx, req)
		if err != spec.ExpErr {
			t.Errorf("[spec %d] expected Pre() to return error %v; got %v", specIndex, spec.ExpErr, err)
		}
		cb.Post(ctx, req, res)

		curState := getState(cb)
		if curState != spec.ExpState {
			t.Errorf("[spec %d] expected circuit-breaker state to be %d; got %d", specIndex, spec.ExpState, curState)
		}

		select {
		case newState := <-stateChangeChan:
			if spec.ExpStateChange == -1 || newState != spec.ExpStateChange {
				t.Errorf("[spec %d] circuit-breaker emitted state change event to state %d; expected %v", specIndex, newState, spec.ExpStateChange)
			}
		default:
			if spec.ExpStateChange != -1 {
				t.Errorf("[spec %d] expected circuit-breaker to emit state change event to state %d", specIndex, spec.ExpStateChange)
			}
		}
	}
}

func TestDynamicConfig(t *testing.T) {
	c := &DynamicConfig{}

	expStore := &config.Store
	s := c.getStore()
	if s != expStore {
		t.Errorf("expected getStore() to return the global config store instance (&config.Store)")
	}

	expStore = &store.Store{}
	c.store = expStore
	s = c.getStore()
	if s != expStore {
		t.Errorf("expected getStore() to return the custom store instance")
	}

	expPath := "/foo/bar"
	c.ConfigPath = "/foo"
	path := c.configPath("bar")
	if path != expPath {
		t.Errorf("expected configPath() to return %q; got %q", expPath, path)
	}

	c.ConfigPath = "/foo/"
	path = c.configPath("bar")
	if path != expPath {
		t.Errorf("expected configPath() to return %q; got %q", expPath, path)
	}
}

func getState(cb *circuitBreaker) State {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	return cb.curState
}
