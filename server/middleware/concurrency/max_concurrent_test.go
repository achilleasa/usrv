package concurrency

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/achilleasa/usrv/config"
	"github.com/achilleasa/usrv/config/flag"
	"github.com/achilleasa/usrv/config/store"
	"github.com/achilleasa/usrv/server"
	"github.com/achilleasa/usrv/transport"
	"github.com/achilleasa/usrv/transport/provider"
)

func TestSingletonFactory(t *testing.T) {
	type request struct{}
	type response struct{}

	sharedMiddleware := []server.MiddlewareFactory{
		SingletonFactory(&StaticConfig{1, 100 * time.Millisecond}),
	}

	tr := provider.NewInMemory()
	defer tr.Close()
	srv, err := server.New(
		"test",
		server.WithTransport(tr),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	workStartChan := make(chan struct{}, 0)
	workDoneChan := make(chan struct{}, 0)
	err = srv.RegisterEndpoints(
		&server.Endpoint{
			Name: "ep1",
			Handler: func(_ context.Context, _ *request, _ *response) error {
				close(workStartChan)
				<-workDoneChan
				return nil
			},
			MiddlewareFactories: sharedMiddleware,
		},
		&server.Endpoint{
			Name: "ep2",
			Handler: func(_ context.Context, _ *request, _ *response) error {
				return nil
			},
			MiddlewareFactories: sharedMiddleware,
		},
		&server.Endpoint{
			Name: "ep3",
			Handler: func(_ context.Context, _ *request, _ *response) error {
				return nil
			},
			MiddlewareFactories: append([]server.MiddlewareFactory{
				func(next server.Middleware) server.Middleware {
					return server.MiddlewareFunc(func(ctx context.Context, req transport.ImmutableMessage, res transport.Message) {
						ctx, _ = context.WithTimeout(ctx, 50*time.Millisecond)
						next.Handle(ctx, req, res)
					})
				},
			}, sharedMiddleware...),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	go srv.Listen()
	<-time.After(100 * time.Millisecond)

	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()

		reqMessage := transport.MakeGenericMessage()
		defer reqMessage.Close()

		reqMessage.PayloadField = []byte("{}")
		reqMessage.ReceiverField = "test"
		reqMessage.ReceiverEndpointField = "ep1"

		resMessage := <-tr.Request(reqMessage)
		defer resMessage.Close()

		_, err := resMessage.Payload()
		if err != nil {
			t.Fatal(err)
		}
	}()

	// This request will fail as a token cannot be acquired within 100ms
	go func() {
		defer wg.Done()

		reqMessage := transport.MakeGenericMessage()
		defer reqMessage.Close()

		reqMessage.PayloadField = []byte("{}")
		reqMessage.ReceiverField = "test"
		reqMessage.ReceiverEndpointField = "ep2"

		// Wait for ep1 to enter handler
		<-workStartChan

		resMessage := <-tr.Request(reqMessage)
		defer resMessage.Close()

		_, err := resMessage.Payload()
		if err != transport.ErrTimeout {
			t.Fatalf("expected to get transport.ErrTimeout; got %v", err)
		}

		// Let ep1 finish its work
		close(workDoneChan)
		<-time.After(100 * time.Millisecond)

		// Retry request to ep2; this time it should work
		resMessage = <-tr.Request(reqMessage)
		defer resMessage.Close()

		_, err = resMessage.Payload()
		if err != nil {
			t.Fatal(err)
		}
	}()

	// This request will fail as a token cannot be acquired before the ctx deadline expires
	go func() {
		defer wg.Done()

		reqMessage := transport.MakeGenericMessage()
		defer reqMessage.Close()

		reqMessage.PayloadField = []byte("{}")
		reqMessage.ReceiverField = "test"
		reqMessage.ReceiverEndpointField = "ep3"

		// Wait for ep1 to enter handler
		<-workStartChan

		resMessage := <-tr.Request(reqMessage)
		defer resMessage.Close()

		_, err := resMessage.Payload()
		if err != transport.ErrTimeout {
			t.Fatalf("expected to get transport.ErrTimeout; got %v", err)
		}
	}()

	wg.Wait()
}

func TestFactory(t *testing.T) {
	type request struct{}
	type response struct{}

	middleware := []server.MiddlewareFactory{
		Factory(&StaticConfig{1, 100 * time.Millisecond}),
	}

	tr := provider.NewInMemory()
	defer tr.Close()
	srv, err := server.New(
		"test",
		server.WithTransport(tr),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	workStartChan := make(chan struct{}, 0)
	workDoneChan := make(chan struct{}, 0)
	err = srv.RegisterEndpoints(
		&server.Endpoint{
			Name: "ep1",
			Handler: func(_ context.Context, _ *request, _ *response) error {
				close(workStartChan)
				<-workDoneChan
				return nil
			},
			MiddlewareFactories: middleware,
		},
		&server.Endpoint{
			Name: "ep2",
			Handler: func(_ context.Context, _ *request, _ *response) error {
				return nil
			},
			MiddlewareFactories: middleware,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	go srv.Listen()
	<-time.After(100 * time.Millisecond)

	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()

		reqMessage := transport.MakeGenericMessage()
		defer reqMessage.Close()

		reqMessage.PayloadField = []byte("{}")
		reqMessage.ReceiverField = "test"
		reqMessage.ReceiverEndpointField = "ep1"

		resMessage := <-tr.Request(reqMessage)
		defer resMessage.Close()

		_, err := resMessage.Payload()
		if err != nil {
			t.Fatal(err)
		}
	}()

	// This request will fail as a token for ep1 cannot be acquired within 100ms
	go func() {
		defer wg.Done()

		reqMessage := transport.MakeGenericMessage()
		defer reqMessage.Close()

		reqMessage.PayloadField = []byte("{}")
		reqMessage.ReceiverField = "test"
		reqMessage.ReceiverEndpointField = "ep1"

		// Wait for ep1 to enter handler
		<-workStartChan

		resMessage := <-tr.Request(reqMessage)
		defer resMessage.Close()

		_, err := resMessage.Payload()
		if err != transport.ErrTimeout {
			t.Fatalf("expected to get transport.ErrTimeout; got %v", err)
		}

		// Let ep1 finish its work
		close(workDoneChan)
	}()

	// This request will succeed as ep2 uses its own private token pool
	go func() {
		defer wg.Done()

		reqMessage := transport.MakeGenericMessage()
		defer reqMessage.Close()

		reqMessage.PayloadField = []byte("{}")
		reqMessage.ReceiverField = "test"
		reqMessage.ReceiverEndpointField = "ep2"

		// Wait for ep1 to enter handler
		<-workStartChan

		resMessage := <-tr.Request(reqMessage)
		defer resMessage.Close()

		_, err := resMessage.Payload()
		if err != nil {
			t.Fatal(err)
		}
	}()

	wg.Wait()
}

func TestSingletonFactoryWithDynamicConfiguration(t *testing.T) {
	var s store.Store

	configPath := "test/concurrency"
	s.SetKeys(1, configPath, map[string]string{
		"max_concurrent": "2",
		"timeout":        fmt.Sprintf("%d", 100*time.Millisecond),
	})

	ctx := context.Background()
	req := transport.MakeGenericMessage()
	res := transport.MakeGenericMessage()
	defer func() {
		req.Close()
		res.Close()
	}()

	var wg sync.WaitGroup
	wg.Add(2)

	syncChan := make(chan struct{}, 0)
	nextFn := server.MiddlewareFunc(func(_ context.Context, req transport.ImmutableMessage, res transport.Message) {
		wg.Done()
		<-syncChan
	})

	f := SingletonFactory(&DynamicConfig{store: &s, ConfigPath: configPath})
	m1 := f(nextFn)
	m2 := f(server.MiddlewareFunc(func(_ context.Context, req transport.ImmutableMessage, res transport.Message) {}))

	// Let one request go through
	okRes := transport.MakeGenericMessage()
	defer okRes.Close()
	m2.Handle(ctx, req, okRes)

	if _, err := okRes.Payload(); err != nil {
		t.Fatalf("expected call to succeed; got %v", err)
	}

	// Consume all tokens and block
	go m1.Handle(ctx, req, res)
	go m1.Handle(ctx, req, res)

	wg.Wait()

	// Next middleware call should fail with a timeout error
	failRes := transport.MakeGenericMessage()
	defer failRes.Close()
	m1.Handle(ctx, req, failRes)

	if _, err := failRes.Payload(); err != transport.ErrTimeout {
		t.Fatalf("expected call to fail with ErrTimeout; got %v", err)
	}

	// Add more tokens to the pool
	s.SetKey(2, configPath+"/max_concurrent", "3")
	okRes = transport.MakeGenericMessage()
	defer okRes.Close()
	m2.Handle(ctx, req, okRes)

	if _, err := okRes.Payload(); err != nil {
		t.Fatalf("expected call to succeed; got %v", err)
	}

	// Pool has 1/3 tokens available. Downsize pool size to 2 tokens and
	// let one of the blocked requests proceed. The pool should now have
	// 2 tokens available (1 old token still held by the 2nd blocked call) so
	// the next call should also succeed
	s.SetKey(3, configPath+"/max_concurrent", "2")
	<-time.After(500 * time.Millisecond)
	syncChan <- struct{}{}
	<-time.After(500 * time.Millisecond)

	okRes = transport.MakeGenericMessage()
	defer okRes.Close()
	m2.Handle(ctx, req, failRes)

	if _, err := okRes.Payload(); err != nil {
		t.Fatalf("expected call to succeed; got %v", err)
	}

	// Pool has 2/2 tokens available. Downsize pool size to 0 tokens and
	// let the second blocked request proceed. The pool should now have
	// 9 tokens available so the next call should fail
	s.SetKey(4, configPath+"/max_concurrent", "0")
	<-time.After(500 * time.Millisecond)
	close(syncChan)
	<-time.After(500 * time.Millisecond)

	failRes = transport.MakeGenericMessage()
	defer failRes.Close()
	m2.Handle(ctx, req, failRes)

	if _, err := failRes.Payload(); err != transport.ErrTimeout {
		t.Fatalf("expected call to fail with ErrTimeout; got %v", err)
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

func TestPoolWorkerCleanup(t *testing.T) {
	origSetFinalizer := setFinalizer
	defer func() {
		setFinalizer = origSetFinalizer
	}()
	var finalizer func(*resizableTokenPool)
	setFinalizer = func(_ interface{}, cb interface{}) {
		finalizer = cb.(func(*resizableTokenPool))
	}

	// Create a pool with available tokens
	f := flag.NewInt32(nil, "")
	f.Set(1)
	p := newResizableTokenPool(f)

	finalizer(p)
	<-time.After(500 * time.Millisecond)

	// Expect worker to close the token channels
	if _, ok := <-p.AcquireChan(); ok {
		t.Fatal("expected that the pool finalizer would close the pool channels")
	}

	// Create a pool without available tokens
	f.Set(0)
	p = newResizableTokenPool(f)

	finalizer(p)
	<-time.After(500 * time.Millisecond)

	// Expect worker to close the token channels
	if _, ok := <-p.AcquireChan(); ok {
		t.Fatal("expected that the pool finalizer would close the pool channels")
	}
}
