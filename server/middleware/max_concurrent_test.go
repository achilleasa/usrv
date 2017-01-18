package middleware

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/achilleasa/usrv/server"
	"github.com/achilleasa/usrv/transport"
	"github.com/achilleasa/usrv/transport/provider"
)

func TestMaxConcurrentRequests(t *testing.T) {
	type request struct{}
	type response struct{}

	sharedMiddleware := []server.MiddlewareFactory{
		MaxConcurrentRequests(1, 100*time.Millisecond),
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
			Middleware: sharedMiddleware,
		},
		&server.Endpoint{
			Name: "ep2",
			Handler: func(_ context.Context, _ *request, _ *response) error {
				return nil
			},
			Middleware: sharedMiddleware,
		},
		&server.Endpoint{
			Name: "ep3",
			Handler: func(_ context.Context, _ *request, _ *response) error {
				return nil
			},
			Middleware: append([]server.MiddlewareFactory{
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

func TestMaxConcurrentRequestsPerEndpoint(t *testing.T) {
	type request struct{}
	type response struct{}

	middleware := []server.MiddlewareFactory{
		MaxConcurrentRequestsPerEndpoint(1, 100*time.Millisecond),
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
			Middleware: middleware,
		},
		&server.Endpoint{
			Name: "ep2",
			Handler: func(_ context.Context, _ *request, _ *response) error {
				return nil
			},
			Middleware: middleware,
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
