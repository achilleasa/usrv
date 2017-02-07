package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/achilleasa/usrv/encoding"
	"github.com/achilleasa/usrv/encoding/json"
	"github.com/achilleasa/usrv/transport"
	"github.com/achilleasa/usrv/transport/provider"
)

func TestOptionsThatReturnErrors(t *testing.T) {
	expError := errors.New("option error")
	opt := func(_ *Server) error { return expError }

	_, err := New("foo", opt)
	if err != expError {
		t.Fatalf("expected to get error %v; got %v", expError, err)
	}
}

func TestEndpointValidation(t *testing.T) {
	type request struct{}
	type response struct{}

	specs := []struct {
		handler  interface{}
		expError error
	}{
		{
			handler:  1337,
			expError: errEndpointHandlerBadSignature,
		},
		{
			handler:  func() {},
			expError: errEndpointHandlerBadSignature,
		},
		{
			handler:  func(_, _ interface{}) {},
			expError: errEndpointHandlerBadSignature,
		},
		{
			handler:  func(_, _, _ interface{}) ([]byte, error) { return nil, nil },
			expError: errEndpointHandlerBadSignature,
		},
		{
			handler:  func(_, _, _ interface{}) int { return 0 },
			expError: errEndpointHandlerBadSignature,
		},
		{
			handler:  func(_ int, _, _ interface{}) error { return nil },
			expError: errEndpointHandlerBadArg0,
		},
		{
			handler:  func(_ context.Context, _ request, _ *response) error { return nil },
			expError: errEndpointHandlerBadArg1,
		},
		{
			handler:  func(_ context.Context, _ *request, _ int) error { return nil },
			expError: errEndpointHandlerBadArg2,
		},
		{
			handler:  func(_ context.Context, _ *request, _ *response) error { return nil },
			expError: nil,
		},
	}

	srv, err := New("test")
	if err != nil {
		t.Fatal(err)
	}

	ep := &Endpoint{}
	err = srv.RegisterEndpoints(ep)
	if err != errEndpointHasNoName {
		t.Fatalf("expected error %q; got %v", errEndpointHasNoName.Error(), err)
	}

	ep.Name = "test"
	for specIndex, spec := range specs {
		ep.Handler = spec.handler
		err = srv.RegisterEndpoints(ep)

		if (spec.expError == nil && err != nil) || (spec.expError != nil && err != spec.expError) {
			t.Errorf("[spec %d] expected to get error %v; got %v", specIndex, spec.expError, err)
		}
	}
}

func TestEndpointHandler(t *testing.T) {
	log := []string{}
	genMiddleware := func(name string) MiddlewareFactory {
		return func(next Middleware) Middleware {
			return MiddlewareFunc(func(ctx context.Context, req transport.ImmutableMessage, res transport.Message) {
				log = append(log, fmt.Sprintf("enter %s", name))
				next.Handle(ctx, req, res)
				log = append(log, fmt.Sprintf("exit %s", name))
			})
		}
	}

	type request struct {
		Name string `json:"name"`
	}

	type response struct {
		Greeting string `json:"greeting"`
	}

	ClearGlobalMiddleware()
	RegisterGlobalMiddleware(
		nil,
		func(next Middleware) Middleware {
			return MiddlewareFunc(func(ctx context.Context, req transport.ImmutableMessage, res transport.Message) {
				log = append(log, "enter global middleware 1")
				next.Handle(ctx, req, res)
				log = append(log, "exit global middleware 1")
			})
		},
		func(next Middleware) Middleware {
			return MiddlewareFunc(func(ctx context.Context, req transport.ImmutableMessage, res transport.Message) {
				log = append(log, "enter global middleware 2")
				next.Handle(ctx, req, res)
				log = append(log, "exit global middleware 2")
			})
		},
	)
	expSender := "test"
	expEndpoint := "test endpoint"

	invocation := 0
	expError := "handler error"
	ep := &Endpoint{
		Name: expEndpoint,
		Handler: func(ctx context.Context, req *request, res *response) error {
			ctxVal := ctx.Value(CtxFieldServiceName).(string)
			if ctxVal != expSender {
				t.Errorf("expected server to inject service %q in req ctx; got %q", expSender, ctxVal)
			}
			ctxVal = ctx.Value(CtxFieldEndpointName).(string)
			if ctxVal != expEndpoint {
				t.Errorf("expected server to inject endpoint %q in req ctx; got %q", expEndpoint, ctxVal)
			}

			invocation++
			switch invocation {
			case 1:
				log = append(log, "call handler")
				res.Greeting = "hello " + req.Name
				return nil
			default:
				return errors.New(expError)
			}
		},
		Middleware: []MiddlewareFactory{
			genMiddleware("1"),
			nil,
			genMiddleware("2"),
			genMiddleware("3"),
		},
	}

	srv, err := New(expSender)
	if err != nil {
		t.Fatal(err)
	}

	codec := json.Codec()
	handler := srv.generateHandler(ep)

	reqObj := &request{Name: "tester"}
	resMessage := transport.MakeGenericMessage()
	reqMessage := transport.MakeGenericMessage()
	defer func() {
		reqMessage.Close()
		resMessage.Close()
	}()
	reqMessage.SetPayload(codec.Marshaler()(reqObj))
	handler.Process(reqMessage, resMessage)

	// Examine log
	expLog := []string{
		"enter global middleware 1",
		"enter global middleware 2",
		"enter 1",
		"enter 2",
		"enter 3",
		"call handler",
		"exit 3",
		"exit 2",
		"exit 1",
		"exit global middleware 2",
		"exit global middleware 1",
	}
	if !reflect.DeepEqual(log, expLog) {
		t.Fatalf("expected log entries to be:\n%v\n\ngot:\n%v", expLog, log)
	}

	// Examine response contents
	resData, err := resMessage.Payload()
	if err != nil {
		t.Fatal(err)
	}

	expData := []byte(`{"greeting":"hello tester"}`)
	if !reflect.DeepEqual(resData, expData) {
		t.Fatalf("expected encoded response data to be:\n%v\n\ngot:\n%v", expData, resData)
	}

	// Test error handling
	handler.Process(reqMessage, resMessage)
	_, err = resMessage.Payload()
	if err == nil || err.Error() != expError {
		t.Fatalf("expected error %q; got %v", expError, err)
	}
}

func TestMarshalingErrorHandling(t *testing.T) {
	type request struct{}
	type response struct{}

	ep := &Endpoint{
		Name:    "test",
		Handler: func(ctx context.Context, req *request, res *response) error { return nil },
	}

	codec := nopCodec()
	srv, err := New(
		"test",
		WithCodec(codec),
	)
	if err != nil {
		t.Fatal(err)
	}

	resMessage := transport.MakeGenericMessage()
	reqMessage := transport.MakeGenericMessage()
	defer func() {
		reqMessage.Close()
		resMessage.Close()
	}()

	// Test response marshaling error
	expError := "marshaler error"
	codec.marshalerFn = func(_ interface{}) ([]byte, error) {
		return nil, errors.New(expError)
	}

	srv.generateHandler(ep).Process(reqMessage, resMessage)
	_, err = resMessage.Payload()
	if err == nil || err.Error() != expError {
		t.Fatalf("expected error %q; got %v", expError, err)
	}

	// Test request unmarshaling error
	expError = "unmarshaler error"
	codec.unmarshalerFn = func(_ []byte, _ interface{}) error {
		return errors.New(expError)
	}

	srv.generateHandler(ep).Process(reqMessage, resMessage)
	_, err = resMessage.Payload()
	if err == nil || err.Error() != expError {
		t.Fatalf("expected error %q; got %v", expError, err)
	}
}

func TestPanicHandling(t *testing.T) {
	type request struct{}
	type response struct{}

	invocation := 0
	panicErr := errors.New("panic with error")
	panicStr := "panic with string"
	ep := &Endpoint{
		Name: "test",
		Handler: func(ctx context.Context, req *request, res *response) error {
			var msg interface{}
			invocation++
			switch invocation {
			case 1:
				msg = panicStr
			default:
				msg = panicErr
			}
			panic(msg)
		},
	}

	var wg sync.WaitGroup
	wg.Add(2)
	panicHandler := func(err error) {
		defer wg.Done()

		switch invocation {
		case 1:
			if err.Error() != panicStr {
				t.Fatalf("expected error to be %q; got %v", panicStr, err)
			}
		default:
			if err != panicErr {
				t.Fatalf("expected error to be %v; got %v", panicErr, err)
			}
		}
	}

	srv, err := New(
		"test",
		WithCodec(nopCodec()),
		WithPanicHandler(panicHandler),
	)
	if err != nil {
		t.Fatal(err)
	}

	handler := srv.generateHandler(ep)

	resMessage := transport.MakeGenericMessage()
	reqMessage := transport.MakeGenericMessage()
	defer func() {
		reqMessage.Close()
		resMessage.Close()
	}()

	handler.Process(reqMessage, resMessage)
	handler.Process(reqMessage, resMessage)

	_, err = resMessage.Payload()
	expErrMsg := "remote endpoint paniced: " + panicErr.Error()
	if err == nil || err.Error() != expErrMsg {
		t.Fatalf("expected to get error %q; got %v", expErrMsg, err)
	}

	wg.Wait()
}

func TestDefaultPanicHandler(t *testing.T) {
	var buf bytes.Buffer
	origWriter := DefaultPanicWriter
	defer func() {
		DefaultPanicWriter = origWriter
	}()
	DefaultPanicWriter = &buf

	expError := errors.New("some error")
	defaultPanicHandler(expError)

	expPrefix := fmt.Sprintf("recovered from panic: %v", expError)
	if !strings.HasPrefix(buf.String(), expPrefix) {
		t.Fatalf("expected panic handler output to contain %q", expPrefix)
	}
}

func TestServer(t *testing.T) {
	type request struct {
		Name string `json:"name"`
	}

	type response struct {
		Greeting string `json:"greeting"`
	}

	srv, err := New(
		"test",
		WithTransport(provider.NewInMemory()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	err = srv.RegisterEndpoints(
		&Endpoint{
			Name: "test",
			Handler: func(ctx context.Context, req *request, res *response) error {
				res.Greeting = "hello " + req.Name
				return nil
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := srv.Listen()
		if err != nil {
			t.Fatal(err)
		}
	}()

	<-time.After(100 * time.Millisecond)

	reqMessage := transport.MakeGenericMessage()
	defer func() {
		reqMessage.Close()
	}()
	reqMessage.ReceiverField = srv.serviceName
	reqMessage.ReceiverEndpointField = srv.endpoints[0].Name
	reqMessage.SetPayload([]byte(`{"name":"tester"}`), nil)

	resMessage := <-srv.transport.Request(reqMessage)

	// Examine response contents
	resData, err := resMessage.Payload()
	if err != nil {
		t.Fatal(err)
	}

	expData := []byte(`{"greeting":"hello tester"}`)
	if !reflect.DeepEqual(resData, expData) {
		t.Fatalf("expected encoded response data to be:\n%v\n\ngot:\n%v", expData, resData)
	}

	// Calling Listen a second time should return an error
	err = srv.Listen()
	if err != errServeAlreadyCalled {
		t.Fatalf("expected second call to Serve to return %v; got %v", errServeAlreadyCalled, err)
	}

	// Signal server to exit
	srv.Close()
	wg.Wait()
}

func TestListenErrors(t *testing.T) {
	type request struct {
	}

	type response struct {
	}

	srv, err := New(
		"test",
		WithTransport(provider.NewInMemory()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	// Transport.Bind() returns an error
	err = srv.RegisterEndpoints(
		&Endpoint{
			Name: "test",
			Handler: func(ctx context.Context, req *request, res *response) error {
				return nil
			},
		},
		&Endpoint{
			Name: "test",
			Handler: func(ctx context.Context, req *request, res *response) error {
				return nil
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	err = srv.Listen()
	expErrorMsg := `binding "test/test" already defined`
	if err == nil || (err != nil && err.Error() != expErrorMsg) {
		t.Fatalf("expected error %q; got %v", expErrorMsg, err)
	}

	// Transport.Dial() returns an error
	srv.transport = &testTransportThatFailsDialing{}
	srv.endpoints = []*Endpoint{}
	err = srv.Listen()
	expError := transport.ErrTransportAlreadyDialed
	if err != expError {
		t.Fatalf("expected error %q; got %v", expError, err)
	}
}

type testCodec struct {
	marshalerFn   encoding.Marshaler
	unmarshalerFn encoding.Unmarshaler
}

func (c *testCodec) Marshaler() encoding.Marshaler {
	return c.marshalerFn
}

func (c *testCodec) Unmarshaler() encoding.Unmarshaler {
	return c.unmarshalerFn
}

func nopCodec() *testCodec {
	return &testCodec{
		marshalerFn: func(_ interface{}) ([]byte, error) {
			return nil, nil
		},
		unmarshalerFn: func(_ []byte, _ interface{}) error {
			return nil
		},
	}
}

type testTransportThatFailsDialing struct {
}

func (tr *testTransportThatFailsDialing) Close() error {
	return nil
}

func (tr *testTransportThatFailsDialing) Request(_ transport.Message) <-chan transport.ImmutableMessage {
	return nil
}

func (tr *testTransportThatFailsDialing) Bind(_, _ string, _ transport.Handler) error {
	return nil
}

func (tr *testTransportThatFailsDialing) Dial() error {
	return transport.ErrTransportAlreadyDialed
}
