package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/achilleasa/usrv/encoding"
	"github.com/achilleasa/usrv/server"
	"github.com/achilleasa/usrv/transport"
	"github.com/achilleasa/usrv/transport/memory"
)

func TestClientOptionError(t *testing.T) {
	expError := errors.New("option error")
	_, err := New("foo", func(_ *Client) error { return expError })
	if err != expError {
		t.Fatalf("expected to get error %v; got %v", expError, err)
	}
}

func TestClientRequest(t *testing.T) {
	tr := memory.New()
	err := tr.Dial(transport.ModeClient)
	if err != nil {
		t.Fatal(err)
	}
	err = tr.Dial(transport.ModeServer)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		tr.Close(transport.ModeServer)
		tr.Close(transport.ModeClient)
	}()

	expGreeting := "hello tester"
	expReqPayload := `{"name":"tester"}`
	expResPayload := `{"greeting":"` + expGreeting + `"}`
	tr.Bind("", "service", "endpoint", transport.HandlerFunc(
		func(req transport.ImmutableMessage, res transport.Message) {
			payload, _ := req.Payload()
			payloadStr := string(payload)
			if payloadStr != expReqPayload {
				t.Fatalf("expected request payload to be:\n%s\n\ngot:\n%s", expReqPayload, payloadStr)
			}

			res.SetPayload([]byte(expResPayload), nil)
			<-time.After(100 * time.Millisecond)
		}),
	)

	c, err := New(
		"service",
		WithTransport(tr),
	)
	if err != nil {
		t.Fatal(err)
	}

	type request struct {
		Name string `json:"name"`
	}

	type response struct {
		Greeting string `json:"greeting"`
	}

	reqObj := &request{Name: "tester"}
	resObj := &response{}

	// Normal request
	err = c.Request(context.Background(), "endpoint", reqObj, resObj)
	if err != nil {
		t.Fatal(err)
	}

	if resObj.Greeting != expGreeting {
		t.Fatalf(`expected response object "Greeting" field to have value %q; got %q`, expGreeting, resObj.Greeting)
	}

	// Client-side timeout
	ctx, cancelFn := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancelFn()
	err = c.Request(ctx, "endpoint", reqObj, resObj)
	if err != transport.ErrTimeout {
		t.Fatalf("expected to get error %v; got %v", transport.ErrTimeout, err)
	}
}

func TestClientRequestWithServerEndpointCtx(t *testing.T) {
	tr := memory.New()
	err := tr.Dial(transport.ModeClient)
	if err != nil {
		t.Fatal(err)
	}
	err = tr.Dial(transport.ModeServer)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		tr.Close(transport.ModeServer)
		tr.Close(transport.ModeClient)
	}()

	expSender := "foo service"
	expEndpoint := "foo endpoint"
	expReceiver := "service"

	expGreeting := "hello tester"
	expReqPayload := `{"name":"tester"}`
	expResPayload := `{"greeting":"` + expGreeting + `"}`
	tr.Bind("", expReceiver, expEndpoint, transport.HandlerFunc(
		func(req transport.ImmutableMessage, res transport.Message) {
			payload, _ := req.Payload()
			payloadStr := string(payload)
			if payloadStr != expReqPayload {
				t.Errorf("expected request payload to be:\n%s\n\ngot:\n%s", expReqPayload, payloadStr)
			}

			if req.Sender() != expSender {
				t.Errorf("expected sender to be %q; got %q", expSender, req.Sender())
			}
			if req.SenderEndpoint() != expEndpoint {
				t.Errorf("expected sender endpoint to be %q; got %q", expEndpoint, req.SenderEndpoint())
			}

			if req.Receiver() != expReceiver {
				t.Errorf("expected receiver to be %q; got %q", expReceiver, req.Receiver())
			}
			if req.ReceiverEndpoint() != expEndpoint {
				t.Errorf("expected receiver endpoint to be %q; got %q", expEndpoint, req.ReceiverEndpoint())
			}

			res.SetPayload([]byte(expResPayload), nil)
			<-time.After(100 * time.Millisecond)
		}),
	)

	c, err := New(
		expReceiver,
		WithTransport(tr),
	)
	if err != nil {
		t.Fatal(err)
	}

	type request struct {
		Name string `json:"name"`
	}

	type response struct {
		Greeting string `json:"greeting"`
	}

	reqObj := &request{Name: "tester"}
	resObj := &response{}

	// Normal request originating from server endpoint handler
	ctx := context.WithValue(
		context.WithValue(context.Background(), server.CtxFieldServiceName, expSender),
		server.CtxFieldEndpointName,
		expEndpoint,
	)

	err = c.Request(ctx, expEndpoint, reqObj, resObj)
	if err != nil {
		t.Fatal(err)
	}

	if resObj.Greeting != expGreeting {
		t.Fatalf(`expected response object "Greeting" field to have value %q; got %q`, expGreeting, resObj.Greeting)
	}
}

func TestClientMiddlewareChain(t *testing.T) {
	origMiddleware := globalMiddlewareFactories
	defer func() {
		globalMiddlewareFactories = origMiddleware
	}()
	ClearGlobalMiddlewareFactories()

	tr := memory.New()
	err := tr.Dial(transport.ModeClient)
	if err != nil {
		t.Fatal(err)
	}
	err = tr.Dial(transport.ModeServer)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		tr.Close(transport.ModeServer)
		tr.Close(transport.ModeClient)
	}()

	expReceiver := "service"
	expSender := "other service"
	expEndpoint := "test"
	expResPayload := `{"hello":"back"}`

	tr.Bind("", expReceiver, expEndpoint, transport.HandlerFunc(
		func(req transport.ImmutableMessage, res transport.Message) {
			res.SetPayload([]byte(expResPayload), nil)
			<-time.After(100 * time.Millisecond)
		}),
	)

	logChan := make(chan string, 8)

	// Should be no-op
	RegisterGlobalMiddlewareFactories(nil)

	RegisterGlobalMiddlewareFactories(
		nil, // invalid middleware; should be filtered out
		testMiddlewareFactory("global middleware 0", logChan, false, nil),
	)
	RegisterGlobalMiddlewareFactories(
		testMiddlewareFactory("global middleware 1", logChan, true, nil),
	)

	c, err := New(
		expReceiver,
		WithTransport(tr),
		WithMiddleware(
			testMiddlewareFactory("local middleware 0", logChan, false, nil),
			nil, // invalid middleware; should be filtered out
		),
		WithMiddleware(nil), // invalid middlware list; should be ignored
		WithMiddleware(
			testMiddlewareFactory("local middleware 1", logChan, true, nil),
		),
	)
	if err != nil {
		t.Fatal(err)
	}

	reqObj := map[string]string{}
	resObj := map[string]string{}

	// Normal request originating from server endpoint handler
	ctx := context.WithValue(
		context.WithValue(context.Background(), server.CtxFieldServiceName, expSender),
		server.CtxFieldEndpointName,
		expEndpoint,
	)

	err = c.Request(ctx, expEndpoint, &reqObj, &resObj)
	if err != nil {
		t.Fatal(err)
	}

	expLog := []string{
		"pre global middleware 0",
		"pre global middleware 1",
		"pre local middleware 0",
		"pre local middleware 1",
		"post local middleware 1",
		"post local middleware 0",
		"post global middleware 1",
		"post global middleware 0",
	}
	for index, expEntry := range expLog {
		entry := <-logChan
		if entry != expEntry {
			t.Fatalf("[entry %d] expected log entry to be %q; got %q", index, expEntry, entry)
		}
	}

	// Client-side timeout; middleware should still be executed
	ctx, cancelFn := context.WithTimeout(ctx, 1*time.Millisecond)
	defer cancelFn()
	err = c.Request(ctx, expEndpoint, reqObj, resObj)
	if err != transport.ErrTimeout {
		t.Fatalf("expected to get error %v; got %v", transport.ErrTimeout, err)
	}

	for index, expEntry := range expLog {
		entry := <-logChan
		if entry != expEntry {
			t.Fatalf("[entry %d] expected log entry to be %q; got %q", index, expEntry, entry)
		}
	}
}

func TestClientMiddlewareThatAbortsRequestExecution(t *testing.T) {
	tr := memory.New()
	err := tr.Dial(transport.ModeClient)
	if err != nil {
		t.Fatal(err)
	}
	err = tr.Dial(transport.ModeServer)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		tr.Close(transport.ModeServer)
		tr.Close(transport.ModeClient)
	}()

	logChan := make(chan string, 8)

	c, err := New(
		"client",
		WithTransport(tr),
		WithMiddleware(
			testMiddlewareFactory("local middleware 0", logChan, false, nil),
			testMiddlewareFactory("local middleware 1", logChan, false, transport.ErrNotAuthorized),
			testMiddlewareFactory("local middleware 2", logChan, true, nil),
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	reqObj := map[string]string{}
	resObj := map[string]string{}

	err = c.Request(nil, "foo", &reqObj, &resObj)
	if err == nil || err != transport.ErrNotAuthorized {
		t.Fatalf("expected to get error %v; got %v", transport.ErrNotAuthorized, err)
	}

	expLog := []string{
		"pre local middleware 0",
		"pre local middleware 1",
		"post local middleware 0",
	}
	for index, expEntry := range expLog {
		entry := <-logChan
		if entry != expEntry {
			t.Fatalf("[entry %d] expected log entry to be %q; got %q", index, expEntry, entry)
		}
	}
}

func TestClientDialAndRequestErrors(t *testing.T) {
	_, err := New("Foo", WithTransport(&testTransportThatFailsDialing{}))
	if err != transport.ErrTransportAlreadyDialed {
		t.Fatal("expected to get a dial error from testTransportThatFailsDialing")
	}

	c, err := New("Foo", WithTransport(memory.New()))
	if err != nil {
		t.Fatal(err)
	}

	c.Close()

	err = c.Request(context.Background(), "foo", nil, nil)
	if err != transport.ErrTransportClosed {
		t.Fatalf("expected to get ErrTransportClosed; got %v", err)
	}
}

func TestClientErrors(t *testing.T) {
	tr := memory.New()
	err := tr.Dial(transport.ModeClient)
	if err != nil {
		t.Fatal(err)
	}
	err = tr.Dial(transport.ModeServer)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		tr.Close(transport.ModeServer)
		tr.Close(transport.ModeClient)
	}()

	var invocation int32
	tr.Bind("", "service", "endpoint", transport.HandlerFunc(
		func(req transport.ImmutableMessage, res transport.Message) {
			atomic.AddInt32(&invocation, 1)
			// first two invocations succeed
			if atomic.LoadInt32(&invocation) <= 2 {
				<-time.After(100 * time.Millisecond)
			} else {
				res.SetPayload(nil, errors.New("server-side error"))
			}
		}),
	)

	codec := &testCodec{
		marshalerFn: func(_ interface{}) ([]byte, error) {
			return nil, errors.New("marshal error")
		},
		unmarshalerFn: func(_ []byte, _ interface{}) error {
			return errors.New("unmarshal error")
		},
	}

	c, err := New(
		"service",
		WithCodec(codec),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.transport = tr

	reqObj := map[string]string{"hello": "world"}
	resObj := map[string]string{}

	// Marshal error handling
	expError := "marshal error"
	err = c.Request(context.Background(), "endpoint", &reqObj, &resObj)
	if err == nil || err.Error() != expError {
		t.Fatalf("expected error %q; got %v", expError, err)
	}

	c.marshaler = json.Marshal

	// Client-side timeout
	ctx, cancelFn := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancelFn()
	err = c.Request(ctx, "endpoint", &reqObj, &resObj)
	if err != transport.ErrTimeout {
		t.Fatalf("expected to get error %v; got %v", transport.ErrTimeout, err)
	}

	// Unmarshal error handling
	expError = "unmarshal error"
	err = c.Request(context.Background(), "endpoint", &reqObj, &resObj)
	if err == nil || err.Error() != expError {
		t.Fatalf("expected error %q; got %v", expError, err)
	}

	// Server-side error
	c.unmarshaler = json.Unmarshal
	expError = "server-side error"
	err = c.Request(context.Background(), "endpoint", &reqObj, &resObj)
	if err == nil || err.Error() != expError {
		t.Fatalf("expected error %q; got %v", expError, err)
	}
}

func TestMiddlewareFactoryReceivesServiceName(t *testing.T) {
	origMiddleware := globalMiddlewareFactories
	defer func() {
		globalMiddlewareFactories = origMiddleware
	}()
	ClearGlobalMiddlewareFactories()

	RegisterGlobalMiddlewareFactories(testMiddlewareFactory("global middleware 0", nil, false, nil))

	expServiceName := "fooService"
	c, err := New(expServiceName)
	if err != nil {
		t.Fatal(err)
	}

	expLen := 1
	if len(c.middleware) != expLen {
		t.Fatalf("expected client to instanciate %d middleware; got %d", expLen, len(c.middleware))
	}

	m := c.middleware[0].(*testMiddleware)
	if m.serviceName != expServiceName {
		t.Fatalf("expected middleware factory to be invoked with service name %q; got %q", expServiceName, m.serviceName)
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

type testMiddleware struct {
	serviceName  string
	name         string
	logChan      chan string
	returnNilCtx bool
	returnErr    error
}

func testMiddlewareFactory(name string, logChan chan string, returnNilCtx bool, returnErr error) MiddlewareFactory {
	return func(serviceName string) Middleware {
		return &testMiddleware{
			serviceName:  serviceName,
			name:         name,
			logChan:      logChan,
			returnNilCtx: returnNilCtx,
			returnErr:    returnErr,
		}
	}
}

func (m *testMiddleware) Pre(ctx context.Context, req transport.Message) (context.Context, error) {
	m.logChan <- "pre " + m.name
	var ctxKey interface{} = "ctx-" + m.name
	ctx = context.WithValue(ctx, ctxKey, m.name)
	if m.returnNilCtx {
		return nil, m.returnErr
	}
	return ctx, m.returnErr
}

func (m *testMiddleware) Post(ctx context.Context, req, res transport.ImmutableMessage) {
	m.logChan <- "post " + m.name
	if m.returnNilCtx {
		return
	}

	var ctxKey interface{} = "ctx-" + m.name
	ctxVal := ctx.Value(ctxKey).(string)
	if ctxVal != m.name {
		panic(fmt.Errorf(`expected ctx value "ctx-%s" to be %q; got %q`, m.name, m.name, ctxVal))
	}
}

type testTransportThatFailsDialing struct {
}

func (tr *testTransportThatFailsDialing) Close(_ transport.Mode) error {
	return nil
}

func (tr *testTransportThatFailsDialing) Request(_ transport.Message) <-chan transport.ImmutableMessage {
	return nil
}

func (tr *testTransportThatFailsDialing) Bind(_, _, _ string, _ transport.Handler) error {
	return nil
}

func (tr *testTransportThatFailsDialing) Unbind(_, _, _ string) {
}

func (tr *testTransportThatFailsDialing) Dial(_ transport.Mode) error {
	return transport.ErrTransportAlreadyDialed
}
