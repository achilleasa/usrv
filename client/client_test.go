package client

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/achilleasa/usrv/encoding"
	"github.com/achilleasa/usrv/server"
	"github.com/achilleasa/usrv/transport"
	"github.com/achilleasa/usrv/transport/provider"
)

func TestClientOptionError(t *testing.T) {
	expError := errors.New("option error")
	_, err := NewClient("foo", func(_ *Client) error { return expError })
	if err != expError {
		t.Fatalf("expected to get error %v; got %v", expError, err)
	}
}

func TestClientRequest(t *testing.T) {
	tr := provider.NewInMemory()
	defer tr.Close()

	expGreeting := "hello tester"
	expReqPayload := `{"name":"tester"}`
	expResPayload := `{"greeting":"` + expGreeting + `"}`
	tr.Bind("service", "endpoint", transport.HandlerFunc(
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

	c, err := NewClient(
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
	ctx, _ := context.WithTimeout(context.Background(), 1*time.Millisecond)
	err = c.Request(ctx, "endpoint", reqObj, resObj)
	if err != transport.ErrTimeout {
		t.Fatalf("expected to get error %v; got %v", transport.ErrTimeout, err)
	}
}

func TestClientRequestWithServerEndpointCtx(t *testing.T) {
	tr := provider.NewInMemory()
	defer tr.Close()

	expSender := "foo service"
	expEndpoint := "foo endpoint"
	expReceiver := "service"

	expGreeting := "hello tester"
	expReqPayload := `{"name":"tester"}`
	expResPayload := `{"greeting":"` + expGreeting + `"}`
	tr.Bind(expReceiver, expEndpoint, transport.HandlerFunc(
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

	c, err := NewClient(
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

func TestClientErrors(t *testing.T) {
	tr := provider.NewInMemory()
	defer tr.Close()

	invocation := 0
	tr.Bind("service", "endpoint", transport.HandlerFunc(
		func(req transport.ImmutableMessage, res transport.Message) {
			invocation++
			// first two invocations succeed
			if invocation <= 2 {
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

	c, err := NewClient(
		"service",
		WithCodec(codec),
	)
	if err != nil {
		t.Fatal(err)
	}
	c.transport = tr

	reqObj := map[string]string{"hello": "world"}
	resObj := map[string]string{}

	// Marshal error handling
	expError := "marshal error"
	err = c.Request(context.Background(), "endpoint", &reqObj, &resObj)
	if err == nil || err.Error() != expError {
		t.Fatal("expected error %q; got %v", expError, err)
	}

	c.marshaler = json.Marshal

	// Client-side timeout
	ctx, _ := context.WithTimeout(context.Background(), 1*time.Millisecond)
	err = c.Request(ctx, "endpoint", &reqObj, &resObj)
	if err != transport.ErrTimeout {
		t.Fatalf("expected to get error %v; got %v", transport.ErrTimeout, err)
	}

	// Unmarshal error handling
	expError = "unmarshal error"
	err = c.Request(context.Background(), "endpoint", &reqObj, &resObj)
	if err == nil || err.Error() != expError {
		t.Fatal("expected error %q; got %v", expError, err)
	}

	// Server-side error
	c.unmarshaler = json.Unmarshal
	expError = "server-side error"
	err = c.Request(context.Background(), "endpoint", &reqObj, &resObj)
	if err == nil || err.Error() != expError {
		t.Fatal("expected error %q; got %v", expError, err)
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
