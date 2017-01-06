package provider

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/achilleasa/usrv/config"
	"github.com/achilleasa/usrv/transport"
)

func TestHTTPErrors(t *testing.T) {
	tr := NewHTTP()
	tr.port.Set(9901)
	tr.URLBuilder = testURLBuilder{addr: "http://localhost:9901"}
	defer tr.Close()

	// Try to bind to already bound endpoint
	err := tr.Bind("service", "endpoint", transport.HandlerFunc(func(_ transport.ImmutableMessage, _ transport.Message) {}))
	if err != nil {
		t.Fatal(err)
	}
	err = tr.Bind("service", "endpoint", transport.HandlerFunc(func(_ transport.ImmutableMessage, _ transport.Message) {}))
	expError := `binding "service/endpoint" already defined`
	if err == nil || err.Error() != expError {
		t.Fatalf("expected to get error %q; got %v", expError, err)
	}

	// Test close on closed transport
	err = tr.Close()
	if err != transport.ErrTransportClosed {
		t.Fatalf("expected error %v; got %v", transport.ErrTransportClosed, err)
	}

	// Test invalid protocol
	tr.protocol.Set("gopher")
	err = tr.Dial()
	expError = `unsupported protocol "gopher"`
	if err == nil || err.Error() != expError {
		t.Fatalf("expected dial to report listen error %q; got %v", expError, err)
	}
	tr.protocol.Set("http")

	// Test listener error
	origListen := listen
	expError = "listener error"
	listen = func(_, _ string) (net.Listener, error) {
		return nil, errors.New(expError)
	}
	err = tr.Dial()
	listen = origListen
	if err == nil || err.Error() != expError {
		t.Fatalf("expected dial to report listen error %q; got %v", expError, err)
	}

	// Test double dial
	err = tr.Dial()
	if err != nil {
		t.Fatal(err)
	}
	err = tr.Dial()
	if err != nil {
		t.Fatal(err)
	}

	// Test bind after dialing
	err = tr.Bind("service", "endpoint1", transport.HandlerFunc(func(_ transport.ImmutableMessage, _ transport.Message) {}))
	if err != transport.ErrTransportAlreadyDialed {
		t.Fatalf("expected to get error %q; got %v", transport.ErrTransportAlreadyDialed, err)
	}

	// Test error while creating client request
	origNewRequest := newRequest
	expError = "new request errror"
	newRequest = func(_, _ string, _ io.Reader) (*http.Request, error) {
		return nil, errors.New(expError)
	}
	res := <-tr.Request(newMessage("test/producer", "service/endpoint"))
	newRequest = origNewRequest
	if _, err := res.Payload(); err == nil || err.Error() != expError {
		t.Fatalf("expected to get error %q; got %v", expError, err)
	}
	res.Close()

	// Test error while reading request body
	origReadAll := readAll
	expError = "readAll errror"
	readAll = func(_ io.Reader) ([]byte, error) {
		return nil, errors.New(expError)
	}
	res = <-tr.Request(newMessage("test/producer", "service/endpoint"))
	readAll = origReadAll
	if _, err := res.Payload(); err == nil || err.Error() != expError {
		t.Fatalf("expected to get error %q; got %v", expError, err)
	}

	httpRes, err := http.Get(tr.URLBuilder.URL("service", "endpoint"))
	if err != nil {
		t.Fatal(err)
	}
	if httpRes.StatusCode != http.StatusNotFound {
		t.Fatalf("expected non-POST request to get 404 status code; got %d", httpRes.StatusCode)
	}
}

func TestHTTPErrorPropagation(t *testing.T) {
	tr := NewHTTP()
	tr.port.Set(9902)
	tr.URLBuilder = testURLBuilder{addr: "http://localhost:9902"}
	defer tr.Close()

	expErrors := []error{
		transport.ErrNotFound,
		transport.ErrTimeout,
		transport.ErrNotAuthorized,
		errors.New("custom"),
		errors.New(""),
	}

	handleRPC := func(req transport.ImmutableMessage, res transport.Message) {
		res.SetPayload(nil, expErrors[0])
	}

	err := tr.Bind("test", "errorEndpoint", transport.HandlerFunc(handleRPC))
	if err != nil {
		t.Fatal(err)
	}

	err = tr.Dial()
	if err != nil {
		t.Fatal(err)
	}

	specIndex := 0
	for ; len(expErrors) > 0; specIndex++ {
		req := newMessage("test/producer", "test/errorEndpoint")
		res := <-tr.Request(req)

		_, err := res.Payload()
		switch expErrors[0].Error() {
		case "":
			if err.Error() != "unknown error" {
				t.Errorf(`[spec %d] expected empty error to be mapped to "unknown error"`, specIndex)
			}
		case "custom":
			if expErrors[0].Error() != err.Error() {
				t.Errorf("[spec %d] expected to get error %v; got %v", specIndex, expErrors[0], err)
			}
		default:
			if expErrors[0] != err {
				t.Errorf("[spec %d] expected to get error %v; got %v", specIndex, expErrors[0], err)
			}
		}
		res.Close()

		if len(expErrors) > 0 {
			expErrors = expErrors[1:]
		}
	}

	// Endpoint not found
	req := newMessage("test/producer", "test/unknownEndpoint")
	res := <-tr.Request(req)
	if _, err := res.Payload(); err == nil || err != transport.ErrNotFound {
		t.Fatalf("expected to get error %q; got %v", transport.ErrNotFound, err)
	}
}

func TestRPCOverHTTP(t *testing.T) {
	tr := NewHTTP()
	tr.port.Set(9903)
	tr.URLBuilder = testURLBuilder{addr: "http://localhost:9903"}
	defer tr.Close()

	expHeaders := map[string]string{
		"Key1": "value1",
		"Key2": "value2",
	}

	handleRPC := func(req transport.ImmutableMessage, res transport.Message) {
		expValues := []string{"fromService", "fromEndpoint", "toService", "toEndpoint", "hello"}
		if req.Sender() != expValues[0] {
			t.Errorf("expected sender to be %q; got %q", expValues[0], req.Sender())
		}
		expValues = expValues[1:]

		if req.SenderEndpoint() != expValues[0] {
			t.Errorf("expected sender endpoint to be %q; got %q", expValues[0], req.SenderEndpoint())
		}
		expValues = expValues[1:]

		if req.Receiver() != expValues[0] {
			t.Errorf("expected receiver to be %q; got %q", expValues[0], req.Receiver())
		}
		expValues = expValues[1:]

		if req.ReceiverEndpoint() != expValues[0] {
			t.Errorf("expected receiver to be %q; got %q", expValues[0], req.ReceiverEndpoint())
		}
		expValues = expValues[1:]

		payload, err := req.Payload()
		if err != nil {
			t.Fatal(err)
		}
		if string(payload) != expValues[0] {
			t.Errorf("expected payload to be %q; got %q", expValues[0], string(payload))
		}

		headers := req.Headers()
		if !reflect.DeepEqual(headers, expHeaders) {
			t.Errorf("header mismatch; expected %v; got %v", expHeaders, headers)
		}

		// Populate respone
		res.SetPayload([]byte("hello back!"), nil)

		// Add extra user header
		res.SetHeader("user-header", "custom user header set by handler")

		// These headers should be ignored by the response parser
		res.SetHeaders(map[string]string{
			requestIDHeader:      "foo",
			senderHeader:         "me",
			senderEndpointHeader: "you",
			errorHeader:          "error",
		})
	}

	err := tr.Bind("service", "nop-endpoint", transport.HandlerFunc(func(_ transport.ImmutableMessage, _ transport.Message) {}))
	if err != nil {
		t.Fatal(err)
	}

	err = tr.Bind("toService", "toEndpoint", transport.HandlerFunc(handleRPC))
	if err != nil {
		t.Fatal(err)
	}

	err = tr.Dial()
	if err != nil {
		t.Fatal(err)
	}

	req := newMessage("fromService/fromEndpoint", "toService/toEndpoint")
	defer req.Close()
	req.SetPayload([]byte("hello"), nil)
	req.SetHeader("key1", "other")
	req.SetHeaders(expHeaders)
	resChan := tr.Request(req)

	// Wait for response
	res := <-resChan

	expValues := []string{"toService", "toEndpoint", "fromService", "fromEndpoint", "hello back!"}
	if res.Sender() != expValues[0] {
		t.Errorf("expected sender to be %q; got %q", expValues[0], res.Sender())
	}
	expValues = expValues[1:]

	if res.SenderEndpoint() != expValues[0] {
		t.Errorf("expected sender endpoint to be %q; got %q", expValues[0], res.SenderEndpoint())
	}
	expValues = expValues[1:]

	if res.Receiver() != expValues[0] {
		t.Errorf("expected receiver to be %q; got %q", expValues[0], res.Receiver())
	}
	expValues = expValues[1:]

	if res.ReceiverEndpoint() != expValues[0] {
		t.Errorf("expected receiver endpoint to be %q; got %q", expValues[0], res.ReceiverEndpoint())
	}
	expValues = expValues[1:]

	payload, err := res.Payload()
	if err != nil {
		t.Fatal(err)
	}
	if string(payload) != expValues[0] {
		t.Errorf("expected payload to be %q; got %q", expValues[0], string(payload))
	}

	headers := res.Headers()
	expLen := 1
	if len(headers) != expLen {
		t.Fatalf("expected response headers to have length %d; got %d", expLen, len(headers))
	}

	expHeaderValue := "custom user header set by handler"
	headerValue := headers["User-Header"]
	if headerValue != expHeaderValue {
		t.Errorf("expected custom response header to contain %q; got %q", expHeaderValue, headerValue)
	}
}

func TestHTTPReconfiguration(t *testing.T) {
	// backup defaults and restore them after the test
	defaultValues, _ := config.Store.Get("transport/http")
	defer config.Store.SetKeys(0, "transport/http", defaultValues)

	// Set our custom values
	config.Store.SetKeys(0, "transport/http", map[string]string{
		"port":              "9904",
		"client/hostsuffix": "",
	})

	tr := NewHTTP()
	tr.URLBuilder = newDefaultURLBuilder()
	defer tr.Close()

	expValue := "hello!"
	handleRPC := func(req transport.ImmutableMessage, res transport.Message) {
		res.SetPayload([]byte(expValue), nil)

		// Ensure that listener uses the new port
		expListenValue := "[::]:9905"
		listenAddr := tr.listener.Addr().String()
		if listenAddr != expListenValue {
			t.Errorf("expected listener to use address %q; got %q", expListenValue, listenAddr)
		}
	}

	err := tr.Bind("localhost", "toEndpoint", transport.HandlerFunc(handleRPC))
	if err != nil {
		t.Fatal(err)
	}

	err = tr.Dial()
	if err != nil {
		t.Fatal(err)
	}

	// Ensure that listener uses the old port
	expListenValue := "[::]:9904"
	listenAddr := tr.listener.Addr().String()
	if listenAddr != expListenValue {
		t.Errorf("expected listener to use address %q; got %q", expListenValue, listenAddr)
	}

	// Update port configuration and wait for transport to redial itself
	config.Store.SetKey(0, "transport/http/port", "9905")
	<-time.After(100 * time.Millisecond)

	req := newMessage("fromService/fromEndpoint", "localhost/toEndpoint")
	defer req.Close()
	resChan := tr.Request(req)

	// Wait for response
	res := <-resChan
	payload, err := res.Payload()
	if err != nil {
		t.Fatal(err)
	}
	if string(payload) != expValue {
		t.Errorf("expected payload to be %q; got %q", expValue, string(payload))
	}
}

func TestDefaultURLBuilder(t *testing.T) {
	builder := newDefaultURLBuilder()
	builder.config.Set(map[string]string{
		"protocol":          "http",
		"port":              "1234",
		"client/hostsuffix": ".service",
	})

	expURL := "http://foo.service:1234/foo/bar"
	url := builder.URL("foo", "bar")
	if expURL != url {
		t.Fatalf("expectd builder to return URL %q; got %q", expURL, url)
	}

	builder.config.Set(map[string]string{
		"protocol":          "https",
		"client/hostsuffix": ".service",
	})

	expURL = "https://foo.service/foo/bar"
	url = builder.URL("foo", "bar")
	if expURL != url {
		t.Fatalf("expectd builder to return URL %q; got %q", expURL, url)
	}
}

type testURLBuilder struct {
	addr string
}

func (b testURLBuilder) URL(service, endpoint string) string {
	return fmt.Sprintf("%s/%s/%s", b.addr, service, endpoint)
}
