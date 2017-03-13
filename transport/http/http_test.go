package http

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/achilleasa/usrv/config"
	"github.com/achilleasa/usrv/transport"
)

func TestHTTPServerErrors(t *testing.T) {
	tr := New()
	tr.port.Set(9901)

	// Try to bind to already bound endpoint
	err := tr.Bind("", "service", "endpoint", transport.HandlerFunc(func(_ transport.ImmutableMessage, _ transport.Message) {}))
	if err != nil {
		t.Fatal(err)
	}
	err = tr.Bind("", "service", "endpoint", transport.HandlerFunc(func(_ transport.ImmutableMessage, _ transport.Message) {}))
	expError := `binding (version: "", service: "service", endpoint: "endpoint") already defined`
	if err == nil || err.Error() != expError {
		t.Fatalf("expected to get error %q; got %v", expError, err)
	}

	// Test close on closed transport
	err = tr.Close(transport.ModeServer)
	if err != transport.ErrTransportClosed {
		t.Fatalf("expected error %v; got %v", transport.ErrTransportClosed, err)
	}

	// Test invalid protocol
	tr.protocol.Set("gopher")
	err = tr.Dial(transport.ModeServer)
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
	err = tr.Dial(transport.ModeServer)
	defer tr.Close(transport.ModeServer)
	listen = origListen
	if err == nil || err.Error() != expError {
		t.Fatalf("expected dial to report listen error %q; got %v", expError, err)
	}

	// Test double dial
	err = tr.Dial(transport.ModeServer)
	defer tr.Close(transport.ModeServer)
	if err != nil {
		t.Fatal(err)
	}
	err = tr.Dial(transport.ModeServer)
	defer tr.Close(transport.ModeServer)
	if err != nil {
		t.Fatal(err)
	}

	// Test bind after dialing
	err = tr.Bind("", "service", "endpoint2", transport.HandlerFunc(func(_ transport.ImmutableMessage, _ transport.Message) {}))
	if err != nil {
		t.Fatal(err)
	}
}

func TestHTTPClientErrors(t *testing.T) {
	tr := New()
	tr.port.Set(9902)
	tr.URLBuilder = testURLBuilder{addr: "http://localhost:9902"}

	err := tr.Close(transport.ModeClient)
	if err != transport.ErrTransportClosed {
		t.Fatalf("expected to get ErrTransportClosed; got %v", err)
	}

	err = tr.Bind("", "service", "endpoint", transport.HandlerFunc(func(_ transport.ImmutableMessage, _ transport.Message) {}))
	if err != nil {
		t.Fatal(err)
	}

	// Request when transport is closed
	res := <-tr.Request(newMessage("test/producer", "service/endpoint"))
	if _, err = res.Payload(); err != transport.ErrTransportClosed {
		t.Fatalf("expected to get error %q; got %v", transport.ErrTransportClosed, err)
	}
	res.Close()

	err = tr.Dial(transport.ModeServer)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close(transport.ModeServer)

	err = tr.Dial(transport.ModeClient)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close(transport.ModeClient)

	// Test error while creating client request
	origNewRequest := newRequest
	expError := "new request errror"
	newRequest = func(_, _ string, _ io.Reader) (*http.Request, error) {
		return nil, errors.New(expError)
	}
	res = <-tr.Request(newMessage("test/producer", "service/endpoint"))
	newRequest = origNewRequest
	if _, err = res.Payload(); err == nil || err.Error() != expError {
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
	if _, err = res.Payload(); err == nil || err.Error() != expError {
		t.Fatalf("expected to get error %q; got %v", expError, err)
	}

	httpRes, err := http.Get(tr.URLBuilder.URL("", "service", "endpoint"))
	if err != nil {
		t.Fatal(err)
	}
	if httpRes.StatusCode != http.StatusNotFound {
		t.Fatalf("expected non-POST request to get 404 status code; got %d", httpRes.StatusCode)
	}

	// Simulate client eeror
	expErr := errors.New("client error")
	tr.clientErr = expErr
	res = <-tr.Request(newMessage("test/producer", "service/endpoint"))
	if _, err := res.Payload(); err != expErr {
		t.Fatalf("expected to get error %q; got %v", expError, err)
	}
}

func TestTLSErrors(t *testing.T) {
	tr := New()
	tr.port.Set(9444)
	tr.protocol.Set("https")

	// Test invalid verify mode
	tr.tlsVerify.Set("bogus")
	expError := errInvalidVerifyMode
	err := createAtomicClient(tr)
	if err != expError {
		t.Fatalf("expected to get error %v; got %v", expError, err)
	}

	// Missing certificate file option
	tr.tlsVerify.Set(tlsVerifyAddToCertPool)
	expError = errMissingCertificate
	tr.tlsCert.Set("")
	err = createAtomicClient(tr)
	if err != expError {
		t.Fatalf("expected to get error %v; got %v", expError, err)
	}

	// Calling dial should also fail with same error
	err = tr.Dial(transport.ModeClient)
	if err != expError {
		t.Fatalf("expected to get error %v; got %v", expError, err)
	}

	// Missing certificate key file option
	tr.tlsCert.Set("foo")
	tr.tlsKey.Set("")
	err = createAtomicClient(tr)
	if err != expError {
		t.Fatalf("expected to get error %v; got %v", expError, err)
	}

	// Load keypair error
	tr.tlsKey.Set("bar")
	origLoadX509KeyPair := loadX509KeyPair
	defer func() {
		loadX509KeyPair = origLoadX509KeyPair
	}()
	expError = errors.New("loadX509KeyPair error")
	loadX509KeyPair = func(_, _ string) (tls.Certificate, error) {
		return tls.Certificate{}, expError
	}
	err = createAtomicClient(tr)
	if err != expError {
		t.Fatalf("expected to get error %v; got %v", expError, err)
	}

	// Trying to dial in server mode should also fail
	err = tr.Dial(transport.ModeServer)
	if err != expError {
		t.Fatalf("expected to get error %v; got %v", expError, err)
	}
	loadX509KeyPair = origLoadX509KeyPair

	// System cert pool errors
	loadX509KeyPair = func(_, _ string) (tls.Certificate, error) {
		return tls.Certificate{}, nil
	}

	origSystemCertPool := systemCertPool
	expError = errors.New("systemCertPool error")
	systemCertPool = func() (*x509.CertPool, error) {
		return nil, expError
	}
	err = createAtomicClient(tr)
	systemCertPool = origSystemCertPool
	if err != expError {
		t.Fatalf("expected to get error %v; got %v", expError, err)
	}

	// Error reading certificate files
	origReadFile := readFile
	expError = errors.New("readFile error")
	readFile = func(_ string) ([]byte, error) {
		return nil, expError
	}
	err = createAtomicClient(tr)
	defer func() {
		readFile = origReadFile
	}()
	if err != expError {
		t.Fatalf("expected to get error %v; got %v", expError, err)
	}

	// AppendPEM errors
	readFile = func(_ string) ([]byte, error) {
		return []byte("invalid"), nil
	}
	expError = errAddCertificateToPool
	err = createAtomicClient(tr)
	if err != expError {
		t.Fatalf("expected to get error %v; got %v", expError, err)
	}
}

func TestHTTPErrorPropagation(t *testing.T) {
	tr := New()
	tr.port.Set(9903)
	tr.URLBuilder = testURLBuilder{addr: "http://localhost:9903"}

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

	err := tr.Bind("", "test", "errorEndpoint", transport.HandlerFunc(handleRPC))
	if err != nil {
		t.Fatal(err)
	}

	err = tr.Dial(transport.ModeServer)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close(transport.ModeServer)

	err = tr.Dial(transport.ModeClient)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close(transport.ModeClient)

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

func TestConcurrentRPCOverHTTP(t *testing.T) {
	tr := New()
	tr.port.Set(9904)
	tr.URLBuilder = testURLBuilder{addr: "http://localhost:9904"}

	handleRPC := func(req transport.ImmutableMessage, res transport.Message) {
		res.SetPayload([]byte("hello back!"), nil)
	}

	// Test double server dial
	err := tr.Dial(transport.ModeServer)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close(transport.ModeServer)

	err = tr.Dial(transport.ModeServer)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close(transport.ModeServer)

	// Test double client dial
	err = tr.Dial(transport.ModeClient)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close(transport.ModeClient)

	err = tr.Dial(transport.ModeClient)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close(transport.ModeClient)

	// Bind after dialing
	err = tr.Bind("", "toService", "toEndpoint", transport.HandlerFunc(handleRPC))
	if err != nil {
		t.Fatal(err)
	}

	req := newMessage("fromService/fromEndpoint", "toService/toEndpoint")
	defer req.Close()
	req.SetPayload([]byte("hello"), nil)

	expPayload := "hello back!"
	numReqs := 100
	var wg sync.WaitGroup
	wg.Add(numReqs)
	for i := 0; i < numReqs; i++ {
		go func(reqId int) {
			defer wg.Done()
			resChan := tr.Request(req)
			res := <-resChan

			payload, err := res.Payload()
			if err != nil {
				t.Errorf("[req %d] got error %v", reqId, err)
				return
			}
			if string(payload) != expPayload {
				t.Errorf("[req %d] expected payload to be %q; got %q", reqId, expPayload, string(payload))
			}
		}(i)
	}

	wg.Wait()
}

func TestRPCOverHTTP(t *testing.T) {
	tr := New()
	tr.port.Set(9905)
	tr.URLBuilder = testURLBuilder{addr: "http://localhost:9905"}

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

		// Populate response
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

	err := tr.Bind("", "service", "nop-endpoint", transport.HandlerFunc(func(_ transport.ImmutableMessage, _ transport.Message) {}))
	if err != nil {
		t.Fatal(err)
	}

	err = tr.Dial(transport.ModeServer)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close(transport.ModeServer)

	err = tr.Dial(transport.ModeClient)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close(transport.ModeClient)

	// Bind after dialing
	err = tr.Bind("", "toService", "toEndpoint", transport.HandlerFunc(handleRPC))
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

	// Unbind endpoint (second Unbind should be a no-op); following calls to this endpoint should fail with ErrNotFound
	tr.Unbind("", "toService", "toEndpoint")
	tr.Unbind("", "toService", "toEndpoint")

	resChan = tr.Request(req)
	res = <-resChan
	_, err = res.Payload()
	if err != transport.ErrNotFound {
		t.Errorf("expected to get ErrNotFound; got %v", err)
	}
}

func TestRPCOverHTTPS(t *testing.T) {
	certFile, keyFile := genSSLCert(t)
	defer os.Remove(certFile)
	defer os.Remove(keyFile)

	// By default, the http provider will add the certificate to the system's certificate pool.
	tr := New()
	tr.port.Set(9443)
	tr.protocol.Set("https")
	tr.tlsStrictMode.Set(true)
	tr.tlsCert.Set(certFile)
	tr.tlsKey.Set(keyFile)
	tr.URLBuilder = testURLBuilder{addr: "https://localhost:9443"}

	expValue := "hello!"
	handleRPC := func(req transport.ImmutableMessage, res transport.Message) {
		res.SetPayload([]byte(expValue), nil)
	}

	err := tr.Bind("", "localhost", "toEndpoint", transport.HandlerFunc(handleRPC))
	if err != nil {
		t.Fatal(err)
	}

	err = tr.Dial(transport.ModeServer)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close(transport.ModeServer)

	err = tr.Dial(transport.ModeClient)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close(transport.ModeClient)

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

	// Test skipVerification mode
	tr.tlsVerify.Set(tlsVerifySkip)
	tr.config.Set(map[string]string{})
	<-time.After(100 * time.Millisecond)

	// Send request again
	resChan = tr.Request(req)
	res = <-resChan

	payload, err = res.Payload()
	if err != nil {
		t.Fatal(err)
	}
	if string(payload) != expValue {
		t.Errorf("expected payload to be %q; got %q", expValue, string(payload))
	}
}

func TestHTTPReconfiguration(t *testing.T) {
	certFile, keyFile := genSSLCert(t)
	defer os.Remove(certFile)
	defer os.Remove(keyFile)

	// backup defaults and restore them after the test
	defaultValues, _ := config.Store.Get("transport/http")
	defer config.Store.SetKeys(0, "transport/http", defaultValues)

	// Set our custom values
	config.Store.SetKeys(0, "transport/http", map[string]string{
		"protocol":          "http",
		"port":              "9906",
		"client/hostsuffix": "",
		"tls/certificate":   certFile,
		"tls/key":           keyFile,
	})

	tr := New()
	tr.URLBuilder = newDefaultURLBuilder()

	expValue := "hello!"
	handleRPC := func(req transport.ImmutableMessage, res transport.Message) {
		res.SetPayload([]byte(expValue), nil)

		// Ensure that listener uses the new port
		expListenValue := "[::]:9907"
		listenAddr := tr.listener.Addr().String()
		if listenAddr != expListenValue {
			t.Errorf("expected listener to use address %q; got %q", expListenValue, listenAddr)
		}
	}

	err := tr.Bind("", "localhost", "toEndpoint", transport.HandlerFunc(handleRPC))
	if err != nil {
		t.Fatal(err)
	}

	err = tr.Dial(transport.ModeServer)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close(transport.ModeServer)

	err = tr.Dial(transport.ModeClient)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close(transport.ModeClient)

	// Ensure that listener uses the old port
	expListenValue := "[::]:9906"
	listenAddr := tr.listener.Addr().String()
	if listenAddr != expListenValue {
		t.Errorf("expected listener to use address %q; got %q", expListenValue, listenAddr)
	}

	// Update port configuration and wait for transport to redial itself
	config.Store.SetKey(0, "transport/http/port", "9907")
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

	// Now switch to https mode
	config.Store.SetKey(0, "transport/http/protocol", "https")
	<-time.After(100 * time.Millisecond)

	// Resend request
	resChan = tr.Request(req)
	res = <-resChan
	payload, err = res.Payload()
	if err != nil {
		t.Fatal(err)
	}
	if string(payload) != expValue {
		t.Errorf("expected payload to be %q; got %q", expValue, string(payload))
	}
}

func TestHTTPClientReconfiguration(t *testing.T) {
	certFile, keyFile := genSSLCert(t)
	defer os.Remove(certFile)
	defer os.Remove(keyFile)

	// backup defaults and restore them after the test
	defaultValues, _ := config.Store.Get("transport/http")
	defer config.Store.SetKeys(0, "transport/http", defaultValues)

	// Set our custom values
	config.Store.SetKeys(0, "transport/http", map[string]string{
		"protocol":          "http",
		"port":              "9910",
		"client/hostsuffix": "",
		"tls/certificate":   certFile,
		"tls/key":           keyFile,
	})

	trClient := Factory().(*Transport)
	trClient.URLBuilder = newDefaultURLBuilder()
	err := trClient.Dial(transport.ModeClient)
	if err != nil {
		t.Fatal(err)
	}
	defer trClient.Close(transport.ModeClient)

	// Now switch to https mode
	config.Store.SetKey(0, "transport/http/protocol", "https")
	<-time.After(100 * time.Millisecond)

	expValue := "hello!"
	handleRPC := func(req transport.ImmutableMessage, res transport.Message) {
		res.SetPayload([]byte(expValue), nil)
	}

	trServer := Factory()

	err = trServer.Bind("", "localhost", "toEndpoint", transport.HandlerFunc(handleRPC))
	if err != nil {
		t.Fatal(err)
	}

	err = trServer.Dial(transport.ModeServer)
	if err != nil {
		t.Fatal(err)
	}
	defer trServer.Close(transport.ModeServer)

	req := newMessage("fromService/fromEndpoint", "localhost/toEndpoint")
	defer req.Close()
	resChan := trClient.Request(req)
	res := <-resChan
	payload, err := res.Payload()
	if err != nil {
		t.Fatal(err)
	}
	if string(payload) != expValue {
		t.Errorf("expected payload to be %q; got %q", expValue, string(payload))
	}
}

func TestHTTPConfigWorkerCleanup(t *testing.T) {
	origSetFinalizer := setFinalizer
	defer func() {
		setFinalizer = origSetFinalizer
	}()
	var finalizer func(*Transport)
	setFinalizer = func(_ interface{}, cb interface{}) {
		finalizer = cb.(func(*Transport))
	}

	tr := New()
	finalizer(tr)
	time.After(500 * time.Millisecond)

	// Trigger change
	tr.client = nil
	tr.port.Set(1234)
	time.After(500 * time.Millisecond)

	if tr.client != nil {
		t.Fatalf("expected client to remain nil")
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
	url := builder.URL("", "foo", "bar")
	if expURL != url {
		t.Fatalf("expectd builder to return URL %q; got %q", expURL, url)
	}

	expURL = "http://foo-123.service:1234/foo/bar"
	url = builder.URL("123", "foo", "bar")
	if expURL != url {
		t.Fatalf("expectd builder to return URL %q; got %q", expURL, url)
	}

	builder.config.Set(map[string]string{
		"protocol":          "https",
		"client/hostsuffix": ".service",
	})

	expURL = "https://foo.service/foo/bar"
	url = builder.URL("", "foo", "bar")
	if expURL != url {
		t.Fatalf("expectd builder to return URL %q; got %q", expURL, url)
	}
}

func TestFactories(t *testing.T) {
	tr1 := SingletonFactory()
	tr2 := SingletonFactory()
	defer tr1.Close(transport.ModeServer)
	defer tr2.Close(transport.ModeServer)

	if tr1 != tr2 {
		t.Fatalf("expected singleton factory to return the same instance")
	}

	tr1 = Factory()
	tr2 = Factory()
	defer tr1.Close(transport.ModeServer)
	defer tr2.Close(transport.ModeServer)

	if tr1 == tr2 {
		t.Fatalf("expected factory to return different instance")
	}
}

type testURLBuilder struct {
	addr string
}

func (b testURLBuilder) URL(version, service, endpoint string) string {
	return fmt.Sprintf("%s/%s/%s", b.addr, service, endpoint)
}

func genSSLCert(t *testing.T) (certFile, keyFile string) {
	certificate := `
-----BEGIN CERTIFICATE-----
MIID9DCCAtygAwIBAgIJAPc67yL6gpa6MA0GCSqGSIb3DQEBBQUAMFkxCzAJBgNV
BAYTAkFVMRMwEQYDVQQIEwpTb21lLVN0YXRlMSEwHwYDVQQKExhJbnRlcm5ldCBX
aWRnaXRzIFB0eSBMdGQxEjAQBgNVBAMTCWxvY2FsaG9zdDAeFw0xNzAxMDcxMDQx
NDdaFw0yNzAxMDUxMDQxNDdaMFkxCzAJBgNVBAYTAkFVMRMwEQYDVQQIEwpTb21l
LVN0YXRlMSEwHwYDVQQKExhJbnRlcm5ldCBXaWRnaXRzIFB0eSBMdGQxEjAQBgNV
BAMTCWxvY2FsaG9zdDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAOu4
ipoPAnx+9pW6lwve60SmtTo8EUcAlFlTcGvSZusH2KFj0bcqTao7JWfHgpE/+4xx
HkrOOF7rcrQe+TecyfddcFpmVJNp37U0fqTraTTDw+fs/xKuRxkH/mLEBUicwPu+
bkJ7IMlH0Ox1nhORhPIZZ3/6nxj7tWv2ezfNNftQ+sYpwLb/FwoLMekuSDYdy68I
7/rpFgioo3siXG2hfaIKM7YKiCjV557qub5H58yy/QodbsjpAAt4HDjMIB5vBSS9
KqhuFyU67u+7XAegU9Luk2Je07PI3EJs/rubbCLWrlWBvE1Z9l6UC0uzb7PgxtmH
qysJ8tc8ScD63Z3iDtUCAwEAAaOBvjCBuzAdBgNVHQ4EFgQUxcW57cAYO8Ollie+
CgC94zdZGFwwgYsGA1UdIwSBgzCBgIAUxcW57cAYO8Ollie+CgC94zdZGFyhXaRb
MFkxCzAJBgNVBAYTAkFVMRMwEQYDVQQIEwpTb21lLVN0YXRlMSEwHwYDVQQKExhJ
bnRlcm5ldCBXaWRnaXRzIFB0eSBMdGQxEjAQBgNVBAMTCWxvY2FsaG9zdIIJAPc6
7yL6gpa6MAwGA1UdEwQFMAMBAf8wDQYJKoZIhvcNAQEFBQADggEBAAW5Dv9/Yc7g
ZIVbO9wGrjRJWwq1K7A9TIsoQCTZotLXNPFrtP+Bn7a1ZKSmFF2hSyRRAZtbgvXE
cZuKw1Uo6HJxsvP73ChqGsJAN8n/46qo/8rpYXyFd9P0wRydmeQBsLK7P0NOkxUF
PrP63jYSfTpxJ0zGBHvwEOLkx/ocGgk/rL8OJIiRSWR864BmDfwQHIyJs8bZ+1k0
83Nr/SPqJua/UVDxXxwP52pCvu5ZVgi7xG9WdIpA1w+YCec9rEJ7Xchycp27Z6lG
3dgjTIXntj4Q64d8HXX/0+DuHgYGJbCXbPzkcJ8mr2+uBgEa/ZHS3QkYDT2kJn7E
aBwkEI28gm4=
-----END CERTIFICATE-----`

	key := `-----BEGIN RSA PRIVATE KEY-----
MIIEpQIBAAKCAQEA67iKmg8CfH72lbqXC97rRKa1OjwRRwCUWVNwa9Jm6wfYoWPR
typNqjslZ8eCkT/7jHEeSs44XutytB75N5zJ911wWmZUk2nftTR+pOtpNMPD5+z/
Eq5HGQf+YsQFSJzA+75uQnsgyUfQ7HWeE5GE8hlnf/qfGPu1a/Z7N801+1D6xinA
tv8XCgsx6S5INh3Lrwjv+ukWCKijeyJcbaF9ogoztgqIKNXnnuq5vkfnzLL9Ch1u
yOkAC3gcOMwgHm8FJL0qqG4XJTru77tcB6BT0u6TYl7Ts8jcQmz+u5tsItauVYG8
TVn2XpQLS7Nvs+DG2YerKwny1zxJwPrdneIO1QIDAQABAoIBAQDkUoQuZYuK+4/t
yCa2oN5SSQgRuE0j8TPAskmaptp5ncf/y6g/OwKveUrqEx4tg0Qs7QTigI2po3Yf
ckED1SLsL928MpKJl2vRIV/qbvwg197Sr4UCmzzSyiNll2lmxC9JqVMzogBH9wAv
il3rpnCX8HOIS0H/+Q/p233Otz8qhZH+uGeceMbUsg3StYWDIJwUeIzx4XN0sre5
OCzrHw+ad6NMhREo93O24eNygl1J5uu9AuYGoRqLQOIBDCyMTb3NChtEJ9a0pm46
sI/3LH6xSwF7J1MgOhDwdEzt1ueOB6dvNWqydoCXiufc9kpcn82xUNoGOhGJmvMz
6fm+kPuhAoGBAPYQWS8NQaPzXlZnkIZyERemPafgcLWrN4pVSbB4wywK6EhawYva
B7jeG2bf5KChpZFHBatknwEn9vM9FNgCt7j7qUrFABD9GS7RXBjpg4SU5Sz3jYcV
wOjpE3Yorki4AJkqO+v8YL2EiE3iJOxtVTsQAvNzwHUm35KtMoCngAADAoGBAPU9
RhML+gZHUqy0/w7/rXf5WqYa/i2fx7RH0Z0iiRp7AMNNcXOfAa+2LEs0cJIgzvBP
slnjcAcpmKhxcqM2755iqnkTdKBRwvPlFFa7679YlOPsoxta1yIec0HIPXtHi3A3
pE1EnL8aM30+VTfNfSXVlyA3evMj+LcleUQNy1pHAoGAHX4CIniVSIBP601IbkTX
tZzwQOHOwIeABa2JQoSG6A16n8l47zk3ubmtURw+u94ECTCZBlzuDeZrW+YTTHyu
5pYLSXHpOyAK16iyQC4k3Ew4V7ZoGSvLTl85PO1NTlv3fmQogHVkZvKun58eS9Qi
5gxaPjG+fIwnOd5WckMhPV8CgYEA8U53cypnvGHVwcbe6f0+zTx4q9UHohEESioY
4UsoKPw7RfEf3yroV+MjNmTFF6Rcuy1QSw52HzYY1jW7HUpjATAImdZA/bc14xLX
rnh+getBpfwkijgaU6Iuut2zUWiWlbbKXpVSvt+jJmt9Isl5iQ7gA31T54bPpjaj
WglQvOUCgYEA6AZxHm061f9CdqQbGjnUPJJ4b1poG9m+hxtuBCjg1CCQiKGwGRKb
o7nSslRFPeLYp/rG8lZIQQYIku9a3aHt6JGV9kBj4Xoe6UvPcyxUn1rBDUELYbVZ
yRVjlpqDKQEAAtSLyU7rvW6Im0uwThEzrsMnxPXA2gPNfDN9TwWhtxY=
-----END RSA PRIVATE KEY-----`

	cf, err := ioutil.TempFile("", "ssl-")
	if err != nil {
		t.Fatal(err)
	}
	cf.WriteString(certificate)
	cf.Close()

	kf, err := ioutil.TempFile("", "ssl-")
	if err != nil {
		t.Fatal(err)
	}
	kf.WriteString(key)
	kf.Close()

	return cf.Name(), kf.Name()
}

func newMessage(from, to string) transport.Message {
	fromFields := strings.Split(from, "/")
	toFields := strings.Split(to, "/")

	m := transport.MakeGenericMessage()
	m.SenderField = fromFields[0]
	m.SenderEndpointField = fromFields[1]
	m.ReceiverField = toFields[0]
	m.ReceiverEndpointField = toFields[1]

	return m
}

// createAtomicClient calls tr.createAtomicClient while holding the lock.
func createAtomicClient(tr *Transport) error {
	tr.rwMutex.Lock()
	defer tr.rwMutex.Unlock()

	return tr.createClient()
}
