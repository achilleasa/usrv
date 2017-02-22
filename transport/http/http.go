// Package http provides a usrv transport over HTTP/HTTPS.
package http

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	nethttp "net/http"
	"runtime"
	"strings"
	"sync"

	"github.com/achilleasa/usrv/config"
	"github.com/achilleasa/usrv/config/flag"
	"github.com/achilleasa/usrv/transport"
)

const (
	// The prefix for usrv headers.
	headerPrefix    = "Usrv-"
	headerPrefixLen = 5

	// The set of reserved header names (in canonical form) used by the transport.
	requestIDHeader      = "Request-Id"
	senderHeader         = "Sender"
	senderEndpointHeader = "Sender-Endpoint"
	errorHeader          = "Error"

	// TLS verification options
	tlsVerifySkip          = "skip"
	tlsVerifyAddToCertPool = "cert_pool"
)

// A set of endpoints that can be hooked by tests
var (
	listen          = net.Listen
	tlsListen       = tls.Listen
	newRequest      = nethttp.NewRequest
	readAll         = ioutil.ReadAll
	readFile        = ioutil.ReadFile
	loadX509KeyPair = tls.LoadX509KeyPair
	systemCertPool  = x509.SystemCertPool
	setFinalizer    = runtime.SetFinalizer
)

var (
	errMissingCertificate   = errors.New("missing tls certificate/key configuration settings")
	errAddCertificateToPool = errors.New("could not add certificate to client certificate pool")
	errInvalidVerifyMode    = errors.New(`invalid tls verify option; supported values are "skip" and "cert_pool"`)
)

var (
	_                 transport.Provider = &Transport{}
	singletonInstance *Transport
)

// Binding encapsulates the details of a service/endpoint combination and
// a handler for requests to it.
type binding struct {
	handler transport.Handler

	// The service and endpoint combination for this binding. We store this
	// separately for quickly populating incoming message fields./Request
	service  string
	endpoint string
	version  string
}

// ServiceURLBuilder defines an interface for mapping a version, service and endpoint
// tuple into a remote path URL.
//
// URL receives the service and endpoint name as input and returns back a URL.
type ServiceURLBuilder interface {
	URL(version, service, endpoint string) string
}

type defaultURLBuilder struct {
	config *flag.Map
}

func newDefaultURLBuilder() *defaultURLBuilder {
	return &defaultURLBuilder{
		config: config.MapFlag("transport/http"),
	}
}

func (b defaultURLBuilder) URL(version, service, endpoint string) string {
	cfg := b.config.Get()

	port := cfg["port"]
	if port != "" {
		port = ":" + port
	}

	if version != "" {
		version = "-" + version
	}

	return fmt.Sprintf(
		"%s://%s%s%s%s/%s/%s",
		cfg["protocol"],
		service,
		version,
		cfg["client/hostsuffix"],
		port,
		service,
		endpoint,
	)
}

type requestMaker interface {
	Do(req *nethttp.Request) (*nethttp.Response, error)
}

// Transport implements a usrv transport over HTTP/HTTPS.
//
// Bindings for this transport must be defined before a call to Dial(). Any
// attempt to define a binding after the transport has been dialed will result
// in a n error. If new bindings need to be defined after the transport has
// been dialed, the Close() method must be invoked before defining the new
// binidngs.
//
// When operating as a server, the transport provides a simple mux that dispatches
// incoming POST requests to the bound endpoints. Each endpoint is mapped to
// a route with pattern "service_name/endpoint_name". Non-POST requests or requests
// not matching the above pattern will fail with http.StatusNotFound (404).
//
// The transport returns http.StatusOK if the incoming request is handled by a
// defined binding and maps common errors to HTTP status codes using the following
// rules:
//  - 404 = Unknown binding or non-POST request
//  - 408 = If the binding handler returns transport.ErrTimeout
//  - 401 = If the binding handler returns transport.ErrNotAuthorized
//  - 500 = If the binding handler returns any other error; it that case the error message gets encoded as a response header
//
// The transport uses prefixed HTTP headers (Usrv-) to decode message headers from HTTP
// requests and encode them into HTTP responses.
//
// The transport server adds watches to the global configuration store for the
// following configuration parameters:
//  - transport/http/protocol (default: http). The protocol to use; either "http" or "https".
//  - transport/http/port (default: ""). The port to listen for incoming connections; if unspecified,
//    the default port for the protocol will be used.
//  - transport/http/tls/certificate (default: ""). The path to a SSL certificate; used only if protocol is "https".
//  - transport/http/tls/key (default: ""). The path to the key for the certificate; used only if protocol is "https".
//  - transport/http/tls/strict (default: "true"). Enables strict TLS configuration to achieve a perfect SSL labs score (see https://blog.bracebin.com/achieving-perfect-ssl-labs-score-with-go); used only if protocol is "https".
//
// The following configuration parameter is used by the transport in client mode
// when the protocol is set to "https":
//  - transport/http/client/verifycert (default: "cert_pool"). Defines how the http client verifies self-signed SSL certificates.
//    Supported values are either "cert_pool" (append certificate to the system's certificate pool) or "skip" which
//    sets the "InsecureSkipVerify" flag in the client's TLS configuration.
//
// If any of the above values changes, the transport will automatically trigger
// a redial while gracefully closing the existing listener.
//
// When operating as a client, the transport needs to be able to generate a
// remote URL for outgoing requests. For this purpose it uses a ServiceURLBuilder
// (URLBuilder field) which maps the outgoing service name and endpoint into an
// HTTP URL.
//
// If no URLBuilder is defined when the transport is dialed it will automatically
// use a default implementation that watches the following configuration fields:
//  - transport/http/protocol.
//  - transport/http/port.
//  - transport/http/client/hostsuffix. A suffix to append to the service name
//    when building the remote URL (default: .service)
//
// Using the above configuration values, the default ServiceURLBuilder generates
// remote URLs using the pattern: "$protocol://service_name(-$version)$suffix(:$port)/service_name/endpoint_name".
// The port is only included if the "transport/http/port" parameter is defined.
// If the outgoing message requests a particular service version then that version
// value will also be included in the generated URL.
//
// The built-in ServiceURLBuilder implementation is designed to enable DNS-based
// service discovery. In case that the default implementation is not sufficient
// or you want to test against a locally running server, you can override the
// URLBuilder field with a custom ServiceURLBuilder implementation prior to calling
// Dial().
type Transport struct {
	// Internal locks.
	rwMutex        sync.RWMutex
	serverRefCount int
	clientRefCount int

	// The declared bindings.
	bindings map[string]*binding

	// A channel which is closed to notify the config watcher to exit
	watcherDoneChan chan struct{}

	// A channel which the server goroutine uses to notify us that it has shut down properly.
	serverDoneChan chan struct{}

	// Config options.
	config        *flag.Map
	protocol      *flag.String
	port          *flag.Uint32
	tlsCert       *flag.String
	tlsKey        *flag.String
	tlsStrictMode *flag.Bool
	tlsVerify     *flag.String

	// The listener for http requests.
	listener net.Listener

	// A client implementation that can perform http requests.
	client    requestMaker
	clientErr error

	// URLBuilder can be overriden to implement custom service discovery rules.
	URLBuilder ServiceURLBuilder
}

// New creates a new http transport instance.
func New() *Transport {
	t := &Transport{
		bindings:      make(map[string]*binding, 0),
		config:        config.MapFlag("transport/http"),
		protocol:      config.StringFlag("transport/http/protocol"),
		port:          config.Uint32Flag("transport/http/port"),
		tlsCert:       config.StringFlag("transport/http/tls/certificate"),
		tlsKey:        config.StringFlag("transport/http/tls/key"),
		tlsVerify:     config.StringFlag("transport/http/client/verifycert"),
		tlsStrictMode: config.BoolFlag("transport/http/tls/strict"),

		watcherDoneChan: make(chan struct{}, 0),
	}

	go t.configChangeMonitor()
	setFinalizer(t, func(t *Transport) { close(t.watcherDoneChan) })

	return t
}

// Dial connects to the transport using the specified dial mode. When
// the dial mode is set to DialModeServer the transport will start
// relaying messages to registered bindings.
func (t *Transport) Dial(mode transport.Mode) error {
	t.rwMutex.Lock()
	defer t.rwMutex.Unlock()

	switch mode {
	case transport.ModeServer:
		// Already dialed
		if t.serverRefCount > 0 {
			t.serverRefCount++
			return nil
		}
		err := t.dial()
		if err == nil {
			t.serverRefCount++
		}
		return err
	default:
		// Already dialed
		if t.clientRefCount > 0 {
			t.clientRefCount++
			return nil
		}

		err := t.createClient()
		if err == nil {
			t.clientRefCount++
		}
		return err
	}
}

// Close shuts down the transport.
func (t *Transport) Close(mode transport.Mode) error {
	t.rwMutex.Lock()
	defer t.rwMutex.Unlock()

	switch mode {
	case transport.ModeServer:
		if t.serverRefCount == 0 {
			return transport.ErrTransportClosed
		}

		t.serverRefCount--
		if t.serverRefCount == 0 && t.listener != nil {
			// Close listener and wait for server goroutine to exit
			t.listener.Close()
			<-t.serverDoneChan
			t.serverDoneChan = nil

			t.listener = nil
		}
	case transport.ModeClient:
		if t.clientRefCount == 0 {
			return transport.ErrTransportClosed
		}

		t.clientRefCount--
	}

	return nil
}

// Bind listens for messages send to a particular service and
// endpoint tuple and invokes the supplied handler to process them.
//
// The HTTP transport ignores the version argument as it assumes that routing
// to a service with a particular version is handled at the DNS level. Attempting
// to define multiple versions for the same service and endpoint tuple will cause
// an error to be returned.
func (t *Transport) Bind(version, service, endpoint string, handler transport.Handler) error {
	t.rwMutex.Lock()
	defer t.rwMutex.Unlock()

	mountPoint := fmt.Sprintf("/%s/%s", service, endpoint)
	if _, exists := t.bindings[mountPoint]; exists {
		return fmt.Errorf(
			"binding (version: %q, service: %q, endpoint: %q) already defined",
			version,
			service,
			endpoint,
		)
	}

	t.bindings[mountPoint] = &binding{
		handler:  handler,
		service:  service,
		endpoint: endpoint,
		version:  version,
	}

	return nil
}

// Unbind removes a message handler previously registered by a call to Bind().
// Calling Unbind with a (version, service, endpoint) tuple that is not
// registered has no effect.
func (t *Transport) Unbind(version, service, endpoint string) {
	t.rwMutex.Lock()
	defer t.rwMutex.Unlock()

	mountPoint := fmt.Sprintf("/%s/%s", service, endpoint)
	delete(t.bindings, mountPoint)
}

// Request performs an RPC and returns back a read-only channel for
// receiving the result.
func (t *Transport) Request(reqMsg transport.Message) <-chan transport.ImmutableMessage {
	resChan := make(chan transport.ImmutableMessage, 1)

	go func() {
		// Setup return message by inverting from and to
		resMsg := transport.MakeGenericMessage()
		resMsg.SenderField = reqMsg.Receiver()
		resMsg.SenderEndpointField = reqMsg.ReceiverEndpoint()
		resMsg.ReceiverField = reqMsg.Sender()
		resMsg.ReceiverEndpointField = reqMsg.SenderEndpoint()

		// Map request message to an HTTP request
		payload, _ := reqMsg.Payload()
		httpReq, err := newRequest(
			"POST",
			t.URLBuilder.URL(reqMsg.ReceiverVersion(), reqMsg.Receiver(), reqMsg.ReceiverEndpoint()),
			bytes.NewReader(payload),
		)
		if err != nil {
			resMsg.ErrField = err
			resChan <- resMsg
			close(resChan)
			return
		}

		// Append headers
		for name, value := range reqMsg.Headers() {
			httpReq.Header.Set(headerPrefix+name, value)
		}

		// Always append internal headers after any user-defined headers
		// so that they cannot be accidentally overriden
		httpReq.Header.Set(headerPrefix+requestIDHeader, reqMsg.ID())
		httpReq.Header.Set(headerPrefix+senderHeader, reqMsg.Sender())
		httpReq.Header.Set(headerPrefix+senderEndpointHeader, reqMsg.SenderEndpoint())

		// Exec request
		var httpRes *nethttp.Response
		t.rwMutex.RLock()
		switch t.clientRefCount {
		case 0:
			err = transport.ErrTransportClosed
		default:
			if t.clientErr != nil {
				err = t.clientErr
				break
			}
			httpRes, err = t.client.Do(httpReq)
		}
		t.rwMutex.RUnlock()

		if httpRes != nil {
			defer httpRes.Body.Close()

			switch httpRes.StatusCode {
			case nethttp.StatusOK:
				resMsg.PayloadField, err = readAll(httpRes.Body)
			case nethttp.StatusRequestTimeout:
				err = transport.ErrTimeout
			case nethttp.StatusUnauthorized:
				err = transport.ErrNotAuthorized
			case nethttp.StatusNotFound:
				err = transport.ErrNotFound
			case nethttp.StatusInternalServerError:
				// Check for the error header
				errMsg := httpRes.Header.Get(headerPrefix + errorHeader)
				if errMsg == "" {
					errMsg = "unknown error"
				}
				err = errors.New(errMsg)
			}
		}

		if err != nil {
			resMsg.ErrField = err
			resChan <- resMsg
			close(resChan)
			return
		}

		// Filter request headers and extract the ones we need
		for name, values := range httpRes.Header {
			if !strings.HasPrefix(name, headerPrefix) || values[0] == "" {
				continue
			}

			// Strip the header prefix and check for reserved headers
			name = name[headerPrefixLen:]
			switch name {
			case requestIDHeader, errorHeader, senderHeader, senderEndpointHeader:
			default:
				resMsg.HeadersField[name] = values[0]
			}
		}

		resChan <- resMsg
		close(resChan)
	}()

	return resChan
}

// dial the transport. Must be invoked after acquiring the rwMutex.
func (t *Transport) dial() error {
	// Check that we support the requested protocol
	var err error
	var tlsConfig *tls.Config
	protocol := t.protocol.Get()
	var listenPort string
	switch protocol {
	case "http":
		listenPort = ":http"
	case "https":
		listenPort = ":https"
		tlsConfig, err = t.buildTLSConfig()
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported protocol %q", protocol)
	}

	// If a port is specified use it to override the listener port
	if t.port.HasValue() {
		listenPort = fmt.Sprintf(":%d", t.port.Get())
	}

	// If we are already listening shutdown the old server
	if t.serverRefCount > 0 && t.listener != nil {
		t.listener.Close()
		<-t.serverDoneChan
	}

	if tlsConfig != nil {
		t.listener, err = tlsListen("tcp", listenPort, tlsConfig)
	} else {
		t.listener, err = listen("tcp", listenPort)
	}
	if err != nil {
		t.listener = nil
		return err
	}

	// Start server goroutine
	var wg sync.WaitGroup
	wg.Add(1)
	t.serverDoneChan = make(chan struct{}, 0)
	go func(doneChan chan struct{}) {
		wg.Done()
		srv := nethttp.Server{
			Handler:      nethttp.HandlerFunc(t.mux(t.tlsStrictMode.Get())),
			TLSConfig:    tlsConfig,
			TLSNextProto: make(map[string]func(*nethttp.Server, *tls.Conn, nethttp.Handler), 0),
		}
		srv.Serve(t.listener)
		close(doneChan)
	}(t.serverDoneChan)

	// Wait for server to start
	wg.Wait()
	return nil
}

// configChangeMonitor watches for configuration changes updates the transport
// client and triggers a redial if the transport was already dialed
func (t *Transport) configChangeMonitor() {
	for {
		select {
		case <-t.watcherDoneChan:
			return
		case <-t.config.ChangeChan():
		}

		// If the transport is dialed in server mode force a redial
		t.rwMutex.Lock()
		if t.serverRefCount > 0 {
			t.dial()
		}

		// If the trnasport is dialed in client mode update the client
		if t.clientRefCount > 0 {
			t.clientErr = t.createClient()
		}
		t.rwMutex.Unlock()
	}
}

// mux returns the http server request handler that is invoked (in a goroutine)
// for each incoming HTTP request.
func (t *Transport) mux(strictMode bool) func(nethttp.ResponseWriter, *nethttp.Request) {
	return func(rw nethttp.ResponseWriter, httpReq *nethttp.Request) {
		defer httpReq.Body.Close()

		if strictMode {
			rw.Header().Add("Strict-Transport-Security", "max-age=63072000; includeSubDomains")
		}

		// All endpoint request must use a POST operation
		if httpReq.Method != "POST" {
			rw.WriteHeader(nethttp.StatusNotFound)
			return
		}

		// Lookup binding by matching the request path to a binding mountPoint
		t.rwMutex.RLock()
		binding, exists := t.bindings[httpReq.URL.Path]
		t.rwMutex.RUnlock()

		// No match; return a 404 back
		if !exists {
			rw.WriteHeader(nethttp.StatusNotFound)
			return
		}

		reqMsg := transport.MakeGenericMessage()
		reqMsg.ReceiverField = binding.service
		reqMsg.ReceiverEndpointField = binding.endpoint
		reqMsg.ReceiverVersionField = binding.version
		reqMsg.PayloadField, reqMsg.ErrField = readAll(httpReq.Body)
		if reqMsg.ErrField != nil {
			rw.Header().Set(headerPrefix+errorHeader, reqMsg.ErrField.Error())
			rw.WriteHeader(nethttp.StatusInternalServerError)
			return
		}

		// Filter request headers and extract the ones we need
		for name, values := range httpReq.Header {
			if !strings.HasPrefix(name, headerPrefix) || values[0] == "" {
				continue
			}

			// Strip the header prefix and check for reserved headers
			name = name[headerPrefixLen:]
			switch name {
			case requestIDHeader:
				reqMsg.IDField = values[0]
			case senderHeader:
				reqMsg.SenderField = values[0]
			case senderEndpointHeader:
				reqMsg.SenderEndpointField = values[0]
			default:
				reqMsg.HeadersField[name] = values[0]
			}
		}

		// Setup return message by inverting from and to
		resMsg := transport.MakeGenericMessage()
		resMsg.SenderField = binding.service
		resMsg.SenderEndpointField = binding.endpoint
		resMsg.ReceiverField = reqMsg.SenderField
		resMsg.ReceiverEndpointField = reqMsg.SenderEndpointField

		// Execute handler
		binding.handler.Process(reqMsg, resMsg)

		// Check for common errors and map them to a HTTP status code
		switch resMsg.ErrField {
		case transport.ErrNotFound:
			rw.WriteHeader(nethttp.StatusNotFound)
			return
		case transport.ErrTimeout:
			rw.WriteHeader(nethttp.StatusRequestTimeout)
			return
		case transport.ErrNotAuthorized:
			rw.WriteHeader(nethttp.StatusUnauthorized)
			return
		default:
			rw.Header().Set(headerPrefix+errorHeader, resMsg.ErrField.Error())
			rw.WriteHeader(nethttp.StatusInternalServerError)
			return
		case nil:
		}

		// Export message headers as usrv-tagged headers
		for name, value := range resMsg.HeadersField {
			rw.Header().Set(headerPrefix+name, value)
		}
		rw.Header().Set(requestIDHeader, resMsg.IDField)

		if resMsg.PayloadField != nil {
			rw.Write(resMsg.PayloadField)
		}

		reqMsg.Close()
		resMsg.Close()
	}
}

// createClient generates a new http client instance. This method must be called
// while holding the write mutex.
func (t *Transport) createClient() error {
	if t.URLBuilder == nil {
		t.URLBuilder = newDefaultURLBuilder()
	}

	var client requestMaker
	switch t.protocol.Get() {
	case "http":
		client = &nethttp.Client{}
	case "https":
		var tlsConfig *tls.Config
		var err error

		verifyMode := t.tlsVerify.Get()
		switch verifyMode {
		case tlsVerifySkip:
			tlsConfig = &tls.Config{
				InsecureSkipVerify: true,
			}
		case tlsVerifyAddToCertPool:
			tlsConfig, err = t.buildTLSConfig()
			if err != nil {
				break
			}

			var certPool *x509.CertPool
			certPool, err = systemCertPool()
			if err != nil {
				break
			}

			var certData []byte
			certData, err = readFile(t.tlsCert.Get())
			if err != nil {
				break
			}
			added := certPool.AppendCertsFromPEM(certData)
			if !added {
				err = errAddCertificateToPool
				break
			}

			tlsConfig.RootCAs = certPool
		default:
			err = errInvalidVerifyMode
		}

		if err != nil {
			t.client = nil
			t.clientErr = err
			return err
		}

		client = &nethttp.Client{
			Transport: &nethttp.Transport{
				TLSClientConfig: tlsConfig,
			},
		}
	}

	// Set client
	t.client = client
	return nil
}

// buildTLSConfig validates the TLS configuration options and generates a TLS
// configuration to be used by a client or a server
func (t *Transport) buildTLSConfig() (tlsConfig *tls.Config, err error) {
	// Verify TLS settings and populate tls config
	tlsCert := t.tlsCert.Get()
	tlsKey := t.tlsKey.Get()
	if tlsCert == "" || tlsKey == "" {
		return nil, errMissingCertificate
	}

	// Load certificate
	cert, err := loadX509KeyPair(tlsCert, tlsKey)
	if err != nil {
		return nil, err
	}

	tlsConfig = &tls.Config{Certificates: []tls.Certificate{cert}}
	if t.tlsStrictMode.Get() {
		tlsConfig.MinVersion = tls.VersionTLS12
		tlsConfig.CurvePreferences = []tls.CurveID{tls.CurveP521, tls.CurveP384, tls.CurveP256}
		tlsConfig.PreferServerCipherSuites = true
		tlsConfig.CipherSuites = []uint16{
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
			tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_RSA_WITH_AES_256_CBC_SHA,
		}
	}

	return tlsConfig, nil
}

// Factory is a factory for creating usrv transport instances
// whose concrete implementation is the HTTP transport. This function behaves
// exactly the same as New() but returns back a Transport interface allowing
// it to be used as usrv.DefaultTransportFactory.
func Factory() transport.Provider {
	return New()
}

// SingletonFactory is a factory for creating singleton HTTP transport
// instances. This function returns back a Transport interface allowing it to
// be used as usrv.DefaultTransportFactory.
func SingletonFactory() transport.Provider {
	if singletonInstance == nil {
		singletonInstance = New()
	}
	return singletonInstance
}

func init() {
	config.SetDefaults("transport/http", map[string]string{
		"protocol":          "http",
		"port":              "",
		"tls/certificate":   "",
		"tls/key":           "",
		"tls/strict":        "true",
		"client/hostsuffix": ".service",
		"client/verifycert": tlsVerifyAddToCertPool,
	})
}
