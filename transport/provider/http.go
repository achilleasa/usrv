package provider

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

// Binding encapsulates the details of a service/endpoint combination and
// a handler for requests to it.
type binding struct {
	handler transport.Handler

	// The request path that for this binding. It uses the following pattern
	// "/:service/:endpoint"
	mountPoint string

	// The service and endpoint combination for this binding. We store this
	// separately for quickly populating incoming message fields.
	service  string
	endpoint string
}

// ServiceURLBuilder defines an interface for mapping a service, endpoint
// combination into a remote path URL.
//
// URL receives the service and endpoint name as input and returns back a URL.
type ServiceURLBuilder interface {
	URL(service, endpoint string) string
}

type defaultURLBuilder struct {
	config *flag.MapFlag
}

func newDefaultURLBuilder() *defaultURLBuilder {
	return &defaultURLBuilder{
		config: config.MapFlag("transport/http"),
	}
}

func (b defaultURLBuilder) URL(service, endpoint string) string {
	cfg := b.config.Get()

	port := cfg["port"]
	if port != "" {
		port = ":" + port
	}

	return fmt.Sprintf(
		"%s://%s%s%s/%s/%s",
		cfg["protocol"],
		service,
		cfg["client/hostsuffix"],
		port,
		service,
		endpoint,
	)
}

type requestMaker interface {
	Do(req *nethttp.Request) (*nethttp.Response, error)
}

type requestMakerThatAlwaysFails struct {
	err error
}

func (rm *requestMakerThatAlwaysFails) Do(req *nethttp.Request) (*nethttp.Response, error) {
	return nil, rm.err
}

// HTTP implements a usrv transport over HTTP.
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
// remote URLs using the pattern: "$protocol://service_name$suffix(:$port)/service_name/endpoint_name".
// (port is only included if its defined). The built-in ServiceURLBuilder implementation
// is designed to enable DNS-based service discovery. In case that the default
// implementation is not sufficient or you want to test against a locally running
// server, you can override the URLBuilder field with a custom ServiceURLBuilder
// implementation prior to calling Dial().
type HTTP struct {
	// Internal locks.
	mutex  sync.Mutex
	dialed bool

	// The list of declared bindings.
	bindings []binding

	// A channel which is closed to notify the config watcher to exit
	watcherDoneChan chan struct{}

	// A channel which the server goroutine uses to notify us that it has shut down properly.
	serverDoneChan chan struct{}

	// Config options.
	config        *flag.MapFlag
	protocol      *flag.StringFlag
	port          *flag.Uint32Flag
	tlsCert       *flag.StringFlag
	tlsKey        *flag.StringFlag
	tlsStrictMode *flag.BoolFlag
	tlsVerify     *flag.StringFlag

	// The listener for http requests.
	listener net.Listener

	// A client implementation that can perform http requests.
	client requestMaker

	// A RW mutex allowing access to the client
	clientMutex sync.RWMutex

	// URLBuilder can be overriden to implement custom service discovery rules.
	URLBuilder ServiceURLBuilder
}

// NewHTTP creates a new http transport instance.
func NewHTTP() *HTTP {
	t := &HTTP{
		bindings:      make([]binding, 0),
		config:        config.MapFlag("transport/http"),
		protocol:      config.StringFlag("transport/http/protocol"),
		port:          config.Uint32Flag("transport/http/port"),
		tlsCert:       config.StringFlag("transport/http/tls/certificate"),
		tlsKey:        config.StringFlag("transport/http/tls/key"),
		tlsVerify:     config.StringFlag("transport/http/client/verifycert"),
		tlsStrictMode: config.BoolFlag("transport/http/tls/strict"),

		watcherDoneChan: make(chan struct{}, 0),
	}

	t.createClient()
	go t.configChangeMonitor()
	setFinalizer(t, func(t *HTTP) { close(t.watcherDoneChan) })

	return t
}

// Dial connects the transport and starts relaying messages.
func (t *HTTP) Dial() error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	return t.dial()
}

// Close shuts down the transport.
func (t *HTTP) Close() error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if !t.dialed {
		return transport.ErrTransportClosed
	}

	// Close listener and wait for server goroutine to exit
	t.listener.Close()
	<-t.serverDoneChan
	t.serverDoneChan = nil

	t.listener = nil
	t.dialed = false

	return nil
}

// Bind listens for messages send to a particular service and
// endpoint combination and invokes the supplied handler to process them.
//
// Bindings can only be established on a closed transport. Calls to Bind
// after a call to Dial will result in an error.
func (t *HTTP) Bind(service, endpoint string, handler transport.Handler) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if t.dialed {
		return transport.ErrTransportAlreadyDialed
	}

	mountPoint := fmt.Sprintf("/%s/%s", service, endpoint)
	for _, existingBinding := range t.bindings {
		if existingBinding.mountPoint == mountPoint {
			return fmt.Errorf("binding %q already defined", strings.TrimLeft(mountPoint, "/"))
		}
	}
	t.bindings = append(t.bindings, binding{
		handler:    handler,
		mountPoint: mountPoint,
		service:    service,
		endpoint:   endpoint,
	})

	return nil
}

// Request performs an RPC and returns back a read-only channel for
// receiving the result.
func (t *HTTP) Request(reqMsg transport.Message) <-chan transport.ImmutableMessage {
	resChan := make(chan transport.ImmutableMessage, 1)

	go func() {
		// Setup return message by inverting from and to
		resMsg := transport.MakeGenericMessage()
		resMsg.SenderField = reqMsg.Receiver()
		resMsg.SenderEndpointField = reqMsg.ReceiverEndpoint()
		resMsg.ReceiverField = reqMsg.Sender()
		resMsg.ReceiverEndpointField = reqMsg.SenderEndpoint()

		// Acquire read lock
		t.clientMutex.RLock()

		// Map request message to an HTTP request
		payload, _ := reqMsg.Payload()
		httpReq, err := newRequest(
			"POST",
			t.URLBuilder.URL(reqMsg.Receiver(), reqMsg.ReceiverEndpoint()),
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
		httpRes, err := t.client.Do(httpReq)

		// Release lock
		t.clientMutex.RUnlock()

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

// The actual implementation of dial. Must be invoked after acquiring the mutex.
func (t *HTTP) dial() error {

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
	if t.dialed {
		t.listener.Close()
		<-t.serverDoneChan
		t.dialed = false
	}

	if tlsConfig != nil {
		t.listener, err = tlsListen("tcp", listenPort, tlsConfig)
	} else {
		t.listener, err = listen("tcp", listenPort)
	}
	if err != nil {
		return err
	}

	// Update client
	err = t.createClient()
	if err != nil {
		t.listener.Close()
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

	t.dialed = true
	return nil
}

// ConfigChangeMonitor watches for configuration changes updates the transport
// client and triggers a redial if the transport was already dialed
func (t *HTTP) configChangeMonitor() {
	for {
		select {
		case <-t.watcherDoneChan:
			return
		case <-t.config.ChangeChan():
		}

		// If the transport is dialed force a redial; this also updates the client
		t.mutex.Lock()
		if t.dialed {
			t.dial()
			t.mutex.Unlock()
			continue
		}

		// Just update the client
		t.createClient()
		t.mutex.Unlock()
	}
}

// Mux returns the http server request handler that is invoked (in a goroutine)
// for each incoming HTTP request.
func (t *HTTP) mux(strictMode bool) func(nethttp.ResponseWriter, *nethttp.Request) {
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
		for _, binding := range t.bindings {
			if binding.mountPoint != httpReq.URL.Path {
				continue
			}

			reqMsg := transport.MakeGenericMessage()
			reqMsg.ReceiverField = binding.service
			reqMsg.ReceiverEndpointField = binding.endpoint
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
			return
		}

		// No match; return a 404 back
		rw.WriteHeader(nethttp.StatusNotFound)
	}
}

// CreateClient generates a new http client instance and atomically replaces the
// one present in the transport.
func (t *HTTP) createClient() error {
	t.clientMutex.Lock()
	defer t.clientMutex.Unlock()

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
				t.client = &requestMakerThatAlwaysFails{err: err}
				return err
			}

			certPool, err := systemCertPool()
			if err != nil {
				t.client = &requestMakerThatAlwaysFails{err: err}
				return err
			}

			certData, err := readFile(t.tlsCert.Get())
			if err != nil {
				t.client = &requestMakerThatAlwaysFails{err: err}
				return err
			}
			added := certPool.AppendCertsFromPEM(certData)
			if !added {
				err = errAddCertificateToPool
				t.client = &requestMakerThatAlwaysFails{err: err}
				return err
			}

			tlsConfig.RootCAs = certPool
		default:
			return errInvalidVerifyMode
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

// BuildTLSConfig validates the TLS configuration options and generates a TLS
// configuration to be used by a client or a server
func (t *HTTP) buildTLSConfig() (tlsConfig *tls.Config, err error) {
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

// HTTPTransportFactory is a factory for creating usrv transport instances
// whose concrete implementation is the HTTP transport. This function behaves
// exactly the same as NewHTTP() but returns back a Transport interface allowing
// it to be used as usrv.DefaultTransportFactory.
func HTTPTransportFactory() transport.Transport {
	return NewHTTP()
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
