package provider

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	nethttp "net/http"
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
)

// A set of endpoints that can be hooked by tests
var (
	listen     = net.Listen
	newRequest = nethttp.NewRequest
	readAll    = ioutil.ReadAll
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
//  - transport/http/protocol. The protocol to use (default: http)
//  - transport/http/port. The port to listen for incoming connections; if unspecified,
//    the default port for the protocol will be used (default: )
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

	// A channel which the server goroutine uses to notify us that it has
	// shut down properly.
	serverDoneChan chan struct{}

	// Server config options.
	protocol *flag.StringFlag
	port     *flag.Uint32Flag

	// The listener for http requests.
	listener net.Listener

	// URLBuilder can be overriden to implement custom service discovery rules.
	URLBuilder ServiceURLBuilder
}

// NewHTTP creates a new http transport instance.
func NewHTTP() *HTTP {
	t := &HTTP{
		bindings: make([]binding, 0),
		protocol: config.StringFlag("transport/http/protocol"),
		port:     config.Uint32Flag("transport/http/port"),
	}

	return t
}

// Dial connects the transport and starts relaying messages.
func (t *HTTP) Dial() error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// Check that we support the requested protocol
	protocol := t.protocol.Get()
	var listenPort string
	switch protocol {
	case "http":
		listenPort = ":http"
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

	var err error
	t.listener, err = listen("tcp", listenPort)
	if err != nil {
		return err
	}

	// Start server goroutine
	t.serverDoneChan = make(chan struct{}, 0)
	go func() {
		nethttp.Serve(t.listener, nethttp.HandlerFunc(t.mux()))
		close(t.serverDoneChan)
	}()

	t.dialed = true
	return nil
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
			reqMsg.Close()
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
		httpRes, err := nethttp.DefaultClient.Do(httpReq)

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
			reqMsg.Close()
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
		reqMsg.Close()
		close(resChan)
	}()

	return resChan
}

// Mux returns the http server request handler that is invoked (in a goroutine)
// for each incoming HTTP request.
func (t *HTTP) mux() func(nethttp.ResponseWriter, *nethttp.Request) {
	return func(rw nethttp.ResponseWriter, httpReq *nethttp.Request) {
		defer httpReq.Body.Close()

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

func init() {
	config.SetDefaults("transport/http", map[string]string{
		"protocol":          "http",
		"port":              "",
		"client/hostsuffix": ".service",
	})
}
