package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"runtime"
	"sync"

	"github.com/achilleasa/usrv"
	"github.com/achilleasa/usrv/encoding"
	"github.com/achilleasa/usrv/transport"
)

var (
	// DefaultPanicWriter is a sink where the server's default panic handler
	// writes its output when a panic is recovered.
	DefaultPanicWriter io.Writer = os.Stderr

	// CtxFieldServiceName defines the context field name where the server
	// stores the service name that responds to an incoming request.
	CtxFieldServiceName interface{} = "Service"

	// CtxFieldEndpointName defines the context field name where the server
	// stores the endpoint name that responds to an incoming request.
	CtxFieldEndpointName interface{} = "Endpoint"

	errServeAlreadyCalled = errors.New("server is already listening for incoming requests")
)

// A PanicHandler is invoked by the server when a panic is recovered while
// processing an incoming request.
type PanicHandler func(error)

// Server implements an RPC server that binds a set of endpoints to a transport
// instance and invokes the endpoint request handlers to process incoming requests
//
// The server uses a codec instance to marshal and unmarshal the request and
// response objects accepted by the endpoint handlers into the low-level message
// format used by the attached transport. Unless overriden by the WithCodec config
// option, the server will invoke usrv.DefaultCodecFactory to fetch a codec instance.
//
// Unless overriden with the WithTransport config option, the server will invoke
// usrv.DefaultTransportFactory to fetch a transport instance.
//
// The server automatically recoveres and handling any panics while a request is
// being handled. A built-in panic handler implementation is used that simply
// write the error and stack-trace to DefaultPanicWriter. However, the panic handler
// can be overriden using the WithPanicHandler config option.
type Server struct {
	// A mutext protecting access to the server fields.
	mutex sync.Mutex

	// The transport used by the server.
	transport transport.Transport

	// A codec instance for handling marshaling/unmarshaling.
	codec encoding.Codec

	// A function for handling panics inside endpoint handlers.
	panicHandler PanicHandler

	// The name of the service exposed by the server.
	serviceName string

	// The service version
	serviceVersion string

	// The set of defined endpoints.
	endpoints []*Endpoint

	// A channel to signal the server go-routine to shut down.
	doneChan chan struct{}
}

// New creates a new server instance for the given service name and applies
// any supplied server options.
func New(serviceName string, options ...Option) (*Server, error) {
	srv := &Server{
		serviceName: serviceName,
		endpoints:   make([]*Endpoint, 0),
	}

	// Apply options
	var err error
	for _, opt := range options {
		err = opt(srv)
		if err != nil {
			return nil, err
		}
	}

	// Apply defaults
	srv.setDefaults()

	return srv, nil
}

// RegisterEndpoints adds one or more endpoints to the set of endpoints exposed
// by this server instance. This method will return an error if the supplied
// endpoint fails validation.
func (s *Server) RegisterEndpoints(endpoints ...*Endpoint) error {
	var err error
	for _, ep := range endpoints {
		err = ep.validate()
		if err != nil {
			return err
		}
	}

	s.endpoints = append(s.endpoints, endpoints...)
	return nil
}

// Listen registers all endpoints with the server's transport and begins serving
// incoming requests.
//
// Calls to Listen block till the server's Close() method is invoked.
func (s *Server) Listen() error {
	s.mutex.Lock()

	// Serve already called
	if s.doneChan != nil {
		s.mutex.Unlock()
		return errServeAlreadyCalled
	}

	var err error
	for _, endpoint := range s.endpoints {
		err = s.transport.Bind(s.serviceVersion, s.serviceName, endpoint.Name, s.generateHandler(endpoint))
		if err != nil {
			s.mutex.Unlock()
			return err
		}
	}

	err = s.transport.Dial()
	if err != nil {
		s.mutex.Unlock()
		return err
	}

	s.doneChan = make(chan struct{}, 0)
	s.mutex.Unlock()

	// Wait for a stop signal
	<-s.doneChan

	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Shutdown transport and ack the close request
	s.transport.Close()
	s.doneChan <- struct{}{}

	return nil
}

// Close shuts down a server that is listening for incoming connections. After
// calling close, any blocked calls to Listen() will be unblocked.
//
// Calling Close on a server not listening for incoming requests has no effect.
func (s *Server) Close() {
	s.mutex.Lock()
	// Already closed
	if s.doneChan == nil {
		s.mutex.Unlock()
		return
	}

	// Signal server to shutdown
	s.doneChan <- struct{}{}
	s.mutex.Unlock()

	// Wait for the server to ack the close request
	<-s.doneChan

	// Cleanup the channel
	s.mutex.Lock()
	defer s.mutex.Unlock()
	close(s.doneChan)
	s.doneChan = nil

}

// setDefaults applies default settings for fields not set by a server option.
func (s *Server) setDefaults() {
	if s.transport == nil {
		s.transport = usrv.DefaultTransportFactory()
	}

	if s.codec == nil {
		s.codec = usrv.DefaultCodecFactory()
	}

	if s.panicHandler == nil {
		s.panicHandler = defaultPanicHandler
	}
}

// generateHandler generates a handler for an endpoint which can be passed to a
// transport's Bind call. The handler executes any defined middleware, applies
// marshaling and unmarshaling via the server's codec and then invokes the application
// defined endpoint handler. It also registers hooks to recover panics and
// pass them to the registered panic handler.
//
// This method assumes that the endpoint has been properly validated.
func (s *Server) generateHandler(ep *Endpoint) transport.Handler {
	handlerType := reflect.TypeOf(ep.Handler)
	handlerFn := reflect.ValueOf(ep.Handler)

	// Analyze the endpoint handler request and response arguments and
	// generate an unmarshaler for the request and a marshaler for the response.
	reqType := handlerType.In(1).Elem()
	resType := handlerType.In(2).Elem()

	resMarshaler := s.codec.Marshaler()
	reqUnmarshaler := s.codec.Unmarshaler()

	reqPool := sync.Pool{
		New: func() interface{} {
			return reflect.New(reqType).Interface()
		},
	}
	resPool := sync.Pool{
		New: func() interface{} {
			return reflect.New(resType).Interface()
		},
	}

	// Generate a middleware that handles marshaling and invocation of the endpoint handler.
	var middlewareChain Middleware = MiddlewareFunc(func(ctx context.Context, req transport.ImmutableMessage, res transport.Message) {
		var reqObj, resObj interface{}
		defer func() {
			if reqObj != nil {
				reqPool.Put(reqObj)
			}
			if resObj != nil {
				resPool.Put(resObj)
			}
		}()

		// Unmarshal request
		reqObj = reqPool.Get()
		defer reqPool.Put(reqObj)
		reqPayload, _ := req.Payload()
		err := reqUnmarshaler(reqPayload, reqObj)
		if err != nil {
			res.SetPayload(nil, err)
			return
		}

		// Execute handler
		resObj = resPool.Get()
		retVals := handlerFn.Call(
			[]reflect.Value{
				reflect.ValueOf(ctx),
				reflect.ValueOf(reqObj),
				reflect.ValueOf(resObj),
			},
		)
		ret := retVals[0].Interface()
		if ret != nil {
			res.SetPayload(nil, ret.(error))
			return
		}

		res.SetPayload(resMarshaler(resObj))
	})

	// Call each middleware factory in the endpoint factory list in reverse
	// order and wrap the generated handler.
	if ep.MiddlewareFactories != nil {
		for index := len(ep.MiddlewareFactories) - 1; index >= 0; index-- {
			if ep.MiddlewareFactories[index] == nil {
				continue
			}
			middlewareChain = ep.MiddlewareFactories[index](middlewareChain)
		}
	}

	// Apply global middleware in reverse order
	for index := len(globalMiddlewareFactories) - 1; index >= 0; index-- {
		middlewareChain = globalMiddlewareFactories[index](middlewareChain)
	}

	// Return a compatible transport handler
	return transport.HandlerFunc(func(req transport.ImmutableMessage, res transport.Message) {
		if s.panicHandler != nil {
			defer func() {
				if r := recover(); r != nil {
					// Map recovered value to an error that we can feed to the panic handler
					var err error
					switch errVal := r.(type) {
					case error:
						err = errVal
					default:
						err = errors.New(fmt.Sprint(errVal))
					}
					s.panicHandler(err)
					res.SetPayload(nil, fmt.Errorf("remote endpoint paniced: %s", err))
				}
			}()
		}

		// Setup initial request context and inject the service and endpoint name.
		ctx := context.WithValue(
			context.WithValue(context.Background(), CtxFieldServiceName, s.serviceName),
			CtxFieldEndpointName,
			ep.Name,
		)
		middlewareChain.Handle(ctx, req, res)
	})
}

// defaultPanicHandler implements a PanicHandler that writes its output to
// DefaultPanicWriter.
func defaultPanicHandler(err error) {
	stackBuf := make([]byte, 4096)
	runtime.Stack(stackBuf, false)

	msg := fmt.Sprintf(
		"recovered from panic: %v\n\nstacktrace:\n%v\n",
		err,
		string(stackBuf),
	)

	DefaultPanicWriter.Write([]byte(msg))
}
