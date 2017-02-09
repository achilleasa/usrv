package server

import (
	"context"
	"errors"
	"reflect"
)

var (
	errEndpointHasNoName           = errors.New("endpoint name cannot be empty")
	errEndpointHandlerBadSignature = errors.New("endpoint handler should be a function with signature func(context.Context, interface{}, interface{}) error")
	errEndpointHandlerBadArg0      = errors.New("endpoint handler should accept a context.Context value as its first argument")
	errEndpointHandlerBadArg1      = errors.New("endpoint handler should accept a pointer to a struct as its second argument")
	errEndpointHandlerBadArg2      = errors.New("endpoint handler should accept a pointer to a struct as its third argument")
)

// Endpoint defines a service endpoint which is exposed by the server via a transport.
type Endpoint struct {
	// The name of this endpoint.
	Name string

	// The endpoint's description.
	Description string

	// The handler responsible for serving endpoint requests. Handlers are
	// expected to adhere to the following signature:
	//
	// func(ctx context.Context, request interface{}, response interface{}) error
	//
	// The request and response parameters must be pointers to structs that
	// define the expected request and response messages for this endpoint.
	//
	// Usrv uses reflection to analyze the handler types and automatically
	// generate a Middleware that deals with request and response
	// marshaling/unmarshaling to the appropriate message format used by the
	// underlying transport.
	//
	// The handler should process the incoming request object, update the response
	// object and then return. Returning signals that the request has been processed.
	//
	// The server may opt to recycle the supplied request and response objects for
	// handling other requests to reduce memory allocations and GC pressure. It
	// is not valid to operate on the request or response objects after or
	// concurrently with the completion of the handler.
	//
	// If the handler panics, the server (the caller of the handler) assumes that
	// the effect of the panic was isolated to the active request. The panic will
	// be recovered and handled by the server's panic handler.
	Handler interface{}

	// An optional set of middleware factories that are used to generate a
	// middleware chain that wraps the endpoint handler. If the slice contains
	// factories [f1, f2, f3] then the server will generate a new handler
	// defined as f1( f2( f3(endpoint handler) ) ).
	MiddlewareFactories []MiddlewareFactory
}

// validate ensures that the endpoint definition is valid.
func (ep *Endpoint) validate() error {
	if ep.Name == "" {
		return errEndpointHasNoName
	}

	handlerType := reflect.TypeOf(ep.Handler)
	if handlerType.Kind() != reflect.Func ||
		handlerType.NumIn() != 3 ||
		handlerType.NumOut() != 1 ||
		handlerType.Out(0).Name() != "error" {
		return errEndpointHandlerBadSignature
	}

	var ctxInterface *context.Context
	if !handlerType.In(0).Implements(reflect.TypeOf(ctxInterface).Elem()) {
		return errEndpointHandlerBadArg0
	}

	if handlerType.In(1).Kind() != reflect.Ptr || handlerType.In(1).Elem().Kind() != reflect.Struct {
		return errEndpointHandlerBadArg1
	}

	if handlerType.In(2).Kind() != reflect.Ptr || handlerType.In(2).Elem().Kind() != reflect.Struct {
		return errEndpointHandlerBadArg2
	}

	return nil
}
