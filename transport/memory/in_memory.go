// Package memory provides an in-memory usrv transport using go channels.
package memory

import (
	"fmt"
	"sync"

	"github.com/achilleasa/usrv/transport"
)

var (
	singletonInstance transport.Provider = &Transport{}
)

// Transport implements the in-memory transport. It uses channels and go-routines
// to facilitate the exchange of messages making it very easy to use when
// writing tests.
type Transport struct {
	rwMutex  sync.RWMutex
	refCount int

	bindings map[string]transport.Handler
}

// New creates a new in-memory transport instance.
func New() *Transport {
	return &Transport{
		bindings: make(map[string]transport.Handler),
	}
}

// Dial connects the transport and starts relaying messages.
func (t *Transport) Dial(_ transport.Mode) error {
	t.rwMutex.Lock()
	defer t.rwMutex.Unlock()

	t.refCount++
	return nil
}

// Close shuts down the transport.
func (t *Transport) Close(mode transport.Mode) error {
	t.rwMutex.Lock()
	defer t.rwMutex.Unlock()

	if t.refCount == 0 {
		return transport.ErrTransportClosed
	}

	t.refCount--
	return nil
}

// Bind listens for messages send to a particular version, service and
// endpoint tuple and invokes the supplied handler to process them.
//
// Calls to bind will also register a binding without a version to allow
// local clients to target this endpoint if no version is specified.
func (t *Transport) Bind(version, service, endpoint string, handler transport.Handler) error {
	t.rwMutex.Lock()
	defer t.rwMutex.Unlock()

	key := fmt.Sprintf("%s%s/%s", version, service, endpoint)
	versionlessKey := fmt.Sprintf("%s/%s", service, endpoint)
	if t.bindings[key] != nil {
		return fmt.Errorf(
			"binding (version: %q, service: %q, endpoint: %q) already defined",
			version,
			service,
			endpoint,
		)
	}
	t.bindings[key] = handler
	t.bindings[versionlessKey] = handler

	return nil
}

// Unbind removes a message handler previously registered by a call to Bind().
// Calling Unbind with a (version, service, endpoint) tuple that is not
// registered has no effect.
func (t *Transport) Unbind(version, service, endpoint string) {
	t.rwMutex.Lock()
	defer t.rwMutex.Unlock()

	key := fmt.Sprintf("%s%s/%s", version, service, endpoint)
	versionlessKey := fmt.Sprintf("%s/%s", service, endpoint)
	delete(t.bindings, key)
	delete(t.bindings, versionlessKey)
}

// Request performs an RPC and returns back a read-only channel for
// receiving the result.
func (t *Transport) Request(msg transport.Message) <-chan transport.ImmutableMessage {
	resChan := make(chan transport.ImmutableMessage, 1)
	go func() {
		// Build destination key for looking up the binding
		version := msg.ReceiverVersion()
		key := fmt.Sprintf("%s%s/%s", version, msg.Receiver(), msg.ReceiverEndpoint())

		t.rwMutex.RLock()
		handler, exists := t.bindings[key]
		closed := t.refCount == 0
		t.rwMutex.RUnlock()

		res := transport.MakeGenericMessage()
		res.SenderField = msg.Receiver()
		res.SenderEndpointField = msg.ReceiverEndpoint()
		res.ReceiverField = msg.Sender()
		res.ReceiverEndpointField = msg.SenderEndpoint()

		// Unknown binding
		switch {
		case closed:
			res.SetPayload(nil, transport.ErrTransportClosed)
		case !exists:
			res.SetPayload(nil, transport.ErrNotFound)
		default:
			handler.Process(msg, res)
		}

		resChan <- res
		close(resChan)
	}()

	return resChan
}

// Factory is a factory for creating usrv transport instances
// whose concrete implementation is the InMemory transport. This function behaves
// exactly the same as New() but returns back a Transport interface allowing
// it to be used as usrv.DefaultTransportFactory.
func Factory() transport.Provider {
	return New()
}

// SingletonFactory is a factory for creating singleton InMemory transport
// instances. This function returns back a Transport interface allowing it to
// be used as usrv.DefaultTransportFactory.
func SingletonFactory() transport.Provider {
	return singletonInstance
}
