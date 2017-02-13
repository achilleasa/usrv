// Package memory provides an in-memory usrv transport using go channels.
package memory

import (
	"fmt"
	"sync"

	"github.com/achilleasa/usrv/transport"
)

var (
	_ transport.Transport = &Transport{}
)

// Transport implements the in-memory transport. It uses channels and go-routines
// to facilitate the exchange of messages making it very easy to use when
// writing tests.
type Transport struct {
	mutex  sync.Mutex
	dialed bool

	bindings map[string]transport.Handler
}

// New creates a new in-memory transport instance.
func New() *Transport {
	return &Transport{
		bindings: make(map[string]transport.Handler, 0),
	}
}

// Dial connects the transport and starts relaying messages.
func (t *Transport) Dial() error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	t.dialed = true
	return nil
}

// Close shuts down the transport.
func (t *Transport) Close() error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if !t.dialed {
		return transport.ErrTransportClosed
	}

	t.dialed = false

	return nil
}

// Bind listens for messages send to a particular version, service and
// endpoint tuple and invokes the supplied handler to process them.
//
// Calls to bind will also register a binding without a version to allow
// local clients to target this endpoint if no version is specified.
//
// Bindings can only be established on a closed transport. Calls to Bind
// after a call to Dial will result in an error.
func (t *Transport) Bind(version, service, endpoint string, handler transport.Handler) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if t.dialed {
		return transport.ErrTransportAlreadyDialed
	}

	if version != "" {
		version = "-" + version
	}

	key := fmt.Sprintf("%s%s/%s", service, version, endpoint)
	versionlessKey := fmt.Sprintf("%s/%s", service, endpoint)
	if t.bindings[key] != nil {
		return fmt.Errorf("binding %q already defined", key)
	}
	t.bindings[key] = handler
	t.bindings[versionlessKey] = handler

	return nil
}

// Request performs an RPC and returns back a read-only channel for
// receiving the result.
func (t *Transport) Request(msg transport.Message) <-chan transport.ImmutableMessage {
	resChan := make(chan transport.ImmutableMessage, 1)

	// Build destination key for looking up the binding
	version := msg.ReceiverVersion()
	if version != "" {
		version = "-" + version
	}
	key := fmt.Sprintf("%s%s/%s", msg.Receiver(), version, msg.ReceiverEndpoint())

	// This is required to prevent go test -race from flagging this as a false-positive data race.
	t.mutex.Lock()
	defer t.mutex.Unlock()

	go func(handler transport.Handler, req transport.Message, resChan chan transport.ImmutableMessage) {
		res := transport.MakeGenericMessage()
		res.SenderField = msg.Receiver()
		res.SenderEndpointField = msg.ReceiverEndpoint()
		res.ReceiverField = msg.Sender()
		res.ReceiverEndpointField = msg.SenderEndpoint()

		// Unknown target; return back an error
		if handler == nil {
			res.SetPayload(nil, transport.ErrNotFound)
		} else {
			handler.Process(req, res)
		}

		resChan <- res
		close(resChan)
	}(t.bindings[key], msg, resChan)

	return resChan
}

// Factory is a factory for creating usrv transport instances
// whose concrete implementation is the InMemory transport. This function behaves
// exactly the same as New() but returns back a Transport interface allowing
// it to be used as usrv.DefaultTransportFactory.
func Factory() transport.Transport {
	return New()
}
