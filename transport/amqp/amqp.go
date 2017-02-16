package amqp

import (
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/achilleasa/usrv/config"
	"github.com/achilleasa/usrv/config/flag"
	"github.com/achilleasa/usrv/transport"
	amqpClient "github.com/streadway/amqp"
)

// Amqp status codes that are mapped to usrv errors
const (
	replyCodeNotAuthorized uint16 = 403
	replyCodeNotFound             = 404
	replyCodeNoConsumers          = 313

	contentTypeUsrvData = "application/usrv+data"
	contentTypeError    = "application/usrv+error"

	usrvExchangeName = "x-usrv"
	usrvExchangeType = "direct"
)

var (
	// A singleton provider instance returned by the factory.
	singleton transport.Provider = New()
)

type amqpChannel interface {
	Ack(tag uint64, multiple bool) error
	Nack(tag uint64, multiple bool, requeue bool) error
	NotifyReturn(c chan amqpClient.Return) chan amqpClient.Return
	Publish(exchange, key string, mandatory, immediate bool, msg amqpClient.Publishing) error
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqpClient.Table) error
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqpClient.Table) (amqpClient.Queue, error)
	QueueBind(name, key, exchange string, noWait bool, args amqpClient.Table) error
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqpClient.Table) (<-chan amqpClient.Delivery, error)
	Close() error
}

var dialAndGetChan = func(amqpURI string) (amqpChannel, io.Closer, error) {
	amqpConn, err := amqpClient.Dial(amqpURI)
	if err != nil {
		return nil, nil, err
	}

	amqpChan, err := amqpConn.Channel()
	if err != nil {
		amqpConn.Close()
		return nil, nil, err
	}

	return amqpChan, amqpConn, nil
}

// rpc instances are used to send client requests. They are sent via a channel
// to the dispatch worker and consist of a request message and a channel for
// receiving th remote endpoint response.
type rpc struct {
	req     transport.ImmutableMessage
	resChan chan transport.ImmutableMessage
}

// makeResponse is a helper method that initializes a response message and
// populates its sender and receiver fields based on the information contained
// in the outgoing request message.
func (r *rpc) makeResponse() transport.Message {
	res := transport.MakeGenericMessage()
	res.SenderField = r.req.Receiver()
	res.SenderEndpointField = r.req.ReceiverEndpoint()
	res.ReceiverField = r.req.Sender()
	res.ReceiverEndpointField = r.req.SenderEndpoint()
	return res
}

// respondWithError is a helper method that initializes a response message using
// the specified error and sends it to the rpc response channel. After a call to
// respondWithError, the rpc response channel will be closed.
func (r *rpc) respondWithError(err error) {
	res := r.makeResponse()
	res.SetPayload(nil, err)
	r.resChan <- res
	close(r.resChan)
}

// respond is a helper method that sends a response message to the rpc response
// channe . After a call to respond, the rpc response channel will be closed.
func (r *rpc) respond(res transport.Message) {
	r.resChan <- res
	close(r.resChan)
}

// Binding encapsulates the details of a service/endpoint combination and
// a handler for requests to it.
type binding struct {
	handler transport.Handler

	// The service and endpoint combination for this binding. We store this
	// separately for quickly populating incoming message fields./Request
	version  string
	service  string
	endpoint string
}

func routingKey(version, service, endpoint string) string {
	return fmt.Sprintf("%s/%s/%s", version, service, endpoint)
}

type Transport struct {
	rwMutex sync.RWMutex

	// Reference counters
	serverRefCount int
	clientRefCount int

	// A flag for providing the broken location.
	amqpURI *flag.String

	// A private queue where the transport receives responses for outgoing
	// requests (client mode). It is used as the ReplyTo field in outgoing messages.
	amqpReplyQueueName string

	// A private queue where the transport receives incoming requests (server mode).
	// The transport defines the usrv direct exchange (usrvExchangeName) and
	// then asks the broker to relay messages for the bindings' routing keys
	// to this queue.
	amqpRequestQueueName string

	amqpConnCloser io.Closer
	amqpChan       amqpChannel

	// The dispatch worker monitors this channel for usrv outgoing requests (client mode).
	outgoingReqChan chan *rpc

	// This channel receives messages sent to amqpReplyQueueName (client mode).
	incomingResChan <-chan amqpClient.Delivery

	// This channel receives any Un-routable messages sent by the client.
	failedDispatchChan <-chan amqpClient.Return

	// This channel receives messages sent to amqpRequestQueueName (server mode).
	incomingReqChan <-chan amqpClient.Delivery

	clientReadyChan chan struct{}
	serverReadyChan chan struct{}

	bindings map[string]*binding
}

// New creates a new amqp transport instance.
func New() *Transport {
	return &Transport{
		bindings: make(map[string]*binding, 0),
		amqpURI:  config.StringFlag("transport/amqp/uri"),
	}
}

// Dial connects the transport and starts relaying messages.
func (t *Transport) Dial(mode transport.Mode) error {
	t.rwMutex.Lock()
	defer t.rwMutex.Unlock()

	if mode == transport.ModeServer {
		return t.dialServer()
	}

	return t.dialClient()
}

func (t *Transport) dialAndVerifyExchange() error {
	// Channel is already opened
	if t.amqpChan != nil {
		return nil
	}

	var err error
	t.amqpChan, t.amqpConnCloser, err = dialAndGetChan(t.amqpURI.Get())
	if err != nil {
		return err
	}

	// Declare usrv exchange
	err = t.amqpChan.ExchangeDeclare(
		usrvExchangeName,
		usrvExchangeType,
		false, // durable
		true,  // auto-delete
		false, // internal
		false, // no-wait
		nil,   // args
	)

	if err != nil {
		t.amqpConnCloser.Close()
		return err
	}

	return nil
}

// dialClient dials the transport and sets up a private queue for receiving
// responses for outgoing requests. This method must be called while holding the mutex.
func (t *Transport) dialClient() error {
	if t.clientRefCount > 0 {
		t.clientRefCount++
		return nil
	}

	err := t.dialAndVerifyExchange()
	if err != nil {
		return err
	}

	// Allocate a private queue for receiving responses
	resQueue, err := t.amqpChan.QueueDeclare(
		"",    // let amqp assign a random name
		false, // non-durable
		true,  // auto-delete
		true,  // exclusive
		false, // noWait
		nil,   // args table
	)

	if err != nil {
		return err
	}

	// Consuming messages sent to the response queue
	t.incomingResChan, err = t.amqpChan.Consume(
		resQueue.Name,
		"",    // assign consumer-id
		false, // auto-ack; we use manual acks
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		return err
	}
	t.amqpReplyQueueName = resQueue.Name

	// Request to be notified about returned deliveries
	t.failedDispatchChan = t.amqpChan.NotifyReturn(make(chan amqpClient.Return, 0))

	// Start the client worker and wait for it to ack that its up & running
	t.outgoingReqChan = make(chan *rpc, 0)
	t.clientReadyChan = make(chan struct{}, 0)
	go t.clientWorker()
	<-t.clientReadyChan

	t.clientRefCount++
	return nil
}

// dialServer dials the transport and sets up a private queue for receiving
// incoming requests. This method must be called while holding the mutex.
func (t *Transport) dialServer() error {
	if t.serverRefCount > 0 {
		t.serverRefCount++
		return nil
	}

	err := t.dialAndVerifyExchange()
	if err != nil {
		return err
	}

	// Allocate a private queue for receiving requests to the bound endpoints
	reqQueue, err := t.amqpChan.QueueDeclare(
		"",    // let amqp assign a random name
		false, // non-durable
		true,  // auto-delete
		true,  // exclusive
		false, // no-wait
		nil,   // args table
	)

	if err != nil {
		return err
	}

	// Consuming messages sent to the request queue
	t.incomingReqChan, err = t.amqpChan.Consume(
		reqQueue.Name,
		"",    // assign consumer-id
		false, // auto-ack; we use manual acks
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		return err
	}

	t.amqpRequestQueueName = reqQueue.Name

	// Connect each binding's routingKey to our private request queue
	for routingKey := range t.bindings {
		err = t.amqpChan.QueueBind(
			t.amqpRequestQueueName,
			routingKey,
			usrvExchangeName,
			false, // no-wait,
			nil,   // args
		)
		if err != nil {
			return err
		}
	}

	// Start the server worker and wait for it to ack that its up & running
	t.outgoingReqChan = make(chan *rpc, 0)
	t.serverReadyChan = make(chan struct{}, 0)
	go t.serverWorker()
	<-t.serverReadyChan

	t.serverRefCount++
	return nil
}

// Close shuts down the transport.
func (t *Transport) Close(mode transport.Mode) error {
	defer func() {
		// If both ref counters reach 0 then we can safely close the amqp channel and connection
		t.rwMutex.Lock()
		if t.serverRefCount+t.clientRefCount != 0 {
			t.rwMutex.Unlock()
			return
		}

		if t.amqpChan != nil {
			t.amqpChan.Close()
			t.amqpChan = nil
		}

		if t.amqpConnCloser != nil {
			t.amqpConnCloser.Close()
			t.amqpConnCloser = nil
		}

		t.rwMutex.Unlock()
	}()

	t.rwMutex.Lock()

	switch mode {
	case transport.ModeServer:
		if t.serverRefCount == 0 {
			t.rwMutex.Unlock()
			return transport.ErrTransportClosed
		}

		t.serverRefCount--
		if t.serverRefCount != 0 {
			t.rwMutex.Unlock()
			return nil
		}

		// Ref counter reached zero; signal the server worker to shut down.
		// Since the worker uses read locks we need to release the lock
		// so as not to block the server go-routine.
		t.serverReadyChan <- struct{}{}
		t.rwMutex.Unlock()
		<-t.serverReadyChan
	default:
		if t.clientRefCount == 0 {
			t.rwMutex.Unlock()
			return transport.ErrTransportClosed
		}

		t.clientRefCount--
		if t.clientRefCount != 0 {
			t.rwMutex.Unlock()
			return nil
		}

		// Ref counter reached zero; signal the client worker to shut down.
		// Since the worker uses read locks we need to release the lock
		// so as not to block the client go-routine.
		t.clientReadyChan <- struct{}{}
		t.rwMutex.Unlock()
		<-t.clientReadyChan
		close(t.outgoingReqChan)
	}

	return nil
}

// Bind listens for messages send to a particular service and
// endpoint tuple and invokes the supplied handler to process them.
func (t *Transport) Bind(version, service, endpoint string, handler transport.Handler) error {
	t.rwMutex.Lock()
	defer t.rwMutex.Unlock()

	routingKey := routingKey(version, service, endpoint)
	if _, exists := t.bindings[routingKey]; exists {
		return fmt.Errorf(
			"binding (version: %q, service: %q, endpoint: %q) already defined",
			version,
			service,
			endpoint,
		)
	}

	// If already dialed, bind this routing key to our private request queue
	if t.serverRefCount > 0 {
		err := t.amqpChan.QueueBind(
			t.amqpRequestQueueName,
			routingKey,
			usrvExchangeName,
			false, // no-wait,
			nil,   // args
		)

		if err != nil {
			return err
		}
	}

	t.bindings[routingKey] = &binding{
		version:  version,
		service:  service,
		endpoint: endpoint,
		handler:  handler,
	}

	return nil
}

// Unbind removes a handler previously registered via a call to Bind().
func (t *Transport) Unbind(version, service, endpoint string) {
	t.rwMutex.Lock()
	defer t.rwMutex.Unlock()

	routingKey := routingKey(version, service, endpoint)
	delete(t.bindings, routingKey)
}

// Request performs an RPC and returns back a read-only channel for
// receiving the result.
func (t *Transport) Request(msg transport.Message) <-chan transport.ImmutableMessage {

	rpc := &rpc{
		req:     msg,
		resChan: make(chan transport.ImmutableMessage, 1),
	}

	go func() {
		select {
		case t.outgoingReqChan <- rpc:
		case <-t.clientReadyChan:
			rpc.respondWithError(transport.ErrTransportClosed)
		}
	}()

	return rpc.resChan
}

func (t *Transport) serverWorker() {
	var pendingServerRequests sync.WaitGroup

	defer func() {
		// Wait for spawned server request go-routines to finish
		pendingServerRequests.Wait()

		// Signal Close() that we exited
		close(t.serverReadyChan)
	}()

	// Signal dialServer() that worker has started
	t.serverReadyChan <- struct{}{}

	for {
		select {
		case amqpReq, ok := <-t.incomingReqChan:
			if !ok {
				return
			}
			pendingServerRequests.Add(1)
			t.rwMutex.RLock()
			b := t.bindings[amqpReq.RoutingKey]
			t.rwMutex.RUnlock()

			go func(amqpReq *amqpClient.Delivery, b *binding) {
				req := decodeAmqpRequest(amqpReq, b)
				res := transport.MakeGenericMessage()

				b.handler.Process(req, res)

				// Build amqp response
				headers := amqpClient.Table{}
				for k, v := range res.Headers() {
					headers[k] = v
				}
				payload, err := res.Payload()
				amqpRes := amqpClient.Publishing{
					CorrelationId: req.ID(),
					AppId:         res.Sender(),
					UserId:        res.SenderEndpoint(),
					Headers:       headers,
				}

				if err != nil {
					amqpRes.ContentType = contentTypeError
					amqpRes.Body = []byte(err.Error())
				} else {
					amqpRes.ContentType = contentTypeUsrvData
					amqpRes.Body = payload
				}

				err = t.amqpChan.Publish(
					"",              // default exchange
					amqpReq.ReplyTo, // reply to client's private queue
					true,            // mandatory
					false,           // immediate
					amqpRes,
				)

				if err != nil {
					amqpReq.Ack(false)
				} else {
					amqpReq.Nack(false, false)
				}

				req.Close()
				res.Close()
				pendingServerRequests.Done()
			}(&amqpReq, b)
		case <-t.serverReadyChan:
			return
		}
	}
}

func (t *Transport) clientWorker() {
	pendingClientRequests := make(map[string]*rpc, 0)

	defer func() {
		// Abort pending client requests with ErrServiceUnavailable
		for _, rpc := range pendingClientRequests {
			rpc.respondWithError(transport.ErrServiceUnavailable)
		}

		// Signal Close() that we exited
		close(t.clientReadyChan)
	}()

	// Signal dialClient that worker has started
	t.clientReadyChan <- struct{}{}

	for {
		select {
		case rpc, ok := <-t.outgoingReqChan:
			if !ok {
				return
			}
			payload, _ := rpc.req.Payload()

			// Map request to amqp payload
			headers := amqpClient.Table{}
			for k, v := range rpc.req.Headers() {
				headers[k] = v
			}

			pub := amqpClient.Publishing{
				DeliveryMode:  amqpClient.Transient,
				ContentType:   contentTypeUsrvData,
				Headers:       headers,
				Body:          payload,
				ReplyTo:       t.amqpReplyQueueName,
				CorrelationId: rpc.req.ID(),
				// Encode from service/endpoint
				AppId:  rpc.req.Sender(),
				UserId: rpc.req.SenderEndpoint(),
			}
			routingKey := routingKey(rpc.req.ReceiverVersion(), rpc.req.Receiver(), rpc.req.ReceiverEndpoint())

			err := t.amqpChan.Publish(
				usrvExchangeName,
				routingKey,
				true,  // notify us for failed deliveries
				false, // immediate
				pub,
			)

			// Error occured; just report the error back
			if err != nil {
				rpc.respondWithError(err)
				continue
			}

			// Add to pendingClientRequests and wait for an async reply
			pendingClientRequests[pub.CorrelationId] = rpc
		case amqpRes, ok := <-t.incomingResChan:
			if !ok {
				return
			}
			rpc, exists := pendingClientRequests[amqpRes.CorrelationId]
			if !exists {
				continue
			}

			delete(pendingClientRequests, amqpRes.CorrelationId)

			res := rpc.makeResponse()
			decodeAmqpResponse(&amqpRes, res)
			rpc.respond(res)
		case amqpRes, ok := <-t.failedDispatchChan:
			if !ok {
				return
			}
			rpc, exists := pendingClientRequests[amqpRes.CorrelationId]
			if !exists {
				continue
			}

			delete(pendingClientRequests, amqpRes.CorrelationId)

			var err error
			switch amqpRes.ReplyCode {
			case replyCodeNotFound:
				err = transport.ErrNotFound
			case replyCodeNotAuthorized:
				err = transport.ErrNotAuthorized
			default:
				err = transport.ErrServiceUnavailable
			}

			rpc.respondWithError(err)
		case <-t.clientReadyChan:
			return
		}
	}
}

func decodeAmqpRequest(amqpReq *amqpClient.Delivery, b *binding) transport.Message {
	req := transport.MakeGenericMessage()
	req.IDField = amqpReq.CorrelationId
	req.SenderField = amqpReq.AppId
	req.SenderEndpointField = amqpReq.UserId
	req.ReceiverField = b.service
	req.ReceiverEndpointField = b.endpoint
	req.ReceiverVersionField = b.version
	req.PayloadField = amqpReq.Body

	for k, v := range amqpReq.Headers {
		if val, isString := v.(string); isString {
			req.SetHeader(k, val)
		}
	}

	return req
}

func decodeAmqpResponse(amqpRes *amqpClient.Delivery, res transport.Message) {
	for k, v := range amqpRes.Headers {
		if val, isString := v.(string); isString {
			res.SetHeader(k, val)
		}
	}

	var err error
	var payload []byte

	if amqpRes.ContentType == contentTypeError {
		// Map common errors
		switch string(amqpRes.Body) {
		case transport.ErrServiceUnavailable.Error():
			err = transport.ErrServiceUnavailable
		case transport.ErrNotAuthorized.Error():
			err = transport.ErrNotAuthorized
		case transport.ErrTimeout.Error():
			err = transport.ErrTimeout
		case "":
			err = errors.New("unknown error")
		default:
			err = errors.New(string(amqpRes.Body))
		}
	} else {
		payload = amqpRes.Body
	}

	res.SetPayload(payload, err)
}

// Factory is a factory for creating usrv transport instances
// whose concrete implementation is the amqp transport. This function behaves
// exactly the same as New() but returns back a Transport interface allowing
// it to be used as usrv.DefaultTransportFactory.
//
// To prevent the creation of different amqp connections when using a server and
// multiple clients, this factory returns a singleton ref-counted transport instance
// that is shared between clients and servers.
func Factory() transport.Provider {
	return singleton
}

func init() {
	config.SetDefaults("", map[string]string{
		"transport/amqp/uri": "amqp://guest:guest@localhost:5672/",
	})
}
