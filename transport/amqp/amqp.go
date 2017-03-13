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

// amqpChannel defines an interface that is compatible with amqpClient.Channel
// and our channel mocks.
type amqpChannel interface {
	Ack(tag uint64, multiple bool) error
	Nack(tag uint64, multiple bool, requeue bool) error
	Reject(tag uint64, requeue bool) error
	NotifyReturn(c chan amqpClient.Return) chan amqpClient.Return
	Publish(exchange, key string, mandatory, immediate bool, msg amqpClient.Publishing) error
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqpClient.Table) error
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqpClient.Table) (amqpClient.Queue, error)
	QueueBind(name, key, exchange string, noWait bool, args amqpClient.Table) error
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqpClient.Table) (<-chan amqpClient.Delivery, error)
	Close() error
}

// dialAndGetChan is a helper function that given an amqpURI returns back an
// amqpChannel interface, an io.Closer for shutting down the connection and a
// channel for receiving connection error events.
//
// This function is overridden by tests to mock calls to the amqp serve.
var dialAndGetChan = func(amqpURI string) (amqpChannel, io.Closer, chan *amqpClient.Error, error) {
	amqpConn, err := amqpClient.Dial(amqpURI)
	if err != nil {
		return nil, nil, nil, err
	}

	amqpChan, err := amqpConn.Channel()
	if err != nil {
		amqpConn.Close()
		return nil, nil, nil, err
	}

	connCloseChan := make(chan *amqpClient.Error, 0)
	return amqpChan, amqpConn, amqpConn.NotifyClose(connCloseChan), nil
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
	// separately for quickly populating incoming message fields.
	version  string
	service  string
	endpoint string
}

func routingKey(version, service, endpoint string) string {
	return fmt.Sprintf("%s/%s/%s", version, service, endpoint)
}

// Transport implements a usrv transport using AMQP. The transport operates in
// both a client and a server mode and utilizes reference counting to share a
// single transport instance with multiple clients and servers. The transport
// maintains and reuses an open connection until both reference counters reach zero.
// It is therefore important that the application always closes the transport before
// exiting to ensure that the AMQP logs do not fill up with "client unexpectedly
// closed TCP connection" wanrings.
//
// In both modes, the transport declares a direct amqp exchange called usrv. All
// usrv messages are routed through this particular exchange. The transport supoprts
// endpoint versioning and generates AMQP routing keys with format "$version/$service/$endpoint".
//
// When operating in server mode, the transport allocates a private queue and for
// each defined endpoint it binds its routing key to the private queue. This
// allows the transport to use a single consumer channel for processing incoming
// requests. For each incoming request, the transport uses the routing key to
// figure out which handler it should invoke and spawns a go-routine to handle
// the request.
//
// When operating in client mode, the transport allocates a private queue for
// receiving responses. Whenever the client sends an outgoing request it populates
// the following AMQP message fields:
//  - AppId: set to the the outgoing message Sender() value.
//  - Type: set to the outgoing message SenderEndpoint() value.
//  - ReplyTo: set to the private queue name for receiving responses.
//  - CorrelationId: set to the outgoing message ID() value.
//
// Since the transport handles responses asynchronously, the correlation ID serves
// as a unique ID for matching pending requests to their responses.
//
// The client also listens for failed deliveries. This allows the transport to
// fail pending requests if no servers are available or if the broker cannot
// route the request.
//
// The transport receives its broker connection URL from a parameter with name:
// "transport/amqp/uri". It's default value is set to "amqp://guest:guest@localhost:5672/"
// which corresponds to a rabbitMQ instance running on localhost. At the moment,
// TLS connections to the broker are not supported.
//
// As with other usrv transports, once connected, the AMQP transport monitors
// the URI config for changes and automatically attempts to re-dial the connection
// whenever its value changes.
type Transport struct {
	rwMutex sync.RWMutex

	// Reference counters
	serverRefCount int
	clientRefCount int

	// A flag for the AMQP broker endpoint.
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

	monitorExitChan       chan struct{}
	clientExitChan        chan struct{}
	serverExitChan        chan struct{}
	serverPendingRequests sync.WaitGroup

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
	var err error
	var amqpConnCloseChan chan *amqpClient.Error
	t.amqpChan, t.amqpConnCloser, amqpConnCloseChan, err = dialAndGetChan(t.amqpURI.Get())
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

	t.monitorExitChan = make(chan struct{}, 0)
	go func(monitorExitChan chan struct{}) {
		var err error
		for {
			select {
			case <-monitorExitChan:
				return
			case err = <-amqpConnCloseChan:
				// The amqp driver reports an EOF error and then closes
				// the channel. We only care about errors. Try redialing.
				// If the attempt fails, leave the monitor running so we
				// can trigger redial if the amqp endpoint changes.
				if err != nil {
					t.rwMutex.Lock()
					t.amqpChan = nil
					t.amqpConnCloser = nil
					t.rwMutex.Unlock()

					if err = t.redial(); err != nil {
						return
					}
				}
			case <-t.amqpURI.ChangeChan():
				// redial() will force-close the connection; make sure that
				// the monitor does not process any events sent to the connection
				// close channel
				amqpConnCloseChan = nil

				// Trigger redial. If the redial succeeds, a new
				// monitor go-routine will be spawned so we need to exit
				// this one.
				if err = t.redial(); err != nil {
					continue
				}

				return
			}
		}
	}(t.monitorExitChan)

	return nil
}

// dialClient dials the transport and sets up a private queue for receiving
// responses for outgoing requests. This method must be called while holding the mutex.
func (t *Transport) dialClient() error {
	if t.amqpChan == nil {
		err := t.dialAndVerifyExchange()
		if err != nil {
			return err
		}
	}

	if t.clientRefCount > 0 {
		t.clientRefCount++
		return nil
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
	t.clientExitChan = make(chan struct{}, 0)
	<-t.spawnClientWorker()

	t.clientRefCount++
	return nil
}

// dialServer dials the transport and sets up a private queue for receiving
// incoming requests. This method must be called while holding the mutex.
func (t *Transport) dialServer() error {
	if t.amqpChan == nil {
		err := t.dialAndVerifyExchange()
		if err != nil {
			return err
		}
	}

	if t.serverRefCount > 0 {
		t.serverRefCount++
		return nil
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
	t.serverExitChan = make(chan struct{}, 1)
	<-t.spawnServerWorker()

	t.serverRefCount++
	return nil
}

// redial is invoked whenever the amqp connection notifies us that an error
// (usually EOF) has occurred. This function will attempt to re-establish a
// connection and dial the client- and/or server-side of the transport.
func (t *Transport) redial() error {
	t.rwMutex.Lock()
	defer t.rwMutex.Unlock()

	// Reset amqp handles so we can redial
	if t.amqpChan != nil {
		t.amqpChan.Close()
		t.amqpChan = nil
	}
	if t.amqpConnCloser != nil {
		t.amqpConnCloser.Close()
		t.amqpConnCloser = nil
	}

	// Keep track of current ref counters and then reset them; otherwise
	// dialServer and dialClient will just increase them instead of re-dialing
	curServerRefCount := t.serverRefCount
	curClientRefCount := t.clientRefCount
	defer func() {
		t.serverRefCount = curServerRefCount
		t.clientRefCount = curClientRefCount
	}()

	t.serverRefCount = 0
	t.clientRefCount = 0

	if curServerRefCount > 0 {
		if err := t.dialServer(); err != nil {
			t.amqpChan = nil
			t.amqpConnCloser = nil
			close(t.monitorExitChan)
			t.monitorExitChan = nil
			return err
		}
	}

	if curClientRefCount > 0 {
		if err := t.dialClient(); err != nil {
			t.amqpChan = nil
			t.amqpConnCloser = nil
			close(t.monitorExitChan)
			t.monitorExitChan = nil
			return err
		}
	}

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

		if t.monitorExitChan != nil {
			close(t.monitorExitChan)
			t.monitorExitChan = nil
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
		close(t.serverExitChan)
		t.rwMutex.Unlock()
		t.serverPendingRequests.Wait()
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
		close(t.clientExitChan)
		close(t.outgoingReqChan)
		t.rwMutex.Unlock()
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

	t.rwMutex.RLock()
	if t.clientRefCount == 0 || t.amqpChan == nil {
		rpc.respondWithError(transport.ErrTransportClosed)
	} else {
		t.outgoingReqChan <- rpc
	}
	t.rwMutex.RUnlock()

	return rpc.resChan
}

// spawnServerWorker starts a go-routine that processes incoming usrv requests.
// The worker spawns a go-routine for each incoming request and acks/nacks the
// request depending on whether an error occurred.
//
// The worker listens on serverExitChan and shuts down if it receives a message
// or the channel closes. This function returns a channel which is closed when
// the worker go-routine has started.
func (t *Transport) spawnServerWorker() chan struct{} {
	serverStartedChan := make(chan struct{}, 0)

	// Keep local copies of the channels we need. This prevents data-race
	// warnings when running tests that involve redialing the transport
	incomingReqChan := t.incomingReqChan
	serverExitChan := t.serverExitChan

	go func() {
		// Signal that the worker has started
		close(serverStartedChan)

		for {
			select {
			case amqpReq, ok := <-incomingReqChan:
				if !ok {
					return
				}
				t.serverPendingRequests.Add(1)
				t.rwMutex.RLock()
				b := t.bindings[amqpReq.RoutingKey]
				t.rwMutex.RUnlock()

				go func(amqpReq *amqpClient.Delivery, b *binding) {
					req := decodeAmqpRequest(amqpReq, b)
					res := transport.MakeGenericMessage()

					switch b {
					case nil:
						res.SetPayload(nil, transport.ErrNotFound)
					default:
						b.handler.Process(req, res)
					}

					// Build amqp response
					headers := amqpClient.Table{}
					for k, v := range res.Headers() {
						headers[k] = v
					}
					payload, err := res.Payload()
					amqpRes := amqpClient.Publishing{
						CorrelationId: req.ID(),
						AppId:         req.Receiver(),
						Type:          req.ReceiverEndpoint(),
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

					if err == nil {
						amqpReq.Ack(false)
					} else {
						amqpReq.Nack(false, false)
					}

					req.Close()
					res.Close()
					t.serverPendingRequests.Done()
				}(&amqpReq, b)
			case <-serverExitChan:
				return
			}
		}
	}()

	return serverStartedChan
}

// spawnClientWorker starts a go-routine that publishes outgoing usrv requests
// and pairs incoming responses to the outgoing requests by matching their
// correlation IDs. The worker also listens for undelivered outgoing requests
// and automatically aborts their pending responses.
//
// The worker listens on clientExitChan and shuts down if it receives a message
// or the channel closes. Before exiting, the worker ensures that any pending
// requests fail with ErrServiceUnavailable.
//
// This function returns a channel which is closed when the worker go-routine has started.
func (t *Transport) spawnClientWorker() chan struct{} {
	clientStartedChan := make(chan struct{}, 0)

	// Keep local copies of the channels we need. This prevents data-race
	// warnings when running tests that involve redialing the transport
	outgoingReqChan := t.outgoingReqChan
	incomingResChan := t.incomingResChan
	failedDispatchChan := t.failedDispatchChan
	clientExitChan := t.clientExitChan

	go func() {
		pendingClientRequests := make(map[string]*rpc, 0)

		defer func() {
			// Abort pending client requests with ErrServiceUnavailable
			for _, rpc := range pendingClientRequests {
				rpc.respondWithError(transport.ErrServiceUnavailable)
			}
		}()

		// Signal that worker has started
		close(clientStartedChan)

		for {
			select {
			case rpc, ok := <-outgoingReqChan:
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
					AppId: rpc.req.Sender(),
					Type:  rpc.req.SenderEndpoint(),
				}
				routingKey := routingKey(rpc.req.ReceiverVersion(), rpc.req.Receiver(), rpc.req.ReceiverEndpoint())

				err := t.amqpChan.Publish(
					usrvExchangeName,
					routingKey,
					true,  // notify us for failed deliveries
					false, // immediate
					pub,
				)

				// Error occurred; just report the error back
				if err != nil {
					rpc.respondWithError(err)
					continue
				}

				// Add to pendingClientRequests and wait for an async reply
				pendingClientRequests[pub.CorrelationId] = rpc
			case amqpResponse, connectionOpen := <-incomingResChan:
				if !connectionOpen {
					return
				}
				rpc, exists := pendingClientRequests[amqpResponse.CorrelationId]
				if !exists {
					continue
				}

				delete(pendingClientRequests, amqpResponse.CorrelationId)

				res := rpc.makeResponse()
				decodeAmqpResponse(&amqpResponse, res)
				rpc.respond(res)
			case amqpReturn, connectionOpen := <-failedDispatchChan:
				if !connectionOpen {
					return
				}

				rpc, exists := pendingClientRequests[amqpReturn.CorrelationId]
				if !exists {
					continue
				}

				delete(pendingClientRequests, amqpReturn.CorrelationId)

				var err error
				switch amqpReturn.ReplyCode {
				case replyCodeNotFound:
					err = transport.ErrNotFound
				case replyCodeNotAuthorized:
					err = transport.ErrNotAuthorized
				default:
					err = transport.ErrServiceUnavailable
				}

				rpc.respondWithError(err)
			case <-clientExitChan:
				return
			}
		}
	}()

	return clientStartedChan
}

func decodeAmqpRequest(amqpReq *amqpClient.Delivery, b *binding) transport.Message {
	req := transport.MakeGenericMessage()
	req.IDField = amqpReq.CorrelationId
	req.SenderField = amqpReq.AppId
	req.SenderEndpointField = amqpReq.Type
	if b != nil {
		req.ReceiverField = b.service
		req.ReceiverEndpointField = b.endpoint
		req.ReceiverVersionField = b.version
	}
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

// SingletonFactory is a factory for creating singleton AMQP transport
// instances. This function returns back a Transport interface allowing it to
// be used as usrv.DefaultTransportFactory.
func SingletonFactory() transport.Provider {
	return singleton
}

func init() {
	config.SetDefaults("", map[string]string{
		"transport/amqp/uri": "amqp://guest:guest@localhost:5672/",
	})
}
