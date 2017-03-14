package amqp

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/achilleasa/usrv/transport"
	amqpClient "github.com/streadway/amqp"
)

const (
	amqpFrameTerminator byte = 0xCE
)

func TestDialServerErrors(t *testing.T) {
	orig := dialAndGetChan
	defer func() {
		dialAndGetChan = orig
	}()

	expError := errors.New("amqp dial error")
	dialAndGetChan = func(_ string) (amqpChannel, io.Closer, chan *amqpClient.Error, error) {
		return nil, nil, nil, expError
	}

	tr := New()
	err := tr.Dial(transport.ModeServer)
	if err != expError {
		t.Fatalf("expected to get error %v; got %v", expError, err)
	}

	mc := &mockChannel{}
	dialAndGetChan = func(_ string) (amqpChannel, io.Closer, chan *amqpClient.Error, error) {
		return mc, mc, nil, nil
	}

	mc.exDeclareErr = errors.New("amqp exchange declare error")
	err = tr.Dial(transport.ModeServer)
	if err != mc.exDeclareErr {
		t.Fatalf("expected to get error %v; got %v", mc.exDeclareErr, err)
	}
	mc.exDeclareErr = nil

	mc.queueDeclareErr = errors.New("amqp queue declare error")
	err = tr.Dial(transport.ModeServer)
	if err != mc.queueDeclareErr {
		t.Fatalf("expected to get error %v; got %v", mc.queueDeclareErr, err)
	}
	mc.queueDeclareErr = nil

	mc.consumeErr = errors.New("amqp queue consume error")
	err = tr.Dial(transport.ModeServer)
	if err != mc.consumeErr {
		t.Fatalf("expected to get error %v; got %v", mc.consumeErr, err)
	}
	mc.consumeErr = nil

	err = tr.Bind("", "foo", "endpoint", transport.HandlerFunc(func(_ transport.ImmutableMessage, _ transport.Message) {}))
	if err != nil {
		t.Fatal(err)
	}

	mc.queueBindErr = errors.New("amqp queue bind error")
	err = tr.Dial(transport.ModeServer)
	if err != mc.queueBindErr {
		t.Fatalf("expected to get error %v; got %v", mc.queueBindErr, err)
	}
	mc.queueBindErr = nil

	err = tr.Close(transport.ModeServer)
	if err != transport.ErrTransportClosed {
		t.Fatalf("expected to get ErrTransportClosed; got %v", err)
	}
}

func TestDialClientErrors(t *testing.T) {
	orig := dialAndGetChan
	defer func() {
		dialAndGetChan = orig
	}()

	expError := errors.New("amqp dial error")
	dialAndGetChan = func(_ string) (amqpChannel, io.Closer, chan *amqpClient.Error, error) {
		return nil, nil, nil, expError
	}

	tr := New()
	err := tr.Dial(transport.ModeClient)
	if err != expError {
		t.Fatalf("expected to get error %v; got %v", expError, err)
	}

	mc := &mockChannel{}
	dialAndGetChan = func(_ string) (amqpChannel, io.Closer, chan *amqpClient.Error, error) {
		return mc, mc, nil, nil
	}

	mc.exDeclareErr = errors.New("amqp exchange declare error")
	err = tr.Dial(transport.ModeClient)
	if err != mc.exDeclareErr {
		t.Fatalf("expected to get error %v; got %v", mc.exDeclareErr, err)
	}
	mc.exDeclareErr = nil

	mc.queueDeclareErr = errors.New("amqp queue declare error")
	err = tr.Dial(transport.ModeClient)
	if err != mc.queueDeclareErr {
		t.Fatalf("expected to get error %v; got %v", mc.queueDeclareErr, err)
	}
	mc.queueDeclareErr = nil

	mc.consumeErr = errors.New("amqp queue consume error")
	err = tr.Dial(transport.ModeClient)
	if err != mc.consumeErr {
		t.Fatalf("expected to get error %v; got %v", mc.consumeErr, err)
	}
	mc.consumeErr = nil

	err = tr.Close(transport.ModeClient)
	if err != transport.ErrTransportClosed {
		t.Fatalf("expected to get ErrTransportClosed; got %v", err)
	}
}

func TestServerDisconnectHandling(t *testing.T) {
	orig := dialAndGetChan
	defer func() {
		dialAndGetChan = orig
	}()

	mc := &mockChannel{
		consumeChan: make(chan amqpClient.Delivery),
	}
	dialAndGetChan = func(_ string) (amqpChannel, io.Closer, chan *amqpClient.Error, error) {
		return mc, mc, nil, nil
	}

	tr := New()
	err := tr.Dial(transport.ModeServer)
	if err != nil {
		t.Fatal(err)
	}

	// Trigger second Close() after we exit; this should be a no-op
	defer tr.Close(transport.ModeServer)

	// Simulate disconnect by closing the delivery channel; server worker
	// should pick it up and exit
	close(mc.consumeChan)
	<-time.After(100 * time.Millisecond)

	// Trigger a close
	tr.Close(transport.ModeServer)
}

func TestBindAndUnbind(t *testing.T) {
	orig := dialAndGetChan
	defer func() {
		dialAndGetChan = orig
	}()

	mc := &mockChannel{}
	dialAndGetChan = func(_ string) (amqpChannel, io.Closer, chan *amqpClient.Error, error) {
		return mc, mc, nil, nil
	}

	tr := New()
	err := tr.Dial(transport.ModeServer)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close(transport.ModeServer)

	err = tr.Bind("", "foo", "endpoint", transport.HandlerFunc(func(_ transport.ImmutableMessage, _ transport.Message) {}))
	if err != nil {
		t.Fatal(err)
	}

	expError := `binding (version: "", service: "foo", endpoint: "endpoint") already defined`
	err = tr.Bind("", "foo", "endpoint", transport.HandlerFunc(func(_ transport.ImmutableMessage, _ transport.Message) {}))
	if err == nil || err.Error() != expError {
		t.Fatalf("expected to get error %q; got %v", expError, err)
	}

	tr.Unbind("", "foo", "endpoint")

	// We should be able to bind again now; this time simulate an amqp error when we try to bind
	mc.queueBindErr = errors.New("amqp queue bind error")
	err = tr.Bind("", "foo", "endpoint", transport.HandlerFunc(func(_ transport.ImmutableMessage, _ transport.Message) {}))
	if err != mc.queueBindErr {
		t.Fatalf("expected to get error %v; got %v", mc.queueBindErr, err)
	}
}

func TestRequestForInvalidBinding(t *testing.T) {
	orig := dialAndGetChan
	defer func() {
		dialAndGetChan = orig
	}()

	mc := &mockChannel{
		consumeChan: make(chan amqpClient.Delivery),
		pubQueue:    make(chan amqpClient.Publishing, 1),
	}
	dialAndGetChan = func(_ string) (amqpChannel, io.Closer, chan *amqpClient.Error, error) {
		return mc, mc, nil, nil
	}

	tr := New()
	err := tr.Dial(transport.ModeServer)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close(transport.ModeServer)

	// Test a message that get's delivered to a non-existing binding
	req := transport.MakeGenericMessage()
	req.SenderField = "test"
	req.SenderEndpointField = "test-endpoint"
	req.ReceiverField = "unknown"
	req.ReceiverEndpointField = "endpoint"
	mc.consumeChan <- makeAmqpDelivery(req, mc)

	var pub amqpClient.Publishing
	select {
	case pub = <-mc.pubQueue:
	case <-time.After(1 * time.Second):
		t.Fatal("timeout while waiting for server worker to publish response")
	}

	if pub.ContentType != contentTypeError {
		t.Fatalf("expected content type to be %q; got %q", contentTypeError, pub.ContentType)
	}

	bodyAsStr := string(pub.Body)
	if bodyAsStr != transport.ErrNotFound.Error() {
		t.Fatalf("expected response body to be %v; got %s", transport.ErrNotFound, bodyAsStr)
	}

	if pub.AppId != "" || pub.Type != "" {
		t.Fatalf("expected response appID and userID to be empty; got %q, %q", pub.AppId, pub.Type)
	}
}

func TestRequestForValidBinding(t *testing.T) {
	orig := dialAndGetChan
	defer func() {
		dialAndGetChan = orig
	}()

	mc := &mockChannel{
		consumeChan: make(chan amqpClient.Delivery),
		pubQueue:    make(chan amqpClient.Publishing, 1),
	}
	dialAndGetChan = func(_ string) (amqpChannel, io.Closer, chan *amqpClient.Error, error) {
		return mc, mc, nil, nil
	}

	tr := New()
	err := tr.Dial(transport.ModeServer)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close(transport.ModeServer)

	req := transport.MakeGenericMessage()
	req.SenderField = "test"
	req.SenderEndpointField = "test-endpoint"
	req.ReceiverField = "service"
	req.ReceiverEndpointField = "remote-endpoint"
	req.HeadersField = map[string]string{
		"Foo": "foo_val",
		"Bar": "bar_val",
	}

	// Define binding
	invokedChan := make(chan struct{})
	expPayload := []byte("response data")
	err = tr.Bind("", "service", "remote-endpoint", transport.HandlerFunc(func(reqMsg transport.ImmutableMessage, res transport.Message) {
		if reqMsg.ID() != req.ID() {
			t.Errorf("expected req ID to be %s; got %s", req.ID(), reqMsg.ID())
		}

		if !reflect.DeepEqual(reqMsg.Headers(), req.Headers()) {
			t.Errorf("expected req headers to be %v; got %v", req.Headers(), reqMsg.Headers())
		}

		if reqMsg.Sender() != req.Sender() {
			t.Errorf("expected req sender to be %s; got %s", req.Sender(), reqMsg.Sender())
		}

		if reqMsg.SenderEndpoint() != req.SenderEndpoint() {
			t.Errorf("expected req sender endpoint to be %s; got %s", req.SenderEndpoint(), reqMsg.SenderEndpoint())
		}

		if reqMsg.Receiver() != req.Receiver() {
			t.Errorf("expected req receiver to be %s; got %s", req.Receiver(), reqMsg.Receiver())
		}

		if reqMsg.ReceiverEndpoint() != req.ReceiverEndpoint() {
			t.Errorf("expected req receiver endpoint to be %s; got %s", req.ReceiverEndpoint(), reqMsg.ReceiverEndpoint())
		}

		if reqMsg.ReceiverVersion() != req.ReceiverVersion() {
			t.Errorf("expected req receiver version to be %s; got %s", req.ReceiverVersion(), reqMsg.ReceiverVersion())
		}

		res.SetPayload(expPayload, nil)
		res.SetHeaders(req.Headers())
		close(invokedChan)
	}))

	if err != nil {
		t.Fatal(err)
	}

	// Simulate incoming delivery
	mc.consumeChan <- makeAmqpDelivery(req, mc)
	select {
	case <-invokedChan:
	case <-time.After(1 * time.Second):
		t.Fatalf("timeout waiting for binding handler to be invoked")
	}

	var pub amqpClient.Publishing
	select {
	case pub = <-mc.pubQueue:
	case <-time.After(1 * time.Second):
		t.Fatal("timeout while waiting for server worker to publish response")
	}

	if pub.ContentType != contentTypeUsrvData {
		t.Errorf("expected content type to be %q; got %q", contentTypeUsrvData, pub.ContentType)
	}

	if !bytes.Equal(pub.Body, expPayload) {
		t.Errorf("expected response body to be %s; got %s", string(expPayload), string(pub.Body))
	}

	if pub.AppId != req.Receiver() || pub.Type != req.ReceiverEndpoint() {
		t.Errorf("expected response appID and userID to be %q, %q; got %q, %q", req.Receiver(), req.ReceiverEndpoint(), pub.AppId, pub.Type)
	}

	// Allow some time for the handler to ack the request
	<-time.After(100 * time.Millisecond)
	expAckCount := 1
	if mc.AckCount() != expAckCount {
		t.Errorf("expected ack count to be %d; got %d", expAckCount, mc.AckCount())
	}
}

func TestRequestForValidBindingWhenPublishFails(t *testing.T) {
	orig := dialAndGetChan
	defer func() {
		dialAndGetChan = orig
	}()

	mc := &mockChannel{
		consumeChan: make(chan amqpClient.Delivery),
		pubQueue:    make(chan amqpClient.Publishing, 1),
	}
	dialAndGetChan = func(_ string) (amqpChannel, io.Closer, chan *amqpClient.Error, error) {
		return mc, mc, nil, nil
	}

	tr := New()
	err := tr.Dial(transport.ModeServer)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close(transport.ModeServer)

	req := transport.MakeGenericMessage()
	req.SenderField = "test"
	req.SenderEndpointField = "test-endpoint"
	req.ReceiverField = "service"
	req.ReceiverEndpointField = "remote-endpoint"

	// Define binding
	invokedChan := make(chan struct{})
	err = tr.Bind("", "service", "remote-endpoint", transport.HandlerFunc(func(reqMsg transport.ImmutableMessage, res transport.Message) {
		close(invokedChan)
	}))

	if err != nil {
		t.Fatal(err)
	}

	// Simulate incoming delivery and setup mc.Publish to fail
	mc.pubErr = errors.New("amqp publish error")
	mc.consumeChan <- makeAmqpDelivery(req, mc)
	select {
	case <-invokedChan:
	case <-time.After(1 * time.Second):
		t.Fatalf("timeout waiting for binding handler to be invoked")
	}

	select {
	case <-mc.pubQueue:
		t.Fatalf("received unexpected published message")
	case <-time.After(1 * time.Second):
	}

	expNackCount := 1
	if mc.NackCount() != expNackCount {
		t.Errorf("expected nack count to be %d; got %d", expNackCount, mc.NackCount())
	}
}

func TestClientRequestWithClosedTransport(t *testing.T) {
	tr := New()
	res := <-tr.Request(transport.MakeGenericMessage())

	_, err := res.Payload()
	if err != transport.ErrTransportClosed {
		t.Fatalf("expected to get transport.ErrTransportClosed; got %v", err)
	}
}

func TestClientWorkerHandlingOfClosedAmqpChannels(t *testing.T) {
	orig := dialAndGetChan
	defer func() {
		dialAndGetChan = orig
	}()

	mc := &mockChannel{
		consumeChan: make(chan amqpClient.Delivery),
		returnChan:  make(chan amqpClient.Return),
		pubQueue:    make(chan amqpClient.Publishing, 2),
	}
	dialAndGetChan = func(_ string) (amqpChannel, io.Closer, chan *amqpClient.Error, error) {
		return mc, mc, nil, nil
	}

	tr := New()
	if err := tr.Dial(transport.ModeClient); err != nil {
		t.Fatal(err)
	}
	defer tr.Close(transport.ModeClient)

	// queue rpc message and then close the response channel
	rpcReq := &rpc{
		req:     transport.MakeGenericMessage(),
		resChan: make(chan transport.ImmutableMessage),
	}
	tr.outgoingReqChan <- rpcReq

	// close incoming request chan; this should cause the worker to exit and the request to fail
	close(mc.consumeChan)
	mc.consumeChan = nil

	res := <-rpcReq.resChan
	_, err := res.Payload()
	if err != transport.ErrServiceUnavailable {
		t.Fatal("expected rpc request to fail with ErrServiceUnavailable")
	}

	tr = New()
	if err = tr.Dial(transport.ModeClient); err != nil {
		t.Fatal(err)
	}
	defer tr.Close(transport.ModeClient)

	// queue rpc message and then close the return channel
	rpcReq = &rpc{
		req:     transport.MakeGenericMessage(),
		resChan: make(chan transport.ImmutableMessage),
	}
	tr.outgoingReqChan <- rpcReq

	// close return chan; this should cause the worker to exit and the request to fail
	close(mc.returnChan)

	res = <-rpcReq.resChan
	_, err = res.Payload()
	if err != transport.ErrServiceUnavailable {
		t.Fatal("expected rpc request to fail with ErrServiceUnavailable")
	}
}

func TestClientWorkerPublishRequest(t *testing.T) {
	orig := dialAndGetChan
	defer func() {
		dialAndGetChan = orig
	}()

	mc := &mockChannel{
		pubQueue: make(chan amqpClient.Publishing, 1),
	}
	dialAndGetChan = func(_ string) (amqpChannel, io.Closer, chan *amqpClient.Error, error) {
		return mc, mc, nil, nil
	}

	tr := New()
	err := tr.Dial(transport.ModeClient)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close(transport.ModeClient)

	req := transport.MakeGenericMessage()
	req.SenderField = "test"
	req.SenderEndpointField = "test-endpoint"
	req.ReceiverField = "service"
	req.ReceiverEndpointField = "remote-endpoint"
	req.HeadersField = map[string]string{
		"Foo": "foo_val",
		"Bar": "bar_val",
	}
	req.SetPayload([]byte("request data"), nil)

	tr.Request(req)

	var pub amqpClient.Publishing
	select {
	case pub = <-mc.pubQueue:
	case <-time.After(1 * time.Second):
		t.Fatal("timeout while waiting for client worker to publish request")
	}

	for k, v := range req.Headers() {
		if pub.Headers[k] != v {
			t.Errorf("expected published header %q to have value %q; got %v", k, v, pub.Headers[k])
		}
	}

	if pub.ContentType != contentTypeUsrvData {
		t.Errorf("expected content type to be %q; got %q", contentTypeUsrvData, pub.ContentType)
	}

	if !bytes.Equal(pub.Body, req.PayloadField) {
		t.Errorf("expected response body to be %s; got %s", string(req.PayloadField), string(pub.Body))
	}

	if pub.AppId != req.Sender() || pub.Type != req.SenderEndpoint() {
		t.Errorf("expected response appID and userID to be %q, %q; got %q, %q", req.Sender(), req.SenderEndpoint(), pub.AppId, pub.Type)
	}
}

func TestClientWorkerWhenPublishFails(t *testing.T) {
	orig := dialAndGetChan
	defer func() {
		dialAndGetChan = orig
	}()

	mc := &mockChannel{
		pubQueue: make(chan amqpClient.Publishing, 1),
	}
	dialAndGetChan = func(_ string) (amqpChannel, io.Closer, chan *amqpClient.Error, error) {
		return mc, mc, nil, nil
	}

	tr := New()
	err := tr.Dial(transport.ModeClient)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close(transport.ModeClient)

	req := transport.MakeGenericMessage()
	req.SenderField = "test"
	req.SenderEndpointField = "test-endpoint"
	req.ReceiverField = "service"
	req.ReceiverEndpointField = "remote-endpoint"

	mc.pubErr = errors.New("amqp publish error")
	resChan := tr.Request(req)

	select {
	case <-mc.pubQueue:
		t.Fatalf("received unexpected published message")
	case <-time.After(1 * time.Second):
	}

	res := <-resChan
	_, err = res.Payload()
	if err != mc.pubErr {
		t.Errorf("expected client to relay amqp publish error %v; got %v", mc.pubErr, err)
	}
}

func TestClientWorkerWhenReceivingResponses(t *testing.T) {
	orig := dialAndGetChan
	defer func() {
		dialAndGetChan = orig
	}()

	mc := &mockChannel{
		consumeChan: make(chan amqpClient.Delivery),
		pubQueue:    make(chan amqpClient.Publishing, 1),
	}
	dialAndGetChan = func(_ string) (amqpChannel, io.Closer, chan *amqpClient.Error, error) {
		return mc, mc, nil, nil
	}

	tr := New()
	err := tr.Dial(transport.ModeClient)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close(transport.ModeClient)

	req := transport.MakeGenericMessage()
	req.SenderField = "test"
	req.SenderEndpointField = "test-endpoint"
	req.ReceiverField = "service"
	req.ReceiverEndpointField = "remote-endpoint"

	resChan := tr.Request(req)

	select {
	case <-mc.pubQueue:
	case <-time.After(1 * time.Second):
		t.Fatal("timeout while waiting for client worker to publish request")
	}

	// Simulate incoming response
	simRes := transport.MakeGenericMessage()
	simRes.SenderField = "service"
	simRes.SenderEndpointField = "remote-endpoint"
	simRes.ReceiverField = "test"
	simRes.ReceiverEndpointField = "test-endpoint"
	simRes.SetHeaders(map[string]string{
		"Foo": "foo_val",
		"Bar": "bar_val",
	})
	simRes.SetPayload([]byte("response data"), nil)
	simRes.IDField = req.ID()
	mc.consumeChan <- makeAmqpDelivery(simRes, mc)

	resMsg := <-resChan

	if resMsg.ID() == simRes.ID() {
		t.Error("expected response message to use a new ID value")
	}

	if !reflect.DeepEqual(resMsg.Headers(), simRes.Headers()) {
		t.Errorf("expected res headers to be %v; got %v", simRes.Headers(), resMsg.Headers())
	}

	if resMsg.SenderEndpoint() != simRes.SenderEndpoint() {
		t.Errorf("expected res sender endpoint to be %s; got %s", simRes.SenderEndpoint(), resMsg.SenderEndpoint())
	}

	if resMsg.Receiver() != simRes.Receiver() {
		t.Errorf("expected res receiver to be %s; got %s", simRes.Receiver(), resMsg.Receiver())
	}

	if resMsg.ReceiverEndpoint() != simRes.ReceiverEndpoint() {
		t.Errorf("expected res receiver endpoint to be %s; got %s", simRes.ReceiverEndpoint(), resMsg.ReceiverEndpoint())
	}

	if resMsg.ReceiverVersion() != simRes.ReceiverVersion() {
		t.Errorf("expected res receiver version to be %s; got %s", simRes.ReceiverVersion(), resMsg.ReceiverVersion())
	}

	payload, err := resMsg.Payload()
	if err != nil {
		t.Error(err)
	}

	if !bytes.Equal(payload, simRes.PayloadField) {
		t.Errorf("expected response body to be %s; got %s", string(simRes.PayloadField), string(payload))
	}
}

func TestClientWorkerWhenReceivingResponsesWithBogusID(t *testing.T) {
	orig := dialAndGetChan
	defer func() {
		dialAndGetChan = orig
	}()

	mc := &mockChannel{
		consumeChan: make(chan amqpClient.Delivery),
		pubQueue:    make(chan amqpClient.Publishing, 1),
	}
	dialAndGetChan = func(_ string) (amqpChannel, io.Closer, chan *amqpClient.Error, error) {
		return mc, mc, nil, nil
	}

	tr := New()
	err := tr.Dial(transport.ModeClient)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close(transport.ModeClient)

	req := transport.MakeGenericMessage()
	req.SenderField = "test"
	req.SenderEndpointField = "test-endpoint"
	req.ReceiverField = "service"
	req.ReceiverEndpointField = "remote-endpoint"

	resChan := tr.Request(req)

	select {
	case <-mc.pubQueue:
	case <-time.After(1 * time.Second):
		t.Fatal("timeout while waiting for client worker to publish request")
	}

	// Simulate incoming response with bogus ID
	simRes := transport.MakeGenericMessage()
	simRes.SenderField = "service"
	simRes.SenderEndpointField = "remote-endpoint"
	simRes.ReceiverField = "test"
	simRes.ReceiverEndpointField = "test-endpoint"
	simRes.IDField = "unknown-ID"
	mc.consumeChan <- makeAmqpDelivery(simRes, mc)

	select {
	case <-resChan:
		t.Fatal("received unexpected response message")
	case <-time.After(1 * time.Second):
	}
}

func TestClientWorkerReturnHandling(t *testing.T) {
	orig := dialAndGetChan
	defer func() {
		dialAndGetChan = orig
	}()

	mc := &mockChannel{
		pubQueue: make(chan amqpClient.Publishing, 1),
	}
	dialAndGetChan = func(_ string) (amqpChannel, io.Closer, chan *amqpClient.Error, error) {
		return mc, mc, nil, nil
	}

	tr := New()
	err := tr.Dial(transport.ModeClient)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close(transport.ModeClient)

	specs := []struct {
		ReplyCode uint16
		ExpError  error
	}{
		{replyCodeNotFound, transport.ErrNotFound},
		{replyCodeNotAuthorized, transport.ErrNotAuthorized},
		{500, transport.ErrServiceUnavailable},
	}

	for specIndex, spec := range specs {
		req := transport.MakeGenericMessage()
		req.SenderField = "test"
		req.SenderEndpointField = "test-endpoint"
		req.ReceiverField = "service"
		req.ReceiverEndpointField = "remote-endpoint"
		resChan := tr.Request(req)

		select {
		case <-mc.pubQueue:
		case <-time.After(1 * time.Second):
			t.Errorf("[spec %d] timeout while waiting for client worker to publish request", specIndex)
			continue
		}

		// Simulate incoming return
		mc.returnChan <- amqpClient.Return{
			CorrelationId: req.ID(),
			ReplyCode:     spec.ReplyCode,
		}

		var resMsg transport.ImmutableMessage
		select {
		case resMsg = <-resChan:
		case <-time.After(1 * time.Second):
			t.Errorf("[spec %d] timeout waiting for response", specIndex)
			continue
		}

		_, err := resMsg.Payload()
		if err != spec.ExpError {
			t.Errorf("[spec %d] expected to get error %v; got %v", specIndex, spec.ExpError, err)
		}
	}
}

func TestClientWorkerReturnHandlingWithBogusID(t *testing.T) {
	orig := dialAndGetChan
	defer func() {
		dialAndGetChan = orig
	}()

	mc := &mockChannel{
		pubQueue: make(chan amqpClient.Publishing, 1),
	}
	dialAndGetChan = func(_ string) (amqpChannel, io.Closer, chan *amqpClient.Error, error) {
		return mc, mc, nil, nil
	}

	tr := New()
	err := tr.Dial(transport.ModeClient)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close(transport.ModeClient)

	req := transport.MakeGenericMessage()
	req.SenderField = "test"
	req.SenderEndpointField = "test-endpoint"
	req.ReceiverField = "service"
	req.ReceiverEndpointField = "remote-endpoint"
	resChan := tr.Request(req)

	select {
	case <-mc.pubQueue:
	case <-time.After(1 * time.Second):
		t.Errorf("timeout while waiting for client worker to publish request")
	}

	// Simulate incoming return with bogus ID
	mc.returnChan <- amqpClient.Return{
		CorrelationId: "unknown-ID",
		ReplyCode:     replyCodeNotFound,
	}

	select {
	case <-resChan:
		t.Fatal("received unexpected response message")
	case <-time.After(1 * time.Second):
	}
}

func TestAmqpResponseDecoder(t *testing.T) {
	expHeaders := amqpClient.Table{
		"Foo": "foo_val",
		"Bar": "bar_val",
	}

	specs := []struct {
		ContentType       string
		Body              string
		ExpPayload        []byte
		ExpError          error
		MatchErrAsPointer bool
	}{
		{contentTypeError, transport.ErrServiceUnavailable.Error(), nil, transport.ErrServiceUnavailable, true},
		{contentTypeError, transport.ErrNotAuthorized.Error(), nil, transport.ErrNotAuthorized, true},
		{contentTypeError, transport.ErrTimeout.Error(), nil, transport.ErrTimeout, true},
		{contentTypeError, "recovered from panic", nil, errors.New("recovered from panic"), false},
		{contentTypeError, "", nil, errors.New("unknown error"), false},
		{contentTypeUsrvData, "response data", []byte("response data"), nil, false},
	}

	res := transport.MakeGenericMessage()
	for specIndex, spec := range specs {
		decodeAmqpResponse(
			&amqpClient.Delivery{
				Headers:     expHeaders,
				ContentType: spec.ContentType,
				Body:        []byte(spec.Body),
			},
			res,
		)

		for k, v := range expHeaders {
			if res.HeadersField[k] != v {
				t.Errorf("[spec %d] expected res header field %q to contain value %q; got %v", specIndex, k, v, res.HeadersField[k])
			}
		}

		payload, err := res.Payload()
		if spec.ExpError != nil {
			switch {
			case spec.MatchErrAsPointer && err != spec.ExpError:
				t.Errorf("[spec %d] expected to get pre-defined error instance %v", specIndex, spec.ExpError)
			case err == nil || spec.ExpError.Error() != err.Error():
				t.Errorf("[spec %d] expected to get error %v; got %v", specIndex, spec.ExpError, err)
			}
		} else {
			if err != nil {
				t.Errorf("[spec %d] got unexpected error %v", specIndex, err)
			}

			if !bytes.Equal(payload, spec.ExpPayload) {
				t.Errorf("[spec %d] expected to get payload %v; got %v", specIndex, spec.ExpPayload, payload)
			}
		}
	}
}

func TestRedialLogic(t *testing.T) {
	orig := dialAndGetChan
	defer func() {
		dialAndGetChan = orig
	}()

	mc := &mockChannel{}
	dialAndGetChan = func(_ string) (amqpChannel, io.Closer, chan *amqpClient.Error, error) {
		return mc, mc, nil, nil
	}

	specs := []struct {
		serverInstances int
		clientInstances int
		redialError     error
	}{
		{1, 0, nil},
		{2, 1, nil},
		{0, 1, nil},
		{1, 0, errors.New("amqp consume error")},
		{2, 1, errors.New("amqp consume error")},
		{0, 1, errors.New("amqp consume error")},
	}

	for specIndex, spec := range specs {
		tr := New()
		mc.consumeErr = nil

		for i := 0; i < spec.serverInstances; i++ {
			if err := tr.Dial(transport.ModeServer); err != nil {
				t.Errorf("[spec %d] %v", specIndex, err)
				continue
			}
		}
		for i := 0; i < spec.clientInstances; i++ {
			if err := tr.Dial(transport.ModeClient); err != nil {
				t.Errorf("[spec %d] %v", specIndex, err)
				continue
			}
		}

		tr.rwMutex.Lock()
		if tr.serverRefCount != spec.serverInstances {
			t.Errorf("[spec %d] expected server ref count to be %d; got %d", specIndex, spec.serverInstances, tr.serverRefCount)
			tr.rwMutex.Unlock()
			continue
		}

		if tr.clientRefCount != spec.clientInstances {
			t.Fatalf("[spec %d] expected client ref count to be %d; got %d", specIndex, spec.clientInstances, tr.clientRefCount)
			tr.rwMutex.Unlock()
			continue
		}
		tr.rwMutex.Unlock()

		// Setup next reconnection attempt error and force a redial
		mc.consumeErr = spec.redialError
		tr.redial()

		tr.rwMutex.Lock()

		switch {
		case spec.redialError != nil:
			if tr.amqpChan != nil {
				t.Errorf("[spec %d] expected amqp channel reference to be nil", specIndex)
			}

			if tr.amqpConnCloser != nil {
				t.Errorf("[spec %d] expected amqp conn closer reference to be nil", specIndex)
			}
		default:
			if tr.amqpChan == nil {
				t.Errorf("[spec %d] expected amqp channel reference not to be nil", specIndex)
			}

			if tr.amqpConnCloser == nil {
				t.Errorf("[spec %d] expected amqp conn closer reference not to be nil", specIndex)
			}
		}
		tr.rwMutex.Unlock()
	}
}

func TestRedialOnDynamicCfgChange(t *testing.T) {
	orig := dialAndGetChan
	defer func() {
		dialAndGetChan = orig
	}()

	mc := &mockChannel{}
	var dialCount int32
	dialAndGetChan = func(e string) (amqpChannel, io.Closer, chan *amqpClient.Error, error) {
		atomic.AddInt32(&dialCount, 1)
		return mc, mc, nil, nil
	}

	tr := New()
	tr.amqpURI.Set("foo")
	if err := tr.Dial(transport.ModeServer); err != nil {
		t.Fatal(err)
	}
	defer tr.Close(transport.ModeServer)

	// Trigger re-dial via a cfg change; the redial will fail but the monitor
	// must still remain active
	<-time.After(100 * time.Millisecond)
	mc.lock.Lock()
	mc.consumeErr = errors.New("amqp consume error")
	mc.lock.Unlock()
	tr.amqpURI.Set("bar")
	<-time.After(100 * time.Millisecond)

	// Trigger re-dial via a cfg change; this time it should work
	<-time.After(100 * time.Millisecond)
	mc.lock.Lock()
	mc.consumeErr = nil
	mc.lock.Unlock()
	tr.amqpURI.Set("bar2")
	<-time.After(100 * time.Millisecond)

	expDialCount := 3
	if int(atomic.LoadInt32(&dialCount)) != expDialCount {
		t.Fatalf("expected dial count to be %d; got %d", expDialCount, atomic.LoadInt32(&dialCount))
	}

	tr.rwMutex.Lock()
	connected := tr.amqpChan != nil
	tr.rwMutex.Unlock()

	if !connected {
		t.Fatal("expected transport to be connected")
	}
}

func TestRefCounting(t *testing.T) {
	orig := dialAndGetChan
	defer func() {
		dialAndGetChan = orig
	}()

	mc := &mockChannel{
		pubQueue: make(chan amqpClient.Publishing, 1),
	}
	dialAndGetChan = func(_ string) (amqpChannel, io.Closer, chan *amqpClient.Error, error) {
		return mc, mc, nil, nil
	}

	var err error
	tr := New()

	// Dial twice in server and in client mode
	for i := 0; i < 2; i++ {
		if err = tr.Dial(transport.ModeServer); err != nil {
			t.Fatal(err)
		}
		if err = tr.Dial(transport.ModeClient); err != nil {
			t.Fatal(err)
		}
	}

	// Close a client and a server
	if err = tr.Close(transport.ModeClient); err != nil {
		t.Fatal(err)
	}
	if err = tr.Close(transport.ModeServer); err != nil {
		t.Fatal(err)
	}

	if mc.CloseCount() != 0 {
		t.Fatalf("expected transport not to close the amqp channel; channel.Close calls: %d", mc.CloseCount())
	}

	// Close the second client. The transport channel should stay open for
	// the server and calls to Close(transport.ModeClient) and Request should
	// now fail with ErrTransportClosed
	if err = tr.Close(transport.ModeClient); err != nil {
		t.Fatal(err)
	}

	if err = tr.Close(transport.ModeClient); err != transport.ErrTransportClosed {
		t.Fatalf("expected calling Close when client refCount set to 0 to return ErrTransportClosed; got %v", err)
	}

	res := <-tr.Request(transport.MakeGenericMessage())
	if _, err = res.Payload(); err != transport.ErrTransportClosed {
		t.Fatalf("expected calling Request when client refCount set to 0 to return ErrTransportClosed; got %v", err)
	}

	if mc.CloseCount() != 0 {
		t.Fatalf("expected transport not to close the amqp channel; channel.Close calls: %d", mc.CloseCount())
	}

	// When we close the second server, the transport should close the channel AND the amqp connection
	// Since we use the same mock for both, we expect mockChannel to receive 2 close calls
	if err = tr.Close(transport.ModeServer); err != nil {
		t.Fatal(err)
	}

	expCloseCount := 2
	if mc.CloseCount() != expCloseCount {
		t.Fatalf("expected mock close count to be %d; got %d", expCloseCount, mc.CloseCount())
	}

	// Trying to close the server again should fail with ErrTransportClosed
	if err = tr.Close(transport.ModeServer); err != transport.ErrTransportClosed {
		t.Fatalf("expected calling Close when server refCount set to 0 to return ErrTransportClosed; got %v", err)
	}
}

func TestConcurrentRequestsUsingRealAmqp(t *testing.T) {
	amqpEnvFlag := os.Getenv("TRANSPORT_AMQP_URI")
	if testing.Short() || amqpEnvFlag == "" {
		t.Skip("skipping real AMQP instance integration test")
	}

	tr := New()
	tr.amqpURI.Set(amqpEnvFlag)

	if err := tr.Dial(transport.ModeServer); err != nil {
		t.Fatal(err)
	}
	defer tr.Close(transport.ModeServer)

	if err := tr.Dial(transport.ModeClient); err != nil {
		t.Fatal(err)
	}
	defer tr.Close(transport.ModeClient)

	expReqHeaders := map[string]string{
		"Foo": "foo_val",
		"Bar": "bar_val",
	}
	expResHeaders := map[string]string{
		"Foo": "foo_val1",
		"Bar": "bar_val2",
	}
	expSender := "from_service"
	expSenderEndpoint := "from_endpoint"
	expReceiver := "to_service"
	expReceiverEndpoint := "to_endpoint"

	handler := transport.HandlerFunc(func(reqMsg transport.ImmutableMessage, resMsg transport.Message) {
		if !reflect.DeepEqual(reqMsg.Headers(), expReqHeaders) {
			t.Errorf("[msg %s] expected req headers to be %v; got %v", reqMsg.ID(), expReqHeaders, reqMsg.Headers())
		}

		if reqMsg.Sender() != expSender {
			t.Errorf("[msg %s] expected req sender endpoint to be %s; got %s", reqMsg.ID(), expSender, reqMsg.Sender())
		}
		if reqMsg.SenderEndpoint() != expSenderEndpoint {
			t.Errorf("[msg %s] expected req sender endpoint to be %s; got %s", reqMsg.ID(), expSenderEndpoint, reqMsg.SenderEndpoint())
		}

		if reqMsg.Receiver() != expReceiver {
			t.Errorf("[msg %s] expected req receiver to be %s; got %s", reqMsg.ID(), expReceiver, reqMsg.Receiver())
		}

		if reqMsg.ReceiverEndpoint() != expReceiverEndpoint {
			t.Errorf("[msg %s] expected req receiver endpoint to be %s; got %s", reqMsg.ID(), expReceiverEndpoint, reqMsg.ReceiverEndpoint())
		}

		payload, err := reqMsg.Payload()
		if err != nil {
			t.Error(err)
		}

		expPayload := []byte(fmt.Sprintf("request %s", reqMsg.ID()))
		if !bytes.Equal(payload, expPayload) {
			t.Errorf("[msg %s] expected request body to be %s; got %s", reqMsg.ID(), string(expPayload), string(payload))
		}

		// Build response
		resMsg.SetHeaders(expResHeaders)
		resMsg.SetPayload([]byte(fmt.Sprintf("response %s", reqMsg.ID())), nil)
	})

	if err := tr.Bind("", expReceiver, expReceiverEndpoint, handler); err != nil {
		t.Fatal(err)
	}

	numReqs := 100
	var wg sync.WaitGroup
	wg.Add(numReqs)
	for i := 0; i < numReqs; i++ {
		go func(reqId int) {
			defer wg.Done()

			reqMsg := transport.MakeGenericMessage()
			reqMsg.IDField = fmt.Sprint(reqId)
			reqMsg.SenderField = expSender
			reqMsg.SenderEndpointField = expSenderEndpoint
			reqMsg.ReceiverField = expReceiver
			reqMsg.ReceiverEndpointField = expReceiverEndpoint
			reqMsg.HeadersField = expReqHeaders
			reqMsg.PayloadField = []byte(fmt.Sprintf("request %s", reqMsg.ID()))
			resMsg := <-tr.Request(reqMsg)

			if resMsg.Sender() != expReceiver {
				t.Errorf("[msg %s] expected res sender endpoint to be %s; got %s", resMsg.ID(), expReceiver, resMsg.Sender())
			}
			if resMsg.SenderEndpoint() != expReceiverEndpoint {
				t.Errorf("[msg %s] expected res sender endpoint to be %s; got %s", resMsg.ID(), expReceiverEndpoint, resMsg.SenderEndpoint())
			}

			if resMsg.Receiver() != expSender {
				t.Errorf("[msg %s] expected res receiver to be %s; got %s", resMsg.ID(), expSender, resMsg.Receiver())
			}

			if resMsg.ReceiverEndpoint() != expSenderEndpoint {
				t.Errorf("[msg %s] expected res receiver endpoint to be %s; got %s", resMsg.ID(), expSenderEndpoint, resMsg.ReceiverEndpoint())
			}

			payload, err := resMsg.Payload()
			if err != nil {
				t.Errorf("[msg %s] received error response %v", reqMsg.ID(), err)
				return
			}

			expPayload := []byte(fmt.Sprintf("response %s", reqMsg.ID()))
			if !bytes.Equal(payload, expPayload) {
				t.Errorf("[msg %s] expected response payload to be %s; got %v", reqMsg.ID(), string(expPayload), payload)
				return
			}

			if !reflect.DeepEqual(resMsg.Headers(), expResHeaders) {
				t.Errorf("[msg %s] expected response headers to be %v; got %v", reqMsg.ID(), expResHeaders, resMsg.Headers())
			}
		}(i)
	}
	wg.Wait()
}

func TestRoutingErrorsUsingRealAmqp(t *testing.T) {
	amqpEnvFlag := os.Getenv("TRANSPORT_AMQP_URI")
	if testing.Short() || amqpEnvFlag == "" {
		t.Skip("skipping real AMQP instance integration test")
	}

	tr := New()
	tr.amqpURI.Set(amqpEnvFlag)

	if err := tr.Dial(transport.ModeClient); err != nil {
		t.Fatal(err)
	}
	defer tr.Close(transport.ModeClient)

	reqMsg := transport.MakeGenericMessage()
	reqMsg.SenderField = "from_service"
	reqMsg.SenderEndpointField = "from_endpoint"
	reqMsg.ReceiverField = "to_service"
	reqMsg.ReceiverEndpointField = "to_endpoint"
	resMsg := <-tr.Request(reqMsg)

	if _, err := resMsg.Payload(); err != transport.ErrServiceUnavailable {
		t.Fatalf("expected to get ErrServiceUnavailable; got %v", err)
	}
}

func TestDialErrorsUsingRealAmqp(t *testing.T) {
	amqpEnvFlag := os.Getenv("TRANSPORT_AMQP_URI")
	if testing.Short() || amqpEnvFlag == "" {
		t.Skip("skipping real AMQP instance integration test")
	}

	tr := New()
	tr.amqpURI.Set("invalid")

	if err := tr.Dial(transport.ModeClient); err == nil {
		t.Fatal("expected dial to fail")
	}
}

func TestChannelErrorsUsingRealAmqp(t *testing.T) {
	amqpEnvFlag := os.Getenv("TRANSPORT_AMQP_URI")
	if testing.Short() || amqpEnvFlag == "" {
		t.Skip("skipping real AMQP instance integration test")
	}

	specs := [][]byte{
		// connection ok frame
		{0x0a, 0x00, 0x29, 0x00, amqpFrameTerminator},
		// channel ok frame
		{0x14, 0x00, 0x0b, 0x00, 0x00, 0x00, 0x00, amqpFrameTerminator},
	}

	tr := New()
	for specIndex, framePattern := range specs {
		proxy := newAmqpProxy(t, amqpEnvFlag, framePattern)
		defer proxy.Close()

		tr.amqpURI.Set(proxy.Endpoint())
		err := tr.Dial(transport.ModeClient)
		if err == nil {
			t.Errorf("[spec %d] expected tr.Dial() to return an error", specIndex)
		}
	}
}

func TestClientRedialUsingRealAmqp(t *testing.T) {
	amqpEnvFlag := os.Getenv("TRANSPORT_AMQP_URI")
	if testing.Short() || amqpEnvFlag == "" {
		t.Skip("skipping real AMQP instance integration test")
	}

	tr := New()
	proxy := newAmqpProxy(t, amqpEnvFlag, nil)
	defer proxy.Close()

	tr.amqpURI.Set(proxy.Endpoint())
	if err := tr.Dial(transport.ModeClient); err != nil {
		t.Fatal(err)
	}
	defer tr.Close(transport.ModeClient)

	<-time.After(100 * time.Millisecond)
	proxy.killConnChan <- struct{}{}
	<-time.After(100 * time.Millisecond)

	// Transport should automatically re-dial
	expConnCount := 2
	if proxy.ConnectionCount() != expConnCount {
		t.Fatalf("expected proxy to receive %d connection attempts; got %d", expConnCount, proxy.ConnectionCount())
	}
}

func TestCfgChangeRedialUsingRealAmqp(t *testing.T) {
	amqpEnvFlag := os.Getenv("TRANSPORT_AMQP_URI")
	if testing.Short() || amqpEnvFlag == "" {
		t.Skip("skipping real AMQP instance integration test")
	}

	tr := New()
	proxy := newAmqpProxy(t, amqpEnvFlag, nil)
	defer proxy.Close()

	tr.amqpURI.Set(amqpEnvFlag)
	if err := tr.Dial(transport.ModeClient); err != nil {
		t.Fatal(err)
	}
	defer tr.Close(transport.ModeClient)

	<-time.After(100 * time.Millisecond)
	tr.amqpURI.Set(proxy.Endpoint())
	<-time.After(100 * time.Millisecond)

	// Transport should automatically re-dial
	expConnCount := 1
	if proxy.ConnectionCount() != expConnCount {
		t.Fatalf("expected proxy to receive %d connection attempts; got %d", expConnCount, proxy.ConnectionCount())
	}
}

func TestServerRedialUsingRealAmqp(t *testing.T) {
	amqpEnvFlag := os.Getenv("TRANSPORT_AMQP_URI")
	if testing.Short() || amqpEnvFlag == "" {
		t.Skip("skipping real AMQP instance integration test")
	}

	tr := New()
	proxy := newAmqpProxy(t, amqpEnvFlag, nil)
	defer proxy.Close()

	tr.amqpURI.Set(proxy.Endpoint())
	if err := tr.Dial(transport.ModeServer); err != nil {
		t.Fatal(err)
	}
	defer tr.Close(transport.ModeServer)

	<-time.After(100 * time.Millisecond)
	proxy.killConnChan <- struct{}{}
	<-time.After(100 * time.Millisecond)

	// Transport should automatically re-dial
	expConnCount := 2
	if proxy.ConnectionCount() != expConnCount {
		t.Fatalf("expected proxy to receive %d connection attempts; got %d", expConnCount, proxy.ConnectionCount())
	}
}

func TestFactory(t *testing.T) {
	tr1 := SingletonFactory()
	tr2 := SingletonFactory()

	if tr1 != tr2 {
		t.Fatalf("expected singleton factory to return the same instance")
	}
}

type amqpProxy struct {
	connections  int32
	killConnChan chan struct{}

	matchPattern []byte

	proxyListener net.Listener
}

func (p *amqpProxy) connWorker(realAmqpHost string) {
	p.killConnChan = make(chan struct{})

	for {
		srcConn, err := p.proxyListener.Accept()
		if err != nil {
			return
		}

		dstConn, err := net.Dial("tcp4", realAmqpHost)
		if err != nil {
			srcConn.Close()
			return
		}

		atomic.AddInt32(&p.connections, 1)

		// Pipe srcConn data to dstConn
		go io.Copy(dstConn, srcConn)

		// Read amqp frames from dstConn and pipe them to srcConn
		// until we encounter matchPattern in the frame data.
		go func(killConnChan chan struct{}) {
			bufReader := bufio.NewReader(dstConn)
			for {
				frame, err := bufReader.ReadBytes(amqpFrameTerminator)
				if err != nil {
					killConnChan <- struct{}{}
					return
				}

				// If the data contains our match pattern close both connections.
				if p.matchPattern != nil && bytes.Contains(frame, p.matchPattern) {
					killConnChan <- struct{}{}
					return
				}

				srcConn.Write(frame)
			}
		}(p.killConnChan)

		<-p.killConnChan
		srcConn.Close()
		dstConn.Close()
	}
}

func (p *amqpProxy) ConnectionCount() int {
	return int(atomic.LoadInt32(&p.connections))
}

func (p *amqpProxy) Close() error {
	return p.proxyListener.Close()
}

func (p *amqpProxy) Endpoint() string {
	return fmt.Sprintf("amqp://guest:guest@%s/", p.proxyListener.Addr().String())
}

func newAmqpProxy(t *testing.T, amqpHost string, matchPattern []byte) *amqpProxy {
	amqpHost = strings.Replace(strings.Split(amqpHost, "@")[1], "/", "", -1)

	ln, err := net.Listen("tcp4", "localhost:")
	if err != nil {
		t.Fatal(err)
	}

	p := &amqpProxy{
		matchPattern:  matchPattern,
		proxyListener: ln,
	}

	go p.connWorker(amqpHost)
	return p
}

func makeAmqpDelivery(msg transport.ImmutableMessage, mc amqpChannel) amqpClient.Delivery {
	d := amqpClient.Delivery{
		RoutingKey:    routingKey(msg.ReceiverVersion(), msg.Receiver(), msg.ReceiverEndpoint()),
		CorrelationId: msg.ID(),
		AppId:         msg.Sender(),
		Type:          msg.SenderEndpoint(),
		Headers:       amqpClient.Table{},
		Acknowledger:  mc,
	}

	payload, err := msg.Payload()
	if err != nil {
		d.ContentType = contentTypeError
		d.Body = []byte(err.Error())
	} else {
		d.ContentType = contentTypeUsrvData
		d.Body = payload
	}

	for k, v := range msg.Headers() {
		d.Headers[k] = v
	}

	return d
}

type mockChannel struct {
	lock sync.Mutex

	ackErr          error
	nackErr         error
	rejectErr       error
	pubErr          error
	exDeclareErr    error
	queueDeclareErr error
	queueBindErr    error
	consumeErr      error
	closeErr        error
	consumeChan     chan amqpClient.Delivery
	returnChan      chan amqpClient.Return
	pubQueue        chan amqpClient.Publishing

	ackCount    int32
	nackCount   int32
	rejectCount int32
	closeCount  int32
}

func (mc *mockChannel) Reject(tag uint64, requeue bool) error {
	mc.lock.Lock()
	defer mc.lock.Unlock()

	if mc.rejectErr != nil {
		return mc.rejectErr
	}

	atomic.AddInt32(&mc.rejectCount, 1)
	return nil
}

func (mc *mockChannel) RejectCount() int {
	return int(atomic.LoadInt32(&mc.rejectCount))
}

func (mc *mockChannel) Ack(tag uint64, multiple bool) error {
	mc.lock.Lock()
	defer mc.lock.Unlock()

	if mc.ackErr != nil {
		return mc.ackErr
	}

	atomic.AddInt32(&mc.ackCount, 1)
	return nil
}

func (mc *mockChannel) AckCount() int {
	return int(atomic.LoadInt32(&mc.ackCount))
}

func (mc *mockChannel) Nack(tag uint64, multiple bool, requeue bool) error {
	mc.lock.Lock()
	defer mc.lock.Unlock()

	if mc.nackErr != nil {
		return mc.nackErr
	}

	atomic.AddInt32(&mc.nackCount, 1)
	return nil
}

func (mc *mockChannel) NackCount() int {
	return int(atomic.LoadInt32(&mc.nackCount))
}

func (mc *mockChannel) NotifyReturn(c chan amqpClient.Return) chan amqpClient.Return {
	mc.returnChan = c
	return c
}

func (mc *mockChannel) Publish(exchange, key string, mandatory, immediate bool, msg amqpClient.Publishing) error {
	mc.lock.Lock()
	defer mc.lock.Unlock()

	if mc.pubErr != nil {
		return mc.pubErr
	}

	if mc.pubQueue != nil {
		mc.pubQueue <- msg
	}

	return nil
}

func (mc *mockChannel) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqpClient.Table) error {
	mc.lock.Lock()
	defer mc.lock.Unlock()

	return mc.exDeclareErr
}

func (mc *mockChannel) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqpClient.Table) (amqpClient.Queue, error) {
	mc.lock.Lock()
	defer mc.lock.Unlock()

	return amqpClient.Queue{
		Name: "test-queue",
	}, mc.queueDeclareErr
}

func (mc *mockChannel) QueueBind(name, key, exchange string, noWait bool, args amqpClient.Table) error {
	mc.lock.Lock()
	defer mc.lock.Unlock()

	return mc.queueBindErr
}

func (mc *mockChannel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqpClient.Table) (<-chan amqpClient.Delivery, error) {
	mc.lock.Lock()
	defer mc.lock.Unlock()

	return mc.consumeChan, mc.consumeErr
}

func (mc *mockChannel) Close() error {
	mc.lock.Lock()
	defer mc.lock.Unlock()

	if mc.closeErr != nil {
		return mc.closeErr
	}

	atomic.AddInt32(&mc.closeCount, 1)
	return nil
}

func (mc *mockChannel) CloseCount() int {
	return int(atomic.LoadInt32(&mc.closeCount))
}
