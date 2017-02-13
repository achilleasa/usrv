package memory

import (
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/achilleasa/usrv/transport"
)

func TestInMemoryBindVersions(t *testing.T) {
	tr := NewInMemory()
	defer tr.Close()

	handler := transport.HandlerFunc(func(_ transport.ImmutableMessage, _ transport.Message) {})
	var err error

	err = tr.Bind("v0", "service", "endpoint", handler)
	if err != nil {
		t.Fatal(err)
	}

	expLen := 2
	if len(tr.bindings) != expLen {
		t.Fatalf("expected bind to generate an additional binding without version")
	}

	err = tr.Bind("v1", "service", "endpoint", handler)
	if err != nil {
		t.Fatal(err)
	}
}

func TestInMemoryErrors(t *testing.T) {
	tr := NewInMemory()
	defer tr.Close()

	// Send to unknown endpoint
	resChan := tr.Request(newMessage("test/case", "unknown/endpoint"))
	res := <-resChan
	if _, err := res.Payload(); err != transport.ErrNotFound {
		t.Fatalf("expected err %v; got %v", transport.ErrNotFound, err)
	}

	// Try to bind to already bound endpoint
	err := tr.Bind("", "service", "endpoint", transport.HandlerFunc(func(_ transport.ImmutableMessage, _ transport.Message) {}))
	if err != nil {
		t.Fatal(err)
	}
	err = tr.Bind("", "service", "endpoint", transport.HandlerFunc(func(_ transport.ImmutableMessage, _ transport.Message) {}))
	expError := `binding "service/endpoint" already defined`
	if err == nil || err.Error() != expError {
		t.Fatalf("expected to get error %q; got %v", expError, err)
	}

	err = tr.Dial()
	if err != nil {
		t.Fatal(err)
	}

	// Bind to dialed transport
	err = tr.Bind("", "service", "endpoint", transport.HandlerFunc(func(_ transport.ImmutableMessage, _ transport.Message) {}))
	if err != transport.ErrTransportAlreadyDialed {
		t.Fatalf("expected err %v; got %v", transport.ErrTransportAlreadyDialed, err)
	}

	// Close already closed transport
	err = tr.Close()
	if err != nil {
		t.Fatal(err)
	}
	err = tr.Close()
	if err != transport.ErrTransportClosed {
		t.Fatalf("expected err %v; got %v", transport.ErrTransportClosed, err)
	}
}

func TestInMemoryRPC(t *testing.T) {
	tr := InMemoryTransportFactory()
	defer tr.Close()

	expHeaders := map[string]string{
		"Key1": "value1",
		"Key2": "value2",
	}

	handleRPC := func(req transport.ImmutableMessage, res transport.Message) {
		expValues := []string{"fromService", "fromEndpoint", "toService", "toEndpoint", "hello"}
		if req.Sender() != expValues[0] {
			t.Errorf("expected sender to be %q; got %q", expValues[0], req.Sender())
		}
		expValues = expValues[1:]

		if req.SenderEndpoint() != expValues[0] {
			t.Errorf("expected sender endpoint to be %q; got %q", expValues[0], req.SenderEndpoint())
		}
		expValues = expValues[1:]

		if req.Receiver() != expValues[0] {
			t.Errorf("expected receiver to be %q; got %q", expValues[0], req.Receiver())
		}
		expValues = expValues[1:]

		if req.ReceiverEndpoint() != expValues[0] {
			t.Errorf("expected receiver to be %q; got %q", expValues[0], req.ReceiverEndpoint())
		}
		expValues = expValues[1:]

		payload, err := req.Payload()
		if err != nil {
			t.Fatal(err)
		}
		if string(payload) != expValues[0] {
			t.Errorf("expected payload to be %q; got %q", expValues[0], string(payload))
		}

		headers := req.Headers()
		if !reflect.DeepEqual(headers, expHeaders) {
			t.Errorf("header mismatch; expected %v; got %v", expHeaders, headers)
		}

		// Populate respone
		res.SetPayload([]byte("hello back!"), nil)
	}

	err := tr.Bind("v0", "toService", "toEndpoint", transport.HandlerFunc(handleRPC))

	if err != nil {
		t.Fatal(err)
	}

	err = tr.Dial()
	if err != nil {
		t.Fatal(err)
	}

	req := newMessage("fromService/fromEndpoint", "toService/toEndpoint")
	defer req.Close()
	req.SetPayload([]byte("hello"), nil)
	req.SetHeader("key1", "other")
	req.SetHeaders(expHeaders)
	resChan := tr.Request(req)

	// Wait for response
	res := <-resChan

	expValues := []string{"toService", "toEndpoint", "fromService", "fromEndpoint", "hello back!"}
	if res.Sender() != expValues[0] {
		t.Errorf("expected sender to be %q; got %q", expValues[0], res.Sender())
	}
	expValues = expValues[1:]

	if res.SenderEndpoint() != expValues[0] {
		t.Errorf("expected sender endpoint to be %q; got %q", expValues[0], res.SenderEndpoint())
	}
	expValues = expValues[1:]

	if res.Receiver() != expValues[0] {
		t.Errorf("expected receiver to be %q; got %q", expValues[0], res.Receiver())
	}
	expValues = expValues[1:]

	if res.ReceiverEndpoint() != expValues[0] {
		t.Errorf("expected receiver endpoint to be %q; got %q", expValues[0], res.ReceiverEndpoint())
	}
	expValues = expValues[1:]

	payload, err := res.Payload()
	if err != nil {
		t.Fatal(err)
	}
	if string(payload) != expValues[0] {
		t.Errorf("expected payload to be %q; got %q", expValues[0], string(payload))
	}

	// Try a request with a specific service verison request
	req.SetReceiverVersion("v0")
	resChan = tr.Request(req)
	res = <-resChan

	payload, err = res.Payload()
	if err != nil {
		t.Fatal(err)
	}

	if string(payload) != expValues[0] {
		t.Errorf("expected payload for versioned request to be %q; got %q", expValues[0], string(payload))
	}
}

func newMessage(from, to string) transport.Message {
	fromFields := strings.Split(from, "/")
	toFields := strings.Split(to, "/")

	m := transport.MakeGenericMessage()
	m.SenderField = fromFields[0]
	m.SenderEndpointField = fromFields[1]
	m.ReceiverField = toFields[0]
	m.ReceiverEndpointField = toFields[1]

	return m
}

func BenchmarkInMemory100Workers(b *testing.B) {
	benchInMemory(b, 100)
}

func benchInMemory(b *testing.B, workers int) {
	tr := NewInMemory()
	defer tr.Close()

	payload := []byte("test payload")
	msgPerWorker := (b.N / workers) + 1

	start := make(chan struct{}, 0)
	var wgStart, wgEnd sync.WaitGroup
	wgStart.Add(workers)
	wgEnd.Add(workers * msgPerWorker)
	for i := 0; i < workers; i++ {
		go func() {
			wgStart.Done()
			<-start

			for j := 0; j < msgPerWorker; j++ {
				msg := newMessage("benchmark/producer", "benchmark/consumer")
				msg.SetPayload(payload, nil)

				resChan := tr.Request(msg)
				res := <-resChan
				if _, err := res.Payload(); err != nil {
					b.Fatal(err)
				}
			}
			wgEnd.Add(-msgPerWorker)
		}()
	}

	err := tr.Bind("", "benchmark", "consumer", transport.HandlerFunc(func(_ transport.ImmutableMessage, res transport.Message) {
		res.SetPayload(payload, nil)
	}))
	if err != nil {
		b.Fatal(err)
	}

	// wait for all workers to start and reset benchmark time;
	// then start pumping messages
	wgStart.Wait()
	b.ResetTimer()
	close(start)

	wgEnd.Wait()
}
