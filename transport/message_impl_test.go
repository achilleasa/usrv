package transport

import (
	"reflect"
	"testing"
)

func TestGenericMessage(t *testing.T) {
	m := MakeGenericMessage()
	var iface interface{} = m

	if _, ok := iface.(ImmutableMessage); !ok {
		t.Fatal("generic message instance does not implement ImmutableMessage")
	}

	if _, ok := iface.(Message); !ok {
		t.Fatal("generic message instance does not implement Message")
	}

	expValue := GenerateID()
	m.IDField = expValue
	if m.ID() != expValue {
		t.Errorf("expected ID() to return %q; got %q", expValue, m.ID())
	}

	expValue = "Sender"
	m.SenderField = expValue
	if m.Sender() != expValue {
		t.Errorf("expected Sender() to return %q; got %q", expValue, m.Sender())
	}

	expValue = "SenderEndpoint"
	m.SenderEndpointField = expValue
	if m.SenderEndpoint() != expValue {
		t.Errorf("expected SenderEndpoint() to return %q; got %q", expValue, m.SenderEndpoint())
	}

	expValue = "Receiver"
	m.ReceiverField = expValue
	if m.Receiver() != expValue {
		t.Errorf("expected Receiver() to return %q; got %q", expValue, m.Receiver())
	}

	expValue = "ReceiverEndpoint"
	m.ReceiverEndpointField = expValue
	if m.ReceiverEndpoint() != expValue {
		t.Errorf("expected ReceiverEndpoint() to return %q; got %q", expValue, m.ReceiverEndpoint())
	}

	expValue = "v0"
	m.ReceiverVersionField = expValue
	if m.ReceiverVersion() != expValue {
		t.Errorf("expected ReceiverVersion() to return %q; got %q", expValue, m.ReceiverVersion())
	}

	expValue = "v1"
	m.SetReceiverVersion(expValue)
	if m.ReceiverVersion() != expValue {
		t.Errorf("expected SetReceiverVersion(%q) to update the version; got %q", expValue, m.ReceiverVersion())
	}

	expValue = "foo"
	m.SetHeader("kEy1", expValue)
	if m.Headers()["Key1"] != expValue {
		t.Errorf(`expected Headers() to contain value ("key1", %q); got value %q`, expValue, m.Headers()["Key1"])
	}

	expHeaders := map[string]string{
		"Key1": "value1",
		"Key2": "value2",
	}
	m.SetHeaders(expHeaders)
	if !reflect.DeepEqual(m.Headers(), expHeaders) {
		t.Errorf("expected headers to be %v; got %v", expHeaders, m.Headers())
	}

	expPayload := []byte("hello")
	m.SetPayload(expPayload, ErrNotAuthorized)

	payload, err := m.Payload()
	if !reflect.DeepEqual(payload, expPayload) {
		t.Errorf("payload body mismatch")
	}
	if err != ErrNotAuthorized {
		t.Errorf("payload error mismatch; expected %v; got %v", ErrNotAuthorized, err)
	}

	m.Close()
}
