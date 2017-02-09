package transport

import "testing"

func TestHandlerFunc(t *testing.T) {
	called := false
	f := HandlerFunc(func(_ ImmutableMessage, _ Message) {
		called = true
	})

	f.Process(nil, nil)

	if !called {
		t.Fatalf("expected wrapped HandlerFunc to call f(req, res)")
	}
}
