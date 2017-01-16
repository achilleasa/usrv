package client

import (
	"github.com/achilleasa/usrv/encoding"
	"github.com/achilleasa/usrv/transport"
)

// Option applies a configuration option to a client instance.
type Option func(s *Client) error

// WithTransport configures the client to use a specific transport instead
// of the default transport.
func WithTransport(transport transport.Transport) Option {
	return func(s *Client) error {
		s.transport = transport
		return nil
	}
}

// WithCodec configures the client to use a specific codec instance instead
// of the default codec.
func WithCodec(codec encoding.Codec) Option {
	return func(s *Client) error {
		s.codec = codec
		return nil
	}
}

// WithMiddleware configures the client to use a set of client-specific middleware.
// The set of middleware will be executed after any globally defined middleware.
func WithMiddleware(middleware ...Middleware) Option {
	return func(s *Client) error {
		list := make([]Middleware, 0)
		for _, m := range middleware {
			if m == nil {
				continue
			}
			list = append(list, m)
		}

		s.middleware = append(s.middleware, list...)

		return nil
	}
}
