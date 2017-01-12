package server

import (
	"github.com/achilleasa/usrv/encoding"
	"github.com/achilleasa/usrv/transport"
)

// Option applies a configuration option to a server instance.
type Option func(s *Server) error

// WithTransport configures the server to use a specific transport instead
// of the default transport.
func WithTransport(transport transport.Transport) Option {
	return func(s *Server) error {
		s.transport = transport
		return nil
	}
}

// WithCodec configures the server to use a specific codec instance instead
// of the default codec.
func WithCodec(codec encoding.Codec) Option {
	return func(s *Server) error {
		s.codec = codec
		return nil
	}
}

// WithPanicHandler configures the server to use a user-defined panic handler.
func WithPanicHandler(handler PanicHandler) Option {
	return func(s *Server) error {
		s.panicHandler = handler
		return nil
	}
}
