package server

import (
	"github.com/achilleasa/usrv/encoding"
	"github.com/achilleasa/usrv/transport"
)

// Option applies a configuration option to a server instance.
type Option func(s *Server) error

// WithTransport configures the server to use a specific transport instead
// of the default transport.
func WithTransport(transport transport.Provider) Option {
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

// WithVersion defines the version of the service provided by the server.
// The version value is passed to Bind calls to the underlying transport.
func WithVersion(version string) Option {
	return func(s *Server) error {
		s.serviceVersion = version
		return nil
	}
}
