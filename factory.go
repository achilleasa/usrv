package usrv

import (
	"github.com/achilleasa/usrv/encoding"
	"github.com/achilleasa/usrv/encoding/json"
	"github.com/achilleasa/usrv/transport"
	"github.com/achilleasa/usrv/transport/http"
)

var (
	// DefaultTransportFactory is a function that returns back a new
	// instance of the default usrv transport.
	//
	// When usrv is imported, DefaultTransportFactory is set up to return
	// HTTP transport instances.
	DefaultTransportFactory func() transport.Transport

	// DefaultCodecFactory is a function that returns back a new
	// instance of the default Codec used for marshaling and unmarshaling
	// requests and response objects.
	//
	// When usrv is imported, DefaultCodecFactory is set up to return JSON
	// codec instances.
	DefaultCodecFactory func() encoding.Codec
)

func init() {
	DefaultTransportFactory = http.HTTPTransportFactory
	DefaultCodecFactory = json.Codec
}
