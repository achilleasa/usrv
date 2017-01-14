package client

import (
	"context"

	"github.com/achilleasa/usrv"
	"github.com/achilleasa/usrv/encoding"
	"github.com/achilleasa/usrv/transport"
)

// Client implements an RPC client.
//
// The client uses a codec instance to marshal and unmarshal the request and
// response objects accepted by the endpoint handlers into the low-level message
// format used by the attached transport. Unless overriden by the WithCodec config
// option, the client will invoke usrv.DefaultCodecFactory to fetch a codec instance.
//
// Unless overriden with the WithTransport config option, the client will invoke
// usrv.DefaultTransportFactory to fetch a transport instance.
type Client struct {
	// The transport used by the client.
	transport transport.Transport

	// A codec instance for handling marshaling/unmarshaling.
	codec encoding.Codec

	// The name of the service exposed by the client.
	serviceName string

	// A marshaler and unmarshaler instance obtained by the codec.
	marshaler   encoding.Marshaler
	unmarshaler encoding.Unmarshaler
}

// NewClient creates a new client instance for the given service name and applies
// any supplied client options.
func NewClient(serviceName string, options ...Option) (*Client, error) {
	c := &Client{
		serviceName: serviceName,
	}

	// Apply options
	var err error
	for _, opt := range options {
		err = opt(c)
		if err != nil {
			return nil, err
		}
	}

	// Apply defaults
	c.setDefaults()

	c.marshaler, c.unmarshaler = c.codec.Marshaler(), c.codec.Unmarshaler()

	return c, nil
}

// setDefaults applies default settings for fields not set by a client option.
func (c *Client) setDefaults() {
	if c.transport == nil {
		c.transport = usrv.DefaultTransportFactory()
	}

	if c.codec == nil {
		c.codec = usrv.DefaultCodecFactory()
	}
}

// Request serializes the supplied request message, executes an RPC call to an
// endpoint and unserializes the response into the supplied response message
// instance.
//
// Calls to Request block till a response is received or the supplied context's
// deadline expires. In the latter case, Request will fail with a transport.ErrTimeout
// error. It is important to note that a request that times out on the client
// side may still be executed by the remote endpoint.
func (c *Client) Request(ctx context.Context, endpoint string, reqMessage, resMessage interface{}) error {
	req := transport.MakeGenericMessage()
	defer req.Close()

	// Serialize request object and populate request message
	req.PayloadField, req.ErrField = c.marshaler(reqMessage)
	if req.ErrField != nil {
		return req.ErrField
	}
	req.ReceiverField = c.serviceName
	req.ReceiverEndpointField = endpoint

	// Send request and wait for reply or for the context deadline to expire
	var res transport.ImmutableMessage
	select {
	case <-ctx.Done():
		return transport.ErrTimeout
	case res = <-c.transport.Request(req):
	}

	defer res.Close()
	resData, err := res.Payload()
	if err != nil {
		return err
	}

	// Unmarshal response in the supplied resMessage instance
	err = c.unmarshaler(resData, resMessage)
	if err != nil {
		return err
	}

	return nil
}
