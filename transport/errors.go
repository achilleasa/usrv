package transport

import "errors"

// Transport errors
var (
	ErrTransportClosed        = errors.New("transport is closed")
	ErrTransportAlreadyDialed = errors.New("transport is already dialed; this operation can only be performed on a closed transport")
)

// Delivery errors
var (
	ErrNotFound      = errors.New("unknown receiver and/or endpoint")
	ErrTimeout       = errors.New("message delivery timed out")
	ErrNotAuthorized = errors.New("not authorized for this operation")
)
