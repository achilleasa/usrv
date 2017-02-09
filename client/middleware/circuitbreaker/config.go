package circuitbreaker

import (
	"errors"
	"strings"
	"time"

	"github.com/achilleasa/usrv/config"
	"github.com/achilleasa/usrv/config/flag"
	"github.com/achilleasa/usrv/config/store"
	"github.com/achilleasa/usrv/transport"
)

var (
	// DefaultTripErrors is a list of errors that will be used by the circuit-breaker
	// if the TripErrors field of its configuration is not defined. This setting
	// will treat timeouts and remote endpoint recovered panic messages as
	// candidates for tripping the circuit-breaker.
	DefaultTripErrors = []error{
		transport.ErrServiceUnavailable,
		transport.ErrTimeout,
		errors.New("recovered from panic"),
	}

	// DefaultCoolOffPeriod will be used by the circuit-breaker if its config
	// does not provide a valid cool off period.
	DefaultCoolOffPeriod = 1 * time.Second
)

// Config is an interface implemented by objects that can be passed to the circuit-breaker
// factories.
//
// GetClosedError returns the error message that is used to abort requests when
// the circuit-breaker is closed.
//
// GetTripErrors returns a slice of errors that can cause the circuit-breaker to
// trip.
//
// GetTripThreshold returns the number of errors after which the circuit-breaker
// will transition from opened to closed state.
//
// GetCoolOffPeriod returns the number of nanoseconds that must elapse before the
// circuit-breaker transitions from closed state to half-open state.
//
// GetResetThreshold returns the number of successful requests before the
// circuit-breaker transitions from half-open to opened state.
//
// GetStateChangeChan returns a channel where the circuit-breaker will publish
// its new state whenever its state changes.
type Config interface {
	GetClosedError() error
	GetTripErrors() []error
	GetTripThreshold() *flag.Uint32
	GetCoolOffPeriod() *flag.Int64
	GetResetThreshold() *flag.Uint32
	GetStateChangeChan() chan<- State
}

// StaticConfig defines a static circuit-breaker configuration.
type StaticConfig struct {
	// The error returned by the circuit-breaker when its tripped. If not
	// specified, the circuit-breaker will use transport.ErrServiceUnavailable.
	ClosedError error

	// A slice of errors that can cause the circuit-breaker to trip. When
	// trying to match an error to this list, the circuit-breaker will first
	// try an equality test, followed by a case-insensitive sub-string match
	// of the error messages (e.g. strings.Contains). If not defined, the
	// circuit-breaker implementation will use DefaultTripErrors.
	TripErrors []error

	// The number of TripErrors that must be encountered for the circuit-breaker
	// to switch from the open to the closed state.
	TripThreshold int

	// The amount of time the circuit-breaker should stay in the Closed state
	// before transitioning to the Half-Open state. If not defined the
	// circuit-breaker implementation will use DefaultCoolOffPeriod.
	CoolOffPeriod time.Duration

	// While in Half-Open state, ResetThreshold defines the number of successful
	// requests before the circuit breaker transitions to the Open state.
	ResetThreshold int

	// If defined, when the circuit-breaker transitions to a new state it will
	// send its new state to this channel. The circuit-breaker uses non-blocking
	// writes to this channel; if no listener is ready to receive the new state
	// value, it will be dropped to the floor.
	StateChangeChan chan<- State
}

// GetClosedError returns the error message that is used to abort requests when
// the circuit-breaker is closed.
func (c *StaticConfig) GetClosedError() error {
	return c.ClosedError
}

// GetTripErrors returns a slice of errors that can cause the circuit-breaker to
// trip.
func (c *StaticConfig) GetTripErrors() []error {
	return c.TripErrors
}

// GetTripThreshold returns the number of errors after which the circuit-breaker
// will transition from opened to closed state.
func (c *StaticConfig) GetTripThreshold() *flag.Uint32 {
	f := flag.NewUint32(nil, "")
	f.Set(uint32(c.TripThreshold))
	return f
}

// GetCoolOffPeriod returns the number of nanoseconds that must elapse before the
// circuit-breaker transitions from closed state to half-open state.
func (c *StaticConfig) GetCoolOffPeriod() *flag.Int64 {
	f := flag.NewInt64(nil, "")
	f.Set(c.CoolOffPeriod.Nanoseconds())
	return f
}

// GetResetThreshold returns the number of successful requests before the
// circuit-breaker transitions from half-open to opened state.
func (c *StaticConfig) GetResetThreshold() *flag.Uint32 {
	f := flag.NewUint32(nil, "")
	f.Set(uint32(c.ResetThreshold))
	return f
}

// GetStateChangeChan returns a channel where the circuit-breaker will publish
// its new state whenever its state changes.
func (c *StaticConfig) GetStateChangeChan() chan<- State {
	return c.StateChangeChan
}

// DynamicConfig defines a circuit-breaker configuration which is synced to a configuration store.
type DynamicConfig struct {
	// The config store to use. If undefined it defaults to the global shared store.
	store *store.Store

	// The error returned by the circuit-breaker when its tripped. If not
	// specified, the circuit-breaker will use transport.ErrServiceUnavailable.
	ClosedError error

	// A slice of errors that can cause the circuit-breaker to trip. When
	// trying to match an error to this list, the circuit-breaker will first
	// try an equality test, followed by a case-insensitive sub-string match
	// of the error messages (e.g. strings.Contains). If not defined, the
	// circuit-breaker implementation will use DefaultTripErrors.
	TripErrors []error

	// If defined, when the circuit-breaker transitions to a new state it will
	// send its new state to this channel. The circuit-breaker uses non-blocking
	// writes to this channel; if no listener is ready to receive the new state
	// value, it will be dropped to the floor.
	StateChangeChan chan<- State

	// The configuration store prefix for fetching the circuit-breaker
	// configuration options. This prefix is prepended to the following keys:
	//  - trip_threshold
	//  - cool_off_period
	//  - reset_threshold
	ConfigPath string
}

// GetClosedError returns the error message that is used to abort requests when
// the circuit-breaker is closed.
func (c *DynamicConfig) GetClosedError() error {
	return c.ClosedError
}

// GetTripErrors returns a slice of errors that can cause the circuit-breaker to
// trip.
func (c *DynamicConfig) GetTripErrors() []error {
	return c.TripErrors
}

// GetTripThreshold returns the number of errors after which the circuit-breaker
// will transition from opened to closed state.
func (c *DynamicConfig) GetTripThreshold() *flag.Uint32 {
	return flag.NewUint32(c.store, c.configPath("trip_threshold"))
}

// GetCoolOffPeriod returns the number of nanoseconds that must elapse before the
// circuit-breaker transitions from closed state to half-open state.
func (c *DynamicConfig) GetCoolOffPeriod() *flag.Int64 {
	return flag.NewInt64(c.store, c.configPath("cool_off_period"))
}

// GetResetThreshold returns the number of successful requests before the
// circuit-breaker transitions from half-open to opened state.
func (c *DynamicConfig) GetResetThreshold() *flag.Uint32 {
	return flag.NewUint32(c.store, c.configPath("reset_threshold"))
}

// GetStateChangeChan returns a channel where the circuit-breaker will publish
// its new state whenever its state changes.
func (c *DynamicConfig) GetStateChangeChan() chan<- State {
	return c.StateChangeChan
}

// getStore returns the config store instance to use for fetching the dynaimc configuration.
func (c *DynamicConfig) getStore() *store.Store {
	if c.store == nil {
		return &config.Store
	}

	return c.store
}

// configPath returns a config path for the given key by concatenating the
// ConfigPath field value with key.
func (c *DynamicConfig) configPath(key string) string {
	return strings.TrimSuffix(c.ConfigPath, "/") + "/" + key
}
