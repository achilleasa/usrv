// Package weightedrouting provides a middleware that sets each outgoing
// request's version by sampling the random number generator and then
// consulting is routing table that assigns a weight to each version.
//
// The middleware obtains its configuration from the global configuration
// store and its meant to be used in conjunction with a dynamic configuration
// provider.
//
// Potential use-cases for this middleware include blue-green deployments
// (http://martinfowler.com/bliki/BlueGreenDeployment.html) and canary releases
// (http://martinfowler.com/bliki/CanaryRelease.html) where a new service version
// is rolled out to production and a small amount of traffic is routed to it so
// it can be tested.
package weightedrouting

import (
	"context"
	"math/rand"
	"runtime"
	"strconv"
	"sync"

	"github.com/achilleasa/usrv/client"
	"github.com/achilleasa/usrv/config"
	"github.com/achilleasa/usrv/config/flag"
	"github.com/achilleasa/usrv/transport"
)

// Values overwritten by tests
var (
	setFinalizer = runtime.SetFinalizer
	randFloat32  = rand.Float32
	cfgStore     = &config.Store
)

// Factory generates a weighted-routing middleware factory that returns new
// weighted-router instances. Each instance obtains its weight configuration
// by monitoring keys under the namespace "weighted_router/$serviceName". Each
// key under this namespace points to a version name and its value should contain
// the weight assigned to that version name (0 to 1.0 range). All weights should
// sum to 1.0.
//
// For example, given a service called "foo" for which we want to route 30% of
// the traffic to "v0" and 70% of the traffic to "v1", the following configuration
// keys need to be defined:
//  - weighted_router/foo/v0 -> "0.3"
//  - weighted_router/foo/v1 -> "0.7"
func Factory() client.MiddlewareFactory {
	return func(serviceName string) client.Middleware {
		return newRouter(serviceName)
	}
}

// route combines a route version and a weight for selecting that route.
type route struct {
	version string
	weight  float32
}

// router is a client middleware that sets the requested service version
// for outgoing messages by consulting a set of weights obtained via a dynamic
// configuration flag.
type router struct {
	// A RW mutex used for synchronizing access to the routing table.
	mutex sync.RWMutex

	// The list of versioned routes and their selection probabilites.
	routes []route

	// A flag providing the weight configurations.
	weightCfg *flag.Map

	// A channel to signal the config monitor to exit.
	doneChan chan struct{}
}

// newRouter creates a weighted router instance that fetches its weights
// using the following configuration key: "weighted_router/$serviceName/".
func newRouter(serviceName string) *router {
	wr := &router{
		weightCfg: flag.NewMap(cfgStore, "weighted_router/"+serviceName),
		doneChan:  make(chan struct{}, 0),
	}

	// fetch initial weights
	wr.updateWeights()
	wr.spawnChangeMonitor()
	setFinalizer(wr, func(wr *router) { close(wr.doneChan) })

	return wr
}

// spawnChangeMonitor starts a worker that istens for configuration weight changes
// and updates the middleware's routing table.
func (wr *router) spawnChangeMonitor() {
	go func() {
		for {
			select {
			case <-wr.doneChan:
				wr.weightCfg.CancelDynamicUpdates()
				return
			case <-wr.weightCfg.ChangeChan():
				wr.updateWeights()
			}
		}
	}()
}

// updateWeights fetches the latest routing weights configuration and updates
// the internal routes table.
func (wr *router) updateWeights() {
	wr.mutex.Lock()
	cfg := wr.weightCfg.Get()

	routes := make([]route, 0)
	for version, weightStr := range cfg {
		weight, err := strconv.ParseFloat(weightStr, 32)
		if err != nil {
			continue
		}
		routes = append(routes, route{version, float32(weight)})
	}

	wr.routes = routes
	wr.mutex.Unlock()
}

// Pre implements a pre-hook as part of the client Middleware interface.
func (wr *router) Pre(ctx context.Context, req transport.Message) (context.Context, error) {
	prob := randFloat32()

	wr.mutex.RLock()

	var probIntegral float32
	for _, route := range wr.routes {
		probIntegral += route.weight

		if prob < probIntegral {
			req.SetReceiverVersion(route.version)
			break
		}
	}

	wr.mutex.RUnlock()
	return ctx, nil
}

// Post implements a post-hook as part of the client Middleware interface.
func (wr *router) Post(_ context.Context, _, _ transport.ImmutableMessage) {
}
