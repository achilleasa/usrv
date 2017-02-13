package weightedrouting

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/achilleasa/usrv/config"
	"github.com/achilleasa/usrv/config/store"
	"github.com/achilleasa/usrv/transport"
)

func TestRouter(t *testing.T) {
	defer func() {
		cfgStore = &config.Store
		randFloat32 = rand.Float32
	}()

	var randMutex sync.Mutex
	var randValues = []float32{0, 0.2999, 0.6999, 0.8, 0.2, 0.9, 0.9999}
	randFloat32 = func() float32 {
		randMutex.Lock()
		defer randMutex.Unlock()
		nextVal := randValues[0]
		randValues = randValues[1:]
		return nextVal
	}

	var s store.Store
	cfgStore = &s

	s.SetKeys(
		1,
		"weighted_router/foo",
		map[string]string{
			"v0": "0.3",
			"v1": "0.7",
		},
	)

	f := Factory()
	m := f("foo").(*router)
	ctx := context.Background()

	req := transport.MakeGenericMessage()
	defer req.Close()

	// Since map accesses in go are random we need to check the decoded
	// route order to select the expectations
	var expectedVersions []string
	switch m.routes[0].version {
	case "v0":
		expectedVersions = []string{"v0", "v0", "v1", "v1", "v0", "v1", "v1"}
	default:
		expectedVersions = []string{"v1", "v1", "v1", "v0", "v1", "v0", "v0"}
	}

	for specIndex, expVersion := range expectedVersions {
		req.SetReceiverVersion("")
		_, err := m.Pre(ctx, req)
		if err != nil {
			t.Fatal(err)
		}

		version := req.ReceiverVersion()
		if version != expVersion {
			t.Errorf("[spec %d] expected middleware to set receiver version %q; got %q", specIndex, expVersion, version)
		}
	}

	// Change the weights so that we always prefer version v0
	s.SetKeys(
		1,
		"weighted_router/foo",
		map[string]string{
			"v0": "1.0",
			"v1": "0",
			"v2": "not-a-float",
		},
	)

	<-time.After(500 * time.Millisecond)

	// Reset RND
	randValues = []float32{0, 0.2999, 0.6999, 0.8, 0.2, 0.9, 0.9999}

	expVersion := "v0"
	for specIndex := range expectedVersions {
		req.SetReceiverVersion("")
		_, err := m.Pre(ctx, req)
		if err != nil {
			t.Fatal(err)
		}

		m.Post(ctx, req, nil)

		version := req.ReceiverVersion()
		if version != expVersion {
			t.Errorf("[spec %d] expected middleware to set receiver version %q; got %q", specIndex, expVersion, version)
		}
	}
}

func TestRouterLocks(t *testing.T) {
	defer func() {
		cfgStore = &config.Store
		randFloat32 = rand.Float32
	}()

	var randMutex sync.Mutex
	var randValues = []float32{0, 0.2999, 0.6999, 0.8, 0.2, 0.9, 0.9999}
	randFloat32 = func() float32 {
		randMutex.Lock()
		defer randMutex.Unlock()
		nextVal := randValues[0]
		randValues = randValues[1:]
		return nextVal
	}

	var s store.Store
	cfgStore = &s

	s.SetKeys(
		1,
		"weighted_router/foo",
		map[string]string{
			"v0": "0.3",
			"v1": "0.7",
		},
	)

	f := Factory()
	m := f("foo").(*router)
	ctx := context.Background()

	versionChan := make(chan string, len(randValues))
	var wg sync.WaitGroup
	wg.Add(len(randValues))
	for _ = range randValues {
		go func() {
			req := transport.MakeGenericMessage()
			defer func() {
				req.Close()
				wg.Done()
			}()

			_, err := m.Pre(ctx, req)
			if err != nil {
				t.Fatal(err)
			}

			m.Post(ctx, req, nil)
			versionChan <- req.ReceiverVersion()
		}()
	}

	wg.Wait()
	close(versionChan)

	var expV0, expV1 int = 3, 4
	var v0, v1 int
	for v := range versionChan {
		switch v {
		case "v0":
			v0++
		case "v1":
			v1++
		}
	}

	if v0 != expV0 || v1 != expV1 {
		t.Fatalf("expected v0 and v1 counts to be %d, %d; got %d, %d", expV0, expV1, v0, v1)
	}

	// Reset RND
	randValues = []float32{0, 0.2999, 0.6999, 0.8, 0.2, 0.9, 0.9999}
	wg.Add(len(randValues))

	for _ = range randValues {
		go func() {
			req := transport.MakeGenericMessage()
			defer func() {
				req.Close()
				wg.Done()
			}()

			_, err := m.Pre(ctx, req)
			if err != nil {
				t.Fatal(err)
			}
			m.Post(ctx, req, nil)
		}()
	}

	// Change the weights so that we always prefer version v0
	s.SetKeys(
		1,
		"weighted_router/foo",
		map[string]string{
			"v0": "1.0",
			"v1": "0",
		},
	)

	wg.Wait()
}

func TestWorkerCleanup(t *testing.T) {
	origSetFinalizer := setFinalizer
	defer func() {
		setFinalizer = origSetFinalizer
	}()
	var finalizer func(*router)
	setFinalizer = func(_ interface{}, cb interface{}) {
		finalizer = cb.(func(*router))
	}

	var s store.Store
	cfgStore = &s

	s.SetKeys(
		1,
		"weighted_router/foo",
		map[string]string{
			"v1": "1.0",
		},
	)

	f := Factory()
	m := f("foo").(*router)
	ctx := context.Background()

	// Shutdown worker
	finalizer(m)
	<-time.After(100 * time.Millisecond)

	// Trigger store update; these changes will not be picked up by the middleware
	s.SetKeys(
		1,
		"weighted_router/foo",
		map[string]string{
			"v0": "1.0",
		},
	)
	<-time.After(500 * time.Millisecond)

	expVersion := "v1"
	req := transport.MakeGenericMessage()
	defer req.Close()
	_, err := m.Pre(ctx, req)
	if err != nil {
		t.Fatal(err)
	}

	version := req.ReceiverVersion()
	if version != expVersion {
		t.Error("expected middleware not to update its routing weights after its finalizer gets called")
	}
}
