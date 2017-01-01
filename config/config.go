package config

import (
	"github.com/achilleasa/usrv/config/provider"
	"github.com/achilleasa/usrv/config/store"
)

var (
	// Store is a global configuration store instance that is used to configure
	// the various usrv components.
	Store store.Store
)

// SetDefaults updates the global store instance with the default values for a
// particular configuration path.
func SetDefaults(path string, cfg map[string]string) error {
	_, err := Store.SetKeys(0, path, cfg)
	return err
}

func init() {
	// Register built-in providers
	Store.RegisterValueProvider(provider.NewEnvVars())
}
