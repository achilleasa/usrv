// Package config exposes a global configuration store instance and provides
// helpers for setting default configuration values.
//
// Including this package will automatically register the built-in configuration
// providers. For more information please see the provider sub-package.
package config

import (
	"github.com/achilleasa/usrv/config/provider"
	"github.com/achilleasa/usrv/config/store"
)

// Store is a global configuration store instance that is used to configure
// the various usrv components. Including the config package automatically
// registers the built-in configuration providers with this store.
var Store store.Store

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
