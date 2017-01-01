// Package provider defines a built-in configuration providers that are
// automatically registered by usrv.
package provider

import (
	"os"
	"regexp"
	"strings"
)

var (
	invalidCharRegex = regexp.MustCompile(`[\s-\/]`)
)

// EnvVars implements a configuration provider that fetches configuration values
// from the environment variables associated with the currently running process.
type EnvVars struct{}

// NewEnvVars creates a new EnvVars provider instance.
func NewEnvVars() *EnvVars {
	return &EnvVars{}
}

// Get returns a map containing configuration values associated with a particular path.
//
// To match the standard envvar form, the path is first normalized by converting
// it to uppercase and then replacing any path separators with an underscore while
// also trimming any leading separators.
//
// The provider then scans the process envvars and selects the ones whose names
// begin with the normalized path. The configuration map keys are built by stripping
// the path prefix from any matched envvars, lowercasing the remaining part of the
// envvar name and replacing any underscores with the path separator.
//
// For example, given the path "/redis/MASTER" the following envvars
// would be matched as configuration options:
//
//  - REDIS_MASTER_SERVICE_HOST=10.0.0.11
//  - REDIS_MASTER_PORT_6379_TCP=tcp://10.0.0.11:6379
//
// The above options would yield the following configuration map:
//  {
//   "service/host": "10.0.0.11",
//   "port/6379/tcp": "tcp://10.0.0.11:6379",
//  }
func (p *EnvVars) Get(path string) map[string]string {
	cfg := make(map[string]string, 0)

	prefix := strings.Trim(strings.ToUpper(invalidCharRegex.ReplaceAllString(path, "_")), "_") + "_"
	for _, envvar := range os.Environ() {
		tokens := strings.SplitN(envvar, "=", 2)
		if !strings.HasPrefix(tokens[0], prefix) {
			continue
		}

		normalizedName := strings.Replace(strings.ToLower(strings.TrimPrefix(tokens[0], prefix)), "_", "/", -1)
		cfg[normalizedName] = tokens[1]
	}

	return cfg
}

// Watch installs a monitor for a particular path and invokes the supplied value
// changer when its value changes. The method returns a function that should be
// used to terminate the watch.
//
// The
func (p *EnvVars) Watch(path string, valueSetter func(string, map[string]string)) func() {
	// Assume that envvars never change and treat the call as a NOOP
	return func() {}
}
