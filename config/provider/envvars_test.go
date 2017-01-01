package provider

import (
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestEnvVarsProviderWatch(t *testing.T) {
	p := NewEnvVars()
	unsubFn := p.Watch("GOPATH", func(_ string, _ map[string]string) {
		t.Fatalf("unexpected call to watch value setter")
	})
	unsubFn()
}

func TestEnvVarsProviderForUnknownPath(t *testing.T) {
	p := NewEnvVars()
	values := p.Get("/unknown")
	if len(values) != 0 {
		t.Fatalf("expected to get back an empty map; got %v", values)
	}
}

func TestEnvVarsProviderForKnownPath(t *testing.T) {
	expValues := map[string]string{
		"service/host": "foo.service",
		"service/port": "7337",
	}

	for key, val := range expValues {
		envKey := strings.ToUpper(strings.Replace(key, "/", "_", -1))
		os.Setenv("FOO__H_A_"+envKey, val)
	}

	// Cleanup env when the test ends
	defer func() {
		for key := range expValues {
			envKey := strings.ToUpper(strings.Replace(key, "/", "_", -1))
			os.Unsetenv("FOO__H_A_" + envKey)
		}
	}()

	p := NewEnvVars()
	values := p.Get("FOO__H_A")
	if !reflect.DeepEqual(values, expValues) {
		t.Fatalf("expected to get values:\n%v\n\ngot:\n%v", expValues, values)
	}

	expValues2 := map[string]string{
		"host": "foo.service",
		"port": "7337",
	}
	values = p.Get("FOO__H_A/service/")
	if !reflect.DeepEqual(values, expValues2) {
		t.Fatalf("expected to get values:\n%v\n\ngot:\n%v", expValues2, values)
	}
}
