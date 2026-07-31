package config

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/providers/structs"
	"github.com/knadh/koanf/v2"

	mapstructure "github.com/go-viper/mapstructure/v2"
)

// EnvPrefix is the prefix for environment variable overrides. Nested keys are
// joined with "__": e.g. TASKFLOWW_BROKER__PREFETCH overrides broker.prefetch.
const EnvPrefix = "TASKFLOWW_"

// envRef matches ${VAR} and ${VAR:-default} for in-file interpolation.
var envRef = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}`)

// Load reads, interpolates, merges, and validates the config at path.
//
// Precedence (low → high): built-in defaults → YAML file → environment vars.
// YAML string values may reference the environment via ${VAR:-default}.
func Load(path string) (*Config, error) {
	k := koanf.New(".")

	// 1. defaults
	if err := k.Load(structs.Provider(defaults(), "koanf"), nil); err != nil {
		return nil, fmt.Errorf("loading defaults: %w", err)
	}

	// 2. YAML file (with ${VAR:-default} interpolation applied before parsing)
	if err := k.Load(file.Provider(path), &envInterpolatingYAML{}); err != nil {
		return nil, fmt.Errorf("loading config file %q: %w", path, err)
	}

	// 3. environment overrides: TASKFLOWW_A__B -> a.b
	envProvider := env.Provider(EnvPrefix, ".", func(s string) string {
		return strings.ReplaceAll(strings.ToLower(strings.TrimPrefix(s, EnvPrefix)), "__", ".")
	})
	if err := k.Load(envProvider, nil); err != nil {
		return nil, fmt.Errorf("loading environment overrides: %w", err)
	}

	// 4. unmarshal (weakly typed so "2"/"2.0"/"true" from YAML/env coerce cleanly)
	var c Config
	if err := k.UnmarshalWithConf("", &c, koanf.UnmarshalConf{
		Tag: "koanf",
		DecoderConfig: &mapstructure.DecoderConfig{
			Result:           &c,
			WeaklyTypedInput: true,
			ErrorUnused:      false,
		},
	}); err != nil {
		return nil, fmt.Errorf("decoding config: %w", err)
	}

	// 5. validate (fail fast, list every problem)
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

// envInterpolatingYAML is a koanf parser that expands ${VAR:-default} before
// delegating to the standard YAML parser. This keeps interpolation transparent
// to the rest of the loader.
type envInterpolatingYAML struct{}

func (envInterpolatingYAML) Unmarshal(b []byte) (map[string]interface{}, error) {
	return yaml.Parser().Unmarshal(expandEnv(b))
}

func (envInterpolatingYAML) Marshal(m map[string]interface{}) ([]byte, error) {
	return yaml.Parser().Marshal(m)
}

// expandEnv replaces ${VAR} / ${VAR:-default} using the process environment.
// An unset (or empty) variable falls back to its default, or "" if none.
func expandEnv(b []byte) []byte {
	return envRef.ReplaceAllFunc(b, func(match []byte) []byte {
		m := envRef.FindSubmatch(match)
		if v, ok := os.LookupEnv(string(m[1])); ok && v != "" {
			return []byte(v)
		}
		return m[2] // default (possibly empty)
	})
}

// RedactURI masks the password in a URI-style DSN for safe logging, preserving
// the rest of the string exactly. DSNs without "://" or without a password
// component are returned unchanged.
func RedactURI(raw string) string {
	if raw == "" {
		return ""
	}
	sep := strings.Index(raw, "://")
	if sep < 0 {
		return raw
	}
	rest := raw[sep+3:]
	at := strings.Index(rest, "@")
	if at < 0 {
		return raw // no userinfo
	}
	userinfo := rest[:at]
	colon := strings.Index(userinfo, ":")
	if colon < 0 {
		return raw // username only, no password to hide
	}
	return raw[:sep+3] + userinfo[:colon] + ":****" + rest[at:]
}
