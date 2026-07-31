package config

import (
	"os"
	"path/filepath"
	"testing"
)

// minimalValidYAML is a complete, valid config used as a baseline.
const minimalValidYAML = `
app:
  name: taskfloww
  environment: development
logging:
  level: info
  format: json
database:
  uri: ${TEST_DB_URI:-postgres://u:p@localhost:5432/db?sslmode=disable}
broker:
  uri: amqp://u:p@localhost:5672/
queues:
  default_exchange: taskfloww.direct
  dead_letter_exchange: taskfloww.dlx
  definitions:
    - name: tasks.default
      routing_key: priority.default
      max_priority: 10
  dead_letter:
    name: tasks.dlq
    routing_key: dead
tasks:
  - name: send_email
    handler: myapp.tasks:send_email
    queue: tasks.default
    max_retries: 3
`

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

func TestLoadDefaultsAndFile(t *testing.T) {
	path := writeConfig(t, minimalValidYAML)
	c, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// value from file
	if got := c.Queues.Definitions[0].Name; got != "tasks.default" {
		t.Errorf("queue name = %q", got)
	}
	// defaults filled in (not present in file)
	if c.Broker.Prefetch != 32 {
		t.Errorf("expected default prefetch 32, got %d", c.Broker.Prefetch)
	}
	if c.Heartbeat.LeaseTTLSeconds != 60 {
		t.Errorf("expected default lease 60, got %d", c.Heartbeat.LeaseTTLSeconds)
	}
	if c.Server.HTTPAddr != ":8080" {
		t.Errorf("expected default addr :8080, got %q", c.Server.HTTPAddr)
	}
	// accessor + inheritance
	tm, ok := c.TaskByName("send_email")
	if !ok || tm.EffectiveMaxRetries(c.Retry.MaxRetries) != 3 {
		t.Errorf("send_email effective max retries wrong: %+v", tm)
	}
}

func TestInterpolationDefaultAndOverride(t *testing.T) {
	// unset → uses the ${:-default}
	path := writeConfig(t, minimalValidYAML)
	c, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if c.Database.URI != "postgres://u:p@localhost:5432/db?sslmode=disable" {
		t.Errorf("interpolation default wrong: %q", c.Database.URI)
	}
	// set → uses the env value
	t.Setenv("TEST_DB_URI", "postgres://real:secret@db:5432/prod")
	c2, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if c2.Database.URI != "postgres://real:secret@db:5432/prod" {
		t.Errorf("interpolation override wrong: %q", c2.Database.URI)
	}
}

func TestEnvOverride(t *testing.T) {
	path := writeConfig(t, minimalValidYAML)
	t.Setenv("TASKFLOWW_BROKER__PREFETCH", "64")
	t.Setenv("TASKFLOWW_LOGGING__LEVEL", "debug")
	c, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if c.Broker.Prefetch != 64 {
		t.Errorf("env override prefetch = %d, want 64", c.Broker.Prefetch)
	}
	if c.Logging.Level != "debug" {
		t.Errorf("env override level = %q, want debug", c.Logging.Level)
	}
}

func TestValidationAggregatesProblems(t *testing.T) {
	bad := `
logging:
  level: verbose
  format: xml
database:
  uri: ""
broker:
  uri: amqp://localhost
queues:
  default_exchange: x
  dead_letter_exchange: y
  definitions:
    - name: q1
      routing_key: rk
  dead_letter:
    name: dlq
heartbeat:
  interval_seconds: 30
  lease_ttl_seconds: 10
  reaper_interval_seconds: 15
tasks:
  - name: t1
    handler: not-a-valid-handler
    queue: missing.queue
`
	path := writeConfig(t, bad)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected validation error")
	}
	ve, ok := err.(*ValidationError)
	if !ok {
		t.Fatalf("expected *ValidationError, got %T: %v", err, err)
	}
	// Spot-check that several distinct problems were caught at once.
	must := []string{
		"logging.level",
		"logging.format",
		"database.uri is required",
		"lease_ttl_seconds",  // 10 <= 30
		"handler",            // bad shape
		"does not match any", // unknown queue reference
	}
	joined := ve.Error()
	for _, sub := range must {
		if !contains(joined, sub) {
			t.Errorf("expected validation error to mention %q; got:\n%s", sub, joined)
		}
	}
}

func TestRedactURI(t *testing.T) {
	cases := map[string]string{
		"postgres://user:secret@host:5432/db": "postgres://user:****@host:5432/db",
		"amqp://guest:guest@localhost:5672/":  "amqp://guest:****@localhost:5672/",
		"postgres://host:5432/db":             "postgres://host:5432/db", // no creds
		"":                                    "",
	}
	for in, want := range cases {
		if got := RedactURI(in); got != want {
			t.Errorf("RedactURI(%q) = %q, want %q", in, got, want)
		}
	}
}

func contains(haystack, needle string) bool {
	return len(needle) == 0 || (len(haystack) >= len(needle) && indexOf(haystack, needle) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
