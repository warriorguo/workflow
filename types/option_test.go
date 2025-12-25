package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWithPostgresConfig(t *testing.T) {
	config := &PostgresConfig{
		Host:     "dbhost",
		Port:     5433,
		User:     "user",
		Password: "pass",
		Database: "db",
		SSLMode:  "require",
	}

	opts := NewFlowOptions()
	opt := WithPostgresConfig(config)
	opt(opts)

	assert.NotNil(t, opts.PostgresConfig)
	assert.Equal(t, "dbhost", opts.PostgresConfig.Host)
	assert.Equal(t, 5433, opts.PostgresConfig.Port)
	assert.Equal(t, "user", opts.PostgresConfig.User)
	assert.Equal(t, "pass", opts.PostgresConfig.Password)
	assert.Equal(t, "db", opts.PostgresConfig.Database)
	assert.Equal(t, "require", opts.PostgresConfig.SSLMode)
}

func TestFlowOptions_PostgresConfigPrecedence(t *testing.T) {
	// Test that PostgresConfig should take precedence over MemStore
	opts := NewFlowOptions()

	// Set both MemStore and PostgresConfig
	EnableMemStore()(opts)
	WithPostgresConfig(&PostgresConfig{
		Host:     "localhost",
		Port:     5432,
		User:     "user",
		Password: "pass",
		Database: "db",
		SSLMode:  "disable",
	})(opts)

	assert.True(t, opts.MemStore)
	assert.NotNil(t, opts.PostgresConfig)

	// The actual precedence is handled in workflow.NewFlowEngine
	// Here we just verify both can be set
}

func TestMultipleOptions(t *testing.T) {
	opts := NewFlowOptions()

	// Apply multiple options
	WithPostgresConfig(&PostgresConfig{
		Host:     "localhost",
		Port:     5432,
		User:     "user",
		Password: "pass",
		Database: "db",
		SSLMode:  "disable",
	})(opts)
	SetMaxNodeConcurrency(50)(opts)
	DisableAutoStart()(opts)

	assert.NotNil(t, opts.PostgresConfig)
	assert.Equal(t, 50, opts.MaxNodeConcurrency)
	assert.False(t, opts.AutoStart)
}
