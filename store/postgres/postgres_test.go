package postgres

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/warriorguo/workflow/store"
)

// getTestConfig returns a test configuration
// You can set environment variables to override defaults:
// - POSTGRES_HOST
// - POSTGRES_PORT
// - POSTGRES_USER
// - POSTGRES_PASSWORD
// - POSTGRES_DB
func getTestConfig() *Config {
	config := DefaultConfig()

	if host := os.Getenv("POSTGRES_HOST"); host != "" {
		config.Host = host
	}
	if port := os.Getenv("POSTGRES_PORT"); port != "" {
		fmt.Sscanf(port, "%d", &config.Port)
	}
	if user := os.Getenv("POSTGRES_USER"); user != "" {
		config.User = user
	}
	if password := os.Getenv("POSTGRES_PASSWORD"); password != "" {
		config.Password = password
	}
	if db := os.Getenv("POSTGRES_DB"); db != "" {
		config.Database = db
	}

	return config
}

// skipIfNoPostgres skips the test if PostgreSQL is not available
func skipIfNoPostgres(t *testing.T) store.Store {
	config := getTestConfig()
	s, err := NewPostgresStore(config)
	if err != nil {
		t.Skipf("PostgreSQL not available: %v", err)
		return nil
	}
	return s
}

func TestPostgresStore_SetAndGet(t *testing.T) {
	s := skipIfNoPostgres(t)
	if s == nil {
		return
	}
	if closer, ok := s.(interface{ Close() error }); ok {
		defer closer.Close()
	}

	ctx := context.Background()

	// Test Set and Get
	err := s.Set(ctx, "/test/", "key1", []byte("value1"))
	assert.Nil(t, err)

	value, err := s.Get(ctx, "/test/", "key1")
	assert.Nil(t, err)
	assert.Equal(t, []byte("value1"), value)

	// Test Get non-existent key
	value, err = s.Get(ctx, "/test/", "non-existent")
	assert.Nil(t, err)
	assert.Nil(t, value)

	// Cleanup
	err = s.Remove(ctx, "/test/", "key1")
	assert.Nil(t, err)
}

func TestPostgresStore_Update(t *testing.T) {
	s := skipIfNoPostgres(t)
	if s == nil {
		return
	}
	if closer, ok := s.(interface{ Close() error }); ok {
		defer closer.Close()
	}

	ctx := context.Background()

	// Set initial value
	err := s.Set(ctx, "/test/", "key1", []byte("value1"))
	assert.Nil(t, err)

	// Update value
	err = s.Set(ctx, "/test/", "key1", []byte("value2"))
	assert.Nil(t, err)

	// Verify update
	value, err := s.Get(ctx, "/test/", "key1")
	assert.Nil(t, err)
	assert.Equal(t, []byte("value2"), value)

	// Cleanup
	err = s.Remove(ctx, "/test/", "key1")
	assert.Nil(t, err)
}

func TestPostgresStore_Remove(t *testing.T) {
	s := skipIfNoPostgres(t)
	if s == nil {
		return
	}
	if closer, ok := s.(interface{ Close() error }); ok {
		defer closer.Close()
	}

	ctx := context.Background()

	// Set value
	err := s.Set(ctx, "/test/", "key1", []byte("value1"))
	assert.Nil(t, err)

	// Remove value
	err = s.Remove(ctx, "/test/", "key1")
	assert.Nil(t, err)

	// Verify removal
	value, err := s.Get(ctx, "/test/", "key1")
	assert.Nil(t, err)
	assert.Nil(t, value)

	// Remove non-existent key should not error
	err = s.Remove(ctx, "/test/", "non-existent")
	assert.Nil(t, err)
}

func TestPostgresStore_List(t *testing.T) {
	s := skipIfNoPostgres(t)
	if s == nil {
		return
	}
	if closer, ok := s.(interface{ Close() error }); ok {
		defer closer.Close()
	}

	ctx := context.Background()

	// Set multiple values with same prefix
	err := s.Set(ctx, "/test/", "key1", []byte("value1"))
	assert.Nil(t, err)
	err = s.Set(ctx, "/test/", "key2", []byte("value2"))
	assert.Nil(t, err)
	err = s.Set(ctx, "/test/", "key3", []byte("value3"))
	assert.Nil(t, err)

	// Set value with different prefix
	err = s.Set(ctx, "/other/", "key1", []byte("other1"))
	assert.Nil(t, err)

	// List keys with /test/ prefix
	keys := make([]string, 0)
	err = s.List(ctx, "/test/", func(key string) bool {
		keys = append(keys, key)
		return true
	})
	assert.Nil(t, err)
	assert.Equal(t, 3, len(keys))
	assert.Contains(t, keys, "key1")
	assert.Contains(t, keys, "key2")
	assert.Contains(t, keys, "key3")

	// Test early termination
	count := 0
	err = s.List(ctx, "/test/", func(key string) bool {
		count++
		return count < 2 // Stop after 2 keys
	})
	assert.Nil(t, err)
	assert.Equal(t, 2, count)

	// Cleanup
	s.Remove(ctx, "/test/", "key1")
	s.Remove(ctx, "/test/", "key2")
	s.Remove(ctx, "/test/", "key3")
	s.Remove(ctx, "/other/", "key1")
}

func TestPostgresStore_ListEmpty(t *testing.T) {
	s := skipIfNoPostgres(t)
	if s == nil {
		return
	}
	if closer, ok := s.(interface{ Close() error }); ok {
		defer closer.Close()
	}

	ctx := context.Background()

	// List keys with non-existent prefix
	keys := make([]string, 0)
	err := s.List(ctx, "/non-existent/", func(key string) bool {
		keys = append(keys, key)
		return true
	})
	assert.Nil(t, err)
	assert.Equal(t, 0, len(keys))
}

func TestConfig_Validate(t *testing.T) {
	// Valid config
	config := DefaultConfig()
	err := config.Validate()
	assert.Nil(t, err)

	// Invalid host
	config = DefaultConfig()
	config.Host = ""
	err = config.Validate()
	assert.NotNil(t, err)

	// Invalid port
	config = DefaultConfig()
	config.Port = 0
	err = config.Validate()
	assert.NotNil(t, err)

	// Invalid user
	config = DefaultConfig()
	config.User = ""
	err = config.Validate()
	assert.NotNil(t, err)

	// Invalid database
	config = DefaultConfig()
	config.Database = ""
	err = config.Validate()
	assert.NotNil(t, err)

	// Invalid SSLMode
	config = DefaultConfig()
	config.SSLMode = "invalid"
	err = config.Validate()
	assert.NotNil(t, err)

	// Empty SSLMode should default to disable
	config = DefaultConfig()
	config.SSLMode = ""
	err = config.Validate()
	assert.Nil(t, err)
	assert.Equal(t, "disable", config.SSLMode)
}

func TestConfig_DSN(t *testing.T) {
	config := &Config{
		Host:     "localhost",
		Port:     5432,
		User:     "testuser",
		Password: "testpass",
		Database: "testdb",
		SSLMode:  "disable",
	}

	dsn := config.DSN()
	expected := "host=localhost port=5432 user=testuser password=testpass dbname=testdb sslmode=disable"
	assert.Equal(t, expected, dsn)
}

func TestParseDSN(t *testing.T) {
	dsn := "host=localhost port=5432 user=testuser password=testpass dbname=testdb sslmode=require"
	config, err := ParseDSN(dsn)
	assert.Nil(t, err)
	assert.Equal(t, "localhost", config.Host)
	assert.Equal(t, 5432, config.Port)
	assert.Equal(t, "testuser", config.User)
	assert.Equal(t, "testpass", config.Password)
	assert.Equal(t, "testdb", config.Database)
	assert.Equal(t, "require", config.SSLMode)
}

func TestPostgresStore_BinaryData(t *testing.T) {
	s := skipIfNoPostgres(t)
	if s == nil {
		return
	}
	if closer, ok := s.(interface{ Close() error }); ok {
		defer closer.Close()
	}

	ctx := context.Background()

	// Test with binary data
	binaryData := []byte{0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD}
	err := s.Set(ctx, "/test/", "binary", binaryData)
	assert.Nil(t, err)

	value, err := s.Get(ctx, "/test/", "binary")
	assert.Nil(t, err)
	assert.Equal(t, binaryData, value)

	// Cleanup
	err = s.Remove(ctx, "/test/", "binary")
	assert.Nil(t, err)
}
