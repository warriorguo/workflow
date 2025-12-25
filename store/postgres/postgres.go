package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/juju/errors"
	_ "github.com/lib/pq"
	"github.com/warriorguo/workflow/store"
)

var (
	_ store.Store = &pgStore{}
)

// Config holds PostgreSQL connection configuration
type Config struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
	SSLMode  string // disable, require, verify-ca, verify-full
}

// DefaultConfig returns a default configuration
func DefaultConfig() *Config {
	return &Config{
		Host:     "localhost",
		Port:     5432,
		User:     "postgres",
		Password: "postgres",
		Database: "workflow",
		SSLMode:  "disable",
	}
}

// pgStore implements Store interface using PostgreSQL
type pgStore struct {
	db *sql.DB
}

// NewPostgresStore creates a new PostgreSQL store with the given configuration
func NewPostgresStore(config *Config) (store.Store, error) {
	if config == nil {
		config = DefaultConfig()
	}

	connStr := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		config.Host, config.Port, config.User, config.Password, config.Database, config.SSLMode,
	)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, errors.Annotatef(err, "failed to open postgres connection")
	}

	// Test connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, errors.Annotatef(err, "failed to ping postgres")
	}

	s := &pgStore{db: db}

	// Initialize table
	if err := s.initTable(context.Background()); err != nil {
		db.Close()
		return nil, errors.Annotatef(err, "failed to initialize table")
	}

	return s, nil
}

// NewPostgresStoreWithDB creates a new PostgreSQL store with an existing database connection
func NewPostgresStoreWithDB(db *sql.DB) (store.Store, error) {
	if db == nil {
		return nil, errors.New("db cannot be nil")
	}

	s := &pgStore{db: db}

	// Initialize table
	if err := s.initTable(context.Background()); err != nil {
		return nil, errors.Annotatef(err, "failed to initialize table")
	}

	return s, nil
}

// initTable creates the workflow_store table if it doesn't exist
func (p *pgStore) initTable(ctx context.Context) error {
	query := `
		CREATE TABLE IF NOT EXISTS workflow_store (
			prefix VARCHAR(255) NOT NULL,
			key VARCHAR(255) NOT NULL,
			value BYTEA,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (prefix, key)
		);

		CREATE INDEX IF NOT EXISTS idx_workflow_store_prefix ON workflow_store(prefix);
	`

	_, err := p.db.ExecContext(ctx, query)
	if err != nil {
		return errors.Annotatef(err, "failed to create table")
	}

	return nil
}

// Get retrieves a value by prefix and key
func (p *pgStore) Get(ctx context.Context, prefix, key string) ([]byte, error) {
	query := `SELECT value FROM workflow_store WHERE prefix = $1 AND key = $2`

	var value []byte
	err := p.db.QueryRowContext(ctx, query, prefix, key).Scan(&value)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil // Return nil for non-existent keys
		}
		return nil, errors.Annotatef(err, "failed to get value for prefix=%s, key=%s", prefix, key)
	}

	return value, nil
}

// Set stores a value with the given prefix and key
func (p *pgStore) Set(ctx context.Context, prefix, key string, value []byte) error {
	query := `
		INSERT INTO workflow_store (prefix, key, value, updated_at)
		VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
		ON CONFLICT (prefix, key)
		DO UPDATE SET value = EXCLUDED.value, updated_at = CURRENT_TIMESTAMP
	`

	_, err := p.db.ExecContext(ctx, query, prefix, key, value)
	if err != nil {
		return errors.Annotatef(err, "failed to set value for prefix=%s, key=%s", prefix, key)
	}

	return nil
}

// Remove deletes a value by prefix and key
func (p *pgStore) Remove(ctx context.Context, prefix, key string) error {
	query := `DELETE FROM workflow_store WHERE prefix = $1 AND key = $2`

	_, err := p.db.ExecContext(ctx, query, prefix, key)
	if err != nil {
		return errors.Annotatef(err, "failed to remove value for prefix=%s, key=%s", prefix, key)
	}

	return nil
}

// List retrieves all keys with the given prefix and calls the iterator for each
func (p *pgStore) List(ctx context.Context, prefix string, iterator func(key string) bool) error {
	query := `SELECT key FROM workflow_store WHERE prefix = $1 ORDER BY key`

	rows, err := p.db.QueryContext(ctx, query, prefix)
	if err != nil {
		return errors.Annotatef(err, "failed to list keys for prefix=%s", prefix)
	}
	defer rows.Close()

	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return errors.Annotatef(err, "failed to scan key")
		}

		if !iterator(key) {
			break
		}
	}

	if err := rows.Err(); err != nil {
		return errors.Annotatef(err, "error iterating rows")
	}

	return nil
}

// Close closes the database connection
func (p *pgStore) Close() error {
	if p.db != nil {
		return p.db.Close()
	}
	return nil
}

// DSN builds a PostgreSQL connection string from Config
func (c *Config) DSN() string {
	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		c.Host, c.Port, c.User, c.Password, c.Database, c.SSLMode,
	)
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if c.Host == "" {
		return errors.New("host cannot be empty")
	}
	if c.Port <= 0 || c.Port > 65535 {
		return errors.New("port must be between 1 and 65535")
	}
	if c.User == "" {
		return errors.New("user cannot be empty")
	}
	if c.Database == "" {
		return errors.New("database cannot be empty")
	}
	if c.SSLMode == "" {
		c.SSLMode = "disable"
	}
	validSSLModes := map[string]bool{
		"disable":     true,
		"require":     true,
		"verify-ca":   true,
		"verify-full": true,
	}
	if !validSSLModes[c.SSLMode] {
		return errors.Errorf("invalid sslmode: %s", c.SSLMode)
	}
	return nil
}

// ParseDSN parses a PostgreSQL connection string into a Config
// Format: "host=localhost port=5432 user=postgres password=secret dbname=workflow sslmode=disable"
func ParseDSN(dsn string) (*Config, error) {
	config := DefaultConfig()

	parts := strings.Fields(dsn)
	for _, part := range parts {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			continue
		}

		key, value := kv[0], kv[1]
		switch key {
		case "host":
			config.Host = value
		case "port":
			var port int
			if _, err := fmt.Sscanf(value, "%d", &port); err == nil {
				config.Port = port
			}
		case "user":
			config.User = value
		case "password":
			config.Password = value
		case "dbname":
			config.Database = value
		case "sslmode":
			config.SSLMode = value
		}
	}

	return config, config.Validate()
}
