# PostgreSQL Store

PostgreSQL implementation of the workflow engine store interface.

## Features

- Full implementation of the `store.Store` interface
- Support for binary data storage
- Automatic table initialization
- Connection pooling via `database/sql`
- Transaction support through PostgreSQL ACID properties
- Configurable connection parameters

## Installation

```bash
go get github.com/lib/pq
```

## Usage

### Basic Usage

```go
package main

import (
    "context"
    "log"

    "github.com/warriorguo/workflow"
    "github.com/warriorguo/workflow/store/postgres"
    "github.com/warriorguo/workflow/types"
)

func main() {
    // Create PostgreSQL store with default config
    config := postgres.DefaultConfig()
    config.Host = "localhost"
    config.Port = 5432
    config.User = "postgres"
    config.Password = "your-password"
    config.Database = "workflow"

    store, err := postgres.NewPostgresStore(config)
    if err != nil {
        log.Fatal(err)
    }
    defer store.(*postgres.pgStore).Close()

    // Create workflow engine with PostgreSQL store
    flowEngine := workflow.NewFlowEngineWithStore(store)

    // Use the engine...
}
```

### Using DSN String

```go
dsn := "host=localhost port=5432 user=postgres password=secret dbname=workflow sslmode=disable"
config, err := postgres.ParseDSN(dsn)
if err != nil {
    log.Fatal(err)
}

store, err := postgres.NewPostgresStore(config)
if err != nil {
    log.Fatal(err)
}
```

### Using Environment Variables

```go
// The test suite supports these environment variables:
// - POSTGRES_HOST
// - POSTGRES_PORT
// - POSTGRES_USER
// - POSTGRES_PASSWORD
// - POSTGRES_DB

config := postgres.DefaultConfig()
if host := os.Getenv("POSTGRES_HOST"); host != "" {
    config.Host = host
}
// ... set other fields from env vars

store, err := postgres.NewPostgresStore(config)
```

### Using Existing Database Connection

```go
import "database/sql"

db, err := sql.Open("postgres", "your-connection-string")
if err != nil {
    log.Fatal(err)
}

store, err := postgres.NewPostgresStoreWithDB(db)
if err != nil {
    log.Fatal(err)
}
```

## Configuration

### Config Structure

```go
type Config struct {
    Host     string  // Database host (default: "localhost")
    Port     int     // Database port (default: 5432)
    User     string  // Database user (default: "postgres")
    Password string  // Database password (default: "postgres")
    Database string  // Database name (default: "workflow")
    SSLMode  string  // SSL mode: disable, require, verify-ca, verify-full (default: "disable")
}
```

### SSL Modes

- `disable`: No SSL
- `require`: Use SSL (skip verification)
- `verify-ca`: Use SSL and verify the server certificate
- `verify-full`: Use SSL and verify the server certificate and hostname

## Database Schema

The PostgreSQL store automatically creates the following table:

```sql
CREATE TABLE IF NOT EXISTS workflow_store (
    prefix VARCHAR(255) NOT NULL,
    key VARCHAR(255) NOT NULL,
    value BYTEA,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (prefix, key)
);

CREATE INDEX IF NOT EXISTS idx_workflow_store_prefix ON workflow_store(prefix);
```

## Running Tests

To run the PostgreSQL store tests, you need a running PostgreSQL instance:

```bash
# Start PostgreSQL using Docker
docker run --name postgres-workflow \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=workflow \
  -p 5432:5432 \
  -d postgres:15

# Run tests
go test ./store/postgres/

# Or with custom PostgreSQL connection
POSTGRES_HOST=localhost \
POSTGRES_PORT=5432 \
POSTGRES_USER=postgres \
POSTGRES_PASSWORD=postgres \
POSTGRES_DB=workflow \
go test ./store/postgres/
```

If PostgreSQL is not available, the tests will be skipped automatically.

## Production Considerations

1. **Connection Pooling**: The store uses `database/sql` which provides built-in connection pooling. You can configure it:

```go
db, _ := sql.Open("postgres", dsn)
db.SetMaxOpenConns(25)
db.SetMaxIdleConns(5)
db.SetConnMaxLifetime(5 * time.Minute)

store, _ := postgres.NewPostgresStoreWithDB(db)
```

2. **SSL/TLS**: For production, use `sslmode=require` or higher:

```go
config.SSLMode = "verify-full"
```

3. **Backup**: Regularly backup your PostgreSQL database:

```bash
pg_dump -U postgres workflow > workflow_backup.sql
```

4. **Monitoring**: Monitor the `workflow_store` table size and indexes.

5. **Cleanup**: Implement cleanup strategies for old workflow data to prevent table bloat.

## Performance Tips

- The table uses a composite primary key on `(prefix, key)` for efficient lookups
- An additional index on `prefix` accelerates `List()` operations
- Use prepared statements for bulk operations
- Consider partitioning for very large datasets

## Error Handling

All errors are wrapped using `github.com/juju/errors` for better error tracing:

```go
store, err := postgres.NewPostgresStore(config)
if err != nil {
    log.Printf("Error: %+v", err)  // Print with stack trace
}
```

## Thread Safety

The PostgreSQL store is thread-safe. All operations are atomic at the database level.
