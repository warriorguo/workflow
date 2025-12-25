package workflow

import (
	"github.com/juju/errors"
	"github.com/warriorguo/workflow/runtime"
	"github.com/warriorguo/workflow/store"
	"github.com/warriorguo/workflow/store/mem"
	"github.com/warriorguo/workflow/store/postgres"
	"github.com/warriorguo/workflow/types"
)

// NewFlowEngine creates a new flow engine with the given options
func NewFlowEngine(opts ...types.FlowOption) (types.FlowEngine, error) {
	options := types.NewFlowOptions()
	for _, opt := range opts {
		opt(options)
	}

	var s store.Store
	var err error

	// PostgresConfig takes precedence over MemStore
	if options.PostgresConfig != nil {
		pgConfig := &postgres.Config{
			Host:     options.PostgresConfig.Host,
			Port:     options.PostgresConfig.Port,
			User:     options.PostgresConfig.User,
			Password: options.PostgresConfig.Password,
			Database: options.PostgresConfig.Database,
			SSLMode:  options.PostgresConfig.SSLMode,
		}

		s, err = postgres.NewPostgresStore(pgConfig)
		if err != nil {
			return nil, errors.Annotatef(err, "failed to create PostgreSQL store")
		}
	} else if options.MemStore {
		s = mem.NewMemStore()
	} else {
		// Default to mem store if not specified
		s = mem.NewMemStore()
	}

	return runtime.NewFlowEngine(s, options), nil
}
