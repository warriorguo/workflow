package types

import (
	"context"

	"github.com/mcuadros/go-defaults"
)

type ExecutionOptions struct {
	Concurrent int
}
type ExecutionOption func(*ExecutionOptions)

func NewFlowOptions() *FlowOptions {
	opts := &FlowOptions{Ctx: context.Background()}
	defaults.SetDefaults(opts)
	return opts
}

type FlowOptions struct {
	Ctx context.Context
	/**
	 * default: 100000
	 * the flowengine will try to run nodes at most to this value.
	 */
	MaxNodeConcurrency int `default:"100000"`
	/**
	 * default: true, can set it to false and *important*
	 * caller should call FlowEngine.RunOnce() looply.
	 */
	AutoStart bool `default:"true"`
	/**
	 * default: true, only set it to false when doing debugging or developing.
	 * TaskRunAsync indicates whether the node run in async mode or not.
	 * FlowEngine will try to run at most MaxConcurrency nodes once a time.
	 * If TaskRunAsync is true, it will run in a goroutine. Otherwise all
	 * the runnable node will run one by one.
	 * If TaskRunAsync is true, after RunOnce, node may not finish running.
	 */
	TaskRunAsync bool `default:"true"`
	/**
	 * default: false, only set it to true when doing testing or developing.
	 */
	MemStore bool `default:"false"`

	// PostgreSQL store configuration
	// If both MemStore and PostgresConfig are set, PostgresConfig takes precedence
	PostgresConfig *PostgresConfig
}

// PostgresConfig holds PostgreSQL connection configuration
type PostgresConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
	SSLMode  string // disable, require, verify-ca, verify-full
}
type FlowOption func(*FlowOptions)

func WithContext(ctx context.Context) FlowOption {
	return func(opts *FlowOptions) {
		opts.Ctx = ctx
	}
}

func SetMaxNodeConcurrency(concurrency int) FlowOption {
	return func(opts *FlowOptions) {
		opts.MaxNodeConcurrency = concurrency
	}
}

func DisableAutoStart() FlowOption {
	return func(opts *FlowOptions) {
		opts.AutoStart = false
	}
}

func DisableTaskRunAsync() FlowOption {
	return func(opts *FlowOptions) {
		opts.TaskRunAsync = false
	}
}

func EnableMemStore() FlowOption {
	return func(opts *FlowOptions) {
		opts.MemStore = true
	}
}

// WithPostgresConfig configures the flow engine to use PostgreSQL store
func WithPostgresConfig(config *PostgresConfig) FlowOption {
	return func(opts *FlowOptions) {
		opts.PostgresConfig = config
	}
}
