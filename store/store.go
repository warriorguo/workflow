package store

import "context"

type Store interface {
	Get(ctx context.Context, prefix, key string) ([]byte, error)
	Set(ctx context.Context, prefix, key string, value []byte) error
	/**
	 * Remove a prefix and key
	 * remove an unexists prefix + key would NOT return error
	 */
	Remove(ctx context.Context, prefix, key string) error

	List(ctx context.Context, prefix string, iterator func(key string) bool) error

	/**
	Append(ctx context.Context, prefix, key string, value []byte) error
	ListAppend(ctx context.Context, prefix, key string, iterator func(value []byte) bool) error
	*/
}
