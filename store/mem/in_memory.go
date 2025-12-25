package mem

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/warriorguo/workflow/store"
)

var (
	_ store.Store = &memStore{}
)

func NewMemStore() store.Store {
	return &memStore{
		m: make(map[string][]byte),
		// setup no error as default
		mockErrHandler: defaultNoErr,
	}
}

func NewMemStoreWithErrHandler(errHandler func() error) store.Store {
	return &memStore{
		m: make(map[string][]byte),
		// .
		mockErrHandler: errHandler,
	}
}

func defaultNoErr() error {
	return nil
}

/**
 * memStore is store implementation based on pure memory, it aims to provide a method for debug & testing
 * NEVER use it in the Production!
 */
type memStore struct {
	mu sync.Mutex

	mockErrHandler func() error

	m map[string][]byte
}

func (m *memStore) String() string {
	s := "\n----------\n"
	for key, value := range m.m {
		s += fmt.Sprintf("%s: %s\n", key, string(value))
	}
	s += "----------\n"
	return s
}

func (m *memStore) Get(ctx context.Context, prefix, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.m[prefix+"|"+key], m.mockErrHandler()
}

func (m *memStore) Set(ctx context.Context, prefix, key string, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.m[prefix+"|"+key] = value
	return m.mockErrHandler()
}

func (m *memStore) Remove(ctx context.Context, prefix, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.m, prefix+"|"+key)
	return m.mockErrHandler()
}

func (m *memStore) List(ctx context.Context, prefix string, iterator func(key string) bool) error {
	m.mu.Lock()

	prefix += "|"
	matchedKeys := make([]string, 0)
	for key := range m.m {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		matchedKeys = append(matchedKeys, key)
	}
	m.mu.Unlock()

	for _, key := range matchedKeys {
		key, _ = strings.CutPrefix(key, prefix)
		if !iterator(key) {
			break
		}
	}
	return m.mockErrHandler()
}
