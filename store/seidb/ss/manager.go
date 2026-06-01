package ss

import (
	"fmt"
	"sort"
	"sync"

	seidbcfg "cosmossdk.io/store/seidb/config"
	"cosmossdk.io/store/seidb/ss/composite"
	sstypes "cosmossdk.io/store/seidb/ss/types"
	ssutils "cosmossdk.io/store/seidb/ss/utils"
	"cosmossdk.io/store/types"
	dbm "github.com/cosmos/cosmos-db"
)

type Builder func(db dbm.DB, cfg seidbcfg.Config, scStore any) (sstypes.StateStore, error)

type snapshotReader interface {
	Get(storeName string, version int64, key []byte) ([]byte, error)
	Has(storeName string, version int64, key []byte) (bool, error)
	Iterator(storeName string, version int64, start, end []byte) (types.Iterator, error)
	ReverseIterator(storeName string, version int64, start, end []byte) (types.Iterator, error)
	Snapshot(storeName string, version int64) (map[string][]byte, bool)
}

var (
	builderMu sync.RWMutex
	builders  = map[string]Builder{
		"pebbledb": func(_ dbm.DB, cfg seidbcfg.Config, _ any) (sstypes.StateStore, error) {
			return composite.NewStateStore(cfg)
		},
	}
)

func RegisterBuilder(backend string, builder Builder) error {
	if backend == "" {
		return fmt.Errorf("backend must not be empty")
	}
	if builder == nil {
		return fmt.Errorf("builder must not be nil")
	}

	builderMu.Lock()
	defer builderMu.Unlock()
	builders[backend] = builder
	return nil
}

func NewFromConfig(db dbm.DB, cfg seidbcfg.Config, scStore any) sstypes.StateStore {
	backend := cfg.StateStoreBackend
	if backend == "" {
		return noopStateStore{}
	}

	builderMu.RLock()
	builder, ok := builders[backend]
	builderMu.RUnlock()
	if !ok {
		panic(fmt.Errorf("unknown seidb state store backend %q", backend))
	}

	store, err := builder(db, cfg, scStore)
	if err != nil {
		panic(fmt.Errorf("failed to initialize seidb state store backend %q: %w", backend, err))
	}
	if store == nil {
		panic(fmt.Errorf("seidb state store backend %q returned nil store", backend))
	}
	return store
}

func NewSCBackedStateStore(scStore any) sstypes.StateStore {
	adapter := &scBackedStateStore{}
	if r, ok := scStore.(snapshotReader); ok {
		adapter.reader = r
	}
	if v, ok := scStore.(interface {
		LatestVersion() int64
		HasVersion(version int64) bool
		EarliestVersion() int64
	}); ok {
		adapter.versioned = v
	}
	if r, ok := scStore.(interface {
		RollbackToVersion(target int64) error
	}); ok {
		adapter.rollbacker = r
	}
	if s, ok := scStore.(interface {
		SyncFromStores(stores map[types.StoreKey]types.CommitKVStore, version int64) error
	}); ok {
		adapter.syncer = s
	}
	return adapter
}

type noopStateStore struct{}

func (noopStateStore) Get(_ string, _ int64, _ []byte) ([]byte, error)                        { return nil, nil }
func (noopStateStore) Has(_ string, _ int64, _ []byte) (bool, error)                          { return false, nil }
func (noopStateStore) Iterator(_ string, _ int64, _, _ []byte) (types.Iterator, error)        { return emptyIterator{}, nil }
func (noopStateStore) ReverseIterator(_ string, _ int64, _, _ []byte) (types.Iterator, error) { return emptyIterator{}, nil }
func (noopStateStore) Snapshot(_ string, _ int64) (map[string][]byte, bool)                    { return nil, false }
func (noopStateStore) LatestVersion() int64                                                    { return 0 }
func (noopStateStore) HasVersion(_ int64) bool                                                  { return false }
func (noopStateStore) EarliestVersion() int64                                                   { return 0 }
func (noopStateStore) ApplyChangeSets(_ int64, _ []*sstypes.NamedChangeSet) error              { return nil }
func (noopStateStore) SetLatestVersion(_ int64) error                                           { return nil }
func (noopStateStore) RollbackToVersion(_ int64) error                                          { return nil }
func (noopStateStore) SyncFromStores(_ map[types.StoreKey]types.CommitKVStore, _ int64) error  { return nil }
func (noopStateStore) WaitForPendingWrites()                                                  {}
func (noopStateStore) Close() error                                                             { return nil }

type scBackedStateStore struct {
	reader     snapshotReader
	versioned  interface {
		LatestVersion() int64
		HasVersion(version int64) bool
		EarliestVersion() int64
	}
	rollbacker interface{ RollbackToVersion(target int64) error }
	syncer     interface{ SyncFromStores(stores map[types.StoreKey]types.CommitKVStore, version int64) error }
}

func (s *scBackedStateStore) Get(storeName string, version int64, key []byte) ([]byte, error) {
	if s.reader == nil {
		return nil, nil
	}
	snap, ok := s.reader.Snapshot(storeName, version)
	if !ok {
		return nil, nil
	}
	return ssutils.CloneBytesNonNil(snap[string(key)]), nil
}

func (s *scBackedStateStore) Has(storeName string, version int64, key []byte) (bool, error) {
	if s.reader == nil {
		return false, nil
	}
	snap, ok := s.reader.Snapshot(storeName, version)
	if !ok {
		return false, nil
	}
	_, found := snap[string(key)]
	return found, nil
}

func (s *scBackedStateStore) Iterator(storeName string, version int64, start, end []byte) (types.Iterator, error) {
	if s.reader == nil {
		return emptyIterator{}, nil
	}
	snap, ok := s.reader.Snapshot(storeName, version)
	if !ok {
		return emptyIterator{}, nil
	}
	return newSnapshotIterator(snap, start, end, true), nil
}

func (s *scBackedStateStore) ReverseIterator(storeName string, version int64, start, end []byte) (types.Iterator, error) {
	if s.reader == nil {
		return emptyIterator{}, nil
	}
	snap, ok := s.reader.Snapshot(storeName, version)
	if !ok {
		return emptyIterator{}, nil
	}
	return newSnapshotIterator(snap, start, end, false), nil
}

func (s *scBackedStateStore) Snapshot(storeName string, version int64) (map[string][]byte, bool) {
	if s.reader == nil {
		return nil, false
	}
	return s.reader.Snapshot(storeName, version)
}

func (s *scBackedStateStore) HasVersion(version int64) bool {
	if s.versioned == nil {
		return false
	}
	return s.versioned.HasVersion(version)
}

func (s *scBackedStateStore) LatestVersion() int64 {
	if s.versioned == nil {
		return 0
	}
	return s.versioned.LatestVersion()
}

func (s *scBackedStateStore) EarliestVersion() int64 {
	if s.versioned == nil {
		return 0
	}
	return s.versioned.EarliestVersion()
}

func (s *scBackedStateStore) ApplyChangeSets(_ int64, _ []*sstypes.NamedChangeSet) error { return nil }
func (s *scBackedStateStore) SetLatestVersion(_ int64) error                              { return nil }
func (s *scBackedStateStore) WaitForPendingWrites()                                       {}
func (s *scBackedStateStore) Close() error                                                { return nil }

func (s *scBackedStateStore) RollbackToVersion(target int64) error {
	if s.rollbacker == nil {
		return nil
	}
	return s.rollbacker.RollbackToVersion(target)
}

func (s *scBackedStateStore) SyncFromStores(stores map[types.StoreKey]types.CommitKVStore, version int64) error {
	if s.syncer == nil {
		return nil
	}
	return s.syncer.SyncFromStores(stores, version)
}

type emptyIterator struct{}

func (emptyIterator) Domain() ([]byte, []byte) { return nil, nil }
func (emptyIterator) Valid() bool              { return false }
func (emptyIterator) Next()                    {}
func (emptyIterator) Key() []byte              { panic("invalid iterator") }
func (emptyIterator) Value() []byte            { panic("invalid iterator") }
func (emptyIterator) Error() error             { return nil }
func (emptyIterator) Close() error             { return nil }

type snapshotIterator struct {
	keys   [][]byte
	values [][]byte
	idx    int
}

func newSnapshotIterator(snap map[string][]byte, start, end []byte, ascending bool) *snapshotIterator {
	keys := make([]string, 0, len(snap))
	for k := range snap {
		if len(start) > 0 && k < string(start) {
			continue
		}
		if len(end) > 0 && k >= string(end) {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if !ascending {
		for i, j := 0, len(keys)-1; i < j; i, j = i+1, j-1 {
			keys[i], keys[j] = keys[j], keys[i]
		}
	}

	keyBytes := make([][]byte, len(keys))
	valBytes := make([][]byte, len(keys))
	for i, k := range keys {
		keyBytes[i] = []byte(k)
		valBytes[i] = ssutils.CloneBytesNonNil(snap[k])
	}
	return &snapshotIterator{keys: keyBytes, values: valBytes}
}

func (it *snapshotIterator) Domain() ([]byte, []byte) { return nil, nil }
func (it *snapshotIterator) Valid() bool              { return it.idx >= 0 && it.idx < len(it.keys) }
func (it *snapshotIterator) Next()                    { it.idx++ }
func (it *snapshotIterator) Key() []byte {
	if !it.Valid() {
		panic("invalid iterator")
	}
	return it.keys[it.idx]
}
func (it *snapshotIterator) Value() []byte {
	if !it.Valid() {
		panic("invalid iterator")
	}
	return it.values[it.idx]
}
func (it *snapshotIterator) Error() error { return nil }
func (it *snapshotIterator) Close() error { return nil }
