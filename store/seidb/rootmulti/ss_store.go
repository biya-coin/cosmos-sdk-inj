package rootmulti

import (
	"fmt"
	"sort"
	"sync"

	dbm "github.com/cosmos/cosmos-db"

	"cosmossdk.io/store/types"
)

// StateStore models the independent SS holder in rootmulti (aligned with
// sei-chain's scStore/ssStore split). M1 keeps behavior-compatible by allowing a
// SC-backed adapter while wiring this interface through rootmulti.
type StateStore interface {
	Get(storeName string, version int64, key []byte) ([]byte, error)
	Has(storeName string, version int64, key []byte) (bool, error)
	Iterator(storeName string, version int64, start, end []byte) (types.Iterator, error)
	ReverseIterator(storeName string, version int64, start, end []byte) (types.Iterator, error)
	Snapshot(storeName string, version int64) (map[string][]byte, bool)
	HasVersion(version int64) bool
	EarliestVersion() int64
	ApplyChangeSets(version int64, changeSets []*NamedChangeSet) error
	SetLatestVersion(version int64) error
	RollbackToVersion(target int64) error
	SyncFromStores(stores map[types.StoreKey]types.CommitKVStore, version int64) error
	Close() error
}

type SnapshotImportNode struct {
	StoreKey string
	Key      []byte
	Value    []byte
}

type snapshotImporter interface {
	ImportSnapshot(version int64, nodes <-chan SnapshotImportNode) error
}

type ssStoreBuilder func(db dbm.DB, cfg Config, scStore SCStore) (StateStore, error)

var (
	ssBuilderMu sync.RWMutex
	ssBuilders  = map[string]ssStoreBuilder{
		"pebbledb": func(_ dbm.DB, cfg Config, _ SCStore) (StateStore, error) {
			return newPebbleStateStore(cfg)
		},
	}
)

func RegisterStateStoreBuilder(backend string, builder ssStoreBuilder) error {
	if backend == "" {
		return fmt.Errorf("backend must not be empty")
	}
	if builder == nil {
		return fmt.Errorf("builder must not be nil")
	}

	ssBuilderMu.Lock()
	defer ssBuilderMu.Unlock()
	ssBuilders[backend] = builder
	return nil
}

func newStateStoreFromConfig(db dbm.DB, cfg Config, scStore SCStore) StateStore {
	backend := cfg.StateStoreBackend
	if backend == "" {
		return noopStateStore{}
	}

	ssBuilderMu.RLock()
	builder, ok := ssBuilders[backend]
	ssBuilderMu.RUnlock()
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

type noopStateStore struct{}

func (noopStateStore) Get(_ string, _ int64, _ []byte) ([]byte, error)                              { return nil, nil }
func (noopStateStore) Has(_ string, _ int64, _ []byte) (bool, error)                                { return false, nil }
func (noopStateStore) Iterator(_ string, _ int64, _, _ []byte) (types.Iterator, error)              { return emptyIterator{}, nil }
func (noopStateStore) ReverseIterator(_ string, _ int64, _, _ []byte) (types.Iterator, error)       { return emptyIterator{}, nil }
func (noopStateStore) Snapshot(_ string, _ int64) (map[string][]byte, bool)                          { return nil, false }
func (noopStateStore) HasVersion(_ int64) bool                                                        { return false }
func (noopStateStore) EarliestVersion() int64                                                         { return 0 }
func (noopStateStore) ApplyChangeSets(_ int64, _ []*NamedChangeSet) error                            { return nil }
func (noopStateStore) SetLatestVersion(_ int64) error                                                 { return nil }
func (noopStateStore) RollbackToVersion(_ int64) error                                                { return nil }
func (noopStateStore) SyncFromStores(_ map[types.StoreKey]types.CommitKVStore, _ int64) error        { return nil }
func (noopStateStore) Close() error                                                                   { return nil }

type scBackedStateStore struct {
	reader interface {
		Get(storeName string, version int64, key []byte) ([]byte, error)
		Has(storeName string, version int64, key []byte) (bool, error)
		Iterator(storeName string, version int64, start, end []byte) (types.Iterator, error)
		ReverseIterator(storeName string, version int64, start, end []byte) (types.Iterator, error)
	}
	snapshot interface {
		Snapshot(storeName string, version int64) (map[string][]byte, bool)
	}
	versioned interface {
		HasVersion(version int64) bool
		EarliestVersion() int64
	}
	rollbacker interface {
		RollbackToVersion(target int64) error
	}
	syncer interface {
		SyncFromStores(stores map[types.StoreKey]types.CommitKVStore, version int64) error
	}
}

func newSCBackedStateStore(scStore SCStore) StateStore {
	adapter := &scBackedStateStore{}
	if r, ok := scStore.(interface {
		Get(storeName string, version int64, key []byte) ([]byte, error)
		Has(storeName string, version int64, key []byte) (bool, error)
		Iterator(storeName string, version int64, start, end []byte) (types.Iterator, error)
		ReverseIterator(storeName string, version int64, start, end []byte) (types.Iterator, error)
	}); ok {
		adapter.reader = r
	}
	if s, ok := scStore.(interface {
		Snapshot(storeName string, version int64) (map[string][]byte, bool)
	}); ok {
		adapter.snapshot = s
	}
	if v, ok := scStore.(interface {
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

func (s *scBackedStateStore) Get(storeName string, version int64, key []byte) ([]byte, error) {
	if s.reader == nil {
		snap, ok := s.Snapshot(storeName, version)
		if !ok {
			return nil, nil
		}
		return cloneBytesNonNil(snap[string(key)]), nil
	}
	return s.reader.Get(storeName, version, key)
}

func (s *scBackedStateStore) Has(storeName string, version int64, key []byte) (bool, error) {
	if s.reader == nil {
		snap, ok := s.Snapshot(storeName, version)
		if !ok {
			return false, nil
		}
		_, found := snap[string(key)]
		return found, nil
	}
	return s.reader.Has(storeName, version, key)
}

func (s *scBackedStateStore) Iterator(storeName string, version int64, start, end []byte) (types.Iterator, error) {
	if s.reader == nil {
		snap, ok := s.Snapshot(storeName, version)
		if !ok {
			return emptyIterator{}, nil
		}
		return newSnapshotIterator(snap, start, end, true), nil
	}
	return s.reader.Iterator(storeName, version, start, end)
}

func (s *scBackedStateStore) ReverseIterator(storeName string, version int64, start, end []byte) (types.Iterator, error) {
	if s.reader == nil {
		snap, ok := s.Snapshot(storeName, version)
		if !ok {
			return emptyIterator{}, nil
		}
		return newSnapshotIterator(snap, start, end, false), nil
	}
	return s.reader.ReverseIterator(storeName, version, start, end)
}

func (s *scBackedStateStore) Snapshot(storeName string, version int64) (map[string][]byte, bool) {
	if s.snapshot == nil {
		return nil, false
	}
	return s.snapshot.Snapshot(storeName, version)
}

func (s *scBackedStateStore) HasVersion(version int64) bool {
	if s.versioned == nil {
		return false
	}
	return s.versioned.HasVersion(version)
}

func (s *scBackedStateStore) EarliestVersion() int64 {
	if s.versioned == nil {
		return 0
	}
	return s.versioned.EarliestVersion()
}

func (s *scBackedStateStore) ApplyChangeSets(_ int64, _ []*NamedChangeSet) error { return nil }

func (s *scBackedStateStore) SetLatestVersion(_ int64) error { return nil }

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

func (s *scBackedStateStore) Close() error { return nil }

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
		valBytes[i] = cloneBytesNonNil(snap[k])
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
