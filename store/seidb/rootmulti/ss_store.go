package rootmulti

import (
	"fmt"
	"sync"

	dbm "github.com/cosmos/cosmos-db"

	"cosmossdk.io/store/types"
)

// StateStore models the independent SS holder in rootmulti (aligned with
// sei-chain's scStore/ssStore split). M1 keeps behavior-compatible by allowing a
// SC-backed adapter while wiring this interface through rootmulti.
type StateStore interface {
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

func (noopStateStore) Snapshot(_ string, _ int64) (map[string][]byte, bool)                     { return nil, false }
func (noopStateStore) HasVersion(_ int64) bool                                                   { return false }
func (noopStateStore) EarliestVersion() int64                                                    { return 0 }
func (noopStateStore) ApplyChangeSets(_ int64, _ []*NamedChangeSet) error                       { return nil }
func (noopStateStore) SetLatestVersion(_ int64) error                                            { return nil }
func (noopStateStore) RollbackToVersion(_ int64) error                                           { return nil }
func (noopStateStore) SyncFromStores(_ map[types.StoreKey]types.CommitKVStore, _ int64) error   { return nil }
func (noopStateStore) Close() error                                                              { return nil }

type scBackedStateStore struct {
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
