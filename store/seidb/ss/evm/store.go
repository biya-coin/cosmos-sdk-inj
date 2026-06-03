package evm

import (
	"fmt"
	"path/filepath"
	"sync"

	commonevm "cosmossdk.io/store/seidb/common/evm"
	seidbcfg "cosmossdk.io/store/seidb/config"
	"cosmossdk.io/store/seidb/db_engine/pebbledb/mvcc"
	iavl "cosmossdk.io/store/seidb/sc/sei-iavl"
	sstypes "cosmossdk.io/store/seidb/ss/types"
	"cosmossdk.io/store/types"
)

const EVMStoreKey = "evm"

type evmStoreType uint8

const (
	evmStoreEmpty evmStoreType = iota
	evmStoreNonce
	evmStoreCodeHash
	evmStoreCode
	evmStoreStorage
	evmStoreLegacy
)

type evmStateStore struct {
	subDBs      map[evmStoreType]sstypes.StateStore
	managedDBs  []sstypes.StateStore
	dir         string
	separateDBs bool
}

func NewStateStore(cfg seidbcfg.Config) (*evmStateStore, error) {
	dir := cfg.StateStoreEVMDBDirectory
	if dir == "" {
		dir = filepath.Join(cfg.Home, "data", "evm_ss")
	}

	store := &evmStateStore{
		subDBs:      make(map[evmStoreType]sstypes.StateStore, 5),
		dir:         dir,
		separateDBs: true,
	}

	for _, storeType := range allEVMStoreTypes() {
		subCfg := cfg
		subCfg.Home = filepath.Join(dir, evmStoreTypeName(storeType))
		db, err := mvcc.NewStore(subCfg)
		if err != nil {
			_ = store.Close()
			return nil, fmt.Errorf("failed to open EVM MVCC DB for %s: %w", evmStoreTypeName(storeType), err)
		}
		store.subDBs[storeType] = db
		store.managedDBs = append(store.managedDBs, db)
	}

	return store, nil
}

func allEVMStoreTypes() []evmStoreType {
	return []evmStoreType{
		evmStoreNonce,
		evmStoreCodeHash,
		evmStoreCode,
		evmStoreStorage,
		evmStoreLegacy,
	}
}

func evmStoreTypeName(storeType evmStoreType) string {
	switch storeType {
	case evmStoreNonce:
		return "nonce"
	case evmStoreCodeHash:
		return "code_hash"
	case evmStoreCode:
		return "code"
	case evmStoreStorage:
		return "storage"
	case evmStoreLegacy:
		return "legacy"
	default:
		return "unknown"
	}
}

func toEVMStoreType(kind commonevm.EVMKeyKind) evmStoreType {
	switch kind {
	case commonevm.EVMKeyNonce:
		return evmStoreNonce
	case commonevm.EVMKeyCodeHash:
		return evmStoreCodeHash
	case commonevm.EVMKeyCode:
		return evmStoreCode
	case commonevm.EVMKeyStorage:
		return evmStoreStorage
	case commonevm.EVMKeyLegacy:
		return evmStoreLegacy
	default:
		return evmStoreEmpty
	}
}

func (s *evmStateStore) routeKey(key []byte) sstypes.StateStore {
	kind, _ := commonevm.ParseEVMKey(key)
	storeType := toEVMStoreType(kind)
	if storeType == evmStoreEmpty {
		return nil
	}
	return s.subDBs[storeType]
}

func (s *evmStateStore) Get(_ string, version int64, key []byte) ([]byte, error) {
	db := s.routeKey(key)
	if db == nil {
		return nil, nil
	}
	return db.Get(EVMStoreKey, version, key)
}

func (s *evmStateStore) Has(_ string, version int64, key []byte) (bool, error) {
	db := s.routeKey(key)
	if db == nil {
		return false, nil
	}
	return db.Has(EVMStoreKey, version, key)
}

func (s *evmStateStore) Iterator(_ string, _ int64, _, _ []byte) (types.Iterator, error) {
	return nil, fmt.Errorf("EVMStateStore: cross-type iteration not supported; use cosmos SS")
}

func (s *evmStateStore) ReverseIterator(_ string, _ int64, _, _ []byte) (types.Iterator, error) {
	return nil, fmt.Errorf("EVMStateStore: cross-type reverse iteration not supported; use cosmos SS")
}

func (s *evmStateStore) Snapshot(_ string, _ int64) (map[string][]byte, bool) {
	return nil, false
}

func (s *evmStateStore) HasVersion(version int64) bool {
	for _, db := range s.managedDBs {
		if !db.HasVersion(version) {
			return false
		}
	}
	return len(s.managedDBs) > 0
}

func (s *evmStateStore) EarliestVersion() int64 {
	var minVersion int64 = -1
	for _, db := range s.managedDBs {
		v := db.EarliestVersion()
		if minVersion < 0 || (v > 0 && v < minVersion) {
			minVersion = v
		}
	}
	if minVersion < 0 {
		return 0
	}
	return minVersion
}

func (s *evmStateStore) ApplyChangeSets(version int64, changeSets []*sstypes.NamedChangeSet) error {
	grouped := s.groupBySubType(changeSets)
	if len(grouped) == 0 {
		return nil
	}
	return s.applyGrouped(version, grouped)
}

func (s *evmStateStore) groupBySubType(changeSets []*sstypes.NamedChangeSet) map[evmStoreType][]*iavl.KVPair {
	grouped := make(map[evmStoreType][]*iavl.KVPair, len(s.subDBs))
	for _, cs := range changeSets {
		if cs == nil || cs.Name != EVMStoreKey || cs.ChangeSet == nil {
			continue
		}
		for _, kvPair := range cs.ChangeSet.Pairs {
			if kvPair == nil {
				continue
			}
			kind, _ := commonevm.ParseEVMKey(kvPair.Key)
			storeType := toEVMStoreType(kind)
			if storeType == evmStoreEmpty {
				continue
			}
			grouped[storeType] = append(grouped[storeType], &iavl.KVPair{
				Key:    kvPair.Key,
				Value:  kvPair.Value,
				Delete: kvPair.Delete,
			})
		}
	}
	return grouped
}

func (s *evmStateStore) applyGrouped(version int64, grouped map[evmStoreType][]*iavl.KVPair) error {
	if len(grouped) == 0 {
		return s.SetLatestVersion(version)
	}

	if len(grouped) == 1 {
		var touched evmStoreType
		for storeType, pairs := range grouped {
			touched = storeType
			if err := s.applyToSubDB(storeType, version, pairs); err != nil {
				return err
			}
		}
		return s.advanceUntouchedSubDBs(version, map[evmStoreType]struct{}{touched: {}})
	}

	var wg sync.WaitGroup
	errCh := make(chan error, len(grouped))
	touched := make(map[evmStoreType]struct{}, len(grouped))
	for storeType, pairs := range grouped {
		touched[storeType] = struct{}{}
		wg.Add(1)
		go func(st evmStoreType, p []*iavl.KVPair) {
			defer wg.Done()
			if err := s.applyToSubDB(st, version, p); err != nil {
				errCh <- err
			}
		}(storeType, pairs)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		return err
	}
	return s.advanceUntouchedSubDBs(version, touched)
}

func (s *evmStateStore) applyToSubDB(storeType evmStoreType, version int64, pairs []*iavl.KVPair) error {
	db := s.subDBs[storeType]
	if db == nil {
		return nil
	}
	cs := []*sstypes.NamedChangeSet{
		{
			Name:      EVMStoreKey,
			ChangeSet: &iavl.ChangeSet{Pairs: pairs},
		},
	}
	return db.ApplyChangeSets(version, cs)
}

func (s *evmStateStore) advanceUntouchedSubDBs(version int64, touched map[evmStoreType]struct{}) error {
	for storeType, db := range s.subDBs {
		if _, ok := touched[storeType]; ok {
			continue
		}
		if err := db.SetLatestVersion(version); err != nil {
			return err
		}
	}
	return nil
}

func (s *evmStateStore) SetLatestVersion(version int64) error {
	for _, db := range s.managedDBs {
		if err := db.SetLatestVersion(version); err != nil {
			return err
		}
	}
	return nil
}

func (s *evmStateStore) RollbackToVersion(target int64) error {
	for _, db := range s.managedDBs {
		if err := db.RollbackToVersion(target); err != nil {
			return err
		}
	}
	return nil
}

func (s *evmStateStore) SyncFromStores(stores map[types.StoreKey]types.CommitKVStore, version int64) error {
	var evmStore types.CommitKVStore
	for key, store := range stores {
		if key != nil && key.Name() == EVMStoreKey {
			evmStore = store
			break
		}
	}
	if evmStore == nil {
		return nil
	}

	grouped := make(map[evmStoreType][]*iavl.KVPair, len(s.subDBs))
	itr := evmStore.Iterator(nil, nil)
	defer itr.Close()
	for ; itr.Valid(); itr.Next() {
		kind, _ := commonevm.ParseEVMKey(itr.Key())
		storeType := toEVMStoreType(kind)
		if storeType == evmStoreEmpty {
			continue
		}
		grouped[storeType] = append(grouped[storeType], &iavl.KVPair{
			Key:   append([]byte(nil), itr.Key()...),
			Value: append([]byte(nil), itr.Value()...),
		})
	}
	if err := itr.Error(); err != nil {
		return err
	}

	for storeType, pairs := range grouped {
		db := s.subDBs[storeType]
		if db == nil {
			continue
		}
		cs := []*sstypes.NamedChangeSet{{Name: EVMStoreKey, ChangeSet: &iavl.ChangeSet{Pairs: pairs}}}
		if err := db.ApplyChangeSets(version, cs); err != nil {
			return err
		}
	}
	return nil
}

func (s *evmStateStore) WaitForPendingWrites() {
	for _, db := range s.managedDBs {
		db.WaitForPendingWrites()
	}
}

func (s *evmStateStore) Close() error {
	var lastErr error
	for _, db := range s.managedDBs {
		if err := db.Close(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

func FilterNamedChangeSets(changeSets []*sstypes.NamedChangeSet) []*sstypes.NamedChangeSet {
	filtered := make([]*sstypes.NamedChangeSet, 0, len(changeSets))
	for _, cs := range changeSets {
		if cs != nil && cs.Name == EVMStoreKey {
			filtered = append(filtered, cs)
		}
	}
	return filtered
}

func StripNamedChangeSets(changeSets []*sstypes.NamedChangeSet) []*sstypes.NamedChangeSet {
	filtered := make([]*sstypes.NamedChangeSet, 0, len(changeSets))
	for _, cs := range changeSets {
		if cs != nil && cs.Name != EVMStoreKey {
			filtered = append(filtered, cs)
		}
	}
	return filtered
}

func ToNamedChangeSets(protoSets []*sstypes.NamedChangeSet) []*sstypes.NamedChangeSet {
	return protoSets
}

func (s *evmStateStore) LatestVersion() int64 {
	var minVersion int64 = -1
	for _, db := range s.managedDBs {
		v := db.LatestVersion()
		if minVersion < 0 || v < minVersion {
			minVersion = v
		}
	}
	if minVersion < 0 {
		return 0
	}
	return minVersion
}
