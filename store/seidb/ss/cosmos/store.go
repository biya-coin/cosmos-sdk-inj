package cosmos

import (
	scproto "cosmossdk.io/store/seidb/sc/proto"
	sstypes "cosmossdk.io/store/seidb/ss/types"
	"cosmossdk.io/store/types"
)

type cosmosStateStore struct {
	db sstypes.StateStore
}

func NewStateStore(db sstypes.StateStore) sstypes.StateStore {
	return &cosmosStateStore{db: db}
}

func (s *cosmosStateStore) Get(storeName string, version int64, key []byte) ([]byte, error) {
	return s.db.Get(storeName, version, key)
}

func (s *cosmosStateStore) Has(storeName string, version int64, key []byte) (bool, error) {
	return s.db.Has(storeName, version, key)
}

func (s *cosmosStateStore) Iterator(storeName string, version int64, start, end []byte) (types.Iterator, error) {
	return s.db.Iterator(storeName, version, start, end)
}

func (s *cosmosStateStore) ReverseIterator(storeName string, version int64, start, end []byte) (types.Iterator, error) {
	return s.db.ReverseIterator(storeName, version, start, end)
}

func (s *cosmosStateStore) Snapshot(storeName string, version int64) (map[string][]byte, bool) {
	return s.db.Snapshot(storeName, version)
}

func (s *cosmosStateStore) LatestVersion() int64 {
	return s.db.LatestVersion()
}

func (s *cosmosStateStore) HasVersion(version int64) bool {
	return s.db.HasVersion(version)
}

func (s *cosmosStateStore) EarliestVersion() int64 {
	return s.db.EarliestVersion()
}

func (s *cosmosStateStore) ApplyChangeSets(version int64, changeSets []*sstypes.NamedChangeSet) error {
	return s.db.ApplyChangeSets(version, changeSets)
}

func (s *cosmosStateStore) SetLatestVersion(version int64) error {
	return s.db.SetLatestVersion(version)
}

func (s *cosmosStateStore) RollbackToVersion(target int64) error {
	return s.db.RollbackToVersion(target)
}

func (s *cosmosStateStore) SyncFromStores(stores map[types.StoreKey]types.CommitKVStore, version int64) error {
	return s.db.SyncFromStores(stores, version)
}

func (s *cosmosStateStore) Close() error {
	return s.db.Close()
}

func (s *cosmosStateStore) ReplayWAL(fromVersion int64, toVersion int64, handler func(entry scproto.ChangelogEntry) error) error {
	if replayer, ok := s.db.(interface {
		ReplayWAL(fromVersion int64, toVersion int64, handler func(entry scproto.ChangelogEntry) error) error
	}); ok {
		return replayer.ReplayWAL(fromVersion, toVersion, handler)
	}
	return nil
}

func (s *cosmosStateStore) WaitForPendingWrites() {
	s.db.WaitForPendingWrites()
}
