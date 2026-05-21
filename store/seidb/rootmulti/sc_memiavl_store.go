package rootmulti

import (
	"fmt"
	"sort"
	"sync"

	iavl "cosmossdk.io/store/seidb/sc/sei-iavl"
	"cosmossdk.io/store/seidb/sc/memiavl"
	scproto "cosmossdk.io/store/seidb/sc/proto"
	sctypes "cosmossdk.io/store/seidb/sc/types"
	"cosmossdk.io/store/types"
)

// memIAVLStore wraps memiavl.CommitStore for rootmulti (PersistedNode + MemNode,
// WAL, and periodic snapshot rewrite for memory relief).
type memIAVLStore struct {
	mtx sync.RWMutex

	home  string
	cfg   memiavl.Config
	store *memiavl.CommitStore
}

func newMemIAVLStore(home string, cfg memiavl.Config) *memIAVLStore {
	if cfg.SnapshotInterval == 0 {
		cfg = memiavl.DefaultConfig()
	}
	return &memIAVLStore{
		home:  home,
		cfg:   cfg,
		store: memiavl.NewCommitStore(home, cfg),
	}
}

func (s *memIAVLStore) Initialize(storeNames []string) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.store.Initialize(storeNames)
}

func (s *memIAVLStore) LoadVersion(targetVersion int64, storeNames []string) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.store.Initialize(storeNames)
	_, err := s.store.LoadVersion(targetVersion, false)
	return err
}

func (s *memIAVLStore) ApplyChangeSets(changeSets []*NamedChangeSet) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	return s.store.ApplyChangeSets(toProtoChangeSets(changeSets))
}

func (s *memIAVLStore) Commit(expectedVersion int64) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	version, err := s.store.Commit()
	if err != nil {
		return err
	}
	if expectedVersion > 0 && version != expectedVersion {
		return fmt.Errorf("memiavl commit version mismatch: expected %d got %d", expectedVersion, version)
	}
	return nil
}

func (s *memIAVLStore) CurrentVersion() int64 {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return s.store.Version()
}

func (s *memIAVLStore) HasVersion(version int64) bool {
	if version <= 0 {
		return false
	}
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	current := s.store.Version()
	if version > current {
		return false
	}
	earliest, err := s.store.GetEarliestVersion()
	if err != nil {
		return version == current
	}
	return version >= earliest
}

func (s *memIAVLStore) EarliestVersion() int64 {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	earliest, err := s.store.GetEarliestVersion()
	if err != nil {
		return 0
	}
	return earliest
}

func (s *memIAVLStore) RollbackToVersion(target int64) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	return s.store.Rollback(target)
}

func (s *memIAVLStore) GetCommitKVStore(name string) sctypes.CommitKVStore {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return s.store.GetChildStoreByName(name)
}

func (s *memIAVLStore) LastCommitInfo() *scproto.CommitInfo {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return s.store.LastCommitInfo()
}

func (s *memIAVLStore) WorkingCommitInfo() *scproto.CommitInfo {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return s.store.WorkingCommitInfo()
}

func (s *memIAVLStore) SyncFromStores(stores map[types.StoreKey]types.CommitKVStore, version int64) error {
	if version <= 0 {
		return fmt.Errorf("invalid sync version: %d", version)
	}

	changeSets := make([]*scproto.NamedChangeSet, 0, len(stores))
	for key, store := range stores {
		if store == nil || store.GetStoreType() != types.StoreTypeIAVL {
			continue
		}
		pairs, err := collectStoreKVPairs(store)
		if err != nil {
			return err
		}
		if len(pairs) == 0 {
			continue
		}
		changeSets = append(changeSets, &scproto.NamedChangeSet{
			Name:      key.Name(),
			Changeset: iavl.ChangeSet{Pairs: pairs},
		})
	}
	sort.Slice(changeSets, func(i, j int) bool {
		return changeSets[i].Name < changeSets[j].Name
	})

	s.mtx.Lock()
	defer s.mtx.Unlock()
	if err := s.store.ApplyChangeSets(changeSets); err != nil {
		return err
	}
	committed, err := s.store.Commit()
	if err != nil {
		return err
	}
	if committed != version {
		return fmt.Errorf("memiavl sync commit version mismatch: expected %d got %d", version, committed)
	}
	return nil
}

func (s *memIAVLStore) Close() error {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	return s.store.Close()
}

func collectStoreKVPairs(store types.CommitKVStore) ([]*iavl.KVPair, error) {
	itr := store.Iterator(nil, nil)
	defer func() { _ = itr.Close() }()

	var pairs []*iavl.KVPair
	for ; itr.Valid(); itr.Next() {
		pairs = append(pairs, &iavl.KVPair{
			Key:   append([]byte(nil), itr.Key()...),
			Value: append([]byte(nil), itr.Value()...),
		})
	}
	return pairs, nil
}
