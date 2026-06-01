package commitment

import (
	"bytes"
	"io"

	dbm "github.com/cosmos/cosmos-db"
	ics23 "github.com/cosmos/ics23/go"
	iavl "cosmossdk.io/store/seidb/sc/sei-iavl"

	"cosmossdk.io/store/cachekv"
	pruningtypes "cosmossdk.io/store/pruning/types"
	"cosmossdk.io/store/seidb/sc/types"
	"cosmossdk.io/store/tracekv"
	storetypes "cosmossdk.io/store/types"
)

// Store wraps a MemIAVL tree (PersistedNode + MemNode). Deliver-time writes only
// accumulate changesets; Commit applies them to the SC engine.
type Store struct {
	tree      types.CommitKVStore
	changeSet iavl.ChangeSet
}

func NewStore(tree types.CommitKVStore) *Store {
	return &Store{
		tree: tree,
	}
}

// LegacyNewStore keeps the transitional wrapper for non-MemIAVL paths.
func LegacyNewStore(_ storetypes.StoreKey, inner storetypes.CommitKVStore) *Store {
	return &Store{
		tree: commitKVStoreAdapter{inner},
	}
}

func (s *Store) Commit() storetypes.CommitID {
	panic("memiavl store is not supposed to be committed alone")
}

func (s *Store) LastCommitID() storetypes.CommitID {
	hash := s.tree.RootHash()
	return storetypes.CommitID{
		Version: s.tree.Version(),
		Hash:    hash,
	}
}

func (s *Store) SetPruning(_ pruningtypes.PruningOptions) {
	panic("cannot set pruning options on an initialized memiavl store")
}

func (s *Store) GetPruning() pruningtypes.PruningOptions {
	panic("cannot get pruning options on an initialized memiavl store")
}

func (s *Store) WorkingHash() []byte {
	return s.tree.RootHash()
}

func (s *Store) GetStoreType() storetypes.StoreType {
	return storetypes.StoreTypeIAVL
}

func (s *Store) CacheWrap() storetypes.CacheWrap {
	return cachekv.NewStore(s)
}

func (s *Store) CacheWrapWithTrace(w io.Writer, tc storetypes.TraceContext) storetypes.CacheWrap {
	return cachekv.NewStore(tracekv.NewStore(s, w, tc))
}

func (s *Store) Set(key, value []byte) {
	s.changeSet.Pairs = append(s.changeSet.Pairs, &iavl.KVPair{
		Key: key, Value: value,
	})
	// MemIAVL tree (PersistedNode + MemNode CoW): deliver only records changeset.
	// Legacy adapter keeps runtime IAVL in sync for Phase A until SC fully owns commit.
	if legacy, ok := s.tree.(commitKVStoreAdapter); ok {
		legacy.CommitKVStore.Set(key, value)
	}
}

func (s *Store) Get(key []byte) []byte {
	return s.tree.Get(key)
}

func (s *Store) Has(key []byte) bool {
	return s.tree.Has(key)
}

func (s *Store) Delete(key []byte) {
	s.changeSet.Pairs = append(s.changeSet.Pairs, &iavl.KVPair{
		Key: key, Delete: true,
	})
	if legacy, ok := s.tree.(commitKVStoreAdapter); ok {
		legacy.CommitKVStore.Delete(key)
	}
}

func (s *Store) Iterator(start, end []byte) storetypes.Iterator {
	return s.tree.Iterator(start, end, true)
}

func (s *Store) ReverseIterator(start, end []byte) storetypes.Iterator {
	return s.tree.Iterator(start, end, false)
}

func (s *Store) SetInitialVersion(_ int64) {
	panic("memiavl store's SetInitialVersion is not supposed to be called directly")
}

func (s *Store) PopChangeSet() *iavl.ChangeSet {
	cs := s.PendingChangeSet()
	s.ClearChangeSet()
	if cs == nil {
		return &iavl.ChangeSet{}
	}
	return cs
}

func (s *Store) PendingChangeSet() *iavl.ChangeSet {
	if len(s.changeSet.Pairs) == 0 {
		return nil
	}
	pairs := make([]*iavl.KVPair, len(s.changeSet.Pairs))
	copy(pairs, s.changeSet.Pairs)
	return &iavl.ChangeSet{Pairs: pairs}
}

func (s *Store) ClearChangeSet() {
	s.changeSet = iavl.ChangeSet{}
}

func (s *Store) GetChangedPairs(prefix []byte) (res []*iavl.KVPair) {
	for _, p := range s.changeSet.Pairs {
		if bytes.HasPrefix(p.Key, prefix) {
			res = append(res, p)
		}
	}
	return
}

// commitKVStoreAdapter bridges legacy IAVL CommitKVStore to the SC tree interface.
type commitKVStoreAdapter struct {
	storetypes.CommitKVStore
}

func (a commitKVStoreAdapter) RootHash() []byte {
	return a.WorkingHash()
}

func (a commitKVStoreAdapter) Version() int64 {
	return a.LastCommitID().Version
}

func (a commitKVStoreAdapter) Remove(key []byte) {
	a.CommitKVStore.Delete(key)
}

func (a commitKVStoreAdapter) Iterator(start, end []byte, ascending bool) dbm.Iterator {
	if ascending {
		return iteratorAdapter{a.CommitKVStore.Iterator(start, end)}
	}
	return iteratorAdapter{a.CommitKVStore.ReverseIterator(start, end)}
}

type iteratorAdapter struct {
	storetypes.Iterator
}

func (a commitKVStoreAdapter) GetProof(_ []byte) *ics23.CommitmentProof {
	return nil
}

func (a commitKVStoreAdapter) Close() error {
	return nil
}
