package commitment

import (
	"bytes"
	"io"

	"cosmossdk.io/store/cachekv"
	"cosmossdk.io/store/tracekv"
	"cosmossdk.io/store/types"
	iavl "github.com/cosmos/iavl"
)

// Store is a transitional commitment wrapper used by the SeiDB backend path.
// It keeps the same behavior as the wrapped CommitKVStore while exposing
// a dedicated package surface for future SC-specific logic.
type Store struct {
	types.CommitKVStore
	changeSet *iavl.ChangeSet
}

func NewStore(_ types.StoreKey, inner types.CommitKVStore) *Store {
	return &Store{
		CommitKVStore: inner,
		changeSet:     &iavl.ChangeSet{},
	}
}

func (s *Store) CacheWrap() types.CacheWrap {
	return cachekv.NewStore(s)
}

func (s *Store) CacheWrapWithTrace(w io.Writer, tc types.TraceContext) types.CacheWrap {
	return cachekv.NewStore(tracekv.NewStore(s, w, tc))
}

// Set preserves existing behavior while recording a change set that can be
// consumed by the SeiDB rootmulti pipeline.
func (s *Store) Set(key, value []byte) {
	s.CommitKVStore.Set(key, value)
	s.changeSet.Pairs = append(s.changeSet.Pairs, &iavl.KVPair{
		Key: key, Value: value,
	})
}

// Delete preserves existing behavior while recording a delete marker.
func (s *Store) Delete(key []byte) {
	s.CommitKVStore.Delete(key)
	s.changeSet.Pairs = append(s.changeSet.Pairs, &iavl.KVPair{
		Key: key, Delete: true,
	})
}

// PopChangeSet returns the currently accumulated changes and resets the buffer.
func (s *Store) PopChangeSet() *iavl.ChangeSet {
	cs := s.PendingChangeSet()
	s.ClearChangeSet()
	if cs == nil {
		return &iavl.ChangeSet{}
	}
	return cs
}

// PendingChangeSet returns a snapshot of currently accumulated changes.
func (s *Store) PendingChangeSet() *iavl.ChangeSet {
	if s.changeSet == nil || len(s.changeSet.Pairs) == 0 {
		return nil
	}
	pairs := make([]*iavl.KVPair, len(s.changeSet.Pairs))
	copy(pairs, s.changeSet.Pairs)
	return &iavl.ChangeSet{Pairs: pairs}
}

// ClearChangeSet drops all accumulated changes.
func (s *Store) ClearChangeSet() {
	s.changeSet = &iavl.ChangeSet{}
}

// GetChangedPairs returns all changed pairs with the given prefix.
func (s *Store) GetChangedPairs(prefix []byte) (res []*iavl.KVPair) {
	for _, p := range s.changeSet.Pairs {
		if bytes.HasPrefix(p.Key, prefix) {
			res = append(res, p)
		}
	}
	return
}
