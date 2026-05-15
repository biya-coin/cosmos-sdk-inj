package state

import (
	"fmt"
	"io"
	"sort"

	"cosmossdk.io/store/cachekv"
	"cosmossdk.io/store/internal/kv"
	"cosmossdk.io/store/tracekv"
	"cosmossdk.io/store/types"
)

const StoreTypeSSStore = 100

type VersionedSnapshotReader interface {
	Snapshot(storeName string, version int64) (map[string][]byte, bool)
}

// Store wraps a versioned snapshot reader and exposes a read-only KVStore.
type Store struct {
	reader   VersionedSnapshotReader
	storeKey types.StoreKey
	version  int64
}

func NewStore(reader VersionedSnapshotReader, storeKey types.StoreKey, version int64) *Store {
	return &Store{reader: reader, storeKey: storeKey, version: version}
}

func (st *Store) GetStoreType() types.StoreType {
	return StoreTypeSSStore
}

func (st *Store) CacheWrap() types.CacheWrap {
	return cachekv.NewStore(st)
}

func (st *Store) CacheWrapWithTrace(w io.Writer, tc types.TraceContext) types.CacheWrap {
	return cachekv.NewStore(tracekv.NewStore(st, w, tc))
}

func (st *Store) Get(key []byte) []byte {
	snap, ok := st.reader.Snapshot(st.storeKey.Name(), st.version)
	if !ok {
		return nil
	}
	return snap[string(key)]
}

func (st *Store) Has(key []byte) bool {
	return st.Get(key) != nil
}

func (st *Store) Set(_, _ []byte) {
	panic("write operation is not supported")
}

func (st *Store) Delete(_ []byte) {
	panic("write operation is not supported")
}

func (st *Store) Iterator(start, end []byte) types.Iterator {
	return newMapIterator(st.snapshot(), start, end, true)
}

func (st *Store) ReverseIterator(start, end []byte) types.Iterator {
	return newMapIterator(st.snapshot(), start, end, false)
}

func (st *Store) snapshot() map[string][]byte {
	snap, ok := st.reader.Snapshot(st.storeKey.Name(), st.version)
	if !ok {
		return map[string][]byte{}
	}
	return snap
}

type mapIterator struct {
	keys   [][]byte
	values [][]byte
	idx    int
}

func newMapIterator(snap map[string][]byte, start, end []byte, ascending bool) *mapIterator {
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
		valBytes[i] = append([]byte(nil), snap[k]...)
	}
	return &mapIterator{keys: keyBytes, values: valBytes}
}

func (it *mapIterator) Domain() (start, end []byte) { return nil, nil }
func (it *mapIterator) Valid() bool                 { return it.idx >= 0 && it.idx < len(it.keys) }
func (it *mapIterator) Next()                       { it.idx++ }
func (it *mapIterator) Key() []byte {
	if !it.Valid() {
		panic("invalid iterator")
	}
	return it.keys[it.idx]
}
func (it *mapIterator) Value() []byte {
	if !it.Valid() {
		panic("invalid iterator")
	}
	return it.values[it.idx]
}
func (it *mapIterator) Error() error { return nil }
func (it *mapIterator) Close() error { return nil }

// Query implements only key/subspace paths used by base store queries.
func (st *Store) Query(req *types.RequestQuery) (*types.ResponseQuery, error) {
	if req.Prove {
		return nil, fmt.Errorf("proof query is not supported on state store")
	}
	if req.Height > 0 && req.Height > st.version {
		return nil, fmt.Errorf("invalid height: %d", req.Height)
	}
	if _, ok := st.reader.Snapshot(st.storeKey.Name(), st.version); !ok {
		return nil, fmt.Errorf("historical version %d is unavailable in state store", st.version)
	}
	res := &types.ResponseQuery{Height: st.version}
	switch req.Path {
	case "/key":
		res.Key = req.Data
		res.Value = st.Get(req.Data)
		return res, nil
	case "/subspace":
		pairs := kv.Pairs{Pairs: make([]kv.Pair, 0)}
		subspace := req.Data
		res.Key = subspace

		iterator := types.KVStorePrefixIterator(st, subspace)
		for ; iterator.Valid(); iterator.Next() {
			pairs.Pairs = append(pairs.Pairs, kv.Pair{Key: iterator.Key(), Value: iterator.Value()})
		}
		if err := iterator.Close(); err != nil {
			return nil, fmt.Errorf("failed to close iterator: %w", err)
		}

		bz, err := pairs.Marshal()
		if err != nil {
			return nil, fmt.Errorf("failed to marshal KV pairs: %w", err)
		}
		res.Value = bz
		return res, nil
	default:
		return nil, fmt.Errorf("unexpected query path: %s", req.Path)
	}
}
