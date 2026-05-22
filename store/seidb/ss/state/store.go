package state

import (
	"fmt"
	"io"

	"cosmossdk.io/store/cachekv"
	"cosmossdk.io/store/internal/kv"
	"cosmossdk.io/store/tracekv"
	"cosmossdk.io/store/types"
)

const StoreTypeSSStore = 100

type VersionedSnapshotReader interface {
	Get(storeName string, version int64, key []byte) ([]byte, error)
	Has(storeName string, version int64, key []byte) (bool, error)
	Iterator(storeName string, version int64, start, end []byte) (types.Iterator, error)
	ReverseIterator(storeName string, version int64, start, end []byte) (types.Iterator, error)
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
	value, err := st.reader.Get(st.storeKey.Name(), st.version, key)
	if err != nil {
		panic(err)
	}
	return value
}

func (st *Store) Has(key []byte) bool {
	found, err := st.reader.Has(st.storeKey.Name(), st.version, key)
	if err != nil {
		panic(err)
	}
	return found
}

func (st *Store) Set(_, _ []byte) {
	panic("write operation is not supported")
}

func (st *Store) Delete(_ []byte) {
	panic("write operation is not supported")
}

func (st *Store) Iterator(start, end []byte) types.Iterator {
	itr, err := st.reader.Iterator(st.storeKey.Name(), st.version, start, end)
	if err != nil {
		panic(err)
	}
	return itr
}

func (st *Store) ReverseIterator(start, end []byte) types.Iterator {
	itr, err := st.reader.ReverseIterator(st.storeKey.Name(), st.version, start, end)
	if err != nil {
		panic(err)
	}
	return itr
}

// Query implements only key/subspace paths used by base store queries.
func (st *Store) Query(req *types.RequestQuery) (*types.ResponseQuery, error) {
	if req.Prove {
		return nil, fmt.Errorf("proof query is not supported on state store")
	}
	if req.Height > 0 && req.Height > st.version {
		return nil, fmt.Errorf("invalid height: %d", req.Height)
	}
	if _, err := st.reader.Iterator(st.storeKey.Name(), st.version, nil, nil); err != nil {
		return nil, fmt.Errorf("historical version %d is unavailable in state store", st.version)
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
