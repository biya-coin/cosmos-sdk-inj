package state

import (
	"sort"
	"testing"

	"cosmossdk.io/store/internal/kv"
	"cosmossdk.io/store/types"
	"github.com/stretchr/testify/require"
)

type mockSnapshotReader struct {
	data map[int64]map[string]map[string][]byte
}

func (m mockSnapshotReader) Get(storeName string, version int64, key []byte) ([]byte, error) {
	snap, ok := m.Snapshot(storeName, version)
	if !ok {
		return nil, nil
	}
	return append([]byte(nil), snap[string(key)]...), nil
}

func (m mockSnapshotReader) Has(storeName string, version int64, key []byte) (bool, error) {
	snap, ok := m.Snapshot(storeName, version)
	if !ok {
		return false, nil
	}
	_, found := snap[string(key)]
	return found, nil
}

func (m mockSnapshotReader) Iterator(storeName string, version int64, start, end []byte) (types.Iterator, error) {
	snap, ok := m.Snapshot(storeName, version)
	if !ok {
		return emptyIterator{}, nil
	}
	return newMapIterator(snap, start, end, true), nil
}

func (m mockSnapshotReader) ReverseIterator(storeName string, version int64, start, end []byte) (types.Iterator, error) {
	snap, ok := m.Snapshot(storeName, version)
	if !ok {
		return emptyIterator{}, nil
	}
	return newMapIterator(snap, start, end, false), nil
}

func (m mockSnapshotReader) Snapshot(storeName string, version int64) (map[string][]byte, bool) {
	byVersion, ok := m.data[version]
	if !ok {
		return nil, false
	}
	storeData, ok := byVersion[storeName]
	if !ok {
		return nil, false
	}
	out := make(map[string][]byte, len(storeData))
	for k, v := range storeData {
		out[k] = append([]byte(nil), v...)
	}
	return out, true
}

type emptyIterator struct{}

func (emptyIterator) Domain() ([]byte, []byte) { return nil, nil }
func (emptyIterator) Valid() bool              { return false }
func (emptyIterator) Next()                    {}
func (emptyIterator) Key() []byte              { panic("invalid iterator") }
func (emptyIterator) Value() []byte            { panic("invalid iterator") }
func (emptyIterator) Error() error             { return nil }
func (emptyIterator) Close() error             { return nil }

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

func TestQuerySubspace(t *testing.T) {
	reader := mockSnapshotReader{
		data: map[int64]map[string]map[string][]byte{
			7: {
				"bank": {
					"a/1": []byte("v1"),
					"a/2": []byte("v2"),
					"b/1": []byte("v3"),
				},
			},
		},
	}
	st := NewStore(reader, types.NewKVStoreKey("bank"), 7)

	resp, err := st.Query(&types.RequestQuery{
		Path: "/subspace",
		Data: []byte("a/"),
	})
	require.NoError(t, err)
	require.Equal(t, int64(7), resp.Height)

	decoded := kv.Pairs{}
	require.NoError(t, decoded.Unmarshal(resp.Value))
	require.Len(t, decoded.Pairs, 2)
	require.Equal(t, []byte("a/1"), decoded.Pairs[0].Key)
	require.Equal(t, []byte("v1"), decoded.Pairs[0].Value)
	require.Equal(t, []byte("a/2"), decoded.Pairs[1].Key)
	require.Equal(t, []byte("v2"), decoded.Pairs[1].Value)
}

func TestQueryRejectsProof(t *testing.T) {
	reader := mockSnapshotReader{
		data: map[int64]map[string]map[string][]byte{
			1: {"bank": {"a": []byte("v")}},
		},
	}
	st := NewStore(reader, types.NewKVStoreKey("bank"), 1)

	_, err := st.Query(&types.RequestQuery{
		Path:  "/key",
		Data:  []byte("a"),
		Prove: true,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "proof query is not supported")
}
