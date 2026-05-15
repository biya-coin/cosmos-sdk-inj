package state

import (
	"testing"

	"cosmossdk.io/store/internal/kv"
	"cosmossdk.io/store/types"
	"github.com/stretchr/testify/require"
)

type mockSnapshotReader struct {
	data map[int64]map[string]map[string][]byte
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
