package commitment

import (
	"testing"

	"cosmossdk.io/store/mem"
	"cosmossdk.io/store/types"
	"github.com/stretchr/testify/require"
)

func TestStoreTracksChangeSet(t *testing.T) {
	base := mem.NewStore()
	store := LegacyNewStore(types.NewKVStoreKey("test"), base)

	store.Set([]byte("a"), []byte("1"))
	store.Set([]byte("b"), []byte("2"))
	store.Delete([]byte("a"))

	cs := store.PopChangeSet()
	require.NotNil(t, cs)
	require.Len(t, cs.Pairs, 3)
	require.Equal(t, []byte("a"), cs.Pairs[0].Key)
	require.Equal(t, []byte("1"), cs.Pairs[0].Value)
	require.Equal(t, []byte("b"), cs.Pairs[1].Key)
	require.True(t, cs.Pairs[2].Delete)

	// ensure pop is destructive
	empty := store.PopChangeSet()
	require.NotNil(t, empty)
	require.Len(t, empty.Pairs, 0)
}
