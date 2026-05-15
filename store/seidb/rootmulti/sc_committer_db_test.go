package rootmulti

import (
	"testing"

	ciavl "github.com/cosmos/iavl"
	dbm "github.com/cosmos/cosmos-db"
	sdkiavl "cosmossdk.io/store/iavl"
	"cosmossdk.io/store/metrics"
	"cosmossdk.io/store/types"
	"github.com/stretchr/testify/require"
)

func TestDBSCCommitter_PersistsVersionAndData(t *testing.T) {
	db := dbm.NewMemDB()
	c := newDBSCCommitter(db, 0)

	cs := &ciavl.ChangeSet{
		Pairs: []*ciavl.KVPair{
			{Key: []byte("k1"), Value: []byte("v1")},
			{Key: []byte("k2"), Value: []byte("v2")},
		},
	}
	require.NoError(t, c.ApplyChangeSets([]*NamedChangeSet{
		{Name: "bank", ChangeSet: cs},
	}))
	require.NoError(t, c.Commit(1))

	latest, err := db.Get([]byte(scLatestVersionKey))
	require.NoError(t, err)
	require.Equal(t, uint64(1), bytesToUint64(latest))

	v, err := db.Get(scDataKey("bank", []byte("k1")))
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), v)

	// Reload should pick up persisted version.
	reloaded := newDBSCCommitter(db, 0)
	require.Equal(t, int64(1), reloaded.version)
	require.True(t, reloaded.HasVersion(1))
}

func TestDBSCCommitter_RejectsNonMonotonicVersion(t *testing.T) {
	db := dbm.NewMemDB()
	c := newDBSCCommitter(db, 0)
	require.NoError(t, c.Commit(1))
	require.Error(t, c.Commit(1))
	require.Error(t, c.Commit(0))
}

func TestDBSCCommitter_KeepRecentPrunesHistory(t *testing.T) {
	db := dbm.NewMemDB()
	c := newDBSCCommitter(db, 2)

	for i := int64(1); i <= 3; i++ {
		cs := &ciavl.ChangeSet{Pairs: []*ciavl.KVPair{{Key: []byte("k"), Value: []byte{byte('0' + i)}}}}
		require.NoError(t, c.ApplyChangeSets([]*NamedChangeSet{{Name: "bank", ChangeSet: cs}}))
		require.NoError(t, c.Commit(i))
	}

	require.False(t, c.HasVersion(1))
	require.True(t, c.HasVersion(2))
	require.True(t, c.HasVersion(3))
	require.Equal(t, int64(2), c.EarliestVersion())
}

func TestDBSCCommitter_RollbackToVersion(t *testing.T) {
	db := dbm.NewMemDB()
	c := newDBSCCommitter(db, 0)

	for i := int64(1); i <= 3; i++ {
		cs := &ciavl.ChangeSet{Pairs: []*ciavl.KVPair{{Key: []byte("k"), Value: []byte{byte('0' + i)}}}}
		require.NoError(t, c.ApplyChangeSets([]*NamedChangeSet{{Name: "bank", ChangeSet: cs}}))
		require.NoError(t, c.Commit(i))
	}

	require.NoError(t, c.RollbackToVersion(2))
	require.Equal(t, int64(2), c.version)
	require.True(t, c.HasVersion(2))
	require.False(t, c.HasVersion(3))

	snap, ok := c.Snapshot("bank", 2)
	require.True(t, ok)
	require.Equal(t, []byte("2"), snap["k"])
}

func TestDBSCCommitter_SyncFromStores(t *testing.T) {
	db := dbm.NewMemDB()
	c := newDBSCCommitter(db, 0)

	key := types.NewKVStoreKey("bank")
	store, err := sdkiavl.LoadStore(dbm.NewMemDB(), nil, key, types.CommitID{}, sdkiavl.DefaultIAVLCacheSize, false, metrics.NewNoOpMetrics())
	require.NoError(t, err)
	store.Set([]byte("a"), []byte("1"))
	store.Set([]byte("b"), []byte("2"))

	err = c.SyncFromStores(map[types.StoreKey]types.CommitKVStore{
		key: store,
	}, 5)
	require.NoError(t, err)
	require.True(t, c.HasVersion(5))
	require.Equal(t, int64(5), c.EarliestVersion())

	snap, ok := c.Snapshot("bank", 5)
	require.True(t, ok)
	require.Equal(t, []byte("1"), snap["a"])
	require.Equal(t, []byte("2"), snap["b"])
}

func TestDBSCCommitter_BinarySafeSCDataKeyRoundTrip(t *testing.T) {
	db := dbm.NewMemDB()
	c := newDBSCCommitter(db, 0)

	rawKey := []byte{'a', '/', 0x00, 'b'}
	cs := &ciavl.ChangeSet{
		Pairs: []*ciavl.KVPair{
			{Key: rawKey, Value: []byte("v")},
		},
	}
	require.NoError(t, c.ApplyChangeSets([]*NamedChangeSet{{Name: "bank", ChangeSet: cs}}))
	require.NoError(t, c.Commit(1))

	reloaded := newDBSCCommitter(db, 0)
	snap, ok := reloaded.Snapshot("bank", 1)
	require.True(t, ok)
	require.Equal(t, []byte("v"), snap[string(rawKey)])
}

func TestDBSCCommitter_CommitAcceptsNilValue(t *testing.T) {
	db := dbm.NewMemDB()
	c := newDBSCCommitter(db, 0)

	cs := &ciavl.ChangeSet{
		Pairs: []*ciavl.KVPair{
			{Key: []byte("k-nil"), Value: nil},
		},
	}
	require.NoError(t, c.ApplyChangeSets([]*NamedChangeSet{{Name: "bank", ChangeSet: cs}}))
	require.NoError(t, c.Commit(1))

	stored, err := db.Get(scDataKey("bank", []byte("k-nil")))
	require.NoError(t, err)
	require.Equal(t, []byte{}, stored)

	reloaded := newDBSCCommitter(db, 0)
	require.True(t, reloaded.HasVersion(1))
	snap, ok := reloaded.Snapshot("bank", 1)
	require.True(t, ok)
	_, _ = snap["k-nil"]
}

func TestDBSCCommitter_CommitAcceptsEmptyNonNilValue(t *testing.T) {
	db := dbm.NewMemDB()
	c := newDBSCCommitter(db, 0)

	cs := &ciavl.ChangeSet{
		Pairs: []*ciavl.KVPair{
			{Key: []byte("k-empty"), Value: []byte{}},
		},
	}
	require.NoError(t, c.ApplyChangeSets([]*NamedChangeSet{{Name: "bank", ChangeSet: cs}}))
	require.NoError(t, c.Commit(1))

	stored, err := db.Get(scDataKey("bank", []byte("k-empty")))
	require.NoError(t, err)
	require.NotNil(t, stored)
	require.Equal(t, []byte{}, stored)
}
