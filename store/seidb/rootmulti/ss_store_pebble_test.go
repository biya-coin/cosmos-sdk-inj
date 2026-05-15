//go:build pebbledb

package rootmulti

import (
	"testing"

	ciavl "github.com/cosmos/iavl"
	"github.com/stretchr/testify/require"
)

func TestPebbleStateStore_PersistAndReload(t *testing.T) {
	home := t.TempDir()
	cfg := Config{
		Home:       home,
		KeepRecent: 0,
	}

	ss, err := newPebbleStateStore(cfg)
	require.NoError(t, err)
	require.NoError(t, ss.ApplyChangeSets(1, []*NamedChangeSet{
		{
			Name: "bank",
			ChangeSet: &ciavl.ChangeSet{
				Pairs: []*ciavl.KVPair{{Key: []byte("k"), Value: []byte("v1")}},
			},
		},
	}))
	require.NoError(t, ss.SetLatestVersion(2))
	require.NoError(t, ss.SetLatestVersion(3))
	require.True(t, ss.HasVersion(1))
	require.True(t, ss.HasVersion(2))
	require.True(t, ss.HasVersion(3))
	require.Equal(t, int64(1), ss.EarliestVersion())

	snap1, ok := ss.Snapshot("bank", 1)
	require.True(t, ok)
	require.Equal(t, []byte("v1"), snap1["k"])

	snap2, ok := ss.Snapshot("bank", 2)
	require.True(t, ok)
	require.Equal(t, []byte("v1"), snap2["k"])
	snap3, ok := ss.Snapshot("bank", 3)
	require.True(t, ok)
	require.Equal(t, []byte("v1"), snap3["k"])
	require.NoError(t, ss.Close())

	reloaded, err := newPebbleStateStore(cfg)
	require.NoError(t, err)
	require.True(t, reloaded.HasVersion(1))
	require.True(t, reloaded.HasVersion(2))
	require.True(t, reloaded.HasVersion(3))
	require.Equal(t, int64(1), reloaded.EarliestVersion())
	snapReloaded, ok := reloaded.Snapshot("bank", 3)
	require.True(t, ok)
	require.Equal(t, []byte("v1"), snapReloaded["k"])
	require.NoError(t, reloaded.Close())
}

func TestPebbleStateStore_KeepRecentPrunes(t *testing.T) {
	home := t.TempDir()
	cfg := Config{
		Home:       home,
		KeepRecent: 2,
	}

	ss, err := newPebbleStateStore(cfg)
	require.NoError(t, err)
	defer ss.Close()

	for v := int64(1); v <= 3; v++ {
		require.NoError(t, ss.ApplyChangeSets(v, []*NamedChangeSet{
			{
				Name: "bank",
				ChangeSet: &ciavl.ChangeSet{
					Pairs: []*ciavl.KVPair{{Key: []byte("k"), Value: []byte{byte('0' + v)}}},
				},
			},
		}))
	}

	require.False(t, ss.HasVersion(1))
	require.True(t, ss.HasVersion(2))
	require.True(t, ss.HasVersion(3))
	require.Equal(t, int64(2), ss.EarliestVersion())
}

func TestPebbleStateStore_Rollback(t *testing.T) {
	home := t.TempDir()
	cfg := Config{
		Home:       home,
		KeepRecent: 0,
	}

	ss, err := newPebbleStateStore(cfg)
	require.NoError(t, err)
	defer ss.Close()

	for v := int64(1); v <= 3; v++ {
		require.NoError(t, ss.ApplyChangeSets(v, []*NamedChangeSet{
			{
				Name: "bank",
				ChangeSet: &ciavl.ChangeSet{
					Pairs: []*ciavl.KVPair{{Key: []byte("k"), Value: []byte{byte('0' + v)}}},
				},
			},
		}))
	}

	require.NoError(t, ss.RollbackToVersion(2))
	require.True(t, ss.HasVersion(2))
	require.False(t, ss.HasVersion(3))
	snap, ok := ss.Snapshot("bank", 2)
	require.True(t, ok)
	require.Equal(t, []byte("2"), snap["k"])
}
