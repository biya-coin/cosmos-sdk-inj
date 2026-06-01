//go:build pebbledb

package mvcc

import (
	"testing"

	ciavl "cosmossdk.io/store/seidb/sc/sei-iavl"
	"github.com/stretchr/testify/require"
)

func TestPebbleStateStore_PersistAndReload(t *testing.T) {
	home := t.TempDir()
	cfg := Config{
		Home:       home,
		KeepRecent: 0,
	}

	ss, err := NewStore(cfg)
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

	reloaded, err := NewStore(cfg)
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

	ss, err := NewStore(cfg)
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

	ss, err := NewStore(cfg)
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

// TestSSWALRecovery verifies that after a simulated crash (Close without flushing), reopening
// the store replays WAL entries and restores the latest committed state.
// Mirrors sei-db/state_db/ss/composite/recovery_test.go.
func TestSSWALRecovery(t *testing.T) {
	home := t.TempDir()
	cfg := Config{Home: home, KeepRecent: 0}

	// First run: commit v1..v3 then close cleanly.
	ss, err := NewStore(cfg)
	require.NoError(t, err)

	values := map[int64][]byte{1: []byte("v1"), 2: []byte("v2"), 3: []byte("v3")}
	for v := int64(1); v <= 3; v++ {
		require.NoError(t, ss.ApplyChangeSets(v, []*NamedChangeSet{
			{
				Name: "bank",
				ChangeSet: &ciavl.ChangeSet{
					Pairs: []*ciavl.KVPair{{Key: []byte("k"), Value: values[v]}},
				},
			},
		}))
	}
	require.NoError(t, ss.Close())

	// Simulate crash: reopen and verify WAL replay restores state.
	reloaded, err := NewStore(cfg)
	require.NoError(t, err)
	defer reloaded.Close()

	require.True(t, reloaded.HasVersion(1))
	require.True(t, reloaded.HasVersion(2))
	require.True(t, reloaded.HasVersion(3))

	for v := int64(1); v <= 3; v++ {
		snap, ok := reloaded.Snapshot("bank", v)
		require.True(t, ok, "version %d should exist after WAL recovery", v)
		require.Equal(t, values[v], snap["k"], "unexpected value at version %d", v)
	}
}

// TestSSWALRollback verifies that RollbackToVersion also truncates the WAL, so that
// after rollback + reopen, entries beyond the target version are gone.
func TestSSWALRollback(t *testing.T) {
	home := t.TempDir()
	cfg := Config{Home: home, KeepRecent: 0}

	ss, err := NewStore(cfg)
	require.NoError(t, err)

	for v := int64(1); v <= 5; v++ {
		require.NoError(t, ss.ApplyChangeSets(v, []*NamedChangeSet{
			{
				Name: "bank",
				ChangeSet: &ciavl.ChangeSet{
					Pairs: []*ciavl.KVPair{{Key: []byte("k"), Value: []byte{byte('0' + v)}}},
				},
			},
		}))
	}

	require.NoError(t, ss.RollbackToVersion(3))
	require.True(t, ss.HasVersion(3))
	require.False(t, ss.HasVersion(4))
	require.False(t, ss.HasVersion(5))
	require.NoError(t, ss.Close())

	// Reopen: WAL should be truncated at v3, so recovery must not replay v4/v5.
	reloaded, err := NewStore(cfg)
	require.NoError(t, err)
	defer reloaded.Close()

	require.True(t, reloaded.HasVersion(3), "v3 must survive rollback")
	require.False(t, reloaded.HasVersion(4), "v4 must be gone after WAL truncation")
	require.False(t, reloaded.HasVersion(5), "v5 must be gone after WAL truncation")

	snap, ok := reloaded.Snapshot("bank", 3)
	require.True(t, ok)
	require.Equal(t, []byte("3"), snap["k"])
}

func TestPebbleStateStore_MVCCGetAndDelete(t *testing.T) {
	home := t.TempDir()
	cfg := Config{Home: home, KeepRecent: 0}

	raw, err := NewStore(cfg)
	require.NoError(t, err)
	ss := raw.(*pebbleStateStore)
	defer ss.Close()

	require.NoError(t, ss.ApplyChangeSets(1, []*NamedChangeSet{
		{
			Name: "bank",
			ChangeSet: &ciavl.ChangeSet{
				Pairs: []*ciavl.KVPair{{Key: []byte("k"), Value: []byte("v1")}},
			},
		},
	}))
	require.NoError(t, ss.ApplyChangeSets(2, []*NamedChangeSet{
		{
			Name: "bank",
			ChangeSet: &ciavl.ChangeSet{
				Pairs: []*ciavl.KVPair{{Key: []byte("k"), Value: []byte("v2")}},
			},
		},
	}))
	require.NoError(t, ss.ApplyChangeSets(3, []*NamedChangeSet{
		{
			Name: "bank",
			ChangeSet: &ciavl.ChangeSet{
				Pairs: []*ciavl.KVPair{{Key: []byte("k"), Delete: true}},
			},
		},
	}))

	v1, err := ss.Get("bank", 1, []byte("k"))
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), v1)

	v2, err := ss.Get("bank", 2, []byte("k"))
	require.NoError(t, err)
	require.Equal(t, []byte("v2"), v2)

	v3, err := ss.Get("bank", 3, []byte("k"))
	require.NoError(t, err)
	require.Nil(t, v3)
}

func TestPebbleStateStore_MVCCIterators(t *testing.T) {
	home := t.TempDir()
	cfg := Config{Home: home, KeepRecent: 0}

	raw, err := NewStore(cfg)
	require.NoError(t, err)
	ss := raw.(*pebbleStateStore)
	defer ss.Close()

	require.NoError(t, ss.ApplyChangeSets(1, []*NamedChangeSet{
		{
			Name: "bank",
			ChangeSet: &ciavl.ChangeSet{
				Pairs: []*ciavl.KVPair{
					{Key: []byte("a"), Value: []byte("1")},
					{Key: []byte("b"), Value: []byte("2")},
				},
			},
		},
	}))
	require.NoError(t, ss.ApplyChangeSets(2, []*NamedChangeSet{
		{
			Name: "bank",
			ChangeSet: &ciavl.ChangeSet{
				Pairs: []*ciavl.KVPair{
					{Key: []byte("a"), Value: []byte("1x")},
					{Key: []byte("c"), Value: []byte("3")},
				},
			},
		},
	}))

	itr, err := ss.Iterator("bank", 2, []byte("a"), []byte("d"))
	require.NoError(t, err)
	defer itr.Close()

	var keys []string
	var vals []string
	for ; itr.Valid(); itr.Next() {
		keys = append(keys, string(itr.Key()))
		vals = append(vals, string(itr.Value()))
	}
	require.NoError(t, itr.Error())
	require.Equal(t, []string{"a", "b", "c"}, keys)
	require.Equal(t, []string{"1x", "2", "3"}, vals)

	ritr, err := ss.ReverseIterator("bank", 2, []byte("a"), []byte("d"))
	require.NoError(t, err)
	defer ritr.Close()

	keys = nil
	vals = nil
	for ; ritr.Valid(); ritr.Next() {
		keys = append(keys, string(ritr.Key()))
		vals = append(vals, string(ritr.Value()))
	}
	require.NoError(t, ritr.Error())
	require.Equal(t, []string{"c", "b", "a"}, keys)
	require.Equal(t, []string{"3", "2", "1x"}, vals)
}

func TestPebbleStateStore_AsyncApplyFlushesOnClose(t *testing.T) {
	home := t.TempDir()
	cfg := Config{
		Home:                       home,
		KeepRecent:                 0,
		StateStoreAsyncWriteBuffer: 8,
	}

	raw, err := NewStore(cfg)
	require.NoError(t, err)
	ss := raw.(*pebbleStateStore)

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

	require.NoError(t, ss.Close())

	reloadedRaw, err := NewStore(cfg)
	require.NoError(t, err)
	reloaded := reloadedRaw.(*pebbleStateStore)
	defer reloaded.Close()

	val, err := reloaded.Get("bank", 3, []byte("k"))
	require.NoError(t, err)
	require.Equal(t, []byte("3"), val)
}
