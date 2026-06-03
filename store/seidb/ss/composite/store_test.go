//go:build pebbledb

package composite

import (
	"testing"

	commonevm "cosmossdk.io/store/seidb/common/evm"
	seidbcfg "cosmossdk.io/store/seidb/config"
	ciavl "cosmossdk.io/store/seidb/sc/sei-iavl"
	ssevm "cosmossdk.io/store/seidb/ss/evm"
	sstypes "cosmossdk.io/store/seidb/ss/types"
	sdkiavl "cosmossdk.io/store/iavl"
	"cosmossdk.io/store/metrics"
	"cosmossdk.io/store/types"
	dbm "github.com/cosmos/cosmos-db"
	"github.com/stretchr/testify/require"
)

func TestCompositeStateStore_CosmosOnly(t *testing.T) {
	cfg := seidbcfg.Config{
		Home:                t.TempDir(),
		StateStoreBackend:   "pebbledb",
		StateStoreWriteMode: "cosmos_only",
		StateStoreReadMode:  "cosmos_only",
	}

	raw, err := NewStateStore(cfg)
	require.NoError(t, err)
	ss := raw.(*compositeStateStore)
	defer ss.Close()

	require.Nil(t, ss.evmStore)
}

func TestCompositeStateStore_DualWriteEVM(t *testing.T) {
	cfg := seidbcfg.Config{
		Home:                        t.TempDir(),
		StateStoreBackend:           "pebbledb",
		StateStoreWriteMode:         "dual_write",
		StateStoreReadMode:          "evm_first",
		StateStoreAsyncWriteBuffer:  0,
		StateStoreEVMDBDirectory:    "",
	}

	raw, err := NewStateStore(cfg)
	require.NoError(t, err)
	ss := raw.(*compositeStateStore)
	defer ss.Close()

	storageKey := commonevm.BuildMemIAVLEVMKey(commonevm.EVMKeyStorage, append(make([]byte, 20), make([]byte, 32)...))
	require.NotNil(t, storageKey)

	changeSets := []*sstypes.NamedChangeSet{
		{
			Name: "bank",
			ChangeSet: &ciavl.ChangeSet{
				Pairs: []*ciavl.KVPair{{Key: []byte("balance"), Value: []byte("1")}},
			},
		},
		{
			Name: "evm",
			ChangeSet: &ciavl.ChangeSet{
				Pairs: []*ciavl.KVPair{{Key: storageKey, Value: []byte("evm-v1")}},
			},
		},
	}

	require.NoError(t, ss.ApplyChangeSets(1, changeSets))

	cosmosVal, err := ss.cosmosStore.Get("evm", 1, storageKey)
	require.NoError(t, err)
	require.Equal(t, []byte("evm-v1"), cosmosVal)

	evmVal, err := ss.evmStore.Get("evm", 1, storageKey)
	require.NoError(t, err)
	require.Equal(t, []byte("evm-v1"), evmVal)
}

func TestCompositeStateStore_SplitWriteRoutesEVMOutOfCosmos(t *testing.T) {
	cfg := seidbcfg.Config{
		Home:                       t.TempDir(),
		StateStoreBackend:          "pebbledb",
		StateStoreWriteMode:        "split_write",
		StateStoreReadMode:         "split_read",
		StateStoreAsyncWriteBuffer: 0,
	}

	raw, err := NewStateStore(cfg)
	require.NoError(t, err)
	ss := raw.(*compositeStateStore)
	defer ss.Close()

	storageKey := commonevm.BuildMemIAVLEVMKey(commonevm.EVMKeyStorage, append(make([]byte, 20), make([]byte, 32)...))
	require.NotNil(t, storageKey)

	changeSets := []*sstypes.NamedChangeSet{
		{
			Name: "bank",
			ChangeSet: &ciavl.ChangeSet{
				Pairs: []*ciavl.KVPair{{Key: []byte("balance"), Value: []byte("1")}},
			},
		},
		{
			Name: "evm",
			ChangeSet: &ciavl.ChangeSet{
				Pairs: []*ciavl.KVPair{{Key: storageKey, Value: []byte("evm-v1")}},
			},
		},
	}

	require.NoError(t, ss.ApplyChangeSets(1, changeSets))

	cosmosVal, err := ss.cosmosStore.Get("evm", 1, storageKey)
	require.NoError(t, err)
	require.Nil(t, cosmosVal)

	evmVal, err := ss.evmStore.Get("evm", 1, storageKey)
	require.NoError(t, err)
	require.Equal(t, []byte("evm-v1"), evmVal)

	readVal, err := ss.Get("evm", 1, storageKey)
	require.NoError(t, err)
	require.Equal(t, []byte("evm-v1"), readVal)
}

func TestCompositeStateStore_EmptyWALIsTreatedAsCleanStart(t *testing.T) {
	cfg := seidbcfg.Config{
		Home:                t.TempDir(),
		StateStoreBackend:   "pebbledb",
		StateStoreWriteMode: "cosmos_only",
		StateStoreReadMode:  "cosmos_only",
	}

	raw, err := NewStateStore(cfg)
	require.NoError(t, err)
	ss := raw.(*compositeStateStore)
	defer ss.Close()

	require.Equal(t, int64(0), ss.LatestVersion())
	require.Equal(t, int64(0), ss.EarliestVersion())
}

func TestCompositeStateStore_RollbackPersistsAcrossReopen(t *testing.T) {
	cfg := seidbcfg.Config{
		Home:                       t.TempDir(),
		StateStoreBackend:          "pebbledb",
		StateStoreWriteMode:        "dual_write",
		StateStoreReadMode:         "evm_first",
		StateStoreAsyncWriteBuffer: 0,
	}

	raw, err := NewStateStore(cfg)
	require.NoError(t, err)
	ss := raw.(*compositeStateStore)

	storageKey := commonevm.BuildMemIAVLEVMKey(commonevm.EVMKeyStorage, append(make([]byte, 20), make([]byte, 32)...))
	changeSetAt := func(v int64) []*sstypes.NamedChangeSet {
		return []*sstypes.NamedChangeSet{
			{
				Name: "bank",
				ChangeSet: &ciavl.ChangeSet{
					Pairs: []*ciavl.KVPair{{Key: []byte("balance"), Value: []byte{byte('0' + v)}}},
				},
			},
			{
				Name: "evm",
				ChangeSet: &ciavl.ChangeSet{
					Pairs: []*ciavl.KVPair{{Key: storageKey, Value: []byte{byte('a' + v)}}},
				},
			},
		}
	}

	for v := int64(1); v <= 3; v++ {
		require.NoError(t, ss.ApplyChangeSets(v, changeSetAt(v)))
	}
	require.NoError(t, ss.RollbackToVersion(2))
	require.NoError(t, ss.Close())

	raw, err = NewStateStore(cfg)
	require.NoError(t, err)
	ss = raw.(*compositeStateStore)
	defer ss.Close()

	require.True(t, ss.HasVersion(2))
	require.False(t, ss.HasVersion(3))

	cosmosVal, err := ss.Get("bank", 2, []byte("balance"))
	require.NoError(t, err)
	require.Equal(t, []byte("2"), cosmosVal)

	evmVal, err := ss.Get("evm", 2, storageKey)
	require.NoError(t, err)
	require.Equal(t, []byte("c"), evmVal)
}

func TestCompositeStateStore_AsyncClosePersistsAcrossReopen(t *testing.T) {
	cfg := seidbcfg.Config{
		Home:                       t.TempDir(),
		StateStoreBackend:          "pebbledb",
		StateStoreWriteMode:        "cosmos_only",
		StateStoreReadMode:         "cosmos_only",
		StateStoreAsyncWriteBuffer: 8,
	}

	raw, err := NewStateStore(cfg)
	require.NoError(t, err)
	ss := raw.(*compositeStateStore)

	for v := int64(1); v <= 3; v++ {
		require.NoError(t, ss.ApplyChangeSets(v, []*sstypes.NamedChangeSet{
			{
				Name: "bank",
				ChangeSet: &ciavl.ChangeSet{
					Pairs: []*ciavl.KVPair{{Key: []byte("balance"), Value: []byte{byte('0' + v)}}},
				},
			},
		}))
	}
	require.NoError(t, ss.Close())

	raw, err = NewStateStore(cfg)
	require.NoError(t, err)
	ss = raw.(*compositeStateStore)
	defer ss.Close()

	val, err := ss.Get("bank", 3, []byte("balance"))
	require.NoError(t, err)
	require.Equal(t, []byte("3"), val)
}

func TestCompositeStateStore_SyncFromStoresPersistsAcrossReopen(t *testing.T) {
	cfg := seidbcfg.Config{
		Home:                       t.TempDir(),
		StateStoreBackend:          "pebbledb",
		StateStoreWriteMode:        "dual_write",
		StateStoreReadMode:         "evm_first",
		StateStoreAsyncWriteBuffer: 0,
	}

	raw, err := NewStateStore(cfg)
	require.NoError(t, err)
	ss := raw.(*compositeStateStore)

	storageKey := commonevm.BuildMemIAVLEVMKey(commonevm.EVMKeyStorage, append(make([]byte, 20), make([]byte, 32)...))
	bankKey := types.NewKVStoreKey("bank")
	evmKey := types.NewKVStoreKey(ssevm.EVMStoreKey)
	bankStore, err := sdkiavl.LoadStore(dbm.NewMemDB(), nil, bankKey, types.CommitID{}, sdkiavl.DefaultIAVLCacheSize, false, metrics.NewNoOpMetrics())
	require.NoError(t, err)
	evmStore, err := sdkiavl.LoadStore(dbm.NewMemDB(), nil, evmKey, types.CommitID{}, sdkiavl.DefaultIAVLCacheSize, false, metrics.NewNoOpMetrics())
	require.NoError(t, err)
	stores := map[types.StoreKey]types.CommitKVStore{
		bankKey: bankStore,
		evmKey:  evmStore,
	}
	bankStore.Set([]byte("balance"), []byte("42"))
	evmStore.Set(storageKey, []byte("evm-sync"))

	require.NoError(t, ss.SyncFromStores(stores, 7))
	require.NoError(t, ss.Close())

	raw, err = NewStateStore(cfg)
	require.NoError(t, err)
	ss = raw.(*compositeStateStore)
	defer ss.Close()

	val, err := ss.Get("bank", 7, []byte("balance"))
	require.NoError(t, err)
	require.Equal(t, []byte("42"), val)

	evmVal, err := ss.Get("evm", 7, storageKey)
	require.NoError(t, err)
	require.Equal(t, []byte("evm-sync"), evmVal)
}
