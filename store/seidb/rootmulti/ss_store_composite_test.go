//go:build pebbledb

package rootmulti

import (
	"testing"

	commonevm "cosmossdk.io/store/seidb/common/evm"
	ciavl "cosmossdk.io/store/seidb/sc/sei-iavl"
	"github.com/stretchr/testify/require"
)

func TestCompositeStateStore_CosmosOnly(t *testing.T) {
	cfg := Config{
		Home:               t.TempDir(),
		StateStoreBackend:  "pebbledb",
		StateStoreWriteMode: "cosmos_only",
		StateStoreReadMode:  "cosmos_only",
	}

	raw, err := newCompositeStateStore(cfg)
	require.NoError(t, err)
	ss := raw.(*compositeStateStore)
	defer ss.Close()

	require.Nil(t, ss.evmStore)
}

func TestCompositeStateStore_DualWriteEVM(t *testing.T) {
	cfg := Config{
		Home:                        t.TempDir(),
		StateStoreBackend:           "pebbledb",
		StateStoreWriteMode:         "dual_write",
		StateStoreReadMode:          "evm_first",
		StateStoreAsyncWriteBuffer:  0,
		StateStoreEVMDBDirectory:    "",
	}

	raw, err := newCompositeStateStore(cfg)
	require.NoError(t, err)
	ss := raw.(*compositeStateStore)
	defer ss.Close()

	storageKey := commonevm.BuildMemIAVLEVMKey(commonevm.EVMKeyStorage, append(make([]byte, 20), make([]byte, 32)...))
	require.NotNil(t, storageKey)

	changeSets := []*NamedChangeSet{
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
	cfg := Config{
		Home:                       t.TempDir(),
		StateStoreBackend:          "pebbledb",
		StateStoreWriteMode:        "split_write",
		StateStoreReadMode:         "split_read",
		StateStoreAsyncWriteBuffer: 0,
	}

	raw, err := newCompositeStateStore(cfg)
	require.NoError(t, err)
	ss := raw.(*compositeStateStore)
	defer ss.Close()

	storageKey := commonevm.BuildMemIAVLEVMKey(commonevm.EVMKeyStorage, append(make([]byte, 20), make([]byte, 32)...))
	require.NotNil(t, storageKey)

	changeSets := []*NamedChangeSet{
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
