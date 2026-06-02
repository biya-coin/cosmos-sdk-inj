package server

import (
	"testing"

	"cosmossdk.io/store"
	"github.com/cosmos/cosmos-sdk/client/flags"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestGetStoreConfig_Default(t *testing.T) {
	v := viper.New()
	cfg := GetStoreConfig(v)

	require.Equal(t, store.StoreBackendType(store.StoreBackendIAVL), cfg.Backend)
	require.False(t, cfg.SeiDB.Enabled)
	require.Equal(t, 100, cfg.SeiDB.MemIAVL.AsyncCommitBuffer)
	require.Equal(t, 100, cfg.SeiDB.StateStoreAsyncWriteBuffer)
}

func TestGetStoreConfig_SeiDBEnabledKeepsAsyncBuffers(t *testing.T) {
	v := viper.New()
	v.Set(FlagSeiDBEnabled, true)
	v.Set(FlagSeiDBHome, "/tmp/seidb")

	cfg := GetStoreConfig(v)

	require.True(t, cfg.SeiDB.Enabled)
	require.Equal(t, 100, cfg.SeiDB.MemIAVL.AsyncCommitBuffer)
	require.Equal(t, 100, cfg.SeiDB.StateStoreAsyncWriteBuffer)
}

func TestGetStoreConfig_SeiDBEnabled(t *testing.T) {
	v := viper.New()
	v.Set(FlagSeiDBEnabled, true)
	v.Set(FlagSeiDBHome, "/tmp/seidb")
	v.Set(FlagSeiDBSCBackend, "memiavl")
	v.Set(FlagSeiDBSSBackend, "pebbledb")
	v.Set(FlagSeiDBKeepRecent, uint64(42))

	cfg := GetStoreConfig(v)

	require.Equal(t, store.StoreBackendType(store.StoreBackendSeiDB), cfg.Backend)
	require.True(t, cfg.SeiDB.Enabled)
	require.Equal(t, "/tmp/seidb", cfg.SeiDB.Home)
	require.Equal(t, uint64(42), cfg.SeiDB.KeepRecent)
}

func TestGetStoreConfig_SeiDBSSEnableFalse(t *testing.T) {
	v := viper.New()
	v.Set(FlagSeiDBEnabled, true)
	v.Set(FlagSeiDBHome, "/tmp/seidb")
	v.Set(FlagSeiDBSSEnable, false)
	v.Set(FlagSeiDBSSBackend, "pebbledb")
	v.Set(FlagSeiDBSSAsyncWriteBuffer, 100)

	cfg := GetStoreConfig(v)

	require.True(t, cfg.SeiDB.Enabled)
	require.Equal(t, "", cfg.SeiDB.StateStoreBackend)
	require.Equal(t, 0, cfg.SeiDB.StateStoreAsyncWriteBuffer)
	require.Equal(t, "", cfg.SeiDB.StateStoreWriteMode)
	require.Equal(t, "", cfg.SeiDB.StateStoreReadMode)
}

func TestGetStoreConfig_MemiAVLEnable(t *testing.T) {
	v := viper.New()
	v.Set(store.MemIAVLOptionEnable, true)
	v.Set(store.MemIAVLOptionSnapshotInterval, uint32(1000))
	v.Set(store.MemIAVLOptionAsyncCommitBuffer, 0)

	cfg := GetStoreConfig(v)

	require.Equal(t, store.StoreBackendType(store.StoreBackendSeiDB), cfg.Backend)
	require.True(t, cfg.SeiDB.Enabled)
	require.Equal(t, "memiavl", cfg.SeiDB.StateCommitmentBackend)
	require.Equal(t, uint32(1000), cfg.SeiDB.MemIAVL.SnapshotInterval)
	require.Equal(t, 0, cfg.SeiDB.MemIAVL.AsyncCommitBuffer)
}

func TestGetStoreConfig_SeiDBHomeFallsBackToNodeHome(t *testing.T) {
	v := viper.New()
	v.Set(FlagSeiDBEnabled, true)
	v.Set(flags.FlagHome, "/tmp/node-home")

	cfg := GetStoreConfig(v)
	require.Equal(t, "/tmp/node-home", cfg.SeiDB.Home)
}
