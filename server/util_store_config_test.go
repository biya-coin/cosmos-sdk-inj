package server

import (
	"testing"

	"cosmossdk.io/store"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestGetStoreConfig_Default(t *testing.T) {
	v := viper.New()
	cfg := GetStoreConfig(v)

	require.Equal(t, store.StoreBackendType(store.StoreBackendIAVL), cfg.Backend)
	require.False(t, cfg.SeiDB.Enabled)
}

func TestGetStoreConfig_SeiDBEnabled(t *testing.T) {
	v := viper.New()
	v.Set(FlagSeiDBEnabled, true)
	v.Set(FlagSeiDBHome, "/tmp/seidb")
	v.Set(FlagSeiDBSCBackend, "memiavl")
	v.Set(FlagSeiDBSSBackend, "pebbledb")
	v.Set(FlagSeiDBKeepRecent, uint64(42))
	v.Set(FlagSeiDBAsyncCommit, true)

	cfg := GetStoreConfig(v)

	require.Equal(t, store.StoreBackendType(store.StoreBackendSeiDB), cfg.Backend)
	require.True(t, cfg.SeiDB.Enabled)
	require.Equal(t, "/tmp/seidb", cfg.SeiDB.Home)
	require.Equal(t, uint64(42), cfg.SeiDB.KeepRecent)
	require.True(t, cfg.SeiDB.AsyncCommit)
}
