package store

import (
	"testing"

	"cosmossdk.io/log"
	"cosmossdk.io/store/metrics"
	"cosmossdk.io/store/types"
	dbm "github.com/cosmos/cosmos-db"
	"github.com/stretchr/testify/require"
)

func TestNewCommitMultiStoreWithConfig_DefaultIAVL(t *testing.T) {
	db := dbm.NewMemDB()
	ms := NewCommitMultiStoreWithConfig(db, log.NewNopLogger(), metrics.NewNoOpMetrics(), DefaultStoreConfig())
	require.NotNil(t, ms)
}

func TestNewCommitMultiStoreWithConfig_SeiDBSkeleton(t *testing.T) {
	db := dbm.NewMemDB()
	cfg := DefaultStoreConfig()
	cfg.Backend = StoreBackendSeiDB
	cfg.SeiDB.Enabled = true
	cfg.SeiDB.StateStoreBackend = ""

	ms := NewCommitMultiStoreWithConfig(db, log.NewNopLogger(), metrics.NewNoOpMetrics(), cfg)
	require.NotNil(t, ms)

	skeleton, ok := ms.(*seiDBSkeletonStore)
	require.True(t, ok)
	require.Equal(t, StoreBackendType(StoreBackendSeiDB), skeleton.StoreConfig().Backend)
	require.True(t, skeleton.StoreConfig().SeiDB.Enabled)
	_, queryable := interface{}(skeleton).(types.Queryable)
	require.True(t, queryable)
}
