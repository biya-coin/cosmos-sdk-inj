package store

import (
	dbm "github.com/cosmos/cosmos-db"

	"cosmossdk.io/log"
	"cosmossdk.io/store/cache"
	"cosmossdk.io/store/metrics"
	"cosmossdk.io/store/rootmulti"
	"cosmossdk.io/store/types"
)

func NewCommitMultiStore(db dbm.DB, logger log.Logger, metricGatherer metrics.StoreMetrics) types.CommitMultiStore {
	return NewCommitMultiStoreWithConfig(db, logger, metricGatherer, DefaultStoreConfig())
}

func NewIAVLCommitMultiStore(db dbm.DB, logger log.Logger, metricGatherer metrics.StoreMetrics) types.CommitMultiStore {
	return rootmulti.NewStore(db, logger, metricGatherer)
}

func NewCommitMultiStoreWithConfig(db dbm.DB, logger log.Logger, metricGatherer metrics.StoreMetrics, cfg StoreConfig) types.CommitMultiStore {
	return cfg.NewCommitMultiStore(db, logger, metricGatherer)
}

func NewCommitKVStoreCacheManager() types.MultiStorePersistentCache {
	return cache.NewCommitKVStoreCacheManager(cache.DefaultCommitKVStoreCacheSize)
}
