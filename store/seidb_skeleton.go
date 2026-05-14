package store

import (
	dbm "github.com/cosmos/cosmos-db"

	"cosmossdk.io/log"
	"cosmossdk.io/store/metrics"
	"cosmossdk.io/store/rootmulti"
	"cosmossdk.io/store/types"
)

type seiDBSkeletonStore struct {
	types.CommitMultiStore
	config StoreConfig
}

func newSeiDBSkeletonStore(db dbm.DB, logger log.Logger, metricGatherer metrics.StoreMetrics, cfg StoreConfig) types.CommitMultiStore {
	return &seiDBSkeletonStore{
		CommitMultiStore: rootmulti.NewStore(db, logger, metricGatherer),
		config:           cfg.Normalize(),
	}
}

func (s *seiDBSkeletonStore) StoreConfig() StoreConfig {
	return s.config
}

