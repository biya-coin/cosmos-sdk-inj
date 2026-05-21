package store

import (
	"fmt"

	dbm "github.com/cosmos/cosmos-db"

	"cosmossdk.io/log"
	"cosmossdk.io/store/metrics"
	seidbrootmulti "cosmossdk.io/store/seidb/rootmulti"
	"cosmossdk.io/store/types"
)

type seiDBSkeletonStore struct {
	types.CommitMultiStore
	config StoreConfig
}

func newSeiDBSkeletonStore(db dbm.DB, logger log.Logger, metricGatherer metrics.StoreMetrics, cfg StoreConfig) types.CommitMultiStore {
	normalized := cfg.Normalize()
	return &seiDBSkeletonStore{
		CommitMultiStore: seidbrootmulti.NewStore(db, logger, metricGatherer, seidbrootmulti.Config{
			Home:                               normalized.SeiDB.Home,
			StateCommitmentBackend:             normalized.SeiDB.StateCommitmentBackend,
			StateStoreBackend:                  normalized.SeiDB.StateStoreBackend,
			KeepRecent:                         normalized.SeiDB.KeepRecent,
			HistoricalProofQueryMaxConcurrency: normalized.SeiDB.HistoricalProofQueryMaxConcurrency,
			MemIAVL:                            normalized.SeiDB.MemIAVL,
		}),
		config: normalized,
	}
}

func (s *seiDBSkeletonStore) StoreConfig() StoreConfig {
	return s.config
}

func (s *seiDBSkeletonStore) Query(req *types.RequestQuery) (*types.ResponseQuery, error) {
	queryable, ok := s.CommitMultiStore.(types.Queryable)
	if !ok {
		return nil, fmt.Errorf("multi-store does not support queries")
	}
	return queryable.Query(req)
}

