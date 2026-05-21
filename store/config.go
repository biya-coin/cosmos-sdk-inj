package store

import (
	dbm "github.com/cosmos/cosmos-db"

	"cosmossdk.io/log"
	"cosmossdk.io/store/metrics"
	"cosmossdk.io/store/seidb/sc/memiavl"
	"cosmossdk.io/store/types"
)

const (
	StoreBackendIAVL  = "iavl"
	StoreBackendSeiDB = "seidb"
)

// StoreConfig is the minimal storage skeleton used to stabilize the app/store
// interface before the full SeiDB implementation is completed.
//
// In this skeleton phase, all backends still delegate to the current rootmulti
// implementation so the chain remains runnable while A/B work in parallel.
type StoreConfig struct {
	Backend StoreBackendType
	SeiDB   SeiDBConfig
}

type StoreBackendType string

type SeiDBConfig struct {
	Enabled                            bool
	Home                               string
	StateCommitmentBackend             string
	StateStoreBackend                  string
	KeepRecent                         uint64
	HistoricalProofQueryMaxConcurrency uint32
	MemIAVL                            memiavl.Config
}

func DefaultStoreConfig() StoreConfig {
	return StoreConfig{
		Backend: StoreBackendIAVL,
		SeiDB: SeiDBConfig{
			Enabled:                            false,
			StateCommitmentBackend:             "memiavl",
			StateStoreBackend:                  "pebbledb",
			KeepRecent:                         0,
			HistoricalProofQueryMaxConcurrency: 0,
			MemIAVL:                            memiavl.DefaultConfig(),
		},
	}
}

func (c StoreConfig) Normalize() StoreConfig {
	cfg := c
	if cfg.Backend == "" {
		cfg.Backend = StoreBackendIAVL
	}
	if cfg.Backend != StoreBackendSeiDB {
		cfg.SeiDB.Enabled = false
	}
	return cfg
}

func (c StoreConfig) NewCommitMultiStore(db dbm.DB, logger log.Logger, metricGatherer metrics.StoreMetrics) types.CommitMultiStore {
	cfg := c.Normalize()
	switch cfg.Backend {
	case StoreBackendSeiDB:
		// Skeleton phase: keep runtime behavior on rootmulti while exposing a
		// stable contract and feature flag surface for parallel development.
		return newSeiDBSkeletonStore(db, logger, metricGatherer, cfg)
	case StoreBackendIAVL:
		fallthrough
	default:
		return NewIAVLCommitMultiStore(db, logger, metricGatherer)
	}
}

