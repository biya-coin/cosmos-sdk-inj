package rootmulti

import (
	"fmt"
	"sync"

	dbm "github.com/cosmos/cosmos-db"
)

// SCStore is the state-commitment store surface used by rootmulti, aligned with
// sei-cosmos storev2's scStore (sctypes.Committer) wiring.
type SCStore interface {
	ApplyChangeSets([]*NamedChangeSet) error
	Commit(version int64) error
}

type scStoreBuilder func(db dbm.DB, cfg Config) (SCStore, error)

// scStoreBuilders holds optional SC backends (default: memiavl), keyed by backend name.
var scStoreBuilders sync.Map

func init() {
	if err := RegisterSCStoreBuilder("memiavl", func(_ dbm.DB, cfg Config) (SCStore, error) {
		if cfg.Home == "" {
			return nil, fmt.Errorf("memiavl requires seidb home path")
		}
		return newMemIAVLStore(cfg.Home, cfg.MemIAVL), nil
	}); err != nil {
		panic(err)
	}
}

// RegisterSCStoreBuilder registers a state-commitment store factory by backend name.
// This mirrors sei-chain's direct construction pattern while allowing test/extension hooks.
func RegisterSCStoreBuilder(backend string, builder scStoreBuilder) error {
	if backend == "" {
		return fmt.Errorf("backend must not be empty")
	}
	if builder == nil {
		return fmt.Errorf("builder must not be nil")
	}
	scStoreBuilders.Store(backend, builder)
	return nil
}

func newSCStoreFromConfig(db dbm.DB, cfg Config) SCStore {
	backend := cfg.StateCommitmentBackend
	if backend == "" {
		backend = "memiavl"
	}

	value, ok := scStoreBuilders.Load(backend)
	if !ok {
		return noopSCStore{}
	}

	builder, ok := value.(scStoreBuilder)
	if !ok {
		return noopSCStore{}
	}

	store, err := builder(db, cfg)
	if err != nil || store == nil {
		return noopSCStore{}
	}
	return store
}

type noopSCStore struct{}

func (noopSCStore) ApplyChangeSets(_ []*NamedChangeSet) error { return nil }
func (noopSCStore) Commit(_ int64) error                      { return nil }
