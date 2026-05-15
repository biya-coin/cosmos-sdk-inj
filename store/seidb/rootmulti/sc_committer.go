package rootmulti

import (
	"fmt"
	"sync"

	dbm "github.com/cosmos/cosmos-db"
)

type memorySCCommitter struct {
	mtx     sync.Mutex
	version int64
	data    map[string]map[string][]byte
}

func newMemorySCCommitter() *memorySCCommitter {
	return &memorySCCommitter{
		data: make(map[string]map[string][]byte),
	}
}

func (m *memorySCCommitter) ApplyChangeSets(changeSets []*NamedChangeSet) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	for _, named := range changeSets {
		if named == nil || named.ChangeSet == nil {
			continue
		}
		if _, ok := m.data[named.Name]; !ok {
			m.data[named.Name] = make(map[string][]byte)
		}
		moduleStore := m.data[named.Name]
		for _, pair := range named.ChangeSet.Pairs {
			if pair == nil {
				continue
			}
			key := string(pair.Key)
			if pair.Delete {
				delete(moduleStore, key)
				continue
			}
			moduleStore[key] = append([]byte(nil), pair.Value...)
		}
	}
	return nil
}

func (m *memorySCCommitter) Commit(version int64) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	if version <= 0 {
		return fmt.Errorf("invalid commit version: %d", version)
	}
	if version <= m.version {
		return fmt.Errorf("non-monotonic commit version: current=%d next=%d", m.version, version)
	}
	m.version = version
	return nil
}

type scCommitterBuilder func(db dbm.DB, cfg Config) (SCCommitter, error)

var (
	scBuilderMu sync.RWMutex
	scBuilders  = map[string]scCommitterBuilder{
		"memiavl": func(db dbm.DB, cfg Config) (SCCommitter, error) {
			return newDBSCCommitter(db, cfg.KeepRecent), nil
		},
	}
)

// RegisterSCCommitterBuilder allows external packages to plug custom
// state-commitment implementations (e.g. composite adapter) by backend name.
func RegisterSCCommitterBuilder(backend string, builder scCommitterBuilder) error {
	if backend == "" {
		return fmt.Errorf("backend must not be empty")
	}
	if builder == nil {
		return fmt.Errorf("builder must not be nil")
	}

	scBuilderMu.Lock()
	defer scBuilderMu.Unlock()
	scBuilders[backend] = builder
	return nil
}

func newSCCommitterFromConfig(db dbm.DB, cfg Config) SCCommitter {
	backend := cfg.StateCommitmentBackend
	if backend == "" {
		backend = "memiavl"
	}

	scBuilderMu.RLock()
	builder, ok := scBuilders[backend]
	scBuilderMu.RUnlock()
	if !ok {
		// Keep chain runnable for experimental backends during migration.
		return noopSCCommitter{}
	}

	committer, err := builder(db, cfg)
	if err != nil || committer == nil {
		return noopSCCommitter{}
	}
	return committer
}
