package rootmulti

import (
	"fmt"
	"path/filepath"
	"sync"

	scproto "cosmossdk.io/store/seidb/sc/proto"
	scwal "cosmossdk.io/store/seidb/sc/wal"
	"cosmossdk.io/store/types"
)

type compositeStateStore struct {
	cosmosStore    StateStore
	evmStore       StateStore
	pruningManager *stateStorePruningManager
	writeMode      stateStoreWriteMode
	readMode       stateStoreReadMode
	closeOnce      sync.Once
	closeErr       error
}

func newCompositeStateStore(cfg Config) (StateStore, error) {
	writeMode, err := parseWriteMode(cfg.StateStoreWriteMode)
	if err != nil {
		return nil, err
	}
	readMode, err := parseReadMode(cfg.StateStoreReadMode)
	if err != nil {
		return nil, err
	}

	cosmosDB, err := newPebbleStateStore(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create cosmos MVCC DB: %w", err)
	}

	cs := &compositeStateStore{
		cosmosStore: newCosmosStateStore(cosmosDB),
		writeMode:   writeMode,
		readMode:    readMode,
	}

	if writeMode != cosmosOnlyWrite || readMode != cosmosOnlyRead {
		evmStore, err := newEVMStateStore(cfg)
		if err != nil {
			_ = cs.cosmosStore.Close()
			return nil, fmt.Errorf("failed to create EVM store: %w", err)
		}
		cs.evmStore = evmStore
	}

	changelogPath := filepath.Join(cfg.Home, "data", "pebbledb", "changelog")
	if err := recoverCompositeStateStore(changelogPath, cs); err != nil {
		_ = cs.Close()
		return nil, fmt.Errorf("failed to recover composite state store: %w", err)
	}

	if pruner, ok := cosmosDB.(*pebbleStateStore); ok {
		cs.pruningManager = newStateStorePruningManager(
			pruner,
			pruner,
			func() int64 { return pruner.latestVersion.Load() },
			int64(cfg.KeepRecent),
			600,
		)
		cs.pruningManager.Start()
	}

	return cs, nil
}

func (s *compositeStateStore) Get(storeKey string, version int64, key []byte) ([]byte, error) {
	if s.evmStore != nil && s.readMode != cosmosOnlyRead && storeKey == evmStoreKey {
		val, err := s.evmStore.Get(storeKey, version, key)
		if err != nil {
			return nil, err
		}
		if val != nil {
			return val, nil
		}
		if s.readMode == splitRead {
			return nil, nil
		}
	}
	return s.cosmosStore.Get(storeKey, version, key)
}

func (s *compositeStateStore) Has(storeKey string, version int64, key []byte) (bool, error) {
	if s.evmStore != nil && s.readMode != cosmosOnlyRead && storeKey == evmStoreKey {
		has, err := s.evmStore.Has(storeKey, version, key)
		if err != nil {
			return false, err
		}
		if has {
			return true, nil
		}
		if s.readMode == splitRead {
			return false, nil
		}
	}
	return s.cosmosStore.Has(storeKey, version, key)
}

func (s *compositeStateStore) Iterator(storeKey string, version int64, start, end []byte) (types.Iterator, error) {
	return s.cosmosStore.Iterator(storeKey, version, start, end)
}

func (s *compositeStateStore) ReverseIterator(storeKey string, version int64, start, end []byte) (types.Iterator, error) {
	return s.cosmosStore.ReverseIterator(storeKey, version, start, end)
}

func (s *compositeStateStore) Snapshot(storeKey string, version int64) (map[string][]byte, bool) {
	return s.cosmosStore.Snapshot(storeKey, version)
}

func (s *compositeStateStore) HasVersion(version int64) bool {
	return s.cosmosStore.HasVersion(version)
}

func (s *compositeStateStore) EarliestVersion() int64 {
	return s.cosmosStore.EarliestVersion()
}

func (s *compositeStateStore) ApplyChangeSets(version int64, changeSets []*NamedChangeSet) error {
	if s.evmStore == nil || s.writeMode == cosmosOnlyWrite {
		return s.cosmosStore.ApplyChangeSets(version, changeSets)
	}

	evmChangesets := filterEVMNamedChangeSets(changeSets)
	cosmosChangesets := changeSets
	if s.writeMode == splitWrite {
		cosmosChangesets = stripEVMFromNamedChangeSets(changeSets)
	}

	if err := s.cosmosStore.ApplyChangeSets(version, cosmosChangesets); err != nil {
		return fmt.Errorf("cosmos store failed: %w", err)
	}
	if len(evmChangesets) > 0 {
		if err := s.evmStore.ApplyChangeSets(version, evmChangesets); err != nil {
			return fmt.Errorf("evm store failed: %w", err)
		}
	}
	return nil
}

func (s *compositeStateStore) SetLatestVersion(version int64) error {
	if err := s.cosmosStore.SetLatestVersion(version); err != nil {
		return err
	}
	if s.evmStore != nil && s.writeMode != cosmosOnlyWrite {
		if err := s.evmStore.SetLatestVersion(version); err != nil {
			return err
		}
	}
	return nil
}

func (s *compositeStateStore) RollbackToVersion(target int64) error {
	if err := s.cosmosStore.RollbackToVersion(target); err != nil {
		return err
	}
	if s.evmStore != nil {
		if err := s.evmStore.RollbackToVersion(target); err != nil {
			return err
		}
	}
	return nil
}

func (s *compositeStateStore) SyncFromStores(stores map[types.StoreKey]types.CommitKVStore, version int64) error {
	if s.evmStore == nil || s.writeMode == cosmosOnlyWrite {
		return s.cosmosStore.SyncFromStores(stores, version)
	}
	if err := s.cosmosStore.SyncFromStores(stores, version); err != nil {
		return err
	}
	return s.evmStore.SyncFromStores(stores, version)
}

func (s *compositeStateStore) Close() error {
	s.closeOnce.Do(func() {
		if s.pruningManager != nil {
			s.pruningManager.Stop()
		}
		if s.evmStore != nil {
			if err := s.evmStore.Close(); err != nil {
				s.closeErr = err
			}
		}
		if err := s.cosmosStore.Close(); err != nil && s.closeErr == nil {
			s.closeErr = err
		}
	})
	return s.closeErr
}

func recoverCompositeStateStore(changelogPath string, compositeStore *compositeStateStore) error {
	var cosmosVersion int64
	if compositeStore.cosmosStore != nil {
		if reader, ok := compositeStore.cosmosStore.(interface{ HasVersion(int64) bool }); ok {
			_ = reader
		}
		if latestReader, ok := compositeStore.cosmosStore.(*cosmosStateStore); ok {
			if ps, ok := latestReader.db.(*pebbleStateStore); ok {
				cosmosVersion = ps.latestVersion.Load()
			}
		}
	}

	var evmVersion int64
	if compositeStore.evmStore != nil {
		if evm, ok := compositeStore.evmStore.(*evmStateStore); ok {
			evmVersion = evm.EarliestVersion()
			if evmVersion == 0 {
				evmVersion = cosmosVersion
			}
		}
	}

	startVersion := cosmosVersion
	if compositeStore.evmStore != nil && evmVersion < startVersion {
		startVersion = evmVersion
	}

	return replayCompositeWAL(changelogPath, startVersion, -1, func(entry scproto.ChangelogEntry) error {
		changeSets := toNamedChangeSets(entry.Changesets)
		if compositeStore.cosmosStore != nil && entry.Version > cosmosVersion {
			cosmosChangesets := changeSets
			if compositeStore.writeMode == splitWrite {
				cosmosChangesets = stripEVMFromNamedChangeSets(changeSets)
			}
			if len(cosmosChangesets) > 0 {
				if err := applyChangeSetsSyncToStore(compositeStore.cosmosStore, entry.Version, cosmosChangesets); err != nil {
					return fmt.Errorf("failed to apply cosmos changeset at version %d: %w", entry.Version, err)
				}
			} else if err := compositeStore.cosmosStore.SetLatestVersion(entry.Version); err != nil {
				return err
			}
		}
		if compositeStore.evmStore != nil && entry.Version > evmVersion {
			evmChangesets := filterEVMNamedChangeSets(changeSets)
			if len(evmChangesets) > 0 {
				if err := applyChangeSetsSyncToStore(compositeStore.evmStore, entry.Version, evmChangesets); err != nil {
					return fmt.Errorf("failed to apply evm changeset at version %d: %w", entry.Version, err)
				}
			} else if err := compositeStore.evmStore.SetLatestVersion(entry.Version); err != nil {
				return err
			}
		}
		return nil
	})
}

func replayCompositeWAL(changelogPath string, fromVersion int64, toVersion int64, handler func(entry scproto.ChangelogEntry) error) error {
	streamHandler, err := scwal.NewChangelogWAL(changelogPath, scwal.Config{})
	if err != nil {
		return nil
	}
	defer func() { _ = streamHandler.Close() }()

	firstOffset, err := streamHandler.FirstOffset()
	if err != nil || firstOffset <= 0 {
		return nil
	}
	lastOffset, err := streamHandler.LastOffset()
	if err != nil || lastOffset <= 0 {
		return nil
	}
	lastEntry, err := streamHandler.ReadAt(lastOffset)
	if err != nil {
		return err
	}
	if lastEntry.Version <= fromVersion {
		return nil
	}
	startOffset, err := findSSReplayStartOffset(streamHandler, firstOffset, lastOffset, fromVersion)
	if err != nil {
		return err
	}
	if startOffset > lastOffset {
		return nil
	}
	return streamHandler.Replay(startOffset, lastOffset, func(_ uint64, entry scproto.ChangelogEntry) error {
		if toVersion >= 0 && entry.Version > toVersion {
			return nil
		}
		return handler(entry)
	})
}

func applyChangeSetsSyncToStore(store StateStore, version int64, changeSets []*NamedChangeSet) error {
	if syncer, ok := store.(interface {
		ApplyChangeSetsSync(version int64, changeSets []*NamedChangeSet) error
	}); ok {
		return syncer.ApplyChangeSetsSync(version, changeSets)
	}
	return store.ApplyChangeSets(version, changeSets)
}
