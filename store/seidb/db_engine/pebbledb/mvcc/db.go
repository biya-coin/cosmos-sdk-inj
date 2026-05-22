package mvcc

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"

	seidbcfg "cosmossdk.io/store/seidb/config"
	scproto "cosmossdk.io/store/seidb/sc/proto"
	scwal "cosmossdk.io/store/seidb/sc/wal"
	sstypes "cosmossdk.io/store/seidb/ss/types"
	ssutils "cosmossdk.io/store/seidb/ss/utils"
	"cosmossdk.io/store/types"
)

type Config = seidbcfg.Config
type StateStore = sstypes.StateStore
type NamedChangeSet = sstypes.NamedChangeSet

const (
	ssVersionSize       = 8
	ssPrefixStore       = "s/k:"
	ssLenPrefixStore    = 4
	ssStorePrefixTpl    = "s/k:%s/"
	ssLatestVersionKey  = "s/_latest"
	ssEarliestVersionKey = "s/_earliest"
	ssTombstoneValue    = "TOMBSTONE"

	ssImportCommitBatchSize = 10000
	ssPruneCommitBatchSize  = 50
	ssMinWALEntriesToKeep   = 1000
)

var ssDefaultWriteOptions = &pebble.WriteOptions{Sync: false}

type pebbleStateStore struct {
	mtx sync.RWMutex

	storage *pebble.DB
	config  Config

	asyncWriteWG  sync.WaitGroup
	pendingChanges chan versionedChangeSets

	earliestVersion atomic.Int64
	latestVersion   atomic.Int64
	storeKeyDirty   sync.Map

	changelog    scwal.ChangelogWAL
	changelogDir string
}

type versionedChangeSets struct {
	Version    int64
	ChangeSets []*NamedChangeSet
	Done       chan struct{}
}

func NewStore(cfg Config) (StateStore, error) {
	if cfg.Home == "" {
		return nil, fmt.Errorf("seidb home must not be empty for pebbledb state store")
	}

	dataDir := filepath.Join(cfg.Home, "data", "pebbledb")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create pebbledb state dir: %w", err)
	}

	db, err := pebble.Open(dataDir, &pebble.Options{
		Comparer: MVCCComparer,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open pebbledb state store: %w", err)
	}

	earliestVersion, err := retrieveSSEarliestVersion(db)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to retrieve earliest version: %w", err)
	}
	latestVersion, err := retrieveSSLatestVersion(db)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to retrieve latest version: %w", err)
	}

	store := &pebbleStateStore{
		storage:        db,
		config:         cfg,
		pendingChanges: make(chan versionedChangeSets, maxInt(cfg.StateStoreAsyncWriteBuffer, 1)),
	}
	store.earliestVersion.Store(earliestVersion)
	store.latestVersion.Store(latestVersion)

	changelogDir := filepath.Join(dataDir, "changelog")
	if err := os.MkdirAll(changelogDir, 0o755); err == nil {
		keepN := uint64(ssMinWALEntriesToKeep)
		if cfg.KeepRecent > 0 && uint64(cfg.KeepRecent)+1 > keepN {
			keepN = uint64(cfg.KeepRecent) + 1
		}
		changelog, walErr := scwal.NewChangelogWAL(changelogDir, scwal.Config{
			KeepRecent:    keepN,
			PruneInterval: 30 * time.Second,
		})
		if walErr == nil {
			store.changelog = changelog
			store.changelogDir = changelogDir
		}
	}

	if err := store.recoverFromWAL(); err != nil {
		if store.changelog != nil {
			_ = store.changelog.Close()
		}
		_ = db.Close()
		return nil, fmt.Errorf("ss wal recovery failed: %w", err)
	}

	if cfg.StateStoreAsyncWriteBuffer > 0 {
		store.asyncWriteWG.Add(1)
		go store.writeAsyncInBackground()
	}

	return store, nil
}

func (s *pebbleStateStore) Snapshot(storeName string, version int64) (map[string][]byte, bool) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	if !s.hasVersionLocked(version) {
		return nil, false
	}

	itr, err := s.Iterator(storeName, version, nil, nil)
	if err != nil {
		return nil, false
	}
	defer itr.Close()

	out := make(map[string][]byte)
	for ; itr.Valid(); itr.Next() {
		out[string(itr.Key())] = ssutils.CloneBytesNonNil(itr.Value())
	}
	if err := itr.Error(); err != nil {
		return nil, false
	}
	return out, true
}

func (s *pebbleStateStore) Get(storeName string, version int64, key []byte) ([]byte, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return s.getLocked(storeName, version, key)
}

func (s *pebbleStateStore) getLocked(storeName string, targetVersion int64, key []byte) ([]byte, error) {
	if targetVersion < s.earliestVersion.Load() {
		return nil, nil
	}

	prefixedVal, err := getMVCCValueSlice(s.storage, storeName, key, targetVersion)
	if err != nil {
		if errors.Is(err, errSSRecordNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to perform pebbledb read: %w", err)
	}

	valBz, tombBz, ok := splitMVCCKey(prefixedVal)
	if !ok {
		return nil, fmt.Errorf("invalid pebbledb mvcc value")
	}
	if len(tombBz) == 0 {
		return valBz, nil
	}

	tombstone, err := decodeUint64Ascending(tombBz)
	if err != nil {
		return nil, fmt.Errorf("failed to decode tombstone version: %w", err)
	}
	if targetVersion < tombstone {
		return valBz, nil
	}
	return nil, nil
}

func (s *pebbleStateStore) Has(storeName string, version int64, key []byte) (bool, error) {
	val, err := s.Get(storeName, version, key)
	if err != nil {
		return false, err
	}
	return val != nil, nil
}

func (s *pebbleStateStore) Iterator(storeName string, version int64, start, end []byte) (types.Iterator, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, fmt.Errorf("key cannot be empty")
	}
	if start != nil && end != nil && bytes.Compare(start, end) > 0 {
		return nil, fmt.Errorf("iterator start is after end")
	}

	lowerBound := mvccEncode(prependStoreKey(storeName, start), 0)
	var upperBound []byte
	if end != nil {
		upperBound = mvccEncode(prependStoreKey(storeName, end), 0)
	}

	itr, err := s.storage.NewIter(&pebble.IterOptions{LowerBound: lowerBound, UpperBound: upperBound})
	if err != nil {
		return nil, fmt.Errorf("failed to create pebbledb iterator: %w", err)
	}
	return newMVCCIterator(itr, storePrefix(storeName), start, end, version, s.earliestVersion.Load(), false), nil
}

func (s *pebbleStateStore) ReverseIterator(storeName string, version int64, start, end []byte) (types.Iterator, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, fmt.Errorf("key cannot be empty")
	}
	if start != nil && end != nil && bytes.Compare(start, end) > 0 {
		return nil, fmt.Errorf("iterator start is after end")
	}

	lowerBound := mvccEncode(prependStoreKey(storeName, start), 0)
	var upperBound []byte
	if end != nil {
		upperBound = mvccEncode(prependStoreKey(storeName, end), 0)
	} else {
		upperBound = mvccEncode(prefixEnd(storePrefix(storeName)), 0)
	}

	itr, err := s.storage.NewIter(&pebble.IterOptions{LowerBound: lowerBound, UpperBound: upperBound})
	if err != nil {
		return nil, fmt.Errorf("failed to create pebbledb iterator: %w", err)
	}
	return newMVCCIterator(itr, storePrefix(storeName), start, end, version, s.earliestVersion.Load(), true), nil
}

func (s *pebbleStateStore) HasVersion(version int64) bool {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return s.hasVersionLocked(version)
}

func (s *pebbleStateStore) hasVersionLocked(version int64) bool {
	latest := s.latestVersion.Load()
	earliest := s.earliestVersion.Load()
	return version > 0 && latest > 0 && version <= latest && (earliest == 0 || version >= earliest)
}

func (s *pebbleStateStore) EarliestVersion() int64 {
	return s.earliestVersion.Load()
}

func (s *pebbleStateStore) LatestVersion() int64 {
	return s.latestVersion.Load()
}

func (s *pebbleStateStore) ApplyChangeSets(version int64, changeSets []*NamedChangeSet) error {
	s.mtx.Lock()
	if err := s.validateNextVersionLocked(version); err != nil {
		s.mtx.Unlock()
		return err
	}

	if s.changelog != nil && len(changeSets) > 0 {
		entry := scproto.ChangelogEntry{
			Version:    version,
			Changesets: ssutils.ToProtoChangeSets(changeSets),
		}
		if err := s.changelog.Write(entry); err != nil {
			s.mtx.Unlock()
			return fmt.Errorf("ss wal write at version %d: %w", version, err)
		}
	}

	if s.config.StateStoreAsyncWriteBuffer > 0 {
		s.pendingChanges <- versionedChangeSets{
			Version:    version,
			ChangeSets: ssutils.CloneNamedChangeSets(changeSets),
		}
		s.mtx.Unlock()
		return nil
	}
	defer s.mtx.Unlock()
	return s.applyChangeSetsNoWAL(version, changeSets)
}

func (s *pebbleStateStore) ApplyChangeSetsSync(version int64, changeSets []*NamedChangeSet) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if err := s.validateNextVersionLocked(version); err != nil {
		return err
	}
	return s.applyChangeSetsNoWAL(version, changeSets)
}

func (s *pebbleStateStore) applyChangeSetsNoWAL(version int64, changeSets []*NamedChangeSet) error {
	if version == 0 {
		version = 1
	}

	batch, err := newSSBatch(s.storage, version)
	if err != nil {
		return err
	}
	if s.earliestVersion.Load() == 0 {
		var earliestBz [ssVersionSize]byte
		binary.LittleEndian.PutUint64(earliestBz[:], uint64(version))
		if err := batch.batch.Set([]byte(ssEarliestVersionKey), earliestBz[:], nil); err != nil {
			_ = batch.Close()
			return fmt.Errorf("failed to write earliest version metadata: %w", err)
		}
	}

	for _, cs := range changeSets {
		if cs == nil || cs.ChangeSet == nil {
			continue
		}
		for _, kvPair := range cs.ChangeSet.Pairs {
			if kvPair == nil {
				continue
			}
			if kvPair.Delete || kvPair.Value == nil {
				if err := batch.Delete(cs.Name, kvPair.Key); err != nil {
					_ = batch.Close()
					return err
				}
			} else if err := batch.Set(cs.Name, kvPair.Key, kvPair.Value); err != nil {
				_ = batch.Close()
				return err
			}
		}
		s.storeKeyDirty.Store(cs.Name, version)
	}

	if err := batch.Write(); err != nil {
		return err
	}
	if version > s.latestVersion.Load() {
		s.latestVersion.Store(version)
	}
	if s.earliestVersion.Load() == 0 {
		s.earliestVersion.Store(version)
	}
	return s.pruneOldVersionsLocked()
}

func (s *pebbleStateStore) SetLatestVersion(version int64) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if version < 0 {
		return fmt.Errorf("version must be non-negative")
	}
	if version == 0 {
		return nil
	}
	if err := s.validateNextVersionLocked(version); err != nil {
		return err
	}

	if err := writeSSVersionMetadata(s.storage, ssLatestVersionKey, version); err != nil {
		return err
	}
	if s.earliestVersion.Load() == 0 {
		if err := writeSSVersionMetadata(s.storage, ssEarliestVersionKey, version); err != nil {
			return err
		}
		s.earliestVersion.Store(version)
	}
	s.latestVersion.Store(version)
	return s.pruneOldVersionsLocked()
}

func (s *pebbleStateStore) RollbackToVersion(target int64) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if target <= 0 {
		return nil
	}
	if s.latestVersion.Load() == 0 || target >= s.latestVersion.Load() {
		return nil
	}
	if target < s.earliestVersion.Load() {
		return fmt.Errorf("rollback target version %d is below earliest version %d", target, s.earliestVersion.Load())
	}

	batch, err := newSSRawBatch(s.storage)
	if err != nil {
		return err
	}
	defer batch.Close()

	itr, err := s.storage.NewIter(nil)
	if err != nil {
		return err
	}
	defer itr.Close()

	deleteCount := 0
	for itr.First(); itr.Valid(); itr.Next() {
		rawKey := append([]byte(nil), itr.Key()...)
		if isSSMetadataKey(rawKey) {
			continue
		}
		_, versionBz, ok := splitMVCCKey(rawKey)
		if !ok {
			return fmt.Errorf("invalid mvcc key during rollback")
		}
		entryVersion, err := decodeUint64Ascending(versionBz)
		if err != nil {
			return err
		}
		if entryVersion <= target {
			continue
		}
		if err := batch.HardDeleteRaw(rawKey); err != nil {
			return err
		}
		deleteCount++
		if deleteCount >= ssPruneCommitBatchSize {
			if err := batch.Write(); err != nil {
				return err
			}
			batch, err = newSSRawBatch(s.storage)
			if err != nil {
				return err
			}
			deleteCount = 0
		}
	}

	if batch.Size() > 0 {
		if err := batch.Write(); err != nil {
			return err
		}
	}

	if err := writeSSVersionMetadata(s.storage, ssLatestVersionKey, target); err != nil {
		return err
	}
	s.latestVersion.Store(target)
	s.truncateWALAfterVersion(target)
	return nil
}

func (s *pebbleStateStore) truncateWALAfterVersion(target int64) {
	if s.changelog == nil {
		return
	}
	firstOffset, err := s.changelog.FirstOffset()
	if err != nil || firstOffset == 0 {
		return
	}
	lastOffset, err := s.changelog.LastOffset()
	if err != nil || lastOffset == 0 {
		return
	}

	startOffset, err := findSSReplayStartOffset(s.changelog, firstOffset, lastOffset, target)
	if err != nil {
		return
	}
	if startOffset <= lastOffset {
		if startOffset == firstOffset {
			type walResetter interface {
				TruncateAll() error
			}
			if resetter, ok := any(s.changelog).(walResetter); ok {
				_ = resetter.TruncateAll()
			}
			return
		}
		_ = s.changelog.TruncateAfter(startOffset - 1)
	}
}

func (s *pebbleStateStore) SyncFromStores(stores map[types.StoreKey]types.CommitKVStore, version int64) error {
	if version <= 0 {
		return nil
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()

	if err := s.clearAllDataLocked(); err != nil {
		return err
	}

	batch, err := newSSBatch(s.storage, version)
	if err != nil {
		return err
	}

	counter := 0
	for key, store := range stores {
		if key == nil || store == nil {
			continue
		}
		it := store.Iterator(nil, nil)
		for ; it.Valid(); it.Next() {
			if err := batch.Set(key.Name(), it.Key(), it.Value()); err != nil {
				_ = it.Close()
				_ = batch.Close()
				return err
			}
			counter++
			if counter%ssImportCommitBatchSize == 0 {
				if err := batch.Write(); err != nil {
					_ = it.Close()
					return err
				}
				batch, err = newSSBatch(s.storage, version)
				if err != nil {
					_ = it.Close()
					return err
				}
			}
		}
		if err := it.Close(); err != nil {
			_ = batch.Close()
			return err
		}
		s.storeKeyDirty.Store(key.Name(), version)
	}

	if batch.Size() > 0 {
		if err := batch.Write(); err != nil {
			return err
		}
	}

	if err := writeSSVersionMetadata(s.storage, ssLatestVersionKey, version); err != nil {
		return err
	}
	if err := writeSSVersionMetadata(s.storage, ssEarliestVersionKey, version); err != nil {
		return err
	}
	s.latestVersion.Store(version)
	s.earliestVersion.Store(version)
	return s.resetWALLocked()
}

func (s *pebbleStateStore) ImportSnapshot(version int64, nodes <-chan sstypes.SnapshotImportNode) error {
	if version <= 0 {
		return fmt.Errorf("invalid snapshot version: %d", version)
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()

	if err := s.clearAllDataLocked(); err != nil {
		return err
	}

	batch, err := newSSBatch(s.storage, version)
	if err != nil {
		return err
	}

	counter := 0
	for node := range nodes {
		if node.StoreKey == "" {
			continue
		}
		if err := batch.Set(node.StoreKey, node.Key, node.Value); err != nil {
			_ = batch.Close()
			return err
		}
		counter++
		if counter%ssImportCommitBatchSize == 0 {
			if err := batch.Write(); err != nil {
				return err
			}
			batch, err = newSSBatch(s.storage, version)
			if err != nil {
				return err
			}
		}
		s.storeKeyDirty.Store(node.StoreKey, version)
	}

	if batch.Size() > 0 {
		if err := batch.Write(); err != nil {
			return err
		}
	}

	if err := writeSSVersionMetadata(s.storage, ssLatestVersionKey, version); err != nil {
		return err
	}
	if err := writeSSVersionMetadata(s.storage, ssEarliestVersionKey, version); err != nil {
		return err
	}
	s.latestVersion.Store(version)
	s.earliestVersion.Store(version)
	return s.resetWALLocked()
}

func (s *pebbleStateStore) Close() error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if s.pendingChanges != nil {
		close(s.pendingChanges)
		s.mtx.Unlock()
		s.asyncWriteWG.Wait()
		s.mtx.Lock()
		s.pendingChanges = nil
	}

	if s.changelog != nil {
		_ = s.changelog.Close()
		s.changelog = nil
	}
	if s.storage == nil {
		return nil
	}
	err := s.storage.Close()
	s.storage = nil
	return err
}

func (s *pebbleStateStore) writeAsyncInBackground() {
	defer s.asyncWriteWG.Done()
	for nextChange := range s.pendingChanges {
		if nextChange.Done != nil {
			close(nextChange.Done)
			continue
		}
		s.mtx.Lock()
		if err := s.applyChangeSetsNoWAL(nextChange.Version, nextChange.ChangeSets); err != nil {
			s.mtx.Unlock()
			panic(err)
		}
		s.mtx.Unlock()
	}
}

func (s *pebbleStateStore) WaitForPendingWrites() {
	if s.config.StateStoreAsyncWriteBuffer <= 0 {
		return
	}
	done := make(chan struct{})
	s.pendingChanges <- versionedChangeSets{Done: done}
	<-done
}

func (s *pebbleStateStore) recoverFromWAL() error {
	if s.changelog == nil {
		return nil
	}

	firstOffset, err := s.changelog.FirstOffset()
	if err != nil {
		return fmt.Errorf("ss wal: read first offset: %w", err)
	}
	if firstOffset == 0 {
		return nil
	}

	lastOffset, err := s.changelog.LastOffset()
	if err != nil {
		return fmt.Errorf("ss wal: read last offset: %w", err)
	}
	if lastOffset == 0 {
		return nil
	}

	lastEntry, err := s.changelog.ReadAt(lastOffset)
	if err != nil {
		return fmt.Errorf("ss wal: read last entry: %w", err)
	}
	if lastEntry.Version <= s.latestVersion.Load() {
		return nil
	}

	startOffset, err := findSSReplayStartOffset(s.changelog, firstOffset, lastOffset, s.latestVersion.Load())
	if err != nil {
		return fmt.Errorf("ss wal: find replay start: %w", err)
	}
	if startOffset > lastOffset {
		return nil
	}

	return s.changelog.Replay(startOffset, lastOffset, func(_ uint64, entry scproto.ChangelogEntry) error {
		if entry.Version <= s.latestVersion.Load() {
			return nil
		}
		if err := s.validateNextVersionLocked(entry.Version); err != nil {
			return fmt.Errorf("ss wal replay: version check at %d: %w", entry.Version, err)
		}
		return s.applyChangeSetsNoWAL(entry.Version, ssutils.FromProtoChangeSets(entry.Changesets))
	})
}

func findSSReplayStartOffset(changelog scwal.ChangelogWAL, first, last uint64, targetVersion int64) (uint64, error) {
	lo, hi := first, last
	result := last + 1

	for lo <= hi {
		mid := lo + (hi-lo)/2
		entry, err := changelog.ReadAt(mid)
		if err != nil {
			return 0, fmt.Errorf("ss wal: read at offset %d: %w", mid, err)
		}
		if entry.Version > targetVersion {
			result = mid
			if mid == first {
				break
			}
			hi = mid - 1
		} else {
			lo = mid + 1
		}
	}
	return result, nil
}

func (s *pebbleStateStore) validateNextVersionLocked(version int64) error {
	if version <= 0 {
		return fmt.Errorf("version must be positive")
	}
	if latest := s.latestVersion.Load(); latest > 0 && version <= latest {
		return fmt.Errorf("version must increase monotonically, latest=%d got=%d", latest, version)
	}
	return nil
}

func (s *pebbleStateStore) pruneOldVersionsLocked() error {
	keepRecent := int64(s.config.KeepRecent)
	if keepRecent <= 0 || s.latestVersion.Load() <= keepRecent {
		return nil
	}
	pruneTo := s.latestVersion.Load() - keepRecent
	if pruneTo < s.earliestVersion.Load() {
		return nil
	}

	if err := s.pruneLocked(pruneTo); err != nil {
		return err
	}
	s.earliestVersion.Store(pruneTo + 1)
	return writeSSVersionMetadata(s.storage, ssEarliestVersionKey, pruneTo+1)
}

func (s *pebbleStateStore) Prune(version int64) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if err := s.pruneLocked(version); err != nil {
		return err
	}
	s.earliestVersion.Store(version + 1)
	return writeSSVersionMetadata(s.storage, ssEarliestVersionKey, version+1)
}

func (s *pebbleStateStore) pruneLocked(version int64) error {
	itr, err := s.storage.NewIter(nil)
	if err != nil {
		return err
	}
	defer itr.Close()

	batch, err := newSSRawBatch(s.storage)
	if err != nil {
		return err
	}
	defer batch.Close()

	var (
		counter                                 int
		prevKey, prevKeyEncoded, prevValEncoded []byte
		prevVersionDecoded                      int64
		prevStore                               string
	)

	for itr.First(); itr.Valid(); {
		currKeyEncoded := append([]byte(nil), itr.Key()...)
		if isSSMetadataKey(currKeyEncoded) {
			itr.Next()
			continue
		}

		currKey, currVersion, ok := splitMVCCKey(currKeyEncoded)
		if !ok {
			return fmt.Errorf("invalid mvcc key")
		}
		storeKey, err := parseStoreKey(currKey)
		if err != nil {
			return err
		}

		if storeKey != prevStore {
			prevStore = storeKey
			if updated, ok := s.storeKeyDirty.Load(storeKey); ok {
				if versionUpdated, typeOK := updated.(int64); typeOK && versionUpdated < s.earliestVersion.Load() {
					itr.SeekGE(storePrefix(storeKey + "0"))
					continue
				}
			}
		}

		currVersionDecoded, err := decodeUint64Ascending(currVersion)
		if err != nil {
			return err
		}

		if currVersionDecoded > version && prevVersionDecoded > version {
			itr.NextPrefix()
			continue
		}

		if prevVersionDecoded <= version && (bytes.Equal(prevKey, currKey) || valTombstoned(prevValEncoded)) {
			if err := batch.HardDeleteRaw(prevKeyEncoded); err != nil {
				return err
			}
			counter++
			if counter >= ssPruneCommitBatchSize {
				if err := batch.Write(); err != nil {
					return err
				}
				batch, err = newSSRawBatch(s.storage)
				if err != nil {
					return err
				}
				counter = 0
			}
		}

		prevKey = currKey
		prevVersionDecoded = currVersionDecoded
		prevKeyEncoded = currKeyEncoded
		prevValEncoded = append([]byte(nil), itr.Value()...)
		itr.Next()
	}

	if batch.Size() > 0 {
		return batch.Write()
	}
	return nil
}

func (s *pebbleStateStore) clearAllDataLocked() error {
	itr, err := s.storage.NewIter(nil)
	if err != nil {
		return err
	}
	defer itr.Close()

	batch, err := newSSRawBatch(s.storage)
	if err != nil {
		return err
	}
	defer batch.Close()

	count := 0
	for itr.First(); itr.Valid(); itr.Next() {
		if err := batch.HardDeleteRaw(append([]byte(nil), itr.Key()...)); err != nil {
			return err
		}
		count++
		if count >= ssImportCommitBatchSize {
			if err := batch.Write(); err != nil {
				return err
			}
			batch, err = newSSRawBatch(s.storage)
			if err != nil {
				return err
			}
			count = 0
		}
	}
	if batch.Size() > 0 {
		if err := batch.Write(); err != nil {
			return err
		}
	}
	s.storeKeyDirty = sync.Map{}
	s.latestVersion.Store(0)
	s.earliestVersion.Store(0)
	return nil
}

func (s *pebbleStateStore) resetWALLocked() error {
	if s.changelog == nil {
		return nil
	}
	type walResetter interface {
		TruncateAll() error
	}
	if resetter, ok := any(s.changelog).(walResetter); ok {
		return resetter.TruncateAll()
	}
	firstOffset, err := s.changelog.FirstOffset()
	if err != nil {
		return nil
	}
	if firstOffset == 0 {
		return nil
	}
	return s.changelog.TruncateAfter(firstOffset - 1)
}

func retrieveSSLatestVersion(db *pebble.DB) (int64, error) {
	return retrieveSSVersionMetadata(db, ssLatestVersionKey)
}

func retrieveSSEarliestVersion(db *pebble.DB) (int64, error) {
	return retrieveSSVersionMetadata(db, ssEarliestVersionKey)
}

func retrieveSSVersionMetadata(db *pebble.DB, key string) (int64, error) {
	bz, closer, err := db.Get([]byte(key))
	defer func() {
		if closer != nil {
			_ = closer.Close()
		}
	}()
	if err != nil || len(bz) == 0 {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, nil
		}
		return 0, err
	}

	uv := binary.LittleEndian.Uint64(bz)
	if uv > math.MaxInt64 {
		return 0, fmt.Errorf("version in database overflows int64: %d", uv)
	}
	return int64(uv), nil
}

func writeSSVersionMetadata(db *pebble.DB, key string, version int64) error {
	var ts [ssVersionSize]byte
	binary.LittleEndian.PutUint64(ts[:], uint64(version))
	return db.Set([]byte(key), ts[:], ssDefaultWriteOptions)
}

var errSSRecordNotFound = errors.New("record not found")

func getMVCCValueSlice(db *pebble.DB, storeKey string, key []byte, version int64) ([]byte, error) {
	prefixedKey := prependStoreKey(storeKey, key)
	lowerBound := mvccEncode(prefixedKey, 0)
	upperBound := mvccEncode(prefixedKey, version+1)

	itr, err := db.NewIter(&pebble.IterOptions{LowerBound: lowerBound, UpperBound: upperBound})
	if err != nil {
		return nil, err
	}
	defer itr.Close()

	if !itr.Last() {
		return nil, errSSRecordNotFound
	}
	return append([]byte(nil), itr.Value()...), nil
}

func isSSMetadataKey(key []byte) bool {
	return bytes.HasPrefix(key, []byte("s/_"))
}

func storePrefix(storeKey string) []byte {
	return []byte(fmt.Sprintf(ssStorePrefixTpl, storeKey))
}

func prependStoreKey(storeKey string, key []byte) []byte {
	if storeKey == "" {
		return key
	}
	return append(storePrefix(storeKey), key...)
}

func parseStoreKey(key []byte) (string, error) {
	keyStr := string(key)
	if !strings.HasPrefix(keyStr, ssPrefixStore) {
		return "", fmt.Errorf("not a valid store key")
	}
	slashIndex := strings.Index(keyStr[ssLenPrefixStore:], "/")
	if slashIndex == -1 {
		return "", fmt.Errorf("not a valid store key")
	}
	return keyStr[ssLenPrefixStore : ssLenPrefixStore+slashIndex], nil
}

func prefixEnd(b []byte) []byte {
	end := make([]byte, len(b))
	copy(end, b)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] != 0xFF {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}

type ssBatch struct {
	storage *pebble.DB
	batch   *pebble.Batch
	version int64
}

func newSSBatch(storage *pebble.DB, version int64) (*ssBatch, error) {
	if version < 0 {
		return nil, fmt.Errorf("version must be non-negative")
	}
	var versionBz [ssVersionSize]byte
	binary.LittleEndian.PutUint64(versionBz[:], uint64(version))

	batch := storage.NewBatch()
	if err := batch.Set([]byte(ssLatestVersionKey), versionBz[:], nil); err != nil {
		_ = batch.Close()
		return nil, fmt.Errorf("failed to write pebbledb batch: %w", err)
	}
	return &ssBatch{storage: storage, batch: batch, version: version}, nil
}

func (b *ssBatch) Size() int {
	return b.batch.Len()
}

func (b *ssBatch) Close() error {
	return b.batch.Close()
}

func (b *ssBatch) set(storeKey string, tombstone int64, key, value []byte) error {
	prefixedKey := mvccEncode(prependStoreKey(storeKey, key), b.version)
	prefixedVal := mvccEncode(value, tombstone)
	if err := b.batch.Set(prefixedKey, prefixedVal, nil); err != nil {
		return fmt.Errorf("failed to write pebbledb batch: %w", err)
	}
	return nil
}

func (b *ssBatch) Set(storeKey string, key, value []byte) error {
	return b.set(storeKey, 0, key, value)
}

func (b *ssBatch) Delete(storeKey string, key []byte) error {
	return b.set(storeKey, b.version, key, []byte(ssTombstoneValue))
}

func (b *ssBatch) Write() (err error) {
	defer func() {
		err = errors.Join(err, b.batch.Close())
	}()
	return b.batch.Commit(ssDefaultWriteOptions)
}

type ssRawBatch struct {
	storage *pebble.DB
	batch   *pebble.Batch
}

func newSSRawBatch(storage *pebble.DB) (*ssRawBatch, error) {
	return &ssRawBatch{
		storage: storage,
		batch:   storage.NewBatch(),
	}, nil
}

func (b *ssRawBatch) Size() int {
	return b.batch.Len()
}

func (b *ssRawBatch) Close() error {
	return b.batch.Close()
}

func (b *ssRawBatch) HardDeleteRaw(key []byte) error {
	if err := b.batch.Delete(key, nil); err != nil {
		return fmt.Errorf("failed to hard delete key: %w", err)
	}
	return nil
}

func (b *ssRawBatch) Write() (err error) {
	defer func() {
		err = errors.Join(err, b.batch.Close())
	}()
	return b.batch.Commit(ssDefaultWriteOptions)
}

var MVCCComparer = &pebble.Comparer{
	Name: "ss_pebbledb_comparator",

	Compare: mvccKeyCompare,

	AbbreviatedKey: func(k []byte) uint64 {
		key, _, ok := splitMVCCKey(k)
		if !ok {
			return 0
		}
		return pebble.DefaultComparer.AbbreviatedKey(key)
	},

	Equal: func(a, b []byte) bool {
		return mvccKeyCompare(a, b) == 0
	},

	Separator: func(dst, a, b []byte) []byte {
		aKey, _, ok := splitMVCCKey(a)
		if !ok {
			return append(dst, a...)
		}
		bKey, _, ok := splitMVCCKey(b)
		if !ok {
			return append(dst, a...)
		}
		if bytes.Equal(aKey, bKey) {
			return append(dst, a...)
		}
		n := len(dst)
		dst = pebble.DefaultComparer.Separator(dst, aKey, bKey)
		buf := dst[n:]
		if bytes.Equal(aKey, buf) {
			return append(dst[:n], a...)
		}
		return append(dst, 0)
	},

	ImmediateSuccessor: func(dst, a []byte) []byte {
		return append(append(dst, a...), 0)
	},

	Successor: func(dst, a []byte) []byte {
		aKey, _, ok := splitMVCCKey(a)
		if !ok {
			return append(dst, a...)
		}
		n := len(dst)
		dst = pebble.DefaultComparer.Successor(dst, aKey)
		buf := dst[n:]
		if bytes.Equal(aKey, buf) {
			return append(dst[:n], a...)
		}
		return append(dst, 0)
	},

	FormatKey: pebble.DefaultComparer.FormatKey,

	Split: func(k []byte) int {
		key, _, ok := splitMVCCKey(k)
		if !ok {
			return len(k)
		}
		return len(key) + 1
	},

}

func splitMVCCKey(mvccKey []byte) (key, version []byte, ok bool) {
	if len(mvccKey) == 0 {
		return nil, nil, false
	}
	mvccKeyCopy := append([]byte(nil), mvccKey...)
	n := len(mvccKeyCopy) - 1
	tsLen := int(mvccKeyCopy[n])
	if n < tsLen {
		return nil, nil, false
	}
	key = mvccKeyCopy[:n-tsLen]
	if tsLen > 0 {
		version = mvccKeyCopy[n-tsLen+1 : len(mvccKeyCopy)-1]
	}
	return key, version, true
}

func mvccKeyCompare(a, b []byte) int {
	aEnd := len(a) - 1
	bEnd := len(b) - 1
	if aEnd < 0 || bEnd < 0 {
		return bytes.Compare(a, b)
	}
	aSep := aEnd - int(a[aEnd])
	bSep := bEnd - int(b[bEnd])
	if aSep < 0 || bSep < 0 {
		return bytes.Compare(a, b)
	}
	if c := bytes.Compare(a[:aSep], b[:bSep]); c != 0 {
		return c
	}
	aTS := a[aSep:aEnd]
	bTS := b[bSep:bEnd]
	if len(aTS) == 0 {
		if len(bTS) == 0 {
			return 0
		}
		return -1
	} else if len(bTS) == 0 {
		return 1
	}
	return bytes.Compare(aTS, bTS)
}

func mvccEncode(key []byte, version int64) (dst []byte) {
	dst = append(dst, key...)
	dst = append(dst, 0)
	if version > 0 {
		extra := byte(1 + 8)
		dst = encodeUint64Ascending(dst, uint64(version))
		dst = append(dst, extra)
	}
	return dst
}

func encodeUint64Ascending(dst []byte, v uint64) []byte {
	return append(
		dst,
		byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32),
		byte(v>>24), byte(v>>16), byte(v>>8), byte(v),
	)
}

func decodeUint64Ascending(b []byte) (int64, error) {
	if len(b) < 8 {
		return 0, fmt.Errorf("insufficient bytes to decode uint64 int value; expected 8; got %d", len(b))
	}
	uv := binary.BigEndian.Uint64(b)
	if uv > math.MaxInt64 {
		return 0, fmt.Errorf("uint64 value overflows int64: %d", uv)
	}
	return int64(uv), nil
}

func valTombstoned(val []byte) bool {
	_, tombBz, ok := splitMVCCKey(val)
	return ok && len(tombBz) > 0
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

type mvccIterator struct {
	source             *pebble.Iterator
	prefix, start, end []byte
	version            int64
	valid              bool
	reverse            bool
}

func newMVCCIterator(src *pebble.Iterator, prefix, start, end []byte, version int64, earliestVersion int64, reverse bool) *mvccIterator {
	if version < earliestVersion {
		return &mvccIterator{
			source:  src,
			prefix:  prefix,
			start:   start,
			end:     end,
			version: version,
			valid:   false,
			reverse: reverse,
		}
	}

	var valid bool
	if reverse {
		valid = src.Last()
	} else {
		valid = src.First()
	}

	itr := &mvccIterator{
		source:  src,
		prefix:  prefix,
		start:   start,
		end:     end,
		version: version,
		valid:   valid,
		reverse: reverse,
	}

	if valid {
		currKey, currKeyVersion, ok := splitMVCCKey(itr.source.Key())
		if !ok {
			panic(fmt.Sprintf("invalid pebbledb mvcc key: %q", itr.source.Key()))
		}
		currKeyVersionDecoded, err := decodeUint64Ascending(currKeyVersion)
		if err != nil {
			itr.valid = false
			return itr
		}
		if currKeyVersionDecoded > itr.version {
			itr.Next()
		} else {
			itr.valid = itr.source.SeekLT(mvccEncode(currKey, itr.version+1))
		}
	}

	if itr.valid && valTombstoned(itr.source.Value()) {
		if reverse {
			itr.nextReverse()
		} else {
			itr.nextForward()
		}
	}

	return itr
}

func (itr *mvccIterator) Domain() ([]byte, []byte) {
	return itr.start, itr.end
}

func (itr *mvccIterator) Valid() bool {
	return itr.valid
}

func (itr *mvccIterator) Next() {
	itr.assertIsValid()
	if itr.reverse {
		itr.nextReverse()
		return
	}
	itr.nextForward()
}

func (itr *mvccIterator) nextForward() {
	if !itr.source.Valid() {
		itr.valid = false
		return
	}

	currKey, _, ok := splitMVCCKey(itr.source.Key())
	if !ok {
		panic(fmt.Sprintf("invalid pebbledb mvcc key: %q", itr.source.Key()))
	}

	next := itr.source.NextPrefix()
	if next {
		nextKey, _, ok := splitMVCCKey(itr.source.Key())
		if !ok {
			itr.valid = false
			return
		}
		if !bytes.HasPrefix(nextKey, itr.prefix) {
			itr.valid = false
			return
		}

		itr.valid = itr.source.SeekLT(mvccEncode(nextKey, itr.version+1))
		tmpKey, tmpKeyVersion, ok := splitMVCCKey(itr.source.Key())
		if !ok {
			itr.valid = false
			return
		}
		if bytes.Equal(tmpKey, currKey) {
			if itr.source.NextPrefix() {
				itr.nextForward()
				return
			}
			itr.valid = false
			return
		}
		tmpKeyVersionDecoded, err := decodeUint64Ascending(tmpKeyVersion)
		if err != nil {
			itr.valid = false
			return
		}
		if tmpKeyVersionDecoded > itr.version {
			itr.nextForward()
			return
		}
		if itr.valid && itr.cursorTombstoned() {
			itr.nextForward()
		}
		return
	}

	itr.valid = false
}

func (itr *mvccIterator) nextReverse() {
	if !itr.source.Valid() {
		itr.valid = false
		return
	}

	currKey, _, ok := splitMVCCKey(itr.source.Key())
	if !ok {
		panic(fmt.Sprintf("invalid pebbledb mvcc key: %q", itr.source.Key()))
	}

	next := itr.source.SeekLT(mvccEncode(currKey, 0))
	if next {
		nextKey, nextKeyVersion, ok := splitMVCCKey(itr.source.Key())
		if !ok {
			itr.valid = false
			return
		}
		if !bytes.HasPrefix(nextKey, itr.prefix) {
			itr.valid = false
			return
		}
		if bytes.Equal(nextKey, currKey) {
			itr.valid = itr.source.SeekLT(mvccEncode(nextKey, 0))
			if !itr.valid {
				return
			}
			nextKey, nextKeyVersion, ok = splitMVCCKey(itr.source.Key())
			if !ok {
				itr.valid = false
				return
			}
			if !bytes.HasPrefix(nextKey, itr.prefix) {
				itr.valid = false
				return
			}
		}

		nextKeyVersionDecoded, err := decodeUint64Ascending(nextKeyVersion)
		if err != nil {
			itr.valid = false
			return
		}
		if nextKeyVersionDecoded > itr.version {
			itr.valid = itr.source.SeekLT(mvccEncode(nextKey, itr.version+1))
			if !itr.valid {
				return
			}
		}
		if itr.valid && itr.cursorTombstoned() {
			itr.nextReverse()
		}
		return
	}
	itr.valid = false
}

func (itr *mvccIterator) Key() []byte {
	itr.assertIsValid()
	key, _, ok := splitMVCCKey(itr.source.Key())
	if !ok {
		panic(fmt.Sprintf("invalid pebbledb mvcc key: %q", itr.source.Key()))
	}
	keyCopy := append([]byte(nil), key...)
	return keyCopy[len(itr.prefix):]
}

func (itr *mvccIterator) Value() []byte {
	itr.assertIsValid()
	val, _, ok := splitMVCCKey(itr.source.Value())
	if !ok {
		panic(fmt.Sprintf("invalid pebbledb mvcc value: %q", itr.source.Key()))
	}
	return append([]byte(nil), val...)
}

func (itr *mvccIterator) Error() error {
	return itr.source.Error()
}

func (itr *mvccIterator) Close() error {
	return itr.source.Close()
}

func (itr *mvccIterator) assertIsValid() {
	if !itr.valid {
		panic("invalid iterator")
	}
}

func (itr *mvccIterator) cursorTombstoned() bool {
	return valTombstoned(itr.source.Value())
}
