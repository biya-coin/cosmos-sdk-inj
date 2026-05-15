package rootmulti

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	dbm "github.com/cosmos/cosmos-db"

	"cosmossdk.io/store/types"
)

const (
	ssLatestVersionKey   = "seidb/ss/latest_version"
	ssEarliestVersionKey = "seidb/ss/earliest_version"
	ssVersionBasePrefix  = "seidb/ss/version_base/"
	ssDataPrefix         = "seidb/ss/data/"
)

type pebbleStateStore struct {
	mtx            sync.RWMutex
	db             dbm.DB
	keepRecent     int64
	version        int64
	earliest       int64
	state          map[string]map[string][]byte
	versionBase    map[int64]int64
	closeOnShutdown bool
}

func newPebbleStateStore(cfg Config) (StateStore, error) {
	if cfg.Home == "" {
		return nil, fmt.Errorf("seidb home must not be empty for pebbledb state store")
	}

	dataDir := filepath.Join(cfg.Home, "data")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create seidb data dir: %w", err)
	}

	ssDB, err := dbm.NewDB("seidb-state-store", dbm.PebbleDBBackend, dataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to open pebbledb state store: %w", err)
	}

	store := &pebbleStateStore{
		db:              ssDB,
		keepRecent:      int64(cfg.KeepRecent),
		state:           make(map[string]map[string][]byte),
		versionBase:     make(map[int64]int64),
		closeOnShutdown: true,
	}
	store.loadVersionMetadata()
	if store.version > 0 {
		baseVersion := store.resolveBaseVersionLocked(store.version)
		if baseVersion > 0 {
			store.state = store.loadSnapshot(baseVersion)
		}
	}

	return store, nil
}

func (s *pebbleStateStore) Snapshot(storeName string, version int64) (map[string][]byte, bool) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	if !s.hasVersionLocked(version) {
		return nil, false
	}
	baseVersion := s.resolveBaseVersionLocked(version)
	if baseVersion <= 0 {
		return nil, false
	}

	snapshot := s.loadSnapshot(baseVersion)
	store, ok := snapshot[storeName]
	if !ok {
		return map[string][]byte{}, true
	}

	return cloneStateMap(store), true
}

func (s *pebbleStateStore) HasVersion(version int64) bool {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return s.hasVersionLocked(version)
}

func (s *pebbleStateStore) EarliestVersion() int64 {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return s.earliest
}

func (s *pebbleStateStore) ApplyChangeSets(version int64, changeSets []*NamedChangeSet) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if err := s.validateNextVersionLocked(version); err != nil {
		return err
	}

	for _, named := range changeSets {
		if named == nil || named.ChangeSet == nil || len(named.ChangeSet.Pairs) == 0 {
			continue
		}
		storeState := s.ensureStoreState(named.Name)
		for _, pair := range named.ChangeSet.Pairs {
			if pair == nil {
				continue
			}
			key := string(pair.GetKey())
			if pair.Delete {
				delete(storeState, key)
				continue
			}
			storeState[key] = cloneBytesNonNil(pair.GetValue())
		}
	}

	if err := s.persistVersionSnapshotLocked(version); err != nil {
		return err
	}
	s.versionBase[version] = version
	return s.commitVersionLocked(version)
}

func (s *pebbleStateStore) SetLatestVersion(version int64) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if version <= 0 {
		return nil
	}

	if version <= s.version {
		return nil
	}

	if err := s.validateNextVersionLocked(version); err != nil {
		return err
	}

	baseVersion := s.version
	if baseVersion <= 0 {
		baseVersion = version
	}
	s.versionBase[version] = baseVersion
	return s.commitVersionLocked(version)
}

func (s *pebbleStateStore) RollbackToVersion(target int64) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if target <= 0 {
		return nil
	}
	if s.version == 0 || target >= s.version {
		return nil
	}
	if target < s.earliest {
		return fmt.Errorf("rollback target version %d is below earliest version %d", target, s.earliest)
	}

	batch := s.db.NewBatch()
	defer batch.Close()

	for version := target + 1; version <= s.version; version++ {
		if err := s.deleteVersionDataInBatch(batch, version); err != nil {
			return err
		}
		if err := batch.Delete(ssVersionBaseKey(version)); err != nil {
			return err
		}
		delete(s.versionBase, version)
	}

	s.version = target
	baseVersion := s.resolveBaseVersionLocked(target)
	if baseVersion > 0 {
		s.state = s.loadSnapshot(baseVersion)
	} else {
		s.state = make(map[string]map[string][]byte)
	}

	if err := batch.Set([]byte(ssLatestVersionKey), uint64ToBytes(uint64(s.version))); err != nil {
		return err
	}
	if err := batch.Set([]byte(ssEarliestVersionKey), uint64ToBytes(uint64(s.earliest))); err != nil {
		return err
	}

	return batch.WriteSync()
}

func (s *pebbleStateStore) SyncFromStores(stores map[types.StoreKey]types.CommitKVStore, version int64) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	nextState := make(map[string]map[string][]byte)
	for key, store := range stores {
		if key == nil || store == nil {
			continue
		}
		name := key.Name()
		pairs := make(map[string][]byte)
		it := store.Iterator(nil, nil)
		for ; it.Valid(); it.Next() {
			pairs[string(it.Key())] = cloneBytesNonNil(it.Value())
		}
		it.Close()
		nextState[name] = pairs
	}

	if version <= 0 {
		return nil
	}

	batch := s.db.NewBatch()
	defer batch.Close()
	for v := s.earliest; v <= s.version; v++ {
		if v <= 0 {
			continue
		}
		if err := s.deleteVersionDataInBatch(batch, v); err != nil {
			return err
		}
		if err := batch.Delete(ssVersionBaseKey(v)); err != nil {
			return err
		}
	}

	s.state = nextState
	s.version = 0
	s.earliest = 0
	s.versionBase = make(map[int64]int64)

	if err := s.persistVersionSnapshotInBatch(batch, version); err != nil {
		return err
	}
	s.versionBase[version] = version

	s.version = version
	s.earliest = version
	if err := batch.Set([]byte(ssLatestVersionKey), uint64ToBytes(uint64(version))); err != nil {
		return err
	}
	if err := batch.Set([]byte(ssEarliestVersionKey), uint64ToBytes(uint64(version))); err != nil {
		return err
	}
	return batch.WriteSync()
}

func (s *pebbleStateStore) ImportSnapshot(version int64, nodes <-chan SnapshotImportNode) error {
	if version <= 0 {
		return fmt.Errorf("invalid snapshot version: %d", version)
	}

	nextState := make(map[string]map[string][]byte)
	for node := range nodes {
		if node.StoreKey == "" {
			continue
		}
		storeState, ok := nextState[node.StoreKey]
		if !ok {
			storeState = make(map[string][]byte)
			nextState[node.StoreKey] = storeState
		}
		storeState[string(node.Key)] = cloneBytesNonNil(node.Value)
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()

	batch := s.db.NewBatch()
	defer batch.Close()

	for v := s.earliest; v <= s.version; v++ {
		if v <= 0 {
			continue
		}
		if err := s.deleteVersionDataInBatch(batch, v); err != nil {
			return err
		}
		if err := batch.Delete(ssVersionBaseKey(v)); err != nil {
			return err
		}
	}

	s.state = nextState
	s.version = 0
	s.earliest = 0
	s.versionBase = make(map[int64]int64)

	if err := s.persistVersionSnapshotInBatch(batch, version); err != nil {
		return err
	}
	s.versionBase[version] = version
	s.version = version
	s.earliest = version

	if err := batch.Set([]byte(ssLatestVersionKey), uint64ToBytes(uint64(version))); err != nil {
		return err
	}
	if err := batch.Set([]byte(ssEarliestVersionKey), uint64ToBytes(uint64(version))); err != nil {
		return err
	}
	return batch.WriteSync()
}

func (s *pebbleStateStore) Close() error {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if !s.closeOnShutdown || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *pebbleStateStore) loadVersionMetadata() {
	latest, err := s.db.Get([]byte(ssLatestVersionKey))
	if err == nil && len(latest) == 8 {
		s.version = int64(bytesToUint64(latest))
	}
	earliest, err := s.db.Get([]byte(ssEarliestVersionKey))
	if err == nil && len(earliest) == 8 {
		s.earliest = int64(bytesToUint64(earliest))
	}
	if s.version > 0 && s.earliest == 0 {
		s.earliest = s.version
	}
	for version := s.earliest; version <= s.version; version++ {
		if version <= 0 {
			continue
		}
		baseBytes, err := s.db.Get(ssVersionBaseKey(version))
		if err != nil || len(baseBytes) != 8 {
			// Backward compatibility with previous format where each version
			// stored a full snapshot without base indirection.
			s.versionBase[version] = version
			continue
		}
		base := int64(bytesToUint64(baseBytes))
		if base <= 0 {
			base = version
		}
		s.versionBase[version] = base
	}
}

func (s *pebbleStateStore) validateNextVersionLocked(version int64) error {
	if version <= 0 {
		return fmt.Errorf("version must be positive")
	}
	if s.version > 0 && version <= s.version {
		return fmt.Errorf("version must increase monotonically, latest=%d got=%d", s.version, version)
	}
	return nil
}

func (s *pebbleStateStore) commitVersionLocked(version int64) error {
	s.version = version
	if s.earliest == 0 {
		s.earliest = version
	}

	if err := s.pruneOldVersionsLocked(); err != nil {
		return err
	}

	batch := s.db.NewBatch()
	defer batch.Close()
	baseVersion := s.versionBase[version]
	if baseVersion <= 0 {
		baseVersion = version
		s.versionBase[version] = baseVersion
	}
	if err := batch.Set(ssVersionBaseKey(version), uint64ToBytes(uint64(baseVersion))); err != nil {
		return err
	}
	if err := batch.Set([]byte(ssLatestVersionKey), uint64ToBytes(uint64(s.version))); err != nil {
		return err
	}
	if err := batch.Set([]byte(ssEarliestVersionKey), uint64ToBytes(uint64(s.earliest))); err != nil {
		return err
	}
	return batch.WriteSync()
}

func (s *pebbleStateStore) pruneOldVersionsLocked() error {
	if s.keepRecent <= 0 || s.version <= s.keepRecent {
		return nil
	}
	pruneTo := s.version - s.keepRecent
	if pruneTo < s.earliest {
		return nil
	}

	batch := s.db.NewBatch()
	defer batch.Close()
	for version := s.earliest; version <= pruneTo; version++ {
		if err := s.deleteVersionDataInBatch(batch, version); err != nil {
			return err
		}
		if err := batch.Delete(ssVersionBaseKey(version)); err != nil {
			return err
		}
		delete(s.versionBase, version)
	}
	if err := batch.WriteSync(); err != nil {
		return err
	}

	s.earliest = pruneTo + 1
	return nil
}

func (s *pebbleStateStore) hasVersionLocked(version int64) bool {
	return version > 0 && s.version > 0 && version <= s.version && (s.earliest == 0 || version >= s.earliest)
}

func (s *pebbleStateStore) ensureStoreState(storeName string) map[string][]byte {
	storeState, ok := s.state[storeName]
	if !ok {
		storeState = make(map[string][]byte)
		s.state[storeName] = storeState
	}
	return storeState
}

func (s *pebbleStateStore) resolveBaseVersionLocked(version int64) int64 {
	if version <= 0 || version > s.version {
		return 0
	}

	current := version
	visited := make(map[int64]struct{})
	for {
		if _, seen := visited[current]; seen {
			return 0
		}
		visited[current] = struct{}{}

		baseVersion, ok := s.versionBase[current]
		if !ok || baseVersion <= 0 {
			// Backward-compatible fallback for historical data before base index.
			return current
		}
		if baseVersion == current {
			return current
		}
		current = baseVersion
	}
}

func (s *pebbleStateStore) persistVersionSnapshotLocked(version int64) error {
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := s.persistVersionSnapshotInBatch(batch, version); err != nil {
		return err
	}
	return batch.WriteSync()
}

func (s *pebbleStateStore) persistVersionSnapshotInBatch(batch dbm.Batch, version int64) error {
	for storeName, kvs := range s.state {
		for key, value := range kvs {
			if err := batch.Set(ssDataKey(version, storeName, []byte(key)), cloneBytesNonNil(value)); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *pebbleStateStore) deleteVersionDataInBatch(batch dbm.Batch, version int64) error {
	start := ssVersionPrefix(version)
	end := append(append([]byte{}, start...), 0xFF)
	it, err := s.db.Iterator(start, end)
	if err != nil {
		return err
	}
	for ; it.Valid(); it.Next() {
		if err := batch.Delete(cloneBytesNonNil(it.Key())); err != nil {
			it.Close()
			return err
		}
	}
	it.Close()
	return nil
}

func (s *pebbleStateStore) loadSnapshot(version int64) map[string]map[string][]byte {
	result := make(map[string]map[string][]byte)
	start := ssVersionPrefix(version)
	end := append(append([]byte{}, start...), 0xFF)

	it, err := s.db.Iterator(start, end)
	if err != nil {
		return result
	}
	defer it.Close()

	for ; it.Valid(); it.Next() {
		storeName, key, ok := parseSSDataKey(it.Key())
		if !ok {
			continue
		}
		storeState, exists := result[storeName]
		if !exists {
			storeState = make(map[string][]byte)
			result[storeName] = storeState
		}
		storeState[string(key)] = cloneBytesNonNil(it.Value())
	}

	return result
}

func ssDataKey(version int64, storeName string, key []byte) []byte {
	storeNameBytes := []byte(storeName)
	buf := make([]byte, 0, len(ssDataPrefix)+8+4+len(storeNameBytes)+len(key))
	buf = append(buf, []byte(ssDataPrefix)...)
	versionBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(versionBytes, uint64(version))
	buf = append(buf, versionBytes...)
	storeLen := make([]byte, 4)
	binary.BigEndian.PutUint32(storeLen, uint32(len(storeNameBytes)))
	buf = append(buf, storeLen...)
	buf = append(buf, storeNameBytes...)
	buf = append(buf, key...)
	return buf
}

func ssVersionPrefix(version int64) []byte {
	buf := make([]byte, 0, len(ssDataPrefix)+8)
	buf = append(buf, []byte(ssDataPrefix)...)
	versionBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(versionBytes, uint64(version))
	buf = append(buf, versionBytes...)
	return buf
}

func ssVersionBaseKey(version int64) []byte {
	buf := make([]byte, 0, len(ssVersionBasePrefix)+8)
	buf = append(buf, []byte(ssVersionBasePrefix)...)
	versionBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(versionBytes, uint64(version))
	buf = append(buf, versionBytes...)
	return buf
}

func parseSSDataKey(raw []byte) (storeName string, key []byte, ok bool) {
	prefix := []byte(ssDataPrefix)
	if len(raw) < len(prefix)+8+4 {
		return "", nil, false
	}
	if string(raw[:len(prefix)]) != ssDataPrefix {
		return "", nil, false
	}
	idx := len(prefix) + 8
	storeNameLen := int(binary.BigEndian.Uint32(raw[idx : idx+4]))
	idx += 4
	if storeNameLen < 0 || idx+storeNameLen > len(raw) {
		return "", nil, false
	}
	storeName = string(raw[idx : idx+storeNameLen])
	idx += storeNameLen
	if idx > len(raw) {
		return "", nil, false
	}
	key = cloneBytesNonNil(raw[idx:])
	return storeName, key, true
}

func cloneStateMap(src map[string][]byte) map[string][]byte {
	out := make(map[string][]byte, len(src))
	for k, v := range src {
		out[k] = cloneBytesNonNil(v)
	}
	return out
}
