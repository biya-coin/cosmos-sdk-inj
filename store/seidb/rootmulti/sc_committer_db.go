package rootmulti

import (
	"encoding/binary"
	"fmt"
	"strings"
	"sync"

	dbm "github.com/cosmos/cosmos-db"

	"cosmossdk.io/store/types"
)

const (
	scLatestVersionKey = "seidb/sc/latest-version"
	scDataPrefix       = "seidb/sc/data/"
)

type dbSCCommitter struct {
	mtx sync.RWMutex

	db      dbm.DB
	pending []*NamedChangeSet
	version int64
	earliestVersion int64
	keepRecent      uint64
	state   map[string]map[string][]byte
	history map[int64]map[string]map[string][]byte
}

func newDBSCCommitter(db dbm.DB, keepRecent uint64) *dbSCCommitter {
	c := &dbSCCommitter{
		db:         db,
		keepRecent: keepRecent,
		state:      make(map[string]map[string][]byte),
		history:    make(map[int64]map[string]map[string][]byte),
	}
	if db == nil {
		return c
	}
	bz, err := db.Get([]byte(scLatestVersionKey))
	if err == nil && len(bz) == 8 {
		c.version = int64(bytesToUint64(bz))
		c.earliestVersion = c.version
	}
	if err := c.loadStateFromDB(); err == nil && c.version > 0 {
		c.history[c.version] = cloneState(c.state)
	}
	return c
}

func (c *dbSCCommitter) ApplyChangeSets(changeSets []*NamedChangeSet) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	c.pending = c.pending[:0]
	for _, named := range changeSets {
		if named == nil || named.ChangeSet == nil || len(named.ChangeSet.Pairs) == 0 {
			continue
		}
		c.pending = append(c.pending, named)
	}
	return nil
}

func (c *dbSCCommitter) Commit(version int64) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	if version <= 0 {
		return fmt.Errorf("invalid commit version: %d", version)
	}
	if version <= c.version {
		return fmt.Errorf("non-monotonic commit version: current=%d next=%d", c.version, version)
	}

	for _, named := range c.pending {
		moduleStore, ok := c.state[named.Name]
		if !ok {
			moduleStore = make(map[string][]byte)
			c.state[named.Name] = moduleStore
		}
		for _, pair := range named.ChangeSet.Pairs {
			if pair == nil {
				continue
			}
			key := string(pair.Key)
			if pair.Delete {
				delete(moduleStore, key)
				continue
			}
			moduleStore[key] = cloneBytesNonNil(pair.Value)
		}
	}

	if c.db == nil {
		c.version = version
		c.history[version] = cloneState(c.state)
		c.pending = c.pending[:0]
		return nil
	}

	batch := c.db.NewBatch()
	defer func() { _ = batch.Close() }()

	for _, named := range c.pending {
		for _, pair := range named.ChangeSet.Pairs {
			if pair == nil {
				continue
			}
			key := scDataKey(named.Name, pair.Key)
			if pair.Delete {
				if err := batch.Delete(key); err != nil {
					return err
				}
				continue
			}
			if err := batch.Set(key, cloneBytesNonNil(pair.Value)); err != nil {
				return err
			}
		}
	}

	if err := batch.Set([]byte(scLatestVersionKey), uint64ToBytes(uint64(version))); err != nil {
		return err
	}
	if err := batch.WriteSync(); err != nil {
		return err
	}

	c.version = version
	c.history[version] = cloneState(c.state)
	c.pruneHistoryLocked(version)
	c.pending = c.pending[:0]
	return nil
}

func (c *dbSCCommitter) snapshot(storeName string, version int64) (map[string][]byte, bool) {
	verState, ok := c.history[version]
	if !ok {
		return nil, false
	}
	storeState, ok := verState[storeName]
	if !ok {
		return map[string][]byte{}, true
	}
	out := make(map[string][]byte, len(storeState))
	for k, v := range storeState {
		out[k] = cloneBytesNonNil(v)
	}
	return out, true
}

func (c *dbSCCommitter) Snapshot(storeName string, version int64) (map[string][]byte, bool) {
	c.mtx.RLock()
	defer c.mtx.RUnlock()
	return c.snapshot(storeName, version)
}

func (c *dbSCCommitter) HasVersion(version int64) bool {
	c.mtx.RLock()
	defer c.mtx.RUnlock()

	if version <= 0 || version > c.version {
		return false
	}
	if c.keepRecent == 0 {
		return true
	}
	return version >= c.earliestVersion
}

func (c *dbSCCommitter) EarliestVersion() int64 {
	c.mtx.RLock()
	defer c.mtx.RUnlock()
	return c.earliestVersion
}

func (c *dbSCCommitter) CurrentVersion() int64 {
	c.mtx.RLock()
	defer c.mtx.RUnlock()
	return c.version
}

func (c *dbSCCommitter) LatestVersion() int64 {
	return c.CurrentVersion()
}

func (c *dbSCCommitter) RollbackToVersion(target int64) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	if target <= 0 {
		return fmt.Errorf("invalid rollback target: %d", target)
	}
	if target > c.version {
		return fmt.Errorf("rollback target %d is greater than current version %d", target, c.version)
	}
	if !c.hasVersionLocked(target) {
		return fmt.Errorf("rollback target %d is unavailable in state store", target)
	}

	targetSnapshot, ok := c.history[target]
	if !ok {
		return fmt.Errorf("rollback target %d snapshot not found", target)
	}

	if c.db != nil {
		batch := c.db.NewBatch()
		defer func() { _ = batch.Close() }()
		if err := batch.Set([]byte(scLatestVersionKey), uint64ToBytes(uint64(target))); err != nil {
			return err
		}
		if err := batch.WriteSync(); err != nil {
			return err
		}
	}

	c.state = cloneState(targetSnapshot)
	for v := range c.history {
		if v > target {
			delete(c.history, v)
		}
	}
	c.version = target
	c.recomputeEarliestLocked()
	return nil
}

func (c *dbSCCommitter) pruneHistoryLocked(latest int64) {
	if c.keepRecent == 0 {
		c.recomputeEarliestLocked()
		return
	}
	minVersion := latest - int64(c.keepRecent) + 1
	if minVersion < 1 {
		minVersion = 1
	}
	for v := range c.history {
		if v < minVersion {
			delete(c.history, v)
		}
	}
	c.recomputeEarliestLocked()
}

func (c *dbSCCommitter) hasVersionLocked(version int64) bool {
	if version <= 0 || version > c.version {
		return false
	}
	if c.keepRecent == 0 {
		return true
	}
	return version >= c.earliestVersion
}

func (c *dbSCCommitter) recomputeEarliestLocked() {
	if len(c.history) == 0 {
		if c.version > 0 {
			c.earliestVersion = c.version
		} else {
			c.earliestVersion = 0
		}
		return
	}
	min := int64(0)
	for v := range c.history {
		if min == 0 || v < min {
			min = v
		}
	}
	c.earliestVersion = min
}

func (c *dbSCCommitter) loadStateFromDB() error {
	if c.db == nil {
		return nil
	}
	itr, err := c.db.Iterator([]byte(scDataPrefix), []byte(scDataPrefix+"~"))
	if err != nil {
		return err
	}
	defer func() { _ = itr.Close() }()

	for ; itr.Valid(); itr.Next() {
		storeName, storeKey, ok := parseSCDataKey(itr.Key())
		if !ok {
			continue
		}
		if _, ok := c.state[storeName]; !ok {
			c.state[storeName] = make(map[string][]byte)
		}
		c.state[storeName][string(storeKey)] = cloneBytesNonNil(itr.Value())
	}
	return nil
}

func (c *dbSCCommitter) SyncFromStores(stores map[types.StoreKey]types.CommitKVStore, version int64) error {
	if version <= 0 {
		return fmt.Errorf("invalid sync version: %d", version)
	}

	c.mtx.Lock()
	defer c.mtx.Unlock()

	newState := make(map[string]map[string][]byte)
	for key, store := range stores {
		if store == nil || store.GetStoreType() != types.StoreTypeIAVL {
			continue
		}
		moduleName := key.Name()
		moduleData := make(map[string][]byte)

		itr := store.Iterator(nil, nil)
		for ; itr.Valid(); itr.Next() {
			moduleData[string(itr.Key())] = cloneBytesNonNil(itr.Value())
		}
		if err := itr.Close(); err != nil {
			return err
		}
		newState[moduleName] = moduleData
	}

	c.state = newState
	c.history = map[int64]map[string]map[string][]byte{
		version: cloneState(newState),
	}
	c.version = version
	c.earliestVersion = version
	c.pending = c.pending[:0]

	if c.db == nil {
		return nil
	}

	batch := c.db.NewBatch()
	defer func() { _ = batch.Close() }()

	prefixItr, err := c.db.Iterator([]byte(scDataPrefix), []byte(scDataPrefix+"~"))
	if err != nil {
		return err
	}
	for ; prefixItr.Valid(); prefixItr.Next() {
		if err := batch.Delete(prefixItr.Key()); err != nil {
			_ = prefixItr.Close()
			return err
		}
	}
	if err := prefixItr.Close(); err != nil {
		return err
	}

	for module, kvs := range newState {
		for k, v := range kvs {
			if err := batch.Set(scDataKey(module, []byte(k)), cloneBytesNonNil(v)); err != nil {
				return err
			}
		}
	}

	if err := batch.Set([]byte(scLatestVersionKey), uint64ToBytes(uint64(version))); err != nil {
		return err
	}
	if err := batch.WriteSync(); err != nil {
		return err
	}
	return nil
}

func cloneState(in map[string]map[string][]byte) map[string]map[string][]byte {
	out := make(map[string]map[string][]byte, len(in))
	for storeName, kvs := range in {
		copied := make(map[string][]byte, len(kvs))
		for k, v := range kvs {
			copied[k] = cloneBytesNonNil(v)
		}
		out[storeName] = copied
	}
	return out
}

func cloneBytesNonNil(bz []byte) []byte {
	if bz == nil || len(bz) == 0 {
		return []byte{}
	}
	return append([]byte(nil), bz...)
}

func scDataKey(storeName string, key []byte) []byte {
	p := []byte(scDataPrefix)
	out := make([]byte, 0, len(p)+4+len(storeName)+len(key))
	out = append(out, p...)
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(storeName)))
	out = append(out, lenBuf[:]...)
	out = append(out, storeName...)
	out = append(out, key...)
	return out
}

func parseSCDataKey(raw []byte) (string, []byte, bool) {
	prefix := []byte(scDataPrefix)
	if len(raw) < len(prefix) || string(raw[:len(prefix)]) != scDataPrefix {
		return "", nil, false
	}

	rest := raw[len(prefix):]
	if len(rest) >= 4 {
		nameLen := int(binary.BigEndian.Uint32(rest[:4]))
		if nameLen >= 0 && len(rest) >= 4+nameLen {
			storeName := string(rest[4 : 4+nameLen])
			storeKey := append([]byte(nil), rest[4+nameLen:]...)
			return storeName, storeKey, true
		}
	}

	// Backward compatibility with old format: seidb/sc/data/<store>/<key>
	legacyRest := string(rest)
	parts := strings.SplitN(legacyRest, "/", 2)
	if len(parts) != 2 {
		return "", nil, false
	}
	return parts[0], []byte(parts[1]), true
}

func uint64ToBytes(v uint64) []byte {
	b := make([]byte, 8)
	b[0] = byte(v >> 56)
	b[1] = byte(v >> 48)
	b[2] = byte(v >> 40)
	b[3] = byte(v >> 32)
	b[4] = byte(v >> 24)
	b[5] = byte(v >> 16)
	b[6] = byte(v >> 8)
	b[7] = byte(v)
	return b
}

func bytesToUint64(b []byte) uint64 {
	return uint64(b[0])<<56 |
		uint64(b[1])<<48 |
		uint64(b[2])<<40 |
		uint64(b[3])<<32 |
		uint64(b[4])<<24 |
		uint64(b[5])<<16 |
		uint64(b[6])<<8 |
		uint64(b[7])
}
