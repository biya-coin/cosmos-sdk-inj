package rootmulti

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"cosmossdk.io/log"
	"cosmossdk.io/store/metrics"
	"cosmossdk.io/store/seidb/commitment"
	"cosmossdk.io/store/seidb/sc/memiavl"
	"cosmossdk.io/store/types"
	dbm "github.com/cosmos/cosmos-db"
	"github.com/stretchr/testify/require"
)

const testStateStoreBackend = "scbacked-test"

var registerStateStoreBackendOnce sync.Once

func testMemIAVLConfig(t *testing.T) Config {
	t.Helper()
	return Config{
		Home:                   t.TempDir(),
		StateCommitmentBackend: "memiavl",
		StateStoreBackend:      "pebbledb",
		MemIAVL:                memiavl.DefaultConfig(),
	}
}

func ensureTestStateStoreBackend(t *testing.T) string {
	t.Helper()
	registerStateStoreBackendOnce.Do(func() {
		err := RegisterStateStoreBuilder(testStateStoreBackend, func(_ dbm.DB, _ Config, scStore SCStore) (StateStore, error) {
			return newSCBackedStateStore(scStore), nil
		})
		require.NoError(t, err)
	})
	return testStateStoreBackend
}

type failingSCStore struct{}

func (failingSCStore) ApplyChangeSets(_ []*NamedChangeSet) error { return nil }
func (failingSCStore) Commit(_ int64) error                      { return fmt.Errorf("forced sc failure") }

type memorySCStore struct {
	mtx     sync.Mutex
	version int64
	data    map[string]map[string][]byte
}

func newMemorySCStore() *memorySCStore {
	return &memorySCStore{data: make(map[string]map[string][]byte)}
}

func (m *memorySCStore) ApplyChangeSets(changeSets []*NamedChangeSet) error {
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

func (m *memorySCStore) Commit(version int64) error {
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

type rollbackTrackingSCStore struct {
	rollbackCalled bool
	rollbackErr    error
	hasVersion      bool
	hasVersionCalled bool
	currentVersion   int64
}

func (c *rollbackTrackingSCStore) ApplyChangeSets(_ []*NamedChangeSet) error { return nil }
func (c *rollbackTrackingSCStore) Commit(_ int64) error                      { return nil }
func (c *rollbackTrackingSCStore) RollbackToVersion(_ int64) error {
	c.rollbackCalled = true
	return c.rollbackErr
}
func (c *rollbackTrackingSCStore) HasVersion(_ int64) bool {
	c.hasVersionCalled = true
	return c.hasVersion
}
func (c *rollbackTrackingSCStore) CurrentVersion() int64 {
	return c.currentVersion
}

type rollbackTrackingStateStore struct {
	rollbackCalled bool
	rollbackErr    error
	earliest       int64
	hasVersion      bool
	hasVersionCalled bool
}

func (s *rollbackTrackingStateStore) Get(_ string, _ int64, _ []byte) ([]byte, error) {
	return nil, nil
}
func (s *rollbackTrackingStateStore) Has(_ string, _ int64, _ []byte) (bool, error) {
	return false, nil
}
func (s *rollbackTrackingStateStore) Iterator(_ string, _ int64, _, _ []byte) (types.Iterator, error) {
	return emptyIterator{}, nil
}
func (s *rollbackTrackingStateStore) ReverseIterator(_ string, _ int64, _, _ []byte) (types.Iterator, error) {
	return emptyIterator{}, nil
}
func (s *rollbackTrackingStateStore) Snapshot(_ string, _ int64) (map[string][]byte, bool) {
	return nil, false
}
func (s *rollbackTrackingStateStore) HasVersion(_ int64) bool {
	s.hasVersionCalled = true
	return s.hasVersion
}
func (s *rollbackTrackingStateStore) EarliestVersion() int64  { return s.earliest }
func (s *rollbackTrackingStateStore) ApplyChangeSets(_ int64, _ []*NamedChangeSet) error {
	return nil
}
func (s *rollbackTrackingStateStore) SetLatestVersion(_ int64) error { return nil }
func (s *rollbackTrackingStateStore) RollbackToVersion(_ int64) error {
	s.rollbackCalled = true
	return s.rollbackErr
}
func (s *rollbackTrackingStateStore) SyncFromStores(_ map[types.StoreKey]types.CommitKVStore, _ int64) error {
	return nil
}
func (s *rollbackTrackingStateStore) Close() error { return nil }

func TestNewStore_ConfigRoundTrip(t *testing.T) {
	db := dbm.NewMemDB()
	cfg := testMemIAVLConfig(t)
	cfg.KeepRecent = 128
	cfg.HistoricalProofQueryMaxConcurrency = 4

	store := NewStore(db, log.NewNopLogger(), metrics.NewNoOpMetrics(), cfg)
	require.NotNil(t, store)
	require.Equal(t, cfg, store.Config())
}

func TestNewStore_UsesStoreV2Runtime(t *testing.T) {
	db := dbm.NewMemDB()
	store := NewStore(db, log.NewNopLogger(), metrics.NewNoOpMetrics(), Config{})
	_, ok := store.runtime.(*storev2Runtime)
	require.True(t, ok)
}

func TestStore_PanicsOnUnknownStateStoreBackend(t *testing.T) {
	db := dbm.NewMemDB()
	require.Panics(t, func() {
		_ = NewStore(db, log.NewNopLogger(), metrics.NewNoOpMetrics(), Config{
			Home:              t.TempDir(),
			StateStoreBackend: "unknown-ss-backend",
		})
	})
}

func TestApplyStoreUpgrades_RemovesDeletedAndRenamedOldKeys(t *testing.T) {
	db := dbm.NewMemDB()
	store := NewStore(db, log.NewNopLogger(), metrics.NewNoOpMetrics(), Config{
		StateStoreBackend: "",
	})

	oldDelete := types.NewKVStoreKey("delete-old")
	oldRename := types.NewKVStoreKey("rename-old")
	newRename := types.NewKVStoreKey("rename-new")

	store.storesParams[oldDelete] = storeParams{key: oldDelete, typ: types.StoreTypeIAVL}
	store.storesParams[oldRename] = storeParams{key: oldRename, typ: types.StoreTypeIAVL}
	store.storesParams[newRename] = storeParams{key: newRename, typ: types.StoreTypeIAVL}
	store.storeKeys["delete-old"] = oldDelete
	store.storeKeys["rename-old"] = oldRename
	store.storeKeys["rename-new"] = newRename
	store.ckvStore[oldDelete] = nil
	store.ckvStore[oldRename] = nil

	store.applyStoreUpgrades(&types.StoreUpgrades{
		Deleted: []string{"delete-old"},
		Renamed: []types.StoreRename{{OldKey: "rename-old", NewKey: "rename-new"}},
	})

	_, ok := store.storeKeys["delete-old"]
	require.False(t, ok)
	_, ok = store.storeKeys["rename-old"]
	require.False(t, ok)
	_, ok = store.storesParams[oldDelete]
	require.False(t, ok)
	_, ok = store.storesParams[oldRename]
	require.False(t, ok)
	// New key metadata should be preserved.
	_, ok = store.storeKeys["rename-new"]
	require.True(t, ok)
	_, ok = store.storesParams[newRename]
	require.True(t, ok)
}

func TestStoreWrapsIAVLAndConsumesChangeSetOnCommit(t *testing.T) {
	db := dbm.NewMemDB()
	store := NewStore(db, log.NewNopLogger(), metrics.NewNoOpMetrics(), Config{})
	key := types.NewKVStoreKey("test")

	store.MountStoreWithDB(key, types.StoreTypeIAVL, nil)
	require.NoError(t, store.LoadLatestVersion())

	kv := store.GetKVStore(key)
	kv.Set([]byte("a"), []byte("1"))

	cid := store.Commit()
	require.Equal(t, int64(1), cid.Version)

	wrapped, ok := store.GetCommitKVStore(key).(*commitment.Store)
	require.True(t, ok)
	require.Equal(t, []byte("1"), store.GetKVStore(key).Get([]byte("a")))

	// Commit() should have popped pending changesets.
	cs := wrapped.PopChangeSet()
	require.NotNil(t, cs)
	require.Len(t, cs.Pairs, 0)
}

func TestLoadLatestVersion_SkipsNonCommitKVStores(t *testing.T) {
	db := dbm.NewMemDB()
	store := NewStore(db, log.NewNopLogger(), metrics.NewNoOpMetrics(), Config{})

	iavlKey := types.NewKVStoreKey("kv")
	transientKey := types.NewTransientStoreKey("transient")
	memoryKey := types.NewMemoryStoreKey("memory")
	objectKey := types.NewObjectStoreKey("object")

	store.MountStoreWithDB(iavlKey, types.StoreTypeIAVL, nil)
	store.MountStoreWithDB(transientKey, types.StoreTypeTransient, nil)
	store.MountStoreWithDB(memoryKey, types.StoreTypeMemory, nil)
	store.MountStoreWithDB(objectKey, types.StoreTypeObject, nil)

	require.NotPanics(t, func() {
		require.NoError(t, store.LoadLatestVersion())
	})

	_, isWrapped := store.GetCommitKVStore(iavlKey).(*commitment.Store)
	require.True(t, isWrapped)
}

func TestCacheMultiStore_RegistersObjectAndTransientStores(t *testing.T) {
	db := dbm.NewMemDB()
	store := NewStore(db, log.NewNopLogger(), metrics.NewNoOpMetrics(), Config{})

	iavlKey := types.NewKVStoreKey("kv")
	transientKey := types.NewTransientStoreKey("transient")
	objectKey := types.NewObjectStoreKey("object")

	store.MountStoreWithDB(iavlKey, types.StoreTypeIAVL, nil)
	store.MountStoreWithDB(transientKey, types.StoreTypeTransient, nil)
	store.MountStoreWithDB(objectKey, types.StoreTypeObject, nil)

	require.NoError(t, store.LoadLatestVersion())

	cms := store.CacheMultiStore()
	require.NotPanics(t, func() {
		_ = cms.GetKVStore(transientKey)
		_ = cms.GetObjKVStore(objectKey)
	})
}

func TestStore_SCFailurePanicsCommit(t *testing.T) {
	db := dbm.NewMemDB()
	store := NewStore(db, log.NewNopLogger(), metrics.NewNoOpMetrics(), Config{})
	store.SetSCStore(failingSCStore{})
	key := types.NewKVStoreKey("test")

	store.MountStoreWithDB(key, types.StoreTypeIAVL, nil)
	require.NoError(t, store.LoadLatestVersion())

	kv := store.GetKVStore(key)
	kv.Set([]byte("a"), []byte("1"))

	require.Panics(t, func() {
		_ = store.Commit()
	})

	wrapped, ok := store.GetCommitKVStore(key).(*commitment.Store)
	require.True(t, ok)
	require.NotNil(t, wrapped.PendingChangeSet())
}

func TestStore_CommitAcceptsEmptyValue(t *testing.T) {
	db := dbm.NewMemDB()
	store := NewStore(db, log.NewNopLogger(), metrics.NewNoOpMetrics(), testMemIAVLConfig(t))
	key := types.NewKVStoreKey("test")
	store.MountStoreWithDB(key, types.StoreTypeIAVL, nil)
	require.NoError(t, store.LoadLatestVersion())

	kv := store.GetKVStore(key)
	kv.Set([]byte("empty"), []byte{})
	require.NotPanics(t, func() {
		_ = store.Commit()
	})

	latest := store.LastCommitID().Version
	res, err := store.Query(&types.RequestQuery{
		Path:   "/test/key",
		Data:   []byte("empty"),
		Height: latest,
		Prove:  false,
	})
	require.NoError(t, err)
	require.NotNil(t, res)
}

func TestCommit_AppHashFromSC(t *testing.T) {
	db := dbm.NewMemDB()
	store := NewStore(db, log.NewNopLogger(), metrics.NewNoOpMetrics(), testMemIAVLConfig(t))
	key := types.NewKVStoreKey("test")
	store.MountStoreWithDB(key, types.StoreTypeIAVL, nil)
	require.NoError(t, store.LoadLatestVersion())

	kv := store.GetKVStore(key)
	kv.Set([]byte("k"), []byte("v"))
	workingHash := store.WorkingHash()
	cid := store.Commit()
	require.Equal(t, int64(1), cid.Version)
	require.NotEmpty(t, cid.Hash)
	require.Equal(t, workingHash, cid.Hash)

	sc, ok := store.scStore.(*memIAVLStore)
	require.True(t, ok)
	scInfo := convertCommitInfo(sc.LastCommitInfo())
	expected := amendCommitInfo(scInfo, store.storesParams).Hash()
	require.Equal(t, expected, cid.Hash)
	require.Equal(t, cid, store.LastCommitID())
}

func TestCacheMultiStoreWithVersion_UsesStateStoreForHistorical(t *testing.T) {
	db := dbm.NewMemDB()
	store := NewStore(db, log.NewNopLogger(), metrics.NewNoOpMetrics(), testMemIAVLConfig(t))
	key := types.NewKVStoreKey("test")

	store.MountStoreWithDB(key, types.StoreTypeIAVL, nil)
	require.NoError(t, store.LoadLatestVersion())

	kv := store.GetKVStore(key)
	kv.Set([]byte("k"), []byte("v1"))
	c1 := store.Commit()
	require.Equal(t, int64(1), c1.Version)

	kv = store.GetKVStore(key)
	kv.Set([]byte("k"), []byte("v2"))
	c2 := store.Commit()
	require.Equal(t, int64(2), c2.Version)

	cmsV1, err := store.CacheMultiStoreWithVersion(1)
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), cmsV1.GetKVStore(key).Get([]byte("k")))

	cmsV2, err := store.CacheMultiStoreWithVersion(2)
	require.NoError(t, err)
	require.Equal(t, []byte("v2"), cmsV2.GetKVStore(key).Get([]byte("k")))
}

func TestQuery_HistoricalNoProofUsesStateSnapshot(t *testing.T) {
	db := dbm.NewMemDB()
	store := NewStore(db, log.NewNopLogger(), metrics.NewNoOpMetrics(), testMemIAVLConfig(t))
	key := types.NewKVStoreKey("test")

	store.MountStoreWithDB(key, types.StoreTypeIAVL, nil)
	require.NoError(t, store.LoadLatestVersion())

	kv := store.GetKVStore(key)
	kv.Set([]byte("k"), []byte("v1"))
	store.Commit()

	kv = store.GetKVStore(key)
	kv.Set([]byte("k"), []byte("v2"))
	store.Commit()

	res, err := store.Query(&types.RequestQuery{
		Path:   "/test/key",
		Data:   []byte("k"),
		Height: 1,
		Prove:  false,
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), res.Height)
	require.Equal(t, []byte("v1"), res.Value)
}

func TestQuery_HistoricalProofUsesRuntimePath(t *testing.T) {
	db := dbm.NewMemDB()
	// Historical proof still routes through runtime IAVL; keep SC disabled for this test.
	store := NewStore(db, log.NewNopLogger(), metrics.NewNoOpMetrics(), Config{})
	key := types.NewKVStoreKey("test")

	store.MountStoreWithDB(key, types.StoreTypeIAVL, nil)
	require.NoError(t, store.LoadLatestVersion())

	kv := store.GetKVStore(key)
	kv.Set([]byte("k"), []byte("v1"))
	store.Commit()
	kv = store.GetKVStore(key)
	kv.Set([]byte("k"), []byte("v2"))
	store.Commit()

	res, err := store.Query(&types.RequestQuery{
		Path:   "/test/key",
		Data:   []byte("k"),
		Height: 1,
		Prove:  true,
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), res.Height)
	require.Equal(t, []byte("v1"), res.Value)
	require.NotNil(t, res.ProofOps)
	require.NotEmpty(t, res.ProofOps.Ops)
}

func TestCacheMultiStoreWithVersion_ErrWhenSSUnavailable(t *testing.T) {
	db := dbm.NewMemDB()
	cfg := testMemIAVLConfig(t)
	cfg.StateStoreBackend = ""
	store := NewStore(db, log.NewNopLogger(), metrics.NewNoOpMetrics(), cfg)
	key := types.NewKVStoreKey("test")

	store.MountStoreWithDB(key, types.StoreTypeIAVL, nil)
	require.NoError(t, store.LoadLatestVersion())

	kv := store.GetKVStore(key)
	kv.Set([]byte("k"), []byte("v1"))
	store.Commit()
	kv = store.GetKVStore(key)
	kv.Set([]byte("k"), []byte("v2"))
	store.Commit()

	_, err := store.CacheMultiStoreWithVersion(1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unavailable")
}

func TestQuery_HistoricalNoProofErrWhenSSUnavailable(t *testing.T) {
	db := dbm.NewMemDB()
	cfg := testMemIAVLConfig(t)
	cfg.StateStoreBackend = ""
	store := NewStore(db, log.NewNopLogger(), metrics.NewNoOpMetrics(), cfg)
	key := types.NewKVStoreKey("test")

	store.MountStoreWithDB(key, types.StoreTypeIAVL, nil)
	require.NoError(t, store.LoadLatestVersion())

	kv := store.GetKVStore(key)
	kv.Set([]byte("k"), []byte("v1"))
	store.Commit()
	kv = store.GetKVStore(key)
	kv.Set([]byte("k"), []byte("v2"))
	store.Commit()

	res, err := store.Query(&types.RequestQuery{
		Path:   "/test/key",
		Data:   []byte("k"),
		Height: 1,
		Prove:  false,
	})
	require.Error(t, err)
	require.Nil(t, res)
	require.Contains(t, err.Error(), "unavailable")
}

func TestQuery_HistoricalProofHonorsConcurrencyGate(t *testing.T) {
	db := dbm.NewMemDB()
	store := NewStore(db, log.NewNopLogger(), metrics.NewNoOpMetrics(), Config{
		StateCommitmentBackend:                "memiavl",
		HistoricalProofQueryMaxConcurrency: 1,
	})
	key := types.NewKVStoreKey("test")

	store.MountStoreWithDB(key, types.StoreTypeIAVL, nil)
	require.NoError(t, store.LoadLatestVersion())

	kv := store.GetKVStore(key)
	kv.Set([]byte("k"), []byte("v1"))
	store.Commit()
	kv = store.GetKVStore(key)
	kv.Set([]byte("k"), []byte("v2"))
	store.Commit()

	// Saturate the gate so next historical proof query must fail fast.
	store.historicalProofQueryGate <- struct{}{}

	_, err := store.Query(&types.RequestQuery{
		Path:   "/test/key",
		Data:   []byte("k"),
		Height: 1,
		Prove:  true,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "too many concurrent historical proof queries")
}

func TestCacheMultiStoreWithVersion_ErrWhenStateHistoryUnavailable(t *testing.T) {
	db := dbm.NewMemDB()
	cfg := testMemIAVLConfig(t)
	cfg.KeepRecent = 1
	store := NewStore(db, log.NewNopLogger(), metrics.NewNoOpMetrics(), cfg)
	key := types.NewKVStoreKey("test")

	store.MountStoreWithDB(key, types.StoreTypeIAVL, nil)
	require.NoError(t, store.LoadLatestVersion())

	kv := store.GetKVStore(key)
	kv.Set([]byte("k"), []byte("v1"))
	store.Commit()
	kv = store.GetKVStore(key)
	kv.Set([]byte("k"), []byte("v2"))
	store.Commit()

	_, err := store.CacheMultiStoreWithVersion(1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unavailable")
}

func TestRollbackToVersion_SyncsSCState(t *testing.T) {
	db := dbm.NewMemDB()
	store := NewStore(db, log.NewNopLogger(), metrics.NewNoOpMetrics(), testMemIAVLConfig(t))
	key := types.NewKVStoreKey("test")

	store.MountStoreWithDB(key, types.StoreTypeIAVL, nil)
	require.NoError(t, store.LoadLatestVersion())

	kv := store.GetKVStore(key)
	kv.Set([]byte("k"), []byte("v1"))
	store.Commit()
	kv = store.GetKVStore(key)
	kv.Set([]byte("k"), []byte("v2"))
	store.Commit()

	require.NoError(t, store.RollbackToVersion(1))
	require.Equal(t, int64(1), store.LastCommitID().Version)
	require.Equal(t, int64(1), store.GetEarliestVersion())

	res, err := store.Query(&types.RequestQuery{
		Path:   "/test/key",
		Data:   []byte("k"),
		Height: 1,
		Prove:  false,
	})
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), res.Value)
}

func TestRollbackToVersion_AttemptsBothSCAndSSEvenOnError(t *testing.T) {
	db := dbm.NewMemDB()
	store := NewStore(db, log.NewNopLogger(), metrics.NewNoOpMetrics(), Config{
		StateStoreBackend: "",
	})
	key := types.NewKVStoreKey("test")
	store.MountStoreWithDB(key, types.StoreTypeIAVL, nil)
	require.NoError(t, store.LoadLatestVersion())

	kv := store.GetKVStore(key)
	kv.Set([]byte("k"), []byte("v1"))
	store.Commit()
	kv = store.GetKVStore(key)
	kv.Set([]byte("k"), []byte("v2"))
	store.Commit()

	sc := &rollbackTrackingSCStore{rollbackErr: fmt.Errorf("sc rollback error")}
	ss := &rollbackTrackingStateStore{rollbackErr: fmt.Errorf("ss rollback error")}
	store.scStore = sc
	store.ss = ss

	err := store.RollbackToVersion(1)
	require.Error(t, err)
	require.True(t, sc.rollbackCalled)
	require.True(t, ss.rollbackCalled)
	require.True(t, strings.Contains(err.Error(), "sc rollback error"))
	require.True(t, strings.Contains(err.Error(), "ss rollback error"))
}

func TestLoadLatestVersion_ChecksSCAndSSConsistency(t *testing.T) {
	db := dbm.NewMemDB()
	store := NewStore(db, log.NewNopLogger(), metrics.NewNoOpMetrics(), testMemIAVLConfig(t))
	key := types.NewKVStoreKey("test")
	store.MountStoreWithDB(key, types.StoreTypeIAVL, nil)
	require.NoError(t, store.LoadLatestVersion())

	kv := store.GetKVStore(key)
	kv.Set([]byte("k"), []byte("v1"))
	store.Commit()

	sc := &rollbackTrackingSCStore{hasVersion: true, currentVersion: 1}
	ss := &rollbackTrackingStateStore{hasVersion: true}
	store.scStore = sc
	store.ss = ss

	require.NoError(t, store.LoadLatestVersion())
	require.True(t, sc.hasVersionCalled)
	require.True(t, ss.hasVersionCalled)
}

func TestLoadLatestVersion_ReturnsConsistencyErrors(t *testing.T) {
	db := dbm.NewMemDB()
	store := NewStore(db, log.NewNopLogger(), metrics.NewNoOpMetrics(), testMemIAVLConfig(t))
	key := types.NewKVStoreKey("test")
	store.MountStoreWithDB(key, types.StoreTypeIAVL, nil)
	require.NoError(t, store.LoadLatestVersion())

	kv := store.GetKVStore(key)
	kv.Set([]byte("k"), []byte("v1"))
	store.Commit()

	sc := &rollbackTrackingSCStore{hasVersion: false, currentVersion: 0}
	ss := &rollbackTrackingStateStore{hasVersion: false}
	store.scStore = sc
	store.ss = ss

	err := store.LoadLatestVersion()
	require.Error(t, err)
	require.Contains(t, err.Error(), "sc consistency check failed")
	require.Contains(t, err.Error(), "ss consistency check failed")
}

func TestGetEarliestVersion_UsesStateStoreValueDirectly(t *testing.T) {
	db := dbm.NewMemDB()
	store := NewStore(db, log.NewNopLogger(), metrics.NewNoOpMetrics(), Config{
		StateStoreBackend: "",
	})
	store.ss = &rollbackTrackingStateStore{earliest: 0}
	require.Equal(t, int64(0), store.GetEarliestVersion())

	store.ss = &rollbackTrackingStateStore{earliest: 7}
	require.Equal(t, int64(7), store.GetEarliestVersion())
}

func TestRollbackToVersion_InvalidTarget(t *testing.T) {
	db := dbm.NewMemDB()
	store := NewStore(db, log.NewNopLogger(), metrics.NewNoOpMetrics(), Config{
		StateStoreBackend: "",
	})
	err := store.RollbackToVersion(0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid rollback height target")
}


func TestStore_DefaultSCStoreFromBackend(t *testing.T) {
	db := dbm.NewMemDB()

	memStore := NewStore(db, log.NewNopLogger(), metrics.NewNoOpMetrics(), testMemIAVLConfig(t))
	require.IsType(t, &memIAVLStore{}, memStore.scStore)

	unknownStore := NewStore(dbm.NewMemDB(), log.NewNopLogger(), metrics.NewNoOpMetrics(), Config{
		StateCommitmentBackend: "unknown-backend",
	})
	require.IsType(t, noopSCStore{}, unknownStore.scStore)
}

func TestStore_UsesRegisteredSCBuilder(t *testing.T) {
	backend := "custom-backend-for-test"
	require.NoError(t, RegisterSCStoreBuilder(backend, func(_ dbm.DB, _ Config) (SCStore, error) {
		return newMemorySCStore(), nil
	}))

	store := NewStore(dbm.NewMemDB(), log.NewNopLogger(), metrics.NewNoOpMetrics(), Config{
		StateCommitmentBackend: backend,
	})
	require.IsType(t, &memorySCStore{}, store.scStore)
}

func TestStore_BuilderErrorFallsBackToNoop(t *testing.T) {
	backend := "error-backend-for-test"
	require.NoError(t, RegisterSCStoreBuilder(backend, func(_ dbm.DB, _ Config) (SCStore, error) {
		return nil, fmt.Errorf("boom")
	}))

	store := NewStore(dbm.NewMemDB(), log.NewNopLogger(), metrics.NewNoOpMetrics(), Config{
		StateCommitmentBackend: backend,
	})
	require.IsType(t, noopSCStore{}, store.scStore)
}
