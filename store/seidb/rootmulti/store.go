package rootmulti

import (
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cosmos/iavl"
	dbm "github.com/cosmos/cosmos-db"

	"cosmossdk.io/store/cachemulti"
	"cosmossdk.io/log"
	"cosmossdk.io/store/metrics"
	pruningtypes "cosmossdk.io/store/pruning/types"
	"cosmossdk.io/store/seidb/commitment"
	"cosmossdk.io/store/seidb/state"
	snapshottypes "cosmossdk.io/store/snapshots/types"
	"cosmossdk.io/store/types"
	protoio "github.com/cosmos/gogoproto/io"
)

// Config carries SeiDB runtime knobs from app/store config.
//
// Phase A keeps behavior compatible by delegating to legacy rootmulti while
// preserving a stable constructor and config surface for the Seidb path.
type Config struct {
	Home                    string
	StateCommitmentBackend string
	StateStoreBackend      string
	KeepRecent             uint64
	HistoricalProofQueryMaxConcurrency uint32
}

type NamedChangeSet struct {
	Name      string
	ChangeSet *iavl.ChangeSet
}

// SCCommitter is the minimal state-commitment interface used in Phase A.
// A no-op implementation is wired by default so the path is executable while
// the full SeiDB committer integration is implemented.
type SCCommitter interface {
	ApplyChangeSets([]*NamedChangeSet) error
	Commit(version int64) error
}

type noopSCCommitter struct{}

func (noopSCCommitter) ApplyChangeSets(_ []*NamedChangeSet) error { return nil }
func (noopSCCommitter) Commit(_ int64) error                      { return nil }

type Store struct {
	runtime runtimeBackend

	db     dbm.DB
	config Config
	sc     SCCommitter
	ss     StateStore
	logger log.Logger
	metrics metrics.StoreMetrics

	mtx      sync.RWMutex
	storesParams map[types.StoreKey]storeParams
	storeKeys    map[string]types.StoreKey
	ckvStore map[types.StoreKey]types.CommitKVStore
	historicalProofQueryGate chan struct{}
	scOpMtx  sync.Mutex
}

type storeParams struct {
	key types.StoreKey
	typ types.StoreType
}

type runtimeBackend interface {
	types.CommitMultiStore
	types.Queryable
	GetCommitInfo(ver int64) (*types.CommitInfo, error)
}

type historicalSnapshotReader interface {
	Snapshot(storeName string, version int64) (map[string][]byte, bool)
	HasVersion(version int64) bool
}

func NewStore(db dbm.DB, logger log.Logger, metricGatherer metrics.StoreMetrics, cfg Config) *Store {
	if metricGatherer == nil {
		metricGatherer = metrics.NewNoOpMetrics()
	}

	store := &Store{
		runtime:      newStoreV2Runtime(db, logger, metricGatherer),
		db:           db,
		config:       cfg,
		logger:       logger,
		metrics:      metricGatherer,
		storesParams: make(map[types.StoreKey]storeParams),
		storeKeys:    make(map[string]types.StoreKey),
		ckvStore:     make(map[types.StoreKey]types.CommitKVStore),
	}
	store.sc = newSCCommitterFromConfig(db, cfg)
	store.ss = newStateStoreFromConfig(db, cfg, store.sc)
	if cfg.HistoricalProofQueryMaxConcurrency > 0 {
		store.historicalProofQueryGate = make(chan struct{}, int(cfg.HistoricalProofQueryMaxConcurrency))
	}
	return store
}

func (s *Store) Config() Config {
	return s.config
}

func (s *Store) LastCommitID() types.CommitID {
	return s.runtime.LastCommitID()
}

func (s *Store) WorkingHash() []byte {
	return s.runtime.WorkingHash()
}

func (s *Store) SetPruning(opts pruningtypes.PruningOptions) {
	s.runtime.SetPruning(opts)
}

func (s *Store) GetPruning() pruningtypes.PruningOptions {
	return s.runtime.GetPruning()
}

func (s *Store) GetStoreType() types.StoreType {
	return s.runtime.GetStoreType()
}

func (s *Store) CacheWrap() types.CacheWrap {
	return s.runtime.CacheWrap()
}

func (s *Store) CacheWrapWithTrace(w io.Writer, tc types.TraceContext) types.CacheWrap {
	return s.runtime.CacheWrapWithTrace(w, tc)
}

func (s *Store) LatestVersion() int64 {
	return s.runtime.LatestVersion()
}

func (s *Store) SetInterBlockCache(cache types.MultiStorePersistentCache) {
	s.runtime.SetInterBlockCache(cache)
}

func (s *Store) SetInitialVersion(version int64) error {
	return s.runtime.SetInitialVersion(version)
}

func (s *Store) SetIAVLCacheSize(size int) {
	s.runtime.SetIAVLCacheSize(size)
}

func (s *Store) SetIAVLDisableFastNode(disable bool) {
	s.runtime.SetIAVLDisableFastNode(disable)
}

func (s *Store) SetIAVLSyncPruning(sync bool) {
	s.runtime.SetIAVLSyncPruning(sync)
}

func (s *Store) GetObjKVStore(key types.StoreKey) types.ObjKVStore {
	return s.runtime.GetObjKVStore(key)
}

func (s *Store) TracingEnabled() bool {
	return s.runtime.TracingEnabled()
}

func (s *Store) SetTracer(w io.Writer) types.MultiStore {
	return s.runtime.SetTracer(w)
}

func (s *Store) SetTracingContext(tc types.TraceContext) types.MultiStore {
	return s.runtime.SetTracingContext(tc)
}

func (s *Store) ListeningEnabled(key types.StoreKey) bool {
	return s.runtime.ListeningEnabled(key)
}

func (s *Store) AddListeners(keys []types.StoreKey) {
	s.runtime.AddListeners(keys)
}

func (s *Store) PopStateCache() []*types.StoreKVPair {
	return s.runtime.PopStateCache()
}

func (s *Store) SetMetrics(metricGatherer metrics.StoreMetrics) {
	s.metrics = metricGatherer
	s.runtime.SetMetrics(metricGatherer)
}

func (s *Store) Snapshot(height uint64, protoWriter protoio.Writer) error {
	return s.runtime.Snapshot(height, protoWriter)
}

func (s *Store) PruneSnapshotHeight(height int64) {
	s.runtime.PruneSnapshotHeight(height)
}

func (s *Store) SetSnapshotInterval(snapshotInterval uint64) {
	s.runtime.SetSnapshotInterval(snapshotInterval)
}

// SetSCCommitter allows replacing the default no-op committer.
func (s *Store) SetSCCommitter(committer SCCommitter) {
	if committer == nil {
		committer = noopSCCommitter{}
	}
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if s.ss != nil {
		_ = s.ss.Close()
	}
	s.sc = committer
	s.ss = newStateStoreFromConfig(s.db, s.config, s.sc)
}

func (s *Store) LoadLatestVersion() error {
	if err := s.runtime.LoadLatestVersion(); err != nil {
		return err
	}
	s.rebuildCommitStores()
	return s.syncBackendsWithRuntimeVersion()
}

func (s *Store) LoadLatestVersionAndUpgrade(upgrades *types.StoreUpgrades) error {
	if err := s.runtime.LoadLatestVersionAndUpgrade(upgrades); err != nil {
		return err
	}
	s.applyStoreUpgrades(upgrades)
	s.rebuildCommitStores()
	return s.syncBackendsWithRuntimeVersion()
}

func (s *Store) LoadVersion(ver int64) error {
	if err := s.runtime.LoadVersion(ver); err != nil {
		return err
	}
	s.rebuildCommitStores()
	return s.syncBackendsWithRuntimeVersion()
}

func (s *Store) LoadVersionAndUpgrade(ver int64, upgrades *types.StoreUpgrades) error {
	if err := s.runtime.LoadVersionAndUpgrade(ver, upgrades); err != nil {
		return err
	}
	s.applyStoreUpgrades(upgrades)
	s.rebuildCommitStores()
	return s.syncBackendsWithRuntimeVersion()
}

func (s *Store) MountStoreWithDB(key types.StoreKey, typ types.StoreType, db dbm.DB) {
	if key == nil {
		panic("MountStoreWithDB() key cannot be nil")
	}

	s.mtx.Lock()
	if _, ok := s.storesParams[key]; ok {
		s.mtx.Unlock()
		panic(fmt.Sprintf("store duplicate store key %v", key))
	}
	if _, ok := s.storeKeys[key.Name()]; ok {
		s.mtx.Unlock()
		panic(fmt.Sprintf("store duplicate store key name %v", key.Name()))
	}
	s.storesParams[key] = storeParams{key: key, typ: typ}
	s.storeKeys[key.Name()] = key
	s.mtx.Unlock()

	s.runtime.MountStoreWithDB(key, typ, db)
	s.mtx.Lock()
	delete(s.ckvStore, key)
	s.mtx.Unlock()
}

func (s *Store) GetStore(key types.StoreKey) types.Store {
	return s.GetCommitKVStore(key)
}

func (s *Store) GetKVStore(key types.StoreKey) types.KVStore {
	return s.GetCommitKVStore(key)
}

func (s *Store) GetCommitStore(key types.StoreKey) types.CommitStore {
	return s.GetCommitKVStore(key)
}

func (s *Store) GetCommitKVStore(key types.StoreKey) types.CommitKVStore {
	s.mtx.RLock()
	store, ok := s.ckvStore[key]
	s.mtx.RUnlock()
	if ok {
		return store
	}
	legacy := s.runtime.GetCommitKVStore(key)
	if legacy == nil {
		return nil
	}
	return legacy
}

func (s *Store) CacheMultiStore() types.CacheMultiStore {
	stores := s.buildCacheStores(0, false)
	if len(stores) == 0 {
		return s.runtime.CacheMultiStore()
	}
	return cachemulti.NewFromKVStore(stores, nil, nil)
}

func (s *Store) CacheMultiStoreWithVersion(version int64) (types.CacheMultiStore, error) {
	latest := s.runtime.LastCommitID().Version
	if version <= 0 || version == latest {
		return s.CacheMultiStore(), nil
	}
	if version > latest {
		return nil, errors.New("requested version is greater than latest version")
	}

	snapshotReader := s.resolveHistoricalSnapshotReader(version)
	if snapshotReader == nil {
		return nil, fmt.Errorf("historical version %d is unavailable in state store", version)
	}
	stores := s.buildCacheStores(version, true, snapshotReader)
	return cachemulti.NewFromKVStore(stores, nil, nil), nil
}

func (s *Store) Query(req *types.RequestQuery) (*types.ResponseQuery, error) {
	storeName, subpath, err := parsePath(req.Path)
	if err != nil {
		return &types.ResponseQuery{}, err
	}

	latest := s.runtime.LastCommitID().Version
	queryVersion := req.Height
	if queryVersion <= 0 || queryVersion > latest {
		queryVersion = latest
	}
	needProof := req.Prove && requireProof(subpath)
	if needProof && queryVersion < latest {
		start := time.Now()
		defer s.metrics.MeasureSinceFrom(start, "store", "seidb", "historical_proof_query")
		if !s.tryAcquireHistoricalProofSlot() {
			s.metrics.IncrCounter(1, "store", "seidb", "historical_proof_query_rejected")
			return nil, fmt.Errorf("too many concurrent historical proof queries (limit=%d)", s.config.HistoricalProofQueryMaxConcurrency)
		}
		defer s.releaseHistoricalProofSlot()
		reqCopy := *req
		reqCopy.Height = queryVersion
		return s.runtime.Query(&reqCopy)
	}

	// Serve SS path only for non-proof queries to avoid ambiguous proof semantics.
	if !req.Prove && !needProof {
		snapshotReader := s.resolveHistoricalSnapshotReader(queryVersion)
		if snapshotReader != nil {
			key := s.lookupStoreKey(storeName)
			if key != nil {
				ssStore := state.NewStore(snapshotReader, key, queryVersion)
				ssReq := *req
				ssReq.Path = subpath
				ssReq.Height = queryVersion
				return ssStore.Query(&ssReq)
			}
		}
		if queryVersion < latest {
			return nil, fmt.Errorf("historical version %d is unavailable in state store", queryVersion)
		}
	}

	key := s.lookupStoreKey(storeName)
	if key == nil {
		return nil, fmt.Errorf("no such store: %s", storeName)
	}

	var store types.Store
	if queryVersion == latest {
		store = s.GetStore(key)
	} else {
		// For historical proof path, follow storev2 flow by loading immutable stores at the queried height.
		cms, err := s.runtime.CacheMultiStoreWithVersion(queryVersion)
		if err != nil {
			return nil, err
		}
		store = cms.GetStore(key)
	}
	if err != nil {
		return nil, err
	}

	queryable, ok := store.(types.Queryable)
	if !ok {
		return &types.ResponseQuery{}, fmt.Errorf("store %s (type %T) doesn't support queries", storeName, store)
	}

	trimmedReq := *req
	trimmedReq.Path = subpath
	trimmedReq.Height = queryVersion
	res, err := queryable.Query(&trimmedReq)
	if err != nil || !req.Prove || !needProof {
		return res, err
	}
	if res.ProofOps == nil || len(res.ProofOps.Ops) == 0 {
		return &types.ResponseQuery{}, fmt.Errorf("proof is unexpectedly empty; ensure height has not been pruned")
	}

	commitInfo, err := s.runtime.GetCommitInfo(res.Height)
	if err != nil {
		return &types.ResponseQuery{}, err
	}
	res.ProofOps.Ops = append(res.ProofOps.Ops, commitInfo.ProofOp(storeName))
	return res, nil
}

func (s *Store) tryAcquireHistoricalProofSlot() bool {
	if s.historicalProofQueryGate == nil {
		return true
	}
	select {
	case s.historicalProofQueryGate <- struct{}{}:
		return true
	default:
		return false
	}
}

func (s *Store) releaseHistoricalProofSlot() {
	if s.historicalProofQueryGate == nil {
		return
	}
	select {
	case <-s.historicalProofQueryGate:
	default:
	}
}

func (s *Store) resolveHistoricalSnapshotReader(version int64) historicalSnapshotReader {
	if version <= 0 {
		return nil
	}
	if s.ss != nil && s.ss.HasVersion(version) {
		return s.ss
	}
	return nil
}

func (s *Store) Commit() types.CommitID {
	changeSets := cloneNamedChangeSets(s.collectPendingChangeSets())
	targetVersion := s.runtime.LastCommitID().Version + 1

	// Follow storev2-style flush ordering: apply SS/SC changes before runtime commit.
	if s.ss != nil {
		if len(changeSets) > 0 {
			if err := s.ss.ApplyChangeSets(targetVersion, changeSets); err != nil {
				panic(fmt.Errorf("seidb ss apply changesets failed at version %d: %w", targetVersion, err))
			}
		} else if err := s.ss.SetLatestVersion(targetVersion); err != nil {
			panic(fmt.Errorf("seidb ss set latest version failed at version %d: %w", targetVersion, err))
		}
	}

	s.scOpMtx.Lock()
	if err := s.sc.ApplyChangeSets(changeSets); err != nil {
		s.scOpMtx.Unlock()
		panic(fmt.Errorf("seidb sc apply changesets failed at version %d: %w", targetVersion, err))
	}
	if err := s.sc.Commit(targetVersion); err != nil {
		s.scOpMtx.Unlock()
		panic(fmt.Errorf("seidb sc commit failed at version %d: %w", targetVersion, err))
	}
	s.scOpMtx.Unlock()

	cid := s.runtime.Commit()
	if cid.Version != targetVersion {
		panic(fmt.Errorf("runtime commit version mismatch: expected %d got %d", targetVersion, cid.Version))
	}
	s.clearPendingChangeSets()
	if err := s.checkBackendsConsistency(cid.Version, true); err != nil {
		panic(fmt.Errorf("seidb post-commit consistency check failed at version %d: %w", cid.Version, err))
	}
	return cid
}

func (s *Store) RollbackToVersion(target int64) error {
	if target <= 0 {
		return fmt.Errorf("invalid rollback height target: %d", target)
	}

	if err := s.runtime.RollbackToVersion(target); err != nil {
		return err
	}

	s.rebuildCommitStores()

	var consistencyErrs []error
	if rollbacker, ok := s.sc.(interface {
		RollbackToVersion(target int64) error
	}); ok {
		s.scOpMtx.Lock()
		if err := rollbacker.RollbackToVersion(target); err != nil {
			consistencyErrs = append(consistencyErrs, fmt.Errorf("sc rollback to version %d failed: %w", target, err))
		}
		s.scOpMtx.Unlock()
	}
	if s.ss != nil {
		if err := s.ss.RollbackToVersion(target); err != nil {
			consistencyErrs = append(consistencyErrs, fmt.Errorf("ss rollback to version %d failed: %w", target, err))
		}
	}

	if err := s.checkBackendsConsistency(s.runtime.LastCommitID().Version, true); err != nil {
		consistencyErrs = append(consistencyErrs, err)
	}

	if len(consistencyErrs) > 0 {
		return errors.Join(consistencyErrs...)
	}
	return nil
}

func (s *Store) GetEarliestVersion() int64 {
	if s.ss != nil {
		return s.ss.EarliestVersion()
	}
	return s.runtime.LastCommitID().Version
}

func (s *Store) Restore(height uint64, format uint32, protoReader protoio.Reader) (snapshottypes.SnapshotItem, error) {
	if height > math.MaxInt64 {
		return snapshottypes.SnapshotItem{}, fmt.Errorf("snapshot height %d exceeds max int64", height)
	}

	var (
		item       snapshottypes.SnapshotItem
		err        error
		ssImportErr error
		ssImported bool
	)
	if importer, ok := s.ss.(snapshotImporter); ok {
		nodeCh := make(chan SnapshotImportNode, 4096)
		doneCh := make(chan error, 1)
		restoreVersion := int64(height)
		go func() {
			doneCh <- importer.ImportSnapshot(restoreVersion, nodeCh)
		}()

		if runtimeWithHook, ok := s.runtime.(interface {
			RestoreWithNodeHook(height uint64, format uint32, protoReader protoio.Reader, nodeHook func(storeName string, key, value []byte, nodeHeight int8)) (snapshottypes.SnapshotItem, error)
		}); ok {
			item, err = runtimeWithHook.RestoreWithNodeHook(height, format, protoReader, func(storeName string, key, value []byte, nodeHeight int8) {
				if nodeHeight != 0 {
					return
				}
				nodeCh <- SnapshotImportNode{
					StoreKey: storeName,
					Key:      append([]byte(nil), key...),
					Value:    append([]byte(nil), value...),
				}
			})
		} else {
			item, err = s.runtime.Restore(height, format, protoReader)
		}
		close(nodeCh)
		ssImportErr = <-doneCh
		if err == nil && ssImportErr == nil {
			ssImported = true
		}
	} else {
		item, err = s.runtime.Restore(height, format, protoReader)
	}
	if err != nil {
		return snapshottypes.SnapshotItem{}, err
	}
	if ssImportErr != nil {
		return snapshottypes.SnapshotItem{}, fmt.Errorf("ss import snapshot at height %d failed: %w", height, ssImportErr)
	}
	s.rebuildCommitStores()

	version := s.runtime.LastCommitID().Version
	if version <= 0 {
		version = int64(height)
	}
	if err := s.checkBackendsConsistency(version, !ssImported); err != nil {
		return snapshottypes.SnapshotItem{}, err
	}
	return item, nil
}

func (s *Store) syncBackendsWithRuntimeVersion() error {
	version := s.runtime.LastCommitID().Version
	if version <= 0 {
		return nil
	}
	return s.checkBackendsConsistency(version, true)
}

func (s *Store) checkBackendsConsistency(version int64, checkSS bool) error {
	var consistencyErrs []error

	if checker, ok := s.sc.(interface {
		HasVersion(version int64) bool
	}); ok {
		s.scOpMtx.Lock()
		if !checker.HasVersion(version) {
			consistencyErrs = append(consistencyErrs, fmt.Errorf("sc consistency check failed: version %d unavailable", version))
		}
		s.scOpMtx.Unlock()
	}
	if checker, ok := s.sc.(interface {
		CurrentVersion() int64
	}); ok {
		s.scOpMtx.Lock()
		if checker.CurrentVersion() < version {
			consistencyErrs = append(consistencyErrs, fmt.Errorf("sc consistency check failed: current_version=%d expected_at_least=%d", checker.CurrentVersion(), version))
		}
		s.scOpMtx.Unlock()
	}
	if checkSS && s.ss != nil && s.config.StateStoreBackend != "" {
		if !s.ss.HasVersion(version) {
			consistencyErrs = append(consistencyErrs, fmt.Errorf("ss consistency check failed: version %d unavailable", version))
		}
	}
	if len(consistencyErrs) > 0 {
		return errors.Join(consistencyErrs...)
	}
	return nil
}

func (s *Store) rebuildCommitStores() {
	keys := s.sortedStoreKeys()

	newStores := make(map[types.StoreKey]types.CommitKVStore, len(keys))
	for _, key := range keys {
		commitStore := s.runtime.GetCommitStore(key)
		if commitStore == nil {
			continue
		}
		legacyStore, ok := commitStore.(types.CommitKVStore)
		if !ok {
			// Transient/Memory/Object stores are not CommitKVStore.
			continue
		}
		if legacyStore.GetStoreType() == types.StoreTypeIAVL {
			newStores[key] = commitment.NewStore(key, legacyStore)
			continue
		}
		newStores[key] = legacyStore
	}

	s.mtx.Lock()
	s.ckvStore = newStores
	s.mtx.Unlock()
}

func (s *Store) buildCacheStores(version int64, historical bool, readers ...interface {
	Snapshot(storeName string, version int64) (map[string][]byte, bool)
}) map[types.StoreKey]types.CacheWrapper {
	keys := s.sortedStoreKeys()

	var snapshotReader interface {
		Snapshot(storeName string, version int64) (map[string][]byte, bool)
	}
	if len(readers) > 0 {
		snapshotReader = readers[0]
	}

	s.mtx.RLock()
	stores := make(map[types.StoreKey]types.CacheWrapper, len(keys))
	for _, key := range keys {

		// Prefer wrapped commit stores so IAVL writes are tracked as changesets.
		if wrapped, ok := s.ckvStore[key]; ok {
			if historical && snapshotReader != nil && wrapped.GetStoreType() == types.StoreTypeIAVL {
				stores[key] = state.NewStore(snapshotReader, key, version)
			} else {
				stores[key] = wrapped
			}
			continue
		}

		// Keep object/transient/memory stores available in cache multi-store.
		base := s.runtime.GetStore(key)
		cacheStore, ok := base.(types.CacheWrapper)
		if !ok {
			continue
		}
		stores[key] = cacheStore
	}
	s.mtx.RUnlock()
	return stores
}

func (s *Store) sortedStoreKeys() []types.StoreKey {
	s.mtx.RLock()
	names := make([]string, 0, len(s.storeKeys))
	for name := range s.storeKeys {
		names = append(names, name)
	}
	sort.Strings(names)
	keys := make([]types.StoreKey, 0, len(names))
	for _, name := range names {
		keys = append(keys, s.storeKeys[name])
	}
	s.mtx.RUnlock()
	return keys
}

func (s *Store) lookupStoreKey(name string) types.StoreKey {
	s.mtx.RLock()
	key := s.storeKeys[name]
	s.mtx.RUnlock()
	return key
}

func (s *Store) applyStoreUpgrades(upgrades *types.StoreUpgrades) {
	if upgrades == nil {
		return
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()

	for _, name := range upgrades.Deleted {
		key, ok := s.storeKeys[name]
		if !ok {
			continue
		}
		delete(s.storeKeys, name)
		delete(s.storesParams, key)
		delete(s.ckvStore, key)
	}

	for _, rename := range upgrades.Renamed {
		oldKey, ok := s.storeKeys[rename.OldKey]
		if !ok {
			continue
		}
		// After rename, old-key metadata should not participate in store lookup.
		delete(s.storeKeys, rename.OldKey)
		delete(s.storesParams, oldKey)
		delete(s.ckvStore, oldKey)
	}
}

func (s *Store) resolveQueryableStore(storeName string, queryVersion, latest int64) (types.Store, error) {
	key := s.lookupStoreKey(storeName)
	if key == nil {
		return nil, fmt.Errorf("no such store: %s", storeName)
	}
	if queryVersion != latest {
		return nil, fmt.Errorf("historical version %d query requires state store path", queryVersion)
	}
	store := s.GetStore(key)
	if store == nil {
		return nil, fmt.Errorf("no such store: %s", storeName)
	}
	return store, nil
}

func (s *Store) collectPendingChangeSets() []*NamedChangeSet {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	changeSets := make([]*NamedChangeSet, 0, len(s.ckvStore))
	for key, store := range s.ckvStore {
		csStore, ok := store.(*commitment.Store)
		if !ok {
			continue
		}
		cs := csStore.PendingChangeSet()
		if cs == nil || len(cs.Pairs) == 0 {
			continue
		}
		changeSets = append(changeSets, &NamedChangeSet{
			Name:      key.Name(),
			ChangeSet: cs,
		})
	}
	sort.SliceStable(changeSets, func(i, j int) bool {
		return changeSets[i].Name < changeSets[j].Name
	})
	return changeSets
}

func (s *Store) commitStoresSnapshot() map[types.StoreKey]types.CommitKVStore {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	stores := make(map[types.StoreKey]types.CommitKVStore, len(s.ckvStore))
	for key, store := range s.ckvStore {
		stores[key] = store
	}
	return stores
}

func (s *Store) clearPendingChangeSets() {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	for _, store := range s.ckvStore {
		csStore, ok := store.(*commitment.Store)
		if !ok {
			continue
		}
		csStore.ClearChangeSet()
	}
}

func cloneNamedChangeSets(in []*NamedChangeSet) []*NamedChangeSet {
	out := make([]*NamedChangeSet, 0, len(in))
	for _, named := range in {
		if named == nil || named.ChangeSet == nil {
			continue
		}
		cs := &iavl.ChangeSet{
			Pairs: make([]*iavl.KVPair, 0, len(named.ChangeSet.Pairs)),
		}
		for _, p := range named.ChangeSet.Pairs {
			if p == nil {
				continue
			}
			cs.Pairs = append(cs.Pairs, &iavl.KVPair{
				Delete: p.Delete,
				Key:    cloneBytesNonNil(p.Key),
				Value:  cloneBytesNonNil(p.Value),
			})
		}
		out = append(out, &NamedChangeSet{
			Name:      named.Name,
			ChangeSet: cs,
		})
	}
	return out
}

func parsePath(path string) (storeName, subpath string, err error) {
	if !strings.HasPrefix(path, "/") {
		return "", "", fmt.Errorf("invalid path: %s", path)
	}

	parts := strings.SplitN(path[1:], "/", 2)
	storeName = parts[0]
	if len(parts) == 2 {
		subpath = "/" + parts[1]
	}
	return storeName, subpath, nil
}

func requireProof(subpath string) bool {
	return subpath == "/key"
}
