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

	dbm "github.com/cosmos/cosmos-db"

	"cosmossdk.io/log"
	"cosmossdk.io/store/cachemulti"
	"cosmossdk.io/store/metrics"
	pruningtypes "cosmossdk.io/store/pruning/types"
	"cosmossdk.io/store/seidb/commitment"
	seidbcfg "cosmossdk.io/store/seidb/config"
	sctypes "cosmossdk.io/store/seidb/sc/types"
	seidbss "cosmossdk.io/store/seidb/ss"
	"cosmossdk.io/store/seidb/ss/state"
	sstypes "cosmossdk.io/store/seidb/ss/types"
	ssutils "cosmossdk.io/store/seidb/ss/utils"
	snapshottypes "cosmossdk.io/store/snapshots/types"
	"cosmossdk.io/store/types"
	"github.com/cometbft/cometbft/monitor"
	protoio "github.com/cosmos/gogoproto/io"
)

// Config carries SeiDB runtime knobs from app/store config.
//
// Phase B (memiavl enabled): AppHash / WorkingHash / CommitID come from SC (memiavl).
// Phase A (legacy): delegates hash and commit metadata to runtime rootmulti + cosmos/iavl.
type Config = seidbcfg.Config
type NamedChangeSet = sstypes.NamedChangeSet
type StateStore = sstypes.StateStore

type Store struct {
	runtime runtimeBackend

	db      dbm.DB
	config  Config
	scStore SCStore
	ss      StateStore
	logger  log.Logger
	metrics metrics.StoreMetrics

	mtx                      sync.RWMutex
	storesParams             map[types.StoreKey]storeParams
	storeKeys                map[string]types.StoreKey
	ckvStore                 map[types.StoreKey]types.CommitKVStore
	lastCommitInfo           *types.CommitInfo
	historicalProofQueryGate chan struct{}
	scOpMtx                  sync.Mutex
}

type storeParams struct {
	key types.StoreKey
	typ types.StoreType
}

type runtimeBackend interface {
	types.CommitMultiStore
	types.Queryable
	GetCommitInfo(ver int64) (*types.CommitInfo, error)
	// SyncCommitMetadata advances runtime DB metadata without committing legacy IAVL trees.
	SyncCommitMetadata(version int64, info *types.CommitInfo)
	// LoadVersionForSCBackedCommit loads multistore metadata while keeping legacy IAVL trees at v0.
	LoadVersionForSCBackedCommit(version int64) error
}

type historicalSnapshotReader interface {
	Get(storeName string, version int64, key []byte) ([]byte, error)
	Has(storeName string, version int64, key []byte) (bool, error)
	Iterator(storeName string, version int64, start, end []byte) (types.Iterator, error)
	ReverseIterator(storeName string, version int64, start, end []byte) (types.Iterator, error)
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
	store.scStore = newSCStoreFromConfig(db, cfg)
	store.ss = seidbss.NewFromConfig(db, cfg, store.scStore)
	if cfg.HistoricalProofQueryMaxConcurrency > 0 {
		store.historicalProofQueryGate = make(chan struct{}, int(cfg.HistoricalProofQueryMaxConcurrency))
	}
	return store
}

func (s *Store) Config() Config {
	return s.config
}

func (s *Store) Close() error {
	var errs []error
	if closer, ok := s.scStore.(interface{ Close() error }); ok {
		if err := closer.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if s.ss != nil {
		if err := s.ss.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (s *Store) scBackendIsMemIAVL() bool {
	backend := s.config.StateCommitmentBackend
	if backend == "" {
		backend = "memiavl"
	}
	return backend == "memiavl"
}

func (s *Store) LastCommitID() types.CommitID {
	if s.usesMemIAVLTreeLayer() {
		s.mtx.RLock()
		defer s.mtx.RUnlock()
		if s.lastCommitInfo == nil {
			return types.CommitID{}
		}
		return s.lastCommitInfo.CommitID()
	}
	return s.runtime.LastCommitID()
}

func (s *Store) WorkingHash() []byte {
	if s.usesMemIAVLTreeLayer() {
		if err := s.flush(); err != nil {
			panic(fmt.Errorf("flush pending changesets: %w", err))
		}
		s.mtx.RLock()
		defer s.mtx.RUnlock()
		if src, ok := s.scStore.(*memIAVLStore); ok {
			workingCommitInfoStart := time.Now()
			ci := amendCommitInfo(convertCommitInfo(src.WorkingCommitInfo()), s.storesParams)
			hash := ci.Hash()
			monitor.LogSeidbWorkingHashTiming(s.lastCommitInfo.Version, float64(time.Since(workingCommitInfoStart).Nanoseconds())/1e6)
			return hash
		}
		return nil
	}
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
	return s.latestCommittedVersion()
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

// SetSCStore allows replacing the default SC store (for tests or custom backends).
func (s *Store) SetSCStore(scStore SCStore) {
	if scStore == nil {
		scStore = noopSCStore{}
	}
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if s.ss != nil {
		_ = s.ss.Close()
	}
	s.scStore = scStore
	s.ss = seidbss.NewFromConfig(s.db, s.config, s.scStore)
}

func (s *Store) loadRuntimeVersion(ver int64, upgrades *types.StoreUpgrades) error {
	if s.scBackendIsMemIAVL() {
		return s.runtime.LoadVersionForSCBackedCommit(ver)
	}
	if upgrades != nil {
		if ver == 0 {
			return s.runtime.LoadLatestVersionAndUpgrade(upgrades)
		}
		return s.runtime.LoadVersionAndUpgrade(ver, upgrades)
	}
	if ver == 0 {
		return s.runtime.LoadLatestVersion()
	}
	return s.runtime.LoadVersion(ver)
}

func (s *Store) LoadLatestVersion() error {
	if err := s.loadRuntimeVersion(0, nil); err != nil {
		return err
	}
	if err := s.loadSCVersion(0); err != nil {
		return err
	}
	return s.finishLoad()
}

func (s *Store) LoadLatestVersionAndUpgrade(upgrades *types.StoreUpgrades) error {
	if err := s.loadRuntimeVersion(0, upgrades); err != nil {
		return err
	}
	s.applyStoreUpgrades(upgrades)
	if err := s.loadSCVersion(0); err != nil {
		return err
	}
	return s.finishLoad()
}

func (s *Store) LoadVersion(ver int64) error {
	if err := s.loadRuntimeVersion(ver, nil); err != nil {
		return err
	}
	if err := s.loadSCVersion(ver); err != nil {
		return err
	}
	return s.finishLoad()
}

func (s *Store) LoadVersionAndUpgrade(ver int64, upgrades *types.StoreUpgrades) error {
	if err := s.loadRuntimeVersion(ver, upgrades); err != nil {
		return err
	}
	s.applyStoreUpgrades(upgrades)
	if err := s.loadSCVersion(ver); err != nil {
		return err
	}
	return s.finishLoad()
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
	// When memIAVL SC is active, IAVL modules must read via mmap+MemNode tree, not runtime IAVL.
	if s.usesMemIAVLTreeLayer() {
		if params, found := s.getStoreParams(key); found && params.typ == types.StoreTypeIAVL {
			return nil
		}
	}
	return s.runtime.GetCommitKVStore(key)
}

// usesMemIAVLTreeLayer reports whether live IAVL reads go through memiavl.Tree (not runtime iavl).
func (s *Store) usesMemIAVLTreeLayer() bool {
	if !s.scBackendIsMemIAVL() {
		return false
	}
	_, ok := s.scStore.(*memIAVLStore)
	return ok
}

func (s *Store) getStoreParams(key types.StoreKey) (storeParams, bool) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	params, ok := s.storesParams[key]
	return params, ok
}

func (s *Store) CacheMultiStore() types.CacheMultiStore {
	stores := s.buildCacheStores(0, false)
	if len(stores) == 0 {
		return s.runtime.CacheMultiStore()
	}
	return cachemulti.NewFromKVStore(stores, nil, nil)
}

func (s *Store) CacheMultiStoreWithVersion(version int64) (types.CacheMultiStore, error) {
	latest := s.latestCommittedVersion()
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

	latest := s.latestCommittedVersion()
	queryVersion := req.Height
	if queryVersion <= 0 || queryVersion > latest {
		queryVersion = latest
	}
	needProof := req.Prove && requireProof(subpath)
	if needProof && queryVersion < latest && !s.usesMemIAVLTreeLayer() {
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

	commitInfo, err := s.commitInfoForProof(res.Height)
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
	targetVersion := s.nextCommitVersion()

	if s.usesMemIAVLTreeLayer() {
		// Pending changesets were flushed in WorkingHash (FinalizeBlock); Commit only
		// persists the SC snapshot, matching sei-chain storev2 ordering.
		// s.commitNonIAVLStores()

		s.scOpMtx.Lock()
		if err := s.scStore.Commit(targetVersion); err != nil {
			s.scOpMtx.Unlock()
			panic(fmt.Errorf("seidb sc commit failed at version %d: %w", targetVersion, err))
		}
		// s.rebuildCommitStores()
		s.scOpMtx.Unlock()
		if err := s.refreshLastCommitInfoFromSC(); err != nil {
			panic(err)
		}
		s.runtime.SyncCommitMetadata(targetVersion, s.lastCommitInfo)
		// ss wal ensure to be committed
		// s.waitForPendingSSWrites()
		if err := s.checkBackendsConsistency(targetVersion, false); err != nil {
			panic(fmt.Errorf("seidb post-commit consistency check failed at version %d: %w", targetVersion, err))
		}
		return s.lastCommitInfo.CommitID()
	}

	changeSets := ssutils.CloneNamedChangeSets(s.collectPendingChangeSets())

	// Legacy path: apply SS/SC changesets synchronously before runtime commit.
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
	if err := s.scStore.ApplyChangeSets(changeSets); err != nil {
		s.scOpMtx.Unlock()
		panic(fmt.Errorf("seidb sc apply changesets failed at version %d: %w", targetVersion, err))
	}
	if err := s.scStore.Commit(targetVersion); err != nil {
		s.scOpMtx.Unlock()
		panic(fmt.Errorf("seidb sc commit failed at version %d: %w", targetVersion, err))
	}
	s.rebuildCommitStores()
	s.scOpMtx.Unlock()

	s.clearPendingChangeSets()

	cid := s.runtime.Commit()
	if cid.Version != targetVersion {
		panic(fmt.Errorf("runtime commit version mismatch: expected %d got %d", targetVersion, cid.Version))
	}
	if err := s.checkBackendsConsistency(cid.Version, true); err != nil {
		panic(fmt.Errorf("seidb post-commit consistency check failed at version %d: %w", cid.Version, err))
	}
	return cid
}

func (s *Store) RollbackToVersion(target int64) error {
	if target <= 0 {
		return fmt.Errorf("invalid rollback height target: %d", target)
	}

	var consistencyErrs []error
	if s.scBackendIsMemIAVL() {
		if rollbacker, ok := s.scStore.(interface {
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
		if err := s.runtime.LoadVersionForSCBackedCommit(target); err != nil {
			consistencyErrs = append(consistencyErrs, err)
		}
		s.rebuildCommitStores()
		_ = s.refreshLastCommitInfoFromSC()
	} else {
		if err := s.runtime.RollbackToVersion(target); err != nil {
			return err
		}
		s.rebuildCommitStores()
		if rollbacker, ok := s.scStore.(interface {
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
	}

	checkVersion := s.latestCommittedVersion()
	if err := s.checkBackendsConsistency(checkVersion, true); err != nil {
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
		item        snapshottypes.SnapshotItem
		err         error
		ssImportErr error
		ssImported  bool
	)
	if importer, ok := s.ss.(sstypes.SnapshotImporter); ok {
		nodeCh := make(chan sstypes.SnapshotImportNode, 4096)
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
				nodeCh <- sstypes.SnapshotImportNode{
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

	version := s.latestCommittedVersion()
	if version <= 0 {
		version = int64(height)
	}
	if s.scBackendIsMemIAVL() {
		_ = s.refreshLastCommitInfoFromSC()
	}
	if err := s.checkBackendsConsistency(version, !ssImported); err != nil {
		return snapshottypes.SnapshotItem{}, err
	}
	return item, nil
}

func (s *Store) syncBackendsWithRuntimeVersion() error {
	if s.scBackendIsMemIAVL() {
		return nil
	}
	version := s.runtime.LastCommitID().Version
	if version <= 0 {
		return nil
	}
	if syncer, ok := s.scStore.(interface {
		CurrentVersion() int64
		SyncFromStores(stores map[types.StoreKey]types.CommitKVStore, version int64) error
	}); ok && syncer.CurrentVersion() < version {
		stores := make(map[types.StoreKey]types.CommitKVStore)
		for _, key := range s.sortedStoreKeys() {
			if store := s.runtime.GetCommitKVStore(key); store != nil && store.GetStoreType() == types.StoreTypeIAVL {
				stores[key] = store
			}
		}
		s.scOpMtx.Lock()
		err := syncer.SyncFromStores(stores, version)
		s.scOpMtx.Unlock()
		if err != nil {
			return fmt.Errorf("sync memiavl from runtime at version %d: %w", version, err)
		}
		s.rebuildCommitStores()
	}
	return s.checkBackendsConsistency(version, true)
}

func (s *Store) checkBackendsConsistency(version int64, checkSS bool) error {
	var consistencyErrs []error

	if checker, ok := s.scStore.(interface {
		HasVersion(version int64) bool
	}); ok {
		s.scOpMtx.Lock()
		if !checker.HasVersion(version) {
			consistencyErrs = append(consistencyErrs, fmt.Errorf("sc consistency check failed: version %d unavailable", version))
		}
		s.scOpMtx.Unlock()
	}
	if checker, ok := s.scStore.(interface {
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
	var treeProvider interface {
		GetCommitKVStore(name string) sctypes.CommitKVStore
	}
	if provider, ok := s.scStore.(interface {
		GetCommitKVStore(name string) sctypes.CommitKVStore
	}); ok {
		treeProvider = provider
	}

	for _, key := range keys {
		commitStore := s.runtime.GetCommitStore(key)
		if commitStore == nil {
			continue
		}
		legacyStore, ok := commitStore.(types.CommitKVStore)
		if !ok {
			continue
		}
		if legacyStore.GetStoreType() == types.StoreTypeIAVL {
			if treeProvider != nil {
				if tree := treeProvider.GetCommitKVStore(key.Name()); tree != nil {
					newStores[key] = commitment.NewStore(tree)
					continue
				}
			}
			if s.usesMemIAVLTreeLayer() {
				// SC not ready for this module yet (e.g. before Initialize); skip until Load completes.
				continue
			}
			newStores[key] = commitment.LegacyNewStore(key, legacyStore)
			continue
		}
		newStores[key] = legacyStore
	}

	s.mtx.Lock()
	s.ckvStore = newStores
	s.mtx.Unlock()
}

func (s *Store) loadSCVersion(version int64) error {
	loader, ok := s.scStore.(interface {
		LoadVersion(targetVersion int64, storeNames []string) error
	})
	if !ok {
		return nil
	}
	s.scOpMtx.Lock()
	defer s.scOpMtx.Unlock()
	return loader.LoadVersion(version, s.iavlStoreNames())
}

func (s *Store) iavlStoreNames() []string {
	keys := s.sortedStoreKeys()
	names := make([]string, 0, len(keys))
	for _, key := range keys {
		params, ok := s.storesParams[key]
		if !ok || params.typ != types.StoreTypeIAVL {
			continue
		}
		names = append(names, key.Name())
	}
	return names
}

func (s *Store) buildCacheStores(version int64, historical bool, readers ...historicalSnapshotReader) map[types.StoreKey]types.CacheWrapper {
	keys := s.sortedStoreKeys()

	var snapshotReader historicalSnapshotReader
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

func (s *Store) finishLoad() error {
	s.rebuildCommitStores()
	if err := s.refreshLastCommitInfoFromSC(); err != nil {
		return err
	}
	if s.scBackendIsMemIAVL() {
		version := s.latestCommittedVersion()
		if version > 0 {
			return s.checkBackendsConsistency(version, true)
		}
		return nil
	}
	return s.syncBackendsWithRuntimeVersion()
}

func (s *Store) refreshLastCommitInfoFromSC() error {
	src, ok := s.scStore.(*memIAVLStore)
	if !ok {
		return nil
	}
	ci := convertCommitInfo(src.LastCommitInfo())
	s.mtx.Lock()
	s.lastCommitInfo = amendCommitInfo(ci, s.storesParams)
	s.mtx.Unlock()
	return nil
}

func (s *Store) latestCommittedVersion() int64 {
	if s.usesMemIAVLTreeLayer() {
		s.mtx.RLock()
		defer s.mtx.RUnlock()
		if s.lastCommitInfo != nil {
			return s.lastCommitInfo.Version
		}
		return 0
	}
	return s.runtime.LastCommitID().Version
}

func (s *Store) nextCommitVersion() int64 {
	if s.usesMemIAVLTreeLayer() {
		s.mtx.RLock()
		defer s.mtx.RUnlock()
		if s.lastCommitInfo != nil {
			return s.lastCommitInfo.Version + 1
		}
		return 1
	}
	return s.runtime.LastCommitID().Version + 1
}

// flush pops pending module changesets and applies them to SS (WAL + optional async Pebble)
// and SC, matching sei-chain storev2 flush ordering. WorkingHash triggers flush so ABCI
// Commit only needs to persist the SC snapshot.
func (s *Store) flush() error {
	changeSets := s.popPendingChangeSets()
	currentVersion := s.nextCommitVersion()

	if s.ss != nil && s.config.StateStoreBackend != "" {
		ssFlushStart := time.Now()
		if len(changeSets) > 0 {
			if err := s.ss.ApplyChangeSets(currentVersion, changeSets); err != nil {
				return fmt.Errorf("ss apply changesets at version %d: %w", currentVersion, err)
			}
		} else if err := s.ss.SetLatestVersion(currentVersion); err != nil {
			return fmt.Errorf("ss set latest version at version %d: %w", currentVersion, err)
		}
		monitor.LogSeidbFlushSS(currentVersion, float64(time.Since(ssFlushStart).Nanoseconds())/1e6)
	}

	s.scOpMtx.Lock()
	defer s.scOpMtx.Unlock()
	scFlushStart := time.Now()
	err := s.scStore.ApplyChangeSets(changeSets)
	if err != nil {
		return fmt.Errorf("sc apply changesets at version %d: %w", currentVersion, err)
	}
	monitor.LogSeidbSCApplyChangeset(currentVersion, float64(time.Since(scFlushStart).Nanoseconds())/1e6)
	return nil
}

func (s *Store) popPendingChangeSets() []*NamedChangeSet {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	changeSets := make([]*NamedChangeSet, 0, len(s.ckvStore))
	for key, store := range s.ckvStore {
		csStore, ok := store.(*commitment.Store)
		if !ok {
			continue
		}
		cs := csStore.PopChangeSet()
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

func (s *Store) waitForPendingSSWrites() {
	if s.ss != nil {
		s.ss.WaitForPendingWrites()
	}
}

func (s *Store) commitNonIAVLStores() {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	for key, store := range s.ckvStore {
		params, ok := s.storesParams[key]
		if !ok || params.typ == types.StoreTypeIAVL {
			continue
		}
		_ = store.Commit()
	}
}

func (s *Store) commitInfoForProof(height int64) (*types.CommitInfo, error) {
	if s.usesMemIAVLTreeLayer() {
		s.mtx.RLock()
		defer s.mtx.RUnlock()
		if s.lastCommitInfo != nil && s.lastCommitInfo.Version == height {
			return s.lastCommitInfo, nil
		}
		latest := int64(0)
		if s.lastCommitInfo != nil {
			latest = s.lastCommitInfo.Version
		}
		return nil, fmt.Errorf("commit info for height %d unavailable (sc latest=%d)", height, latest)
	}
	return s.runtime.GetCommitInfo(height)
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
