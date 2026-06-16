package rootmulti

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"sync"

	dbm "github.com/cosmos/cosmos-db"
	protoio "github.com/cosmos/gogoproto/io"
	gogotypes "github.com/cosmos/gogoproto/types"
	iavltree "github.com/cosmos/iavl"

	"cosmossdk.io/log"
	"cosmossdk.io/store/cachemulti"
	"cosmossdk.io/store/dbadapter"
	"cosmossdk.io/store/iavl"
	"cosmossdk.io/store/listenkv"
	"cosmossdk.io/store/mem"
	"cosmossdk.io/store/metrics"
	"cosmossdk.io/store/pruning"
	pruningtypes "cosmossdk.io/store/pruning/types"
	snapshottypes "cosmossdk.io/store/snapshots/types"
	"cosmossdk.io/store/tracekv"
	"cosmossdk.io/store/transient"
	"cosmossdk.io/store/types"
)

const (
	storeV2LatestVersionKey = "s/latest"
	storeV2CommitInfoKeyFmt = "s/%d" // s/<version>
)

type storev2Runtime struct {
	db                  dbm.DB
	logger              log.Logger
	lastCommitInfo      *types.CommitInfo
	pruningManager      *pruning.Manager
	iavlCacheSize       int
	iavlDisableFastNode bool
	iavlSyncPruning     bool
	storesParams        map[types.StoreKey]runtimeStoreParams
	stores              map[types.StoreKey]types.CommitStore
	keysByName          map[string]types.StoreKey
	initialVersion      int64
	removalMap          map[types.StoreKey]bool
	traceWriter         io.Writer
	traceContext        types.TraceContext
	traceContextMutex   sync.Mutex
	interBlockCache     types.MultiStorePersistentCache
	listeners           map[types.StoreKey]*types.MemoryListener
	metrics             metrics.StoreMetrics
}

type runtimeStoreParams struct {
	key            types.StoreKey
	db             dbm.DB
	typ            types.StoreType
	initialVersion uint64
}

func newStoreV2Runtime(db dbm.DB, logger log.Logger, metricGatherer metrics.StoreMetrics) *storev2Runtime {
	if metricGatherer == nil {
		metricGatherer = metrics.NewNoOpMetrics()
	}
	return &storev2Runtime{
		db:                  db,
		logger:              logger,
		iavlCacheSize:       iavl.DefaultIAVLCacheSize,
		iavlDisableFastNode: false,
		storesParams:        make(map[types.StoreKey]runtimeStoreParams),
		stores:              make(map[types.StoreKey]types.CommitStore),
		keysByName:          make(map[string]types.StoreKey),
		listeners:           make(map[types.StoreKey]*types.MemoryListener),
		removalMap:          make(map[types.StoreKey]bool),
		pruningManager:      pruning.NewManager(db, logger),
		metrics:             metricGatherer,
	}
}

func (rs *storev2Runtime) GetPruning() pruningtypes.PruningOptions {
	return rs.pruningManager.GetOptions()
}

func (rs *storev2Runtime) SetPruning(pruningOpts pruningtypes.PruningOptions) {
	rs.pruningManager.SetOptions(pruningOpts)
}

func (rs *storev2Runtime) SetMetrics(metricGatherer metrics.StoreMetrics) {
	if metricGatherer == nil {
		metricGatherer = metrics.NewNoOpMetrics()
	}
	rs.metrics = metricGatherer
}

func (rs *storev2Runtime) SetSnapshotInterval(snapshotInterval uint64) {
	rs.pruningManager.SetSnapshotInterval(snapshotInterval)
}

func (rs *storev2Runtime) PruneSnapshotHeight(height int64) {
	rs.pruningManager.HandleSnapshotHeight(height)
}

func (rs *storev2Runtime) SetIAVLCacheSize(cacheSize int) {
	rs.iavlCacheSize = cacheSize
}

func (rs *storev2Runtime) SetIAVLDisableFastNode(disableFastNode bool) {
	rs.iavlDisableFastNode = disableFastNode
}

func (rs *storev2Runtime) SetIAVLSyncPruning(syncPruning bool) {
	rs.iavlSyncPruning = syncPruning
}

func (rs *storev2Runtime) GetStoreType() types.StoreType {
	return types.StoreTypeMulti
}

func (rs *storev2Runtime) MountStoreWithDB(key types.StoreKey, typ types.StoreType, db dbm.DB) {
	if key == nil {
		panic("MountStoreWithDB() key cannot be nil")
	}
	if _, ok := rs.storesParams[key]; ok {
		panic(fmt.Sprintf("store duplicate store key %v", key))
	}
	if _, ok := rs.keysByName[key.Name()]; ok {
		panic(fmt.Sprintf("store duplicate store key name %v", key))
	}

	rs.storesParams[key] = runtimeStoreParams{
		key: key,
		db:  db,
		typ: typ,
	}
	rs.keysByName[key.Name()] = key
}

func (rs *storev2Runtime) GetCommitStore(key types.StoreKey) types.CommitStore {
	if rs.interBlockCache != nil {
		if store := rs.interBlockCache.Unwrap(key); store != nil {
			return store
		}
	}
	return rs.stores[key]
}

func (rs *storev2Runtime) GetCommitKVStore(key types.StoreKey) types.CommitKVStore {
	store := rs.GetCommitStore(key)
	if store == nil {
		return nil
	}
	kvStore, ok := store.(types.CommitKVStore)
	if !ok {
		panic(fmt.Sprintf("store with key %v is not CommitKVStore (got: %T)", key, store))
	}
	return kvStore
}

func (rs *storev2Runtime) LoadLatestVersionAndUpgrade(upgrades *types.StoreUpgrades) error {
	ver := rs.getLatestVersion()
	return rs.loadVersion(ver, upgrades)
}

func (rs *storev2Runtime) LoadVersionAndUpgrade(ver int64, upgrades *types.StoreUpgrades) error {
	return rs.loadVersion(ver, upgrades)
}

func (rs *storev2Runtime) LoadLatestVersion() error {
	ver := rs.getLatestVersion()
	return rs.loadVersion(ver, nil)
}

func (rs *storev2Runtime) LoadVersion(ver int64) error {
	return rs.loadVersion(ver, nil)
}

func (rs *storev2Runtime) loadVersion(ver int64, upgrades *types.StoreUpgrades) error {
	infos := make(map[string]types.StoreInfo)
	cInfo := &types.CommitInfo{}

	if ver != 0 {
		var err error
		cInfo, err = rs.GetCommitInfo(ver)
		if err != nil {
			return err
		}
		for _, storeInfo := range cInfo.StoreInfos {
			infos[storeInfo.Name] = storeInfo
		}
	}

	storeKeys := make([]types.StoreKey, 0, len(rs.storesParams))
	for key := range rs.storesParams {
		storeKeys = append(storeKeys, key)
	}
	if upgrades != nil {
		sort.Slice(storeKeys, func(i, j int) bool {
			return storeKeys[i].Name() < storeKeys[j].Name()
		})
	}

	newStores := make(map[types.StoreKey]types.CommitStore)
	for _, key := range storeKeys {
		params := rs.storesParams[key]
		commitID := rs.getCommitID(infos, key.Name())

		if upgrades.IsAdded(key.Name()) || upgrades.RenamedFrom(key.Name()) != "" {
			params.initialVersion = uint64(ver) + 1
		} else if commitID.Version != ver && params.typ == types.StoreTypeIAVL {
			return fmt.Errorf(
				"version of store %s mismatch root store's version; expected %d got %d; new stores should be added using StoreUpgrades",
				key.Name(), ver, commitID.Version,
			)
		}

		store, err := rs.loadCommitStoreFromParams(key, commitID, params)
		if err != nil {
			return fmt.Errorf("failed to load store: %w", err)
		}
		newStores[key] = store

		if upgrades.IsDeleted(key.Name()) {
			if kv, ok := store.(types.KVStore); ok {
				if err := deleteKVStore(kv); err != nil {
					return fmt.Errorf("failed to delete store %s: %w", key.Name(), err)
				}
			}
			rs.removalMap[key] = true
		} else if oldName := upgrades.RenamedFrom(key.Name()); oldName != "" {
			oldKey := types.NewKVStoreKey(oldName)
			oldParams := runtimeStoreParams{
				key: oldKey,
				db:  params.db,
				typ: params.typ,
			}
			oldStore, err := rs.loadCommitStoreFromParams(oldKey, rs.getCommitID(infos, oldName), oldParams)
			if err != nil {
				return fmt.Errorf("failed to load old store %s: %w", oldName, err)
			}
			oldKV, oldOK := oldStore.(types.KVStore)
			newKV, newOK := store.(types.KVStore)
			if oldOK && newOK {
				if err := moveKVStoreData(oldKV, newKV); err != nil {
					return fmt.Errorf("failed to move store %s -> %s: %w", oldName, key.Name(), err)
				}
				newStores[oldKey] = oldStore
				rs.removalMap[oldKey] = true
			}
		}
	}

	rs.lastCommitInfo = cInfo
	rs.stores = newStores

	if err := rs.pruningManager.LoadSnapshotHeights(rs.db); err != nil {
		return err
	}
	return nil
}

func (rs *storev2Runtime) getCommitID(infos map[string]types.StoreInfo, name string) types.CommitID {
	info, ok := infos[name]
	if !ok {
		return types.CommitID{}
	}
	return info.CommitId
}

func (rs *storev2Runtime) SetInterBlockCache(c types.MultiStorePersistentCache) {
	rs.interBlockCache = c
}

func (rs *storev2Runtime) SetTracer(w io.Writer) types.MultiStore {
	rs.traceWriter = w
	return rs
}

func (rs *storev2Runtime) SetTracingContext(tc types.TraceContext) types.MultiStore {
	rs.traceContextMutex.Lock()
	defer rs.traceContextMutex.Unlock()
	rs.traceContext = rs.traceContext.Merge(tc)
	return rs
}

func (rs *storev2Runtime) getTracingContext() types.TraceContext {
	rs.traceContextMutex.Lock()
	defer rs.traceContextMutex.Unlock()

	if rs.traceContext == nil {
		return nil
	}
	ctx := types.TraceContext{}
	for k, v := range rs.traceContext {
		ctx[k] = v
	}
	return ctx
}

func (rs *storev2Runtime) TracingEnabled() bool {
	return rs.traceWriter != nil
}

func (rs *storev2Runtime) AddListeners(keys []types.StoreKey) {
	for _, key := range keys {
		if rs.listeners[key] == nil {
			rs.listeners[key] = types.NewMemoryListener()
		}
	}
}

func (rs *storev2Runtime) ListeningEnabled(key types.StoreKey) bool {
	if ls, ok := rs.listeners[key]; ok {
		return ls != nil
	}
	return false
}

func (rs *storev2Runtime) PopStateCache() []*types.StoreKVPair {
	var cache []*types.StoreKVPair
	for key := range rs.listeners {
		ls := rs.listeners[key]
		if ls != nil {
			cache = append(cache, ls.PopStateCache()...)
		}
	}
	sort.SliceStable(cache, func(i, j int) bool {
		return cache[i].StoreKey < cache[j].StoreKey
	})
	return cache
}

func (rs *storev2Runtime) LatestVersion() int64 {
	return rs.LastCommitID().Version
}

func (rs *storev2Runtime) LastCommitID() types.CommitID {
	if rs.lastCommitInfo == nil {
		emptyHash := sha256.Sum256([]byte{})
		return types.CommitID{
			Version: rs.getLatestVersion(),
			Hash:    emptyHash[:],
		}
	}
	if len(rs.lastCommitInfo.CommitID().Hash) == 0 {
		emptyHash := sha256.Sum256([]byte{})
		return types.CommitID{
			Version: rs.lastCommitInfo.Version,
			Hash:    emptyHash[:],
		}
	}
	return rs.lastCommitInfo.CommitID()
}

func (rs *storev2Runtime) Commit() types.CommitID {
	var previousHeight, version int64
	if rs.lastCommitInfo != nil && rs.lastCommitInfo.GetVersion() == 0 && rs.initialVersion > 1 {
		version = rs.initialVersion
	} else {
		if rs.lastCommitInfo != nil {
			previousHeight = rs.lastCommitInfo.GetVersion()
		}
		version = previousHeight + 1
	}

	rs.pausePruning(true)
	rs.lastCommitInfo = runtimeCommitStores(version, rs.stores, rs.removalMap)
	rs.pausePruning(false)

	rs.flushMetadata(version, rs.lastCommitInfo)

	for sk := range rs.removalMap {
		if _, ok := rs.stores[sk]; ok {
			delete(rs.stores, sk)
			delete(rs.storesParams, sk)
			delete(rs.keysByName, sk.Name())
		}
	}
	rs.removalMap = make(map[types.StoreKey]bool)

	if err := rs.handlePruning(version); err != nil {
		rs.logger.Error("failed to prune store, please check your pruning configuration", "err", err)
	}

	return types.CommitID{
		Version: version,
		Hash:    rs.lastCommitInfo.Hash(),
	}
}

// SyncCommitMetadata records the SC commit info in the legacy metadata DB without
// committing mounted IAVL trees (Phase B: AppHash is owned by memiavl SC).
func (rs *storev2Runtime) SyncCommitMetadata(version int64, info *types.CommitInfo) {
	if info == nil {
		info = &types.CommitInfo{Version: version}
	}
	rs.pausePruning(true)
	rs.lastCommitInfo = info
	rs.pausePruning(false)

	rs.flushMetadata(version, info)

	storeKeys := runtimeKeysFromStoreKeyMap(rs.stores)
	for _, key := range storeKeys {
		if rs.removalMap[key] {
			continue
		}
		if rs.stores[key].GetStoreType() == types.StoreTypeIAVL {
			continue
		}
		_ = rs.stores[key].Commit()
	}

	for sk := range rs.removalMap {
		if _, ok := rs.stores[sk]; ok {
			delete(rs.stores, sk)
			delete(rs.storesParams, sk)
			delete(rs.keysByName, sk.Name())
		}
	}
	rs.removalMap = make(map[types.StoreKey]bool)

	if err := rs.handlePruning(version); err != nil {
		rs.logger.Error("failed to prune store, please check your pruning configuration", "err", err)
	}
}

// LoadVersionForSCBackedCommit reloads multistore metadata for the target height while
// keeping legacy cosmos/iavl trees at their on-disk version (Phase B: SC owns IAVL data).
func (rs *storev2Runtime) LoadVersionForSCBackedCommit(version int64) error {
	if version == 0 {
		version = rs.getLatestVersion()
	}

	infos := make(map[string]types.StoreInfo)
	cInfo := &types.CommitInfo{}
	if version != 0 {
		var err error
		cInfo, err = rs.GetCommitInfo(version)
		if err != nil {
			return err
		}
		for _, storeInfo := range cInfo.StoreInfos {
			infos[storeInfo.Name] = storeInfo
		}
	}

	storeKeys := runtimeKeysFromStoreKeyMap(rs.storesParams)
	newStores := make(map[types.StoreKey]types.CommitStore, len(storeKeys))
	for _, key := range storeKeys {
		params := rs.storesParams[key]
		commitID := rs.getCommitID(infos, key.Name())
		if params.typ == types.StoreTypeIAVL {
			commitID = types.CommitID{}
		}

		store, err := rs.loadCommitStoreFromParams(key, commitID, params)
		if err != nil {
			return fmt.Errorf("failed to load store: %w", err)
		}
		newStores[key] = store
	}

	rs.lastCommitInfo = cInfo
	rs.stores = newStores

	if err := rs.pruningManager.LoadSnapshotHeights(rs.db); err != nil {
		return err
	}
	return nil
}

func (rs *storev2Runtime) WorkingHash() []byte {
	storeInfos := make([]types.StoreInfo, 0, len(rs.stores))
	storeKeys := runtimeKeysFromStoreKeyMap(rs.stores)
	for _, key := range storeKeys {
		store := rs.stores[key]
		if store.GetStoreType() != types.StoreTypeIAVL {
			continue
		}
		if !rs.removalMap[key] {
			storeInfos = append(storeInfos, types.StoreInfo{
				Name: key.Name(),
				CommitId: types.CommitID{
					Hash: store.WorkingHash(),
				},
			})
		}
	}
	return buildWorkingCommitInfoFromRuntimeStores(storeInfos, rs.storesParams).Hash()
}

func (rs *storev2Runtime) CacheWrap() types.CacheWrap {
	return rs.CacheMultiStore().(types.CacheWrap)
}

func (rs *storev2Runtime) CacheWrapWithTrace(_ io.Writer, _ types.TraceContext) types.CacheWrap {
	return rs.CacheWrap()
}

func (rs *storev2Runtime) CacheMultiStore() types.CacheMultiStore {
	stores := make(map[types.StoreKey]types.CacheWrapper)
	for key, store := range rs.stores {
		cacheStore := types.CacheWrapper(store)
		if kvStore, ok := cacheStore.(types.KVStore); ok && rs.ListeningEnabled(key) {
			cacheStore = listenkv.NewStore(kvStore, key, rs.listeners[key])
		}
		stores[key] = cacheStore
	}
	return cachemulti.NewFromKVStore(stores, rs.traceWriter, rs.getTracingContext())
}

func (rs *storev2Runtime) CacheMultiStoreWithVersion(version int64) (types.CacheMultiStore, error) {
	cachedStores := make(map[types.StoreKey]types.CacheWrapper)
	var commitInfo *types.CommitInfo
	storeInfos := map[string]bool{}
	for key, store := range rs.stores {
		var cacheStore types.CacheWrapper
		switch store.GetStoreType() {
		case types.StoreTypeIAVL:
			store = rs.GetCommitKVStore(key)
			var err error
			cacheStore, err = store.(*iavl.Store).GetImmutable(version)
			if err != nil {
				if commitInfo == nil {
					commitInfo, err = rs.GetCommitInfo(version)
					if err != nil {
						return nil, err
					}
					for _, storeInfo := range commitInfo.StoreInfos {
						storeInfos[storeInfo.Name] = true
					}
				}
				if storeInfos[key.Name()] {
					return nil, err
				}
				cacheStore = dbadapter.Store{DB: dbm.NewMemDB()}
			}
		default:
			cacheStore = store
		}

		if kvStore, ok := cacheStore.(types.KVStore); ok && rs.ListeningEnabled(key) {
			cacheStore = listenkv.NewStore(kvStore, key, rs.listeners[key])
		}
		cachedStores[key] = cacheStore
	}
	return cachemulti.NewFromKVStore(cachedStores, rs.traceWriter, rs.getTracingContext()), nil
}

func (rs *storev2Runtime) GetStore(key types.StoreKey) types.Store {
	store := rs.GetCommitStore(key)
	if store == nil {
		panic(fmt.Sprintf("store does not exist for key: %s", key.Name()))
	}
	return store
}

func (rs *storev2Runtime) GetKVStore(key types.StoreKey) types.KVStore {
	store := rs.stores[key]
	if store == nil {
		panic(fmt.Sprintf("store does not exist for key: %s", key.Name()))
	}
	kvStore, ok := store.(types.KVStore)
	if !ok {
		panic(fmt.Sprintf("store with key %v is not KVStore", key))
	}
	if rs.TracingEnabled() {
		kvStore = tracekv.NewStore(kvStore, rs.traceWriter, rs.getTracingContext())
	}
	if rs.ListeningEnabled(key) {
		kvStore = listenkv.NewStore(kvStore, key, rs.listeners[key])
	}
	return kvStore
}

func (rs *storev2Runtime) GetObjKVStore(key types.StoreKey) types.ObjKVStore {
	store := rs.stores[key]
	if store == nil {
		panic(fmt.Sprintf("store does not exist for key: %s", key.Name()))
	}
	objStore, ok := store.(types.ObjKVStore)
	if !ok {
		panic(fmt.Sprintf("store with key %v is not ObjKVStore", key))
	}
	return objStore
}

func (rs *storev2Runtime) Query(req *types.RequestQuery) (*types.ResponseQuery, error) {
	storeName, subpath, err := parsePath(req.Path)
	if err != nil {
		return &types.ResponseQuery{}, err
	}

	store := rs.GetStoreByName(storeName)
	if store == nil {
		return &types.ResponseQuery{}, fmt.Errorf("no such store: %s", storeName)
	}

	queryable, ok := store.(types.Queryable)
	if !ok {
		return &types.ResponseQuery{}, fmt.Errorf("store %s (type %T) doesn't support queries", storeName, store)
	}

	trimmedReq := *req
	trimmedReq.Path = subpath
	res, err := queryable.Query(&trimmedReq)
	if err != nil || !req.Prove || !requireProof(subpath) {
		return res, err
	}

	if res.ProofOps == nil || len(res.ProofOps.Ops) == 0 {
		return &types.ResponseQuery{}, fmt.Errorf("proof is unexpectedly empty; ensure height has not been pruned")
	}

	var commitInfo *types.CommitInfo
	if rs.lastCommitInfo != nil && res.Height == rs.lastCommitInfo.Version {
		commitInfo = rs.lastCommitInfo
	} else {
		commitInfo, err = rs.GetCommitInfo(res.Height)
		if err != nil {
			return &types.ResponseQuery{}, err
		}
	}

	res.ProofOps.Ops = append(res.ProofOps.Ops, commitInfo.ProofOp(storeName))
	return res, nil
}

func (rs *storev2Runtime) SetInitialVersion(version int64) error {
	rs.initialVersion = version
	for key, store := range rs.stores {
		if store.GetStoreType() == types.StoreTypeIAVL {
			store = rs.GetCommitKVStore(key)
			store.(types.StoreWithInitialVersion).SetInitialVersion(version)
		}
	}
	return nil
}

func (rs *storev2Runtime) Snapshot(height uint64, protoWriter protoio.Writer) error {
	if height == 0 {
		return fmt.Errorf("cannot snapshot height 0")
	}
	if height > uint64(rs.getLatestVersion()) {
		return fmt.Errorf("cannot snapshot future height %v", height)
	}

	type namedStore struct {
		*iavl.Store
		name string
	}
	stores := []namedStore{}
	keys := runtimeKeysFromStoreKeyMap(rs.stores)
	for _, key := range keys {
		switch store := rs.GetCommitStore(key).(type) {
		case *iavl.Store:
			stores = append(stores, namedStore{name: key.Name(), Store: store})
		case *transient.Store, *mem.Store, *transient.ObjStore:
			continue
		default:
			return fmt.Errorf("don't know how to snapshot store %q of type %T", key.Name(), store)
		}
	}
	sort.Slice(stores, func(i, j int) bool { return strings.Compare(stores[i].name, stores[j].name) < 0 })

	for _, store := range stores {
		exporter, err := store.Export(int64(height))
		if err != nil {
			return err
		}

		err = func() error {
			defer exporter.Close()

			if err := protoWriter.WriteMsg(&snapshottypes.SnapshotItem{
				Item: &snapshottypes.SnapshotItem_Store{
					Store: &snapshottypes.SnapshotStoreItem{Name: store.name},
				},
			}); err != nil {
				return err
			}

			for {
				node, err := exporter.Next()
				if err == iavltree.ErrorExportDone {
					break
				}
				if err != nil {
					return err
				}
				if err := protoWriter.WriteMsg(&snapshottypes.SnapshotItem{
					Item: &snapshottypes.SnapshotItem_IAVL{
						IAVL: &snapshottypes.SnapshotIAVLItem{
							Key:     node.Key,
							Value:   node.Value,
							Height:  int32(node.Height),
							Version: node.Version,
						},
					},
				}); err != nil {
					return err
				}
			}
			return nil
		}()
		if err != nil {
			return err
		}
	}
	return nil
}

func (rs *storev2Runtime) Restore(
	height uint64,
	format uint32,
	protoReader protoio.Reader,
) (snapshottypes.SnapshotItem, error) {
	return rs.restoreWithNodeHook(height, format, protoReader, nil)
}

func (rs *storev2Runtime) RestoreWithNodeHook(
	height uint64,
	format uint32,
	protoReader protoio.Reader,
	nodeHook func(storeName string, key, value []byte, nodeHeight int8),
) (snapshottypes.SnapshotItem, error) {
	return rs.restoreWithNodeHook(height, format, protoReader, nodeHook)
}

func (rs *storev2Runtime) restoreWithNodeHook(
	height uint64,
	_ uint32,
	protoReader protoio.Reader,
	nodeHook func(storeName string, key, value []byte, nodeHeight int8),
) (snapshottypes.SnapshotItem, error) {
	var importer *iavltree.Importer
	var snapshotItem snapshottypes.SnapshotItem
	var currentStoreName string
loop:
	for {
		snapshotItem = snapshottypes.SnapshotItem{}
		err := protoReader.ReadMsg(&snapshotItem)
		if err == io.EOF {
			break
		}
		if err != nil {
			return snapshottypes.SnapshotItem{}, fmt.Errorf("invalid protobuf message: %w", err)
		}

		switch item := snapshotItem.Item.(type) {
		case *snapshottypes.SnapshotItem_Store:
			currentStoreName = item.Store.Name
			if importer != nil {
				if err := importer.Commit(); err != nil {
					return snapshottypes.SnapshotItem{}, fmt.Errorf("IAVL commit failed: %w", err)
				}
				importer.Close()
			}
			store, ok := rs.GetStoreByName(currentStoreName).(*iavl.Store)
			if !ok || store == nil {
				return snapshottypes.SnapshotItem{}, fmt.Errorf("cannot import into non-IAVL store %q", currentStoreName)
			}
			var err error
			importer, err = store.Import(int64(height))
			if err != nil {
				return snapshottypes.SnapshotItem{}, fmt.Errorf("import failed: %w", err)
			}
			defer importer.Close()
		case *snapshottypes.SnapshotItem_IAVL:
			if importer == nil {
				return snapshottypes.SnapshotItem{}, fmt.Errorf("received IAVL node item before store item")
			}
			if item.IAVL.Height > math.MaxInt8 {
				return snapshottypes.SnapshotItem{}, fmt.Errorf("node height %v cannot exceed %v", item.IAVL.Height, math.MaxInt8)
			}
			node := &iavltree.ExportNode{
				Key:     item.IAVL.Key,
				Value:   item.IAVL.Value,
				Height:  int8(item.IAVL.Height),
				Version: item.IAVL.Version,
			}
			if node.Key == nil {
				node.Key = []byte{}
			}
			if node.Height == 0 && node.Value == nil {
				node.Value = []byte{}
			}
			if nodeHook != nil {
				nodeHook(currentStoreName, node.Key, node.Value, node.Height)
			}
			if err := importer.Add(node); err != nil {
				return snapshottypes.SnapshotItem{}, fmt.Errorf("IAVL node import failed: %w", err)
			}
		default:
			break loop
		}
	}

	if importer != nil {
		if err := importer.Commit(); err != nil {
			return snapshottypes.SnapshotItem{}, fmt.Errorf("IAVL commit failed: %w", err)
		}
		importer.Close()
	}

	rs.flushMetadata(int64(height), rs.buildCommitInfo(int64(height)))
	return snapshotItem, rs.LoadLatestVersion()
}

func (rs *storev2Runtime) RollbackToVersion(target int64) error {
	if target <= 0 {
		return fmt.Errorf("invalid rollback height target: %d", target)
	}

	for key, store := range rs.stores {
		if store.GetStoreType() == types.StoreTypeIAVL {
			store = rs.GetCommitKVStore(key)
			if err := store.(*iavl.Store).LoadVersionForOverwriting(target); err != nil {
				return err
			}
		}
	}

	rs.flushMetadata(target, rs.buildCommitInfo(target))
	return rs.LoadLatestVersion()
}

func (rs *storev2Runtime) GetCommitInfo(ver int64) (*types.CommitInfo, error) {
	cInfoKey := fmt.Sprintf(storeV2CommitInfoKeyFmt, ver)
	bz, err := rs.db.Get([]byte(cInfoKey))
	if err != nil {
		return nil, fmt.Errorf("failed to get commit info: %w", err)
	}
	if bz == nil {
		return nil, errors.New("no commit info found")
	}

	cInfo := &types.CommitInfo{}
	if err = cInfo.Unmarshal(bz); err != nil {
		return nil, fmt.Errorf("failed unmarshal commit info: %w", err)
	}
	return cInfo, nil
}

func (rs *storev2Runtime) loadCommitStoreFromParams(
	key types.StoreKey,
	id types.CommitID,
	params runtimeStoreParams,
) (types.CommitStore, error) {
	var db dbm.DB
	if params.db != nil {
		db = dbm.NewPrefixDB(params.db, []byte("s/_/"))
	} else {
		prefix := "s/k:" + params.key.Name() + "/"
		db = dbm.NewPrefixDB(rs.db, []byte(prefix))
	}

	switch params.typ {
	case types.StoreTypeMulti:
		panic("recursive MultiStores not yet supported")
	case types.StoreTypeIAVL:
		store, err := iavl.LoadStoreWithOpts(
			db,
			rs.logger,
			key,
			id,
			params.initialVersion,
			rs.iavlCacheSize,
			rs.iavlDisableFastNode,
			rs.metrics,
			iavltree.AsyncPruningOption(!rs.iavlSyncPruning),
		)
		if err != nil {
			return nil, err
		}
		if rs.interBlockCache != nil {
			store = rs.interBlockCache.GetStoreCache(key, store)
		}
		return store, nil
	case types.StoreTypeDB:
		return commitDBStoreAdapter{Store: dbadapter.Store{DB: db}}, nil
	case types.StoreTypeTransient:
		if _, ok := key.(*types.TransientStoreKey); !ok {
			return nil, fmt.Errorf("unexpected key type for a TransientStoreKey; got: %s, %T", key.String(), key)
		}
		return transient.NewStore(), nil
	case types.StoreTypeMemory:
		if _, ok := key.(*types.MemoryStoreKey); !ok {
			return nil, fmt.Errorf("unexpected key type for a MemoryStoreKey; got: %s, %T", key.String(), key)
		}
		return mem.NewStore(), nil
	case types.StoreTypeObject:
		if _, ok := key.(*types.ObjectStoreKey); !ok {
			return nil, fmt.Errorf("unexpected key type for a ObjectStoreKey; got: %s, %T", key.String(), key)
		}
		return transient.NewObjStore(), nil
	default:
		panic(fmt.Sprintf("unrecognized store type %v", params.typ))
	}
}

func (rs *storev2Runtime) buildCommitInfo(version int64) *types.CommitInfo {
	keys := runtimeKeysFromStoreKeyMap(rs.stores)
	storeInfos := []types.StoreInfo{}
	for _, key := range keys {
		store := rs.stores[key]
		storeType := store.GetStoreType()
		if storeType == types.StoreTypeTransient || storeType == types.StoreTypeMemory || storeType == types.StoreTypeObject {
			continue
		}
		storeInfos = append(storeInfos, types.StoreInfo{
			Name:     key.Name(),
			CommitId: store.LastCommitID(),
		})
	}
	return &types.CommitInfo{
		Version:    version,
		StoreInfos: storeInfos,
	}
}

func (rs *storev2Runtime) GetStoreByName(name string) types.Store {
	key := rs.keysByName[name]
	if key == nil {
		return nil
	}
	return rs.GetCommitStore(key)
}

func (rs *storev2Runtime) handlePruning(version int64) error {
	pruneHeight := rs.pruningManager.GetPruningHeight(version)
	return rs.pruneStores(pruneHeight)
}

func (rs *storev2Runtime) pruneStores(pruningHeight int64) error {
	if pruningHeight <= 0 {
		return nil
	}
	for key, store := range rs.stores {
		if store.GetStoreType() != types.StoreTypeIAVL {
			continue
		}
		store = rs.GetCommitKVStore(key)
		err := store.(*iavl.Store).DeleteVersionsTo(pruningHeight)
		if err == nil {
			continue
		}
		if errors.Is(err, iavltree.ErrVersionDoesNotExist) {
			return err
		}
		rs.logger.Error("failed to prune store", "key", key, "err", err)
	}
	return nil
}

func (rs *storev2Runtime) pausePruning(pause bool) {
	for _, store := range rs.stores {
		if pauseable, ok := store.(types.PausablePruner); ok {
			pauseable.PausePruning(pause)
		}
	}
}

func (rs *storev2Runtime) flushMetadata(version int64, cInfo *types.CommitInfo) {
	batch := rs.db.NewBatch()
	defer func() {
		_ = batch.Close()
	}()

	if cInfo != nil {
		bz, err := cInfo.Marshal()
		if err != nil {
			panic(err)
		}
		if err := batch.Set([]byte(fmt.Sprintf(storeV2CommitInfoKeyFmt, version)), bz); err != nil {
			panic(err)
		}
	}

	bz, err := gogotypes.StdInt64Marshal(version)
	if err != nil {
		panic(err)
	}
	if err := batch.Set([]byte(storeV2LatestVersionKey), bz); err != nil {
		panic(err)
	}

	if err := batch.WriteSync(); err != nil {
		panic(fmt.Errorf("error on batch write %w", err))
	}
}

func (rs *storev2Runtime) getLatestVersion() int64 {
	bz, err := rs.db.Get([]byte(storeV2LatestVersionKey))
	if err != nil {
		panic(err)
	}
	if bz == nil {
		return 0
	}

	var latestVersion int64
	if err := gogotypes.StdInt64Unmarshal(&latestVersion, bz); err != nil {
		panic(err)
	}
	return latestVersion
}

func runtimeCommitStores(
	version int64,
	storeMap map[types.StoreKey]types.CommitStore,
	removalMap map[types.StoreKey]bool,
) *types.CommitInfo {
	storeInfos := make([]types.StoreInfo, 0, len(storeMap))
	storeKeys := runtimeKeysFromStoreKeyMap(storeMap)

	for _, key := range storeKeys {
		store := storeMap[key]
		last := store.LastCommitID()

		var commitID types.CommitID
		if last.Version >= version {
			last.Version = version
			commitID = last
		} else {
			commitID = store.Commit()
		}

		storeType := store.GetStoreType()
		if storeType == types.StoreTypeTransient || storeType == types.StoreTypeMemory || storeType == types.StoreTypeObject {
			continue
		}
		if !removalMap[key] {
			storeInfos = append(storeInfos, types.StoreInfo{
				Name:     key.Name(),
				CommitId: commitID,
			})
		}
	}

	sort.SliceStable(storeInfos, func(i, j int) bool {
		return strings.Compare(storeInfos[i].Name, storeInfos[j].Name) < 0
	})

	return &types.CommitInfo{
		Version:    version,
		StoreInfos: storeInfos,
	}
}

func runtimeKeysFromStoreKeyMap[V any](m map[types.StoreKey]V) []types.StoreKey {
	keys := make([]types.StoreKey, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i].Name() < keys[j].Name()
	})
	return keys
}

func deleteKVStore(kv types.KVStore) error {
	var keys [][]byte
	itr := kv.Iterator(nil, nil)
	for itr.Valid() {
		keys = append(keys, itr.Key())
		itr.Next()
	}
	if err := itr.Close(); err != nil {
		return err
	}
	for _, key := range keys {
		kv.Delete(key)
	}
	return nil
}

func moveKVStoreData(oldDB, newDB types.KVStore) error {
	itr := oldDB.Iterator(nil, nil)
	for itr.Valid() {
		newDB.Set(itr.Key(), itr.Value())
		itr.Next()
	}
	if err := itr.Close(); err != nil {
		return err
	}
	return deleteKVStore(oldDB)
}

var runtimeCommitHash = []byte("FAKE_HASH")

// commitDBStoreAdapter mirrors rootmulti's DB store adapter behavior.
type commitDBStoreAdapter struct {
	dbadapter.Store
}

func (cdsa commitDBStoreAdapter) Commit() types.CommitID {
	return types.CommitID{
		Version: -1,
		Hash:    runtimeCommitHash,
	}
}

func (cdsa commitDBStoreAdapter) LastCommitID() types.CommitID {
	return types.CommitID{
		Version: -1,
		Hash:    runtimeCommitHash,
	}
}

func (cdsa commitDBStoreAdapter) WorkingHash() []byte {
	return runtimeCommitHash
}

func (cdsa commitDBStoreAdapter) SetPruning(_ pruningtypes.PruningOptions) {}

func (cdsa commitDBStoreAdapter) GetPruning() pruningtypes.PruningOptions {
	return pruningtypes.NewPruningOptions(pruningtypes.PruningUndefined)
}
