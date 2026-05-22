package types

import (
	iavl "cosmossdk.io/store/seidb/sc/sei-iavl"
	storetypes "cosmossdk.io/store/types"
)

type NamedChangeSet struct {
	Name      string
	ChangeSet *iavl.ChangeSet
}

type SnapshotImportNode struct {
	StoreKey string
	Key      []byte
	Value    []byte
}

type StateStore interface {
	Get(storeName string, version int64, key []byte) ([]byte, error)
	Has(storeName string, version int64, key []byte) (bool, error)
	Iterator(storeName string, version int64, start, end []byte) (storetypes.Iterator, error)
	ReverseIterator(storeName string, version int64, start, end []byte) (storetypes.Iterator, error)
	Snapshot(storeName string, version int64) (map[string][]byte, bool)
	LatestVersion() int64
	HasVersion(version int64) bool
	EarliestVersion() int64
	ApplyChangeSets(version int64, changeSets []*NamedChangeSet) error
	SetLatestVersion(version int64) error
	RollbackToVersion(target int64) error
	SyncFromStores(stores map[storetypes.StoreKey]storetypes.CommitKVStore, version int64) error
	Close() error
}

type SnapshotImporter interface {
	ImportSnapshot(version int64, nodes <-chan SnapshotImportNode) error
}
