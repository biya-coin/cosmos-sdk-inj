package memstore

import (
	"sync/atomic"

	"cosmossdk.io/store/memstore/internal"
	"cosmossdk.io/store/types"
)

var _ types.MemStoreManager = &memStoreManager{}

type memStoreManager struct {
	// root is an atomic pointer to the current root of the memStoreManager.
	// When a branch is committed, it creates a new root node and atomically
	// swaps it with the existing one.
	root *atomic.Pointer[btree]
	// The current B-tree is stored in the root atomic.Pointer when memStoreManager.Commit() occurs.
	// The reason for creating it temporarily in this way is that it should not be read
	// in the FinalizeBlock state before BaseApp.Commit happens.
	//
	// `current` is implemented with the assumption that it is accessed only by a single writer.
	current *btree
	// base ensures that only one branch (L1) can be committed.
	//
	// It is set to nil for Trees retrieved from the snapshotPool.
	base *atomic.Pointer[btree]

	snapshotPool SnapshotPool
}

// NewMemStoreManager creates a new empty memStoreManager.
func NewMemStoreManager() *memStoreManager {
	tree := internal.NewBTree()

	root := &atomic.Pointer[btree]{}
	root.Store(tree)

	base := &atomic.Pointer[btree]{}
	base.Store(tree)

	return &memStoreManager{
		root:    root,
		current: tree.Copy(),
		base:    base,

		snapshotPool: newSnapshotPool(),
	}
}

func (t *memStoreManager) SetSnapshotPoolLimit(limit int64) {
	t.snapshotPool.ResizeAndClear(limit)
}

// GetSnapshotBranch retrieves a read-only view of the state at the given height.
// The snapshot btree is stored directly in the pool. Each call returns
// a new memStore with a copy-on-write snapshot, ensuring isolation between callers.
// The returned memStore has no manager, so top-level Commit() will panic.
func (t *memStoreManager) GetSnapshotBranch(height int64) (types.MemStore, bool) {
	snapshotTree, ok := t.snapshotPool.Get(height)
	if !ok {
		return nil, false
	}

	// Create a copy-on-write copy of the snapshot tree for isolation.
	// Each caller gets their own copy that won't affect the stored snapshot.
	return &memStore{
		parent:  nil,
		current: snapshotTree.Copy(),
		base:    nil,
		manager: nil,
	}, true
}

// Branch creates a top-level branch.
// It creates a copy-on-write snapshot of the tree's root btree as its working copy.
func (t *memStoreManager) Branch() types.MemStore {
	root := t.root.Load()
	// Create a copy-on-write snapshot for the current
	current := root.Copy()

	var base *btree
	if t.base != nil {
		base = t.base.Load()
	}

	return &memStore{
		// This is a top-level branch, so parent is nil
		parent: nil,

		current: current,
		base:    base,
		manager: t,
	}
}

// Commit finalizes the current state at the specified height by:
// 1. Ensuring the base hasn't changed (preventing concurrent commits)
// 2. Atomically updating the root btree with current changes
// 3. Creating a snapshot at the given height for future queries
// The height must be non-negative.
func (t *memStoreManager) Commit(height int64) {
	if height < 0 {
		// NOTE: When height is 0, it occurs when calling LoadLatestVersion() on an empty app.
		// Since InitChain, which registers the genesis, is called afterward,
		// MemStore.Commit(0) can happen.
		//
		// For this reason, it should be allowed.
		panic("height cannot be a negative value.")
	}

	// Since `current` is used only in a single thread, direct access is safe.
	current := t.current

	if current == nil {
		panic("No current BTree to commit")
	}

	// copy as defensive measure to prevent accidental mutations after commit
	copiedTree := current.Copy()

	if !t.root.CompareAndSwap(t.base.Load(), current) {
		panic("commit failed: concurrent modification detected")
	}
	t.base.Store(current)

	t.current = copiedTree

	snapshotTree := copiedTree.Copy()
	t.snapshotPool.Set(height, snapshotTree)
}
