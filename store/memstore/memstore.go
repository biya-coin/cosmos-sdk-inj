package memstore

import (
	"cosmossdk.io/store/memstore/internal"
	"cosmossdk.io/store/types"
)

var _ types.MemStore = &memStore{}

type btree = internal.BTree

// `memStore` implements a copy-on-write memStore operation pattern.
// Nested branches follow this structure as well, updating their parent's current on `Commit()`.
type memStore struct {
	// parent is nil for top-level branches, non-nil for nested branches
	parent *memStore

	// current holds the current working copy of the btree for this top-level branch.
	current *btree

	// base points to memStoreManager.base when the branch is created.
	//
	// If memstoreManager.base differs from base at commit time, the commit fails and a panic occurs.
	base *btree

	// manager is a reference to the parent memStoreManager, used by top-level branches to update the manager during commit.
	manager *memStoreManager
}

// Get retrieves a value for the given key from the current branch.
func (b *memStore) Get(key []byte) any {
	return b.current.Get(key)
}

// Iterator returns an iterator over the key-value pairs in the branch
// within the specified range.
//
// The iterator will include items with key >= start and key < end.
// If start is nil, it returns all items from the beginning.
// If end is nil, it returns all items until the end.
//
// If an error occurs during initialization, this method panics.
func (b *memStore) Iterator(start, end []byte) types.MemStoreIterator {
	// Create a snapshot for stable iteration (snapshot isolation).
	// Writes made after this point won't be visible to the iterator.
	snapshot := b.current.Copy()

	iter, err := snapshot.Iterator(start, end)
	if err != nil {
		panic(err)
	}

	return iter
}

// ReverseIterator returns an iterator over the key-value pairs in the branch
// within the specified range, in reverse order (from end to start).
//
// The iterator will include items with key >= start and key < end.
// If start is nil, it returns all items from the beginning.
// If end is nil, it returns all items until the end.
//
// If an error occurs during initialization, this method panics.
func (b *memStore) ReverseIterator(start, end []byte) types.MemStoreIterator {
	// Create a snapshot for stable iteration (snapshot isolation).
	// Writes made after this point won't be visible to the iterator.
	snapshot := b.current.Copy()

	iter, err := snapshot.ReverseIterator(start, end)
	if err != nil {
		panic(err)
	}

	return iter
}

// Set adds or updates a key-value pair in the current branch.
func (b *memStore) Set(key []byte, value any) {
	b.current.Set(key, value)
}

// Delete removes a key from the current branch.
func (b *memStore) Delete(key []byte) {
	b.current.Delete(key)
}

// Branch creates a nested branch on top of the current branch.
// It copies the current branch's btree to create an independent workspace.
func (b *memStore) Branch() types.MemStore {
	// Here, current refers to the current level's -1 level branch:
	//   -> If current is L3: points to L2's current
	//   -> If current is L2: points to L1's current
	//   -> If current is L1: points to memstoreManager.current
	//
	// newCurrent is a Copy()'d memstoreManager.
	newCurrent := b.current.Copy()

	return &memStore{
		parent: b,

		current: newCurrent,
	}
}

// IsChildOf returns true if this memStore was created by calling Branch()
// on the given parent memStore.
func (b *memStore) IsChildOf(parent types.MemStore) bool {
	if b.parent == nil {
		return false
	}
	parentMs, ok := parent.(*memStore)
	if !ok {
		return false
	}
	return b.parent == parentMs
}

// Commit applies the changes in the branch:
// - For nested branches, it updates the parent branch's current pointer.
// - For top-level branches, it updates memStoreManager.current with the branch's current btree.
//
// WARNING: For nested branches, this is a full replacement, not a merge.
// Any writes made to the parent AFTER creating this branch will be lost.
// Always ensure all writes go through the child branch, not the parent.
func (b *memStore) Commit() {
	if b.parent != nil {
		// nested branch: update parent's current pointer
		b.parent.current = b.current
		return
	}

	if b.current != nil {
		// top-level branch: swap *memstoreManager.current
		if b.manager.base.Load() != b.base {
			panic("commit failed: concurrent modification detected")
		}

		b.manager.current = b.current
		return
	}

	panic("unreachable code, parent is nil & current is nil")
}
