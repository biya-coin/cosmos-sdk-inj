package memstore

import (
	"sync"

	"cosmossdk.io/store/memstore/internal"
)

type (
	snapshotPool struct {
		limit int64
		list  []*snapshotItem
	}

	SnapshotPool interface {
		Get(height int64) (*internal.BTree, bool)

		Set(height int64, tree *internal.BTree)

		Limit(length int64)
	}

	snapshotItem struct {
		mtx    *sync.RWMutex
		tree   *internal.BTree
		height int64
	}
)

const defaultLimit = 10

func newSnapshotPool() *snapshotPool {
	list := make([]*snapshotItem, defaultLimit)
	for i := 0; i < defaultLimit; i++ {
		list[i] = &snapshotItem{
			mtx:    &sync.RWMutex{},
			tree:   nil,
			height: 0,
		}
	}

	return &snapshotPool{defaultLimit, list}
}

func (p *snapshotPool) Get(height int64) (*internal.BTree, bool) {
	idx := height % p.limit

	p.list[idx].mtx.RLock()
	defer p.list[idx].mtx.RUnlock()

	item := p.list[idx]
	if item.height != height {
		return nil, false
	}

	return item.tree, item.tree != nil
}

func (p *snapshotPool) Set(height int64, tree *internal.BTree) {
	idx := height % p.limit

	p.list[idx].mtx.Lock()
	p.list[idx].tree = tree
	p.list[idx].height = height
	p.list[idx].mtx.Unlock()
}

func (p *snapshotPool) Limit(limit int64) {
	if limit <= 0 {
		panic("snapshot pool limit must be positive")
	}

	p.limit = limit
	p.list = make([]*snapshotItem, limit)

	for i := int64(0); i < limit; i++ {
		p.list[i] = &snapshotItem{
			mtx:    &sync.RWMutex{},
			tree:   nil,
			height: 0,
		}
	}
}
