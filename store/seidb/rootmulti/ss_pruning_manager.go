package rootmulti

import (
	"math/rand"
	"sync"
	"time"
)

type stateStorePruningManager struct {
	stateStore    interface{ HasVersion(int64) bool; EarliestVersion() int64 }
	pruner        interface{ Prune(int64) error }
	latestVersion func() int64
	keepRecent    int64
	pruneInterval int64

	startOnce sync.Once
	stopCh    chan struct{}
	stopOnce  sync.Once
	wg        sync.WaitGroup
}

func newStateStorePruningManager(
	stateStore interface{ HasVersion(int64) bool; EarliestVersion() int64 },
	pruner interface{ Prune(int64) error },
	latestVersion func() int64,
	keepRecent int64,
	pruneInterval int64,
) *stateStorePruningManager {
	return &stateStorePruningManager{
		stateStore:    stateStore,
		pruner:        pruner,
		latestVersion: latestVersion,
		keepRecent:    keepRecent,
		pruneInterval: pruneInterval,
		stopCh:        make(chan struct{}),
	}
}

func (m *stateStorePruningManager) Start() {
	if m.keepRecent <= 0 || m.pruneInterval <= 0 || m.pruner == nil || m.latestVersion == nil {
		return
	}
	m.startOnce.Do(func() {
		m.wg.Add(1)
		go m.pruneLoop()
	})
}

func (m *stateStorePruningManager) Stop() {
	m.stopOnce.Do(func() {
		close(m.stopCh)
	})
	m.wg.Wait()
}

func (m *stateStorePruningManager) pruneLoop() {
	defer m.wg.Done()

	for {
		select {
		case <-m.stopCh:
			return
		default:
		}

		latestVersion := m.latestVersion()
		pruneVersion := latestVersion - m.keepRecent
		if pruneVersion > 0 {
			_ = m.pruner.Prune(pruneVersion)
		}

		randomPercentage := rand.Float64()
		randomDelay := int64(float64(m.pruneInterval) * randomPercentage)
		sleepDuration := time.Duration(m.pruneInterval+randomDelay) * time.Second

		select {
		case <-m.stopCh:
			return
		case <-time.After(sleepDuration):
		}
	}
}
