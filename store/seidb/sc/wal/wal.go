package wal

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	lws "chainmaker.org/chainmaker/lws"

	"cosmossdk.io/store/seidb/sc/common/threading"
)

const defaultBufferSize = 1024

type WAL[T any] struct {
	ctx    context.Context
	cancel context.CancelFunc

	dir       string
	log       *lws.Lws
	config    Config
	marshal   MarshalFn[T]
	unmarshal UnmarshalFn[T]

	asyncWrites bool

	writeChan    chan *writeRequest[T]
	truncateChan chan *truncateRequest
	waitChan     chan chan struct{}
	closeReqChan chan struct{}
	closeErrChan chan error

	asyncError atomic.Pointer[error]

	mu sync.RWMutex
}

type truncater interface {
	TruncateBefore(index uint64) error
	TruncateAfter(index uint64) error
}

type truncateRequest struct {
	before  bool
	index   uint64
	errChan chan error
}

type writeRequest[T any] struct {
	entry   T
	errChan chan error
}

type Config struct {
	KeepRecent uint64

	PruneInterval time.Duration

	WriteBufferSize int

	WriteBatchSize int

	FsyncEnabled bool

	DeepCopyEnabled bool

	AllowEmpty bool
}

func NewWAL[T any](
	ctx context.Context,
	marshal MarshalFn[T],
	unmarshal UnmarshalFn[T],
	dir string,
	config Config,
) (*WAL[T], error) {
	log, err := open(dir, config)
	if err != nil {
		return nil, err
	}

	bufferSize := config.WriteBufferSize
	if bufferSize <= 0 {
		bufferSize = defaultBufferSize
	}

	asyncWrites := config.WriteBufferSize > 0

	ctx, cancel := context.WithCancel(ctx)

	w := &WAL[T]{
		ctx:          ctx,
		cancel:       cancel,
		dir:          dir,
		log:          log,
		config:       config,
		marshal:      marshal,
		unmarshal:    unmarshal,
		asyncWrites:  asyncWrites,
		closeReqChan: make(chan struct{}),
		closeErrChan: make(chan error, 1),
		writeChan:    make(chan *writeRequest[T], bufferSize),
		truncateChan: make(chan *truncateRequest, bufferSize),
		waitChan:     make(chan chan struct{}, 1),
	}

	go w.mainLoop()
	return w, nil
}

func (walLog *WAL[T]) Write(entry T) error {
	backgroundErr := walLog.asyncError.Load()
	if backgroundErr != nil {
		return fmt.Errorf("WAL encountered an error and is now shut down: %w", *backgroundErr)
	}

	errChan := make(chan error, 1)
	req := &writeRequest[T]{entry: entry, errChan: errChan}

	err := threading.InterruptiblePush(walLog.ctx, walLog.writeChan, req)
	if err != nil {
		return fmt.Errorf("failed to push write request: %w", err)
	}

	if walLog.asyncWrites {
		return nil
	}

	err, pullErr := threading.InterruptiblePull(walLog.ctx, errChan)
	if pullErr != nil {
		return fmt.Errorf("failed to pull write error: %w", pullErr)
	}
	if err != nil {
		return fmt.Errorf("failed to write data: %w", err)
	}

	return nil
}

func (walLog *WAL[T]) reportFatalError(err error, chanErr chan error) {
	if chanErr != nil {
		chanErr <- err
	}
	p := new(error)
	*p = err
	walLog.asyncError.Store(p)
}

func (walLog *WAL[T]) handleWrite(req *writeRequest[T]) {
	bz, err := walLog.marshal(req.entry)
	if err != nil {
		walLog.reportFatalError(fmt.Errorf("marshalling error: %w", err), req.errChan)
		return
	}

	walLog.mu.Lock()
	_, err = walLog.log.WriteBytes(bz)
	walLog.mu.Unlock()
	if err != nil {
		walLog.reportFatalError(fmt.Errorf("failed to write: %w", err), req.errChan)
		return
	}

	if !walLog.asyncWrites && walLog.config.FsyncEnabled {
		walLog.mu.Lock()
		err = walLog.log.Flush()
		walLog.mu.Unlock()
		if err != nil {
			walLog.reportFatalError(fmt.Errorf("failed to flush: %w", err), req.errChan)
			return
		}
	}

	req.errChan <- nil
}

func (walLog *WAL[T]) TruncateAfter(index uint64) error {
	backgroundErr := walLog.asyncError.Load()
	if backgroundErr != nil {
		return fmt.Errorf("WAL encountered an error and is now shut down: %w", *backgroundErr)
	}
	return walLog.sendTruncate(false, index)
}

func (walLog *WAL[T]) TruncateBefore(index uint64) error {
	backgroundErr := walLog.asyncError.Load()
	if backgroundErr != nil {
		return fmt.Errorf("WAL encountered an error and is now shut down: %w", *backgroundErr)
	}
	return walLog.sendTruncate(true, index)
}

func (walLog *WAL[T]) TruncateAll() error {
	backgroundErr := walLog.asyncError.Load()
	if backgroundErr != nil {
		return fmt.Errorf("WAL encountered an error and is now shut down: %w", *backgroundErr)
	}
	last, err := walLog.LastOffset()
	if err != nil {
		return err
	}
	first, err := walLog.FirstOffset()
	if err != nil {
		return err
	}
	if first == 0 && last == 0 {
		return nil
	}
	return walLog.sendTruncate(true, last+1)
}

func (walLog *WAL[T]) sendTruncate(before bool, index uint64) error {
	req := &truncateRequest{
		before:  before,
		index:   index,
		errChan: make(chan error, 1),
	}

	err := threading.InterruptiblePush(walLog.ctx, walLog.truncateChan, req)
	if err != nil {
		return fmt.Errorf("failed to push truncate request: %w", err)
	}

	err, pullErr := threading.InterruptiblePull(walLog.ctx, req.errChan)
	if pullErr != nil {
		return fmt.Errorf("failed to pull truncate error: %w", pullErr)
	}
	if err != nil {
		return fmt.Errorf("failed to truncate: %w", err)
	}
	return nil
}

func (walLog *WAL[T]) handleTruncate(req *truncateRequest) {
	var err error
	if req.before {
		err = walLog.truncateBefore(req.index)
	} else {
		err = walLog.truncateAfter(req.index)
	}
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			req.errChan <- err
			return
		}
		walLog.reportFatalError(err, req.errChan)
		return
	}
	req.errChan <- nil
}

func (walLog *WAL[T]) truncateBefore(index uint64) error {
	if t, ok := any(walLog.log).(truncater); ok {
		return t.TruncateBefore(index)
	}
	return walLog.rewrite(index, 0)
}

func (walLog *WAL[T]) truncateAfter(index uint64) error {
	if t, ok := any(walLog.log).(truncater); ok {
		return t.TruncateAfter(index)
	}
	return walLog.rewrite(0, index)
}

func (walLog *WAL[T]) rewrite(startKeep, endKeep uint64) error {
	walLog.mu.Lock()
	defer walLog.mu.Unlock()

	first, _ := walLog.firstOffsetLocked()
	last, _ := walLog.lastOffsetLocked()
	if first == 0 || last == 0 || first > last {
		return nil
	}

	var entries [][]byte
	var keepStart, keepEnd uint64
	switch {
	case startKeep > 0:
		if startKeep > last+1 {
			startKeep = last + 1
		}
		keepStart, keepEnd = startKeep, last
	case endKeep >= first:
		keepStart, keepEnd = first, endKeep
	default:
		keepStart, keepEnd = 0, 0
	}

	if keepStart > 0 && keepStart <= keepEnd {
		it := walLog.log.NewLogIterator()
		defer it.Release()
		it.SkipToFirst()
		for it.HasNext() {
			elem := it.Next()
			if elem.Index() < keepStart {
				continue
			}
			if elem.Index() > keepEnd {
				break
			}
			data, err := elem.Get()
			if err != nil {
				return err
			}
			copyBz := make([]byte, len(data))
			copy(copyBz, data)
			entries = append(entries, copyBz)
		}
	}

	walLog.log.Close()
	if err := os.RemoveAll(walLog.dir); err != nil {
		return err
	}
	log, err := open(walLog.dir, walLog.config)
	if err != nil {
		return err
	}
	walLog.log = log

	for _, bz := range entries {
		if _, err := walLog.log.WriteBytes(bz); err != nil {
			return err
		}
	}
	if walLog.config.FsyncEnabled {
		return walLog.log.Flush()
	}
	return nil
}

func (walLog *WAL[T]) FirstOffset() (uint64, error) {
	backgroundErr := walLog.asyncError.Load()
	if backgroundErr != nil {
		return 0, fmt.Errorf("WAL encountered an error and is now shut down: %w", *backgroundErr)
	}
	walLog.mu.RLock()
	defer walLog.mu.RUnlock()
	return walLog.firstOffsetLocked()
}

func (walLog *WAL[T]) firstOffsetLocked() (uint64, error) {
	it := walLog.log.NewLogIterator()
	defer it.Release()
	it.SkipToFirst()
	if !it.HasNext() {
		return 0, nil
	}
	return it.Next().Index(), nil
}

func (walLog *WAL[T]) LastOffset() (uint64, error) {
	backgroundErr := walLog.asyncError.Load()
	if backgroundErr != nil {
		return 0, fmt.Errorf("WAL encountered an error and is now shut down: %w", *backgroundErr)
	}
	walLog.mu.RLock()
	defer walLog.mu.RUnlock()
	return walLog.lastOffsetLocked()
}

func (walLog *WAL[T]) lastOffsetLocked() (uint64, error) {
	it := walLog.log.NewLogIterator()
	defer it.Release()
	it.SkipToLast()
	if !it.HasPre() {
		return 0, nil
	}
	return it.Previous().Index(), nil
}

func (walLog *WAL[T]) ReadAt(index uint64) (T, error) {
	var zero T
	backgroundErr := walLog.asyncError.Load()
	if backgroundErr != nil {
		return zero, fmt.Errorf("WAL encountered an error and is now shut down: %w", *backgroundErr)
	}

	walLog.mu.RLock()
	defer walLog.mu.RUnlock()
	it := walLog.log.NewLogIterator()
	defer it.Release()
	it.SkipToFirst()
	for it.HasNext() {
		elem := it.Next()
		if elem.Index() == index {
			bz, err := elem.Get()
			if err != nil {
				return zero, fmt.Errorf("read log failed, %w", err)
			}
			entry, err := walLog.unmarshal(bz)
			if err != nil {
				return zero, fmt.Errorf("unmarshal log failed, %w", err)
			}
			return entry, nil
		}
		if elem.Index() > index {
			break
		}
	}
	return zero, fmt.Errorf("read log failed, offset %d not found", index)
}

func (walLog *WAL[T]) Replay(start uint64, end uint64, processFn func(index uint64, entry T) error) error {
	backgroundErr := walLog.asyncError.Load()
	if backgroundErr != nil {
		return fmt.Errorf("WAL encountered an error and is now shut down: %w", *backgroundErr)
	}

	walLog.mu.RLock()
	defer walLog.mu.RUnlock()
	it := walLog.log.NewLogIterator()
	defer it.Release()
	it.SkipToFirst()
	for it.HasNext() {
		elem := it.Next()
		idx := elem.Index()
		if idx < start {
			continue
		}
		if idx > end {
			break
		}
		bz, err := elem.Get()
		if err != nil {
			return fmt.Errorf("read log failed, %w", err)
		}
		entry, err := walLog.unmarshal(bz)
		if err != nil {
			return fmt.Errorf("unmarshal log failed, %w", err)
		}
		if err := processFn(idx, entry); err != nil {
			return fmt.Errorf("process log failed, %w", err)
		}
	}
	return nil
}

func (walLog *WAL[T]) prune() {
	keepRecent := walLog.config.KeepRecent
	if keepRecent <= 0 || walLog.config.PruneInterval <= 0 {
		return
	}

	lastIndex, err := walLog.LastOffset()
	if err != nil {
		walLog.reportFatalError(fmt.Errorf("failed to get last index for pruning: %w", err), nil)
		return
	}
	firstIndex, err := walLog.FirstOffset()
	if err != nil {
		walLog.reportFatalError(fmt.Errorf("failed to get first index for pruning: %w", err), nil)
		return
	}

	if lastIndex > keepRecent && (lastIndex-keepRecent) > firstIndex {
		prunePos := lastIndex - keepRecent
		if err := walLog.TruncateBefore(prunePos); err != nil {
			walLog.reportFatalError(fmt.Errorf("failed to prune changelog till index %d: %w", prunePos, err), nil)
		}
	}
}

func (walLog *WAL[T]) drain() {
	for walLog.asyncError.Load() == nil {
		select {
		case req := <-walLog.writeChan:
			walLog.handleWrite(req)
		case req := <-walLog.truncateChan:
			walLog.handleTruncate(req)
		case done := <-walLog.waitChan:
			close(done)
		default:
			return
		}
	}
}

func (walLog *WAL[T]) WaitForPendingWrites() {
	if !walLog.asyncWrites {
		return
	}
	done := make(chan struct{})
	_ = threading.InterruptiblePush(walLog.ctx, walLog.waitChan, done)
	<-done
}

func (walLog *WAL[T]) Close() error {
	_ = threading.InterruptiblePush(walLog.ctx, walLog.closeReqChan, struct{}{})
	err := <-walLog.closeErrChan
	walLog.closeErrChan <- err
	if err != nil {
		return fmt.Errorf("error encountered while shutting down: %w", err)
	}
	return nil
}

func open(dir string, opts Config) (*lws.Lws, error) {
	if err := os.MkdirAll(filepath.Clean(dir), 0o755); err != nil {
		return nil, err
	}

	writeFlag := lws.WF_TIMEDFLUSH
	flushQuota := 1000
	switch {
	case opts.FsyncEnabled:
		writeFlag = lws.WF_SYNCFLUSH
		flushQuota = 0
	case opts.WriteBufferSize <= 0:
		writeFlag = lws.WF_SYNCWRITE
		flushQuota = 0
	}

	// lws.BufferSize is the underlying file/mmap buffer size in bytes, while our
	// Config.WriteBufferSize is the async queue length. They are not equivalent.
	// Passing small queue sizes like 100 into lws.BufferSize can create tiny mmap
	// windows and trigger repeated remaps or faults under large changelog entries.
	// Let lws pick its own sensible default buffer sizing.
	log, err := lws.Open(
		dir,
		lws.WithFilePrex("segment_"),
		lws.WithFileExtension("wal"),
		lws.WithWriteFlag(writeFlag, flushQuota),
		lws.WithWriteFileType(lws.FT_MMAP),
		lws.WithReadNoCopy(),
	)
	if err != nil {
		return nil, err
	}
	return log, nil
}

func (walLog *WAL[T]) mainLoop() {
	var pruneChan <-chan time.Time
	if walLog.config.PruneInterval > 0 && walLog.config.KeepRecent > 0 {
		pruneTicker := time.NewTicker(walLog.config.PruneInterval)
		defer pruneTicker.Stop()
		pruneChan = pruneTicker.C
	}

	running := true
	for running && walLog.asyncError.Load() == nil {
		select {
		case <-walLog.ctx.Done():
			running = false
		case req := <-walLog.writeChan:
			walLog.handleWrite(req)
		case req := <-walLog.truncateChan:
			walLog.handleTruncate(req)
		case done := <-walLog.waitChan:
			close(done)
		case <-pruneChan:
			walLog.prune()
		case <-walLog.closeReqChan:
			running = false
		}
	}

	walLog.cancel()
	walLog.drain()

	var closeErr error
	if storedErr := walLog.asyncError.Load(); storedErr != nil && *storedErr != nil {
		closeErr = *storedErr
	}
	walLog.mu.Lock()
	walLog.log.Close()
	walLog.mu.Unlock()
	walLog.closeErrChan <- closeErr
}
