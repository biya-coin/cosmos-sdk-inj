package wal

import (
	"context"
	"fmt"
	"testing"

	"cosmossdk.io/store/seidb/sc/proto"
	"github.com/stretchr/testify/require"
)

func newBytesWAL(t *testing.T, dir string) *WAL[[]byte] {
	t.Helper()
	w, err := NewWAL(
		context.Background(),
		func(entry []byte) ([]byte, error) { return entry, nil },
		func(data []byte) ([]byte, error) { return append([]byte(nil), data...), nil },
		dir,
		Config{FsyncEnabled: true},
	)
	require.NoError(t, err)
	return w
}

func TestWALTruncateBeforePersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	w := newBytesWAL(t, dir)
	for i := 0; i < 5; i++ {
		require.NoError(t, w.Write([]byte(fmt.Sprintf("v%d", i+1))))
	}
	require.NoError(t, w.TruncateBefore(3))
	require.NoError(t, w.Close())

	w = newBytesWAL(t, dir)
	defer w.Close()

	first, err := w.FirstOffset()
	require.NoError(t, err)
	last, err := w.LastOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(3), first)
	require.Equal(t, uint64(5), last)

	entry, err := w.ReadAt(3)
	require.NoError(t, err)
	require.Equal(t, []byte("v3"), entry)

	var values []string
	require.NoError(t, w.Replay(3, 5, func(_ uint64, entry []byte) error {
		values = append(values, string(entry))
		return nil
	}))
	require.Equal(t, []string{"v3", "v4", "v5"}, values)
}

func TestWALTruncateAllPersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	w := newBytesWAL(t, dir)
	for i := 0; i < 5; i++ {
		require.NoError(t, w.Write([]byte(fmt.Sprintf("v%d", i+1))))
	}
	require.NoError(t, w.TruncateAll())
	require.NoError(t, w.Close())

	w = newBytesWAL(t, dir)
	defer w.Close()

	first, err := w.FirstOffset()
	require.NoError(t, err)
	last, err := w.LastOffset()
	require.NoError(t, err)
	require.Zero(t, first)
	require.Zero(t, last)
}

func TestWALMmapAsyncClosePersistsLargeEntryAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWAL(
		context.Background(),
		func(entry []byte) ([]byte, error) { return entry, nil },
		func(data []byte) ([]byte, error) { return append([]byte(nil), data...), nil },
		dir,
		Config{WriteBufferSize: 8},
	)
	require.NoError(t, err)

	payload := make([]byte, 2<<20)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	require.NoError(t, w.Write(payload))
	require.NoError(t, w.Close())

	w, err = NewWAL(
		context.Background(),
		func(entry []byte) ([]byte, error) { return entry, nil },
		func(data []byte) ([]byte, error) { return append([]byte(nil), data...), nil },
		dir,
		Config{WriteBufferSize: 8},
	)
	require.NoError(t, err)
	defer w.Close()

	first, err := w.FirstOffset()
	require.NoError(t, err)
	last, err := w.LastOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(1), first)
	require.Equal(t, uint64(1), last)

	got, err := w.ReadAt(1)
	require.NoError(t, err)
	require.Equal(t, payload, got)
}

func TestChangelogWALMmapSyncRollbackStyleLifecycle(t *testing.T) {
	dir := t.TempDir()

	open := func() ChangelogWAL {
		w, err := NewChangelogWAL(dir, Config{WriteBufferSize: 0})
		require.NoError(t, err)
		return w
	}

	mkEntry := func(v int64) proto.ChangelogEntry {
		return proto.ChangelogEntry{
			Version:  v,
			Upgrades: []*proto.TreeNameUpgrade{{Name: "test"}},
		}
	}

	w := open()
	for v := int64(1); v <= 5; v++ {
		require.NoError(t, w.Write(mkEntry(v)))
	}
	require.NoError(t, w.Close())

	w = open()
	require.NoError(t, w.TruncateAfter(3))
	require.NoError(t, w.Close())

	w = open()
	first, err := w.FirstOffset()
	require.NoError(t, err)
	last, err := w.LastOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(1), first)
	require.Equal(t, uint64(3), last)
	require.NoError(t, w.Write(mkEntry(4)))
	require.NoError(t, w.Close())

	w = open()
	defer w.Close()
	first, err = w.FirstOffset()
	require.NoError(t, err)
	last, err = w.LastOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(1), first)
	require.Equal(t, uint64(4), last)
}
