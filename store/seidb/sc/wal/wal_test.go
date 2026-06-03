package wal

import (
	"context"
	"fmt"
	"testing"

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
