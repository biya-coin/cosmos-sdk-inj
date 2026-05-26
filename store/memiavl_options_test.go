package store

import (
	"testing"

	"cosmossdk.io/store/seidb/sc/memiavl"
	"github.com/stretchr/testify/require"
)

type mapAppOptions map[string]interface{}

func (m mapAppOptions) Get(key string) interface{} {
	return m[key]
}

func TestMemIAVLConfigFromAppOpts_UnsetKeepsAsyncDefault(t *testing.T) {
	cfg := MemIAVLConfigFromAppOpts(mapAppOptions{})
	require.Equal(t, memiavl.DefaultAsyncCommitBuffer, cfg.AsyncCommitBuffer)
}

func TestMemIAVLConfigFromAppOpts_ZeroBufferDisablesAsync(t *testing.T) {
	cfg := MemIAVLConfigFromAppOpts(mapAppOptions{
		MemIAVLOptionAsyncCommitBuffer: 0,
	})
	require.Equal(t, 0, cfg.AsyncCommitBuffer)
}

func TestMemIAVLConfigFromAppOpts(t *testing.T) {
	opts := mapAppOptions{
		MemIAVLOptionEnable:             true,
		MemIAVLOptionZeroCopy:           true,
		MemIAVLOptionAsyncCommitBuffer:  0,
		MemIAVLOptionSnapshotInterval:   uint32(1000),
		MemIAVLOptionSnapshotKeepRecent: uint32(2),
		MemIAVLOptionCacheSize:          500,
	}

	cfg := MemIAVLConfigFromAppOpts(opts)
	require.True(t, cfg.Enable)
	require.True(t, cfg.ZeroCopy)
	require.Equal(t, 0, cfg.AsyncCommitBuffer)
	require.Equal(t, uint32(1000), cfg.SnapshotInterval)
	require.Equal(t, uint32(2), cfg.SnapshotKeepRecent)
	require.Equal(t, 500, cfg.CacheSize)
}
