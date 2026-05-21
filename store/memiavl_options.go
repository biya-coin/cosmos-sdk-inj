package store

import (
	"github.com/spf13/cast"

	"cosmossdk.io/store/seidb/sc/memiavl"
)

// AppOptions is the minimal surface needed to read memIAVL settings (viper, flags, etc.).
type AppOptions interface {
	Get(string) interface{}
}

// MemIAVL app.toml / CLI option keys under the [memiavl] section.
const (
	MemIAVLOptionEnable                    = "memiavl.enable"
	MemIAVLOptionZeroCopy                  = "memiavl.zero-copy"
	MemIAVLOptionCacheSize                 = "memiavl.cache-size"
	MemIAVLOptionAsyncCommitBuffer         = "memiavl.async-commit-buffer"
	MemIAVLOptionSnapshotKeepRecent        = "memiavl.snapshot-keep-recent"
	MemIAVLOptionSnapshotInterval          = "memiavl.snapshot-interval"
	MemIAVLOptionSnapshotMinTimeInterval   = "memiavl.snapshot-min-time-interval"
	MemIAVLOptionSnapshotWriterLimit       = "memiavl.snapshot-writer-limit"
	MemIAVLOptionSnapshotPrefetchThreshold = "memiavl.snapshot-prefetch-threshold"
	MemIAVLOptionSnapshotWriteRateMBps     = "memiavl.snapshot-write-rate-mbps"
)

// MemIAVLConfigFromAppOpts reads [memiavl] settings from viper/app options.
// Unset keys keep memiavl.DefaultConfig() values (flag defaults should match).
func MemIAVLConfigFromAppOpts(appOpts AppOptions) memiavl.Config {
	cfg := memiavl.DefaultConfig()
	if appOpts == nil {
		return cfg
	}

	cfg.Enable = cast.ToBool(appOpts.Get(MemIAVLOptionEnable))
	cfg.ZeroCopy = cast.ToBool(appOpts.Get(MemIAVLOptionZeroCopy))
	cfg.CacheSize = cast.ToInt(appOpts.Get(MemIAVLOptionCacheSize))
	cfg.AsyncCommitBuffer = cast.ToInt(appOpts.Get(MemIAVLOptionAsyncCommitBuffer))
	cfg.SnapshotKeepRecent = cast.ToUint32(appOpts.Get(MemIAVLOptionSnapshotKeepRecent))
	cfg.SnapshotInterval = cast.ToUint32(appOpts.Get(MemIAVLOptionSnapshotInterval))
	cfg.SnapshotMinTimeInterval = cast.ToUint32(appOpts.Get(MemIAVLOptionSnapshotMinTimeInterval))
	cfg.SnapshotWriterLimit = cast.ToInt(appOpts.Get(MemIAVLOptionSnapshotWriterLimit))
	cfg.SnapshotPrefetchThreshold = cast.ToFloat64(appOpts.Get(MemIAVLOptionSnapshotPrefetchThreshold))
	cfg.SnapshotWriteRateMBps = cast.ToInt(appOpts.Get(MemIAVLOptionSnapshotWriteRateMBps))

	return cfg
}
