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
// Unset keys keep memiavl.DefaultConfig() values.
//
// Async commit is controlled solely by AsyncCommitBuffer: >0 enables async WAL
// (channel buffered writes); <=0 forces synchronous commit. Default is 100.
func MemIAVLConfigFromAppOpts(appOpts AppOptions) memiavl.Config {
	cfg := memiavl.DefaultConfig()
	if appOpts == nil {
		return cfg
	}

	if v := appOpts.Get(MemIAVLOptionEnable); v != nil {
		cfg.Enable = cast.ToBool(v)
	}
	if v := appOpts.Get(MemIAVLOptionZeroCopy); v != nil {
		cfg.ZeroCopy = cast.ToBool(v)
	}
	if v := appOpts.Get(MemIAVLOptionCacheSize); v != nil {
		cfg.CacheSize = cast.ToInt(v)
	}
	if v := appOpts.Get(MemIAVLOptionAsyncCommitBuffer); v != nil {
		cfg.AsyncCommitBuffer = cast.ToInt(v)
	}
	if v := appOpts.Get(MemIAVLOptionSnapshotKeepRecent); v != nil {
		cfg.SnapshotKeepRecent = cast.ToUint32(v)
	}
	if v := appOpts.Get(MemIAVLOptionSnapshotInterval); v != nil {
		cfg.SnapshotInterval = cast.ToUint32(v)
	}
	if v := appOpts.Get(MemIAVLOptionSnapshotMinTimeInterval); v != nil {
		cfg.SnapshotMinTimeInterval = cast.ToUint32(v)
	}
	if v := appOpts.Get(MemIAVLOptionSnapshotWriterLimit); v != nil {
		cfg.SnapshotWriterLimit = cast.ToInt(v)
	}
	if v := appOpts.Get(MemIAVLOptionSnapshotPrefetchThreshold); v != nil {
		cfg.SnapshotPrefetchThreshold = cast.ToFloat64(v)
	}
	if v := appOpts.Get(MemIAVLOptionSnapshotWriteRateMBps); v != nil {
		cfg.SnapshotWriteRateMBps = cast.ToInt(v)
	}

	return cfg
}
