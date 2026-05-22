package config

import "cosmossdk.io/store/seidb/sc/memiavl"

type Config struct {
	Home                               string
	StateCommitmentBackend             string
	StateStoreBackend                  string
	StateStoreAsyncWriteBuffer         int
	StateStoreWriteMode                string
	StateStoreReadMode                 string
	StateStoreEVMDBDirectory           string
	KeepRecent                         uint64
	HistoricalProofQueryMaxConcurrency uint32
	MemIAVL                            memiavl.Config
}
