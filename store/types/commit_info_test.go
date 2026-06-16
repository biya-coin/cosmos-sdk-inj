package types

import (
	"crypto/sha256"
	"testing"

	"github.com/stretchr/testify/require"

	sdkmaps "cosmossdk.io/store/internal/maps"
)

func TestCommitInfoHashMatchesLegacyImplementation(t *testing.T) {
	ci := CommitInfo{
		Version: 7,
		StoreInfos: []StoreInfo{
			{Name: "bank", CommitId: CommitID{Hash: []byte("hash-bank")}},
			{Name: "auth", CommitId: CommitID{Hash: []byte("hash-auth")}},
			{Name: "staking", CommitId: CommitID{Hash: []byte("hash-staking")}},
		},
	}

	require.Equal(t, legacyCommitInfoHash(ci), ci.Hash())
}

func TestCommitInfoHashMatchesLegacyImplementationUnsortedInput(t *testing.T) {
	ci := CommitInfo{
		Version: 7,
		StoreInfos: []StoreInfo{
			{Name: "staking", CommitId: CommitID{Hash: []byte("hash-staking")}},
			{Name: "bank", CommitId: CommitID{Hash: []byte("hash-bank")}},
			{Name: "auth", CommitId: CommitID{Hash: []byte("hash-auth")}},
		},
	}

	require.Equal(t, legacyCommitInfoHash(ci), ci.Hash())
}

func TestCommitInfoHashEmptyMatchesLegacyImplementation(t *testing.T) {
	ci := CommitInfo{}
	require.Equal(t, legacyCommitInfoHash(ci), ci.Hash())
}

func legacyCommitInfoHash(ci CommitInfo) []byte {
	if len(ci.StoreInfos) == 0 {
		emptyHash := sha256.Sum256(nil)
		return emptyHash[:]
	}

	rootHash, _, _ := sdkmaps.ProofsFromMap(ci.toMap())
	if len(rootHash) == 0 {
		emptyHash := sha256.Sum256(nil)
		return emptyHash[:]
	}
	return rootHash
}
