package types

import (
	"crypto/sha256"
	"encoding/binary"
	"sort"

	cmtprotocrypto "github.com/cometbft/cometbft/api/cometbft/crypto/v1"
	"github.com/cometbft/cometbft/crypto/tmhash"

	"cosmossdk.io/store/internal/conv"
	"cosmossdk.io/store/internal/tree"
)

var emptyCommitInfoHash = sha256.Sum256(nil)

// GetHash returns the GetHash from the CommitID.
// This is used in CommitInfo.Hash()
//
// When we commit to this in a merkle proof, we create a map of storeInfo.Name -> storeInfo.GetHash()
// and build a merkle proof from that.
// This is then chained with the substore proof, so we prove the root hash from the substore before this
// and need to pass that (unmodified) as the leaf value of the multistore proof.
func (si StoreInfo) GetHash() []byte {
	return si.CommitId.Hash
}

func (ci CommitInfo) toMap() map[string][]byte {
	m := make(map[string][]byte, len(ci.StoreInfos))
	for _, storeInfo := range ci.StoreInfos {
		m[storeInfo.Name] = storeInfo.GetHash()
	}

	return m
}

// Hash returns the simple merkle root hash of the stores sorted by name.
func (ci CommitInfo) Hash() []byte {
	if len(ci.StoreInfos) == 0 {
		return emptyCommitInfoHash[:]
	}

	storeInfos := ci.StoreInfos
	if !sort.SliceIsSorted(storeInfos, func(i, j int) bool {
		return storeInfos[i].Name < storeInfos[j].Name
	}) {
		storeInfos = append([]StoreInfo(nil), storeInfos...)
		sort.SliceStable(storeInfos, func(i, j int) bool {
			return storeInfos[i].Name < storeInfos[j].Name
		})
	}

	kvsBytes := make([][]byte, len(storeInfos))
	for i, storeInfo := range storeInfos {
		kvsBytes[i] = commitInfoPairBytes(storeInfo.Name, storeInfo.GetHash())
	}

	rootHash := tree.HashFromByteSlices(kvsBytes)
	if len(rootHash) == 0 {
		return emptyCommitInfoHash[:]
	}
	return rootHash
}

func (ci CommitInfo) ProofOp(storeName string) cmtprotocrypto.ProofOp {
	ret, err := ProofOpFromMap(ci.toMap(), storeName)
	if err != nil {
		panic(err)
	}
	return ret
}

func (ci CommitInfo) CommitID() CommitID {
	return CommitID{
		Version: ci.Version,
		Hash:    ci.Hash(),
	}
}

func commitInfoPairBytes(name string, value []byte) []byte {
	key := conv.UnsafeStrToBytes(name)
	valueHash := tmhash.Sum(value)

	var lenBuf [binary.MaxVarintLen64]byte
	keyLen := binary.PutUvarint(lenBuf[:], uint64(len(key)))
	valueLen := binary.PutUvarint(lenBuf[keyLen:], uint64(len(valueHash)))

	out := make([]byte, keyLen+len(key)+valueLen+len(valueHash))
	n := copy(out, lenBuf[:keyLen])
	n += copy(out[n:], key)
	n += copy(out[n:], lenBuf[keyLen:keyLen+valueLen])
	copy(out[n:], valueHash)
	return out
}
