package memiavl

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHashNodeMatchesLegacyEncoding(t *testing.T) {
	leaf := newLeafNode([]byte("hello"), []byte("world"), 7)
	require.Equal(t, legacyHashNode(leaf), HashNode(leaf))

	left := newLeafNode([]byte("a"), []byte("1"), 7)
	right := newLeafNode([]byte("b"), []byte("2"), 7)
	branch := newBranchNode(1, 2, 7, []byte("b"), left, right)
	require.Equal(t, legacyHashNode(branch), HashNode(branch))
}

func TestMemNodeHashCacheStable(t *testing.T) {
	leaf := newLeafNode([]byte("hello"), []byte("world"), 7)
	first := leaf.Hash()
	second := leaf.Hash()

	require.Equal(t, first, second)
	require.True(t, leaf.hashValid)
	require.True(t, leaf.valueHashValid)
}

func TestSetRecursiveSameValueSameVersionDoesNotInvalidateHash(t *testing.T) {
	leaf := newLeafNode([]byte("hello"), []byte("world"), 7)
	originalHash := append([]byte(nil), leaf.Hash()...)

	next, updated, changed := setRecursive(leaf, []byte("hello"), []byte("world"), 7, 0)
	require.True(t, updated)
	require.False(t, changed)
	require.Same(t, leaf, next)
	require.Equal(t, originalHash, next.Hash())
}

func BenchmarkHashNodeLeaf(b *testing.B) {
	node := newLeafNode([]byte("hello"), []byte("world"), 7)
	for i := 0; i < b.N; i++ {
		node.hashValid = false
		node.valueHashValid = false
		_ = node.Hash()
	}
}

func BenchmarkHashNodeBranch(b *testing.B) {
	left := newLeafNode([]byte("a"), []byte("1"), 7)
	right := newLeafNode([]byte("b"), []byte("2"), 7)
	node := newBranchNode(1, 2, 7, []byte("b"), left, right)
	for i := 0; i < b.N; i++ {
		left.hashValid = false
		left.valueHashValid = false
		right.hashValid = false
		right.valueHashValid = false
		node.hashValid = false
		_ = node.Hash()
	}
}

func legacyHashNode(node Node) []byte {
	if node == nil {
		return nil
	}
	h := sha256.New()
	if err := legacyWriteHashBytes(node, h); err != nil {
		panic(err)
	}
	return h.Sum(nil)
}

func legacyWriteHashBytes(node Node, w io.Writer) error {
	var (
		n   int
		buf [binary.MaxVarintLen64]byte
	)

	n = binary.PutVarint(buf[:], int64(node.Height()))
	if _, err := w.Write(buf[0:n]); err != nil {
		return err
	}
	n = binary.PutVarint(buf[:], node.Size())
	if _, err := w.Write(buf[0:n]); err != nil {
		return err
	}
	n = binary.PutVarint(buf[:], int64(node.Version()))
	if _, err := w.Write(buf[0:n]); err != nil {
		return err
	}

	if node.IsLeaf() {
		if err := EncodeBytes(w, node.Key()); err != nil {
			return err
		}
		valueHash := sha256.Sum256(node.Value())
		if err := EncodeBytes(w, valueHash[:]); err != nil {
			return err
		}
	} else {
		if err := EncodeBytes(w, node.Left().Hash()); err != nil {
			return err
		}
		if err := EncodeBytes(w, node.Right().Hash()); err != nil {
			return err
		}
	}
	return nil
}

func TestEncodeBytesOutputStable(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, EncodeBytes(&buf, []byte("abc")))
	require.Equal(t, append([]byte{3}, []byte("abc")...), buf.Bytes())
}
