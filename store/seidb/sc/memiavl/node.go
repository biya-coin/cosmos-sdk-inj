package memiavl

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"sync"
)

type resetHash interface {
	hash.Hash
	Reset()
}

var sha256Pool = sync.Pool{
	New: func() any {
		return sha256.New()
	},
}

var fixedMetaBufPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 3*binary.MaxVarintLen64)
		return &buf
	},
}

// Node interface encapsulate the interface of both PersistedNode and MemNode.
type Node interface {
	Height() uint8
	IsLeaf() bool
	Size() int64
	Version() uint32
	Key() []byte
	Value() []byte
	Left() Node
	Right() Node
	Hash() []byte

	// SafeHash returns byte slice that's safe to retain
	SafeHash() []byte

	// PersistedNode clone a new node, MemNode modify in place
	Mutate(version, cowVersion uint32) *MemNode

	// Get query the value for a key, it's put into interface because a specialized implementation is more efficient.
	Get(key []byte) ([]byte, uint32)
	GetByIndex(uint32) ([]byte, []byte)
}

// setRecursive performs a set operation and returns:
//   - the resulting subtree node
//   - whether an existing key was updated (shape unchanged)
//   - whether the subtree content visible to hashing actually changed
//
// Note: writing the same value in a new version is still a semantic change because node.Version()
// participates in hashing. The only no-op case is writing the same value to a node that is already
// at the target version within the current working tree.
func setRecursive(node Node, key, value []byte, version, cowVersion uint32) (Node, bool, bool) {
	if node == nil {
		leafNode := newLeafNode(key, value, version)
		IncrementMemNodeSize(leafNode)
		return leafNode, false, true
	}

	nodeKey := node.Key()
	if node.IsLeaf() {
		switch bytes.Compare(key, nodeKey) {
		case -1:
			branchNode := newBranchNode(1, 2, version, nodeKey, newLeafNode(key, value, version), node)
			IncrementMemNodeSize(branchNode)
			return branchNode, false, true
		case 1:
			branchNode := newBranchNode(1, 2, version, key, node, newLeafNode(key, value, version))
			IncrementMemNodeSize(branchNode)
			return branchNode, false, true
		default:
			if node.Version() == version && bytes.Equal(node.Value(), value) {
				return node, true, false
			}
			newNode := node.Mutate(version, cowVersion)
			if !bytes.Equal(newNode.value, value) {
				newNode.value = value
				newNode.valueHashValid = false
			}
			return newNode, true, true
		}
	} else {
		var (
			newChild         Node
			newNode          *MemNode
			updated, changed bool
		)
		if bytes.Compare(key, nodeKey) == -1 {
			newChild, updated, changed = setRecursive(node.Left(), key, value, version, cowVersion)
			if !changed {
				return node, updated, false
			}
			newNode = node.Mutate(version, cowVersion)
			newNode.left = newChild
		} else {
			newChild, updated, changed = setRecursive(node.Right(), key, value, version, cowVersion)
			if !changed {
				return node, updated, false
			}
			newNode = node.Mutate(version, cowVersion)
			newNode.right = newChild
		}

		if updated {
			return newNode, true, true
		}

		if !updated {
			newNode.updateHeightSize()
			newNode = newNode.reBalance(version, cowVersion)
		}

		return newNode, updated, true
	}
}

// removeRecursive returns:
// - (nil, origNode, nil) -> nothing changed in subtree
// - (value, nil, newKey) -> leaf node is removed
// - (value, new node, newKey) -> subtree changed
func removeRecursive(node Node, key []byte, version, cowVersion uint32) ([]byte, Node, []byte) {
	if node == nil {
		return nil, nil, nil
	}

	if node.IsLeaf() {
		if bytes.Equal(node.Key(), key) {
			return node.Value(), nil, nil
		}
		return nil, node, nil
	}

	if bytes.Compare(key, node.Key()) == -1 {
		value, newLeft, newKey := removeRecursive(node.Left(), key, version, cowVersion)
		if value == nil {
			return nil, node, nil
		}
		if newLeft == nil {
			return value, node.Right(), node.Key()
		}
		newNode := node.Mutate(version, cowVersion)
		newNode.left = newLeft
		newNode.updateHeightSize()
		return value, newNode.reBalance(version, cowVersion), newKey
	}

	value, newRight, newKey := removeRecursive(node.Right(), key, version, cowVersion)
	if value == nil {
		return nil, node, nil
	}
	if newRight == nil {
		return value, node.Left(), nil
	}

	newNode := node.Mutate(version, cowVersion)
	newNode.right = newRight
	if newKey != nil {
		newNode.key = newKey
	}
	newNode.updateHeightSize()
	return value, newNode.reBalance(version, cowVersion), nil
}

// Writes the node's hash to the given `io.Writer`. This function recursively calls
// children to update hashes.
func writeHashBytes(node Node, w io.Writer) error {
	bufp := fixedMetaBufPool.Get().(*[]byte)
	buf := *bufp
	defer fixedMetaBufPool.Put(bufp)

	n := binary.PutVarint(buf, int64(node.Height()))
	n += binary.PutVarint(buf[n:], node.Size())
	n += binary.PutVarint(buf[n:], int64(node.Version()))
	if _, err := w.Write(buf[:n]); err != nil {
		return fmt.Errorf("writing height, %w", err)
	}

	// Key is not written for inner nodes, unlike writeBytes.

	if node.IsLeaf() {
		if err := EncodeBytes(w, node.Key()); err != nil {
			return fmt.Errorf("writing key, %w", err)
		}

		// Indirection needed to provide proofs without values.
		// (e.g. ProofLeafNode.ValueHash)
		valueHash := nodeValueHash(node)

		if err := EncodeBytes(w, valueHash); err != nil {
			return fmt.Errorf("writing value, %w", err)
		}
	} else {
		if err := EncodeBytes(w, node.Left().Hash()); err != nil {
			return fmt.Errorf("writing left hash, %w", err)
		}
		if err := EncodeBytes(w, node.Right().Hash()); err != nil {
			return fmt.Errorf("writing right hash, %w", err)
		}
	}

	return nil
}

// HashNode computes the hash of the node.
func HashNode(node Node) []byte {
	if node == nil {
		return nil
	}
	h := sha256Pool.Get().(resetHash)
	h.Reset()
	defer sha256Pool.Put(h)
	if err := writeHashBytes(node, h); err != nil {
		panic(err)
	}
	return h.Sum(nil)
}

func sumHashNode(dst *[sha256.Size]byte, node Node) {
	h := sha256Pool.Get().(resetHash)
	h.Reset()
	defer sha256Pool.Put(h)

	if err := writeHashBytes(node, h); err != nil {
		panic(err)
	}

	sum := h.Sum((*dst)[:0])
	copy(dst[:], sum)
}

func nodeValueHash(node Node) []byte {
	if memNode, ok := node.(*MemNode); ok {
		return memNode.leafValueHash()
	}
	valueHash := sha256.Sum256(node.Value())
	return valueHash[:]
}

// VerifyHash compare node's cached hash with computed one
func VerifyHash(node Node) bool {
	return bytes.Equal(HashNode(node), node.Hash())
}
