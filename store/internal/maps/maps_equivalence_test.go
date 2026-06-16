package maps

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRootHashFromMapMatchesProofsFromMapRoot(t *testing.T) {
	m := map[string][]byte{
		"bank":    []byte("hash-bank"),
		"auth":    []byte("hash-auth"),
		"staking": []byte("hash-staking"),
	}

	root1, _, _ := ProofsFromMap(m)
	root2 := RootHashFromMap(m)
	require.Equal(t, root1, root2)
}

func TestRootHashFromMapEmptyMatchesProofsFromMapRoot(t *testing.T) {
	m := map[string][]byte{}

	root1, _, _ := ProofsFromMap(m)
	root2 := RootHashFromMap(m)
	require.Equal(t, root1, root2)
}
