package memiavl

import (
	"testing"

	"cosmossdk.io/store/seidb/sc/proto"
	iavl "cosmossdk.io/store/seidb/sc/sei-iavl"
	"github.com/stretchr/testify/require"
)

func TestMmapWALCommitRollbackReopenLifecycle(t *testing.T) {
	for _, asyncBuf := range []int{0, 100} {
		t.Run(
			func() string {
				if asyncBuf == 0 {
					return "sync_wal"
				}
				return "async_wal"
			}(),
			func(t *testing.T) {
				dir := t.TempDir()
				cs := NewCommitStore(dir, Config{
					AsyncCommitBuffer: asyncBuf,
				})
				cs.Initialize([]string{"test"})

				_, err := cs.LoadVersion(0, false)
				require.NoError(t, err)

				changeSetAt := func(v string) []*proto.NamedChangeSet {
					return []*proto.NamedChangeSet{
						{
							Name: "test",
							Changeset: iavl.ChangeSet{
								Pairs: []*iavl.KVPair{
									{Key: []byte("k"), Value: []byte(v)},
								},
							},
						},
					}
				}

				require.NoError(t, cs.ApplyChangeSets(changeSetAt("v1")))
				_, err = cs.Commit()
				require.NoError(t, err)

				require.NoError(t, cs.ApplyChangeSets(changeSetAt("v2")))
				_, err = cs.Commit()
				require.NoError(t, err)

				require.NoError(t, cs.Rollback(1))
				require.Equal(t, int64(1), cs.Version())
				require.NoError(t, cs.Close())

				cs2 := NewCommitStore(dir, Config{
					AsyncCommitBuffer: asyncBuf,
				})
				cs2.Initialize([]string{"test"})
				_, err = cs2.LoadVersion(0, false)
				require.NoError(t, err)
				require.Equal(t, int64(1), cs2.Version())
				require.NoError(t, cs2.Close())
			},
		)
	}
}
