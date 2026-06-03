package wal

import (
	"path/filepath"

	iavl "cosmossdk.io/store/seidb/sc/sei-iavl"
)

func LogPath(dir string) string {
	return filepath.Join(dir, "changelog")
}

func GetLastIndex(dir string) (index uint64, err error) {
	w, err := open(dir, Config{})
	if err != nil {
		return 0, err
	}
	defer w.Close()

	it := w.NewLogIterator()
	defer it.Release()
	it.SkipToLast()
	if !it.HasPre() {
		return 0, nil
	}
	return it.Previous().Index(), nil
}

func channelBatchRecv[T any](ch <-chan T) []T {
	item, ok := <-ch
	if !ok {
		return nil
	}

	remaining := len(ch)
	result := make([]T, 0, remaining+1)
	result = append(result, item)
	for i := 0; i < remaining; i++ {
		result = append(result, <-ch)
	}
	return result
}

func MockKVPairs(kvPairs ...string) []*iavl.KVPair {
	result := make([]*iavl.KVPair, len(kvPairs)/2)
	for i := 0; i < len(kvPairs); i += 2 {
		result[i/2] = &iavl.KVPair{
			Key:   []byte(kvPairs[i]),
			Value: []byte(kvPairs[i+1]),
		}
	}
	return result
}
