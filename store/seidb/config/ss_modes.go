package config

import "fmt"

type StateStoreWriteMode string
type StateStoreReadMode string

const (
	CosmosOnlyWrite StateStoreWriteMode = "cosmos_only"
	DualWrite       StateStoreWriteMode = "dual_write"
	SplitWrite      StateStoreWriteMode = "split_write"

	CosmosOnlyRead StateStoreReadMode = "cosmos_only"
	EVMFirstRead   StateStoreReadMode = "evm_first"
	SplitRead      StateStoreReadMode = "split_read"
)

func ParseWriteMode(v string) (StateStoreWriteMode, error) {
	mode := StateStoreWriteMode(v)
	switch mode {
	case "", CosmosOnlyWrite:
		return CosmosOnlyWrite, nil
	case DualWrite, SplitWrite:
		return mode, nil
	default:
		return "", fmt.Errorf("invalid state store write mode: %s", v)
	}
}

func ParseReadMode(v string) (StateStoreReadMode, error) {
	mode := StateStoreReadMode(v)
	switch mode {
	case "", CosmosOnlyRead:
		return CosmosOnlyRead, nil
	case EVMFirstRead, SplitRead:
		return mode, nil
	default:
		return "", fmt.Errorf("invalid state store read mode: %s", v)
	}
}
