package rootmulti

import "fmt"

type stateStoreWriteMode string
type stateStoreReadMode string

const (
	cosmosOnlyWrite stateStoreWriteMode = "cosmos_only"
	dualWrite       stateStoreWriteMode = "dual_write"
	splitWrite      stateStoreWriteMode = "split_write"

	cosmosOnlyRead stateStoreReadMode = "cosmos_only"
	evmFirstRead   stateStoreReadMode = "evm_first"
	splitRead      stateStoreReadMode = "split_read"
)

func parseWriteMode(v string) (stateStoreWriteMode, error) {
	mode := stateStoreWriteMode(v)
	switch mode {
	case "", cosmosOnlyWrite:
		return cosmosOnlyWrite, nil
	case dualWrite, splitWrite:
		return mode, nil
	default:
		return "", fmt.Errorf("invalid state store write mode: %s", v)
	}
}

func parseReadMode(v string) (stateStoreReadMode, error) {
	mode := stateStoreReadMode(v)
	switch mode {
	case "", cosmosOnlyRead:
		return cosmosOnlyRead, nil
	case evmFirstRead, splitRead:
		return mode, nil
	default:
		return "", fmt.Errorf("invalid state store read mode: %s", v)
	}
}
