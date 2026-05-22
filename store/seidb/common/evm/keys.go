package evm

import (
	"bytes"
	"errors"
)

const (
	addressLen = 20
	slotLen    = 32
)

var (
	stateKeyPrefix    = []byte{0x03}
	codeKeyPrefix     = []byte{0x07}
	codeHashKeyPrefix = []byte{0x08}
	codeSizeKeyPrefix = []byte{0x09}
	nonceKeyPrefix    = []byte{0x0a}
)

func StateKeyPrefix() []byte { return stateKeyPrefix }

var ErrMalformedEVMKey = errors.New("seidb: malformed evm key")

type EVMKeyKind uint8

const (
	EVMKeyEmpty EVMKeyKind = iota
	EVMKeyNonce
	EVMKeyCodeHash
	EVMKeyCode
	EVMKeyStorage
	EVMKeyLegacy
)

const EVMKeyUnknown = EVMKeyEmpty

func ParseEVMKey(key []byte) (kind EVMKeyKind, keyBytes []byte) {
	if len(key) == 0 {
		return EVMKeyEmpty, nil
	}

	switch {
	case bytes.HasPrefix(key, nonceKeyPrefix):
		if len(key) != len(nonceKeyPrefix)+addressLen {
			return EVMKeyLegacy, key
		}
		return EVMKeyNonce, key[len(nonceKeyPrefix):]
	case bytes.HasPrefix(key, codeHashKeyPrefix):
		if len(key) != len(codeHashKeyPrefix)+addressLen {
			return EVMKeyLegacy, key
		}
		return EVMKeyCodeHash, key[len(codeHashKeyPrefix):]
	case bytes.HasPrefix(key, codeKeyPrefix):
		if len(key) != len(codeKeyPrefix)+addressLen {
			return EVMKeyLegacy, key
		}
		return EVMKeyCode, key[len(codeKeyPrefix):]
	case bytes.HasPrefix(key, stateKeyPrefix):
		if len(key) != len(stateKeyPrefix)+addressLen+slotLen {
			return EVMKeyLegacy, key
		}
		return EVMKeyStorage, key[len(stateKeyPrefix):]
	case bytes.HasPrefix(key, codeSizeKeyPrefix):
		return EVMKeyLegacy, key
	default:
		return EVMKeyLegacy, key
	}
}

func BuildMemIAVLEVMKey(kind EVMKeyKind, keyBytes []byte) []byte {
	var prefix []byte
	switch kind {
	case EVMKeyStorage:
		prefix = stateKeyPrefix
	case EVMKeyNonce:
		prefix = nonceKeyPrefix
	case EVMKeyCodeHash:
		prefix = codeHashKeyPrefix
	case EVMKeyCode:
		prefix = codeKeyPrefix
	default:
		return nil
	}

	result := make([]byte, 0, len(prefix)+len(keyBytes))
	result = append(result, prefix...)
	result = append(result, keyBytes...)
	return result
}

func InternalKeyLen(kind EVMKeyKind) int {
	switch kind {
	case EVMKeyStorage:
		return addressLen + slotLen
	case EVMKeyNonce, EVMKeyCodeHash, EVMKeyCode:
		return addressLen
	default:
		return 0
	}
}
