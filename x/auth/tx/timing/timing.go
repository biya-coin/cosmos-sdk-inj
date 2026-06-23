package timing

import "sync/atomic"

// Snapshot is a process-local view of protobuf tx decode work.
type Snapshot struct {
	Calls             uint64
	Errors            uint64
	TotalNs           int64
	ADR027Ns          int64
	TxRawUnknownNs    int64
	TxRawUnmarshalNs  int64
	TxBodyUnknownNs   int64
	TxBodyUnmarshalNs int64
	AuthUnknownNs     int64
	AuthUnmarshalNs   int64
	WrapperBuildNs    int64
}

var current decodeTiming

type decodeTiming struct {
	calls             atomic.Uint64
	errors            atomic.Uint64
	totalNs           atomic.Int64
	adr027Ns          atomic.Int64
	txRawUnknownNs    atomic.Int64
	txRawUnmarshalNs  atomic.Int64
	txBodyUnknownNs   atomic.Int64
	txBodyUnmarshalNs atomic.Int64
	authUnknownNs     atomic.Int64
	authUnmarshalNs   atomic.Int64
	wrapperBuildNs    atomic.Int64
}

// Reset clears process-local tx decode counters.
func Reset() {
	current.calls.Store(0)
	current.errors.Store(0)
	current.totalNs.Store(0)
	current.adr027Ns.Store(0)
	current.txRawUnknownNs.Store(0)
	current.txRawUnmarshalNs.Store(0)
	current.txBodyUnknownNs.Store(0)
	current.txBodyUnmarshalNs.Store(0)
	current.authUnknownNs.Store(0)
	current.authUnmarshalNs.Store(0)
	current.wrapperBuildNs.Store(0)
}

// SnapshotAndReset returns current counters and clears them for the next block.
func SnapshotAndReset() Snapshot {
	s := Snapshot{
		Calls:             current.calls.Load(),
		Errors:            current.errors.Load(),
		TotalNs:           current.totalNs.Load(),
		ADR027Ns:          current.adr027Ns.Load(),
		TxRawUnknownNs:    current.txRawUnknownNs.Load(),
		TxRawUnmarshalNs:  current.txRawUnmarshalNs.Load(),
		TxBodyUnknownNs:   current.txBodyUnknownNs.Load(),
		TxBodyUnmarshalNs: current.txBodyUnmarshalNs.Load(),
		AuthUnknownNs:     current.authUnknownNs.Load(),
		AuthUnmarshalNs:   current.authUnmarshalNs.Load(),
		WrapperBuildNs:    current.wrapperBuildNs.Load(),
	}
	Reset()
	return s
}

func AddCall()                       { current.calls.Add(1) }
func AddError()                      { current.errors.Add(1) }
func AddTotal(ns int64)              { current.totalNs.Add(ns) }
func AddADR027(ns int64)             { current.adr027Ns.Add(ns) }
func AddTxRawUnknown(ns int64)       { current.txRawUnknownNs.Add(ns) }
func AddTxRawUnmarshal(ns int64)     { current.txRawUnmarshalNs.Add(ns) }
func AddTxBodyUnknown(ns int64)      { current.txBodyUnknownNs.Add(ns) }
func AddTxBodyUnmarshal(ns int64)    { current.txBodyUnmarshalNs.Add(ns) }
func AddAuthInfoUnknown(ns int64)    { current.authUnknownNs.Add(ns) }
func AddAuthInfoUnmarshal(ns int64)  { current.authUnmarshalNs.Add(ns) }
func AddWrapperBuild(ns int64)       { current.wrapperBuildNs.Add(ns) }
func NsToMs(ns int64) float64        { return float64(ns) / 1e6 }
func SecondsFromNs(ns int64) float64 { return float64(ns) / 1e9 }
