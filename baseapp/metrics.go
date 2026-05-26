package baseapp

// Metrics contains Prometheus metrics for BaseApp performance instrumentation.
// These histograms mirror every field already logged via Loki (fmt.Printf),
// so both observability pipelines stay in sync.
//
// Follow the same pattern as CometBFT internal/consensus/metrics.go:
//   - Metrics is a plain struct with metrics.Histogram interface fields.
//   - PrometheusMetrics() registers and returns real Prometheus collectors.
//   - NopMetrics() returns a no-op implementation (zero overhead when
//     prometheus = false in config.toml).
//
// Usage in BaseApp:
//
//	app.metrics = PrometheusMetrics("cosmos")   // when prometheus enabled
//	app.metrics = NopMetrics()                  // otherwise

import (
	cmtmetrics "github.com/cometbft/cometbft/libs/metrics"
	"github.com/cometbft/cometbft/libs/metrics/discard"
	prommetrics "github.com/cometbft/cometbft/libs/metrics/prometheus"
	stdprometheus "github.com/prometheus/client_golang/prometheus"
)

const metricsSubsystem = "baseapp"

// perfMetrics holds all custom performance histograms for BaseApp.
type perfMetrics struct {
	// ── PrepareProposal 各子步骤（秒） ─────────────────────────────────────
	// 对应 Loki msg=baseapp_prepare_proposal_timing
	// Labels: step = total | build_header | set_state | set_ctx | prepare
	PrepareProposalStepSeconds cmtmetrics.Histogram

	// ── internalFinalizeBlock 各子步骤（秒） ──────────────────────────────
	// 对应 Loki msg=app_internal_finalize_block
	// Labels: step = total | begin_block | execute_txs | end_block
	InternalFinalizeBlockStepSeconds cmtmetrics.Histogram

	// ── executeTxs 各子步骤（秒） ─────────────────────────────────────────
	// 对应 Loki msg=execute_txs_substep
	// Labels: step = ante | msgs | post
	ExecuteTxsStepSeconds cmtmetrics.Histogram

	// ── FinalizeBlock 各子步骤（秒） ──────────────────────────────────────
	// 对应 Loki msg=app_finalize_block
	// Labels: step = total | oe_wait | internal_exec | working_hash
	FinalizeBlockStepSeconds cmtmetrics.Histogram
}

// PrometheusMetrics constructs a perfMetrics backed by real Prometheus
// collectors registered under the given namespace (e.g. "cosmos" or "biyachain").
func newPrometheusMetrics(namespace string) *perfMetrics {
	return &perfMetrics{
		PrepareProposalStepSeconds: prommetrics.NewHistogramFrom(stdprometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: metricsSubsystem,
			Name:      "prepare_proposal_step_seconds",
			Help:      "Sub-step durations inside BaseApp.PrepareProposal.",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 3, 5},
		}, []string{"step"}),

		InternalFinalizeBlockStepSeconds: prommetrics.NewHistogramFrom(stdprometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: metricsSubsystem,
			Name:      "internal_finalize_block_step_seconds",
			Help:      "Sub-step durations inside internalFinalizeBlock.",
			Buckets:   []float64{0.1, 0.25, 0.5, 1, 2, 3, 4, 5, 6, 7, 8},
		}, []string{"step"}),

		ExecuteTxsStepSeconds: prommetrics.NewHistogramFrom(stdprometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: metricsSubsystem,
			Name:      "execute_txs_step_seconds",
			Help:      "Per-block cumulative time for ante / msgs / post handlers across all txs.",
			Buckets:   []float64{0.1, 0.25, 0.5, 1, 2, 3, 4, 5, 6, 7, 8},
		}, []string{"step"}),

		FinalizeBlockStepSeconds: prommetrics.NewHistogramFrom(stdprometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: metricsSubsystem,
			Name:      "finalize_block_step_seconds",
			Help:      "Sub-step durations inside FinalizeBlock (OE path and non-OE path).",
			Buckets:   []float64{0.1, 0.5, 1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 15},
		}, []string{"step"}),
	}
}

// newNopMetrics returns a perfMetrics whose every Observe call is a no-op.
// Used when prometheus is disabled in config.toml.
func newNopMetrics() *perfMetrics {
	return &perfMetrics{
		PrepareProposalStepSeconds:       discard.NewHistogram(),
		InternalFinalizeBlockStepSeconds: discard.NewHistogram(),
		ExecuteTxsStepSeconds:            discard.NewHistogram(),
		FinalizeBlockStepSeconds:         discard.NewHistogram(),
	}
}
