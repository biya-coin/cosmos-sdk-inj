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
	"sync"

	cmtmetrics "github.com/cometbft/cometbft/libs/metrics"
	prommetrics "github.com/cometbft/cometbft/libs/metrics/prometheus"
	stdprometheus "github.com/prometheus/client_golang/prometheus"
)

var (
	globalPerfMetrics     *perfMetrics
	globalPerfMetricsOnce sync.Once
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

	// runMsgs framework sub-step durations.
	// Labels: step = route_check | msg_handler | create_events | tag_msg_index |
	// append_events | collect_response | make_abci_data | to_abci_events | result_build
	RunMsgsSubstepSeconds cmtmetrics.Histogram

	// ── FinalizeBlock 各子步骤（秒） ──────────────────────────────────────
	// 对应 Loki msg=app_finalize_block
	// Labels: step = total | oe_wait | internal_exec | working_hash
	FinalizeBlockStepSeconds cmtmetrics.Histogram

	// ── BaseApp Commit 子步骤（秒） ──────────────────────────────────────
	// 对应 Loki msg=baseapp_commit_timing
	// Labels: step = total | cms_commit
	BaseAppCommitStepSeconds cmtmetrics.Histogram

	// ── CheckTx 单笔耗时（秒） ──────────────────────────────────────────
	// Labels: step = total | run_tx
	CheckTxStepSeconds cmtmetrics.Histogram

	// CheckTx runTx sub-step durations.
	// Labels: step = ctx_init | tx_decode | get_msgs | validate_basic | route_lookup |
	// ante_cache_context | ante_handler | ante_cache_write | ante_events_to_abci |
	// mempool_insert | runmsg_cache_context | get_msgs_v2 | run_msgs | post_handler
	CheckTxRunTxSubstepSeconds cmtmetrics.Histogram

	// ── CheckTx 按 checkState 高度聚合的上一高度区间统计 ────────────────
	// Labels: kind = total_seconds | run_tx_seconds | count | height
	CheckTxHeightWindow cmtmetrics.Gauge
}

// newPrometheusMetrics constructs a perfMetrics backed by real Prometheus
// collectors registered under the given namespace (e.g. "cosmos" or "biyachain").
// It is safe to call multiple times; metrics are registered only once.
func newPrometheusMetrics(namespace string) *perfMetrics {
	globalPerfMetricsOnce.Do(func() {
		globalPerfMetrics = &perfMetrics{
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

			RunMsgsSubstepSeconds: prommetrics.NewHistogramFrom(stdprometheus.HistogramOpts{
				Namespace: namespace,
				Subsystem: metricsSubsystem,
				Name:      "run_msgs_substep_seconds",
				Help:      "Per-block cumulative time for runMsgs framework sub-steps.",
				Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 3, 5},
			}, []string{"step"}),

			FinalizeBlockStepSeconds: prommetrics.NewHistogramFrom(stdprometheus.HistogramOpts{
				Namespace: namespace,
				Subsystem: metricsSubsystem,
				Name:      "finalize_block_step_seconds",
				Help:      "Sub-step durations inside FinalizeBlock (OE path and non-OE path).",
				Buckets:   []float64{0.1, 0.5, 1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 15},
			}, []string{"step"}),

			BaseAppCommitStepSeconds: prommetrics.NewHistogramFrom(stdprometheus.HistogramOpts{
				Namespace: namespace,
				Subsystem: "baseapp_commit",
				Name:      "step_seconds",
				Help:      "BaseApp Commit sub-step durations (total / cms_commit).",
				Buckets:   []float64{0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 1, 1.5, 2},
			}, []string{"step"}),

			CheckTxStepSeconds: prommetrics.NewHistogramFrom(stdprometheus.HistogramOpts{
				Namespace: namespace,
				Subsystem: metricsSubsystem,
				Name:      "check_tx_step_seconds",
				Help:      "Per-transaction CheckTx durations for total ABCI CheckTx and internal runTx.",
				Buckets:   []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2},
			}, []string{"step"}),

			CheckTxRunTxSubstepSeconds: prommetrics.NewHistogramFrom(stdprometheus.HistogramOpts{
				Namespace: namespace,
				Subsystem: metricsSubsystem,
				Name:      "check_tx_run_tx_substep_seconds",
				Help:      "Per-transaction CheckTx runTx sub-step durations.",
				Buckets:   []float64{0.00001, 0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1},
			}, []string{"step"}),

			CheckTxHeightWindow: prommetrics.NewGaugeFrom(stdprometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: metricsSubsystem,
				Name:      "check_tx_height_window",
				Help:      "Last flushed CheckTx aggregation window keyed by checkState height. Height is exported as a value to avoid high-cardinality labels.",
			}, []string{"kind"}),
		}
	})
	return globalPerfMetrics
}
