package rootmulti

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// RootmultiCommitStepSeconds 记录 rootmulti.Commit 各子步骤耗时（秒）。
// 对应 Loki msg=rootmulti_commit_timing 各字段。
// label "step": total / version_calc / commit_stores / flush_metadata / cleanup_removed / prune
var RootmultiCommitStepSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "biyachain",
	Subsystem: "rootmulti_commit",
	Name:      "step_seconds",
	Help:      "Sub-step durations inside rootmulti.Commit.",
	Buckets:   []float64{0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 1, 1.5, 2},
}, []string{"step"}) // step: total / version_calc / commit_stores / flush_metadata / cleanup_removed / prune
