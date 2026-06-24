package rootmulti

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// RootmultiCommitStepSeconds records rootmulti.Commit sub-step durations.
	// step: total / version_calc / commit_stores / flush_metadata / cleanup_removed / prune.
	RootmultiCommitStepSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "biyachain",
		Subsystem: "rootmulti_commit",
		Name:      "step_seconds",
		Help:      "Sub-step durations inside rootmulti.Commit.",
		Buckets:   []float64{0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 1, 1.5, 2},
	}, []string{"step"})

	RootmultiCommitStoreSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "biyachain",
		Subsystem: "rootmulti_commit",
		Name:      "store_seconds",
		Help:      "Per-store commit duration inside rootmulti.commitStores.",
		Buckets:   []float64{0.0001, 0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2},
	}, []string{"store", "store_type", "reused_last"})

	RootmultiCommitStoresSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "biyachain",
		Subsystem: "rootmulti_commit",
		Name:      "stores_seconds",
		Help:      "Total duration of rootmulti.commitStores.",
		Buckets:   []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2},
	})

	RootmultiCommitStoresCount = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "biyachain",
		Subsystem: "rootmulti_commit",
		Name:      "stores_count",
		Help:      "Number of stores processed by rootmulti.commitStores in the latest block.",
	})
)
