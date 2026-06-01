package metrics

import (
	"time"

	"github.com/hashicorp/go-metrics"
)

// StoreMetrics defines the set of metrics for the store package
type StoreMetrics interface {
	MeasureSince(keys ...string)
	MeasureSinceFrom(start time.Time, keys ...string)
	SetGauge(val float32, keys ...string)
	IncrCounter(val float32, keys ...string)
}

var (
	_ StoreMetrics = Metrics{}
	_ StoreMetrics = NoOpMetrics{}
)

// Metrics defines the metrics wrapper for the store package
type Metrics struct {
	Labels []metrics.Label
}

// NewMetrics returns a new instance of the Metrics with labels set by the node operator
func NewMetrics(labels [][]string) Metrics {
	gatherer := Metrics{}

	if numGlobalLables := len(labels); numGlobalLables > 0 {
		parsedGlobalLabels := make([]metrics.Label, numGlobalLables)
		for i, gl := range labels {
			parsedGlobalLabels[i] = metrics.Label{Name: gl[0], Value: gl[1]}
		}

		gatherer.Labels = parsedGlobalLabels
	}

	return gatherer
}

// MeasureSince provides a wrapper functionality for emitting a time measure
// metric with global labels (if any).
func (m Metrics) MeasureSince(keys ...string) {
	start := time.Now()
	metrics.MeasureSinceWithLabels(keys, start.UTC(), m.Labels)
}

// MeasureSinceFrom emits duration from a provided start timestamp.
func (m Metrics) MeasureSinceFrom(start time.Time, keys ...string) {
	metrics.MeasureSinceWithLabels(keys, start.UTC(), m.Labels)
}

// SetGauge emits a gauge metric with global labels (if any).
func (m Metrics) SetGauge(val float32, keys ...string) {
	metrics.SetGaugeWithLabels(keys, val, m.Labels)
}

// IncrCounter emits a counter metric with global labels (if any).
func (m Metrics) IncrCounter(val float32, keys ...string) {
	metrics.IncrCounterWithLabels(keys, val, m.Labels)
}

// NoOpMetrics is a no-op implementation of the StoreMetrics interface
type NoOpMetrics struct{}

// NewNoOpMetrics returns a new instance of the NoOpMetrics
func NewNoOpMetrics() NoOpMetrics {
	return NoOpMetrics{}
}

// MeasureSince is a no-op implementation of the StoreMetrics interface to avoid time.Now() calls
func (m NoOpMetrics) MeasureSince(keys ...string) {}

// MeasureSinceFrom is a no-op implementation of the StoreMetrics interface.
func (m NoOpMetrics) MeasureSinceFrom(start time.Time, keys ...string) {}

// SetGauge is a no-op implementation of the StoreMetrics interface.
func (m NoOpMetrics) SetGauge(val float32, keys ...string) {}

// IncrCounter is a no-op implementation of the StoreMetrics interface.
func (m NoOpMetrics) IncrCounter(val float32, keys ...string) {}
