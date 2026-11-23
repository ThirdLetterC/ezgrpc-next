// Package otlpvalidate applies the OTLP metrics receiver's data point rules to the official Go
// bindings. It mirrors examples/otlp_metrics/src/otlp_validate.c rule for rule, so the grpc-go
// comparison server does the same work as the C receiver and both report the same rejected
// counts for the same input.
package otlpvalidate

import (
	"fmt"
	"math"

	collectorpb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
)

const noRecordedValue = uint32(metricspb.DataPointFlags_DATA_POINT_FLAGS_NO_RECORDED_VALUE_MASK)

// Counts is the outcome of validating one request.
type Counts struct {
	Points   uint64
	Rejected uint64
	Reason   string // why the first rejected point was rejected
}

func temporalityValid(t metricspb.AggregationTemporality) bool {
	return t == metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA ||
		t == metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE
}

func checkMetric(m *metricspb.Metric) string {
	if m.GetName() == "" {
		return "metric name is empty"
	}
	switch d := m.Data.(type) {
	case *metricspb.Metric_Sum:
		if !temporalityValid(d.Sum.GetAggregationTemporality()) {
			return "sum aggregation temporality is unspecified"
		}
	case *metricspb.Metric_Histogram:
		if !temporalityValid(d.Histogram.GetAggregationTemporality()) {
			return "histogram aggregation temporality is unspecified"
		}
	case *metricspb.Metric_ExponentialHistogram:
		if !temporalityValid(d.ExponentialHistogram.GetAggregationTemporality()) {
			return "exponential histogram aggregation temporality is unspecified"
		}
	}
	return ""
}

func checkNumber(p *metricspb.NumberDataPoint) string {
	if p.GetTimeUnixNano() == 0 {
		return "data point time_unix_nano is missing"
	}
	if p.Value == nil && p.GetFlags()&noRecordedValue == 0 {
		return "number data point has no value"
	}
	return ""
}

func addU64(a, b uint64) (uint64, bool) {
	if a > math.MaxUint64-b {
		return 0, false
	}
	return a + b, true
}

func checkHistogram(p *metricspb.HistogramDataPoint) string {
	if p.GetTimeUnixNano() == 0 {
		return "data point time_unix_nano is missing"
	}
	b := p.GetExplicitBounds()
	for i := range b {
		if math.IsNaN(b[i]) || (i > 0 && !(b[i] > b[i-1])) {
			return "histogram explicit_bounds are not strictly increasing"
		}
	}
	c := p.GetBucketCounts()
	if len(c) == 0 {
		return ""
	}
	if len(c) != len(b)+1 {
		return "histogram bucket_counts must have one more entry than explicit_bounds"
	}
	if p.GetFlags()&noRecordedValue != 0 {
		return ""
	}
	var total uint64
	for _, v := range c {
		var ok bool
		if total, ok = addU64(total, v); !ok {
			return "histogram bucket_counts do not add up to count"
		}
	}
	if total != p.GetCount() {
		return "histogram bucket_counts do not add up to count"
	}
	return ""
}

func addBuckets(b *metricspb.ExponentialHistogramDataPoint_Buckets, total uint64) (uint64, bool) {
	for _, v := range b.GetBucketCounts() {
		var ok bool
		if total, ok = addU64(total, v); !ok {
			return 0, false
		}
	}
	return total, true
}

func checkExpHistogram(p *metricspb.ExponentialHistogramDataPoint) string {
	if p.GetTimeUnixNano() == 0 {
		return "data point time_unix_nano is missing"
	}
	if p.GetScale() < -10 || p.GetScale() > 20 {
		return "exponential histogram scale is outside [-10, 20]"
	}
	if z := p.GetZeroThreshold(); math.IsNaN(z) || z < 0 {
		return "exponential histogram zero_threshold is negative"
	}
	if p.GetFlags()&noRecordedValue != 0 {
		return ""
	}
	total, ok := addBuckets(p.GetPositive(), p.GetZeroCount())
	if ok {
		total, ok = addBuckets(p.GetNegative(), total)
	}
	if !ok || total != p.GetCount() {
		return "exponential histogram bucket counts do not add up to count"
	}
	return ""
}

func checkSummary(p *metricspb.SummaryDataPoint) string {
	if p.GetTimeUnixNano() == 0 {
		return "data point time_unix_nano is missing"
	}
	for _, q := range p.GetQuantileValues() {
		if q == nil || math.IsNaN(q.GetQuantile()) || q.GetQuantile() < 0 || q.GetQuantile() > 1 {
			return "summary quantile is outside [0, 1]"
		}
	}
	return ""
}

// pointChecks returns the number of points of m and a check for point i.
func pointChecks(m *metricspb.Metric) (int, func(i int) string) {
	switch d := m.Data.(type) {
	case *metricspb.Metric_Gauge:
		p := d.Gauge.GetDataPoints()
		return len(p), func(i int) string { return checkNumber(p[i]) }
	case *metricspb.Metric_Sum:
		p := d.Sum.GetDataPoints()
		return len(p), func(i int) string { return checkNumber(p[i]) }
	case *metricspb.Metric_Histogram:
		p := d.Histogram.GetDataPoints()
		return len(p), func(i int) string { return checkHistogram(p[i]) }
	case *metricspb.Metric_ExponentialHistogram:
		p := d.ExponentialHistogram.GetDataPoints()
		return len(p), func(i int) string { return checkExpHistogram(p[i]) }
	case *metricspb.Metric_Summary:
		p := d.Summary.GetDataPoints()
		return len(p), func(i int) string { return checkSummary(p[i]) }
	}
	return 0, nil
}

// Validate counts the data points of req and the ones that break a rule.
func Validate(req *collectorpb.ExportMetricsServiceRequest) Counts {
	var c Counts
	note := func(m *metricspb.Metric, reason string) {
		if c.Reason == "" {
			name := m.GetName()
			if len(name) > 64 {
				name = name[:64]
			}
			c.Reason = fmt.Sprintf("%s (metric %q)", reason, name)
		}
	}
	for _, rm := range req.GetResourceMetrics() {
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				n, check := pointChecks(m)
				c.Points += uint64(n)
				if n == 0 {
					continue
				}
				if reason := checkMetric(m); reason != "" {
					c.Rejected += uint64(n)
					note(m, reason)
					continue
				}
				for i := 0; i < n; i++ {
					if reason := check(i); reason != "" {
						c.Rejected++
						note(m, reason)
					}
				}
			}
		}
	}
	return c
}

// Filter removes the rejected points of req in place, with the same envelope rules as the C
// receiver: metrics, scopes and resources emptied by the removal go, empty ones stay.
func Filter(req *collectorpb.ExportMetricsServiceRequest) {
	keepRM := req.ResourceMetrics[:0]
	for _, rm := range req.ResourceMetrics {
		if len(rm.ScopeMetrics) == 0 {
			keepRM = append(keepRM, rm)
			continue
		}
		keepSM := rm.ScopeMetrics[:0]
		for _, sm := range rm.ScopeMetrics {
			if len(sm.Metrics) == 0 {
				keepSM = append(keepSM, sm)
				continue
			}
			keepM := sm.Metrics[:0]
			for _, m := range sm.Metrics {
				if !filterMetric(m) {
					keepM = append(keepM, m)
				}
			}
			sm.Metrics = keepM
			if len(keepM) > 0 {
				keepSM = append(keepSM, sm)
			}
		}
		rm.ScopeMetrics = keepSM
		if len(keepSM) > 0 {
			keepRM = append(keepRM, rm)
		}
	}
	req.ResourceMetrics = keepRM
}

func compact[T any](points []T, dropAll bool, check func(T) string) []T {
	kept := points[:0]
	for _, p := range points {
		if !dropAll && check(p) == "" {
			kept = append(kept, p)
		}
	}
	return kept
}

// filterMetric drops the rejected points of m; true when it had points and has none left.
func filterMetric(m *metricspb.Metric) bool {
	n, _ := pointChecks(m)
	if n == 0 {
		return false
	}
	dropAll := checkMetric(m) != ""
	var left int
	switch d := m.Data.(type) {
	case *metricspb.Metric_Gauge:
		d.Gauge.DataPoints = compact(d.Gauge.DataPoints, dropAll, checkNumber)
		left = len(d.Gauge.DataPoints)
	case *metricspb.Metric_Sum:
		d.Sum.DataPoints = compact(d.Sum.DataPoints, dropAll, checkNumber)
		left = len(d.Sum.DataPoints)
	case *metricspb.Metric_Histogram:
		d.Histogram.DataPoints = compact(d.Histogram.DataPoints, dropAll, checkHistogram)
		left = len(d.Histogram.DataPoints)
	case *metricspb.Metric_ExponentialHistogram:
		d.ExponentialHistogram.DataPoints = compact(d.ExponentialHistogram.DataPoints, dropAll, checkExpHistogram)
		left = len(d.ExponentialHistogram.DataPoints)
	case *metricspb.Metric_Summary:
		d.Summary.DataPoints = compact(d.Summary.DataPoints, dropAll, checkSummary)
		left = len(d.Summary.DataPoints)
	}
	return left == 0
}
