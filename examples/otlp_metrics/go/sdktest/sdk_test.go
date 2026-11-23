// Integration tests of the C receiver with the official OpenTelemetry Go SDK and OTLP/gRPC
// metric exporter, and the official Go protobuf bindings.
//
//	OTLP_SERVER=build/otlp_metrics_server go test ./sdktest
//
// The exporter is driven through a ManualReader, so each Export is one explicit, checked call.
package sdktest

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	collectorpb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// ---- the server under test --------------------------------------------------------------------

type server struct {
	cmd  *exec.Cmd
	port int
	conn *grpc.ClientConn
}

func startServer(t *testing.T, args ...string) *server {
	t.Helper()
	bin := os.Getenv("OTLP_SERVER")
	if bin == "" {
		t.Skip("OTLP_SERVER is not set")
	}
	all := append([]string{"--host", "127.0.0.1", "--port", "0", "--admin"}, args...)
	cmd := exec.Command(bin, all...)
	cmd.Stderr = os.Stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	// the READY line, read with a bound (no sleeping, no port probing)
	line := make(chan string, 1)
	go func() {
		s, _ := bufio.NewReader(stdout).ReadString('\n')
		line <- s
	}()
	var ready string
	select {
	case ready = <-line:
	case <-time.After(30 * time.Second):
		cmd.Process.Kill()
		t.Fatal("server did not report READY")
	}
	s := &server{cmd: cmd}
	if _, err := fmt.Sscanf(strings.TrimSpace(ready), "READY port=%d", &s.port); err != nil {
		cmd.Process.Kill()
		t.Fatalf("unexpected first line %q", ready)
	}
	s.conn, err = grpc.NewClient(s.target(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.stop(t) })
	return s
}

func (s *server) target() string { return fmt.Sprintf("127.0.0.1:%d", s.port) }

func (s *server) stop(t *testing.T) {
	s.conn.Close()
	s.cmd.Process.Signal(syscall.SIGTERM)
	if err := s.cmd.Wait(); err != nil {
		t.Errorf("server exit: %v", err)
	}
}

func (s *server) admin(t *testing.T, method string, body []byte, md ...string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if len(md) > 0 {
		ctx = metadata.AppendToOutgoingContext(ctx, md...)
	}
	var out rawBytes
	in := rawBytes(body)
	if err := s.conn.Invoke(ctx, "/otlp_metrics.Admin/"+method, &in, &out, grpc.ForceCodec(rawCodec{})); err != nil {
		t.Fatalf("Admin/%s: %v", method, err)
	}
	return out
}

// numericStats keeps the numeric counters of a stats document ("status" is a nested object).
func numericStats(t *testing.T, body []byte) map[string]uint64 {
	t.Helper()
	var doc struct {
		Stats map[string]json.RawMessage `json:"stats"`
	}
	if err := json.Unmarshal(body, &doc); err != nil {
		t.Fatal(err)
	}
	out := map[string]uint64{}
	for k, v := range doc.Stats {
		var n uint64
		if json.Unmarshal(v, &n) == nil {
			out[k] = n
		}
	}
	return out
}

func (s *server) stats(t *testing.T) map[string]uint64 {
	return numericStats(t, s.admin(t, "Stats", nil))
}

func (s *server) dump(t *testing.T) *collectorpb.ExportMetricsServiceRequest {
	req := &collectorpb.ExportMetricsServiceRequest{}
	if err := proto.Unmarshal(s.admin(t, "Dump", nil), req); err != nil {
		t.Fatal(err)
	}
	return req
}

type rawBytes []byte

type rawCodec struct{}

func (rawCodec) Name() string                  { return "raw" }
func (rawCodec) Marshal(v any) ([]byte, error) { return *v.(*rawBytes), nil }
func (rawCodec) Unmarshal(data []byte, v any) error {
	*v.(*rawBytes) = append([]byte(nil), data...)
	return nil
}

// ---- SDK export --------------------------------------------------------------------------------

// record fills every instrument kind of the Go SDK once and returns the collected data.
func record(t *testing.T, service string, temporality metricdata.Temporality) metricdata.ResourceMetrics {
	t.Helper()
	ctx := context.Background()
	reader := sdkmetric.NewManualReader(sdkmetric.WithTemporalitySelector(
		func(sdkmetric.InstrumentKind) metricdata.Temporality { return temporality }))
	res := resource.NewSchemaless(attribute.String("service.name", service))
	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
		sdkmetric.WithResource(res),
		sdkmetric.WithView(sdkmetric.NewView(
			sdkmetric.Instrument{Name: "go.exp_histogram"},
			sdkmetric.Stream{Aggregation: sdkmetric.AggregationBase2ExponentialHistogram{MaxSize: 160, MaxScale: 20}},
		)),
	)
	defer provider.Shutdown(ctx)
	meter := provider.Meter("otlp.go.integration", metric.WithInstrumentationVersion("4.5.6"))

	counter, _ := meter.Int64Counter("go.counter")
	counter.Add(ctx, 5, metric.WithAttributes(attribute.String("route", "/a")))
	counter.Add(ctx, 7, metric.WithAttributes(attribute.String("route", "/b")))
	updown, _ := meter.Float64UpDownCounter("go.updown")
	updown.Add(ctx, -2.5)
	hist, _ := meter.Float64Histogram("go.histogram", metric.WithExplicitBucketBoundaries(1, 10, 100))
	for _, v := range []float64{0.5, 5, 50, 500} {
		hist.Record(ctx, v)
	}
	exp, _ := meter.Float64Histogram("go.exp_histogram")
	for _, v := range []float64{0.001, 1, 1000} {
		exp.Record(ctx, v)
	}
	gauge, _ := meter.Int64Gauge("go.gauge")
	gauge.Record(ctx, 99, metric.WithAttributes(attribute.Bool("hot", true)))
	_, _ = meter.Float64ObservableGauge("go.observable_gauge",
		metric.WithFloat64Callback(func(_ context.Context, o metric.Float64Observer) error {
			o.Observe(0.75)
			return nil
		}))
	_, _ = meter.Int64ObservableCounter("go.observable_counter",
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(1234)
			return nil
		}))

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatal(err)
	}
	return rm
}

var expectedKinds = map[string]string{
	"go.counter":            "sum",
	"go.updown":             "sum",
	"go.histogram":          "histogram",
	"go.exp_histogram":      "exponential_histogram",
	"go.gauge":              "gauge",
	"go.observable_gauge":   "gauge",
	"go.observable_counter": "sum",
}

func kinds(req *collectorpb.ExportMetricsServiceRequest) map[string]string {
	out := map[string]string{}
	for _, rm := range req.ResourceMetrics {
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				switch m.Data.(type) {
				case *metricspb.Metric_Gauge:
					out[m.Name] = "gauge"
				case *metricspb.Metric_Sum:
					out[m.Name] = "sum"
				case *metricspb.Metric_Histogram:
					out[m.Name] = "histogram"
				case *metricspb.Metric_ExponentialHistogram:
					out[m.Name] = "exponential_histogram"
				case *metricspb.Metric_Summary:
					out[m.Name] = "summary"
				}
			}
		}
	}
	return out
}

func findMetric(req *collectorpb.ExportMetricsServiceRequest, name string) *metricspb.Metric {
	for _, rm := range req.ResourceMetrics {
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				if m.Name == name {
					return m
				}
			}
		}
	}
	return nil
}

func newExporter(t *testing.T, s *server, opts ...otlpmetricgrpc.Option) *otlpmetricgrpc.Exporter {
	t.Helper()
	all := append([]otlpmetricgrpc.Option{
		otlpmetricgrpc.WithEndpoint(s.target()),
		otlpmetricgrpc.WithInsecure(),
		otlpmetricgrpc.WithTimeout(20 * time.Second),
	}, opts...)
	exp, err := otlpmetricgrpc.New(context.Background(), all...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { exp.Shutdown(context.Background()) })
	return exp
}

func TestSDKExport(t *testing.T) {
	for _, tc := range []struct {
		name        string
		compression string
		temporality metricdata.Temporality
	}{
		{"identity_cumulative", "", metricdata.CumulativeTemporality},
		{"gzip_cumulative", "gzip", metricdata.CumulativeTemporality},
		{"identity_delta", "", metricdata.DeltaTemporality},
		{"gzip_delta", "gzip", metricdata.DeltaTemporality},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := startServer(t, "--sink", "memory")
			var opts []otlpmetricgrpc.Option
			if tc.compression != "" {
				opts = append(opts, otlpmetricgrpc.WithCompressor(tc.compression))
			}
			exp := newExporter(t, s, opts...)
			rm := record(t, "go-sdk", tc.temporality)
			if err := exp.Export(context.Background(), &rm); err != nil {
				t.Fatalf("export: %v", err)
			}
			stored := s.dump(t)
			got := kinds(stored)
			for name, kind := range expectedKinds {
				if got[name] != kind {
					t.Errorf("%s: stored as %q, want %q (all: %v)", name, got[name], kind, got)
				}
			}
			counter := findMetric(stored, "go.counter").GetSum()
			if !counter.IsMonotonic || len(counter.DataPoints) != 2 {
				t.Errorf("counter: %v", counter)
			}
			wantTemporality := metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE
			if tc.temporality == metricdata.DeltaTemporality {
				wantTemporality = metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA
			}
			if counter.AggregationTemporality != wantTemporality {
				t.Errorf("temporality %v, want %v", counter.AggregationTemporality, wantTemporality)
			}
			h := findMetric(stored, "go.histogram").GetHistogram().DataPoints[0]
			if h.Count != 4 || h.Sum == nil || *h.Sum != 555.5 || len(h.BucketCounts) != 4 {
				t.Errorf("histogram point: %v", h)
			}
			e := findMetric(stored, "go.exp_histogram").GetExponentialHistogram().DataPoints[0]
			if e.Count != 3 || e.Min == nil || *e.Min != 0.001 {
				t.Errorf("exponential histogram point: %v", e)
			}
			if rm := stored.ResourceMetrics[0]; rm.ScopeMetrics[0].Scope.Version != "4.5.6" {
				t.Errorf("scope: %v", rm.ScopeMetrics[0].Scope)
			}
			st := s.stats(t)
			if st["requests_ok"] != 1 || st["points_accepted"] != 8 || st["points_rejected"] != 0 {
				t.Errorf("stats: %v", st)
			}
			wantGzip := uint64(0)
			if tc.compression == "gzip" {
				wantGzip = 1
			}
			if st["encoding_gzip"] != wantGzip {
				t.Errorf("encoding_gzip = %d, want %d", st["encoding_gzip"], wantGzip)
			}
		})
	}
}

func TestSDKRetriesAfterRetryInfo(t *testing.T) {
	// every second batch is refused as retryable with RetryInfo{1.5 s} in the status details; the
	// Go exporter waits for the greater of RetryInfo and its own backoff (10 ms here)
	s := startServer(t, "--sink", "memory", "--fault", "retryable-every=2", "--retry-delay-ms", "1500")
	exp := newExporter(t, s, otlpmetricgrpc.WithRetry(otlpmetricgrpc.RetryConfig{
		Enabled: true, InitialInterval: 10 * time.Millisecond, MaxInterval: 10 * time.Millisecond,
		MaxElapsedTime: 20 * time.Second,
	}))
	for i := 0; i < 2; i++ {
		rm := record(t, fmt.Sprintf("retry-%d", i), metricdata.CumulativeTemporality)
		start := time.Now()
		if err := exp.Export(context.Background(), &rm); err != nil {
			t.Fatalf("export %d: %v", i, err)
		}
		if i == 1 && time.Since(start) < 1400*time.Millisecond {
			t.Errorf("retried after %v: RetryInfo was not honored", time.Since(start))
		}
	}
	st := s.stats(t)
	if st["sink_retryable"] != 1 || st["points_accepted"] != 16 || st["requests_ok"] != 2 {
		t.Errorf("stats: %v", st)
	}
}

func TestSDKAuthentication(t *testing.T) {
	s := startServer(t, "--sink", "memory", "--auth-token", "go-token")
	rm := record(t, "auth", metricdata.CumulativeTemporality)
	err := newExporter(t, s).Export(context.Background(), &rm)
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("without token: %v", err)
	}
	exp := newExporter(t, s, otlpmetricgrpc.WithHeaders(map[string]string{"authorization": "Bearer go-token"}))
	if err := exp.Export(context.Background(), &rm); err != nil {
		t.Fatalf("with token: %v", err)
	}
	st := numericStats(t, s.admin(t, "Stats", nil, "authorization", "Bearer go-token"))
	if st["points_accepted"] != 8 || st["requests"] != 1 {
		t.Errorf("stats: %v", st)
	}
}

// ---- the official stub ----------------------------------------------------------------------------

// summaryRequest has what the Go SDK cannot produce: a summary, and histogram optional fields
// left unset next to set ones.
func summaryRequest() *collectorpb.ExportMetricsServiceRequest {
	sum := 12.5
	return &collectorpb.ExportMetricsServiceRequest{ResourceMetrics: []*metricspb.ResourceMetrics{{
		Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{{
			Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "stub"}}}}},
		ScopeMetrics: []*metricspb.ScopeMetrics{{
			Scope: &commonpb.InstrumentationScope{Name: "stub"},
			Metrics: []*metricspb.Metric{
				{Name: "stub.summary", Data: &metricspb.Metric_Summary{Summary: &metricspb.Summary{
					DataPoints: []*metricspb.SummaryDataPoint{{
						TimeUnixNano: 1, Count: 3, Sum: 6,
						QuantileValues: []*metricspb.SummaryDataPoint_ValueAtQuantile{{Quantile: 0.5, Value: 2}, {Quantile: 1, Value: 3}},
					}}}}},
				{Name: "stub.histogram", Data: &metricspb.Metric_Histogram{Histogram: &metricspb.Histogram{
					AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA,
					DataPoints: []*metricspb.HistogramDataPoint{{
						TimeUnixNano: 1, Count: 2, Sum: &sum, BucketCounts: []uint64{1, 1}, ExplicitBounds: []float64{5},
						Exemplars: []*metricspb.Exemplar{{TimeUnixNano: 1, Value: &metricspb.Exemplar_AsInt{AsInt: 4},
							TraceId: make([]byte, 16), SpanId: make([]byte, 8)}},
					}}}}},
			},
		}},
	}}}
}

func TestStubSummaryRoundTrip(t *testing.T) {
	s := startServer(t, "--sink", "memory")
	client := collectorpb.NewMetricsServiceClient(s.conn)
	req := summaryRequest()
	resp, err := client.Export(context.Background(), req, grpc.UseCompressor(gzip.Name))
	if err != nil {
		t.Fatal(err)
	}
	if resp.PartialSuccess != nil {
		t.Errorf("partial success on valid data: %v", resp.PartialSuccess)
	}
	if stored := s.dump(t); !proto.Equal(stored, req) {
		t.Errorf("stored data differs:\n got %v\nwant %v", stored, req)
	}
	h := s.dump(t).ResourceMetrics[0].ScopeMetrics[0].Metrics[1].GetHistogram().DataPoints[0]
	if h.Min != nil || h.Max != nil || h.Sum == nil {
		t.Errorf("optional presence not preserved: %v", h)
	}
}

func TestStubPartialSuccess(t *testing.T) {
	s := startServer(t, "--sink", "memory")
	client := collectorpb.NewMetricsServiceClient(s.conn)
	req := summaryRequest()
	sm := req.ResourceMetrics[0].ScopeMetrics[0]
	sm.Metrics[0].GetSummary().DataPoints = append(sm.Metrics[0].GetSummary().DataPoints,
		&metricspb.SummaryDataPoint{TimeUnixNano: 1, QuantileValues: []*metricspb.SummaryDataPoint_ValueAtQuantile{{Quantile: 2}}},
		&metricspb.SummaryDataPoint{TimeUnixNano: 0})
	resp, err := client.Export(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.PartialSuccess == nil || resp.PartialSuccess.RejectedDataPoints != 2 || resp.PartialSuccess.ErrorMessage == "" {
		t.Fatalf("partial success: %v", resp.PartialSuccess)
	}
	stored := s.dump(t)
	if n := len(stored.ResourceMetrics[0].ScopeMetrics[0].Metrics[0].GetSummary().DataPoints); n != 1 {
		t.Errorf("stored %d summary points, want the 1 valid one", n)
	}
}

func TestConcurrentStubClients(t *testing.T) {
	s := startServer(t, "--sink", "memory", "--workers", "2")
	const clients, calls = 16, 50
	var wg sync.WaitGroup
	errs := make(chan error, clients)
	for c := 0; c < clients; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := grpc.NewClient(s.target(), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				errs <- err
				return
			}
			defer conn.Close()
			client := collectorpb.NewMetricsServiceClient(conn)
			for i := 0; i < calls; i++ {
				if _, err := client.Export(context.Background(), summaryRequest()); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	st := s.stats(t)
	if st["requests_ok"] != clients*calls || st["points_accepted"] != 2*clients*calls {
		t.Errorf("stats: %v", st)
	}
}

// ---- the grpc-go comparison server ---------------------------------------------------------------

// startGoServer starts the grpc-go comparison receiver (GRPCGO_SERVER) like startServer.
func startGoServer(t *testing.T, args ...string) *server {
	t.Helper()
	bin := os.Getenv("GRPCGO_SERVER")
	if bin == "" {
		t.Skip("GRPCGO_SERVER is not set")
	}
	cmd := exec.Command(bin, append([]string{"-addr", "127.0.0.1:0"}, args...)...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	line, _ := bufio.NewReader(stdout).ReadString('\n')
	s := &server{cmd: cmd}
	if _, err := fmt.Sscanf(strings.TrimSpace(line), "READY port=%d", &s.port); err != nil {
		cmd.Process.Kill()
		t.Fatalf("unexpected first line %q", line)
	}
	s.conn, _ = grpc.NewClient(s.target(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	t.Cleanup(func() { s.stop(t) })
	return s
}

// TestComparisonServerEquivalence sends the same requests to both receivers: statuses, rejected
// counts and accepted points must match, so benchmarks compare equal work.
func TestComparisonServerEquivalence(t *testing.T) {
	c := startServer(t, "--sink", "memory")
	g := startGoServer(t, "-sink", "memory")
	bad := summaryRequest()
	sm := bad.ResourceMetrics[0].ScopeMetrics[0]
	sm.Metrics[0].GetSummary().DataPoints[0].QuantileValues[0].Quantile = -1
	sm.Metrics[1].GetHistogram().AggregationTemporality = metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_UNSPECIFIED
	sm.Metrics = append(sm.Metrics, &metricspb.Metric{Name: "", Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{
		DataPoints: []*metricspb.NumberDataPoint{{TimeUnixNano: 1, Value: &metricspb.NumberDataPoint_AsInt{AsInt: 1}}}}}})
	cases := []*collectorpb.ExportMetricsServiceRequest{summaryRequest(), bad, {}}
	for i, req := range cases {
		for _, compressor := range []string{"", gzip.Name} {
			var opts []grpc.CallOption
			if compressor != "" {
				opts = append(opts, grpc.UseCompressor(compressor))
			}
			rc, errc := collectorpb.NewMetricsServiceClient(c.conn).Export(context.Background(), req, opts...)
			rg, errg := collectorpb.NewMetricsServiceClient(g.conn).Export(context.Background(), req, opts...)
			if status.Code(errc) != status.Code(errg) {
				t.Fatalf("case %d: C %v, Go %v", i, errc, errg)
			}
			if rc.GetPartialSuccess().GetRejectedDataPoints() != rg.GetPartialSuccess().GetRejectedDataPoints() {
				t.Errorf("case %d: rejected C %d, Go %d", i, rc.GetPartialSuccess().GetRejectedDataPoints(),
					rg.GetPartialSuccess().GetRejectedDataPoints())
			}
		}
	}
	sc, sg := c.stats(t), g.stats(t)
	for _, k := range []string{"requests", "requests_ok", "requests_partial", "points_received", "points_accepted", "points_rejected"} {
		if sc[k] != sg[k] {
			t.Errorf("%s: C %d, Go %d", k, sc[k], sg[k])
		}
	}
	// per bad request: the summary point (quantile -1), the histogram point (unspecified
	// temporality) and the unnamed gauge's point; sent twice (identity and gzip)
	if sc["points_rejected"] != 2*3 || sc["points_accepted"] != 2*2 {
		t.Errorf("C stats: %v", sc)
	}
}
