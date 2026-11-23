// grpcgo-server is the grpc-go comparison receiver for the OTLP metrics benchmarks.
//
// It serves /opentelemetry.proto.collector.metrics.v1.MetricsService/Export with the same
// semantics as the C receiver (examples/otlp_metrics): the request is decoded completely with the
// official Go bindings (protobuf-go, nesting limited to the same depth), validated with the same
// rules (package otlpvalidate), answered with a full or partial success, and handed to the same
// kinds of sinks: "null" (count and drop) or "memory" (keep a copy of the accepted data: the
// received bytes when everything was accepted, the re-encoded accepted subset otherwise, in a
// bounded store that evicts the oldest batches). It prints "READY port=<n>" once listening and
// serves /otlp_metrics.Admin/Stats with the counters the benchmark harness reads.
//
//	grpcgo-server [-addr 127.0.0.1:4317] [-sink null|memory] [-memory-bytes N] ...
//
// GOMAXPROCS bounds the CPUs it uses (set it like the C receiver's --loops for comparisons).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding"
	_ "google.golang.org/grpc/encoding/gzip" // register the gzip decompressor
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/ThirdLetterC/ezgrpc-next/examples/otlp_metrics/go/otlpvalidate"
	collectorpb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
)

// exportRequest receives the decoded request and, when the sink stores data, its bytes.
type exportRequest struct {
	raw  []byte
	size int
	msg  collectorpb.ExportMetricsServiceRequest
}

var (
	keepRaw   bool
	unmarshal = proto.UnmarshalOptions{RecursionLimit: 32}
)

// codec is protobuf-go plus the two shapes the handlers use: *exportRequest (decoded, raw bytes
// copied only when the sink needs them) and []byte (admin methods).
type codec struct{}

func (codec) Name() string { return "proto" }

func (codec) Marshal(v any) ([]byte, error) {
	switch m := v.(type) {
	case []byte:
		return m, nil
	case proto.Message:
		return proto.Marshal(m)
	}
	return nil, fmt.Errorf("cannot marshal %T", v)
}

func (codec) Unmarshal(data []byte, v any) error {
	switch m := v.(type) {
	case *exportRequest:
		m.size = len(data)
		if keepRaw {
			m.raw = append([]byte(nil), data...) // grpc-go may reuse data after Unmarshal
		}
		return unmarshal.Unmarshal(data, &m.msg)
	case *[]byte:
		*m = append([]byte(nil), data...)
		return nil
	case proto.Message:
		return unmarshal.Unmarshal(data, m)
	}
	return fmt.Errorf("cannot unmarshal into %T", v)
}

type counters struct {
	Requests        atomic.Uint64 `json:"requests"`
	RequestsOK      atomic.Uint64 `json:"requests_ok"`
	RequestsPartial atomic.Uint64 `json:"requests_partial"`
	RequestsFailed  atomic.Uint64 `json:"requests_failed"`
	BytesReceived   atomic.Uint64 `json:"bytes_received"`
	PointsReceived  atomic.Uint64 `json:"points_received"`
	PointsAccepted  atomic.Uint64 `json:"points_accepted"`
	PointsRejected  atomic.Uint64 `json:"points_rejected"`
	Malformed       atomic.Uint64 `json:"malformed"`
}

func (c *counters) snapshot() map[string]uint64 {
	return map[string]uint64{
		"requests":         c.Requests.Load(),
		"requests_ok":      c.RequestsOK.Load(),
		"requests_partial": c.RequestsPartial.Load(),
		"requests_failed":  c.RequestsFailed.Load(),
		"bytes_received":   c.BytesReceived.Load(),
		"points_received":  c.PointsReceived.Load(),
		"points_accepted":  c.PointsAccepted.Load(),
		"points_rejected":  c.PointsRejected.Load(),
		"malformed":        c.Malformed.Load(),
	}
}

// memorySink keeps batches up to a byte budget, evicting the oldest.
type memorySink struct {
	mu       sync.Mutex
	max      int
	bytes    int
	batches  [][]byte
	evicted  uint64
	accepted uint64
}

func (s *memorySink) store(b []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for s.bytes+len(b) > s.max && len(s.batches) > 0 {
		s.bytes -= len(s.batches[0])
		s.batches[0] = nil
		s.batches = s.batches[1:]
		s.evicted++
	}
	s.batches = append(s.batches, b)
	s.bytes += len(b)
	s.accepted++
}

type server struct {
	stats  counters
	memory *memorySink
}

func (s *server) export(ctx context.Context, req *exportRequest) (*collectorpb.ExportMetricsServiceResponse, error) {
	counts := otlpvalidate.Validate(&req.msg)
	s.stats.PointsReceived.Add(counts.Points)
	resp := &collectorpb.ExportMetricsServiceResponse{}
	if counts.Rejected > 0 {
		resp.PartialSuccess = &collectorpb.ExportMetricsPartialSuccess{
			RejectedDataPoints: int64(counts.Rejected),
			ErrorMessage: fmt.Sprintf("rejected %d of %d data points; first: %s",
				counts.Rejected, counts.Points, counts.Reason),
		}
	}
	if accepted := counts.Points - counts.Rejected; accepted > 0 && s.memory != nil {
		payload := req.raw
		if counts.Rejected > 0 {
			otlpvalidate.Filter(&req.msg)
			var err error
			if payload, err = proto.Marshal(&req.msg); err != nil {
				s.stats.RequestsFailed.Add(1)
				return nil, status.Error(codes.Internal, "cannot encode the accepted data")
			}
		}
		s.memory.store(payload)
	}
	if counts.Rejected > 0 {
		s.stats.RequestsPartial.Add(1)
	} else {
		s.stats.RequestsOK.Add(1)
	}
	s.stats.PointsAccepted.Add(counts.Points - counts.Rejected)
	s.stats.PointsRejected.Add(counts.Rejected)
	return resp, nil
}

func exportHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	s := srv.(*server)
	s.stats.Requests.Add(1)
	req := &exportRequest{}
	if err := dec(req); err != nil {
		s.stats.Malformed.Add(1)
		s.stats.RequestsFailed.Add(1)
		return nil, status.Error(codes.InvalidArgument, "malformed ExportMetricsServiceRequest")
	}
	s.stats.BytesReceived.Add(uint64(req.size))
	return s.export(ctx, req)
}

func statsHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	s := srv.(*server)
	var body []byte
	if err := dec(&body); err != nil {
		return nil, err
	}
	doc := map[string]any{"stats": s.stats.snapshot(), "server": "grpc-go"}
	if s.memory != nil {
		s.memory.mu.Lock()
		doc["sink"] = map[string]any{"name": "memory", "batches": len(s.memory.batches),
			"bytes": s.memory.bytes, "evicted_batches": s.memory.evicted}
		s.memory.mu.Unlock()
	} else {
		doc["sink"] = map[string]any{"name": "null"}
	}
	return json.Marshal(doc)
}

var serviceDesc = grpc.ServiceDesc{
	ServiceName: "opentelemetry.proto.collector.metrics.v1.MetricsService",
	HandlerType: (*any)(nil),
	Methods:     []grpc.MethodDesc{{MethodName: "Export", Handler: exportHandler}},
}

var adminDesc = grpc.ServiceDesc{
	ServiceName: "otlp_metrics.Admin",
	HandlerType: (*any)(nil),
	Methods:     []grpc.MethodDesc{{MethodName: "Stats", Handler: statsHandler}},
}

func main() {
	addr := flag.String("addr", "127.0.0.1:4317", "listen address (port 0 = ephemeral)")
	sink := flag.String("sink", "null", "null or memory")
	memoryBytes := flag.Int("memory-bytes", 64<<20, "memory sink capacity (oldest batches are evicted)")
	maxMsg := flag.Int("max-message-bytes", 4<<20, "request size limit, also after decompression")
	maxStreams := flag.Uint("max-streams", 64, "concurrent calls per connection")
	flag.Parse()

	encoding.RegisterCodec(codec{})
	s := &server{}
	switch *sink {
	case "null":
	case "memory":
		s.memory = &memorySink{max: *memoryBytes}
		keepRaw = true
	default:
		log.Fatalf("unknown sink %q", *sink)
	}
	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	g := grpc.NewServer(
		grpc.ForceServerCodec(codec{}),
		grpc.MaxRecvMsgSize(*maxMsg),
		grpc.MaxConcurrentStreams(uint32(*maxStreams)),
	)
	g.RegisterService(&serviceDesc, s)
	g.RegisterService(&adminDesc, s)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		g.GracefulStop()
	}()
	fmt.Printf("READY port=%d\n", lis.Addr().(*net.TCPAddr).Port)
	fmt.Fprintf(os.Stderr, "grpcgo-server: %s, GOMAXPROCS=%d, sink=%s\n", runtime.Version(), runtime.GOMAXPROCS(0), *sink)
	if err := g.Serve(lis); err != nil {
		log.Fatal(err)
	}
}
