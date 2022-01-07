package client

import (
	"context"
	"strings"
	"time"

	"go.temporal.io/sdk/internal/common/metrics"
	"google.golang.org/grpc"
)

// MetricsHandler is a handler for metrics emitted by the SDK. This interface is
// intentionally limited to only what the SDK needs to emit metrics and is not
// built to be a general purpose metrics abstraction for all uses.
//
// A common implementation is at
// go.temporal.io/sdk/contrib/tally.NewMetricsHandler. The MetricsNopHandler is
// a noop handler. A handler may implement "Unwrap() client.MetricsHandler" if
// it wraps a handler.
type MetricsHandler interface {
	// WithTags returns a new handler with the given tags set for each metric
	// created from it.
	WithTags(map[string]string) MetricsHandler

	// Counter obtains a counter for the given name.
	Counter(name string) MetricsCounter

	// Gauge obtains a gauge for the given name.
	Gauge(name string) MetricsGauge

	// Timer obtains a timer for the given name.
	Timer(name string) MetricsTimer
}

// MetricsCounter is an ever-increasing counter.
type MetricsCounter interface {
	// Inc increments the counter value.
	Inc(int64)
}

// MetricsGauge can be set to any float.
type MetricsGauge interface {
	// Update updates the gauge value.
	Update(float64)
}

// MetricsTimer records time durations.
type MetricsTimer interface {
	// Record sets the timer value.
	Record(time.Duration)
}

// MetricsNopHandler is a noop handler that does nothing with the metrics.
var MetricsNopHandler MetricsHandler = nopHandler{}

type nopHandler struct{}

func (nopHandler) WithTags(map[string]string) MetricsHandler { return nopHandler{} }
func (nopHandler) Counter(string) MetricsCounter             { return nopHandler{} }
func (nopHandler) Gauge(string) MetricsGauge                 { return nopHandler{} }
func (nopHandler) Timer(string) MetricsTimer                 { return nopHandler{} }
func (nopHandler) Inc(int64)                                 {}
func (nopHandler) Update(float64)                            {}
func (nopHandler) Record(time.Duration)                      {}

type metricsHandlerContextKey struct{}
type metricsLongPollContextKey struct{}

func newMetricsGRPCInterceptor(defaultHandler MetricsHandler, suffix string) grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req interface{},
		reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		handler, _ := ctx.Value(metricsHandlerContextKey{}).(MetricsHandler)
		if handler == nil {
			handler = defaultHandler
		}
		longPoll, ok := ctx.Value(metricsHandlerContextKey{}).(bool)
		if !ok {
			longPoll = false
		}

		// Only take method name after the last slash
		operation := method[strings.LastIndex(method, "/")+1:]
		handler = handler.WithTags(map[string]string{metrics.OperationTagName: operation})

		// Capture time, record start, run, and record end
		start := time.Now()
		recordRequestStart(handler, longPoll, suffix)
		err := invoker(ctx, method, req, reply, cc, opts...)
		recordRequestEnd(handler, longPoll, suffix, start, err)
		return err
	}
}

func recordRequestStart(handler MetricsHandler, longPoll bool, suffix string) {
	// Count request
	metric := metrics.TemporalRequest
	if longPoll {
		metric = metrics.TemporalLongRequest
	}
	metric += suffix
	handler.Counter(metric).Inc(1)
}

func recordRequestEnd(handler MetricsHandler, longPoll bool, suffix string, start time.Time, err error) {
	// Record latency
	timerMetric := metrics.TemporalRequestLatency
	if longPoll {
		timerMetric = metrics.TemporalLongRequestLatency
	}
	timerMetric += suffix
	handler.Timer(timerMetric).Record(time.Since(start))

	// Count failure
	if err != nil {
		failureMetric := metrics.TemporalRequestFailure
		if longPoll {
			failureMetric = metrics.TemporalLongRequestFailure
		}
		failureMetric += suffix
		handler.Counter(failureMetric).Inc(1)
	}
}
