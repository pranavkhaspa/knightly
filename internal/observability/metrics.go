package observability

import (
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// MetricsPrefix namespaces every metric this package registers.
const MetricsPrefix = "nightshift_"

type metricKind int

const (
	kindCounter metricKind = iota
	kindHistogram
)

// DefaultBuckets are the latency buckets (seconds) shared by the latency
// histograms. They are bounded so cardinality and memory stay predictable.
var DefaultBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

type family struct {
	name       string
	help       string
	kind       metricKind
	labelNames []string
	buckets    []float64

	mu     sync.Mutex
	series map[string]*series
}

type series struct {
	values       []string
	value        float64
	count        uint64
	sum          float64
	bucketCounts []uint64
}

// Registry holds the bounded metric families for one process.
type Registry struct {
	mu       sync.Mutex
	families map[string]*family
	order    []string
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{families: make(map[string]*family)}
}

// Counter registers (or returns) a counter family with the given label names.
func (r *Registry) Counter(name, help string, labelNames ...string) *Counter {
	return &Counter{family: r.family(name, help, kindCounter, labelNames, nil)}
}

// Histogram registers (or returns) a histogram family with the given buckets.
func (r *Registry) Histogram(name, help string, buckets []float64, labelNames ...string) *Histogram {
	if len(buckets) == 0 {
		buckets = DefaultBuckets
	}
	return &Histogram{family: r.family(name, help, kindHistogram, labelNames, buckets)}
}

func (r *Registry) family(name, help string, kind metricKind, labelNames []string, buckets []float64) *family {
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.families[name]; ok {
		return existing
	}
	created := &family{
		name:       name,
		help:       help,
		kind:       kind,
		labelNames: append([]string(nil), labelNames...),
		buckets:    append([]float64(nil), buckets...),
		series:     make(map[string]*series),
	}
	r.families[name] = created
	r.order = append(r.order, name)
	return created
}

// Counter is a monotonically increasing metric.
type Counter struct{ family *family }

// Add increments the counter by delta for the labelled series.
func (c *Counter) Add(labels map[string]string, delta float64) {
	if c == nil || c.family == nil || delta == 0 {
		return
	}
	c.family.mu.Lock()
	series := c.family.seriesFor(labels)
	series.value += delta
	c.family.mu.Unlock()
}

// Inc increments the counter by one.
func (c *Counter) Inc(labels map[string]string) { c.Add(labels, 1) }

// Histogram records distributions of a value such as a latency in seconds.
type Histogram struct{ family *family }

// Observe records one value for the labelled series.
func (h *Histogram) Observe(labels map[string]string, value float64) {
	if h == nil || h.family == nil {
		return
	}
	if value < 0 {
		value = 0
	}
	h.family.mu.Lock()
	series := h.family.seriesFor(labels)
	series.count++
	series.sum += value
	for index, bound := range h.family.buckets {
		if value <= bound {
			series.bucketCounts[index]++
		}
	}
	h.family.mu.Unlock()
}

func (f *family) seriesFor(labels map[string]string) *series {
	values := make([]string, len(f.labelNames))
	key := make([]byte, 0, len(f.labelNames)*8)
	for index, name := range f.labelNames {
		values[index] = labels[name]
		key = append(key, values[index]...)
		key = append(key, 0x1f)
	}
	existing, ok := f.series[string(key)]
	if !ok {
		existing = &series{values: values}
		if f.kind == kindHistogram {
			existing.bucketCounts = make([]uint64, len(f.buckets))
		}
		f.series[string(key)] = existing
	}
	return existing
}

// Render writes the registry in the Prometheus text exposition format.
func (r *Registry) Render(w io.Writer) error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	names := append([]string(nil), r.order...)
	r.mu.Unlock()
	for _, name := range names {
		r.mu.Lock()
		f := r.families[name]
		r.mu.Unlock()
		if f == nil {
			continue
		}
		if err := f.render(w); err != nil {
			return err
		}
	}
	return nil
}

func (f *family) render(w io.Writer) error {
	f.mu.Lock()
	seriesList := make([]*series, 0, len(f.series))
	for _, value := range f.series {
		seriesList = append(seriesList, value)
	}
	f.mu.Unlock()
	sort.Slice(seriesList, func(i, j int) bool {
		return strings.Join(seriesList[i].values, "\x1f") < strings.Join(seriesList[j].values, "\x1f")
	})

	if f.help != "" {
		if _, err := fmt.Fprintf(w, "# HELP %s %s\n", f.name, sanitizeHelp(f.help)); err != nil {
			return err
		}
	}
	typeName := "counter"
	if f.kind == kindHistogram {
		typeName = "histogram"
	}
	if _, err := fmt.Fprintf(w, "# TYPE %s %s\n", f.name, typeName); err != nil {
		return err
	}
	for _, value := range seriesList {
		if f.kind == kindHistogram {
			var cumulative uint64
			for index, bound := range f.buckets {
				cumulative += value.bucketCounts[index]
				labels := f.labelPairs(value.values, "le", formatFloat(bound))
				if _, err := fmt.Fprintf(w, "%s_bucket{%s} %d\n", f.name, labels, cumulative); err != nil {
					return err
				}
			}
			labels := f.labelPairs(value.values, "le", "+Inf")
			if _, err := fmt.Fprintf(w, "%s_bucket{%s} %d\n", f.name, labels, value.count); err != nil {
				return err
			}
			if _, err := fmt.Fprintf(w, "%s_sum%s %s\n", f.name, f.simpleLabels(value.values), formatFloat(value.sum)); err != nil {
				return err
			}
			if _, err := fmt.Fprintf(w, "%s_count%s %d\n", f.name, f.simpleLabels(value.values), value.count); err != nil {
				return err
			}
			continue
		}
		if _, err := fmt.Fprintf(w, "%s%s %s\n", f.name, f.simpleLabels(value.values), formatFloat(value.value)); err != nil {
			return err
		}
	}
	return nil
}

func (f *family) simpleLabels(values []string) string {
	if len(f.labelNames) == 0 {
		return ""
	}
	pairs := make([]string, 0, len(f.labelNames))
	for index, name := range f.labelNames {
		pairs = append(pairs, fmt.Sprintf(`%s="%s"`, name, escapeLabel(values[index])))
	}
	return "{" + strings.Join(pairs, ",") + "}"
}

func (f *family) labelPairs(values []string, extraKey, extraValue string) string {
	pairs := make([]string, 0, len(f.labelNames)+1)
	for index, name := range f.labelNames {
		pairs = append(pairs, fmt.Sprintf(`%s="%s"`, name, escapeLabel(values[index])))
	}
	pairs = append(pairs, fmt.Sprintf(`%s="%s"`, extraKey, escapeLabel(extraValue)))
	return strings.Join(pairs, ",")
}

// Metrics is the fixed set of counter and histogram families the daemon emits.
// A nil *Metrics is valid and every method is a no-op, so handlers and tests do
// not need to guard each call site.
type Metrics struct {
	registry *Registry

	httpRequests *Counter
	httpDuration *Histogram
	authTotal    *Counter

	providerDuration *Histogram
	providerErrors   *Counter
	graphDuration    *Histogram
	graphErrors      *Counter

	schedulerDuration *Histogram
	schedulerRuns     *Counter
	tasks             *Counter
	reminders         *Counter
	briefings         *Counter
	approvals         *Counter
	otlpExports       *Counter
}

// NewMetrics registers every metric family on a fresh registry.
func NewMetrics() *Metrics {
	registry := NewRegistry()
	metrics := &Metrics{registry: registry}
	metrics.httpRequests = registry.Counter(MetricsPrefix+"http_requests_total", "HTTP requests handled by method, route, and status code.", "method", "route", "status_code")
	metrics.httpDuration = registry.Histogram(MetricsPrefix+"http_request_duration_seconds", "HTTP request latency in seconds.", DefaultBuckets, "method", "route")
	metrics.authTotal = registry.Counter(MetricsPrefix+"auth_total", "Authentication decisions by outcome and reason.", "decision", "reason")
	metrics.providerDuration = registry.Histogram(MetricsPrefix+"provider_request_duration_seconds", "Outbound provider request latency in seconds.", DefaultBuckets, "provider")
	metrics.providerErrors = registry.Counter(MetricsPrefix+"provider_errors_total", "Outbound provider requests that failed.", "provider")
	metrics.graphDuration = registry.Histogram(MetricsPrefix+"graph_query_duration_seconds", "FalkorDB command latency in seconds.", DefaultBuckets, "operation")
	metrics.graphErrors = registry.Counter(MetricsPrefix+"graph_errors_total", "FalkorDB commands that returned an error.", "operation")
	metrics.schedulerDuration = registry.Histogram(MetricsPrefix+"scheduler_run_duration_seconds", "Scheduled collection run latency in seconds.", DefaultBuckets)
	metrics.schedulerRuns = registry.Counter(MetricsPrefix+"scheduler_runs_total", "Scheduled collection runs by outcome.", "result")
	metrics.tasks = registry.Counter(MetricsPrefix+"tasks_total", "Agent tasks by kind and result.", "kind", "result")
	metrics.reminders = registry.Counter(MetricsPrefix+"reminders_total", "Reminders reconciled by status.", "status")
	metrics.briefings = registry.Counter(MetricsPrefix+"briefings_total", "Briefing pipeline runs by status.", "status")
	metrics.approvals = registry.Counter(MetricsPrefix+"approvals_total", "Action approvals by result.", "result")
	metrics.otlpExports = registry.Counter(MetricsPrefix+"otlp_exports_total", "OTLP export attempts by result.", "result")
	return metrics
}

// Registry exposes the underlying registry for the /metrics endpoint and OTLP.
func (m *Metrics) Registry() *Registry {
	if m == nil {
		return nil
	}
	return m.registry
}

// ObserveHTTP records request count and latency and derives the auth metrics.
func (m *Metrics) ObserveHTTP(method, route string, status int, duration time.Duration) {
	if m == nil {
		return
	}
	statusCode := strconv.Itoa(status)
	m.httpRequests.Inc(map[string]string{"method": method, "route": route, "status_code": statusCode})
	m.httpDuration.Observe(map[string]string{"method": method, "route": route}, duration.Seconds())
	if status == 401 {
		m.authTotal.Inc(map[string]string{"decision": "denied", "reason": "unauthenticated"})
	}
}

// RecordAuth records an explicit authentication decision.
func (m *Metrics) RecordAuth(decision, reason string) {
	if m == nil {
		return
	}
	m.authTotal.Inc(map[string]string{"decision": decision, "reason": reason})
}

// ObserveProvider records provider latency and, when err is non-nil, an error.
func (m *Metrics) ObserveProvider(provider string, duration time.Duration, err error) {
	if m == nil {
		return
	}
	m.providerDuration.Observe(map[string]string{"provider": provider}, duration.Seconds())
	if err != nil {
		m.providerErrors.Inc(map[string]string{"provider": provider})
	}
}

// ObserveGraph records a FalkorDB command latency and error count.
func (m *Metrics) ObserveGraph(operation string, duration time.Duration, err error) {
	if m == nil {
		return
	}
	if operation == "" {
		operation = "unknown"
	}
	m.graphDuration.Observe(map[string]string{"operation": operation}, duration.Seconds())
	if err != nil {
		m.graphErrors.Inc(map[string]string{"operation": operation})
	}
}

// ObserveScheduler records a scheduled run latency and outcome.
func (m *Metrics) ObserveScheduler(duration time.Duration, err error) {
	if m == nil {
		return
	}
	m.schedulerDuration.Observe(nil, duration.Seconds())
	result := "success"
	if err != nil {
		result = "error"
	}
	m.schedulerRuns.Inc(map[string]string{"result": result})
}

// RecordTask records an agent task by kind and result.
func (m *Metrics) RecordTask(kind, result string) {
	if m == nil {
		return
	}
	m.tasks.Inc(map[string]string{"kind": kind, "result": result})
}

// RecordReminder records reminder counts by status.
func (m *Metrics) RecordReminder(status string, count int) {
	if m == nil || count <= 0 {
		return
	}
	m.reminders.Add(map[string]string{"status": status}, float64(count))
}

// RecordBriefing records a briefing pipeline run by status.
func (m *Metrics) RecordBriefing(status string) {
	if m == nil {
		return
	}
	m.briefings.Inc(map[string]string{"status": status})
}

// RecordApproval records an action approval by result.
func (m *Metrics) RecordApproval(result string) {
	if m == nil {
		return
	}
	m.approvals.Inc(map[string]string{"result": result})
}

// RecordOTLP records an OTLP export attempt by result.
func (m *Metrics) RecordOTLP(result string) {
	if m == nil {
		return
	}
	m.otlpExports.Inc(map[string]string{"result": result})
}

func formatFloat(value float64) string {
	if value == float64(int64(value)) {
		return strconv.FormatInt(int64(value), 10)
	}
	return strconv.FormatFloat(value, 'g', -1, 64)
}

func escapeLabel(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, `"`, `\"`)
	return strings.ReplaceAll(value, "\n", `\n`)
}

func sanitizeHelp(value string) string {
	return strings.ReplaceAll(value, "\n", " ")
}
