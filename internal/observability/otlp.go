package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// OTLPConfig configures the optional OpenTelemetry/OTLP metrics exporter. An
// empty Endpoint disables export entirely.
type OTLPConfig struct {
	Endpoint   string
	Service    string
	InstanceID string
	Headers    map[string]string
	Client     *http.Client
	Interval   time.Duration
	Timeout    time.Duration
}

// Exporter periodically snapshots the registry and posts OTLP/HTTP JSON to the
// configured endpoint. Export happens on a background goroutine, so the request
// path never blocks: RequestExport drops the signal when the queue is full.
type Exporter struct {
	config    OTLPConfig
	registry  *Registry
	metrics   *Metrics
	client    *http.Client
	queue     chan struct{}
	done      chan struct{}
	stopped   chan struct{}
	stopOnce  sync.Once
	startOnce sync.Once
}

// NewExporter validates the configuration. It returns (nil, nil) when no
// endpoint is configured so callers can treat export as disabled.
func NewExporter(cfg OTLPConfig, registry *Registry, metrics *Metrics) (*Exporter, error) {
	endpoint := strings.TrimSpace(cfg.Endpoint)
	if endpoint == "" {
		return nil, nil
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return nil, fmt.Errorf("OTLP endpoint must be an absolute http(s) URL")
	}
	client := cfg.Client
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 15 * time.Second
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 5 * time.Second
	}
	cfg.Endpoint = endpoint
	return &Exporter{
		config:   cfg,
		registry: registry,
		metrics:  metrics,
		client:   client,
		queue:    make(chan struct{}, 1),
		done:     make(chan struct{}),
		stopped:  make(chan struct{}),
	}, nil
}

// Start begins the background export loop. It is safe to call once.
func (e *Exporter) Start() {
	if e == nil {
		return
	}
	e.startOnce.Do(func() { go e.loop() })
}

// Stop flushes once and stops the export loop.
func (e *Exporter) Stop(ctx context.Context) error {
	if e == nil {
		return nil
	}
	e.stopOnce.Do(func() { close(e.done) })
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-e.stopped:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// RequestExport asks for an out-of-band export without blocking. When the queue
// is already full the request is dropped and counted.
func (e *Exporter) RequestExport() {
	if e == nil {
		return
	}
	select {
	case e.queue <- struct{}{}:
	default:
		e.metrics.RecordOTLP("dropped")
	}
}

func (e *Exporter) loop() {
	defer close(e.stopped)
	ticker := time.NewTicker(e.config.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-e.done:
			e.export()
			return
		case <-ticker.C:
			e.export()
		case <-e.queue:
			e.export()
		}
	}
}

func (e *Exporter) export() {
	if e.registry == nil {
		return
	}
	payload := e.payload(time.Now().UTC())
	body, err := json.Marshal(payload)
	if err != nil {
		e.metrics.RecordOTLP("error")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), e.config.Timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, e.metricsURL(), bytes.NewReader(body))
	if err != nil {
		e.metrics.RecordOTLP("error")
		return
	}
	request.Header.Set("Content-Type", "application/json")
	for key, value := range e.config.Headers {
		request.Header.Set(key, value)
	}
	response, err := e.client.Do(request)
	if err != nil {
		e.metrics.RecordOTLP("error")
		return
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4<<10))
	_ = response.Body.Close()
	if response.StatusCode >= 300 {
		e.metrics.RecordOTLP("error")
		return
	}
	e.metrics.RecordOTLP("success")
}

func (e *Exporter) metricsURL() string {
	endpoint := strings.TrimSuffix(e.config.Endpoint, "/")
	if strings.HasSuffix(endpoint, "/v1/metrics") {
		return endpoint
	}
	return endpoint + "/v1/metrics"
}

func (e *Exporter) payload(now time.Time) otlpPayload {
	attributes := make([]otlpAttribute, 0, 3)
	if service := strings.TrimSpace(e.config.Service); service != "" {
		attributes = append(attributes, stringAttribute("service.name", service))
	}
	if instance := strings.TrimSpace(e.config.InstanceID); instance != "" {
		attributes = append(attributes, stringAttribute("service.instance.id", instance))
	}
	timestamp := strconv.FormatInt(now.UnixNano(), 10)

	metrics := make([]otlpMetric, 0, 16)
	for _, family := range e.registry.Snapshot() {
		metric := otlpMetric{Name: family.Name, Description: family.Help}
		switch family.Kind {
		case "histogram":
			points := make([]otlpHistogramDataPoint, 0, len(family.Series))
			for _, item := range family.Series {
				points = append(points, otlpHistogramDataPoint{
					Attributes: attributesFromLabels(item.Labels), Count: item.Count, Sum: item.Sum,
					BucketCounts: item.BucketCounts, ExplicitBounds: family.Buckets, TimeUnixNano: timestamp,
				})
			}
			metric.Unit = "s"
			metric.Histogram = &otlpHistogram{DataPoints: points, AggregationTemporality: 2}
		default:
			points := make([]otlpNumberDataPoint, 0, len(family.Series))
			for _, item := range family.Series {
				points = append(points, otlpNumberDataPoint{
					Attributes: attributesFromLabels(item.Labels), AsDouble: item.Value, TimeUnixNano: timestamp,
				})
			}
			metric.Sum = &otlpSum{DataPoints: points, AggregationTemporality: 2, IsMonotonic: true}
		}
		metrics = append(metrics, metric)
	}

	return otlpPayload{ResourceMetrics: []otlpResourceMetrics{{
		Resource: otlpResource{Attributes: attributes},
		ScopeMetrics: []otlpScopeMetrics{{
			Scope:   otlpScope{Name: "nightshift/observability"},
			Metrics: metrics,
		}},
	}}}
}

// FamilySnapshot is a point-in-time copy of one metric family.
type FamilySnapshot struct {
	Name       string
	Help       string
	Kind       string
	LabelNames []string
	Buckets    []float64
	Series     []SeriesSnapshot
}

// SeriesSnapshot is a point-in-time copy of one labelled series.
type SeriesSnapshot struct {
	Labels       map[string]string
	Value        float64
	Count        uint64
	Sum          float64
	BucketCounts []uint64
}

// Snapshot copies every family so an exporter can read them without holding the
// registry lock during network I/O.
func (r *Registry) Snapshot() []FamilySnapshot {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	names := append([]string(nil), r.order...)
	r.mu.Unlock()

	snapshots := make([]FamilySnapshot, 0, len(names))
	for _, name := range names {
		r.mu.Lock()
		f := r.families[name]
		r.mu.Unlock()
		if f == nil {
			continue
		}
		f.mu.Lock()
		family := FamilySnapshot{
			Name: f.name, Help: f.help, LabelNames: append([]string(nil), f.labelNames...),
			Buckets: append([]float64(nil), f.buckets...),
		}
		if f.kind == kindHistogram {
			family.Kind = "histogram"
		} else {
			family.Kind = "counter"
		}
		family.Series = make([]SeriesSnapshot, 0, len(f.series))
		for _, value := range f.series {
			labels := make(map[string]string, len(f.labelNames))
			for index, label := range f.labelNames {
				labels[label] = value.values[index]
			}
			family.Series = append(family.Series, SeriesSnapshot{
				Labels: labels, Value: value.value, Count: value.count, Sum: value.sum,
				BucketCounts: append([]uint64(nil), value.bucketCounts...),
			})
		}
		f.mu.Unlock()
		snapshots = append(snapshots, family)
	}
	return snapshots
}

func attributesFromLabels(labels map[string]string) []otlpAttribute {
	if len(labels) == 0 {
		return nil
	}
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sortStrings(keys)
	attributes := make([]otlpAttribute, 0, len(keys))
	for _, key := range keys {
		attributes = append(attributes, stringAttribute(key, labels[key]))
	}
	return attributes
}

func stringAttribute(key, value string) otlpAttribute {
	return otlpAttribute{Key: key, Value: otlpValue{StringValue: value}}
}

func sortStrings(values []string) {
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && values[j] < values[j-1]; j-- {
			values[j], values[j-1] = values[j-1], values[j]
		}
	}
}

type otlpPayload struct {
	ResourceMetrics []otlpResourceMetrics `json:"resourceMetrics"`
}

type otlpResourceMetrics struct {
	Resource     otlpResource       `json:"resource"`
	ScopeMetrics []otlpScopeMetrics `json:"scopeMetrics"`
}

type otlpResource struct {
	Attributes []otlpAttribute `json:"attributes"`
}

type otlpScopeMetrics struct {
	Scope   otlpScope    `json:"scope"`
	Metrics []otlpMetric `json:"metrics"`
}

type otlpScope struct {
	Name string `json:"name"`
}

type otlpMetric struct {
	Name        string         `json:"name"`
	Description string         `json:"description,omitempty"`
	Unit        string         `json:"unit,omitempty"`
	Sum         *otlpSum       `json:"sum,omitempty"`
	Histogram   *otlpHistogram `json:"histogram,omitempty"`
}

type otlpSum struct {
	DataPoints             []otlpNumberDataPoint `json:"dataPoints"`
	AggregationTemporality int                   `json:"aggregationTemporality"`
	IsMonotonic            bool                  `json:"isMonotonic"`
}

type otlpHistogram struct {
	DataPoints             []otlpHistogramDataPoint `json:"dataPoints"`
	AggregationTemporality int                      `json:"aggregationTemporality"`
}

type otlpNumberDataPoint struct {
	Attributes   []otlpAttribute `json:"attributes,omitempty"`
	AsDouble     float64         `json:"asDouble"`
	TimeUnixNano string          `json:"timeUnixNano"`
}

type otlpHistogramDataPoint struct {
	Attributes     []otlpAttribute `json:"attributes,omitempty"`
	Count          uint64          `json:"count"`
	Sum            float64         `json:"sum"`
	BucketCounts   []uint64        `json:"bucketCounts"`
	ExplicitBounds []float64       `json:"explicitBounds"`
	TimeUnixNano   string          `json:"timeUnixNano"`
}

type otlpAttribute struct {
	Key   string    `json:"key"`
	Value otlpValue `json:"value"`
}

type otlpValue struct {
	StringValue string `json:"stringValue"`
}
