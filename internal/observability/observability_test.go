package observability

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRedactorScrubsCredentials(t *testing.T) {
	redactor := NewRedactor()
	scrubCases := map[string]string{
		"client_secret=super-secret":                     "client_secret=" + Placeholder,
		"Bearer abc.def.ghi":                             Placeholder,
		"access_token=xyz&q=ok":                          "access_token=" + Placeholder + "&q=ok",
		"email body: my password=hunter2 ok":             "email body: my password=" + Placeholder + " ok",
		"eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.signature": Placeholder,
		"plain text with no secrets":                     "plain text with no secrets",
	}
	for input, want := range scrubCases {
		if got := redactor.Scrub(input); got != want {
			t.Errorf("Scrub(%q) = %q, want %q", input, got, want)
		}
	}
	for _, key := range []string{"Authorization", "Cookie", "X-Nightshift-Proxy-Signature", "access_token"} {
		if got := redactor.Value(key, "anything"); got != Placeholder {
			t.Errorf("Value(%q) = %q, want placeholder", key, got)
		}
	}
	if got := redactor.ScrubURL("https://example.test/cb?code=secret&state=keep"); got != "https://example.test/cb?code=secret&state=keep" {
		t.Errorf("ScrubURL kept non-credential query: %q", got)
	}
	if got := redactor.ScrubURL("https://example.test/cb?access_token=secret"); strings.Contains(got, "secret") {
		t.Errorf("ScrubURL leaked credential: %q", got)
	}
}

func TestLoggerRedactsStructuredAttributes(t *testing.T) {
	var buffer bytes.Buffer
	logger := NewLogger(LogConfig{Format: "json", Level: "debug", Writer: &buffer, Service: "nightshift"})
	logger.Info("request authorization=Bearer top-secret-token",
		slog.String("authorization", "Bearer top-secret-token"),
		slog.String("access_token", "raw-token"),
		slog.String("note", "client_secret=raw-secret"),
	)
	output := buffer.String()
	for _, secret := range []string{"top-secret-token", "raw-token", "raw-secret"} {
		if strings.Contains(output, secret) {
			t.Fatalf("log output leaked %q: %s", secret, output)
		}
	}
	if !strings.Contains(output, Placeholder) {
		t.Fatalf("log output did not contain the placeholder: %s", output)
	}
	if !strings.Contains(output, `"service"`) {
		t.Fatalf("log output missing structured fields: %s", output)
	}
}

func TestMetricsRenderCountersAndHistograms(t *testing.T) {
	metrics := NewMetrics()
	metrics.ObserveHTTP(http.MethodGet, "/api/v1/status", http.StatusOK, 50*time.Millisecond)
	metrics.ObserveHTTP(http.MethodGet, "/api/v1/status", http.StatusUnauthorized, 5*time.Millisecond)
	metrics.ObserveProvider("github", 120*time.Millisecond, nil)
	metrics.ObserveProvider("github", 3*time.Second, context.DeadlineExceeded)
	metrics.ObserveGraph("graph.query", 20*time.Millisecond, nil)
	metrics.RecordTask("action", "executed")
	metrics.RecordApproval("approved")
	metrics.RecordBriefing("success")

	var buffer bytes.Buffer
	if err := metrics.Registry().Render(&buffer); err != nil {
		t.Fatalf("Render() error = %v", err)
	}
	output := buffer.String()
	for _, want := range []string{
		`nightshift_http_requests_total{method="GET",route="/api/v1/status",status_code="200"} 1`,
		`nightshift_http_requests_total{method="GET",route="/api/v1/status",status_code="401"} 1`,
		`nightshift_auth_total{decision="denied",reason="unauthenticated"} 1`,
		`nightshift_provider_errors_total{provider="github"} 1`,
		`nightshift_graph_query_duration_seconds_count{operation="graph.query"} 1`,
		`nightshift_tasks_total{kind="action",result="executed"} 1`,
		`nightshift_approvals_total{result="approved"} 1`,
		`nightshift_briefings_total{status="success"} 1`,
		"# TYPE nightshift_http_request_duration_seconds histogram",
	} {
		if !strings.Contains(output, want) {
			t.Errorf("metrics output missing %q\n%s", want, output)
		}
	}

	snapshot := metrics.Registry().Snapshot()
	if len(snapshot) == 0 {
		t.Fatal("Snapshot() returned no families")
	}
}

func TestRedactionCoversPromptAndMailBodySamples(t *testing.T) {
	redactor := NewRedactor()
	samples := []string{
		"Mail body: please review the invoice. refresh_token=1//0eXample",
		"Prompt: summarize this mail. api_key=sk-live-example-123",
		"Cookie: session=abcdef; Authorization: Bearer raw.jwt.value",
	}
	for _, sample := range samples {
		scrubbed := redactor.Scrub(sample)
		for _, secret := range []string{"1//0eXample", "sk-live-example-123", "abcdef", "raw.jwt.value"} {
			if strings.Contains(scrubbed, secret) {
				t.Errorf("Scrub(%q) leaked %q: %q", sample, secret, scrubbed)
			}
		}
	}
}

func TestMiddlewareRouteLabelsStayBounded(t *testing.T) {
	metrics := NewMetrics()
	logger := NewLogger(LogConfig{Writer: io.Discard})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/mail/messages/{id}", func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK)
	})
	handler := Middleware(logger, metrics, mux)
	request := httptest.NewRequest(http.MethodGet, "/api/v1/mail/messages/abc123?access_token=super-secret", nil)
	handler.ServeHTTP(httptest.NewRecorder(), request)

	var buffer bytes.Buffer
	if err := metrics.Registry().Render(&buffer); err != nil {
		t.Fatalf("Render() error = %v", err)
	}
	output := buffer.String()
	if !strings.Contains(output, `route="GET /api/v1/mail/messages/{id}"`) {
		t.Errorf("route label was not the bounded pattern:\n%s", output)
	}
	if strings.Contains(output, "super-secret") || strings.Contains(output, "abc123") {
		t.Errorf("metric label leaked request payload:\n%s", output)
	}
}

func TestNilMetricsAreSafe(t *testing.T) {
	var metrics *Metrics
	metrics.ObserveHTTP(http.MethodGet, "/", http.StatusOK, time.Second)
	metrics.ObserveProvider("x", time.Second, nil)
	metrics.ObserveGraph("x", time.Second, nil)
	metrics.ObserveScheduler(time.Second, nil)
	metrics.RecordTask("x", "y")
	metrics.RecordReminder("claimed", 1)
	metrics.RecordBriefing("success")
	metrics.RecordApproval("approved")
	metrics.RecordOTLP("success")
	metrics.RecordAuth("denied", "reason")
	if registry := metrics.Registry(); registry != nil {
		t.Fatal("nil metrics exposed a registry")
	}
}

func TestMiddlewareRecordsHTTPAuthAndCorrelation(t *testing.T) {
	metrics := NewMetrics()
	logger := NewLogger(LogConfig{Writer: io.Discard})
	handler := Middleware(logger, metrics, http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		if _, ok := writer.(http.Flusher); !ok {
			t.Error("response writer does not support flushing")
		}
		writer.WriteHeader(http.StatusUnauthorized)
	}))
	request := httptest.NewRequest(http.MethodGet, "/api/v1/status", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Header().Get("X-Request-ID") == "" {
		t.Fatal("middleware did not set X-Request-ID")
	}
	var buffer bytes.Buffer
	if err := metrics.Registry().Render(&buffer); err != nil {
		t.Fatalf("Render() error = %v", err)
	}
	output := buffer.String()
	if !strings.Contains(output, `nightshift_http_requests_total{method="GET",route="unmatched",status_code="401"} 1`) {
		t.Errorf("missing http counter:\n%s", output)
	}
	if !strings.Contains(output, `nightshift_auth_total{decision="denied",reason="unauthenticated"} 1`) {
		t.Errorf("missing auth counter:\n%s", output)
	}
}

func TestTransportRecordsProviderErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	metrics := NewMetrics()
	client := &http.Client{Transport: NewTransport("github", nil, metrics)}
	response, err := client.Get(server.URL)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	_ = response.Body.Close()

	var buffer bytes.Buffer
	if err := metrics.Registry().Render(&buffer); err != nil {
		t.Fatalf("Render() error = %v", err)
	}
	if !strings.Contains(buffer.String(), `nightshift_provider_errors_total{provider="github"} 1`) {
		t.Errorf("missing provider error counter:\n%s", buffer.String())
	}
}

func TestExporterPostsOTLPJSONAndIsNonBlocking(t *testing.T) {
	var (
		mu     sync.Mutex
		bodies []string
	)
	received := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, _ := io.ReadAll(request.Body)
		mu.Lock()
		bodies = append(bodies, string(body))
		mu.Unlock()
		writer.WriteHeader(http.StatusOK)
		select {
		case received <- string(body):
		default:
		}
	}))
	defer server.Close()

	metrics := NewMetrics()
	metrics.RecordApproval("approved")
	exporter, err := NewExporter(OTLPConfig{
		Endpoint: server.URL, Service: "nightshift", InstanceID: "test", Interval: time.Hour,
	}, metrics.Registry(), metrics)
	if err != nil {
		t.Fatalf("NewExporter() error = %v", err)
	}
	exporter.Start()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = exporter.Stop(ctx)
	}()

	exporter.RequestExport()
	select {
	case body := <-received:
		if !strings.Contains(body, "resourceMetrics") || !strings.Contains(body, "nightshift_approvals_total") {
			t.Fatalf("OTLP body missing expected content: %s", body)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("exporter did not post an OTLP payload")
	}
}

func TestExporterDisabledWithoutEndpoint(t *testing.T) {
	exporter, err := NewExporter(OTLPConfig{}, nil, NewMetrics())
	if err != nil {
		t.Fatalf("NewExporter() error = %v", err)
	}
	if exporter != nil {
		t.Fatal("expected a disabled exporter")
	}
	exporter.Start()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := exporter.Stop(ctx); err != nil {
		t.Fatalf("disabled Stop() error = %v", err)
	}
}
