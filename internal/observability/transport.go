package observability

import (
	"net/http"
	"time"
)

// Transport is an http.RoundTripper that records provider latency and errors
// for every outbound request. It is safe to share across goroutines.
type Transport struct {
	provider string
	base     http.RoundTripper
	metrics  *Metrics
}

// NewTransport wraps base (defaulting to http.DefaultTransport) and attributes
// every round trip to the named provider.
func NewTransport(provider string, base http.RoundTripper, metrics *Metrics) *Transport {
	if base == nil {
		base = http.DefaultTransport
	}
	return &Transport{provider: provider, base: base, metrics: metrics}
}

// RoundTrip implements http.RoundTripper.
func (t *Transport) RoundTrip(request *http.Request) (*http.Response, error) {
	start := time.Now()
	response, err := t.base.RoundTrip(request)
	duration := time.Since(start)
	if err == nil && response != nil && response.StatusCode >= 500 {
		err = &statusError{status: response.StatusCode}
	}
	t.metrics.ObserveProvider(t.provider, duration, err)
	if _, isStatus := err.(*statusError); isStatus {
		return response, nil
	}
	return response, err
}

type statusError struct{ status int }

func (e *statusError) Error() string { return http.StatusText(e.status) }
