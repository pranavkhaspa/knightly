package observability

import (
	"log/slog"
	"net/http"
	"time"
)

// Middleware logs one structured record per request and records the HTTP and
// auth metrics. It installs the correlation identifier on the request context
// so downstream handlers can attach it to their own logs.
func Middleware(logger *slog.Logger, metrics *Metrics, next http.Handler) http.Handler {
	if next == nil {
		next = http.NotFoundHandler()
	}
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		start := time.Now()
		correlationID := request.Header.Get("X-Request-ID")
		if correlationID == "" {
			correlationID = requestIdentifier(request)
		}
		recorder := &statusWriter{ResponseWriter: writer, status: http.StatusOK}
		request = request.WithContext(WithCorrelationID(request.Context(), correlationID))
		request.Header.Set("X-Request-ID", correlationID)
		writer.Header().Set("X-Request-ID", correlationID)
		next.ServeHTTP(recorder, request)

		duration := time.Since(start)
		route := routeLabel(request)
		metrics.ObserveHTTP(request.Method, route, recorder.status, duration)

		level := slog.LevelInfo
		switch {
		case recorder.status >= 500:
			level = slog.LevelError
		case recorder.status >= 400:
			level = slog.LevelWarn
		}
		LogAttrs(request.Context(), logger, level, "http_request",
			slog.String("method", request.Method),
			slog.String("route", route),
			slog.Int("status", recorder.status),
			slog.Int("bytes", recorder.bytes),
			slog.Duration("duration", duration),
			slog.String("request_id", correlationID),
		)
	})
}

func routeLabel(request *http.Request) string {
	if request == nil {
		return "unmatched"
	}
	pattern := request.Pattern
	if pattern == "" {
		if request.URL != nil && request.URL.Path == "" {
			return "unmatched"
		}
		return "unmatched"
	}
	return pattern
}

func requestIdentifier(request *http.Request) string {
	if request == nil {
		return "req_unknown"
	}
	return "req_" + time.Now().UTC().Format("20060102T150405.000000000")
}

// statusWriter records the status code and byte count while preserving the
// optional Flusher and Unwrap interfaces used by streaming handlers and
// http.ResponseController.
type statusWriter struct {
	http.ResponseWriter
	status      int
	bytes       int
	wroteHeader bool
}

func (w *statusWriter) WriteHeader(status int) {
	if w.wroteHeader {
		return
	}
	w.status = status
	w.wroteHeader = true
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusWriter) Write(data []byte) (int, error) {
	if !w.wroteHeader {
		w.status = http.StatusOK
		w.wroteHeader = true
	}
	written, err := w.ResponseWriter.Write(data)
	w.bytes += written
	return written, err
}

// Flush forwards to the wrapped writer when it supports flushing so
// server-sent-event handlers keep streaming.
func (w *statusWriter) Flush() {
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

// Unwrap exposes the wrapped writer to http.ResponseController.
func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
