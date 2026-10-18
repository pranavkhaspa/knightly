package observability

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
)

// LogConfig configures the process logger. Empty values select safe defaults:
// JSON at info level on stderr with credentials redacted.
type LogConfig struct {
	Level      string
	Format     string
	Service    string
	InstanceID string
	Writer     io.Writer
	Redactor   *Redactor
}

// NewLogger builds the structured logger used by the daemon. Every attribute
// value passes through the redactor before it reaches the writer.
func NewLogger(cfg LogConfig) *slog.Logger {
	writer := cfg.Writer
	if writer == nil {
		writer = os.Stderr
	}
	redactor := cfg.Redactor
	if redactor == nil {
		redactor = NewRedactor()
	}
	options := &slog.HandlerOptions{Level: ParseLevel(cfg.Level)}

	var handler slog.Handler
	if strings.EqualFold(strings.TrimSpace(cfg.Format), "text") {
		handler = slog.NewTextHandler(writer, options)
	} else {
		handler = slog.NewJSONHandler(writer, options)
	}
	handler = &redactingHandler{next: handler, redactor: redactor}

	logger := slog.New(handler)
	if service := strings.TrimSpace(cfg.Service); service != "" {
		logger = logger.With(slog.String("service", service))
	}
	if instance := strings.TrimSpace(cfg.InstanceID); instance != "" {
		logger = logger.With(slog.String("instance_id", instance))
	}
	return logger
}

// ParseLevel maps a configuration string onto a slog level. Unknown values
// default to info so a typo cannot silence the daemon.
func ParseLevel(value string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// redactingHandler scrubs every string attribute and the record message.
type redactingHandler struct {
	next     slog.Handler
	redactor *Redactor
	groups   []string
}

func (h *redactingHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

func (h *redactingHandler) Handle(ctx context.Context, record slog.Record) error {
	cleaned := slog.NewRecord(record.Time, record.Level, h.redactor.Scrub(record.Message), record.PC)
	record.Attrs(func(attr slog.Attr) bool {
		cleaned.AddAttrs(h.cleanAttr(attr))
		return true
	})
	return h.next.Handle(ctx, cleaned)
}

func (h *redactingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	cleaned := make([]slog.Attr, 0, len(attrs))
	for _, attr := range attrs {
		cleaned = append(cleaned, h.cleanAttr(attr))
	}
	return &redactingHandler{next: h.next.WithAttrs(cleaned), redactor: h.redactor, groups: h.groups}
}

func (h *redactingHandler) WithGroup(name string) slog.Handler {
	return &redactingHandler{next: h.next.WithGroup(name), redactor: h.redactor, groups: append(append([]string(nil), h.groups...), name)}
}

func (h *redactingHandler) cleanAttr(attr slog.Attr) slog.Attr {
	if attr.Value.Kind() == slog.KindGroup {
		group := attr.Value.Group()
		cleaned := make([]slog.Attr, 0, len(group))
		for _, nested := range group {
			cleaned = append(cleaned, h.cleanAttr(nested))
		}
		attr.Value = slog.GroupValue(cleaned...)
		return attr
	}
	if attr.Value.Kind() == slog.KindString {
		attr.Value = slog.StringValue(h.redactor.Value(attr.Key, attr.Value.String()))
	}
	return attr
}

// LogAttrs is a small helper that logs a structured record with a correlation
// identifier attached when one is present on the request context.
func LogAttrs(ctx context.Context, logger *slog.Logger, level slog.Level, message string, attrs ...slog.Attr) {
	if logger == nil {
		return
	}
	if correlationID, ok := CorrelationID(ctx); ok {
		attrs = append(attrs, slog.String("request_id", correlationID))
	}
	logger.LogAttrs(ctx, level, message, attrs...)
}

type correlationKey struct{}

// WithCorrelationID stores a request/correlation identifier on the context.
func WithCorrelationID(ctx context.Context, id string) context.Context {
	if ctx == nil || strings.TrimSpace(id) == "" {
		return ctx
	}
	return context.WithValue(ctx, correlationKey{}, id)
}

// CorrelationID returns the correlation identifier carried by the context.
func CorrelationID(ctx context.Context) (string, bool) {
	if ctx == nil {
		return "", false
	}
	value, ok := ctx.Value(correlationKey{}).(string)
	return value, ok && value != ""
}
