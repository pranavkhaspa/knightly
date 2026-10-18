package observability

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// GraphHook is a go-redis Hook that records FalkorDB command latency and
// errors. It is installable with falkor.Client.AddHook. The FalkorDB client
// speaks the Redis protocol, so every graph query passes through it.
type GraphHook struct{ metrics *Metrics }

var _ redis.Hook = GraphHook{}

// NewGraphHook returns a hook that reports into metrics.
func NewGraphHook(metrics *Metrics) GraphHook { return GraphHook{metrics: metrics} }

// DialHook leaves connection dialing uninstrumented.
func (h GraphHook) DialHook(next redis.DialHook) redis.DialHook { return next }

// ProcessHook measures a single command round trip.
func (h GraphHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, command redis.Cmder) error {
		start := time.Now()
		err := next(ctx, command)
		h.metrics.ObserveGraph(commandName(command), time.Since(start), err)
		return err
	}
}

// ProcessPipelineHook measures a pipelined batch as one operation.
func (h GraphHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, commands []redis.Cmder) error {
		start := time.Now()
		err := next(ctx, commands)
		h.metrics.ObserveGraph("pipeline", time.Since(start), err)
		return err
	}
}

func commandName(command redis.Cmder) string {
	if command == nil {
		return "unknown"
	}
	if name := command.Name(); name != "" {
		return name
	}
	return "unknown"
}
