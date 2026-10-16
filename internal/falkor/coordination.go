package falkor

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/pranavkhaspa/knightly/internal/coordination"
)

var _ coordination.Repository = (*Client)(nil)

// FindIdempotency reads the durable result from the control graph. Results
// live in FalkorDB so a restarted or separately scaled daemon sees the same
// completion record.
func (c *Client) FindIdempotency(ctx context.Context, key string) (coordination.IdempotencyRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return coordination.IdempotencyRecord{}, false, err
	}
	result, err := c.control.ROQuery(`
		MATCH (i:Idempotency {key: $key})
		RETURN i.key AS key, i.token AS token, i.payload AS payload, i.stored_at AS stored_at
		LIMIT 1
	`, map[string]interface{}{"key": key}, c.options())
	if err != nil {
		return coordination.IdempotencyRecord{}, false, fmt.Errorf("read idempotency record: %w", err)
	}
	if !result.Next() {
		return coordination.IdempotencyRecord{}, false, nil
	}
	record := result.Record()
	return coordination.IdempotencyRecord{
		Key:      stringValue(recordValue(record, "key")),
		Token:    stringValue(recordValue(record, "token")),
		Payload:  []byte(stringValue(recordValue(record, "payload"))),
		StoredAt: time.UnixMilli(int64Value(recordValue(record, "stored_at"))),
	}, true, nil
}

// AcquireLease atomically claims a key when it is new or expired. The
// conditional SET is evaluated by FalkorDB, so separate daemon instances do
// not coordinate through local process memory.
func (c *Client) AcquireLease(ctx context.Context, request coordination.LeaseRequest) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if request.Key == "" || request.Owner == "" || request.Token == "" || request.Lifetime <= 0 {
		return false, coordination.ErrInvalidRequest
	}
	now := request.Now.UnixMilli()
	expires := request.Now.Add(request.Lifetime).UnixMilli()
	result, err := c.control.Query(`
		MERGE (l:Lease {key: $key})
		WITH l,
		     CASE WHEN coalesce(l.expires_at, 0) <= $now
		               OR (l.owner_id = $owner_id AND l.token = $token)
		          THEN true ELSE false END AS can_acquire
		SET l.owner_id = CASE WHEN can_acquire THEN $owner_id ELSE l.owner_id END,
		    l.token = CASE WHEN can_acquire THEN $token ELSE l.token END,
		    l.expires_at = CASE WHEN can_acquire THEN $expires_at ELSE l.expires_at END,
		    l.updated_at = CASE WHEN can_acquire THEN $now ELSE l.updated_at END
		RETURN can_acquire AS acquired
	`, map[string]interface{}{
		"key":        request.Key,
		"owner_id":   request.Owner,
		"token":      request.Token,
		"now":        now,
		"expires_at": expires,
	}, c.options())
	if err != nil {
		return false, fmt.Errorf("acquire graph lease: %w", err)
	}
	if !result.Next() {
		return false, nil
	}
	return boolValue(recordValue(result.Record(), "acquired")), nil
}

// StoreIdempotency publishes a result only while the supplied token still
// owns the graph lease and only if no result exists for the key.
func (c *Client) StoreIdempotency(ctx context.Context, write coordination.IdempotencyWrite) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if write.Key == "" || write.Token == "" {
		return false, coordination.ErrInvalidRequest
	}
	result, err := c.control.Query(`
		MATCH (l:Lease {key: $key, token: $token})
		MERGE (i:Idempotency {key: $key})
		WITH i, CASE WHEN i.token IS NULL THEN true ELSE false END AS is_new
		SET i.token = CASE WHEN is_new THEN $token ELSE i.token END,
		    i.payload = CASE WHEN is_new THEN $payload ELSE i.payload END,
		    i.stored_at = CASE WHEN is_new THEN $stored_at ELSE i.stored_at END
		RETURN is_new AS stored
	`, map[string]interface{}{
		"key":       write.Key,
		"token":     write.Token,
		"payload":   string(write.Payload),
		"stored_at": write.StoredAt.UnixMilli(),
	}, c.options())
	if err != nil {
		return false, fmt.Errorf("store graph idempotency record: %w", err)
	}
	if !result.Next() {
		return false, nil
	}
	return boolValue(recordValue(result.Record(), "stored")), nil
}

func (c *Client) ReleaseLease(ctx context.Context, key, token string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if key == "" || token == "" {
		return coordination.ErrInvalidRequest
	}
	if _, err := c.control.Query(`
		MATCH (l:Lease {key: $key, token: $token})
		DELETE l
	`, map[string]interface{}{"key": key, "token": token}, c.options()); err != nil {
		return fmt.Errorf("release graph lease: %w", err)
	}
	return nil
}

func boolValue(value interface{}) bool {
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		parsed, _ := strconv.ParseBool(typed)
		return parsed
	case []byte:
		parsed, _ := strconv.ParseBool(string(typed))
		return parsed
	case int:
		return typed != 0
	case int64:
		return typed != 0
	default:
		return false
	}
}
