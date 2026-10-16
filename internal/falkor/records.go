package falkor

import (
	"encoding/json"
	"fmt"
	"time"

	dbclient "github.com/FalkorDB/falkordb-go/v2"
)

type recordReader interface {
	Get(string) (interface{}, bool)
}

func stringValue(value interface{}) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return typed
	case []byte:
		return string(typed)
	default:
		return fmt.Sprint(value)
	}
}

func stringSliceValue(value interface{}) []string {
	switch typed := value.(type) {
	case []string:
		return append([]string(nil), typed...)
	case []interface{}:
		result := make([]string, 0, len(typed))
		for _, item := range typed {
			result = append(result, stringValue(item))
		}
		return result
	default:
		return nil
	}
}

func timeValue(value interface{}) time.Time {
	var millis int64
	switch typed := value.(type) {
	case int64:
		millis = typed
	case int:
		millis = int64(typed)
	case float64:
		millis = int64(typed)
	case time.Time:
		return typed.UTC()
	default:
		return time.Time{}
	}
	if millis == 0 {
		return time.Time{}
	}
	return time.UnixMilli(millis).UTC()
}

func jsonMap(value map[string]string) string {
	encoded, _ := json.Marshal(value)
	return string(encoded)
}

func parseStringMap(value interface{}) map[string]string {
	encoded := stringValue(value)
	if encoded == "" || encoded == "<nil>" {
		return nil
	}
	result := make(map[string]string)
	if err := json.Unmarshal([]byte(encoded), &result); err != nil {
		return nil
	}
	return result
}

func requireQueryRow(result *dbclient.QueryResult, operation string) error {
	if result == nil || !result.Next() {
		return fmt.Errorf("%s returned no row", operation)
	}
	return nil
}
