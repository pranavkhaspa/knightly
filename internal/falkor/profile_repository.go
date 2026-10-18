package falkor

import (
	"context"
	"fmt"
	"strings"

	dbclient "github.com/FalkorDB/falkordb-go/v2"
	"github.com/pranavkhaspa/knightly/internal/identity"
	"github.com/pranavkhaspa/knightly/internal/profile"
)

type ProfileRepository struct {
	selector UserGraphSelector
}

var _ profile.Repository = (*ProfileRepository)(nil)

func NewProfileRepository(selector UserGraphSelector) *ProfileRepository {
	return &ProfileRepository{selector: selector}
}

func (r *ProfileRepository) Save(ctx context.Context, value profile.Profile) error {
	if r == nil {
		return fmt.Errorf("profile repository is not configured")
	}
	scope := identity.Scope{TenantID: value.TenantID, UserID: value.UserID}
	graph, err := routedGraph(ctx, r.selector, scope, "profile save")
	if err != nil {
		return err
	}
	result, err := graph.Query(`
		MERGE (u:User {id: $user_id, tenant_id: $tenant_id})
		ON CREATE SET u.created_at = $created_at
		SET u.display_name = $display_name,
		    u.interests = $interests,
		    u.skills = $skills,
		    u.preferred_technologies = $preferred_technologies,
		    u.goals = $goals,
		    u.location_preferences = $location_preferences,
		    u.content_preferences_json = $content_preferences_json,
		    u.quiet_hours_start = $quiet_hours_start,
		    u.quiet_hours_end = $quiet_hours_end,
		    u.quiet_hours_timezone = $quiet_hours_timezone,
		    u.notification_preferences_json = $notification_preferences_json,
		    u.updated_at = $updated_at
		RETURN u.id AS id
	`, profileParams(value), queryOptionsFor(r.selector))
	if err != nil {
		return fmt.Errorf("save profile in graph: %w", err)
	}
	if err := requireQueryRow(result, "save profile"); err != nil {
		return err
	}
	for _, relationship := range []struct {
		kind   string
		values []string
	}{
		{kind: "interest", values: value.Interests},
		{kind: "skill", values: value.Skills},
		{kind: "technology", values: value.PreferredTechnologies},
	} {
		if err := syncProfileRelationship(ctx, graph, value, relationship.kind, relationship.values, r.selector); err != nil {
			return err
		}
	}
	return nil
}

func (r *ProfileRepository) Get(ctx context.Context, scope identity.Scope) (profile.Profile, error) {
	if r == nil {
		return profile.Profile{}, fmt.Errorf("profile repository is not configured")
	}
	graph, err := routedGraph(ctx, r.selector, scope, "profile get")
	if err != nil {
		return profile.Profile{}, err
	}
	result, err := graph.ROQuery(`
		MATCH (u:User {id: $user_id, tenant_id: $tenant_id})
		RETURN u.id AS user_id, u.tenant_id AS tenant_id,
		       u.display_name AS display_name, u.interests AS interests,
		       u.skills AS skills, u.preferred_technologies AS preferred_technologies,
		       u.goals AS goals, u.location_preferences AS location_preferences,
		       u.content_preferences_json AS content_preferences_json,
		       u.quiet_hours_start AS quiet_hours_start,
		       u.quiet_hours_end AS quiet_hours_end,
		       u.quiet_hours_timezone AS quiet_hours_timezone,
		       u.notification_preferences_json AS notification_preferences_json,
		       u.created_at AS created_at, u.updated_at AS updated_at
		LIMIT 1
	`, map[string]interface{}{"tenant_id": scope.TenantID, "user_id": scope.UserID}, queryOptionsFor(r.selector))
	if err != nil {
		return profile.Profile{}, fmt.Errorf("get profile from graph: %w", err)
	}
	if err := requireQueryRow(result, "get profile"); err != nil {
		return profile.Profile{}, fmt.Errorf("%w: %s", profile.ErrProfileNotFound, err)
	}
	return profileFromRecord(result.Record()), nil
}

func profileParams(value profile.Profile) map[string]interface{} {
	return map[string]interface{}{
		"tenant_id":                     value.TenantID,
		"user_id":                       value.UserID,
		"display_name":                  value.DisplayName,
		"interests":                     value.Interests,
		"skills":                        value.Skills,
		"preferred_technologies":        value.PreferredTechnologies,
		"goals":                         value.Goals,
		"location_preferences":          value.LocationPreferences,
		"content_preferences_json":      jsonMap(value.ContentPreferences),
		"quiet_hours_start":             value.QuietHours.Start,
		"quiet_hours_end":               value.QuietHours.End,
		"quiet_hours_timezone":          value.QuietHours.Timezone,
		"notification_preferences_json": jsonMap(value.NotificationPreferences),
		"created_at":                    optionalMillis(value.CreatedAt),
		"updated_at":                    optionalMillis(value.UpdatedAt),
	}
}

// syncProfileRelationship keeps the bounded User properties for compatibility
// while materializing the graph relationships used by relevance queries.
func syncProfileRelationship(ctx context.Context, graph *dbclient.Graph, value profile.Profile, kind string, values []string, selector UserGraphSelector) error {
	var nodeLabel, relationship string
	switch kind {
	case "interest":
		nodeLabel, relationship = "Interest", "INTERESTED_IN"
	case "skill":
		nodeLabel, relationship = "Skill", "HAS_SKILL"
	case "technology":
		nodeLabel, relationship = "Technology", "PREFERS"
	default:
		return fmt.Errorf("unsupported profile relationship kind %q", kind)
	}
	valuesByID := make([]map[string]interface{}, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, raw := range values {
		name := strings.TrimSpace(raw)
		normalized := strings.ToLower(strings.Join(strings.Fields(name), " "))
		if normalized == "" {
			continue
		}
		id := stableID(strings.ToLower(kind), normalized)
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		valuesByID = append(valuesByID, map[string]interface{}{
			"id": id, "name": name, "normalized_name": normalized,
		})
	}
	query := fmt.Sprintf(`
		MATCH (u:User {id: $user_id, tenant_id: $tenant_id})
		OPTIONAL MATCH (u)-[old:%s]->(old_node:%s {tenant_id: $tenant_id, user_id: $user_id})
		DELETE old
		WITH u
		UNWIND $values AS value
		MERGE (node:%s {id: value.id, tenant_id: $tenant_id, user_id: $user_id})
		SET node.name = value.name, node.normalized_name = value.normalized_name,
		    node.updated_at = $updated_at
		MERGE (u)-[:%s]->(node)
		RETURN count(node) AS synced
	`, relationship, nodeLabel, nodeLabel, relationship)
	result, err := graph.Query(query, map[string]interface{}{
		"tenant_id": value.TenantID, "user_id": value.UserID,
		"values": valuesByID, "updated_at": optionalMillis(value.UpdatedAt),
	}, queryOptionsFor(selector))
	if err != nil {
		return fmt.Errorf("sync %s profile relationships: %w", kind, err)
	}
	if len(valuesByID) > 0 {
		if err := requireQueryRow(result, "sync profile relationships"); err != nil {
			return err
		}
	}
	return nil
}

func profileFromRecord(record recordReader) profile.Profile {
	return profile.Profile{
		TenantID:                stringValue(recordField(record, "tenant_id")),
		UserID:                  stringValue(recordField(record, "user_id")),
		DisplayName:             stringValue(recordField(record, "display_name")),
		Interests:               stringSliceValue(recordField(record, "interests")),
		Skills:                  stringSliceValue(recordField(record, "skills")),
		PreferredTechnologies:   stringSliceValue(recordField(record, "preferred_technologies")),
		Goals:                   stringSliceValue(recordField(record, "goals")),
		LocationPreferences:     stringSliceValue(recordField(record, "location_preferences")),
		ContentPreferences:      parseStringMap(recordField(record, "content_preferences_json")),
		QuietHours:              profile.QuietHours{Start: stringValue(recordField(record, "quiet_hours_start")), End: stringValue(recordField(record, "quiet_hours_end")), Timezone: stringValue(recordField(record, "quiet_hours_timezone"))},
		NotificationPreferences: parseStringMap(recordField(record, "notification_preferences_json")),
		CreatedAt:               timeValue(recordField(record, "created_at")),
		UpdatedAt:               timeValue(recordField(record, "updated_at")),
	}
}
