package falkor

import (
	"context"
	"fmt"
)

// Dashboard is the small, graph-backed read model used by the embedded UI.
// It is intentionally assembled from FalkorDB on every request instead of
// being cached in a second local store.
type Dashboard struct {
	Project   ProjectSummary  `json:"project"`
	GraphPath []GraphPathNode `json:"graph_path"`
	Tasks     []TaskStatus    `json:"tasks"`
}

type ProjectSummary struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Status string `json:"status"`
}

type GraphPathNode struct {
	Type   string `json:"type"`
	ID     string `json:"id"`
	Name   string `json:"name"`
	Detail string `json:"detail,omitempty"`
}

type TaskStatus struct {
	ID        string `json:"id"`
	Title     string `json:"title"`
	Agent     string `json:"agent,omitempty"`
	Status    string `json:"status"`
	Priority  int64  `json:"priority,omitempty"`
	UpdatedAt int64  `json:"updated_at,omitempty"`
}

// Dashboard reads the seeded project path and any persisted agent tasks. A
// missing task is a valid empty state; a missing graph path is returned as an
// empty path so the UI can explain that no evidence is connected yet.
func (c *Client) Dashboard(ctx context.Context) (Dashboard, error) {
	dashboard := Dashboard{
		GraphPath: make([]GraphPathNode, 0),
		Tasks:     make([]TaskStatus, 0),
	}

	if err := ctx.Err(); err != nil {
		return dashboard, err
	}

	pathResult, err := c.user.ROQuery(`
		MATCH (p:Project)-[:HAS_REQUIREMENT]->(r:Requirement)-[:NEEDS]->(tech:Technology)
		WHERE p.tenant_id IS NOT NULL AND p.user_id IS NOT NULL
		  AND r.tenant_id = p.tenant_id AND r.user_id = p.user_id
		  AND tech.tenant_id = p.tenant_id AND tech.user_id = p.user_id
		RETURN p.id AS project_id, p.name AS project_name, p.status AS project_status,
		       r.id AS requirement_id, r.title AS requirement_title,
		       tech.id AS technology_id, tech.name AS technology_name,
		       tech.category AS technology_category
		ORDER BY coalesce(r.priority, 999999), p.id
		LIMIT 1
	`, nil, c.options())
	if err != nil {
		return dashboard, fmt.Errorf("read graph path: %w", err)
	}
	if pathResult.Next() {
		record := pathResult.Record()
		dashboard.Project = ProjectSummary{
			ID:     stringValue(recordValue(record, "project_id")),
			Name:   stringValue(recordValue(record, "project_name")),
			Status: stringValue(recordValue(record, "project_status")),
		}
		dashboard.GraphPath = []GraphPathNode{
			{Type: "project", ID: dashboard.Project.ID, Name: dashboard.Project.Name},
			{
				Type: "requirement",
				ID:   stringValue(recordValue(record, "requirement_id")),
				Name: stringValue(recordValue(record, "requirement_title")),
			},
			{
				Type:   "technology",
				ID:     stringValue(recordValue(record, "technology_id")),
				Name:   stringValue(recordValue(record, "technology_name")),
				Detail: stringValue(recordValue(record, "technology_category")),
			},
		}
	} else {
		projectResult, projectErr := c.user.ROQuery(`
			MATCH (p:Project)
			WHERE p.tenant_id IS NOT NULL AND p.user_id IS NOT NULL
			RETURN p.id AS project_id, p.name AS project_name, p.status AS project_status
			ORDER BY p.id
			LIMIT 1
		`, nil, c.options())
		if projectErr != nil {
			return dashboard, fmt.Errorf("read project: %w", projectErr)
		}
		if projectResult.Next() {
			record := projectResult.Record()
			dashboard.Project = ProjectSummary{
				ID:     stringValue(recordValue(record, "project_id")),
				Name:   stringValue(recordValue(record, "project_name")),
				Status: stringValue(recordValue(record, "project_status")),
			}
		}
	}

	if err := ctx.Err(); err != nil {
		return dashboard, err
	}
	taskResult, err := c.user.ROQuery(`
		MATCH (t:Task)
		WHERE t.tenant_id IS NOT NULL AND t.user_id IS NOT NULL
		RETURN t.id AS task_id, t.title AS task_title, t.agent_type AS task_agent,
		       t.status AS task_status, t.priority AS task_priority,
		       t.updated_at AS task_updated_at
		ORDER BY coalesce(t.updated_at, 0) DESC, t.id
		LIMIT 20
	`, nil, c.options())
	if err != nil {
		return dashboard, fmt.Errorf("read tasks: %w", err)
	}
	for taskResult.Next() {
		record := taskResult.Record()
		dashboard.Tasks = append(dashboard.Tasks, TaskStatus{
			ID:        stringValue(recordValue(record, "task_id")),
			Title:     stringValue(recordValue(record, "task_title")),
			Agent:     stringValue(recordValue(record, "task_agent")),
			Status:    stringValue(recordValue(record, "task_status")),
			Priority:  int64Value(recordValue(record, "task_priority")),
			UpdatedAt: int64Value(recordValue(record, "task_updated_at")),
		})
	}
	return dashboard, nil
}
