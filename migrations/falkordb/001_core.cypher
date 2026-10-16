// NightShift foundation schema.
// FalkorDB does not require a separate relational migration for this model.
// All statements are idempotent and are executed through the graph repository.

// Control graph
MERGE (schema:Schema {id: "nightshift"})
SET schema.version = "0.1-foundation", schema.updated_at = timestamp();

// User graph seed used by the deterministic demo.
MERGE (u:User {id: "user_demo"})
SET u.tenant_id = "tenant_demo", u.display_name = "Demo Builder";
MERGE (p:Project {id: "project_nightshift"})
SET p.tenant_id = "tenant_demo", p.user_id = "user_demo",
    p.name = "NightShift", p.status = "active";
MERGE (u)-[:OWNS]->(p);
MERGE (interest:Interest {id: "interest_graph_agents"})
SET interest.name = "Graph-powered agents", interest.normalized_name = "graph agents";
MERGE (u)-[:INTERESTED_IN]->(interest);
MERGE (requirement:Requirement {id: "requirement_graph_context"})
SET requirement.tenant_id = "tenant_demo", requirement.user_id = "user_demo",
    requirement.title = "Use connected context for explainable recommendations",
    requirement.status = "open", requirement.priority = 1;
MERGE (p)-[:HAS_REQUIREMENT]->(requirement);
MERGE (technology:Technology {id: "technology_falkordb"})
SET technology.tenant_id = "tenant_demo", technology.user_id = "user_demo",
    technology.name = "FalkorDB", technology.normalized_name = "falkordb",
    technology.category = "graph database";
MERGE (requirement)-[:NEEDS]->(technology);
