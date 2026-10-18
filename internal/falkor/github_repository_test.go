package falkor

import (
	"strings"
	"testing"
	"time"

	"github.com/pranavkhaspa/knightly/internal/github"
	"github.com/pranavkhaspa/knightly/internal/identity"
)

func TestExternalObjectRecordMapping(t *testing.T) {
	value := externalObjectFromRecord(sourceRecord{
		"id": "ghobject-1", "tenant_id": "tenant-a", "user_id": "user-a", "provider": "github",
		"object_type": "issue", "object_id": "42", "repository": "owner/repo", "title": "Bug",
		"summary": "details", "link": "https://example/42", "sha": "abc", "state": "open",
		"observed_at": int64(1_700_000_000_000),
	})
	if value.ObjectType != github.ObjectIssue || value.Repository != "owner/repo" || value.ObjectID != "42" {
		t.Fatalf("value = %+v", value)
	}
	if value.UntrustedOrigin != github.UntrustedOrigin {
		t.Fatalf("origin = %q, want %q", value.UntrustedOrigin, github.UntrustedOrigin)
	}
	if !value.ObservedAt.Equal(time.UnixMilli(1_700_000_000_000).UTC()) {
		t.Fatalf("observed_at = %v", value.ObservedAt)
	}
}

func TestGitHubObjectIDIsScopeStable(t *testing.T) {
	scope := identity.Scope{TenantID: "tenant-a", UserID: "user-a"}
	first := github.NewExternalObjectID(scope, github.ObjectIssue, "Owner/Repo", "42")
	second := github.NewExternalObjectID(scope, github.ObjectIssue, "owner/repo", "42")
	if first != second {
		t.Fatalf("object id is not normalized: %q vs %q", first, second)
	}
	other := github.NewExternalObjectID(identity.Scope{TenantID: "tenant-b", UserID: "user-a"}, github.ObjectIssue, "owner/repo", "42")
	if other == first {
		t.Fatal("object id must differ across scopes")
	}
}

func TestGitHubObjectQueriesKeepStateInFalkorDB(t *testing.T) {
	combined := GitHubObjectSaveQuery + GitHubObjectListQuery
	for _, fragment := range []string{
		"ExternalObject", "Repository", "ProviderAccount", "HAS_OBJECT", "IN_REPOSITORY",
		"tenant_id", "user_id", "object_type", "sha", "untrusted_origin",
	} {
		if !strings.Contains(combined, fragment) {
			t.Errorf("GitHub object queries missing %q", fragment)
		}
	}
	if !strings.Contains(GitHubObjectSaveQuery, "MERGE") {
		t.Error("save query must be idempotent")
	}
}
