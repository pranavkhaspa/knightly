package github

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/pranavkhaspa/knightly/internal/identity"
)

// ObjectType classifies a stored external object.
type ObjectType string

const (
	ObjectRepository  ObjectType = "repository"
	ObjectIssue       ObjectType = "issue"
	ObjectPullRequest ObjectType = "pull_request"
	ObjectCheck       ObjectType = "check"
	ObjectRelease     ObjectType = "release"
)

func (t ObjectType) valid() bool {
	switch t {
	case ObjectRepository, ObjectIssue, ObjectPullRequest, ObjectCheck, ObjectRelease:
		return true
	default:
		return false
	}
}

const ProviderGitHub = "github"

// ExternalObject is a bounded, scope-tagged reference to a GitHub object. It
// stores identifiers, links, commit SHAs, and bounded summaries only.
type ExternalObject struct {
	ID              string     `json:"id"`
	TenantID        string     `json:"tenant_id"`
	UserID          string     `json:"user_id"`
	Provider        string     `json:"provider"`
	ObjectType      ObjectType `json:"object_type"`
	ObjectID        string     `json:"object_id"`
	Repository      string     `json:"repository"`
	Title           string     `json:"title,omitempty"`
	Summary         string     `json:"summary,omitempty"`
	Link            string     `json:"link,omitempty"`
	SHA             string     `json:"sha,omitempty"`
	State           string     `json:"state,omitempty"`
	UntrustedOrigin string     `json:"untrusted_origin"`
	ObservedAt      time.Time  `json:"observed_at"`
}

// ObjectStore persists bounded external objects in the graph.
type ObjectStore interface {
	SaveExternalObjects(context.Context, identity.Scope, []ExternalObject) ([]ExternalObject, error)
	ListExternalObjects(context.Context, identity.Scope, ListObjectsQuery) ([]ExternalObject, error)
}

// ListObjectsQuery filters stored objects.
type ListObjectsQuery struct {
	Repository string
	ObjectType ObjectType
	Limit      int
}

// NewExternalObjectID derives a stable object identifier from scope and provider.
func NewExternalObjectID(scope identity.Scope, objectType ObjectType, repository, objectID string) string {
	sum := sha256.Sum256([]byte(scope.TenantID + "\x00" + scope.UserID + "\x00" + ProviderGitHub + "\x00" +
		string(objectType) + "\x00" + NormalizeRepository(repository) + "\x00" + objectID))
	return "ghobject_" + hex.EncodeToString(sum[:])[:24]
}

func (o ExternalObject) normalize(scope identity.Scope) (ExternalObject, error) {
	if !o.ObjectType.valid() {
		return ExternalObject{}, fmt.Errorf("%w: unknown object type %q", ErrInvalidObject, o.ObjectType)
	}
	o.Repository = NormalizeRepository(o.Repository)
	if o.Repository == "" || !strings.Contains(o.Repository, "/") {
		return ExternalObject{}, fmt.Errorf("%w: repository must be owner/name", ErrInvalidObject)
	}
	o.ObjectID = bound(o.ObjectID, MaxIDLength)
	if o.ObjectID == "" {
		return ExternalObject{}, fmt.Errorf("%w: object id is required", ErrInvalidObject)
	}
	o.TenantID = scope.TenantID
	o.UserID = scope.UserID
	o.Provider = ProviderGitHub
	o.Title = boundRun(o.Title, MaxTitle)
	o.Summary = bound(o.Summary, MaxSummary)
	o.Link = bound(o.Link, MaxURLLength)
	o.SHA = bound(o.SHA, MaxSHALength)
	o.State = bound(strings.ToLower(o.State), 32)
	o.UntrustedOrigin = UntrustedOrigin
	o.ID = NewExternalObjectID(scope, o.ObjectType, o.Repository, o.ObjectID)
	if o.ObservedAt.IsZero() {
		o.ObservedAt = time.Now().UTC()
	}
	return o, nil
}

// Service reads GitHub objects and persists bounded references to the graph.
type Service struct {
	reader Reader
	store  ObjectStore
	now    func() time.Time
}

func NewService(reader Reader, store ObjectStore) (*Service, error) {
	if reader == nil {
		return nil, fmt.Errorf("%w: reader is required", ErrInvalidObject)
	}
	if store == nil {
		return nil, fmt.Errorf("%w: object store is required", ErrInvalidObject)
	}
	return &Service{reader: reader, store: store, now: func() time.Time { return time.Now().UTC() }}, nil
}

func (s *Service) Repository(ctx context.Context, scope identity.Scope, fullName string) (Repository, error) {
	value, err := s.reader.GetRepository(ctx, scope, fullName)
	if err != nil {
		return Repository{}, err
	}
	object, err := (ExternalObject{
		ObjectType: ObjectRepository, ObjectID: value.FullName, Repository: value.FullName,
		Title: value.FullName, Summary: value.Description, Link: value.HTMLURL,
	}).normalize(scope)
	if err != nil {
		return Repository{}, err
	}
	if _, err := s.store.SaveExternalObjects(ctx, scope, []ExternalObject{object}); err != nil {
		return Repository{}, err
	}
	return value, nil
}

func (s *Service) Issues(ctx context.Context, scope identity.Scope, query ListQuery) ([]Issue, error) {
	values, err := s.reader.ListIssues(ctx, scope, query)
	if err != nil {
		return nil, err
	}
	objects := make([]ExternalObject, 0, len(values))
	for _, value := range values {
		objects = append(objects, ExternalObject{
			ObjectType: ObjectIssue, ObjectID: fmt.Sprintf("%d", value.ID), Repository: value.Repository,
			Title: value.Title, Summary: firstLine(value.Body), Link: value.HTMLURL, State: value.State,
		})
	}
	if _, err := s.persist(ctx, scope, objects); err != nil {
		return nil, err
	}
	return values, nil
}

func (s *Service) PullRequests(ctx context.Context, scope identity.Scope, query ListQuery) ([]PullRequest, error) {
	values, err := s.reader.ListPullRequests(ctx, scope, query)
	if err != nil {
		return nil, err
	}
	objects := make([]ExternalObject, 0, len(values))
	for _, value := range values {
		objects = append(objects, ExternalObject{
			ObjectType: ObjectPullRequest, ObjectID: fmt.Sprintf("%d", value.ID), Repository: value.Repository,
			Title: value.Title, Summary: firstLine(value.Body), Link: value.HTMLURL, State: value.State,
			SHA: value.Head,
		})
	}
	if _, err := s.persist(ctx, scope, objects); err != nil {
		return nil, err
	}
	return values, nil
}

func (s *Service) Checks(ctx context.Context, scope identity.Scope, query ListQuery) ([]CheckRun, error) {
	values, err := s.reader.ListChecks(ctx, scope, query)
	if err != nil {
		return nil, err
	}
	objects := make([]ExternalObject, 0, len(values))
	for _, value := range values {
		objects = append(objects, ExternalObject{
			ObjectType: ObjectCheck, ObjectID: fmt.Sprintf("%d", value.ID), Repository: value.Repository,
			Title: value.Name, Summary: value.Conclusion, Link: value.HTMLURL, State: value.Status, SHA: value.HeadSHA,
		})
	}
	if _, err := s.persist(ctx, scope, objects); err != nil {
		return nil, err
	}
	return values, nil
}

func (s *Service) Releases(ctx context.Context, scope identity.Scope, query ListQuery) ([]Release, error) {
	values, err := s.reader.ListReleases(ctx, scope, query)
	if err != nil {
		return nil, err
	}
	objects := make([]ExternalObject, 0, len(values))
	for _, value := range values {
		objects = append(objects, ExternalObject{
			ObjectType: ObjectRelease, ObjectID: fmt.Sprintf("%d", value.ID), Repository: value.Repository,
			Title: value.Name, Summary: value.TagName, Link: value.HTMLURL, State: value.TagName,
		})
	}
	if _, err := s.persist(ctx, scope, objects); err != nil {
		return nil, err
	}
	return values, nil
}

func (s *Service) Objects(ctx context.Context, scope identity.Scope, query ListObjectsQuery) ([]ExternalObject, error) {
	if err := scope.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrScopeRequired, err)
	}
	if query.Limit < 1 || query.Limit > MaxListLimit {
		query.Limit = DefaultListLimit
	}
	if query.ObjectType != "" && !query.ObjectType.valid() {
		return nil, fmt.Errorf("%w: unknown object type %q", ErrInvalidObject, query.ObjectType)
	}
	return s.store.ListExternalObjects(ctx, scope, query)
}

func (s *Service) persist(ctx context.Context, scope identity.Scope, objects []ExternalObject) ([]ExternalObject, error) {
	if len(objects) == 0 {
		return nil, nil
	}
	now := s.now().UTC()
	normalized := make([]ExternalObject, 0, len(objects))
	for _, object := range objects {
		object.ObservedAt = now
		value, err := object.normalize(scope)
		if err != nil {
			return nil, err
		}
		normalized = append(normalized, value)
	}
	return s.store.SaveExternalObjects(ctx, scope, normalized)
}

func firstLine(value string) string {
	value = strings.TrimSpace(value)
	if index := strings.IndexAny(value, "\r\n"); index >= 0 {
		value = value[:index]
	}
	return bound(value, MaxSummary)
}
