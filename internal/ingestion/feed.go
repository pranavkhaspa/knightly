package ingestion

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"
)

const (
	SourceKindFixture       = "fixture"
	SourceKindRSS           = "rss"
	SourceKindAtom          = "atom"
	SourceKindGitHub        = "github"
	SourceKindGitHubRelease = "github_release"

	defaultMaxItems       = 50
	defaultMaxTitleBytes  = 512
	defaultMaxSummarySize = 8192
	defaultMaxTerms       = 32
	hardMaxItems          = 100
	hardMaxTitleBytes     = 2048
	hardMaxSummaryBytes   = 32768
	hardMaxTerms          = 64
)

// Limits bounds all feed data retained by a single source run. Zero values use
// safe defaults; values above the hard caps are reduced rather than allowing an
// accidentally unbounded source to consume memory.
type Limits struct {
	MaxItems       int
	MaxTitleBytes  int
	MaxSummarySize int
	MaxTerms       int
}

func DefaultLimits() Limits {
	return Limits{
		MaxItems:       defaultMaxItems,
		MaxTitleBytes:  defaultMaxTitleBytes,
		MaxSummarySize: defaultMaxSummarySize,
		MaxTerms:       defaultMaxTerms,
	}
}

func (l Limits) bounded() Limits {
	defaults := DefaultLimits()
	if l.MaxItems <= 0 {
		l.MaxItems = defaults.MaxItems
	}
	if l.MaxTitleBytes <= 0 {
		l.MaxTitleBytes = defaults.MaxTitleBytes
	}
	if l.MaxSummarySize <= 0 {
		l.MaxSummarySize = defaults.MaxSummarySize
	}
	if l.MaxTerms <= 0 {
		l.MaxTerms = defaults.MaxTerms
	}
	if l.MaxItems > hardMaxItems {
		l.MaxItems = hardMaxItems
	}
	if l.MaxTitleBytes > hardMaxTitleBytes {
		l.MaxTitleBytes = hardMaxTitleBytes
	}
	if l.MaxSummarySize > hardMaxSummaryBytes {
		l.MaxSummarySize = hardMaxSummaryBytes
	}
	if l.MaxTerms > hardMaxTerms {
		l.MaxTerms = hardMaxTerms
	}
	return l
}

// SourceConfig describes the approved public source that owns normalized items.
// The ID is stable across runs and is written to FalkorDB with every item.
type SourceConfig struct {
	ID              string
	TenantID        string
	UserID          string
	Name            string
	Kind            string
	URL             string
	Enabled         bool
	TrustScore      float64
	PollInterval    time.Duration
	MaxItems        int
	TechnologyHints []string
	InterestHints   []string
}

// RawItem is the small, structured boundary between a source adapter and the
// normalizer. Source adapters must not pass raw response bodies through this
// boundary.
type RawItem struct {
	ExternalID   string
	URL          string
	Title        string
	Summary      string
	PublishedAt  time.Time
	Technologies []string
	Interests    []string
}

// Item is the bounded, canonical representation persisted in FalkorDB.
type Item struct {
	ID           string
	TenantID     string
	UserID       string
	SourceID     string
	CanonicalURL string
	Title        string
	Summary      string
	PublishedAt  time.Time
	ContentHash  string
	FetchedAt    time.Time
	Technologies []string
	Interests    []string
}

type NormalizationStats struct {
	Seen             int
	Accepted         int
	DroppedInvalid   int
	DroppedDuplicate int
}

// Normalizer canonicalizes and deduplicates a bounded feed without making any
// recommendation decision. Invalid individual entries are skipped so one bad
// public item does not discard the rest of a source run.
type Normalizer struct {
	Limits Limits
}

func NewNormalizer(limits Limits) Normalizer {
	return Normalizer{Limits: limits.bounded()}
}

func (n Normalizer) limits() Limits {
	return n.Limits.bounded()
}

func (n Normalizer) NormalizeSource(source SourceConfig) (SourceConfig, error) {
	source.Name = normalizeText(source.Name)
	source.Kind = normalizeText(source.Kind)
	if source.Kind == "" {
		source.Kind = SourceKindRSS
	}
	if source.PollInterval <= 0 {
		source.PollInterval = DefaultSourcePollInterval
	}
	if source.MaxItems <= 0 {
		source.MaxItems = DefaultSourceMaxItems
	}
	if source.MaxItems > hardMaxItems {
		source.MaxItems = hardMaxItems
	}
	if source.Name == "" {
		return SourceConfig{}, errors.New("source name is required")
	}

	canonicalURL, err := CanonicalizeURL(source.URL)
	if err != nil {
		return SourceConfig{}, fmt.Errorf("canonicalize source URL: %w", err)
	}
	source.URL = canonicalURL
	if strings.TrimSpace(source.ID) == "" {
		source.ID = StableID("source", canonicalURL)
	} else {
		source.ID = normalizeText(source.ID)
	}
	return source, nil
}

func (n Normalizer) Normalize(source SourceConfig, raw []RawItem, fetchedAt time.Time) ([]Item, NormalizationStats, error) {
	source, err := n.NormalizeSource(source)
	if err != nil {
		return nil, NormalizationStats{}, err
	}
	if fetchedAt.IsZero() {
		fetchedAt = time.Now().UTC()
	}
	fetchedAt = fetchedAt.UTC()

	limits := n.limits()
	stats := NormalizationStats{Seen: min(len(raw), limits.MaxItems)}
	items := make([]Item, 0, stats.Seen)
	seenURLs := make(map[string]struct{}, stats.Seen)
	seenHashes := make(map[string]struct{}, stats.Seen)

	for _, candidate := range raw[:stats.Seen] {
		title := truncate(normalizeText(candidate.Title), limits.MaxTitleBytes)
		summary := truncate(normalizeText(candidate.Summary), limits.MaxSummarySize)
		canonicalURL, urlErr := CanonicalizeURL(candidate.URL)
		if title == "" || urlErr != nil {
			stats.DroppedInvalid++
			continue
		}

		contentHash := contentHash(canonicalURL, title, summary)
		if _, exists := seenURLs[canonicalURL]; exists {
			stats.DroppedDuplicate++
			continue
		}
		if _, exists := seenHashes[contentHash]; exists {
			stats.DroppedDuplicate++
			continue
		}
		seenURLs[canonicalURL] = struct{}{}
		seenHashes[contentHash] = struct{}{}

		items = append(items, Item{
			ID:           StableID("item", canonicalURL),
			TenantID:     source.TenantID,
			UserID:       source.UserID,
			SourceID:     source.ID,
			CanonicalURL: canonicalURL,
			Title:        title,
			Summary:      summary,
			PublishedAt:  candidate.PublishedAt.UTC(),
			ContentHash:  contentHash,
			FetchedAt:    fetchedAt,
			Technologies: normalizeTerms(candidate.Technologies, limits.MaxTerms),
			Interests:    normalizeTerms(candidate.Interests, limits.MaxTerms),
		})
	}

	stats.Accepted = len(items)
	return items, stats, nil
}

// CanonicalizeURL removes only known tracking parameters and presentation
// differences. Meaningful query parameters remain part of the item identity.
func CanonicalizeURL(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", errors.New("URL is required")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("parse URL: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", fmt.Errorf("unsupported URL scheme %q", u.Scheme)
	}
	if u.Host == "" {
		return "", errors.New("URL host is required")
	}
	if u.User != nil {
		return "", errors.New("URL credentials are not allowed")
	}

	u.Scheme = strings.ToLower(u.Scheme)
	host := strings.ToLower(u.Hostname())
	port := u.Port()
	if port != "" && !((u.Scheme == "http" && port == "80") || (u.Scheme == "https" && port == "443")) {
		host = host + ":" + port
	}
	u.Host = host
	u.Fragment = ""
	if u.Path == "" {
		u.Path = "/"
	} else {
		u.Path = strings.TrimRight(u.Path, "/")
		if u.Path == "" {
			u.Path = "/"
		}
	}

	query := u.Query()
	for key := range query {
		if isTrackingParameter(key) {
			query.Del(key)
		}
	}
	u.RawQuery = query.Encode()
	return u.String(), nil
}

func StableID(kind, value string) string {
	sum := sha256.Sum256([]byte(value))
	return kind + "_" + hex.EncodeToString(sum[:])
}

func contentHash(canonicalURL, title, summary string) string {
	return StableID("content", canonicalURL+"\x00"+title+"\x00"+summary)
}

func normalizeText(value string) string {
	return strings.Join(strings.Fields(value), " ")
}

func normalizeTerms(values []string, limit int) []string {
	terms := make([]string, 0, min(len(values), limit))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		term := strings.ToLower(normalizeText(value))
		if term == "" {
			continue
		}
		if _, exists := seen[term]; exists {
			continue
		}
		seen[term] = struct{}{}
		terms = append(terms, term)
		if len(terms) == limit {
			break
		}
	}
	sort.Strings(terms)
	return terms
}

func truncate(value string, maxBytes int) string {
	if len(value) <= maxBytes {
		return value
	}
	value = value[:maxBytes]
	for len(value) > 0 && (value[len(value)-1]&0xc0) == 0x80 {
		value = value[:len(value)-1]
	}
	if len(value) > 0 && (value[len(value)-1]&0xc0) == 0xc0 {
		value = value[:len(value)-1]
	}
	return strings.TrimSpace(value)
}

func isTrackingParameter(key string) bool {
	key = strings.ToLower(key)
	return strings.HasPrefix(key, "utm_") || key == "fbclid" || key == "gclid" || key == "mc_cid" || key == "mc_eid"
}

func min(left, right int) int {
	if left < right {
		return left
	}
	return right
}
