// Package untrusted wraps externally fetched content (web results, mail,
// calendar events, GitHub objects) so downstream code cannot mistake it for
// instructions, policy, or trusted application input. Every provider adapter
// must route fetched text through this boundary before it reaches a prompt,
// an event, or durable graph state.
package untrusted

import (
	"errors"
	"strings"
	"unicode/utf8"
)

// MaxText is the hard bound applied to any wrapped external text so a hostile
// or oversized response cannot inflate a prompt, event, or graph record.
const MaxText = 4096

// fence is deliberately unusual so wrapped content cannot forge its own
// boundary markers without containing the exact delimiter.
const fence = "[[UNTRUSTED-CONTENT]]"

var (
	ErrInvalidOrigin = errors.New("invalid untrusted content origin")
	ErrEmptyText     = errors.New("untrusted content text is empty")
)

// Origin identifies where external content came from. It is a bounded enum,
// never a free-form provider string.
type Origin string

const (
	Web      Origin = "web"
	Mail     Origin = "mail"
	Calendar Origin = "calendar"
	GitHub   Origin = "github"
)

func (o Origin) Valid() bool {
	switch o {
	case Web, Mail, Calendar, GitHub:
		return true
	default:
		return false
	}
}

// Content is a bounded, origin-tagged external payload. The zero value is
// invalid, which keeps callers from accidentally treating raw text as trusted.
type Content struct {
	Origin Origin
	Text   string
}

// New validates the origin and trims and bounds the text.
func New(origin Origin, text string) (Content, error) {
	if !origin.Valid() {
		return Content{}, ErrInvalidOrigin
	}
	value := bound(text, MaxText)
	if value == "" {
		return Content{}, ErrEmptyText
	}
	return Content{Origin: origin, Text: value}, nil
}

// Bounded returns the trimmed, length-bounded external text without markers.
func (c Content) Bounded() string {
	return bound(c.Text, MaxText)
}

// Prompt renders the content inside explicit untrusted markers. The originating
// system policy must treat this block as data, never as instructions.
func (c Content) Prompt() string {
	origin := c.Origin
	if !origin.Valid() {
		origin = "unknown"
	}
	return fence + " origin=" + string(origin) + "\n" + c.Bounded() + "\n" + fence
}

func bound(value string, limit int) string {
	trimmed := strings.TrimSpace(value)
	runes := []rune(trimmed)
	if len(runes) > limit {
		return string(runes[:limit])
	}
	return string(runes)
}

// Valid reports whether the value passed through New successfully.
func (c Content) Valid() bool {
	return c.Origin.Valid() && utf8.ValidString(c.Text) && strings.TrimSpace(c.Text) != ""
}
