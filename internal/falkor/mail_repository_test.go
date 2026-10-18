package falkor

import (
	"strings"
	"testing"
	"time"

	"github.com/pranavkhaspa/knightly/internal/mail"
)

func TestMailAccountRecordMapping(t *testing.T) {
	account := mailAccountFromRecord(sourceRecord{
		"id":            "mail_account_1",
		"tenant_id":     "tenant-a",
		"user_id":       "user-a",
		"provider":      "gmail",
		"email_address": "user@example.com",
		"scopes":        []interface{}{"https://www.googleapis.com/auth/gmail.readonly"},
		"status":        "connected",
		"connected_at":  int64(1_700_000_000_000),
		"updated_at":    int64(1_700_000_000_000),
	})
	if account.ID != "mail_account_1" || account.Provider != "gmail" || account.Status != mail.AccountConnected {
		t.Fatalf("account = %+v", account)
	}
	if len(account.Scopes) != 1 || !strings.Contains(account.Scopes[0], "gmail.readonly") {
		t.Fatalf("scopes = %+v", account.Scopes)
	}
	if account.ConnectedAt.IsZero() {
		t.Fatal("connected_at was not parsed")
	}
}

func TestMailMessageRecordMapping(t *testing.T) {
	message := mailMessageFromRecord(sourceRecord{
		"id":               "m1",
		"account_id":       "mail_account_1",
		"thread_id":        "t1",
		"from_name":        "Ops",
		"from_email":       "ops@example.com",
		"subject":          "Deploy",
		"snippet":          "build failed",
		"body_excerpt":     "details",
		"received_at":      int64(1_700_000_000_000),
		"labels":           []interface{}{"UNREAD", "INBOX"},
		"has_attachments":  true,
		"attachments_json": `[{"filename":"a.pdf","mime_type":"application/pdf","size":10}]`,
		"unread":           true,
		"untrusted_origin": "mail",
	})
	if message.ID != "m1" || message.From.Email != "ops@example.com" || !message.HasAttachments || !message.Unread {
		t.Fatalf("message = %+v", message)
	}
	if len(message.Attachments) != 1 || message.Attachments[0].Filename != "a.pdf" {
		t.Fatalf("attachments = %+v", message.Attachments)
	}
	if message.Origin != mail.UntrustedOrigin {
		t.Fatalf("origin = %q", message.Origin)
	}
	if !message.ReceivedAt.Equal(time.UnixMilli(1_700_000_000_000).UTC()) {
		t.Fatalf("received_at = %v", message.ReceivedAt)
	}
}

func TestMailRepositoryQueriesKeepStateInFalkorDB(t *testing.T) {
	combined := MailAccountListQuery + MailAccountGetQuery + MailAccountSaveQuery +
		MailMessageSaveQuery + MailThreadLinkQuery + MailMessageListQuery + MailMessageGetQuery
	for _, fragment := range []string{
		"ProviderAccount", "MailMessage", "MailThread", "HAS_MESSAGE", "IN_THREAD",
		"tenant_id", "user_id", "content_hash", "attachments_json", "untrusted_origin",
	} {
		if !strings.Contains(combined, fragment) {
			t.Errorf("mail repository queries missing %q", fragment)
		}
	}
	for name, query := range map[string]string{"thread link": MailThreadLinkQuery} {
		if !strings.Contains(query, "MERGE") {
			t.Errorf("%s query is not idempotent", name)
		}
	}
}

func TestMailAttachmentEncodingRoundTrip(t *testing.T) {
	attachments := []mail.Attachment{{ID: "a1", Filename: "report.pdf", MIMEType: "application/pdf", Size: 2048}}
	decoded := decodeAttachments(encodeAttachments(attachments))
	if len(decoded) != 1 || decoded[0].Filename != "report.pdf" || decoded[0].Size != 2048 {
		t.Fatalf("decoded = %+v", decoded)
	}
	if decodeAttachments("") != nil {
		t.Error("empty attachment JSON should decode to nil")
	}
}

func TestMailContentHashTracksBodyChanges(t *testing.T) {
	base := mail.Message{ID: "m1", From: mail.Address{Email: "ops@example.com"}, Subject: "Deploy", Snippet: "one"}
	if mailContentHash(base) == mailContentHash(mail.Message{ID: "m1", From: base.From, Subject: "Deploy", Snippet: "two"}) {
		t.Error("content hash did not change with the snippet")
	}
}
