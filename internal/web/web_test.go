package web

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandlerServesEmbeddedDashboardAtRoot(t *testing.T) {
	recording := httptest.NewRecorder()
	request := httptest.NewRequest("GET", "/", nil)
	Handler().ServeHTTP(recording, request)
	if recording.Code != 200 {
		t.Fatalf("status = %d, want 200", recording.Code)
	}
	body, err := io.ReadAll(recording.Result().Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if !strings.Contains(string(body), "NightShift") {
		t.Fatal("embedded dashboard did not contain NightShift")
	}
}
