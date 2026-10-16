package web

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed static/index.html
var staticFiles embed.FS

func Handler() http.Handler {
	content, err := fs.Sub(staticFiles, "static")
	if err != nil {
		// The embedded path is compile-time controlled; returning a handler that
		// reports the problem is safer than panicking during process startup.
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "embedded web assets unavailable", http.StatusInternalServerError)
		})
	}
	return http.FileServer(http.FS(content))
}
