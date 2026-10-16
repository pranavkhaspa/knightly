package web

import (
	"embed"
	"net/http"
)

//go:embed static/index.html
var staticFiles embed.FS

func Handler() http.Handler {
	return http.FileServer(http.FS(staticFiles))
}
