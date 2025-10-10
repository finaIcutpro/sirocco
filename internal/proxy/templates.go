package proxy

import (
	"embed"
	"html/template"
)

//go:embed dashboard.html
var dashboardFS embed.FS

var dashboardTemplate = template.Must(template.New("dashboard").ParseFS(dashboardFS, "dashboard.html"))
