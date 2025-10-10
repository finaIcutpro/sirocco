package proxy

import (
	"embed"
	"html/template"
)

//go:embed dashboard.html
var dashboardFS embed.FS

var dashboardTemplate *template.Template

func init() {
	data, err := dashboardFS.ReadFile("dashboard.html")
	if err != nil {
		panic(err)
	}
	dashboardTemplate, err = template.New("dashboard").Parse(string(data))
	if err != nil {
		panic(err)
	}
}
