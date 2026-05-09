package discord

import _ "embed"

// OpenAPI is Discord's standard public OpenAPI document.
//
//go:embed openapi.json
var OpenAPI []byte
