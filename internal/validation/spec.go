package validation

import _ "embed"

// discordOpenAPISpec holds the Discord REST API OpenAPI document.
//
//go:embed discord_openapi.json
var discordOpenAPISpec []byte
