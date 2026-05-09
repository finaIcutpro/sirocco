#!/usr/bin/env sh
set -eu

url="https://raw.githubusercontent.com/discord/discord-api-spec/main/specs/openapi.json"
dst="api/discord/openapi.json"
tmp="${dst}.tmp"

mkdir -p "$(dirname "$dst")"
curl -fsSL "$url" -o "$tmp"
go run ./internal/tools/jsoncheck "$tmp"
mv "$tmp" "$dst"
go test ./internal/validation

printf '%s\n' "updated $dst from $url"