package validation

import (
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
)

func TestNormalizeSchemaTypesWithNullUnion(t *testing.T) {
	types := openapi3.Types{openapi3.TypeString, openapi3.TypeNull}
	schema := &openapi3.Schema{Type: &types}

	normalizeSchema(schema, make(map[*openapi3.Schema]struct{}))

	if !schema.Nullable {
		t.Fatal("expected schema to be nullable")
	}
	if schema.Type == nil {
		t.Fatalf("expected schema type to remain, got nil")
	}
	got := schema.Type.Slice()
	if len(got) != 1 || got[0] != openapi3.TypeString {
		t.Fatalf("expected remaining type to be %q, got %v", openapi3.TypeString, got)
	}
	if schema.Enum != nil {
		t.Fatalf("expected enum to remain nil, got %v", schema.Enum)
	}
}

func TestNormalizeSchemaTypesOnlyNull(t *testing.T) {
	types := openapi3.Types{openapi3.TypeNull}
	schema := &openapi3.Schema{Type: &types}

	normalizeSchema(schema, make(map[*openapi3.Schema]struct{}))

	if !schema.Nullable {
		t.Fatal("expected schema to be nullable")
	}
	if schema.Type != nil {
		t.Fatalf("expected schema type to be nil, got %v", schema.Type.Slice())
	}
	if len(schema.Enum) != 1 || schema.Enum[0] != nil {
		t.Fatalf("expected enum to restrict to nil, got %v", schema.Enum)
	}
}

func TestNormalizeSchemaConst(t *testing.T) {
	schema := &openapi3.Schema{Extensions: map[string]any{"const": "users"}}

	normalizeSchema(schema, make(map[*openapi3.Schema]struct{}))

	if len(schema.Enum) != 1 || schema.Enum[0] != "users" {
		t.Fatalf("expected enum to contain const value, got %v", schema.Enum)
	}
	if schema.Extensions != nil {
		t.Fatalf("expected const extension to be removed, got %v", schema.Extensions)
	}
}

func TestNormalizeSchemaContentEncoding(t *testing.T) {
	schema := &openapi3.Schema{Extensions: map[string]any{"contentEncoding": "base64"}}

	normalizeSchema(schema, make(map[*openapi3.Schema]struct{}))

	if schema.Format != "byte" {
		t.Fatalf("expected format to be byte, got %q", schema.Format)
	}
	if schema.Extensions != nil {
		t.Fatalf("expected contentEncoding extension to be removed, got %v", schema.Extensions)
	}
}

func TestNormalizeLicenseIdentifier(t *testing.T) {
	doc := &openapi3.T{
		Info: &openapi3.Info{
			Title:   "test",
			Version: "1",
			License: &openapi3.License{
				Name:       "MIT",
				Extensions: map[string]any{"identifier": "MIT"},
			},
		},
	}

	normalizeOpenAPISpec(doc)

	if doc.Info.License.Extensions != nil {
		t.Fatalf("expected license identifier to be dropped, got %v", doc.Info.License.Extensions)
	}
}

func TestNormalizeServersAddsRelativeEntry(t *testing.T) {
	doc := &openapi3.T{
		Info:    &openapi3.Info{Title: "test", Version: "1"},
		Servers: openapi3.Servers{{URL: "https://discord.com/api/v10"}},
	}

	normalizeOpenAPISpec(doc)

	if len(doc.Servers) < 2 {
		t.Fatalf("expected normalized servers to include relative entries, got %v", doc.Servers)
	}
	if doc.Servers[0].URL != "/api/v10" {
		t.Fatalf("expected first server to be relative /api/v10, got %s", doc.Servers[0].URL)
	}
	foundRoot := false
	for _, srv := range doc.Servers {
		if srv.URL == "/" {
			foundRoot = true
			break
		}
	}
	if !foundRoot {
		t.Fatalf("expected normalized servers to include root entry, got %v", doc.Servers)
	}
}
