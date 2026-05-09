package validation

import "github.com/getkin/kin-openapi/openapi3"

func normalize(doc *openapi3.T) {
	if doc == nil {
		return
	}
	if doc.Info != nil {
		doc.Info.Extensions = map[string]any{}
	}

	if doc.Components == nil || doc.Components.Schemas == nil {
		return
	}
	seen := make(map[*openapi3.Schema]struct{})
	for _, schemaRef := range doc.Components.Schemas {
		normalizeSchema(schemaRef, seen)
	}
	if doc.Paths != nil {
		for _, path := range doc.Paths.Map() {
			normalizePath(path, seen)
		}
	}
}

func normalizePath(path *openapi3.PathItem, seen map[*openapi3.Schema]struct{}) {
	if path == nil {
		return
	}
	for _, param := range path.Parameters {
		normalizeParameter(param, seen)
	}
	for _, op := range path.Operations() {
		normalizeOperation(op, seen)
	}
}

func normalizeOperation(op *openapi3.Operation, seen map[*openapi3.Schema]struct{}) {
	if op == nil {
		return
	}
	for _, param := range op.Parameters {
		normalizeParameter(param, seen)
	}
	if op.RequestBody != nil && op.RequestBody.Value != nil {
		normalizeContent(op.RequestBody.Value.Content, seen)
	}
	if op.Responses != nil {
		for _, resp := range op.Responses.Map() {
			if resp != nil && resp.Value != nil {
				normalizeContent(resp.Value.Content, seen)
			}
		}
	}
}

func normalizeParameter(param *openapi3.ParameterRef, seen map[*openapi3.Schema]struct{}) {
	if param == nil || param.Value == nil {
		return
	}
	normalizeSchema(param.Value.Schema, seen)
	normalizeContent(param.Value.Content, seen)
}

func normalizeContent(content openapi3.Content, seen map[*openapi3.Schema]struct{}) {
	for _, media := range content {
		if media != nil {
			normalizeSchema(media.Schema, seen)
		}
	}
}

func normalizeSchema(ref *openapi3.SchemaRef, seen map[*openapi3.Schema]struct{}) {
	if ref == nil || ref.Value == nil {
		return
	}
	schema := ref.Value
	if _, ok := seen[schema]; ok {
		return
	}
	seen[schema] = struct{}{}

	delete(schema.Extensions, "const")
	delete(schema.Extensions, "contentEncoding")
	delete(schema.Extensions, "contentMediaType")
	delete(schema.Extensions, "x-discord-union")
	if schema.Type != nil && schema.Type.Includes("null") {
		schema.Nullable = true
		types := make([]string, 0, len(schema.Type.Slice()))
		for _, typ := range schema.Type.Slice() {
			if typ != "null" {
				types = append(types, typ)
			}
		}
		if len(types) == 0 {
			schema.Type = nil
		} else {
			normalized := openapi3.Types(types)
			schema.Type = &normalized
		}
	}
	for _, item := range schema.OneOf {
		normalizeSchema(item, seen)
	}
	for _, item := range schema.AnyOf {
		normalizeSchema(item, seen)
	}
	for _, item := range schema.AllOf {
		normalizeSchema(item, seen)
	}
	if schema.Items != nil {
		normalizeSchema(schema.Items, seen)
	}
	for _, prop := range schema.Properties {
		normalizeSchema(prop, seen)
	}
	if schema.AdditionalProperties.Schema != nil {
		normalizeSchema(schema.AdditionalProperties.Schema, seen)
	}
}
