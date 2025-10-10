package validation

import (
	"net/url"
	"strings"

	"github.com/getkin/kin-openapi/openapi3"
)

// normalizeOpenAPISpec patches OpenAPI 3.1 nullable constructs so kin-openapi can validate them.
func normalizeOpenAPISpec(doc *openapi3.T) {
	if doc == nil {
		return
	}
	seen := make(map[*openapi3.Schema]struct{})

	if doc.Components != nil {
		normalizeComponents(doc.Components, seen)
	}
	normalizeInfo(doc.Info)
	doc.Servers = normalizeServers(doc.Servers)
	if doc.Paths != nil {
		for _, pathItem := range doc.Paths.Map() {
			normalizePathItem(pathItem, seen)
		}
	}
}

func dropExtension(m map[string]any, key string) map[string]any {
	if len(m) == 0 {
		return m
	}
	if _, ok := m[key]; !ok {
		return m
	}
	delete(m, key)
	if len(m) == 0 {
		return nil
	}
	return m
}

func normalizeComponents(components *openapi3.Components, seen map[*openapi3.Schema]struct{}) {
	for _, schemaRef := range components.Schemas {
		normalizeSchemaRef(schemaRef, seen)
	}
	for _, parameterRef := range components.Parameters {
		normalizeParameterRef(parameterRef, seen)
	}
	for _, headerRef := range components.Headers {
		normalizeHeaderRef(headerRef, seen)
	}
	for _, requestBodyRef := range components.RequestBodies {
		normalizeRequestBodyRef(requestBodyRef, seen)
	}
	for _, responseRef := range components.Responses {
		normalizeResponseRef(responseRef, seen)
	}
	for _, callbackRef := range components.Callbacks {
		normalizeCallbackRef(callbackRef, seen)
	}
}

func normalizePathItem(pathItem *openapi3.PathItem, seen map[*openapi3.Schema]struct{}) {
	if pathItem == nil {
		return
	}
	for _, parameterRef := range pathItem.Parameters {
		normalizeParameterRef(parameterRef, seen)
	}
	for _, operation := range pathItem.Operations() {
		normalizeOperation(operation, seen)
	}
}

func normalizeOperation(operation *openapi3.Operation, seen map[*openapi3.Schema]struct{}) {
	if operation == nil {
		return
	}
	for _, parameterRef := range operation.Parameters {
		normalizeParameterRef(parameterRef, seen)
	}
	if operation.RequestBody != nil {
		normalizeRequestBodyRef(operation.RequestBody, seen)
	}
	if operation.Responses != nil {
		for _, responseRef := range operation.Responses.Map() {
			normalizeResponseRef(responseRef, seen)
		}
	}
	for _, callbackRef := range operation.Callbacks {
		normalizeCallbackRef(callbackRef, seen)
	}
}

func normalizeCallbackRef(callbackRef *openapi3.CallbackRef, seen map[*openapi3.Schema]struct{}) {
	if callbackRef == nil || callbackRef.Value == nil {
		return
	}
	for _, pathItem := range callbackRef.Value.Map() {
		normalizePathItem(pathItem, seen)
	}
}

func normalizeParameterRef(parameterRef *openapi3.ParameterRef, seen map[*openapi3.Schema]struct{}) {
	if parameterRef == nil || parameterRef.Value == nil {
		return
	}
	normalizeSchemaRef(parameterRef.Value.Schema, seen)
	normalizeContent(parameterRef.Value.Content, seen)
}

func normalizeHeaderRef(headerRef *openapi3.HeaderRef, seen map[*openapi3.Schema]struct{}) {
	if headerRef == nil || headerRef.Value == nil {
		return
	}
	normalizeSchemaRef(headerRef.Value.Schema, seen)
	normalizeContent(headerRef.Value.Content, seen)
}

func normalizeRequestBodyRef(requestBodyRef *openapi3.RequestBodyRef, seen map[*openapi3.Schema]struct{}) {
	if requestBodyRef == nil || requestBodyRef.Value == nil {
		return
	}
	normalizeContent(requestBodyRef.Value.Content, seen)
}

func normalizeResponseRef(responseRef *openapi3.ResponseRef, seen map[*openapi3.Schema]struct{}) {
	if responseRef == nil || responseRef.Value == nil {
		return
	}
	for _, headerRef := range responseRef.Value.Headers {
		normalizeHeaderRef(headerRef, seen)
	}
	normalizeContent(responseRef.Value.Content, seen)
}

func normalizeContent(content openapi3.Content, seen map[*openapi3.Schema]struct{}) {
	for _, mediaType := range content {
		normalizeMediaType(mediaType, seen)
	}
}

func normalizeMediaType(mediaType *openapi3.MediaType, seen map[*openapi3.Schema]struct{}) {
	if mediaType == nil {
		return
	}
	normalizeSchemaRef(mediaType.Schema, seen)
	for _, encoding := range mediaType.Encoding {
		if encoding == nil {
			continue
		}
		for _, headerRef := range encoding.Headers {
			normalizeHeaderRef(headerRef, seen)
		}
	}
}

func normalizeSchemaRef(ref *openapi3.SchemaRef, seen map[*openapi3.Schema]struct{}) {
	if ref == nil || ref.Value == nil {
		return
	}
	normalizeSchema(ref.Value, seen)
}

func normalizeSchema(schema *openapi3.Schema, seen map[*openapi3.Schema]struct{}) {
	if schema == nil {
		return
	}
	if _, ok := seen[schema]; ok {
		return
	}
	seen[schema] = struct{}{}

	normalizeSchemaTypes(schema)
	normalizeSchemaConst(schema)
	normalizeSchemaContentEncoding(schema)

	if schema.Items != nil {
		normalizeSchemaRef(schema.Items, seen)
	}
	for _, ref := range schema.AllOf {
		normalizeSchemaRef(ref, seen)
	}
	for _, ref := range schema.AnyOf {
		normalizeSchemaRef(ref, seen)
	}
	for _, ref := range schema.OneOf {
		normalizeSchemaRef(ref, seen)
	}
	if schema.Not != nil {
		normalizeSchemaRef(schema.Not, seen)
	}
	for _, ref := range schema.Properties {
		normalizeSchemaRef(ref, seen)
	}
	if schema.AdditionalProperties.Schema != nil {
		normalizeSchemaRef(schema.AdditionalProperties.Schema, seen)
	}
}

func normalizeSchemaTypes(schema *openapi3.Schema) {
	if schema == nil || schema.Type == nil {
		return
	}
	raw := schema.Type.Slice()
	if len(raw) == 0 {
		schema.Type = nil
		return
	}
	if len(raw) == 1 && raw[0] == openapi3.TypeNull {
		schema.Nullable = true
		schema.Type = nil
		schema.Enum = []any{nil}
		return
	}
	filtered := make([]string, 0, len(raw))
	removedNull := false
	for _, candidate := range raw {
		if candidate == openapi3.TypeNull {
			schema.Nullable = true
			removedNull = true
			continue
		}
		filtered = append(filtered, candidate)
	}
	if !removedNull {
		return
	}
	if len(filtered) == 0 {
		schema.Type = nil
		schema.Enum = []any{nil}
		return
	}
	types := openapi3.Types(filtered)
	schema.Type = &types
}

func normalizeSchemaConst(schema *openapi3.Schema) {
	if schema == nil || len(schema.Extensions) == 0 {
		return
	}
	value, ok := schema.Extensions["const"]
	if !ok {
		return
	}
	schema.Enum = []any{value}
	delete(schema.Extensions, "const")
	if len(schema.Extensions) == 0 {
		schema.Extensions = nil
	}
}

func normalizeSchemaContentEncoding(schema *openapi3.Schema) {
	if schema == nil || len(schema.Extensions) == 0 {
		return
	}
	value, ok := schema.Extensions["contentEncoding"]
	if !ok {
		return
	}
	if encoding, ok := value.(string); ok {
		if schema.Format == "" {
			switch encoding {
			case "base64":
				schema.Format = "byte"
			case "binary":
				schema.Format = "binary"
			default:
				schema.Format = encoding
			}
		}
	}
	delete(schema.Extensions, "contentEncoding")
	if len(schema.Extensions) == 0 {
		schema.Extensions = nil
	}
}

func normalizeInfo(info *openapi3.Info) {
	if info == nil {
		return
	}
	info.Extensions = dropExtension(info.Extensions, "identifier")
	if info.License != nil {
		normalizeLicense(info.License)
	}
}

func normalizeLicense(license *openapi3.License) {
	if license == nil {
		return
	}
	license.Extensions = dropExtension(license.Extensions, "identifier")
}

func normalizeServers(servers openapi3.Servers) openapi3.Servers {
	if len(servers) == 0 {
		return servers
	}
	seen := make(map[string]struct{})
	var extras openapi3.Servers
	for _, server := range servers {
		if server == nil {
			continue
		}
		urlValue := strings.TrimSpace(server.URL)
		if urlValue == "" {
			continue
		}
		seen[urlValue] = struct{}{}
		u, err := url.Parse(urlValue)
		if err != nil {
			continue
		}
		path := u.EscapedPath()
		if path == "" {
			path = "/"
		}
		if _, ok := seen[path]; ok {
			continue
		}
		extras = append(extras, &openapi3.Server{URL: path})
		seen[path] = struct{}{}
	}
	if _, ok := seen["/"]; !ok {
		extras = append(extras, &openapi3.Server{URL: "/"})
	}
	return append(extras, servers...)
}
