package completions

import (
	"encoding/json"
	"fmt"
)

const schemaTypeObject = "object"

// normalizeSchemaForKimi converts the supported subset of standard JSON
// Schema into Moonshot-flavored JSON Schema (MFJS). It always works on a deep
// copy so a provider request cannot mutate the caller's tool definition.
func normalizeSchemaForKimi(value any) (any, error) {
	if value == nil {
		return nil, nil
	}

	data, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("marshal: %w", err)
	}
	var decoded any
	if err := json.Unmarshal(data, &decoded); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	switch schema := decoded.(type) {
	case nil:
		return nil, nil
	case map[string]any:
		if err := normalizeKimiSchemaMap(schema, "$"); err != nil {
			return nil, err
		}
		return schema, nil
	case bool:
		return nil, fmt.Errorf("$: boolean schemas are not supported")
	default:
		return nil, fmt.Errorf("$: expected a schema object, got %T", decoded)
	}
}

func normalizeKimiSchemaMap(schema map[string]any, path string) error {
	if schema == nil {
		return nil
	}

	if rawAnyOf, exists := schema["anyOf"]; exists {
		anyOf, ok := rawAnyOf.([]any)
		if !ok {
			return fmt.Errorf("%s.anyOf: expected an array", path)
		}
		if err := normalizeKimiAnyOf(schema, anyOf, path); err != nil {
			return err
		}
		for index, rawBranch := range anyOf {
			branch, ok := rawBranch.(map[string]any)
			if !ok {
				return fmt.Errorf("%s.anyOf[%d]: boolean schemas are not supported", path, index)
			}
			if err := normalizeKimiSchemaMap(branch, fmt.Sprintf("%s.anyOf[%d]", path, index)); err != nil {
				return err
			}
		}
	}

	for _, key := range []string{"oneOf", "allOf"} {
		rawList, exists := schema[key]
		if !exists {
			continue
		}
		list, ok := rawList.([]any)
		if !ok {
			return fmt.Errorf("%s.%s: expected an array", path, key)
		}
		for index, rawItem := range list {
			item, ok := rawItem.(map[string]any)
			if !ok {
				return fmt.Errorf("%s.%s[%d]: boolean schemas are not supported", path, key, index)
			}
			if err := normalizeKimiSchemaMap(item, fmt.Sprintf("%s.%s[%d]", path, key, index)); err != nil {
				return err
			}
		}
	}

	if rawNot, exists := schema["not"]; exists {
		not, ok := rawNot.(map[string]any)
		if !ok {
			return fmt.Errorf("%s.not: boolean schemas are not supported", path)
		}
		if err := normalizeKimiSchemaMap(not, path+".not"); err != nil {
			return err
		}
	}

	if rawProperties, exists := schema["properties"]; exists {
		properties, ok := rawProperties.(map[string]any)
		if !ok {
			return fmt.Errorf("%s.properties: expected an object", path)
		}
		for name, rawProperty := range properties {
			property, ok := rawProperty.(map[string]any)
			if !ok {
				return fmt.Errorf("%s.properties.%s: boolean schemas are not supported", path, name)
			}
			if err := normalizeKimiSchemaMap(property, path+".properties."+name); err != nil {
				return err
			}
		}
	}

	if rawItems, exists := schema["items"]; exists {
		switch items := rawItems.(type) {
		case map[string]any:
			if err := normalizeKimiSchemaMap(items, path+".items"); err != nil {
				return err
			}
		case []any:
			for index, rawItem := range items {
				item, ok := rawItem.(map[string]any)
				if !ok {
					return fmt.Errorf("%s.items[%d]: boolean schemas are not supported", path, index)
				}
				if err := normalizeKimiSchemaMap(item, fmt.Sprintf("%s.items[%d]", path, index)); err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("%s.items: expected an object or array", path)
		}
	}

	if rawAdditional, exists := schema["additionalProperties"]; exists {
		switch additional := rawAdditional.(type) {
		case bool:
		case map[string]any:
			if err := normalizeKimiSchemaMap(additional, path+".additionalProperties"); err != nil {
				return err
			}
		default:
			return fmt.Errorf("%s.additionalProperties: expected a boolean or an object", path)
		}
	}

	for _, key := range []string{"$defs", "definitions"} {
		rawDefinitions, exists := schema[key]
		if !exists {
			continue
		}
		definitions, ok := rawDefinitions.(map[string]any)
		if !ok {
			return fmt.Errorf("%s.%s: expected an object", path, key)
		}
		for name, rawDefinition := range definitions {
			definition, ok := rawDefinition.(map[string]any)
			if !ok {
				return fmt.Errorf("%s.%s.%s: boolean schemas are not supported", path, key, name)
			}
			if err := normalizeKimiSchemaMap(definition, path+"."+key+"."+name); err != nil {
				return err
			}
		}
	}

	return nil
}

func normalizeKimiAnyOf(schema map[string]any, anyOf []any, path string) error {
	rawParentType, hasParentType := schema[keyType]
	if !hasParentType {
		return nil
	}
	parentType, ok := rawParentType.(string)
	if !ok || parentType == "" {
		return fmt.Errorf("%s.type: expected a single non-empty type", path)
	}

	hasObjectBundle := hasAnySchemaKeyword(schema, "properties", "required", "additionalProperties")
	if parentType == schemaTypeObject && hasObjectBundle {
		return distributeKimiObjectBundle(schema, anyOf, path)
	}
	if hasObjectBundle {
		return fmt.Errorf("%s: object keywords cannot be combined with type %q around anyOf", path, parentType)
	}

	for index, rawBranch := range anyOf {
		branch, ok := rawBranch.(map[string]any)
		if !ok {
			return fmt.Errorf("%s.anyOf[%d]: boolean schemas are not supported", path, index)
		}
		if rawBranchType, exists := branch[keyType]; exists {
			branchType, ok := rawBranchType.(string)
			if !ok || branchType != parentType {
				return fmt.Errorf(
					"%s.anyOf[%d].type: %v conflicts with parent type %q",
					path,
					index,
					rawBranchType,
					parentType,
				)
			}
		} else {
			branch[keyType] = parentType
		}
	}
	delete(schema, keyType)
	return nil
}

func distributeKimiObjectBundle(schema map[string]any, anyOf []any, path string) error {
	for key := range schema {
		if isKimiObjectBundleKeyword(key) || isSchemaAnnotationKeyword(key) {
			continue
		}
		return fmt.Errorf("%s.%s: cannot safely distribute this keyword into anyOf", path, key)
	}

	rawProperties, exists := schema["properties"]
	if !exists {
		return fmt.Errorf("%s.properties: required when object constraints surround anyOf", path)
	}
	properties, ok := rawProperties.(map[string]any)
	if !ok {
		return fmt.Errorf("%s.properties: expected an object", path)
	}

	parentRequired, err := schemaStringArray(schema["required"], path+".required")
	if err != nil {
		return err
	}
	if err := validateRequiredProperties(parentRequired, properties, path+".required"); err != nil {
		return err
	}

	rawAdditional, hasAdditional := schema["additionalProperties"]
	if hasAdditional {
		if _, ok := rawAdditional.(bool); !ok {
			return fmt.Errorf("%s.additionalProperties: schema-valued constraints cannot be safely distributed", path)
		}
	}

	for index, rawBranch := range anyOf {
		branchPath := fmt.Sprintf("%s.anyOf[%d]", path, index)
		branch, ok := rawBranch.(map[string]any)
		if !ok {
			return fmt.Errorf("%s: boolean schemas are not supported", branchPath)
		}
		for key := range branch {
			if key == keyType || key == "required" || isSchemaAnnotationKeyword(key) {
				continue
			}
			return fmt.Errorf("%s.%s: cannot safely merge this keyword with parent object constraints", branchPath, key)
		}
		if rawBranchType, exists := branch[keyType]; exists {
			branchType, ok := rawBranchType.(string)
			if !ok || branchType != schemaTypeObject {
				return fmt.Errorf("%s.type: %v conflicts with parent type %q", branchPath, rawBranchType, schemaTypeObject)
			}
		}
		branchRequired, err := schemaStringArray(branch["required"], branchPath+".required")
		if err != nil {
			return err
		}
		required := mergeSchemaStrings(parentRequired, branchRequired)
		if err := validateRequiredProperties(required, properties, branchPath+".required"); err != nil {
			return err
		}

		branch[keyType] = schemaTypeObject
		branch["properties"] = cloneJSONValue(properties)
		if hasAdditional {
			branch["additionalProperties"] = rawAdditional
		}
		if len(required) > 0 {
			requiredValues := make([]any, len(required))
			for index, name := range required {
				requiredValues[index] = name
			}
			branch["required"] = requiredValues
		} else {
			delete(branch, "required")
		}
	}

	delete(schema, keyType)
	delete(schema, "properties")
	delete(schema, "required")
	delete(schema, "additionalProperties")
	return nil
}

func hasAnySchemaKeyword(schema map[string]any, keys ...string) bool {
	for _, key := range keys {
		if _, exists := schema[key]; exists {
			return true
		}
	}
	return false
}

func isKimiObjectBundleKeyword(key string) bool {
	switch key {
	case keyType, "properties", "required", "additionalProperties", "anyOf":
		return true
	default:
		return false
	}
}

func isSchemaAnnotationKeyword(key string) bool {
	switch key {
	case "$comment", "title", "description", "default", "examples", "deprecated", "readOnly", "writeOnly":
		return true
	default:
		return false
	}
}

func schemaStringArray(value any, path string) ([]string, error) {
	if value == nil {
		return nil, nil
	}
	switch values := value.(type) {
	case []any:
		result := make([]string, 0, len(values))
		for index, rawValue := range values {
			item, ok := rawValue.(string)
			if !ok || item == "" {
				return nil, fmt.Errorf("%s[%d]: expected a non-empty string", path, index)
			}
			result = append(result, item)
		}
		return result, nil
	case []string:
		return append([]string(nil), values...), nil
	default:
		return nil, fmt.Errorf("%s: expected an array of strings", path)
	}
}

func mergeSchemaStrings(left, right []string) []string {
	seen := make(map[string]struct{}, len(left)+len(right))
	result := make([]string, 0, len(left)+len(right))
	for _, values := range [][]string{left, right} {
		for _, value := range values {
			if _, exists := seen[value]; exists {
				continue
			}
			seen[value] = struct{}{}
			result = append(result, value)
		}
	}
	return result
}

func validateRequiredProperties(required []string, properties map[string]any, path string) error {
	for _, name := range required {
		if _, exists := properties[name]; !exists {
			return fmt.Errorf("%s: required property %q is not defined in properties", path, name)
		}
	}
	return nil
}

func cloneJSONValue(value any) any {
	switch value := value.(type) {
	case map[string]any:
		cloned := make(map[string]any, len(value))
		for key, item := range value {
			cloned[key] = cloneJSONValue(item)
		}
		return cloned
	case []any:
		cloned := make([]any, len(value))
		for index, item := range value {
			cloned[index] = cloneJSONValue(item)
		}
		return cloned
	case []string:
		return append([]string(nil), value...)
	default:
		return value
	}
}
