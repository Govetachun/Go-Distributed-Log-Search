package commands

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	"toshokan/src/config"
)

// ParseFunc represents a function that parses a JSON value into a specific type
type ParseFunc func(value interface{}) (interface{}, error)

// FieldParserVariation represents the different types of field parsers
type FieldParserVariation interface {
	isFieldParserVariation()
}

// ValueFieldParser represents a parser for a simple value field
type ValueFieldParser struct {
	Field   string    // Field identifier
	ParseFn ParseFunc // Function to parse the value
}

func (v ValueFieldParser) isFieldParserVariation() {}

// ObjectFieldParser represents a parser for an object with nested parsers
type ObjectFieldParser struct {
	Parsers []*FieldParser // Nested field parsers
}

func (o ObjectFieldParser) isFieldParserVariation() {}

// FieldParser represents a field parser with metadata
type FieldParser struct {
	// The field name. Example: "world"
	Name string

	// The full flattened and escaped name. Example: "hello.world"
	// Only used for debug logging
	FullName string

	// Whether the field is a simple field or an object of parsers
	Variation FieldParserVariation

	// Whether the field is an array
	IsArray bool
}

// AddParsedFieldValue parses the JSON value and adds it to the document
// Equivalent to add_parsed_field_value in Rust
func (fp *FieldParser) AddParsedFieldValue(doc map[string]interface{}, jsonValue interface{}) error {
	switch variation := fp.Variation.(type) {
	case ValueFieldParser:
		if fp.IsArray {
			// Handle array values
			values, ok := jsonValue.([]interface{})
			if !ok {
				return fmt.Errorf("expected array for field %s", fp.Name)
			}

			var parsedValues []interface{}
			for _, value := range values {
				parsed, err := variation.ParseFn(value)
				if err != nil {
					return fmt.Errorf("failed to parse array element for field %s: %w", fp.Name, err)
				}
				parsedValues = append(parsedValues, parsed)
			}
			doc[variation.Field] = parsedValues
		} else {
			// Handle single value
			parsed, err := variation.ParseFn(jsonValue)
			if err != nil {
				return fmt.Errorf("failed to parse field %s: %w", fp.Name, err)
			}
			doc[variation.Field] = parsed
		}

	case ObjectFieldParser:
		// Handle object with nested parsers
		jsonObj, ok := jsonValue.(map[string]interface{})
		if !ok {
			return fmt.Errorf("expected object for field %s", fp.Name)
		}

		for _, parser := range variation.Parsers {
			value, exists := jsonObj[parser.Name]
			if !exists {
				// Field in schema but not found in data - this is okay, just log
				continue
			}

			if err := parser.AddParsedFieldValue(doc, value); err != nil {
				return err
			}
		}

	default:
		return fmt.Errorf("unknown field parser variation for field %s", fp.Name)
	}

	return nil
}

// commonParse handles basic JSON value parsing
func commonParse(value interface{}) (interface{}, error) {
	return value, nil
}

// buildParserFromFieldConfig creates a FieldParser from a FieldConfig
// Equivalent to build_parser_from_field_config in Rust
func buildParserFromFieldConfig(
	fieldConfig config.FieldConfig,
	fullName string,
) (*FieldParser, error) {
	var variation FieldParserVariation

	switch fieldType := fieldConfig.Type.(type) {
	case config.TextFieldType:
		variation = ValueFieldParser{
			Field:   fullName,
			ParseFn: commonParse,
		}

	case config.NumberFieldType:
		parseString := fieldType.ParseString
		numberType := fieldType.Type

		parseFunc := func(value interface{}) (interface{}, error) {
			if !parseString {
				return commonParse(value)
			}

			// Try to parse as string first
			if valueStr, ok := value.(string); ok {
				switch numberType {
				case config.NumberTypeU64:
					val, err := strconv.ParseUint(valueStr, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("failed to parse '%s' as uint64: %w", valueStr, err)
					}
					return val, nil
				case config.NumberTypeI64:
					val, err := strconv.ParseInt(valueStr, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("failed to parse '%s' as int64: %w", valueStr, err)
					}
					return val, nil
				case config.NumberTypeF64:
					val, err := strconv.ParseFloat(valueStr, 64)
					if err != nil {
						return nil, fmt.Errorf("failed to parse '%s' as float64: %w", valueStr, err)
					}
					return val, nil
				}
			}

			return commonParse(value)
		}

		variation = ValueFieldParser{
			Field:   fullName,
			ParseFn: parseFunc,
		}

	case config.BooleanFieldType:
		parseString := fieldType.ParseString

		parseFunc := func(value interface{}) (interface{}, error) {
			if !parseString {
				return commonParse(value)
			}

			if valueStr, ok := value.(string); ok {
				trimmed := strings.TrimSpace(valueStr)
				if len(trimmed) < 4 || len(trimmed) > 5 {
					return nil, fmt.Errorf("cannot parse '%s' as boolean", trimmed)
				}

				switch strings.ToLower(trimmed) {
				case "true":
					return true, nil
				case "false":
					return false, nil
				default:
					return nil, fmt.Errorf("cannot parse '%s' as boolean", trimmed)
				}
			}

			return commonParse(value)
		}

		variation = ValueFieldParser{
			Field:   fullName,
			ParseFn: parseFunc,
		}

	case config.DatetimeFieldType:
		parseFunc := func(value interface{}) (interface{}, error) {
			return fieldType.Formats.TryParse(value)
		}

		variation = ValueFieldParser{
			Field:   fullName,
			ParseFn: parseFunc,
		}

	case config.IPFieldType:
		parseFunc := func(value interface{}) (interface{}, error) {
			ipStr, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf("expected string for IP field")
			}

			addr := net.ParseIP(ipStr)
			if addr == nil {
				return nil, fmt.Errorf("invalid IP address: %s", ipStr)
			}

			// Convert to IPv6 format (like Rust implementation)
			if addr.To4() != nil {
				// IPv4 address, convert to IPv6 mapped
				return addr.To16(), nil
			}

			return addr, nil
		}

		variation = ValueFieldParser{
			Field:   fullName,
			ParseFn: parseFunc,
		}

	case config.DynamicObjectFieldType:
		variation = ValueFieldParser{
			Field:   fullName,
			ParseFn: commonParse,
		}

	case config.StaticObjectFieldType:
		// Build nested parsers for static object fields
		parsers, err := buildParsersFromFieldConfigsInner(
			fieldType.Fields,
			&fullName,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to build parsers for static object %s: %w", fullName, err)
		}

		variation = ObjectFieldParser{
			Parsers: parsers,
		}

	default:
		return nil, fmt.Errorf("unsupported field type for field %s", fieldConfig.Name)
	}

	return &FieldParser{
		Name:      fieldConfig.Name,
		FullName:  fullName,
		Variation: variation,
		IsArray:   fieldConfig.Array,
	}, nil
}

// buildParsersFromFieldConfigsInner builds parsers with optional parent name
func buildParsersFromFieldConfigsInner(
	fields config.FieldConfigs,
	parentName *string,
) ([]*FieldParser, error) {
	var parsers []*FieldParser

	for _, field := range fields {
		name := config.EscapedWithParentName(field.Name, parentName)
		parser, err := buildParserFromFieldConfig(field, name)
		if err != nil {
			return nil, err
		}
		parsers = append(parsers, parser)
	}

	return parsers, nil
}

// BuildParsersFromFieldConfigs builds field parsers from field configurations
// Equivalent to build_parsers_from_field_configs in Rust
func BuildParsersFromFieldConfigs(fields config.FieldConfigs) ([]*FieldParser, error) {
	return buildParsersFromFieldConfigsInner(fields, nil)
}
