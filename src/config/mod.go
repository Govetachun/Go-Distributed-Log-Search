package config

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strings"

	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

const VERSION = 1

// defaultVersion returns the default version
func defaultVersion() uint32 {
	return VERSION
}

// defaultTrue returns true as default value
func defaultTrue() bool {
	return true
}

// FastFieldNormalizerType represents fast field normalization options
// Equivalent to FastFieldNormalizerType enum in Rust
type FastFieldNormalizerType string

const (
	FastFieldNormalizerTypeFalse FastFieldNormalizerType = "false"
	FastFieldNormalizerTypeTrue  FastFieldNormalizerType = "true"
	FastFieldNormalizerTypeRaw   FastFieldNormalizerType = "raw"
)

// From converts FastFieldNormalizerType to string option
// Equivalent to From<FastFieldNormalizerType> for Option<&str> in Rust
func (f FastFieldNormalizerType) From() *string {
	switch f {
	case FastFieldNormalizerTypeFalse:
		return nil
	case FastFieldNormalizerTypeTrue:
		s := "default"
		return &s
	case FastFieldNormalizerTypeRaw:
		s := "raw"
		return &s
	default:
		return nil
	}
}

// FieldTokenizerType represents tokenizer types
// Equivalent to FieldTokenizerType enum in Rust
type FieldTokenizerType string

const (
	FieldTokenizerTypeDefault    FieldTokenizerType = "default"
	FieldTokenizerTypeRaw        FieldTokenizerType = "raw"
	FieldTokenizerTypeEnStem     FieldTokenizerType = "en_stem"
	FieldTokenizerTypeWhitespace FieldTokenizerType = "whitespace"
)

// From converts FieldTokenizerType to string
// Equivalent to From<FieldTokenizerType> for &str in Rust
func (f FieldTokenizerType) From() string {
	switch f {
	case FieldTokenizerTypeDefault:
		return "default"
	case FieldTokenizerTypeRaw:
		return "raw"
	case FieldTokenizerTypeEnStem:
		return "en_stem"
	case FieldTokenizerTypeWhitespace:
		return "whitespace"
	default:
		return "default"
	}
}

// FieldType represents different field types
// Equivalent to FieldType enum in Rust
type FieldType interface {
	IsIndexed() bool
}

// FieldTypeText represents a text field type
type FieldTypeText struct {
	Config TextFieldConfig
}

func (f FieldTypeText) IsIndexed() bool { return f.Config.IsIndexed() }

// FieldTypeNumber represents a number field type
type FieldTypeNumber struct {
	Config NumberFieldConfig
}

func (f FieldTypeNumber) IsIndexed() bool { return f.Config.IsIndexed() }

// FieldTypeBoolean represents a boolean field type
type FieldTypeBoolean struct {
	Config BooleanFieldConfig
}

func (f FieldTypeBoolean) IsIndexed() bool { return f.Config.IsIndexed() }

// FieldTypeDatetime represents a datetime field type
type FieldTypeDatetime struct {
	Config DateTimeFieldConfig
}

func (f FieldTypeDatetime) IsIndexed() bool { return f.Config.IsIndexed() }

// FieldTypeIp represents an IP field type
type FieldTypeIp struct {
	Config IpFieldConfig
}

func (f FieldTypeIp) IsIndexed() bool { return f.Config.IsIndexed() }

// FieldTypeDynamicObject represents a dynamic object field type
type FieldTypeDynamicObject struct {
	Config DynamicObjectFieldConfig
}

func (f FieldTypeDynamicObject) IsIndexed() bool { return f.Config.IsIndexed() }

// FieldTypeStaticObject represents a static object field type
type FieldTypeStaticObject struct {
	Config StaticObjectFieldConfig
}

func (f FieldTypeStaticObject) IsIndexed() bool { return f.Config.IsIndexed() }

// FieldConfig represents a field configuration
// Equivalent to FieldConfig struct in Rust
type FieldConfig struct {
	Name     string      `json:"name" yaml:"name"`
	Array    bool        `json:"array" yaml:"array"`
	Type     string      `json:"type" yaml:"type"` // Store as string for YAML parsing
	TypeImpl FieldType   `json:"-" yaml:"-"`       // Actual implementation, populated after parsing
	Stored   bool        `json:"stored" yaml:"stored"`
	Fast     interface{} `json:"fast" yaml:"fast"`
	Indexed  interface{} `json:"indexed" yaml:"indexed"`
}

// FieldConfigs represents a slice of field configurations
// Equivalent to FieldConfigs struct in Rust
type FieldConfigs []FieldConfig

// convertType converts the string type to actual FieldType implementation
func (fc *FieldConfig) convertType() error {
	logrus.Debugf("Converting field %s with type %s", fc.Name, fc.Type)
	// Set default values if not specified
	if fc.Stored == false {
		fc.Stored = true
	}

	// Convert fast field
	var fastField FastFieldNormalizerType
	if fc.Fast == nil || fc.Fast == false {
		fastField = FastFieldNormalizerTypeFalse
	} else if fc.Fast == true {
		fastField = FastFieldNormalizerTypeTrue
	} else if fastStr, ok := fc.Fast.(string); ok {
		fastField = FastFieldNormalizerType(fastStr)
	} else {
		fastField = FastFieldNormalizerTypeFalse
	}

	// Convert indexed field
	var indexedField interface{}
	if fc.Indexed == nil || fc.Indexed == false {
		indexedField = &IndexedTextFieldTypeFalse{}
	} else if fc.Indexed == true {
		indexedField = &IndexedTextFieldTypeTrue{}
	} else {
		// Handle indexed configuration
		if indexedMap, ok := fc.Indexed.(map[string]interface{}); ok {
			// Create indexed config based on type
			switch fc.Type {
			case "text":
				indexedConfig := &IndexedTextFieldConfig{
					Record:     IndexRecordOptionBasic,
					Fieldnorms: true,
					Tokenizer:  FieldTokenizerTypeDefault,
				}

				if tokenizer, ok := indexedMap["tokenizer"].(string); ok {
					indexedConfig.Tokenizer = FieldTokenizerType(tokenizer)
				}
				if record, ok := indexedMap["record"].(string); ok {
					indexedConfig.Record = IndexRecordOption(record)
				}

				indexedField = &IndexedTextFieldTypeIndexed{Config: *indexedConfig}
			default:
				indexedField = &IndexedTextFieldTypeTrue{}
			}
		} else {
			indexedField = &IndexedTextFieldTypeTrue{}
		}
	}

	switch fc.Type {
	case "text":
		var indexed IndexedTextFieldType
		if indexedField == nil {
			indexed = &IndexedTextFieldTypeTrue{}
		} else if _, ok := indexedField.(*IndexedTextFieldTypeFalse); ok {
			indexed = &IndexedTextFieldTypeFalse{}
		} else if _, ok := indexedField.(*IndexedTextFieldTypeTrue); ok {
			indexed = &IndexedTextFieldTypeTrue{}
		} else if indexedConfig, ok := indexedField.(*IndexedTextFieldTypeIndexed); ok {
			indexed = indexedConfig
		} else {
			indexed = &IndexedTextFieldTypeTrue{}
		}

		fc.TypeImpl = &FieldTypeText{
			Config: TextFieldConfig{
				Stored:  FastFieldNormalizerType(fmt.Sprintf("%v", fc.Stored)),
				Indexed: indexed,
				Fast:    fastField,
			},
		}
	case "number":
		fc.TypeImpl = &FieldTypeNumber{
			Config: NumberFieldConfig{
				Type:    NumberFieldTypeF64,
				Stored:  fc.Stored,
				Fast:    fastField == FastFieldNormalizerTypeTrue,
				Indexed: indexedField != nil,
			},
		}
	case "boolean":
		fc.TypeImpl = &FieldTypeBoolean{
			Config: BooleanFieldConfig{
				Stored:  fc.Stored,
				Fast:    fastField == FastFieldNormalizerTypeTrue,
				Indexed: indexedField != nil,
			},
		}
	case "datetime":
		fc.TypeImpl = &FieldTypeDatetime{
			Config: DateTimeFieldConfig{
				Stored:  fc.Stored,
				Indexed: indexedField != nil,
				Fast:    DateTimeFastPrecisionTypeFalse,
				Formats: DefaultDateTimeFormats(),
			},
		}
	case "ip":
		fc.TypeImpl = &FieldTypeIp{
			Config: IpFieldConfig{
				Stored:  fc.Stored,
				Fast:    fastField == FastFieldNormalizerTypeTrue,
				Indexed: indexedField != nil,
			},
		}
	case "dynamic_object":
		fc.TypeImpl = &FieldTypeDynamicObject{
			Config: DynamicObjectFieldConfig{
				Stored:     fc.Stored,
				Fast:       fastField,
				Indexed:    &IndexedDynamicObjectFieldTypeTrue{},
				ExpandDots: true,
			},
		}
	case "static_object":
		fc.TypeImpl = &FieldTypeStaticObject{
			Config: StaticObjectFieldConfig{
				Fields: FieldConfigs{},
			},
		}
	default:
		return fmt.Errorf("unknown field type: %s", fc.Type)
	}
	return nil
}

// GetIndexedInner gets indexed fields with optional parent name
// Equivalent to get_indexed_inner method in Rust
func (fields FieldConfigs) GetIndexedInner(parentName *string) []FieldConfig {
	var indexedFields []FieldConfig

	for _, field := range fields {
		if field.TypeImpl != nil && field.TypeImpl.IsIndexed() {
			indexedFields = append(indexedFields, field.WithParentName(parentName))
		}
	}

	// Handle static object fields recursively
	for _, field := range fields {
		if staticObj, ok := field.TypeImpl.(*StaticObjectFieldConfig); ok {
			name := EscapedWithParentName(field.Name, parentName)
			parentNamePtr := &name
			nestedIndexed := staticObj.Fields.GetIndexedInner(parentNamePtr)
			indexedFields = append(indexedFields, nestedIndexed...)
		}
	}

	return indexedFields
}

// GetIndexed returns only the indexed fields from the field configs
// Equivalent to get_indexed method in Rust
func (fields FieldConfigs) GetIndexed() []FieldConfig {
	return fields.GetIndexedInner(nil)
}

// WithParentName creates a field config with parent name
// Equivalent to with_parent_name method in Rust
func (fc FieldConfig) WithParentName(parentName *string) FieldConfig {
	return FieldConfig{
		Name:  EscapedWithParentName(fc.Name, parentName),
		Array: fc.Array,
		Type:  fc.Type,
	}
}

// IndexSchema represents the index schema configuration
// Equivalent to IndexSchema struct in Rust
type IndexSchema struct {
	Fields    FieldConfigs `json:"fields" yaml:"fields"`
	TimeField *string      `json:"time_field,omitempty" yaml:"time_field,omitempty"`
}

// IndexConfig represents the main index configuration
// Equivalent to IndexConfig struct in Rust
type IndexConfig struct {
	Name    string      `json:"name" yaml:"name"`
	Path    string      `json:"path" yaml:"path"`
	Version uint32      `json:"version" yaml:"version"`
	Schema  IndexSchema `json:"schema" yaml:"schema"`
}

// FromPath loads an index configuration from a file path
// Equivalent to from_path method in Rust
func (ic *IndexConfig) FromPath(path string) error {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}
	return ic.FromString(string(data))
}

// FromString loads an index configuration from a string
// Equivalent to from_str implementation in Rust
func (ic *IndexConfig) FromString(s string) error {
	err := yaml.Unmarshal([]byte(s), ic)
	if err != nil {
		return err
	}

	// Convert string types to actual FieldType implementations
	return ic.ConvertFieldTypes()
}

// ConvertFieldTypes converts string type names to actual FieldType implementations
func (ic *IndexConfig) ConvertFieldTypes() error {
	for i := range ic.Schema.Fields {
		if err := ic.Schema.Fields[i].convertType(); err != nil {
			return err
		}
	}
	return nil
}

// SplitObjectFieldName splits a field name by dots
// Equivalent to split_object_field_name function in Rust
func SplitObjectFieldName(s string) []string {
	var result []string
	start := 0

	for i, c := range s {
		if c == '.' && (i == 0 || (i > 0 && s[i-1:i] != "\\")) {
			result = append(result, s[start:i])
			start = i + 1
		}
	}

	result = append(result, s[start:])
	return result
}

// EscapedFieldName escapes dots in field names
// Equivalent to escaped_field_name function in Rust
func EscapedFieldName(name string) string {
	return strings.ReplaceAll(name, ".", "\\.")
}

// UnescapedFieldName unescapes dots in field names
// Equivalent to unescaped_field_name function in Rust
func UnescapedFieldName(name string) string {
	return strings.ReplaceAll(name, "\\.", ".")
}

// EscapedWithParentName creates an escaped field name with optional parent
// Equivalent to escaped_with_parent_name function in Rust
func EscapedWithParentName(name string, parentName *string) string {
	escaped := EscapedFieldName(name)
	if parentName != nil {
		return filepath.Join(*parentName, escaped)
	}
	return escaped
}

// LoadIndexConfigFromPath loads an index configuration from a file path
// Equivalent to IndexConfig::from_path in Rust
func LoadIndexConfigFromPath(path string) (*IndexConfig, error) {
	var config IndexConfig
	err := config.FromPath(path)
	if err != nil {
		return nil, err
	}
	return &config, nil
}
